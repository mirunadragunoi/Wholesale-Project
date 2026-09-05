"""SMPP egress connector.

A pool of N transceiver binds to one provider. Messages are encoded (GSM7/UCS-2,
segmented with UDH) and submitted across the binds; a single aggregate token
bucket caps TPS per provider (the contractual limit is per account, not per
connection). Responses are classified: temporary errors and timeouts leave the
message for redelivery (never acked), permanent errors are dropped, success is
acked. deliver_sm DLRs are parsed and logged only — no DLR pipeline yet.

The connector exposes ``handle(batch) -> list[str]`` so it plugs into the same
egress consume-loop as the HTTP connector.
"""

from __future__ import annotations

import asyncio
import re
import time
from enum import Enum, auto

from relay.common.config import SmppEgressConfig
from relay.common.logging import get_logger
from relay.common.metrics import (
    egress_submit_duration_seconds,
    egress_submitted_total,
    end_to_end_duration_seconds,
    smpp_bind_state,
    smpp_window_usage,
)
from relay.egress.shaper import TokenBucket
from relay.queues.base import ReceivedMessage
from relay.smpp.constants import ErrorCategory, Tlv, classify
from relay.smpp.encoding import EncodedMessage, encode_message
from relay.smpp.pdu import PDU, BindTransceiver, DeliverSm, SubmitSm, SubmitSmResp
from relay.smpp.session import (
    BindError,
    SessionClosed,
    SessionConfig,
    SessionState,
    SmppSession,
    SubmitTimeout,
    backoff_delays,
    open_connection,
)

_log = get_logger("egress.smpp")

_DLR_RE = re.compile(r"id:(?P<id>\S+).*?stat:(?P<stat>\w+)", re.IGNORECASE)


class Outcome(Enum):
    ACK = auto()  # provider accepted — delete from queue
    PERMANENT = auto()  # will never succeed — drop (delete), do not retry
    TEMPORARY = auto()  # retry via redelivery — do NOT delete
    TIMEOUT = auto()  # response lost; may have been sent — retry via redelivery
    NO_BIND = auto()  # no bind available — retry via redelivery


class SmppEgressConnector:
    def __init__(self, config: SmppEgressConfig) -> None:
        self._config = config
        self._name = config.connector_name
        self._shaper = TokenBucket(config.tps_limit)
        self._binds: list[SmppSession | None] = [None] * config.bind_count
        self._supervisors: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        self._rr = 0  # round-robin cursor
        self._ref_counters: dict[str, int] = {}  # per-destination concat reference
        # message_id correlation (provider id -> our ULID), bounded by nature of POC
        self._submitted_ids: dict[str, str] = {}
        # DLR observability (also lets tests assert the hex/dec correlation trap)
        self.dlr_count = 0
        self.dlr_correlated = 0
        self.dlr_missed = 0

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> None:
        self._supervisors = [
            asyncio.create_task(self._supervise(i)) for i in range(self._config.bind_count)
        ]

    async def stop(self) -> None:
        self._stopping.set()
        for session in self._binds:
            if session is not None:
                await session.unbind()
        for task in self._supervisors:
            task.cancel()

    async def _supervise(self, index: int) -> None:
        cfg = self._config
        bind_id = str(index)
        delays = backoff_delays(base=1.0, cap=60.0)
        while not self._stopping.is_set():
            smpp_bind_state.labels(connector=self._name, bind_id=bind_id).set(0)
            try:
                reader, writer = await open_connection(cfg.host, cfg.port)
                session = SmppSession(
                    reader,
                    writer,
                    SessionConfig(
                        window_size=cfg.window_size,
                        response_timeout=cfg.submit_timeout_s,
                    ),
                    on_deliver_sm=self._on_deliver_sm,
                    log=_log.bind(bind_id=bind_id),
                )
                session.start()
                smpp_bind_state.labels(connector=self._name, bind_id=bind_id).set(1)
                await session.bind(
                    BindTransceiver(
                        system_id=cfg.system_id,
                        password=cfg.password,
                        system_type=cfg.system_type,
                    ),
                    SessionState.BOUND_TRX,
                )
                smpp_bind_state.labels(connector=self._name, bind_id=bind_id).set(2)
                self._binds[index] = session
                _log.info("smpp_bind_ready", bind_id=bind_id)
                await session.wait_closed()
            except (BindError, ConnectionError, OSError, SubmitTimeout, SessionClosed) as exc:
                _log.warning("smpp_bind_error", bind_id=bind_id, error=str(exc))
            finally:
                self._binds[index] = None
                smpp_bind_state.labels(connector=self._name, bind_id=bind_id).set(0)
            if self._stopping.is_set():
                return
            await asyncio.sleep(next(delays))

    def _pick_bind(self) -> tuple[int, SmppSession] | None:
        n = len(self._binds)
        for _ in range(n):
            idx = self._rr % n
            self._rr += 1
            session = self._binds[idx]
            if session is not None and session.state == SessionState.BOUND_TRX:
                return idx, session
        return None

    def _next_ref(self, dest: str) -> int:
        ref = (self._ref_counters.get(dest, 0) + 1) & 0xFF
        self._ref_counters[dest] = ref
        return ref

    # -- submit path -------------------------------------------------------- #
    async def handle(self, batch: list[ReceivedMessage]) -> list[str]:
        results = await asyncio.gather(*(self._submit_message(item) for item in batch))
        return [handle for handle, delete in results if delete]

    async def _submit_message(self, item: ReceivedMessage) -> tuple[str, bool]:
        msg = item.message
        dest = msg.to.lstrip("+")
        ref = self._next_ref(dest)
        encoded = encode_message(msg.text, ref=ref)
        outcome = await self._submit_segments(encoded, msg.id, dest, msg.sender)

        result_label = outcome.name.lower()
        egress_submitted_total.labels(connector=self._name, result=result_label).inc()
        end_to_end_duration_seconds.observe(max(0.0, time.time() - msg.received_at))
        # Delete (ack) on ACK or PERMANENT; leave temporary/timeout/no-bind for redelivery.
        delete = outcome in (Outcome.ACK, Outcome.PERMANENT)
        if outcome == Outcome.PERMANENT:
            _log.warning("smpp_permanent_failure", message_id=msg.id, to=msg.to)
        return item.handle, delete

    async def _submit_segments(
        self, encoded: EncodedMessage, message_id: str, dest: str, sender: str | None
    ) -> Outcome:
        source = sender or self._config.source_addr
        source_ton = self._config.source_ton if sender is None else 5
        for segment in encoded.segments:
            await self._shaper.acquire()
            picked = self._pick_bind()
            if picked is None:
                return Outcome.NO_BIND
            idx, session = picked
            submit = SubmitSm(
                source_addr=source,
                source_addr_ton=source_ton,
                source_addr_npi=self._config.source_npi,
                destination_addr=dest,
                dest_addr_ton=self._config.dest_ton,
                dest_addr_npi=self._config.dest_npi,
                esm_class=segment.esm_class,
                data_coding=segment.data_coding,
                registered_delivery=self._config.registered_delivery,
                short_message=segment.data,
                # Carry our ULID for end-to-end correlation / dup detection.
                tlvs=((int(Tlv.RELAY_MESSAGE_ID), message_id.encode("latin-1")),),
            )
            try:
                with egress_submit_duration_seconds.labels(connector=self._name).time():
                    resp = await session.request(submit, self._config.submit_timeout_s)
            except SubmitTimeout:
                return Outcome.TIMEOUT
            except SessionClosed:
                return Outcome.NO_BIND
            finally:
                smpp_window_usage.labels(connector=self._name, bind_id=str(idx)).set(
                    session.window_used
                )
            category = classify(resp.command_status)
            if category == ErrorCategory.OK:
                if isinstance(resp, SubmitSmResp) and resp.message_id:
                    self._submitted_ids[resp.message_id] = message_id
                continue  # next segment
            if category == ErrorCategory.PERMANENT:
                return Outcome.PERMANENT
            # TEMPORARY or FATAL -> retry via redelivery (FATAL also tears the bind).
            return Outcome.TEMPORARY
        return Outcome.ACK

    # -- DLR (log only) ----------------------------------------------------- #
    async def _on_deliver_sm(self, pdu: PDU) -> None:
        assert isinstance(pdu, DeliverSm)
        text = pdu.short_message.decode("latin-1", errors="replace")
        match = _DLR_RE.search(text)
        provider_id = match.group("id") if match else "?"
        stat = match.group("stat") if match else "?"
        our_id = self._submitted_ids.get(provider_id)
        self.dlr_count += 1
        if our_id is not None:
            self.dlr_correlated += 1
        else:
            self.dlr_missed += 1
        _log.info(
            "smpp_dlr",
            provider_id=provider_id,
            stat=stat,
            correlated=our_id is not None,
            message_id=our_id,
        )
