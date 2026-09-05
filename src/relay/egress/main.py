"""Egress process entry point. Selects the HTTP or SMPP connector by config."""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

import aiohttp

from relay.common.config import HttpEgressConfig, SmppEgressConfig, load_config
from relay.common.logging import configure_logging, get_logger
from relay.common.metrics import start_metrics_server
from relay.common.worker import consume_loop, install_shutdown
from relay.egress.http_connector import HttpEgressConnector
from relay.egress.smpp_connector import SmppEgressConnector
from relay.queues.factory import create_backend

_log = get_logger("egress")


async def run_http(config: HttpEgressConfig) -> None:
    backend = create_backend(config.queue)
    await backend.start()
    consumer = await backend.consumer(config.egress_queue)
    stop = asyncio.Event()
    install_shutdown(stop)

    connector_cfg = aiohttp.TCPConnector(limit=config.http_pool_limit)
    timeout = aiohttp.ClientTimeout(total=config.request_timeout_s)
    async with aiohttp.ClientSession(connector=connector_cfg, timeout=timeout) as session:
        connector = HttpEgressConnector(config, session)
        start_metrics_server(config.metrics_port)
        _log.info(
            "egress_started",
            connector="http",
            endpoint=config.endpoint_url,
            workers=config.workers,
            metrics_port=config.metrics_port,
        )
        try:
            await consume_loop(consumer, connector.handle, config.workers, stop)
        finally:
            _log.info("egress_stopping")
            await backend.close()


async def run_smpp(config: SmppEgressConfig) -> None:
    backend = create_backend(config.queue)
    await backend.start()
    consumer = await backend.consumer(config.egress_queue)
    stop = asyncio.Event()
    install_shutdown(stop)

    connector = SmppEgressConnector(config)
    connector.start()
    start_metrics_server(config.metrics_port)
    _log.info(
        "egress_started",
        connector="smpp",
        provider=f"{config.host}:{config.port}",
        binds=config.bind_count,
        workers=config.workers,
        metrics_port=config.metrics_port,
    )
    try:
        await consume_loop(consumer, connector.handle, config.workers, stop)
    finally:
        _log.info("egress_stopping")
        await connector.stop()
        await backend.close()


def _install_uvloop() -> None:
    try:
        import uvloop  # Linux/macOS only; faster event loop for network I/O.

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="relay egress")
    parser.add_argument("--config", default="config/egress.yaml")
    args = parser.parse_args()
    raw: dict[str, Any] = load_config(args.config)
    connector_type = str(raw.get("service", {}).get("connector", "http"))
    log_level = str(raw.get("service", {}).get("log_level", "INFO"))
    configure_logging(log_level)
    _install_uvloop()
    if connector_type == "smpp":
        asyncio.run(run_smpp(SmppEgressConfig.from_dict(raw)))
    else:
        asyncio.run(run_http(HttpEgressConfig.from_dict(raw)))


if __name__ == "__main__":
    main()
