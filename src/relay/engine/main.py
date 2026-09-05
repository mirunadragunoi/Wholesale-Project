"""Engine process: consume ``ingress`` → run pipeline → publish ``egress``.

Pass-through for the POC. Communicates only through queues; knows nothing about
HTTP, SMPP or CSV.
"""

from __future__ import annotations

import argparse
import asyncio

from relay.common.config import EngineConfig, load_config
from relay.common.logging import configure_logging, get_logger
from relay.common.message import Message
from relay.common.metrics import engine_processed_total, start_metrics_server
from relay.common.worker import consume_loop, install_shutdown
from relay.engine.pipeline import default_pipeline
from relay.queues.base import Producer, ReceivedMessage
from relay.queues.factory import create_backend

_log = get_logger("engine")


async def run(config: EngineConfig) -> None:
    pipeline = default_pipeline()
    backend = create_backend(config.queue)
    await backend.start()
    consumer = await backend.consumer(config.in_queue)
    producer: Producer = await backend.producer(config.out_queue)
    stop = asyncio.Event()
    install_shutdown(stop)

    async def handle(batch: list[ReceivedMessage]) -> list[str]:
        forward: list[Message] = []
        for item in batch:
            result = await pipeline.process(item.message)
            if result is not None:
                forward.append(result)
        if forward:
            await producer.publish(forward)
        engine_processed_total.inc(len(batch))
        # Ack the whole batch: dropped messages are intentionally not redelivered.
        return [item.handle for item in batch]

    start_metrics_server(config.metrics_port)
    _log.info(
        "engine_started",
        in_queue=config.in_queue,
        out_queue=config.out_queue,
        workers=config.workers,
        metrics_port=config.metrics_port,
    )
    try:
        await consume_loop(consumer, handle, config.workers, stop)
    finally:
        _log.info("engine_stopping")
        await backend.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="relay engine")
    parser.add_argument("--config", default="config/engine.yaml")
    args = parser.parse_args()
    config = EngineConfig.from_dict(load_config(args.config))
    configure_logging(config.log_level)
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
