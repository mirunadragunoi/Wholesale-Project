"""Egress process entry point (HTTP connector for M1)."""

from __future__ import annotations

import argparse
import asyncio

import aiohttp

from relay.common.config import HttpEgressConfig, load_config
from relay.common.logging import configure_logging, get_logger
from relay.common.metrics import start_metrics_server
from relay.common.worker import consume_loop, install_shutdown
from relay.egress.http_connector import HttpEgressConnector
from relay.queues.factory import create_backend

_log = get_logger("egress")


async def run(config: HttpEgressConfig) -> None:
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
            egress_queue=config.egress_queue,
            endpoint=config.endpoint_url,
            workers=config.workers,
            metrics_port=config.metrics_port,
        )
        try:
            await consume_loop(consumer, connector.handle, config.workers, stop)
        finally:
            _log.info("egress_stopping")
            await backend.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="relay egress (HTTP)")
    parser.add_argument("--config", default="config/egress.yaml")
    args = parser.parse_args()
    config = HttpEgressConfig.from_dict(load_config(args.config))
    configure_logging(config.log_level)
    try:
        import uvloop  # Linux/macOS only; faster event loop for network I/O.

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
