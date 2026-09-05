"""Ingress process entry point. Selects the HTTP or SMPP connector by config."""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

import uvicorn

from relay.common.config import (
    CsvIngressConfig,
    HttpIngressConfig,
    SmppIngressConfig,
    load_config,
)
from relay.common.logging import configure_logging, get_logger
from relay.common.worker import install_shutdown
from relay.ingress.csv_connector import CsvIngress
from relay.ingress.http_connector import create_app
from relay.ingress.smpp_connector import SmppIngress

_log = get_logger("ingress")


def _install_uvloop() -> None:
    try:
        import uvloop  # Linux/macOS only; faster event loop for network I/O.

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass


def run_http(raw: dict[str, Any]) -> None:
    config = HttpIngressConfig.from_dict(raw)
    app = create_app(config)
    # log_config=None: keep our JSON logging, don't let uvicorn install its own.
    uvicorn.run(app, host=config.http_host, port=config.http_port, log_config=None)


async def run_smpp(config: SmppIngressConfig) -> None:
    ingress = SmppIngress(config)
    stop = asyncio.Event()
    install_shutdown(stop)
    await ingress.start()
    try:
        await stop.wait()
    finally:
        await ingress.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="relay ingress")
    parser.add_argument("--config", default="config/ingress.yaml")
    args = parser.parse_args()
    raw: dict[str, Any] = load_config(args.config)
    connector_type = str(raw.get("service", {}).get("connector", "http"))
    log_level = str(raw.get("service", {}).get("log_level", "INFO"))
    configure_logging(log_level)
    _install_uvloop()
    if connector_type == "smpp":
        asyncio.run(run_smpp(SmppIngressConfig.from_dict(raw)))
    elif connector_type == "csv":
        stats = asyncio.run(CsvIngress(CsvIngressConfig.from_dict(raw)).run())
        _log.info("csv_ingest_complete", total=stats.total, sent=stats.sent, skipped=stats.skipped)
    else:
        run_http(raw)


if __name__ == "__main__":
    main()
