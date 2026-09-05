"""Ingress process entry point (HTTP connector for M1)."""

from __future__ import annotations

import argparse

import uvicorn

from relay.common.config import HttpIngressConfig, load_config
from relay.common.logging import configure_logging
from relay.ingress.http_connector import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="relay ingress (HTTP)")
    parser.add_argument("--config", default="config/ingress.yaml")
    args = parser.parse_args()
    config = HttpIngressConfig.from_dict(load_config(args.config))
    configure_logging(config.log_level)
    app = create_app(config)
    # log_config=None: keep our JSON logging, don't let uvicorn install its own.
    uvicorn.run(app, host=config.http_host, port=config.http_port, log_config=None)


if __name__ == "__main__":
    main()
