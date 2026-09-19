"""Independent browser service process."""

import argparse
import logging
import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from browser_service.api import create_app
from browser_service.runtime import BrowserService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    if args.env_file is not None:
        load_dotenv(args.env_file)
    token = os.environ.get("BROWSER_API_TOKEN")
    host = os.environ.get("BROWSER_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"} and not token:
        parser.error("BROWSER_API_TOKEN is required when listening beyond localhost")
    logging.basicConfig(level=logging.INFO)
    service = BrowserService(
        Path(os.environ.get("BROWSER_STATE_DIR", "./browser-state")),
        count=int(os.environ.get("BROWSER_COUNT", "2")),
        max_pending=int(os.environ.get("BROWSER_MAX_PENDING", "100")),
        idle_timeout=float(os.environ.get("BROWSER_IDLE_TIMEOUT_SECONDS", "120")),
    )
    if service.multiplexer.idle_timeout <= 0:
        parser.error("BROWSER_IDLE_TIMEOUT_SECONDS must be positive")
    uvicorn.run(
        create_app(service, api_token=token),
        host=host,
        port=int(os.environ.get("BROWSER_PORT", "8081")),
        timeout_graceful_shutdown=330,
        access_log=False,
    )


if __name__ == "__main__":
    main()
