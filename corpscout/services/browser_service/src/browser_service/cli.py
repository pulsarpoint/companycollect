"""Independent browser service process."""

import argparse
import logging
import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from pydantic import ValidationError

from browser_service.api import create_app
from browser_service.runtime import BrowserRuntimeSettings, BrowserService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--idle-timeout-seconds",
        type=float,
        help="Initial idle session timeout; saved SQLite settings take precedence.",
    )
    parser.add_argument(
        "--max-browsers",
        type=int,
        help="Maximum simultaneous browser executions; SQLite overrides startup settings.",
    )
    parser.add_argument(
        "--session-retention-days",
        type=float,
        help="Retain closed session profiles for this many days.",
    )
    parser.add_argument(
        "--base-profile",
        type=Path,
        help="Closed, read-only Chromium profile copied only for new sessions.",
    )
    args = parser.parse_args()
    if args.env_file is not None:
        load_dotenv(args.env_file)
    token = os.environ.get("BROWSER_API_TOKEN")
    host = os.environ.get("BROWSER_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"} and not token:
        parser.error("BROWSER_API_TOKEN is required when listening beyond localhost")
    logging.basicConfig(level=logging.INFO)
    try:
        settings = BrowserRuntimeSettings(
            max_browsers=args.max_browsers
            if args.max_browsers is not None
            else int(os.environ.get("BROWSER_MAX_BROWSERS", "6")),
            idle_timeout_seconds=args.idle_timeout_seconds
            if args.idle_timeout_seconds is not None
            else float(os.environ.get("BROWSER_IDLE_TIMEOUT_SECONDS", "120")),
            session_retention_days=args.session_retention_days
            if args.session_retention_days is not None
            else float(os.environ.get("BROWSER_SESSION_RETENTION_DAYS", "7")),
        )
        service = BrowserService(
            Path(os.environ.get("BROWSER_STATE_DIR", "./browser-state")),
            settings=settings,
            base_profile=args.base_profile
            or (
                Path(os.environ["BROWSER_BASE_PROFILE"])
                if os.environ.get("BROWSER_BASE_PROFILE")
                else None
            ),
            proxy_routes={
                f"crawl_proxy{i}": value
                for i in range(1, 4)
                if (value := os.environ.get(f"BROWSER_CRAWL_PROXY{i}"))
            },
        )
    except ValidationError as error:
        parser.error(
            str(
                error.errors(
                    include_input=False, include_context=False, include_url=False
                )
            )
        )
    except ValueError as error:
        parser.error(str(error))
    uvicorn.run(
        create_app(
            service,
            api_token=token,
            deepseek_api_key=os.environ.get("DEEPSEEK"),
            openrouter_api_key=os.environ.get("OPENROUTER_API_KEY"),
            llm_encryption_key=os.environ.get("BROWSER_LLM_ENCRYPTION_KEY"),
        ),
        host=host,
        port=int(os.environ.get("BROWSER_PORT", "8081")),
        timeout_graceful_shutdown=330,
        access_log=False,
    )


if __name__ == "__main__":
    main()
