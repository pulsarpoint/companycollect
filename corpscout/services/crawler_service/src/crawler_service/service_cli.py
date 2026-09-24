"""Run the REST crawl service with local recovery state and optional S3 delivery."""

import argparse
import logging
import os
from pathlib import Path

import uvicorn
from botocore.exceptions import BotoCoreError
from dotenv import dotenv_values
from pydantic import ValidationError

from crawler_service.service import CrawlService
from crawler_service.service_api import create_app
from crawler_service.service_results import S3Results, S3Settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--max-pending", type=int, default=100)
    parser.add_argument(
        "--s3-bucket", help="Enable S3 delivery of results and failed attempts"
    )
    parser.add_argument("--s3-prefix")
    parser.add_argument("--s3-endpoint-url")
    parser.add_argument("--s3-region")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    environment = (
        {k: v for k, v in dotenv_values(args.env_file).items() if v is not None}
        if args.env_file is not None
        else {}
    ) | dict(os.environ)
    api_token = environment.get("CRAWL_API_TOKEN") or None
    if args.host not in {"127.0.0.1", "localhost", "::1"} and api_token is None:
        parser.error("Set CRAWL_API_TOKEN before binding REST beyond localhost")
    try:
        service = CrawlService(
            args.output_dir,
            environment,
            concurrency=args.concurrency,
            max_pending=args.max_pending,
        )
        results = None
        bucket = args.s3_bucket or environment.get("CRAWL_S3_BUCKET")
        if bucket:
            storage_values = {
                "bucket": bucket,
                "prefix": args.s3_prefix or environment.get("CRAWL_S3_PREFIX"),
                "endpoint_url": args.s3_endpoint_url
                or environment.get("CRAWL_S3_ENDPOINT_URL")
                or None,
                "region": args.s3_region
                or environment.get("AWS_REGION")
                or environment.get("AWS_DEFAULT_REGION"),
            }
            storage_settings = S3Settings.model_validate(
                {k: v for k, v in storage_values.items() if v is not None}
            )
            results = S3Results(storage_settings, environment)
        service.results = results
        if service.human_enabled and results is None:
            parser.error("Human assistance requires S3 storage for failed attempts")
    except ValidationError as error:
        parser.error(
            str(
                error.errors(
                    include_input=False, include_context=False, include_url=False
                )
            )
        )
    except BotoCoreError as error:
        parser.error(f"Cannot configure S3 ({type(error).__name__})")
    except ValueError as error:
        parser.error(str(error))
    uvicorn.run(
        create_app(service, api_token=api_token),
        host=args.host,
        port=args.port,
        access_log=False,
        # Long-lived SSE/VNC connections must not prevent profile shutdown.
        timeout_graceful_shutdown=10,
    )


if __name__ == "__main__":
    main()
