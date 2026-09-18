"""Run crawl inputs with local recovery state and optional S3 delivery for NATS."""

import argparse
import asyncio
import logging
import os
import signal
from pathlib import Path

import uvicorn
from botocore.exceptions import BotoCoreError
from dotenv import dotenv_values
from pydantic import ValidationError

from company_research.service import CrawlService
from company_research.service_api import create_app
from company_research.service_nats import JetStreamInput, JetStreamSettings
from company_research.service_results import S3Results, S3Settings


async def run_nats(service: CrawlService, jetstream: JetStreamInput) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await service.start()
    try:
        await jetstream.start()
        stop_task = asyncio.create_task(stop.wait())
        try:
            assert jetstream.task is not None
            done, _ = await asyncio.wait(
                [stop_task, jetstream.task, *service.workers],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                await task
        finally:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
    finally:
        await jetstream.close()
        await service.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=["rest", "nats", "both"], default="rest")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--max-pending", type=int, default=100)
    parser.add_argument("--nats-url", action="append")
    parser.add_argument("--nats-stream")
    parser.add_argument("--nats-subject")
    parser.add_argument("--nats-durable")
    parser.add_argument("--nats-ack-wait", type=float)
    parser.add_argument("--nats-credentials", type=Path)
    parser.add_argument("--nats-result-stream")
    parser.add_argument("--nats-result-subject")
    parser.add_argument(
        "--nats-result-max-age",
        type=float,
        help="New result stream retention in seconds (default: 604800)",
    )
    parser.add_argument(
        "--s3-bucket", help="Enable S3 delivery and completion events for NATS requests"
    )
    parser.add_argument("--s3-prefix")
    parser.add_argument("--s3-endpoint-url")
    parser.add_argument("--s3-region")
    parser.add_argument("--create-stream", action="store_true")
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
    if (
        args.transport != "nats"
        and args.host not in {"127.0.0.1", "localhost", "::1"}
        and api_token is None
    ):
        parser.error("Set CRAWL_API_TOKEN before binding REST beyond localhost")
    try:
        service = CrawlService(
            args.output_dir,
            environment,
            concurrency=args.concurrency,
            max_pending=args.max_pending,
        )
        jetstream = None
        if args.transport in {"nats", "both"}:
            values = {
                "servers": args.nats_url
                or ([environment["NATS_URL"]] if environment.get("NATS_URL") else None),
                "stream": args.nats_stream,
                "subject": args.nats_subject,
                "durable": args.nats_durable,
                "ack_wait": args.nats_ack_wait,
                "credentials": args.nats_credentials,
                "create_stream": args.create_stream,
                "result_stream": args.nats_result_stream,
                "result_subject": args.nats_result_subject,
                "result_max_age": args.nats_result_max_age,
            }
            settings = JetStreamSettings.model_validate(
                {k: v for k, v in values.items() if v is not None}
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
            jetstream = JetStreamInput(service, settings, results)
        elif args.s3_bucket:
            parser.error("S3 delivery requires --transport nats or both")
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
    if args.transport == "nats":
        assert jetstream is not None
        asyncio.run(run_nats(service, jetstream))
    else:
        uvicorn.run(
            create_app(service, api_token=api_token, jetstream=jetstream),
            host=args.host,
            port=args.port,
        )


if __name__ == "__main__":
    main()
