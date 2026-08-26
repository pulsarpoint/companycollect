import asyncio
import logging
import os
from functools import partial

import boto3
import click
import clickhouse_connect
from botocore.config import Config as BotoConfig
from temporalio.client import Client
from temporalio.worker import Worker

from crawler_ratsit.activities import RatsitActivities
from crawler_ratsit.config import WorkerSettings
from crawler_ratsit.crawler import crawl_ratsit_page
from crawler_ratsit.object_store import RatsitObjectStore
from crawler_ratsit.result_store import RatsitResultStore
from crawler_ratsit.workflows import RatsitCompanyWorkflow


LOGGER = logging.getLogger(__name__)


async def run_worker(settings: WorkerSettings) -> None:
    temporal_client = await Client.connect(
        settings.temporal.temporal_address,
        namespace=settings.temporal.temporal_namespace,
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=BotoConfig(s3={"addressing_style": "path"}),
    )
    clickhouse_client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_http_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
    )
    activities = RatsitActivities(
        crawl_page=partial(
            crawl_ratsit_page,
            cdp_url=settings.cdp_url,
            content_selector=settings.content_selector,
            timeout_ms=settings.page_timeout_ms,
        ),
        object_store=RatsitObjectStore(
            s3_client,
            bucket=settings.s3_bucket,
            prefix=settings.s3_prefix,
        ),
        result_store=RatsitResultStore(
            clickhouse_client,
            database=settings.clickhouse_database,
        ),
    )

    LOGGER.info(
        "starting Ratsit Temporal worker task_queue=%s concurrency=%d",
        settings.temporal.temporal_task_queue,
        settings.max_concurrent_activities,
    )
    try:
        await Worker(
            temporal_client,
            task_queue=settings.temporal.temporal_task_queue,
            workflows=[RatsitCompanyWorkflow],
            activities=[
                activities.crawl_and_upload_company,
                activities.record_crawl_result,
            ],
            max_concurrent_activities=settings.max_concurrent_activities,
        ).run()
    finally:
        await asyncio.to_thread(clickhouse_client.close)
        s3_client.close()


@click.command()
def main() -> None:
    """Run the Ratsit Temporal worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = WorkerSettings.from_environment(os.environ)
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    asyncio.run(run_worker(settings))


if __name__ == "__main__":
    main()
