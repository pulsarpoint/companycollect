import asyncio
import logging
import os
import signal
import socket
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import boto3
import click
import clickhouse_connect
from botocore.config import Config as BotoConfig
from cloakbrowser import launch_persistent_context_async
from playwright.async_api import BrowserContext
from temporalio.client import Client
from temporalio.worker import Worker

from crawler_ratsit.activities import (
    RatsitCrawlActivities,
    RatsitResultActivities,
)
from crawler_ratsit.config import BrowserSettings, ProcessSettings, WorkerSettings
from crawler_ratsit.constants import HTTP_TASK_QUEUE
from crawler_ratsit.crawler import crawl_ratsit_page
from crawler_ratsit.object_store import RatsitObjectStore
from crawler_ratsit.result_store import RatsitResultStore
from crawler_ratsit.workflows import RatsitCompanyWorkflow

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BrowserRuntime:
    settings: BrowserSettings
    context: BrowserContext


async def run_process(
    worker_settings: WorkerSettings,
    process_settings: ProcessSettings,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Run browser, workflow, crawl, S3, and result workers in one process."""
    process_settings.state_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporal_client = await Client.connect(
        worker_settings.temporal.temporal_address,
        namespace=worker_settings.temporal.temporal_namespace,
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=worker_settings.s3_endpoint,
        aws_access_key_id=worker_settings.s3_access_key,
        aws_secret_access_key=worker_settings.s3_secret_key,
        region_name=worker_settings.s3_region,
        config=BotoConfig(s3={"addressing_style": "path"}),
    )
    clickhouse_client = clickhouse_connect.get_client(
        host=worker_settings.clickhouse_host,
        port=worker_settings.clickhouse_http_port,
        username=worker_settings.clickhouse_user,
        password=worker_settings.clickhouse_password,
        database=worker_settings.clickhouse_database,
        secure=worker_settings.clickhouse_secure,
    )
    browsers: list[BrowserRuntime] = []
    registered_signals: list[signal.Signals] = []
    disconnected_browsers: set[str] = set()

    def request_shutdown() -> None:
        stop_event.set()

    def browser_disconnected(browser_id: str) -> None:
        if not stop_event.is_set():
            LOGGER.error("CloakBrowser disconnected browser_id=%s", browser_id)
            disconnected_browsers.add(browser_id)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, request_shutdown)
        registered_signals.append(shutdown_signal)

    try:
        for browser_settings in process_settings.browsers:
            if stop_event.is_set():
                break
            browser_runtime = await _launch_browser(
                browser_settings,
                process_settings=process_settings,
                license_key=worker_settings.cloakbrowser_license_key,
                disconnected_callback=browser_disconnected,
            )
            browsers.append(browser_runtime)

        object_store = RatsitObjectStore(
            s3_client,
            bucket=worker_settings.s3_bucket,
            prefix=worker_settings.s3_prefix,
        )
        result_activities = RatsitResultActivities(
            result_store=RatsitResultStore(
                clickhouse_client,
                database=worker_settings.clickhouse_database,
            )
        )
        identity_prefix = f"ratsit-process/{socket.gethostname()}/{os.getpid()}"
        workflow_worker = Worker(
            temporal_client,
            task_queue=worker_settings.temporal.temporal_task_queue,
            workflows=[RatsitCompanyWorkflow],
            activities=[result_activities.record_crawl_result],
            max_concurrent_activities=worker_settings.max_concurrent_activities,
            identity=f"{identity_prefix}/workflow-results",
        )
        http_workers = [
            _http_worker(
                temporal_client,
                browser_runtime=browser_runtime,
                worker_settings=worker_settings,
                process_settings=process_settings,
                object_store=object_store,
                identity_prefix=identity_prefix,
            )
            for browser_runtime in browsers
        ]

        LOGGER.info(
            "starting Ratsit process browsers=%d workflow_task_queue=%s "
            "http_task_queue=%s per_browser_rate=%g global_rate=%g",
            len(browsers),
            worker_settings.temporal.temporal_task_queue,
            HTTP_TASK_QUEUE,
            process_settings.per_browser_activities_per_second,
            process_settings.task_queue_activities_per_second,
        )
        async with AsyncExitStack() as worker_stack:
            await worker_stack.enter_async_context(workflow_worker)
            for http_worker in http_workers:
                await worker_stack.enter_async_context(http_worker)
            await stop_event.wait()

        if disconnected_browsers:
            browser_ids = ", ".join(sorted(disconnected_browsers))
            raise RuntimeError(f"CloakBrowser disconnected: {browser_ids}")
    finally:
        stop_event.set()
        for shutdown_signal in registered_signals:
            loop.remove_signal_handler(shutdown_signal)
        await _close_browsers(browsers)
        await asyncio.to_thread(clickhouse_client.close)
        s3_client.close()


async def _launch_browser(
    browser_settings: BrowserSettings,
    *,
    process_settings: ProcessSettings,
    license_key: str | None,
    disconnected_callback: Callable[[str], None],
) -> BrowserRuntime:
    profile_directory = (
        process_settings.state_directory / browser_settings.browser_id
    )
    profile_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    LOGGER.info(
        "launching CloakBrowser browser_id=%s proxy=%s profile=%s mode=%s",
        browser_settings.browser_id,
        "configured" if browser_settings.proxy_url is not None else "direct",
        profile_directory,
        "headless" if process_settings.headless else "headed",
    )
    context = await launch_persistent_context_async(
        profile_directory,
        license_key=license_key,
        headless=process_settings.headless,
        proxy=browser_settings.proxy_url,
        geoip=True,
    )
    browser = context.browser
    if browser is None:
        await context.close()
        raise RuntimeError(
            f"persistent browser context {browser_settings.browser_id} has no browser"
        )
    browser.on(
        "disconnected",
        lambda _browser: disconnected_callback(browser_settings.browser_id),
    )
    return BrowserRuntime(settings=browser_settings, context=context)


def _http_worker(
    temporal_client: Client,
    *,
    browser_runtime: BrowserRuntime,
    worker_settings: WorkerSettings,
    process_settings: ProcessSettings,
    object_store: RatsitObjectStore,
    identity_prefix: str,
) -> Worker:
    browser_id = browser_runtime.settings.browser_id
    crawl_activities = RatsitCrawlActivities(
        browser_id=browser_id,
        crawl_page=partial(
            crawl_ratsit_page,
            context=browser_runtime.context,
            content_selector=worker_settings.content_selector,
            timeout_ms=worker_settings.page_timeout_ms,
        ),
        object_store=object_store,
    )
    return Worker(
        temporal_client,
        task_queue=HTTP_TASK_QUEUE,
        activities=[crawl_activities.crawl_and_upload_company],
        max_concurrent_activities=1,
        max_activities_per_second=(
            process_settings.per_browser_activities_per_second
        ),
        max_task_queue_activities_per_second=(
            process_settings.task_queue_activities_per_second
        ),
        identity=f"{identity_prefix}/browser/{browser_id}",
    )


async def _close_browsers(browsers: list[BrowserRuntime]) -> None:
    close_results = await asyncio.gather(
        *(browser.context.close() for browser in reversed(browsers)),
        return_exceptions=True,
    )
    for browser, close_result in zip(reversed(browsers), close_results, strict=True):
        if isinstance(close_result, BaseException):
            LOGGER.error(
                "failed to close CloakBrowser browser_id=%s error=%s",
                browser.settings.browser_id,
                close_result,
            )


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(
        path_type=Path,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    envvar="RATSIT_PROCESS_CONFIG",
    required=True,
    help="Protected TOML file describing browser contexts and crawl rates.",
)
def main(config_path: Path) -> None:
    """Run the complete Ratsit crawler process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        worker_settings = WorkerSettings.from_environment(os.environ)
        process_settings = ProcessSettings.from_file(config_path)
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    asyncio.run(
        run_process(
            worker_settings,
            process_settings,
            stop_event=asyncio.Event(),
        )
    )


if __name__ == "__main__":
    main()
