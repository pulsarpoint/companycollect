from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any
from urllib.parse import parse_qs, urlparse

import dagster as dg
from cloakbrowser import launch
from lxml import html as lxml_html

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_update_response_partition_prefix,
)
from dagster_v3.defs.norway_brreg_financial.response_pipeline import (
    materialize_response_json_partition,
    verified_response_index_frame,
)

FINANCIAL_FETCHED_AT_DTYPE = financial_fetches.FINANCIAL_FETCHED_AT_DTYPE
FINANCIAL_FETCHES_PARQUET_SCHEMA = financial_fetches.financial_fetches_parquet_schema()
NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
)
BRREG_ANNOUNCEMENT_SEARCH_URL = "https://w2.brreg.no/kunngjoring/kombisok.jsp"


def daily_financial_report_candidates(
    partition_date: str,
    *,
    launcher: Callable[[], Any] = launch,
) -> list[dict[str, str]]:
    """Find companies with approved annual-account announcements on one day."""
    # Convert the Dagster partition key to the date format used by BRREG search.
    search_date = date.fromisoformat(partition_date).strftime("%d.%m.%Y")
    search_url = (
        f"{BRREG_ANNOUNCEMENT_SEARCH_URL}?datoFra={search_date}"
        f"&datoTil={search_date}&id_region=0&id_niva1=70"
        "&id_niva2=-+-+-&id_bransje1=0"
    )

    # Open exactly one search result page for the partition date.
    browser = launcher()
    try:
        page = browser.new_page()
        page.goto(search_url, wait_until="networkidle")
        page_html = page.evaluate("() => document.documentElement.outerHTML")
    finally:
        browser.close()

    # Turn announcement links into the organization numbers used by the downloader.
    return parse_daily_financial_report_candidates(page_html)


def parse_daily_financial_report_candidates(
    page_html: str,
) -> list[dict[str, str]]:
    """Parse and deduplicate organization numbers from BRREG result HTML."""
    document = lxml_html.fromstring(page_html)
    announcement_links = document.xpath(
        "//a[contains(@href, 'hent_en.jsp?kid=')]/@href"
    )
    org_numbers: set[str] = set()

    for link in announcement_links:
        query = parse_qs(urlparse(str(link)).query)
        org_number = query.get("sokeverdi", [""])[0]
        if len(org_number) != 9 or not org_number.isdigit():
            raise RuntimeError(
                f"Invalid organization number in BRREG announcement link: {link}"
            )
        org_numbers.add(org_number)

    return [{"org_number": org_number} for org_number in sorted(org_numbers)]


@dg.asset(
    name="norway_brreg_financial_responses_updates_json",
    group_name=GROUP_NAME,
    kinds={"python", "browser", "s3", "json", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="norway_brreg_financial_api",
    description=(
        "Searches approved annual-account announcements for one day, then downloads "
        "the announced companies' Norway BRREG responses as immutable JSON."
    ),
)
def norway_brreg_financial_responses_updates_json(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    # Search only the date represented by this Dagster partition.
    candidates = daily_financial_report_candidates(partition_date)
    # Download and checkpoint one BRREG response for every announced company.
    metadata = materialize_response_json_partition(
        candidates=candidates,
        partition_prefix=financial_update_response_partition_prefix(partition_date),
        source_run_id=context.op_execution_context.run_id,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            **metadata,
        }
    )


@dg.asset(
    name="norway_brreg_financial_responses_updates_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_responses_updates_json")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Builds a metadata-only Parquet index for one verified update JSON "
        "response partition."
    ),
)
def norway_brreg_financial_responses_updates_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    partition_prefix = financial_update_response_partition_prefix(partition_date)
    frame, metadata = verified_response_index_frame(
        partition_prefix=partition_prefix,
        storage=norway_brreg_financial_storage,
    )
    output_key = norway_brreg_financial_storage.write_update_response_index(
        partition_date,
        frame,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
            **metadata,
        }
    )
