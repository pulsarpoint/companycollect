"""Resumable Brave answer collection for active Swedish companies."""

from collections import Counter
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.company_domains.browser import (
    PROMPT_VERSION,
    ROUTES,
    BraveBrowserResource,
    BraveSearchResult,
    CompanySearchInput,
)

RESULT_TABLE = "company_brave_search_results"
ANSWER_BUCKET = "company-domains-brave"
RESULT_COLUMNS = (
    "country_code",
    "company_id",
    "company_name",
    "query",
    "prompt_version",
    "status",
    "connection_mode",
    "proxy_name",
    "source_url",
    "fetched_at",
    "answer_bucket",
    "answer_object_key",
    "answer_bytes",
    "error_type",
    "source_run_id",
)

# Keyset pagination keeps both selection memory and outstanding browser work bounded.
# Match the name and prompt version as well as ID so renamed companies are refreshed.
PENDING_COMPANIES_SQL = """
SELECT c.company_id, trimBoth(ifNull(c.legal_name, '')) AS company_name
FROM corpscout.se_company_basic_info AS c FINAL
LEFT ANTI JOIN (
    SELECT company_id, company_name
    FROM corpscout.company_brave_search_results
    WHERE country_code = 'SE' AND status = 'success'
      AND prompt_version = %(prompt_version)s AND fetched_at >= %(freshness_cutoff)s
) AS done ON c.company_id = done.company_id
         AND trimBoth(ifNull(c.legal_name, '')) = done.company_name
WHERE c.status = 'active' AND trimBoth(ifNull(c.legal_name, '')) != ''
  AND c.company_id > %(after_company_id)s
  AND (%(all_companies)s OR c.company_id IN %(company_ids)s)
ORDER BY c.company_id
LIMIT %(page_size)s
"""


class BraveSearchConfig(dg.Config):
    requests_per_route: int = Field(default=1, ge=1, le=8)
    max_companies: int | None = Field(default=None, ge=1)
    company_ids: list[str] = Field(default_factory=list)
    freshness_days: int = Field(default=30, ge=0)
    page_size: int = Field(default=1_000, ge=1, le=5_000)


def iter_pending_companies(
    clickhouse: ClickhouseResource,
    config: BraveSearchConfig,
    *,
    started_at: datetime,
) -> Iterator[CompanySearchInput]:
    params = {
        "prompt_version": PROMPT_VERSION,
        "freshness_cutoff": started_at - timedelta(days=config.freshness_days),
        "after_company_id": "",
        "all_companies": not config.company_ids,
        "company_ids": tuple(config.company_ids) or ("",),
    }
    selected = 0
    # This reader is advanced under the browser resource's input lock. Persistence
    # uses its own connection in the asset's main thread, never this connection.
    with clickhouse.get_connection() as client:
        while config.max_companies is None or selected < config.max_companies:
            params["page_size"] = min(
                config.page_size,
                config.max_companies - selected
                if config.max_companies is not None
                else config.page_size,
            )
            rows = client.execute(PENDING_COMPANIES_SQL, params)
            if not rows:
                return
            for company_id, name in rows:
                selected += 1
                params["after_company_id"] = company_id
                yield CompanySearchInput(company_id, name)


def persist_result(
    client: Any,
    object_store: ObjectStoreResource,
    result: BraveSearchResult,
    *,
    run_id: str,
) -> None:
    key = ""
    answer_bytes = 0
    if result.status == "success":
        if not result.answer.strip():
            raise ValueError("cannot persist an empty successful Brave answer")
        key = f"SE/{quote(result.company.company_id, safe='')}/{quote(run_id, safe='')}/answer.txt"
        body = result.answer.encode("utf-8")
        object_store.write_bytes(key, body)
        answer_bytes = len(body)
    # The completion marker follows the object write. A storage failure cannot make
    # a company disappear from the next run's pending selection.
    row = (
        "SE",
        result.company.company_id,
        result.company.company_name,
        result.query,
        PROMPT_VERSION,
        result.status,
        "direct" if result.route == "direct" else "proxy",
        "" if result.route == "direct" else result.route,
        result.source_url,
        result.fetched_at,
        object_store.bucket if key else "",
        key,
        answer_bytes,
        result.error_type,
        run_id,
    )
    client.execute(
        f"INSERT INTO corpscout.{RESULT_TABLE} ({', '.join(RESULT_COLUMNS)}) VALUES",
        [row],
    )


@dg.asset(
    group_name="company_domains",
    kinds={"python", "browser", "s3", "clickhouse"},
    pool="company_domains_brave",
    tags={"country": "sweden", "source": "brave"},
    description=(
        "Collect Brave's copied answers for active Swedish companies using one direct route "
        "and crawl_proxy1/2/3. Each free slot immediately takes the next company, with one "
        "request per route by default (four total). Drain eligible companies or stop at "
        "max_companies. Save each answer to S3 and index every outcome in ClickHouse; "
        "skip successful same-name/same-prompt answers from the last 30 days."
    ),
)
def company_brave_search_results(
    context: dg.AssetExecutionContext,
    config: BraveSearchConfig,
    clickhouse: ClickhouseResource,
    company_brave_browser: BraveBrowserResource,
    company_brave_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database="corpscout",
        tables=("se_company_basic_info", RESULT_TABLE),
    )
    company_brave_object_store.ensure_bucket()
    counts: Counter[str] = Counter()
    route_counts: Counter[str] = Counter()
    started_at = datetime.now(UTC)
    context.log.info(
        "Starting Brave collection with %s slots (%s per route)",
        len(ROUTES) * config.requests_per_route,
        config.requests_per_route,
    )
    with closing(
        iter_pending_companies(clickhouse, config, started_at=started_at)
    ) as companies:
        with closing(
            company_brave_browser.iter_answers(
                companies, requests_per_route=config.requests_per_route
            )
        ) as results:
            with clickhouse.get_connection() as client:
                for result in results:
                    persist_result(
                        client,
                        company_brave_object_store,
                        result,
                        run_id=context.run_id,
                    )
                    counts[result.status] += 1
                    route_counts[result.route] += 1
                    context.log.info(
                        "Brave company=%s route=%s status=%s completed=%s error_type=%s",
                        result.company.company_id,
                        result.route,
                        result.status,
                        counts.total(),
                        result.error_type,
                    )
    metadata = {
        "selected_companies": counts.total(),
        "successful_companies": counts["success"],
        "failed_companies": counts["error"],
        "request_slots": len(ROUTES) * config.requests_per_route,
        "requests_per_route": config.requests_per_route,
        "route_counts": dg.MetadataValue.json(dict(route_counts)),
        "table": f"corpscout.{RESULT_TABLE}",
        "answer_bucket": company_brave_object_store.bucket,
    }
    if counts["error"]:
        raise dg.Failure(
            "Some Brave searches failed; successful answers are saved. Rerun to retry.",
            metadata=metadata,
        )
    return dg.MaterializeResult(metadata=metadata)


company_brave_search_job = dg.define_asset_job(
    name="company_brave_search_job",
    selection=dg.AssetSelection.assets(company_brave_search_results),
)

defs = dg.Definitions(
    assets=[company_brave_search_results],
    jobs=[company_brave_search_job],
    resources={
        "company_brave_browser": BraveBrowserResource(
            crawl_proxy1=dg.EnvVar("crawl_proxy1"),
            crawl_proxy2=dg.EnvVar("crawl_proxy2"),
            crawl_proxy3=dg.EnvVar("crawl_proxy3"),
        ),
        "company_brave_object_store": ObjectStoreResource(bucket=ANSWER_BUCKET),
    },
)
