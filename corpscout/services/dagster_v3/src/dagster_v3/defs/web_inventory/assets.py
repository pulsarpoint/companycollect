"""Build website/page snapshots without changing processing queues or result history."""

import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

import dagster as dg
from clickhouse_driver import Client
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.webtech.pages import page_identity

# Only source tables explicitly selected for this integration. Source URLs, not
# JSON-LD entity_url/canonical links, identify the page that was actually processed.
COMMONCRAWL_URLS = (
    ("commoncrawl_domains", "url"),
    ("commoncrawl_page_technologies", "page_url"),
    ("commoncrawl_page_jsonld", "page_url"),
    ("commoncrawl_domain_page_meta", "source_url"),
)
PAGE_COLUMNS = (
    "root_domain", "website_origin", "page_url", "sources", "first_seen_at",
    "last_seen_at", "last_observed_at", "last_successful_fetch_at", "source_run_id",
)
WEBSITE_COLUMNS = tuple(column for column in PAGE_COLUMNS if column != "page_url")
ROOT_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:[.][a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$")


class WebInventoryConfig(dg.Config):
    sources: list[str] = Field(
        default_factory=lambda: ["commoncrawl", "webtech"], min_length=1,
    )
    insert_batch_rows: int = Field(default=50_000, ge=1, le=50_000)
    merge_batch_rows: int = Field(default=100_000, ge=1, le=1_000_000)
    max_threads: int = Field(default=4, ge=1, le=16)
    max_execution_time: int = Field(default=3600, ge=1, le=43200)

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, value: list[str]) -> list[str]:
        if not value or set(value) - {"commoncrawl", "webtech"}:
            raise ValueError("Select commoncrawl, webtech, or both")
        return sorted(set(value))


def normalize_target(root: str, url: str) -> tuple[str, str, str]:
    """Use the exact Webtech identity, validating the source's domain association."""
    root = root.strip().rstrip(".").lower().encode("idna").decode("ascii")
    origin, page = page_identity(url)
    host = origin.split("://", 1)[1].split(":", 1)[0]
    if (len(root) > 253 or ROOT_PATTERN.fullmatch(root) is None
            or re.fullmatch(r"[0-9]+(?:\.[0-9]+){3}", root) is not None
            or not (host == root or host.endswith("." + root))):
        raise ValueError("URL is not within its source root domain")
    return root, origin, page


def stage_source_pages(
    reader: Client, writer: Client, *, stage: str, config: WebInventoryConfig,
    stamp: datetime, run_id: str, settings: dict, query_prefix: str,
    log: logging.Logger,
) -> dict[str, dict[str, int]]:
    """Stream narrow URL evidence and insert bounded batches, never individual rows."""
    queries = []
    if "commoncrawl" in config.sources:
        for table, column in COMMONCRAWL_URLS:
            queries.append(("commoncrawl", table, f"""
                SELECT root_domain,{column},'',resolved_at
                FROM corpscout.{table} WHERE notEmpty({column})
            """))
    if "webtech" in config.sources:
        # Read scans instead of technology rows: a success with zero technologies
        # still establishes a page. Failed attempts are not proof of a website.
        queries.append(("webtech", "webtech_domain_scan_results", """
            SELECT root_domain,if(empty(page_url),requested_url,page_url),final_url,scanned_at
            FROM corpscout.webtech_domain_scan_results FINAL WHERE outcome='success'
        """))
    counts = {}
    for source, table, query in queries:
        statistics = {"read_rows": 0, "accepted_urls": 0, "rejected_urls": 0}
        batch = []
        log.info("Reading website/page inventory source %s from corpscout.%s", source, table)
        rows = reader.execute_iter(query, settings=settings, query_id=f"{query_prefix}read-{table}")
        for root, requested, final, observed in rows:
            statistics["read_rows"] += 1
            # Record both requested and final page identities for in-domain redirects.
            # A cross-domain redirect needs a separately established root association.
            candidates = [requested]
            if source == "webtech" and final and final != requested:
                candidates.append(final)
            for url in candidates:
                try:
                    identity = normalize_target(root, url)
                except (ValueError, UnicodeError):
                    statistics["rejected_urls"] += 1
                    continue
                if observed.timestamp() <= 0:
                    statistics["rejected_urls"] += 1
                    continue
                # CC resolved_at is extraction/processing evidence time, NOT WARC
                # capture time or a live fetch. Webtech scanned_at is live evidence.
                fetched = observed if source == "webtech" else None
                batch.append((*identity, [source], stamp, stamp, observed, fetched, run_id))
                statistics["accepted_urls"] += 1
                if len(batch) >= config.insert_batch_rows:
                    writer.execute(f"INSERT INTO {stage} ({','.join(PAGE_COLUMNS)}) VALUES", batch,
                                   settings=settings, query_id=f"{query_prefix}insert-{uuid4().hex}")
                    batch.clear()
                    log.info("Source %s: %s URLs staged, %s rejected", table,
                             statistics["accepted_urls"], statistics["rejected_urls"])
        if batch:
            writer.execute(f"INSERT INTO {stage} ({','.join(PAGE_COLUMNS)}) VALUES", batch,
                           settings=settings, query_id=f"{query_prefix}insert-{uuid4().hex}")
        counts[table] = statistics
        log.info("Completed source %s: %s", table, statistics)
    return counts


def fold_inventory(
    client: Client, *, contributions: str, stage: str, table: Literal["pages", "websites"],
    config: WebInventoryConfig, settings: dict, query_prefix: str, run_id: str,
    log: logging.Logger,
) -> int:
    """Merge bounded root ranges, keeping all pages for a root together."""
    keys = "root_domain,website_origin,page_url" if table == "pages" else "root_domain,website_origin"
    columns = PAGE_COLUMNS if table == "pages" else WEBSITE_COLUMNS
    after = ""
    batches = 0
    while True:
        boundary = client.execute(
            f"SELECT root_domain FROM {contributions} WHERE root_domain > %(after)s "
            "ORDER BY root_domain LIMIT 1 OFFSET %(batch)s",
            {"after": after, "batch": config.merge_batch_rows}, settings=settings,
            query_id=f"{query_prefix}{table}-boundary-{batches}",
        )
        through = boundary[0][0] if boundary else None
        selection = "root_domain > %(after)s"
        if through is not None:
            selection += " AND root_domain <= %(through)s"
        client.execute(f"""
            INSERT INTO {stage} ({','.join(columns)})
            SELECT {keys},arraySort(groupUniqArrayArray(sources)),
                min(first_seen_at),max(last_seen_at),max(last_observed_at),
                max(last_successful_fetch_at),%(run)s
            FROM {contributions} WHERE {selection} GROUP BY {keys}
        """, {"after": after, "through": through, "run": run_id}, settings=settings,
            query_id=f"{query_prefix}{table}-fold-{batches}")
        batches += 1
        log.info("Merged %s range %s: %s through %s", table, batches, after, through or "end")
        if through is None:
            return batches
        after = through


def publish_web_inventory(
    clickhouse: ClickhouseResource, *, config: WebInventoryConfig, run_id: str,
    log: logging.Logger,
) -> dict:
    required = ["domains", "websites", "pages"]
    if "commoncrawl" in config.sources:
        required.extend(table for table, _ in COMMONCRAWL_URLS)
    if "webtech" in config.sources:
        required.append("webtech_domain_scan_results")
    assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=required)
    suffix = uuid4().hex
    query_prefix = f"web-inventory:{suffix}:"
    pages_input = f"corpscout.pages_sources_{suffix}"
    pages_stage = f"corpscout.pages_stage_{suffix}"
    sites_input = f"corpscout.websites_sources_{suffix}"
    sites_stage = f"corpscout.websites_stage_{suffix}"
    settings = {
        "max_threads": config.max_threads,
        "max_execution_time": config.max_execution_time,
        "max_memory_usage": 4 * 1024**3,
        "max_bytes_before_external_group_by": 512 * 1024**2,
        "max_bytes_before_external_sort": 512 * 1024**2,
        "optimize_aggregation_in_order": 0,
        "join_algorithm": "full_sorting_merge",
        "join_use_nulls": 1,
        "async_insert": 0,
    }
    created = []
    stamp = datetime.now(UTC)
    with clickhouse.get_connection() as writer, clickhouse.get_connection() as reader:
        try:
            for temporary, base in ((pages_input, "pages"), (pages_stage, "pages"),
                                    (sites_input, "websites"), (sites_stage, "websites")):
                writer.execute(f"CREATE TABLE {temporary} AS corpscout.{base}")
                created.append(temporary)
            counts = stage_source_pages(
                reader, writer, stage=pages_input, config=config, stamp=stamp,
                run_id=run_id, settings=settings, query_prefix=query_prefix, log=log,
            )
            if sum(item["accepted_urls"] for item in counts.values()) == 0:
                raise ValueError("No valid source pages found; refusing to replace the inventories")
            # Retain previous evidence even when a source is not selected this run,
            # an archive is retired, or the latest scan failed. This is an inventory,
            # not a claim that every historical page is currently reachable.
            for target, base, columns in ((pages_input, "pages", PAGE_COLUMNS),
                                          (sites_input, "websites", WEBSITE_COLUMNS)):
                writer.execute(f"INSERT INTO {target} ({','.join(columns)}) "
                               f"SELECT {','.join(columns)} FROM corpscout.{base}",
                               settings=settings, query_id=f"{query_prefix}retain-{base}")
            page_batches = fold_inventory(
                writer, contributions=pages_input, stage=pages_stage, table="pages",
                config=config, settings=settings, query_prefix=query_prefix, run_id=run_id, log=log,
            )
            writer.execute(f"INSERT INTO {sites_input} ({','.join(WEBSITE_COLUMNS)}) "
                           f"SELECT {','.join(WEBSITE_COLUMNS)} FROM {pages_stage}",
                           settings=settings, query_id=f"{query_prefix}derive-websites")
            site_batches = fold_inventory(
                writer, contributions=sites_input, stage=sites_stage, table="websites",
                config=config, settings=settings, query_prefix=query_prefix, run_id=run_id, log=log,
            )
            # Fail closed rather than silently adding roots to domains or publishing
            # orphan sites. Sorted joins avoid a hash set of the 123M-domain inventory.
            missing = writer.execute(f"""
                SELECT sites.root_domain FROM {sites_stage} sites
                LEFT ANY JOIN corpscout.domains parents ON sites.root_domain=parents.root_domain
                WHERE isNull(parents.root_domain) LIMIT 10
            """, settings=settings, query_id=f"{query_prefix}validate-domains")
            if missing:
                raise ValueError(f"Website roots missing from domains; refresh domain inventory first: {missing}")
            [(page_count,)] = writer.execute(f"SELECT count() FROM {pages_stage}")
            [(site_count,)] = writer.execute(f"SELECT count() FROM {sites_stage}")
            if not page_count or not site_count:
                raise ValueError("Empty website/page stage; refusing publication")
            # EXCHANGE is atomic per table, not across tables. Publish the superset
            # of old/new parents first, then pages. If the second exchange fails,
            # old pages still have their parents and a retry converges safely.
            writer.execute(f"EXCHANGE TABLES {sites_stage} AND corpscout.websites",
                           query_id=f"{query_prefix}publish-websites")
            writer.execute(f"EXCHANGE TABLES {pages_stage} AND corpscout.pages",
                           query_id=f"{query_prefix}publish-pages")
            return {"websites": site_count, "pages": page_count, "source_counts": counts,
                    "page_merge_batches": page_batches, "website_merge_batches": site_batches,
                    "source_run_id": run_id}
        except BaseException:
            reader.disconnect()
            writer.disconnect()
            writer.execute("KILL QUERY WHERE startsWith(query_id,%(prefix)s) SYNC", {"prefix": query_prefix})
            raise
        finally:
            for temporary in reversed(created):
                writer.execute(f"DROP TABLE IF EXISTS {temporary}")


SOURCE_DEPS = [
    dg.AssetKey("domains"),
    *(dg.AssetKey(["corpscout", table]) for table, _ in COMMONCRAWL_URLS),
    dg.AssetKey("commoncrawl_webtech_results_clickhouse"),
    dg.AssetKey("webtech_scan_results"),
]


@dg.multi_asset(
    specs=[
        dg.AssetSpec("websites", deps=SOURCE_DEPS, group_name="web_inventory",
                     kinds={"clickhouse"}, metadata={"dagster/table_name": "corpscout.websites"}),
        dg.AssetSpec("pages", deps=[*SOURCE_DEPS, "websites"], group_name="web_inventory",
                     kinds={"clickhouse"}, metadata={"dagster/table_name": "corpscout.pages"}),
    ],
    pool="web_inventory_publish",
)
def web_inventory(
    context: dg.AssetExecutionContext, config: WebInventoryConfig,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.MaterializeResult]:
    result = publish_web_inventory(clickhouse, config=config, run_id=context.run.run_id, log=context.log)
    for table in ("websites", "pages"):
        yield dg.MaterializeResult(asset_key=table, metadata={
            **result, "dagster/row_count": result[table],
        })


web_inventory_job = dg.define_asset_job(
    "web_inventory_job", selection=dg.AssetSelection.assets(web_inventory),
)
defs = dg.Definitions(assets=[web_inventory], jobs=[web_inventory_job])
