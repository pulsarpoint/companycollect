"""One coordinated parse produces nine durable, table-named Dagster assets."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.normalization.load import (
    NormalizationConfig,
    assert_schema,
    pending_attempts,
    publish_attempt,
    read_archive,
)
from dagster_v3.defs.website_crawl.normalization.tables import COLUMNS, PARSER_VERSION


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            "website_crawl_" + table,
            group_name="website_crawl_normalized",
            deps=[
                "website_full_crawl_results",
                "website_jobs_crawl_results",
                "website_site_info_results",
            ],
            kinds={"clickhouse", "duckdb"},
            code_version=PARSER_VERSION,
            metadata={"dagster/table_name": "corpscout.website_crawl_" + table},
            description="Normalized saved crawl "
            + table.replace("_", " ")
            + ". Source attempt and page evidence are retained.",
        )
        for table in COLUMNS
    ],
    pool="website_crawl_normalization",
    can_subset=False,
)
def website_crawl_normalized(
    context: dg.AssetExecutionContext,
    config: NormalizationConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
):
    with processing.get_store() as store, clickhouse.get_connection() as client:
        lock_name = "website_crawl_normalization"
        with store.transaction() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS acquired",
                (lock_name,),
            )
            if not cursor.fetchone()["acquired"]:
                raise ValueError("Crawl normalization is already running")
        try:
            assert_schema(client)
            sources = pending_attempts(client, config)
            totals = {table: 0 for table in COLUMNS}
            for source in sources:
                context.log.info(
                    "Normalizing %s %s request=%s attempt=%s",
                    source["domain"],
                    source["crawl_type"],
                    source["request_id"],
                    source["attempt"],
                )
                counts = publish_attempt(
                    client, source, read_archive(client, source), context.run_id
                )
                for table, count in counts.items():
                    totals[table] += count
            for table, count in totals.items():
                yield dg.MaterializeResult(
                    asset_key="website_crawl_" + table,
                    metadata={
                        "rows_written": count,
                        "attempt_count": len(sources),
                        "parser_version": PARSER_VERSION,
                    },
                )
        finally:
            with store.transaction() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (lock_name,)
                )


defs = dg.Definitions(
    assets=[website_crawl_normalized],
    jobs=[
        dg.define_asset_job(
            "website_crawl_normalized_job",
            selection=dg.AssetSelection.assets(website_crawl_normalized),
        )
    ],
)
