import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikipedia.snapshot import (
    build_article_snapshot,
    freeze_company_inventory,
    load_manifest,
    publish_articles,
    snapshot_prefix,
)
from dagster_v3.defs.wikipedia.source import (
    ARTICLE_BUCKET,
    ARTICLE_TABLE,
    WikipediaClient,
    WikipediaSnapshotConfig,
)


@dg.asset(
    deps=["wikidata_snapshot_complete"],
    group_name="wikidata",
    kinds={"python", "s3", "wikipedia"},
    pool="wikidata_wikipedia",
    retry_policy=dg.RetryPolicy(max_retries=3, delay=300),
    description="Frozen company sitelinks and full Wikipedia responses, checkpointed in compressed S3 batches.",
)
def wikidata_company_wikipedia_articles_s3(
    context: dg.AssetExecutionContext,
    config: WikipediaSnapshotConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    store = ObjectStoreResource(bucket=ARTICLE_BUCKET)
    store.ensure_bucket()
    if not store.exists(snapshot_prefix(config.source_run_id) + "companies.json"):
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey("wikidata_snapshot_complete")
        )
        materialization = event.asset_materialization if event is not None else None
        if (
            materialization is None
            or materialization.metadata["source_run_id"].value != config.source_run_id
        ):
            raise ValueError(
                "Materialize wikidata_snapshot_complete for this source_run_id before Wikipedia discovery"
            )
    with clickhouse.get_connection() as client:
        freeze_company_inventory(store, client, config.source_run_id)
    manifest = build_article_snapshot(
        store, WikipediaClient(config), config.source_run_id, context.log.info
    )
    return dg.MaterializeResult(
        metadata={
            "source_run_id": config.source_run_id,
            "company_count": manifest["company_count"],
            "sitelink_count": manifest["sitelink_count"],
            "row_count": manifest["row_count"],
            "missing_count": manifest["missing_count"],
            "language_counts": manifest["language_counts"],
            "object_count": len(manifest["objects"]),
            "manifest_uri": f"s3://{ARTICLE_BUCKET}/{snapshot_prefix(config.source_run_id)}manifest.json",
        }
    )


@dg.asset(
    name=ARTICLE_TABLE,
    deps=["wikidata_company_wikipedia_articles_s3"],
    group_name="wikidata",
    kinds={"python", "s3", "clickhouse"},
    pool="wikidata_wikipedia",
    description="Current full Wikipedia text per company and language edition, bulk-published from a complete S3 snapshot.",
)
def wikidata_company_wikipedia_articles(
    context: dg.AssetExecutionContext,
    config: WikipediaSnapshotConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database="corpscout", tables=[ARTICLE_TABLE]
    )
    store = ObjectStoreResource(bucket=ARTICLE_BUCKET)
    manifest = load_manifest(store, config.source_run_id)
    with clickhouse.get_connection() as client:
        count = publish_articles(
            client, store, manifest, context.run_id, context.log.info
        )
    return dg.MaterializeResult(
        metadata={
            "source_run_id": config.source_run_id,
            "row_count": count,
            "table": f"corpscout.{ARTICLE_TABLE}",
            "language_counts": manifest["language_counts"],
        }
    )


wikipedia_articles_job = dg.define_asset_job(
    "wikidata_wikipedia_articles_job",
    selection=dg.AssetSelection.assets(
        wikidata_company_wikipedia_articles_s3, wikidata_company_wikipedia_articles
    ),
    tags={"wikidata_wikipedia": "true"},
)


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_snapshot_complete"),
    job=wikipedia_articles_job,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def wikidata_wikipedia_articles_sensor(
    context: dg.SensorEvaluationContext, event: dg.EventLogEntry
) -> dg.RunRequest:
    materialization = event.asset_materialization
    if materialization is None:
        raise ValueError("Wikidata snapshot completion has no materialization")
    source_run_id = materialization.metadata["source_run_id"].value
    WikipediaSnapshotConfig(source_run_id=source_run_id)
    return dg.RunRequest(
        run_key=f"wikidata-wikipedia:{source_run_id}",
        run_config={
            "ops": {
                name: {"config": {"source_run_id": source_run_id}}
                for name in (
                    "wikidata_company_wikipedia_articles_s3",
                    "wikidata_company_wikipedia_articles",
                )
            }
        },
    )


class WikipediaArticlesComponent(dg.Component, dg.Model, dg.Resolvable):
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(
            assets=[
                wikidata_company_wikipedia_articles_s3,
                wikidata_company_wikipedia_articles,
            ],
            jobs=[wikipedia_articles_job],
            sensors=[wikidata_wikipedia_articles_sensor],
        )
