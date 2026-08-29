"""Technology catalog: extension bundle + webappanalyzer overlay → ClickHouse.

One asset does the whole run: load the vendored extension bundle (frozen
bootstrap), fetch the maintained public catalog pinned to one commit SHA
(overlay — newer wins per technology name), sync icons into the dedicated
technology-icons bucket, and atomically replace corpscout.technology_catalog
via stage + EXCHANGE TABLES. No DuckDB staging and no pool: nothing here
opens a shared single-writer file (same shape as company_markets).

Weekly cadence — the overlay repository moves a few times a month, and the
extension layer never changes at all.
"""

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.technology_catalog import tables
from dagster_v3.defs.technology_catalog.catalog import (
    MergedTechnology,
    load_extension_layer,
    merge_layers,
)
from dagster_v3.defs.technology_catalog.icons import (
    IconRef,
    IconSyncResult,
    sync_icons,
)
from dagster_v3.defs.technology_catalog.source import (
    fetch_overlay_icon,
    fetch_overlay_layer,
    resolve_overlay_commit,
)

GROUP_NAME = "technology_catalog"

_EMPTY_ICON = IconRef(object_key="", content_type="")


def extension_bundle_dir() -> Path:
    """The vendored, READ-ONLY Wappalyzer extension bundle.

    Defaults to the checked-in copy at <repo>/extensions/6.12.5_0; override
    with TECHNOLOGY_CATALOG_EXTENSION_DIR where the deploy lays files out
    differently.
    """
    override = os.getenv("TECHNOLOGY_CATALOG_EXTENSION_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[7] / "extensions" / "6.12.5_0"


def build_rows(
    technologies: list[MergedTechnology],
    icon_refs: dict[str, IconRef] | IconSyncResult,
    *,
    source_run_id: str,
    updated_at: datetime,
) -> list[tuple]:
    """Rows in tables.TECHNOLOGY_CATALOG_COLUMNS order (migration 000350)."""
    refs = icon_refs.refs if isinstance(icon_refs, IconSyncResult) else icon_refs
    rows = []
    for technology in technologies:
        icon = refs.get(technology.technology, _EMPTY_ICON)
        rows.append(
            (
                technology.technology,
                technology.slug,
                technology.description,
                technology.website,
                list(technology.category_ids),
                list(technology.categories),
                list(technology.groups),
                icon.object_key,
                icon.content_type,
                int(technology.saas),
                int(technology.oss),
                list(technology.pricing),
                technology.source,
                technology.source_version,
                source_run_id,
                updated_at,
            )
        )
    return rows


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "clickhouse"},
    description=(
        "Merged technology catalog (vendored Wappalyzer extension bundle "
        "overlaid by the maintained webappanalyzer catalog, newer wins per "
        "name), icons synced to the technology-icons bucket, published via "
        "stage + EXCHANGE TABLES."
    ),
)
def technology_catalog_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    technology_catalog_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    # The migration owns the schema; refuse to run against a database that
    # has not applied it rather than issuing DDL of our own.
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.TECHNOLOGY_CATALOG_TABLES,
    )

    bundle_dir = extension_bundle_dir()
    extension = load_extension_layer(bundle_dir)
    context.log.info("extension layer: %d technologies", len(extension.technologies))

    overlay_sha = resolve_overlay_commit()
    overlay = fetch_overlay_layer(overlay_sha)
    context.log.info(
        "overlay layer @ %s: %d technologies", overlay_sha, len(overlay.technologies)
    )

    merged = merge_layers(extension, overlay)

    technology_catalog_object_store.ensure_bucket()
    icon_result = sync_icons(
        merged,
        bundle_icons_dir=bundle_dir / "images" / "icons",
        s3_client=technology_catalog_object_store.client(),
        bucket=tables.ICON_BUCKET,
        fetch_overlay_icon=lambda filename: fetch_overlay_icon(overlay_sha, filename),
        log=context.log.warning,
    )
    context.log.info(
        "icons: %d uploaded, %d already current, %d missing, %d overlay fetches",
        icon_result.uploaded,
        icon_result.skipped,
        icon_result.missing,
        icon_result.overlay_fetches,
    )

    rows = build_rows(
        merged,
        icon_result,
        source_run_id=context.run_id,
        updated_at=datetime.now(UTC).replace(tzinfo=None),
    )
    row_count = _replace_catalog(clickhouse, rows)

    per_source = {
        source: sum(1 for technology in merged if technology.source == source)
        for source in (tables.EXTENSION_SOURCE, tables.OVERLAY_SOURCE)
    }
    return dg.MaterializeResult(
        metadata={
            "rows": row_count,
            **{f"source_{source}": count for source, count in per_source.items()},
            "icons_uploaded": icon_result.uploaded,
            "icons_skipped": icon_result.skipped,
            "icons_missing": icon_result.missing,
            "overlay_icon_fetches": icon_result.overlay_fetches,
            "overlay_sha": overlay_sha,
        }
    )


def _replace_catalog(clickhouse: ClickhouseResource, rows: list[tuple]) -> int:
    """Fill a staging copy, enforce the floor, then swap it in atomically."""
    qualified = f"`{RESOLVED_DATABASE}`.`{tables.TECHNOLOGY_CATALOG_TABLE}`"
    stage = (
        f"`{RESOLVED_DATABASE}`."
        f"`_tmp_{tables.TECHNOLOGY_CATALOG_TABLE}_{uuid.uuid4().hex}`"
    )
    column_list = ", ".join(tables.TECHNOLOGY_CATALOG_COLUMNS)

    with clickhouse.get_connection() as client:
        try:
            client.execute(f"CREATE TABLE {stage} AS {qualified}")
            if rows:
                client.execute(f"INSERT INTO {stage} ({column_list}) VALUES", rows)
            row_count = int(client.execute(f"SELECT count() FROM {stage}")[0][0])
            if row_count < tables.MIN_TECHNOLOGY_CATALOG_ROWS:
                # The extension layer alone guarantees 7k+ names; a short
                # result is a broken merge, never a legitimate catalog.
                raise ValueError(
                    f"technology_catalog produced {row_count} rows, below the "
                    f"{tables.MIN_TECHNOLOGY_CATALOG_ROWS} floor"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
    return row_count


@dg.asset(
    name="technology_adoption_clickhouse",
    deps=[dg.AssetKey("technology_catalog_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "Global adoption rollup: distinct crawled root domains per technology "
        "(corpscout.technology_adoption, migration 000351). One GROUP BY pass "
        "over commoncrawl_page_technologies -- `technology` sits last in that "
        "table's sort key, so live per-technology counts scan all ~10.6B rows "
        "(6-26s each, measured 2026-08-29); this rollup is what makes the "
        "number affordable on the technology pages."
    ),
)
def technology_adoption_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=("technology_adoption",),
    )
    qualified = f"`{RESOLVED_DATABASE}`.`technology_adoption`"
    stage = f"`{RESOLVED_DATABASE}`.`_tmp_technology_adoption_{uuid.uuid4().hex}`"
    computed_at = datetime.now(UTC).replace(tzinfo=None)
    with clickhouse.get_connection() as client:
        try:
            client.execute(f"CREATE TABLE {stage} AS {qualified}")
            client.execute(
                f"""INSERT INTO {stage} (technology, domain_count, computed_at)
SELECT technology, uniqExact(root_domain), %(computed_at)s
FROM `{RESOLVED_DATABASE}`.`commoncrawl_page_technologies`
GROUP BY technology""",
                {"computed_at": computed_at},
                # The scan is the cost, not the 4.6k aggregate states; spill
                # settings + the memory cap keep it inside the shared budget
                # (same bounds the serving view and company_markets use).
                settings={
                    "max_bytes_before_external_group_by": 8 * 1024**3,
                    "max_memory_usage": 12 * 1024**3,
                },
            )
            rows = int(client.execute(f"SELECT count() FROM {stage}")[0][0])
            if rows < 1000:
                raise ValueError(
                    f"technology_adoption produced {rows} rows -- the detector "
                    "vocabulary is ~4.6k names, a short result is a broken scan"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
    context.log.info("technology_adoption: %d rows", rows)
    return dg.MaterializeResult(metadata={"rows": rows})


technology_catalog_job = dg.define_asset_job(
    name="technology_catalog_job",
    selection=dg.AssetSelection.assets(
        technology_catalog_clickhouse, technology_adoption_clickhouse
    ),
)

# Sunday 05:20 UTC — staggered minute unused by any other source. STOPPED by
# default per house pattern for new schedules; start it at instance level.
technology_catalog_weekly = dg.ScheduleDefinition(
    name="technology_catalog_weekly",
    job=technology_catalog_job,
    cron_schedule="20 5 * * 0",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[technology_catalog_clickhouse, technology_adoption_clickhouse],
    jobs=[technology_catalog_job],
    schedules=[technology_catalog_weekly],
    resources={
        "technology_catalog_object_store": ObjectStoreResource(
            bucket=tables.ICON_BUCKET
        ),
    },
)
