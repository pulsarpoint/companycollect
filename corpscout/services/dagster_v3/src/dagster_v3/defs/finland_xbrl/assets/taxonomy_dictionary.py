"""Finland SBR taxonomy dictionary: download the official package once per
version and build an Arelle-derived concept/label dictionary in ClickHouse.

Both assets are intentionally NOT wired into any job selection -- the
taxonomy dictionary is a rare, manual/yearly run (a new SBR-DPM taxonomy
version), unlike the daily/monthly Finland XBRL data chains.
"""

from datetime import UTC, datetime
import tempfile
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers.requests import Client as DltRequestsClient

from dagster_v3.defs.finland_xbrl.assets.common import (
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_BUCKET,
    XBRL_TIMEOUT_SECONDS,
)
from dagster_v3.defs.finland_xbrl.taxonomy import TAXONOMY_SOURCE_URL, TAXONOMY_VERSION
from dagster_v3.defs.finland_xbrl.unified_adapter import FINLAND_PROFILE
from dagster_v3.defs.finland_xbrl.unified_clickhouse import (
    CLICKHOUSE_DATABASE,
    replace_clickhouse_table_with_rows,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.xbrl_common.tables import (
    TAXONOMY_CONCEPT_COLUMNS,
    TAXONOMY_LABEL_COLUMNS,
)
from dagster_v3.defs.xbrl_common.taxonomy import (
    concept_rows_from_model,
    load_taxonomy_package,
)

FI_TAXONOMY_PACKAGE_KEY = f"finland_xbrl/taxonomy/{TAXONOMY_VERSION}/package.zip"
FI_TAXONOMY_CONCEPTS_TABLE = "fi_taxonomy_concepts"
FI_TAXONOMY_LABELS_TABLE = "fi_taxonomy_labels"
TAXONOMY_DOWNLOAD_USER_AGENT = "corpscout-dagster-v3-fi-taxonomy/0.1"


def _taxonomy_download_client() -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=XBRL_TIMEOUT_SECONDS,
        session_attrs={"headers": {"User-Agent": TAXONOMY_DOWNLOAD_USER_AGENT}},
    )


@dg.asset(
    name="finland_taxonomy_package_s3",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    kinds={"python", "s3", "xbrl", "taxonomy"},
    description=(
        "Downloads the official Finnish SBR taxonomy distribution zip for "
        f"{TAXONOMY_VERSION} and uploads it to S3 (additive-only: skips the "
        "download when the object already exists). Launched manually per "
        "taxonomy version, not part of any job."
    ),
)
def finland_taxonomy_package_s3(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    if object_store.exists(FI_TAXONOMY_PACKAGE_KEY, bucket=XBRL_BUCKET):
        context.log.info(
            "Finland taxonomy package already present at s3://%s/%s; skipping download",
            XBRL_BUCKET,
            FI_TAXONOMY_PACKAGE_KEY,
        )
        return dg.MaterializeResult(
            metadata={
                "taxonomy_version": TAXONOMY_VERSION,
                "s3_key": FI_TAXONOMY_PACKAGE_KEY,
                "skipped": True,
            }
        )

    response = _taxonomy_download_client().get(
        TAXONOMY_SOURCE_URL, timeout=XBRL_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    body = response.content
    object_store.ensure_bucket(XBRL_BUCKET)
    object_store.write_bytes(FI_TAXONOMY_PACKAGE_KEY, body, bucket=XBRL_BUCKET)
    context.log.info(
        "Uploaded Finland taxonomy package %s (%d bytes) to s3://%s/%s",
        TAXONOMY_VERSION,
        len(body),
        XBRL_BUCKET,
        FI_TAXONOMY_PACKAGE_KEY,
    )
    return dg.MaterializeResult(
        metadata={
            "taxonomy_version": TAXONOMY_VERSION,
            "s3_key": FI_TAXONOMY_PACKAGE_KEY,
            "size_bytes": len(body),
            "skipped": False,
        }
    )


@dg.asset(
    name="fi_taxonomy_dictionary_clickhouse",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[finland_taxonomy_package_s3],
    kinds={"python", "clickhouse", "xbrl", "taxonomy"},
    description=(
        "Loads the Finland SBR taxonomy package with Arelle and replaces "
        "corpscout.fi_taxonomy_concepts / fi_taxonomy_labels with the "
        "resulting concept and label dictionary. Launched manually per "
        "taxonomy version, not part of any job."
    ),
)
def fi_taxonomy_dictionary_clickhouse(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(FI_TAXONOMY_CONCEPTS_TABLE, FI_TAXONOMY_LABELS_TABLE),
    )

    with tempfile.TemporaryDirectory(prefix="fi_taxonomy_package_") as temp_dir:
        package_path = Path(temp_dir) / "package.zip"
        object_store.download_file(
            FI_TAXONOMY_PACKAGE_KEY, package_path, bucket=XBRL_BUCKET
        )
        try:
            model_xbrl, entrypoint = load_taxonomy_package(package_path=package_path)
        except Exception as exc:
            raise dg.Failure(
                description=(
                    "Arelle failed to load the Finland SBR taxonomy package "
                    f"({TAXONOMY_VERSION}): {exc}"
                ),
                metadata={
                    "taxonomy_version": TAXONOMY_VERSION,
                    "s3_key": FI_TAXONOMY_PACKAGE_KEY,
                    "error": str(exc),
                },
            ) from exc

        try:
            concept_rows, label_rows = concept_rows_from_model(
                model_xbrl,
                taxonomy_version=TAXONOMY_VERSION,
                profile=FINLAND_PROFILE,
                loaded_at=datetime.now(UTC).isoformat(),
            )
        finally:
            model_xbrl.close()

    if not concept_rows:
        raise ValueError(
            "Refusing to publish Finland taxonomy dictionary: concept_rows_from_model "
            f"returned zero concept rows for entrypoint {entrypoint!r} "
            "(would blank a populated table)"
        )

    with clickhouse.get_connection() as client:
        concept_count = replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            table=FI_TAXONOMY_CONCEPTS_TABLE,
            columns=TAXONOMY_CONCEPT_COLUMNS,
            rows=concept_rows,
            converter=lambda row: tuple(row[column] for column in TAXONOMY_CONCEPT_COLUMNS),
        )
        label_count = replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            table=FI_TAXONOMY_LABELS_TABLE,
            columns=TAXONOMY_LABEL_COLUMNS,
            rows=label_rows,
            converter=lambda row: tuple(row[column] for column in TAXONOMY_LABEL_COLUMNS),
        )

    context.log.info(
        "Published Finland taxonomy dictionary %s (entrypoint=%s): "
        "concepts=%d labels=%d",
        TAXONOMY_VERSION,
        entrypoint,
        concept_count,
        label_count,
    )
    return dg.MaterializeResult(
        metadata={
            "taxonomy_version": TAXONOMY_VERSION,
            "entrypoint": entrypoint,
            "concepts_row_count": concept_count,
            "labels_row_count": label_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
        }
    )
