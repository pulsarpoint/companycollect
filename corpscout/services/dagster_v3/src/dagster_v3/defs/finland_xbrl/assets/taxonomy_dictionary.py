"""Finland SBR taxonomy dictionary: download the official package once per
version and build an Arelle-derived concept/label dictionary in ClickHouse.

Both assets are intentionally NOT wired into any job selection -- the
taxonomy dictionary is a rare, manual/yearly run (a new SBR-DPM taxonomy
version), unlike the daily/monthly Finland XBRL data chains.
"""

from datetime import UTC, datetime
import tempfile
from pathlib import Path
from typing import Any

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


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _required_loaded_at(value: Any) -> datetime:
    """Mirrors unified_clickhouse.py's `_required_datetime` (private there,
    so replicated here): clickhouse-driver's DateTime64Column only accepts
    int/datetime, never str, so `concept_rows_from_model`'s string
    `loaded_at` must be parsed before insert -- row dicts themselves keep the
    ISO string form so the builder's signature and its tests stay untouched."""
    if isinstance(value, datetime):
        return _utc_datetime(value)
    if value is None or value == "":
        raise ValueError("loaded_at is required and cannot be empty")
    return _utc_datetime(datetime.fromisoformat(str(value)))


def taxonomy_row_converter(columns: tuple[str, ...]):
    def convert(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(
            _required_loaded_at(row[column]) if column == "loaded_at" else row[column]
            for column in columns
        )

    return convert


def replace_taxonomy_dictionary_tables(
    *,
    clickhouse: Any,
    concept_rows: list[dict[str, Any]],
    label_rows: list[dict[str, Any]],
    entrypoint: str,
) -> tuple[int, int]:
    """Refuse-then-replace, extracted from the asset body so both refusals
    are testable without an Arelle load or a real ClickHouse connection --
    mirrors `unified_clickhouse.export_finland_unified_clickhouse`'s
    refuse-before-touching-ClickHouse shape."""
    if not concept_rows:
        raise ValueError(
            "Refusing to publish Finland taxonomy dictionary: concept_rows_from_model "
            f"returned zero concept rows for entrypoint {entrypoint!r} "
            "(would blank a populated table)"
        )
    if not label_rows:
        raise ValueError(
            "Refusing to publish Finland taxonomy dictionary: concept_rows_from_model "
            f"returned zero label rows for entrypoint {entrypoint!r} (a partial Arelle "
            "load -- concepts ok, label linkbase missing -- would blank a populated "
            "corpscout.fi_taxonomy_labels table)"
        )

    with clickhouse.get_connection() as client:
        concept_count = replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            table=FI_TAXONOMY_CONCEPTS_TABLE,
            columns=TAXONOMY_CONCEPT_COLUMNS,
            rows=concept_rows,
            converter=taxonomy_row_converter(TAXONOMY_CONCEPT_COLUMNS),
        )
        label_count = replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            table=FI_TAXONOMY_LABELS_TABLE,
            columns=TAXONOMY_LABEL_COLUMNS,
            rows=label_rows,
            converter=taxonomy_row_converter(TAXONOMY_LABEL_COLUMNS),
        )
    return concept_count, label_count


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

    concept_count, label_count = replace_taxonomy_dictionary_tables(
        clickhouse=clickhouse,
        concept_rows=concept_rows,
        label_rows=label_rows,
        entrypoint=entrypoint,
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
