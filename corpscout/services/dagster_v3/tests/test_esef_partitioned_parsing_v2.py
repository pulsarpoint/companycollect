import json
from datetime import date
from datetime import timedelta
from pathlib import Path

import duckdb
from dagster import AssetKey

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.partitioned_assets import (
    ESEF_PARSING_V2_ASSETS,
    build_facts_partition_database,
)
from dagster_v3.defs.esef_filings.partitioned_publish import (
    ESEF_PARSING_V2_CLICKHOUSE_ASSETS,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    ESEF_PROCESSED_WEEK_PARTITIONS,
    document_result_object_key,
)
from dagster_v3.defs.esef_filings.partitioned_storage import (
    QUALIFIED_PARTITION_STATUS_TABLE,
    ResultProjection,
    SOURCE_DOCUMENTS_PROJECTION,
    write_result_projection_partition,
)


EXPECTED_DUCKDB_ASSETS = {
    "esef_source_documents_duckdb_v2",
    "esef_filing_facts_duckdb_v2",
    "esef_document_contact_candidates_duckdb_v2",
    "esef_document_concept_labels_duckdb_v2",
    "esef_fact_disclosures_duckdb_v2",
}

EXPECTED_CLICKHOUSE_DEPENDENCIES = {
    "esef_source_documents_clickhouse_v2": "esef_source_documents_duckdb_v2",
    "esef_facts_clickhouse_v2": "esef_filing_facts_duckdb_v2",
    "esef_document_contact_candidates_clickhouse_v2": (
        "esef_document_contact_candidates_duckdb_v2"
    ),
    "esef_document_concept_labels_clickhouse_v2": (
        "esef_document_concept_labels_duckdb_v2"
    ),
    "esef_fact_disclosures_clickhouse_v2": "esef_fact_disclosures_duckdb_v2",
}


def test_v2_duckdb_assets_are_independent_artifact_projections() -> None:
    assets_by_name = {asset.key.path[-1]: asset for asset in ESEF_PARSING_V2_ASSETS}

    assert set(assets_by_name) == EXPECTED_DUCKDB_ASSETS
    assert len({asset.op.pool for asset in assets_by_name.values()}) == 5

    for asset_name, asset in assets_by_name.items():
        assert asset.asset_deps[AssetKey(asset_name)] == {
            AssetKey("esef_document_artifacts_s3")
        }
        assert asset.partitions_def is ESEF_PROCESSED_WEEK_PARTITIONS
        assert asset.backfill_policy.max_partitions_per_run == 1


def test_v2_clickhouse_assets_publish_only_their_matching_duckdb_output() -> None:
    assets_by_name = {
        asset.key.path[-1]: asset for asset in ESEF_PARSING_V2_CLICKHOUSE_ASSETS
    }

    assert set(assets_by_name) == set(EXPECTED_CLICKHOUSE_DEPENDENCIES)
    assert len({asset.op.pool for asset in assets_by_name.values()}) == 5

    for asset_name, duckdb_asset_name in EXPECTED_CLICKHOUSE_DEPENDENCIES.items():
        asset = assets_by_name[asset_name]
        assert asset.asset_deps[AssetKey(asset_name)] == {AssetKey(duckdb_asset_name)}
        assert asset.partitions_def is ESEF_PROCESSED_WEEK_PARTITIONS
        assert asset.backfill_policy.max_partitions_per_run == 1


def test_v2_processed_week_partition_is_seven_days() -> None:
    window = ESEF_PROCESSED_WEEK_PARTITIONS.time_window_for_partition_key("2025-03-30")

    assert window.end - window.start == timedelta(days=7)


def test_source_document_projection_writes_atomic_completed_partition(
    tmp_path: Path,
) -> None:
    partition_key = "2025-03-30"
    result = {
        "schema_version": 3,
        "processed_week": partition_key,
        "source_run_id": "run-1",
        "extracted_at": "2025-04-01T00:00:00Z",
        "source_document_ids": ["filing-1"],
        "document_rows": [_projection_row(SOURCE_DOCUMENTS_PROJECTION)],
        "candidate_rows": [],
        "concept_label_rows": [],
        "metadata": {},
    }
    object_store = _FakeObjectStore(
        {
            (
                ESEF_DOCUMENT_BUCKET,
                document_result_object_key(partition_key),
            ): json.dumps(result).encode(),
        }
    )
    target_path = tmp_path / "source-documents.duckdb"

    metadata = write_result_projection_partition(
        object_store=object_store,
        partition_key=partition_key,
        projection=SOURCE_DOCUMENTS_PROJECTION,
        target_path=target_path,
    )

    assert metadata["row_count"] == 1
    assert metadata["source_document_count"] == 1
    assert target_path.exists()
    with duckdb.connect(str(target_path), read_only=True) as connection:
        [(source_document_id, processed_week)] = connection.execute(
            "select source_document_id, processed_week "
            "from esef_filings.esef_source_documents"
        ).fetchall()
        [(expected_rows, actual_rows)] = connection.execute(
            f"select expected_row_count, actual_row_count "
            f"from {QUALIFIED_PARTITION_STATUS_TABLE}"
        ).fetchall()
    assert source_document_id == "source_document_id-value"
    assert processed_week == date(2025, 3, 30)
    assert (expected_rows, actual_rows) == (1, 1)


def test_empty_facts_partition_initializes_a_new_duckdb_before_checkpoint_read(
    tmp_path: Path,
) -> None:
    partition_key = "2025-03-30"
    result = {
        "schema_version": 3,
        "processed_week": partition_key,
        "source_run_id": "artifact-run",
        "extracted_at": "2025-04-01T00:00:00Z",
        "source_document_ids": [],
        "document_rows": [],
        "candidate_rows": [],
        "concept_label_rows": [],
        "metadata": {},
    }
    object_store = _FakeObjectStore(
        {
            (
                ESEF_DOCUMENT_BUCKET,
                document_result_object_key(partition_key),
            ): json.dumps(result).encode(),
        }
    )
    target_path = tmp_path / "facts.duckdb"

    metadata = build_facts_partition_database(
        object_store=object_store,
        partition_key=partition_key,
        source_run_id="facts-run",
        log_info=lambda *_args: None,
        log_warning=lambda *_args: None,
        target_path=target_path,
    )

    assert metadata["row_count"] == 0
    assert target_path.exists()
    with duckdb.connect(str(target_path), read_only=True) as connection:
        [(actual_rows,)] = connection.execute(
            f"select count(*) from {tables.QUALIFIED_FACTS_TABLE}"
        ).fetchall()
        [(expected_rows, status_rows)] = connection.execute(
            f"select expected_row_count, actual_row_count "
            f"from {QUALIFIED_PARTITION_STATUS_TABLE}"
        ).fetchall()
    assert actual_rows == 0
    assert (expected_rows, status_rows) == (0, 0)


def test_esef_v2_migration_creates_five_weekly_partitioned_tables() -> None:
    migration_path = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000309_corpscout_esef_parsing_v2.up.sql"
    )
    migration_sql = migration_path.read_text(encoding="utf-8")

    assert migration_sql.count("CREATE TABLE IF NOT EXISTS corpscout.esef_") == 5
    assert migration_sql.count("PARTITION BY processed_week") == 5
    assert migration_sql.count("processed_week") >= 15


def test_esef_v2_label_uid_default_converts_fixed_string_before_lowering() -> None:
    migration_path = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000310_corpscout_esef_concept_label_uid_default.up.sql"
    )

    migration_sql = migration_path.read_text(encoding="utf-8")

    assert "lowerUTF8(toString(package_sha256))" in migration_sql


def test_esef_v2_source_record_uid_matches_production_contracts() -> None:
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )

    migration_sql = (
        migration_directory / "000311_corpscout_esef_v2_source_record_uid.up.sql"
    ).read_text(encoding="utf-8")
    down_sql = (
        migration_directory / "000311_corpscout_esef_v2_source_record_uid.down.sql"
    ).read_text(encoding="utf-8")

    assert migration_sql.count("ADD COLUMN IF NOT EXISTS source_record_uid String") == 2
    assert "ALTER TABLE corpscout.esef_source_documents_v2" in migration_sql
    assert "ALTER TABLE corpscout.esef_document_contact_candidates_v2" in migration_sql
    assert migration_sql.count("lowerUTF8(toString(package_sha256))") == 2
    assert down_sql.count("DROP COLUMN IF EXISTS source_record_uid") == 2


def _projection_row(projection: ResultProjection) -> dict[str, object]:
    row: dict[str, object] = {}
    for column in projection.columns:
        if column == "processed_week":
            continue
        if column in projection.integer_columns:
            row[column] = 1
        elif column in projection.boolean_columns:
            row[column] = True
        else:
            row[column] = f"{column}-value"
    return row


class _FakeObjectStore:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def download_file(
        self,
        key: str,
        target_path: Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        target_path.write_bytes(self.objects[(bucket, key)])
