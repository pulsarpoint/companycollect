from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dagster_v3.defs.common.object_catalog import (
    OBJECT_CATALOG_REQUIRED_COLUMNS,
    ObjectCatalogCommit,
    ObjectCatalogFile,
    ObjectCatalogLocation,
)


def test_object_catalog_location_builds_deterministic_keys() -> None:
    location = ObjectCatalogLocation(
        source="denmark_cvr",
        dataset="company_details",
        partition={"year": "2026", "hash_bucket": "0a/1"},
    )

    assert location.partition_prefix() == (
        "v2/source=denmark_cvr/dataset=company_details/partition/"
        "hash_bucket=0a%2F1/year=2026/"
    )
    assert location.data_object_key("a" * 64, object_format="ndjson.gz") == (
        f"{location.partition_prefix()}objects/sha256={'a' * 64}.ndjson.gz"
    )
    assert location.catalog_object_key("run/42") == (
        f"{location.partition_prefix()}catalogs/run_id=run%2F42/catalog.parquet"
    )
    assert location.commit_object_key() == f"{location.partition_prefix()}commit.json"


def test_object_catalog_commit_round_trips_as_canonical_json() -> None:
    location = ObjectCatalogLocation(
        source="sweden_company",
        dataset="raw_archives",
        partition={"snapshot_date": "2026-08-16"},
    )
    commit = ObjectCatalogCommit(
        location=location,
        source_run_id="run-42",
        created_at=datetime(2026, 8, 16, 12, 30, tzinfo=UTC),
        catalog=ObjectCatalogFile(
            key=location.catalog_object_key("run-42"),
            sha256="b" * 64,
            size_bytes=1_024,
            row_count=2,
        ),
        data_object_count=2,
        data_size_bytes=50_000,
        data_row_count=100,
    )

    body = commit.to_json_bytes()

    assert ObjectCatalogCommit.from_json_bytes(body) == commit
    assert body.endswith(b"\n")
    assert body.index(b'"catalog"') < body.index(b'"created_at"')


def test_object_catalog_commit_rejects_catalog_for_another_run() -> None:
    location = ObjectCatalogLocation(
        source="sweden_company",
        dataset="raw_archives",
        partition={"snapshot_date": "2026-08-16"},
    )

    with pytest.raises(ValidationError, match="catalog key must match"):
        ObjectCatalogCommit(
            location=location,
            source_run_id="run-42",
            created_at=datetime.now(UTC),
            catalog=ObjectCatalogFile(
                key=location.catalog_object_key("another-run"),
                sha256="c" * 64,
                size_bytes=100,
                row_count=1,
            ),
            data_object_count=1,
            data_size_bytes=100,
            data_row_count=1,
        )


def test_object_catalog_commit_rejects_inconsistent_object_count() -> None:
    location = ObjectCatalogLocation(
        source="norway_brreg_financial",
        dataset="annual_accounts",
        partition={"filing_year": "2025", "chunk": "0001"},
    )

    with pytest.raises(ValidationError, match="data object count must equal"):
        ObjectCatalogCommit(
            location=location,
            source_run_id="run-42",
            created_at=datetime.now(UTC),
            catalog=ObjectCatalogFile(
                key=location.catalog_object_key("run-42"),
                sha256="d" * 64,
                size_bytes=100,
                row_count=2,
            ),
            data_object_count=1,
            data_size_bytes=100,
            data_row_count=1,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source", "Sweden Company", "lowercase storage name"),
        ("dataset", "raw/archive", "lowercase storage name"),
        ("partition", {}, "at least one dimension"),
    ],
)
def test_object_catalog_location_rejects_unsafe_names(
    field: str,
    value: object,
    message: str,
) -> None:
    values: dict[str, object] = {
        "source": "sweden_company",
        "dataset": "raw_archives",
        "partition": {"snapshot_date": "2026-08-16"},
    }
    values[field] = value

    with pytest.raises(ValidationError, match=message):
        ObjectCatalogLocation.model_validate(values)


def test_object_catalog_file_rejects_invalid_sha256() -> None:
    with pytest.raises(ValidationError, match="lowercase SHA-256"):
        ObjectCatalogFile(
            key="catalog.parquet",
            sha256="not-a-sha256",
            size_bytes=1,
            row_count=0,
        )


def test_object_catalog_required_columns_are_a_stable_contract() -> None:
    assert OBJECT_CATALOG_REQUIRED_COLUMNS == (
        "schema_version",
        "source",
        "dataset",
        "partition_json",
        "source_run_id",
        "created_at",
        "object_key",
        "object_format",
        "size_bytes",
        "sha256",
        "row_count",
    )
