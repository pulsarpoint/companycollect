from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.duckdb.schema_contract import (
    create_duckdb_table_from_contract,
    validate_duckdb_table_contract,
)
from dagster_v3.defs.eurostat import source, tables


@dataclass(frozen=True)
class LocalDatasetSnapshot:
    dataset: source.EurostatDataset
    data_path: Path
    structure_path: Path
    metadata: source.EurostatStructureMetadata
    source_data_url: str
    source_data_object_key: str
    source_data_hash: str
    source_structure_url: str
    source_structure_object_key: str
    source_structure_hash: str


@dataclass(frozen=True)
class LocalSnapshot:
    datasets: tuple[LocalDatasetSnapshot, ...]
    source_run_id: str
    pulled_at: datetime


@contextmanager
def local_snapshot_files(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    datasets: tuple[source.EurostatDataset, ...],
) -> Iterator[LocalSnapshot]:
    manifest_entries = manifest.get("datasets")
    if not isinstance(manifest_entries, list):
        raise ValueError("Eurostat snapshot manifest has no datasets list")
    entries_by_code: dict[str, dict[str, Any]] = {}
    for raw_entry in manifest_entries:
        if not isinstance(raw_entry, dict):
            raise ValueError("Eurostat snapshot contains a non-object dataset entry")
        dataset_code = str(raw_entry.get("dataset_code") or "").casefold()
        if dataset_code == "" or dataset_code in entries_by_code:
            raise ValueError(
                "Eurostat snapshot has a missing or duplicate dataset code"
            )
        entries_by_code[dataset_code] = raw_entry

    expected_codes = {dataset.code for dataset in datasets}
    actual_codes = set(entries_by_code)
    if actual_codes != expected_codes:
        missing = sorted(expected_codes - actual_codes)
        unexpected = sorted(actual_codes - expected_codes)
        raise ValueError(
            "Eurostat snapshot dataset set does not match the source registry: "
            f"missing={missing}, unexpected={unexpected}"
        )

    source_run_id = str(manifest.get("run_id") or "")
    pulled_at_text = str(manifest.get("retrieved_at") or "")
    if source_run_id == "" or pulled_at_text == "":
        raise ValueError("Eurostat snapshot is missing run or retrieval metadata")
    pulled_at = _parse_manifest_timestamp(pulled_at_text)

    with tempfile.TemporaryDirectory(prefix="eurostat_duckdb_input_") as temp_dir:
        temp_path = Path(temp_dir)
        local_datasets: list[LocalDatasetSnapshot] = []
        for dataset in datasets:
            entry = entries_by_code[dataset.code]
            data_entry = _manifest_file_entry(entry, "data", dataset.code)
            structure_entry = _manifest_file_entry(
                entry,
                "structure",
                dataset.code,
            )
            data_path = temp_path / f"{dataset.code}.tsv.gz"
            structure_path = temp_path / f"{dataset.code}.structure.xml"
            _download_and_verify(
                object_store=object_store,
                object_key=str(data_entry["object_key"]),
                expected_hash=str(data_entry["sha256"]),
                target_path=data_path,
            )
            _download_and_verify(
                object_store=object_store,
                object_key=str(structure_entry["object_key"]),
                expected_hash=str(structure_entry["sha256"]),
                target_path=structure_path,
            )
            source.validate_tsv_gzip(data_path, dataset=dataset)
            metadata = source.parse_structure_metadata(
                structure_path.read_bytes(),
                dataset=dataset,
            )
            local_datasets.append(
                LocalDatasetSnapshot(
                    dataset=dataset,
                    data_path=data_path,
                    structure_path=structure_path,
                    metadata=metadata,
                    source_data_url=str(data_entry.get("source_url") or ""),
                    source_data_object_key=str(data_entry["object_key"]),
                    source_data_hash=str(data_entry["sha256"]),
                    source_structure_url=str(structure_entry.get("source_url") or ""),
                    source_structure_object_key=str(structure_entry["object_key"]),
                    source_structure_hash=str(structure_entry["sha256"]),
                )
            )

        yield LocalSnapshot(
            datasets=tuple(local_datasets),
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )


def ensure_eurostat_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.EUROSTAT_DUCKDB_SCHEMA}")
    for table_name, (_, contract) in tables.EUROSTAT_TABLE_CONTRACTS.items():
        create_duckdb_table_from_contract(
            connection,
            schema=tables.EUROSTAT_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def replace_eurostat_snapshot(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
) -> dict[str, int]:
    if len(local_snapshot.datasets) == 0:
        raise ValueError("Eurostat local snapshot contains no datasets")

    connection.execute("begin transaction")
    try:
        ensure_eurostat_duckdb_schema(connection)
        _clear_snapshot_tables(connection)
        for dataset_snapshot in local_snapshot.datasets:
            _insert_dataset_metadata(
                connection=connection,
                dataset_snapshot=dataset_snapshot,
                local_snapshot=local_snapshot,
            )
            _insert_dimension_values(
                connection=connection,
                dataset_snapshot=dataset_snapshot,
            )
            _normalize_dataset_tsv(
                connection=connection,
                dataset_snapshot=dataset_snapshot,
            )
        counts = _validate_normalized_snapshot(
            connection=connection,
            expected_dataset_count=len(local_snapshot.datasets),
        )
        _validate_contracts(connection)
        connection.execute("commit")
        return counts
    except Exception:
        connection.execute("rollback")
        raise


def _insert_dataset_metadata(
    *,
    connection: duckdb.DuckDBPyConnection,
    dataset_snapshot: LocalDatasetSnapshot,
    local_snapshot: LocalSnapshot,
) -> None:
    metadata = dataset_snapshot.metadata
    connection.execute(
        f"""
        insert into {_qualified(tables.EUROSTAT_DATASETS_TABLE)}
        ({", ".join(tables.EUROSTAT_DATASETS_COLUMNS)})
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            metadata.dataset_code,
            metadata.title,
            metadata.dsd_version,
            metadata.source_observation_count,
            metadata.source_oldest_period,
            metadata.source_latest_period,
            _utc_naive(metadata.data_updated_at),
            _utc_naive(metadata.structure_updated_at),
            dataset_snapshot.source_data_url,
            dataset_snapshot.source_data_object_key,
            dataset_snapshot.source_data_hash,
            dataset_snapshot.source_structure_url,
            dataset_snapshot.source_structure_object_key,
            dataset_snapshot.source_structure_hash,
            local_snapshot.source_run_id,
            _utc_naive(local_snapshot.pulled_at),
        ],
    )


def _insert_dimension_values(
    *,
    connection: duckdb.DuckDBPyConnection,
    dataset_snapshot: LocalDatasetSnapshot,
) -> None:
    rows = [
        (
            dataset_snapshot.dataset.code,
            dimension.code,
            dimension.label,
            dimension.position,
            value.code,
            value.label,
            value.position,
        )
        for dimension in dataset_snapshot.metadata.dimensions
        for value in dimension.values
    ]
    if len(rows) == 0:
        raise ValueError(
            f"Eurostat dataset {dataset_snapshot.dataset.code} has no dimension values"
        )
    connection.executemany(
        f"""
        insert into {_qualified(tables.EUROSTAT_DIMENSION_VALUES_TABLE)}
        ({", ".join(tables.EUROSTAT_DIMENSION_VALUES_COLUMNS)})
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _normalize_dataset_tsv(
    *,
    connection: duckdb.DuckDBPyConnection,
    dataset_snapshot: LocalDatasetSnapshot,
) -> None:
    dataset = dataset_snapshot.dataset
    connection.execute(
        """
        create or replace temp table eurostat_source as
        select row_number() over () + 1 as source_line_number, *
        from read_csv(
          ?,
          delim = '\t',
          header = true,
          all_varchar = true,
          compression = 'gzip',
          quote = '',
          escape = '',
          strict_mode = true
        )
        """,
        [str(dataset_snapshot.data_path)],
    )
    source_columns = tuple(
        str(row[0]) for row in connection.execute("describe eurostat_source").fetchall()
    )
    if len(source_columns) < 3:
        raise ValueError(f"Eurostat dataset {dataset.code} has no observation columns")
    series_column = source_columns[1]
    expected_series_column = ",".join(dataset.expected_dimensions) + "\\TIME_PERIOD"
    if series_column != expected_series_column:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has unexpected series header "
            f"{series_column!r}"
        )
    year_columns = tuple(
        column
        for column in source_columns[2:]
        if column.isdigit() and int(column) >= source.EUROSTAT_START_YEAR
    )
    if len(year_columns) == 0:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has no observation years from "
            f"{source.EUROSTAT_START_YEAR}"
        )

    quoted_series_column = _quote_identifier(series_column)
    connection.execute(
        f"""
        create or replace temp table eurostat_series_source as
        select
          cast(source_line_number as ubigint) as source_line_number,
          trim({quoted_series_column}) as series_key,
          string_split(trim({quoted_series_column}), ',') as dimension_values
        from eurostat_source
        """
    )
    invalid_series = int(
        connection.execute(
            """
            select count(*)
            from eurostat_series_source
            where series_key = ''
               or len(dimension_values) <> ?
            """,
            [len(dataset.expected_dimensions)],
        ).fetchone()[0]
    )
    if invalid_series > 0:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has {invalid_series} malformed "
            "series keys"
        )
    duplicate_series = int(
        connection.execute(
            """
            select count(*)
            from (
              select series_key
              from eurostat_series_source
              group by series_key
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_series > 0:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has {duplicate_series} duplicate "
            "series keys"
        )

    frequency_position = dataset.expected_dimensions.index("freq") + 1
    geo_position = dataset.expected_dimensions.index("geo") + 1
    unit_expression = "cast(null as varchar)"
    if "unit" in dataset.expected_dimensions:
        unit_position = dataset.expected_dimensions.index("unit") + 1
        unit_expression = (
            f"nullif(trim(list_extract(dimension_values, {unit_position})), '')"
        )
    invalid_common_dimensions = connection.execute(
        f"""
        select
          count_if(
            trim(list_extract(dimension_values, {frequency_position})) <> 'A'
          ),
          count_if(
            nullif(trim(list_extract(dimension_values, {geo_position})), '') is null
          )
        from eurostat_series_source
        """
    ).fetchone()
    if int(invalid_common_dimensions[0]) > 0:
        raise ValueError(f"Eurostat dataset {dataset.code} contains non-annual series")
    if int(invalid_common_dimensions[1]) > 0:
        raise ValueError(
            f"Eurostat dataset {dataset.code} contains series without geography"
        )

    connection.execute(
        f"""
        insert into {_qualified(tables.EUROSTAT_SERIES_TABLE)}
        ({", ".join(tables.EUROSTAT_SERIES_COLUMNS)})
        select
          ?,
          series_key,
          trim(list_extract(dimension_values, {geo_position})),
          trim(list_extract(dimension_values, {frequency_position})),
          {unit_expression},
          source_line_number
        from eurostat_series_source
        """,
        [dataset.code],
    )
    for dimension_position, dimension_code in enumerate(
        dataset.expected_dimensions,
        start=1,
    ):
        connection.execute(
            f"""
            insert into {_qualified(tables.EUROSTAT_SERIES_DIMENSIONS_TABLE)}
            ({", ".join(tables.EUROSTAT_SERIES_DIMENSIONS_COLUMNS)})
            select
              ?,
              series_key,
              ?,
              trim(list_extract(dimension_values, {dimension_position})),
              cast(? as usmallint)
            from eurostat_series_source
            """,
            [dataset.code, dimension_code, dimension_position],
        )

    quoted_years = ", ".join(_quote_identifier(column) for column in year_columns)
    connection.execute(
        f"""
        create or replace temp table eurostat_observation_source as
        with source_observations as (
          unpivot eurostat_source
          on {quoted_years}
          into name time_period value raw_cell
        ), cleaned as (
          select
            source_line_number,
            trim(time_period) as time_period,
            trim(raw_cell) as raw_cell
          from source_observations
        )
        select
          source_line_number,
          time_period,
          raw_cell,
          regexp_extract(raw_cell, '^([^[:space:]]+)', 1) as value_token,
          trim(regexp_replace(raw_cell, '^[^[:space:]]+', '')) as status
        from cleaned
        """
    )
    malformed_observations = int(
        connection.execute(
            """
            select count(*)
            from eurostat_observation_source
            where value_token <> ':'
              and try_cast(replace(value_token, ',', '') as double) is null
            """
        ).fetchone()[0]
    )
    if malformed_observations > 0:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has {malformed_observations} "
            "malformed observation values"
        )

    connection.execute(
        f"""
        insert into {_qualified(tables.EUROSTAT_OBSERVATIONS_TABLE)}
        ({", ".join(tables.EUROSTAT_OBSERVATIONS_COLUMNS)})
        select
          ?,
          trim(list_extract(series.dimension_values, {geo_position})),
          series.series_key,
          observations.time_period,
          make_date(cast(observations.time_period as integer), 1, 1),
          cast(observations.time_period as usmallint),
          case
            when observations.value_token = ':' then cast(null as double)
            else try_cast(replace(observations.value_token, ',', '') as double)
          end,
          observations.status
        from eurostat_observation_source as observations
        inner join eurostat_series_source as series using (source_line_number)
        where observations.value_token <> ':'
           or observations.status <> ''
        """,
        [dataset.code],
    )


def _validate_normalized_snapshot(
    *,
    connection: duckdb.DuckDBPyConnection,
    expected_dataset_count: int,
) -> dict[str, int]:
    dataset_count = _table_count(connection, tables.EUROSTAT_DATASETS_TABLE)
    if dataset_count != expected_dataset_count:
        raise ValueError(
            f"Eurostat normalized {dataset_count} datasets; expected "
            f"{expected_dataset_count}"
        )
    table_counts = {
        "datasets": dataset_count,
        "dimension_values": _table_count(
            connection,
            tables.EUROSTAT_DIMENSION_VALUES_TABLE,
        ),
        "series": _table_count(connection, tables.EUROSTAT_SERIES_TABLE),
        "series_dimensions": _table_count(
            connection,
            tables.EUROSTAT_SERIES_DIMENSIONS_TABLE,
        ),
        "observations": _table_count(
            connection,
            tables.EUROSTAT_OBSERVATIONS_TABLE,
        ),
    }
    empty_tables = [name for name, count in table_counts.items() if count == 0]
    if empty_tables:
        raise ValueError(
            "Eurostat normalized tables are empty: " + ", ".join(empty_tables)
        )

    duplicate_observations = int(
        connection.execute(
            f"""
            select count(*)
            from (
              select dataset_code, series_key, time_period
              from {_qualified(tables.EUROSTAT_OBSERVATIONS_TABLE)}
              group by dataset_code, series_key, time_period
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_observations > 0:
        raise ValueError(
            f"Eurostat normalized data has {duplicate_observations} duplicate "
            "observations"
        )

    missing_dimension_values = int(
        connection.execute(
            f"""
            select count(*)
            from {_qualified(tables.EUROSTAT_SERIES_DIMENSIONS_TABLE)} as dimensions
            left join {_qualified(tables.EUROSTAT_DIMENSION_VALUES_TABLE)} as values
              using (dataset_code, dimension_code, value_code)
            where values.value_code is null
            """
        ).fetchone()[0]
    )
    if missing_dimension_values > 0:
        raise ValueError(
            f"Eurostat normalized series have {missing_dimension_values} "
            "dimension values absent from the SDMX codelists"
        )

    observation_summary = connection.execute(
        f"""
        select
          count(distinct geo_code),
          count_if(status <> ''),
          count_if(status <> '' and value is null),
          min(year),
          max(year),
          count_if(year < {source.EUROSTAT_START_YEAR})
        from {_qualified(tables.EUROSTAT_OBSERVATIONS_TABLE)}
        """
    ).fetchone()
    if int(observation_summary[5]) > 0:
        raise ValueError(
            f"Eurostat normalized observations contain years before "
            f"{source.EUROSTAT_START_YEAR}"
        )
    return {
        **table_counts,
        "geographies": int(observation_summary[0]),
        "flagged_observations": int(observation_summary[1]),
        "flagged_missing_observations": int(observation_summary[2]),
        "min_year": int(observation_summary[3]),
        "max_year": int(observation_summary[4]),
    }


def _clear_snapshot_tables(connection: duckdb.DuckDBPyConnection) -> None:
    for table_name in (
        tables.EUROSTAT_OBSERVATIONS_TABLE,
        tables.EUROSTAT_SERIES_DIMENSIONS_TABLE,
        tables.EUROSTAT_SERIES_TABLE,
        tables.EUROSTAT_DIMENSION_VALUES_TABLE,
        tables.EUROSTAT_DATASETS_TABLE,
    ):
        connection.execute(f"delete from {_qualified(table_name)}")


def _validate_contracts(connection: duckdb.DuckDBPyConnection) -> None:
    for table_name, (_, contract) in tables.EUROSTAT_TABLE_CONTRACTS.items():
        validate_duckdb_table_contract(
            connection,
            schema=tables.EUROSTAT_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def _table_count(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    return int(
        connection.execute(f"select count(*) from {_qualified(table_name)}").fetchone()[
            0
        ]
    )


def _manifest_file_entry(
    dataset_entry: dict[str, Any],
    kind: str,
    dataset_code: str,
) -> dict[str, Any]:
    file_entry = dataset_entry.get(kind)
    if not isinstance(file_entry, dict):
        raise ValueError(
            f"Eurostat dataset {dataset_code} manifest has no {kind} object"
        )
    object_key = str(file_entry.get("object_key") or "")
    expected_hash = str(file_entry.get("sha256") or "")
    if object_key == "" or len(expected_hash) != 64:
        raise ValueError(
            f"Eurostat dataset {dataset_code} {kind} object is missing key or hash"
        )
    return file_entry


def _download_and_verify(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
    expected_hash: str,
    target_path: Path,
) -> None:
    object_store.download_file(
        object_key,
        target_path,
        bucket=source.EUROSTAT_RAW_BUCKET,
    )
    actual_hash = _file_sha256(target_path)
    if actual_hash != expected_hash:
        raise ValueError(
            f"Eurostat S3 object {object_key} hash mismatch: "
            f"expected {expected_hash}, got {actual_hash}"
        )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while chunk := file_handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_manifest_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"Eurostat manifest has invalid retrieval timestamp {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError("Eurostat manifest retrieval timestamp has no timezone")
    return parsed.astimezone(UTC)


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _qualified(table_name: str) -> str:
    return (
        f"{_quote_identifier(tables.EUROSTAT_DUCKDB_SCHEMA)}."
        f"{_quote_identifier(table_name)}"
    )


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
