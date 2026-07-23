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
from dagster_v3.defs.un_comtrade import source, tables


@dataclass(frozen=True)
class LocalSourceFile:
    path: Path
    source_url: str
    source_object_key: str
    source_object_hash: str
    record_count: int
    year: int | None
    periods: tuple[int, ...]


@dataclass(frozen=True)
class LocalSnapshot:
    annual_totals: tuple[LocalSourceFile, ...]
    availability: tuple[LocalSourceFile, ...]
    source_run_id: str
    pulled_at: datetime
    start_year: int
    end_year: int


@contextmanager
def local_snapshot_files(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
) -> Iterator[LocalSnapshot]:
    source_run_id = str(manifest.get("run_id") or "")
    pulled_at_text = str(manifest.get("retrieved_at") or "")
    if source_run_id == "" or pulled_at_text == "":
        raise ValueError("UN Comtrade snapshot is missing run or retrieval metadata")
    pulled_at = _parse_manifest_timestamp(pulled_at_text)

    start_year = _manifest_year(manifest, "start_year")
    end_year = _manifest_year(manifest, "end_year")
    if start_year > end_year:
        raise ValueError("UN Comtrade manifest start year exceeds end year")
    expected_years = tuple(range(start_year, end_year + 1))
    _validate_manifest_filters(manifest.get("filters"))

    annual_entries = manifest.get("annual_totals")
    if not isinstance(annual_entries, list):
        raise ValueError("UN Comtrade snapshot has no annual_totals list")
    entries_by_year: dict[int, dict[str, Any]] = {}
    for raw_entry in annual_entries:
        if not isinstance(raw_entry, dict):
            raise ValueError(
                "UN Comtrade snapshot has a non-object annual totals entry"
            )
        year = _manifest_year(raw_entry, "year")
        if year in entries_by_year:
            raise ValueError(
                f"UN Comtrade snapshot has duplicate annual totals for {year}"
            )
        entries_by_year[year] = raw_entry
    if tuple(sorted(entries_by_year)) != expected_years:
        raise ValueError(
            "UN Comtrade annual totals do not cover the complete manifest range"
        )

    availability_entries = manifest.get("availability")
    if not isinstance(availability_entries, list):
        raise ValueError("UN Comtrade snapshot has no availability list")
    parsed_availability: list[tuple[tuple[int, ...], dict[str, Any]]] = []
    availability_periods: list[int] = []
    for raw_entry in availability_entries:
        if not isinstance(raw_entry, dict):
            raise ValueError("UN Comtrade snapshot has a non-object availability entry")
        raw_periods = raw_entry.get("periods")
        if not isinstance(raw_periods, list):
            raise ValueError("UN Comtrade availability entry has no periods list")
        periods = tuple(
            _integer_value(item, "availability period") for item in raw_periods
        )
        if len(periods) == 0:
            raise ValueError("UN Comtrade availability entry has no periods")
        parsed_availability.append((periods, raw_entry))
        availability_periods.extend(periods)
    if tuple(sorted(availability_periods)) != expected_years:
        raise ValueError(
            "UN Comtrade availability objects do not cover the complete "
            "manifest range exactly once"
        )

    with tempfile.TemporaryDirectory(prefix="un_comtrade_duckdb_input_") as temp_dir:
        temp_path = Path(temp_dir)
        local_annual_totals: list[LocalSourceFile] = []
        for year in expected_years:
            entry = entries_by_year[year]
            local_path = temp_path / f"annual_totals_{year}.csv"
            local_annual_totals.append(
                _download_manifest_file(
                    object_store=object_store,
                    entry=entry,
                    target_path=local_path,
                    required_columns=source.TOTALS_REQUIRED_COLUMNS,
                    year=year,
                    periods=(),
                )
            )

        local_availability: list[LocalSourceFile] = []
        for index, (periods, entry) in enumerate(parsed_availability):
            local_path = temp_path / f"availability_{index}.csv"
            local_availability.append(
                _download_manifest_file(
                    object_store=object_store,
                    entry=entry,
                    target_path=local_path,
                    required_columns=source.AVAILABILITY_REQUIRED_COLUMNS,
                    year=None,
                    periods=periods,
                )
            )

        yield LocalSnapshot(
            annual_totals=tuple(local_annual_totals),
            availability=tuple(local_availability),
            source_run_id=source_run_id,
            pulled_at=pulled_at,
            start_year=start_year,
            end_year=end_year,
        )


def ensure_un_comtrade_duckdb_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        f"create schema if not exists {tables.UN_COMTRADE_DUCKDB_SCHEMA}"
    )
    for table_name, (_, contract) in tables.UN_COMTRADE_TABLE_CONTRACTS.items():
        create_duckdb_table_from_contract(
            connection,
            schema=tables.UN_COMTRADE_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def replace_un_comtrade_snapshot(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
    minimum_historical_reporters: int,
) -> dict[str, int]:
    if len(local_snapshot.annual_totals) == 0:
        raise ValueError("UN Comtrade local snapshot contains no annual totals")
    if len(local_snapshot.availability) == 0:
        raise ValueError("UN Comtrade local snapshot contains no availability data")
    if minimum_historical_reporters <= 0:
        raise ValueError("UN Comtrade minimum reporter count must be positive")

    connection.execute("begin transaction")
    try:
        ensure_un_comtrade_duckdb_schema(connection)
        _clear_snapshot_tables(connection)
        _normalize_availability(
            connection=connection,
            local_snapshot=local_snapshot,
        )
        _normalize_annual_totals(
            connection=connection,
            local_snapshot=local_snapshot,
        )
        counts = _validate_normalized_snapshot(
            connection=connection,
            local_snapshot=local_snapshot,
            minimum_historical_reporters=minimum_historical_reporters,
        )
        _validate_contracts(connection)
        connection.execute("commit")
        return counts
    except Exception:
        connection.execute("rollback")
        raise


def _normalize_annual_totals(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
) -> None:
    connection.execute(
        """
        create or replace temp table un_comtrade_totals_source_files (
          local_path varchar not null,
          expected_year usmallint not null,
          source_url varchar not null,
          source_object_key varchar not null,
          source_object_hash varchar not null
        )
        """
    )
    connection.executemany(
        """
        insert into un_comtrade_totals_source_files
        values (?, ?, ?, ?, ?)
        """,
        [
            (
                str(item.path),
                item.year,
                item.source_url,
                item.source_object_key,
                item.source_object_hash,
            )
            for item in local_snapshot.annual_totals
        ],
    )
    connection.execute(
        """
        create or replace temp table un_comtrade_raw_totals as
        select
          cast(row_number() over (partition by filename) + 1 as ubigint)
            as source_line_number,
          *
        from read_csv(
          ?,
          header = true,
          all_varchar = true,
          filename = true,
          union_by_name = true,
          strict_mode = true
        )
        """,
        [[str(item.path) for item in local_snapshot.annual_totals]],
    )
    _require_columns(
        connection=connection,
        table_name="un_comtrade_raw_totals",
        required_columns=source.TOTALS_REQUIRED_COLUMNS | {"filename"},
    )
    invalid_filters = int(
        connection.execute(
            """
            select count(*)
            from un_comtrade_raw_totals as raw
            inner join un_comtrade_totals_source_files as files
              on raw.filename = files.local_path
            where trim(raw."typeCode") <> 'C'
               or trim(raw."freqCode") <> 'A'
               or cast(trim(raw."refYear") as integer) <> files.expected_year
               or cast(trim(raw."period") as integer) <> files.expected_year
               or trim(raw."flowCode") not in ('M', 'X')
               or cast(trim(raw."partnerCode") as integer) <> 0
               or cast(trim(raw."partner2Code") as integer) <> 0
               or trim(raw."classificationSearchCode") <> 'HS'
               or not cast(trim(raw."isOriginalClassification") as boolean)
               or trim(raw."cmdCode") <> 'TOTAL'
               or cast(trim(raw."aggrLevel") as integer) <> 0
               or trim(raw."customsCode") <> 'C00'
               or cast(trim(raw."motCode") as integer) <> 0
            """
        ).fetchone()[0]
    )
    if invalid_filters > 0:
        raise ValueError(
            f"UN Comtrade source has {invalid_filters} annual-total rows "
            "outside the requested total/world filters"
        )

    connection.execute(
        f"""
        insert into {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
        ({", ".join(tables.UN_COMTRADE_ANNUAL_TOTALS_COLUMNS)})
        select
          cast(trim(raw."refYear") as usmallint),
          cast(trim(raw."reporterCode") as usmallint),
          trim(raw."reporterISO"),
          trim(raw."reporterDesc"),
          trim(raw."flowCode"),
          trim(raw."flowDesc"),
          trim(raw."classificationCode"),
          trim(raw."classificationSearchCode"),
          cast(trim(raw."isOriginalClassification") as boolean),
          cast(trim(raw."primaryValue") as decimal(38, 3)),
          cast(nullif(trim(raw."cifvalue"), '') as decimal(38, 3)),
          cast(nullif(trim(raw."fobvalue"), '') as decimal(38, 3)),
          cast(trim(raw."legacyEstimationFlag") as smallint),
          cast(trim(raw."isReported") as boolean),
          cast(trim(raw."isAggregate") as boolean),
          files.source_url,
          files.source_object_key,
          files.source_object_hash,
          ?,
          raw.source_line_number,
          cast(? as timestamp)
        from un_comtrade_raw_totals as raw
        inner join un_comtrade_totals_source_files as files
          on raw.filename = files.local_path
        """,
        [
            local_snapshot.source_run_id,
            _utc_naive(local_snapshot.pulled_at),
        ],
    )


def _normalize_availability(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
) -> None:
    connection.execute(
        """
        create or replace temp table un_comtrade_availability_source_files (
          local_path varchar not null,
          source_url varchar not null,
          source_object_key varchar not null,
          source_object_hash varchar not null
        )
        """
    )
    connection.executemany(
        """
        insert into un_comtrade_availability_source_files
        values (?, ?, ?, ?)
        """,
        [
            (
                str(item.path),
                item.source_url,
                item.source_object_key,
                item.source_object_hash,
            )
            for item in local_snapshot.availability
        ],
    )
    connection.execute(
        """
        create or replace temp table un_comtrade_raw_availability as
        select
          cast(row_number() over (partition by filename) + 1 as ubigint)
            as source_line_number,
          *
        from read_csv(
          ?,
          header = true,
          all_varchar = true,
          filename = true,
          union_by_name = true,
          strict_mode = true
        )
        """,
        [[str(item.path) for item in local_snapshot.availability]],
    )
    _require_columns(
        connection=connection,
        table_name="un_comtrade_raw_availability",
        required_columns=source.AVAILABILITY_REQUIRED_COLUMNS | {"filename"},
    )
    invalid_filters = int(
        connection.execute(
            """
            select count(*)
            from un_comtrade_raw_availability
            where trim("TypeCode") <> 'C'
               or trim("FreqCode") <> 'A'
               or trim("ClassificationSearchCode") <> 'HS'
               or not cast(trim("IsOriginalClassification") as boolean)
            """
        ).fetchone()[0]
    )
    if invalid_filters > 0:
        raise ValueError(
            f"UN Comtrade source has {invalid_filters} availability rows "
            "outside the annual original-HS scope"
        )

    connection.execute(
        f"""
        insert into {_qualified(tables.UN_COMTRADE_AVAILABILITY_TABLE)}
        ({", ".join(tables.UN_COMTRADE_AVAILABILITY_COLUMNS)})
        select
          trim(raw."DatasetCode"),
          cast(trim(raw."Period") as usmallint),
          cast(trim(raw."ReporterCode") as usmallint),
          trim(raw."ReporterISO"),
          trim(raw."ReporterDesc"),
          trim(raw."ClassificationCode"),
          trim(raw."ClassificationSearchCode"),
          cast(trim(raw."IsOriginalClassification") as boolean),
          cast(trim(raw."IsExtendedFlowCode") as boolean),
          cast(trim(raw."IsExtendedPartnerCode") as boolean),
          cast(trim(raw."IsExtendedPartner2Code") as boolean),
          cast(trim(raw."IsExtendedCmdCode") as boolean),
          cast(trim(raw."IsExtendedCustomsCode") as boolean),
          cast(trim(raw."IsExtendedMotCode") as boolean),
          cast(trim(raw."TotalRecords") as ubigint),
          trim(raw."DatasetChecksum"),
          strptime(
            trim(raw."FirstReleased"),
            ['%m/%d/%Y %I:%M:%S %p', '%Y-%m-%dT%H:%M:%S']
          ),
          strptime(
            trim(raw."LastReleased"),
            ['%m/%d/%Y %I:%M:%S %p', '%Y-%m-%dT%H:%M:%S']
          ),
          files.source_url,
          files.source_object_key,
          files.source_object_hash,
          ?,
          raw.source_line_number,
          cast(? as timestamp)
        from un_comtrade_raw_availability as raw
        inner join un_comtrade_availability_source_files as files
          on raw.filename = files.local_path
        """,
        [
            local_snapshot.source_run_id,
            _utc_naive(local_snapshot.pulled_at),
        ],
    )


def _validate_normalized_snapshot(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
    minimum_historical_reporters: int,
) -> dict[str, int]:
    availability_rows = _table_count(
        connection,
        tables.UN_COMTRADE_AVAILABILITY_TABLE,
    )
    annual_total_rows = _table_count(
        connection,
        tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE,
    )
    expected_availability_rows = sum(
        item.record_count for item in local_snapshot.availability
    )
    expected_total_rows = sum(
        item.record_count for item in local_snapshot.annual_totals
    )
    if availability_rows != expected_availability_rows:
        raise ValueError(
            f"UN Comtrade normalized {availability_rows} availability rows; "
            f"expected {expected_availability_rows}"
        )
    if annual_total_rows != expected_total_rows:
        raise ValueError(
            f"UN Comtrade normalized {annual_total_rows} annual totals; "
            f"expected {expected_total_rows}"
        )

    duplicate_availability = int(
        connection.execute(
            f"""
            select count(*)
            from (
              select dataset_code
              from {_qualified(tables.UN_COMTRADE_AVAILABILITY_TABLE)}
              group by dataset_code
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_availability > 0:
        raise ValueError(
            f"UN Comtrade normalized data has {duplicate_availability} "
            "duplicate availability datasets"
        )
    duplicate_totals = int(
        connection.execute(
            f"""
            select count(*)
            from (
              select reporter_code, year, flow_code
              from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
              group by reporter_code, year, flow_code
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_totals > 0:
        raise ValueError(
            f"UN Comtrade normalized data has {duplicate_totals} "
            "duplicate annual totals"
        )

    invalid_identity_rows = int(
        connection.execute(
            f"""
            select count(*)
            from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
            where length(reporter_iso) <> 3
               or reporter_name = ''
               or classification_code = ''
               or primary_value_usd < 0
            """
        ).fetchone()[0]
    )
    if invalid_identity_rows > 0:
        raise ValueError(
            f"UN Comtrade normalized data has {invalid_identity_rows} invalid "
            "country identities or trade values"
        )

    totals_without_availability = int(
        connection.execute(
            f"""
            select count(*)
            from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)} as totals
            left join {_qualified(tables.UN_COMTRADE_AVAILABILITY_TABLE)}
              as availability
              on availability.year = totals.year
             and availability.reporter_code = totals.reporter_code
             and availability.classification_code = totals.classification_code
            where availability.dataset_code is null
            """
        ).fetchone()[0]
    )
    if totals_without_availability > 0:
        raise ValueError(
            f"UN Comtrade normalized data has {totals_without_availability} "
            "annual totals without data-availability metadata"
        )

    expected_years = tuple(
        range(local_snapshot.start_year, local_snapshot.end_year + 1)
    )
    actual_years = tuple(
        int(row[0])
        for row in connection.execute(
            f"""
            select distinct year
            from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
            order by year
            """
        ).fetchall()
    )
    if actual_years != expected_years:
        raise ValueError(
            f"UN Comtrade annual totals cover years {actual_years}; expected "
            f"{expected_years}"
        )

    reporters_by_year = {
        int(year): int(reporter_count)
        for year, reporter_count in connection.execute(
            f"""
            select year, count(distinct reporter_code)
            from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
            group by year
            order by year
            """
        ).fetchall()
    }
    sparse_historical_years = {
        year: count
        for year, count in reporters_by_year.items()
        if year < local_snapshot.end_year - 1 and count < minimum_historical_reporters
    }
    if sparse_historical_years:
        raise ValueError(
            "UN Comtrade historical years have too few reporters: "
            + ", ".join(
                f"{year}={count}" for year, count in sparse_historical_years.items()
            )
        )

    summary = connection.execute(
        f"""
        select
          count(distinct reporter_code),
          count(distinct year),
          count_if(flow_code = 'M'),
          count_if(flow_code = 'X'),
          min(year),
          max(year)
        from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}
        """
    ).fetchone()
    return {
        "availability_rows": availability_rows,
        "annual_total_rows": annual_total_rows,
        "reporters": int(summary[0]),
        "years": int(summary[1]),
        "import_rows": int(summary[2]),
        "export_rows": int(summary[3]),
        "min_year": int(summary[4]),
        "max_year": int(summary[5]),
        "latest_year_reporters": reporters_by_year[local_snapshot.end_year],
    }


def _clear_snapshot_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"delete from {_qualified(tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE)}"
    )
    connection.execute(
        f"delete from {_qualified(tables.UN_COMTRADE_AVAILABILITY_TABLE)}"
    )


def _validate_contracts(connection: duckdb.DuckDBPyConnection) -> None:
    for table_name, (_, contract) in tables.UN_COMTRADE_TABLE_CONTRACTS.items():
        validate_duckdb_table_contract(
            connection,
            schema=tables.UN_COMTRADE_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def _download_manifest_file(
    *,
    object_store: ObjectStoreResource,
    entry: dict[str, Any],
    target_path: Path,
    required_columns: frozenset[str],
    year: int | None,
    periods: tuple[int, ...],
) -> LocalSourceFile:
    source_url = str(entry.get("source_url") or "")
    object_key = str(entry.get("object_key") or "")
    expected_hash = str(entry.get("sha256") or "")
    record_count = _integer_value(entry.get("record_count"), "record_count")
    if (
        source_url == ""
        or object_key == ""
        or len(expected_hash) != 64
        or record_count <= 0
    ):
        raise ValueError(
            "UN Comtrade manifest file is missing URL, key, hash, or record count"
        )
    object_store.download_file(
        object_key,
        target_path,
        bucket=source.UN_COMTRADE_RAW_BUCKET,
    )
    actual_hash = _file_sha256(target_path)
    if actual_hash != expected_hash:
        raise ValueError(
            f"UN Comtrade S3 object {object_key} hash mismatch: "
            f"expected {expected_hash}, got {actual_hash}"
        )
    actual_record_count = source._validate_csv(
        target_path,
        required_columns=required_columns,
    )
    if actual_record_count != record_count:
        raise ValueError(
            f"UN Comtrade S3 object {object_key} has {actual_record_count} "
            f"records; manifest declares {record_count}"
        )
    return LocalSourceFile(
        path=target_path,
        source_url=source_url,
        source_object_key=object_key,
        source_object_hash=expected_hash,
        record_count=record_count,
        year=year,
        periods=periods,
    )


def _validate_manifest_filters(value: object) -> None:
    expected = {
        "type_code": "C",
        "frequency_code": "A",
        "classification_search_code": "HS",
        "command_code": "TOTAL",
        "flow_codes": ["M", "X"],
        "partner_code": 0,
        "partner2_code": 0,
        "customs_code": "C00",
        "mode_of_transport_code": 0,
    }
    if value != expected:
        raise ValueError(
            "UN Comtrade manifest filters do not match the annual "
            "total/world source contract"
        )


def _require_columns(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    required_columns: frozenset[str],
) -> None:
    actual_columns = {
        str(row[0]) for row in connection.execute(f"describe {table_name}").fetchall()
    }
    missing_columns = sorted(required_columns - actual_columns)
    if missing_columns:
        raise ValueError(
            f"UN Comtrade DuckDB source {table_name} is missing columns: "
            f"{', '.join(missing_columns)}"
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


def _manifest_year(value: dict[str, Any], field: str) -> int:
    return _integer_value(value.get(field), field)


def _integer_value(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"UN Comtrade manifest {field} is not an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"UN Comtrade manifest {field} is not an integer") from exc


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
            f"UN Comtrade manifest has invalid retrieval timestamp {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError("UN Comtrade manifest retrieval timestamp has no timezone")
    return parsed.astimezone(UTC)


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _qualified(table_name: str) -> str:
    return (
        f"{_quote_identifier(tables.UN_COMTRADE_DUCKDB_SCHEMA)}."
        f"{_quote_identifier(table_name)}"
    )


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
