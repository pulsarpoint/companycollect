from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.duckdb.schema_contract import (
    create_duckdb_table_from_contract,
    validate_duckdb_table_contract,
)
from dagster_v3.defs.imf_weo import tables
from dagster_v3.defs.imf_weo.source import IMF_WEO_RAW_BUCKET

IMF_WEO_START_YEAR = 2000
REQUIRED_COUNTRIES_COLUMNS = frozenset(
    {
        "DATASET",
        "SERIES_CODE",
        "COUNTRY.ID",
        "COUNTRY",
        "INDICATOR.ID",
        "INDICATOR",
        "INDICATOR.Description",
        "FREQUENCY",
        "SCALE",
        "UNIT",
        "COUNTRY_UPDATE_DATE",
        "PUBLICATION_DATE",
        "UPDATE_DATE",
        "METHODOLOGY.ID",
        "METHODOLOGY_NOTES",
        "LATEST_ACTUAL_ANNUAL_DATA",
        "HISTORICAL_DATA_SOURCE",
        "BASE_YEAR",
        "START_END_MONTHS_OF_REPORTING_YEAR",
        "CHAIN_WEIGHTED",
        "BASIS_OF_PROJECTIONS",
        "VALUATION",
        "PRICES_SECTOR_HARMONIZED_PRICES",
        "LABOR_SECTOR_EMPLOYMENT_TYPE",
        "FISCAL_SECTOR_GENERAL_GOVERNMENT_COMPOSITION",
        "FISCAL_SECTOR_VALUATION_OF_DEBT",
        "FISCAL_SECTOR_INSTRUMENTS_INCLUDED_IN_GROSS_AND_NET_DEBT",
        "TRADE_SECTOR_OIL_COVERAGE",
        "PRIMARY_DOMESTIC_CURRENCY",
    }
)


@dataclass(frozen=True)
class LocalSnapshot:
    workbook_path: Path
    source_url: str
    source_object_key: str
    source_payload_hash: str
    source_run_id: str
    pulled_at: str


@contextmanager
def local_snapshot_file(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
) -> Iterator[LocalSnapshot]:
    file_entry = manifest.get("file")
    if not isinstance(file_entry, dict):
        raise ValueError("IMF WEO snapshot manifest has no file object")
    object_key = str(file_entry.get("object_key") or "")
    expected_hash = str(file_entry.get("sha256") or "")
    if object_key == "" or len(expected_hash) != 64:
        raise ValueError("IMF WEO snapshot file is missing object key or hash")

    with tempfile.TemporaryDirectory(prefix="imf_weo_duckdb_input_") as temp_dir:
        workbook_path = Path(temp_dir) / "WEO.xlsx"
        object_store.download_file(
            object_key,
            workbook_path,
            bucket=IMF_WEO_RAW_BUCKET,
        )
        actual_hash = _file_sha256(workbook_path)
        if actual_hash != expected_hash:
            raise ValueError(
                f"IMF WEO S3 object {object_key} hash mismatch: "
                f"expected {expected_hash}, got {actual_hash}"
            )

        source_run_id = str(manifest.get("run_id") or "")
        pulled_at = str(manifest.get("retrieved_at") or "")
        if source_run_id == "" or pulled_at == "":
            raise ValueError("IMF WEO snapshot is missing run or retrieval metadata")
        yield LocalSnapshot(
            workbook_path=workbook_path,
            source_url=str(file_entry.get("source_url") or ""),
            source_object_key=object_key,
            source_payload_hash=expected_hash,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )


def ensure_excel_extension_installed() -> None:
    # Extension installation may access DuckDB's extension repository. Use an
    # ephemeral connection so the persistent IMF database is not locked while
    # any network or filesystem setup occurs.
    with duckdb.connect(":memory:") as connection:
        connection.execute("install excel")


def ensure_imf_weo_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.IMF_WEO_DUCKDB_SCHEMA}")
    for table_name, contract in (
        (tables.IMF_WEO_VINTAGES_TABLE, tables.IMF_WEO_VINTAGES_CONTRACT),
        (tables.IMF_WEO_SERIES_TABLE, tables.IMF_WEO_SERIES_CONTRACT),
        (tables.IMF_WEO_OBSERVATIONS_TABLE, tables.IMF_WEO_OBSERVATIONS_CONTRACT),
    ):
        create_duckdb_table_from_contract(
            connection,
            schema=tables.IMF_WEO_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def replace_imf_weo_vintage(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
    minimum_country_count: int,
    minimum_indicator_count: int,
) -> dict[str, int | str]:
    if minimum_country_count <= 0 or minimum_indicator_count <= 0:
        raise ValueError(
            "IMF WEO minimum country and indicator counts must be positive"
        )

    connection.execute("load excel")
    connection.execute("begin transaction")
    try:
        ensure_imf_weo_duckdb_schema(connection)
        _create_source_table(connection, local_snapshot.workbook_path)
        year_columns = _source_year_columns(connection)
        _validate_source_table(
            connection=connection,
            year_columns=year_columns,
            minimum_country_count=minimum_country_count,
            minimum_indicator_count=minimum_indicator_count,
        )
        _create_normalized_series_source(connection)
        vintage_date = _source_vintage_date(connection)
        _replace_vintage(
            connection=connection,
            local_snapshot=local_snapshot,
            vintage_date=vintage_date,
            year_columns=year_columns,
        )
        counts = _validate_vintage(
            connection=connection,
            vintage_date=vintage_date,
        )
        _validate_contracts(connection)
        connection.execute("commit")
        return counts
    except Exception:
        connection.execute("rollback")
        raise


def _create_source_table(
    connection: duckdb.DuckDBPyConnection,
    workbook_path: Path,
) -> None:
    connection.execute(
        """
        create or replace temp table imf_weo_source as
        select *
        from read_xlsx(
          ?,
          sheet = 'Countries',
          header = true,
          all_varchar = true
        )
        """,
        [str(workbook_path)],
    )


def _source_year_columns(connection: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    columns = tuple(
        str(row[0]) for row in connection.execute("describe imf_weo_source").fetchall()
    )
    missing = sorted(REQUIRED_COUNTRIES_COLUMNS.difference(columns))
    if missing:
        raise ValueError(
            "IMF WEO Countries sheet is missing columns: " + ", ".join(missing)
        )
    year_columns = tuple(
        column
        for column in columns
        if column.isdigit() and int(column) >= IMF_WEO_START_YEAR
    )
    if not year_columns:
        raise ValueError(
            f"IMF WEO Countries sheet has no year columns from {IMF_WEO_START_YEAR}"
        )
    expected_years = tuple(
        str(year) for year in range(int(year_columns[0]), int(year_columns[-1]) + 1)
    )
    if year_columns != expected_years:
        raise ValueError("IMF WEO Countries sheet has non-contiguous year columns")
    return year_columns


def _validate_source_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    year_columns: tuple[str, ...],
    minimum_country_count: int,
    minimum_indicator_count: int,
) -> None:
    source_counts = connection.execute(
        """
        select
          count(*) as series,
          count(distinct upper(trim("COUNTRY.ID"))) as countries,
          count(distinct trim("INDICATOR.ID")) as indicators,
          count_if(not regexp_matches(trim("COUNTRY.ID"), '^[A-Z]{3}$')) as bad_countries,
          count_if(trim("FREQUENCY") <> 'Annual') as non_annual
        from imf_weo_source
        """
    ).fetchone()
    if int(source_counts[1]) < minimum_country_count:
        raise ValueError(
            f"IMF WEO workbook contains {source_counts[1]} countries; "
            f"expected at least {minimum_country_count}"
        )
    if int(source_counts[2]) < minimum_indicator_count:
        raise ValueError(
            f"IMF WEO workbook contains {source_counts[2]} indicators; "
            f"expected at least {minimum_indicator_count}"
        )
    if int(source_counts[3]) > 0:
        raise ValueError(
            f"IMF WEO workbook contains {source_counts[3]} invalid country codes"
        )
    if int(source_counts[4]) > 0:
        raise ValueError(
            f"IMF WEO workbook contains {source_counts[4]} non-annual series"
        )

    duplicate_series = int(
        connection.execute(
            """
            select count(*)
            from (
              select "COUNTRY.ID", "INDICATOR.ID"
              from imf_weo_source
              group by "COUNTRY.ID", "INDICATOR.ID"
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_series > 0:
        raise ValueError(
            f"IMF WEO workbook contains {duplicate_series} duplicate country series"
        )

    unsupported_scales = [
        str(row[0])
        for row in connection.execute(
            """
            select distinct trim("SCALE")
            from imf_weo_source
            where nullif(trim("SCALE"), '') is not null
              and trim("SCALE") not in ('Billions', 'Millions', 'Thousands', 'Units')
            order by trim("SCALE")
            """
        ).fetchall()
    ]
    if unsupported_scales:
        raise ValueError(
            "IMF WEO workbook contains unsupported scales: "
            + ", ".join(unsupported_scales)
        )

    quoted_years = ", ".join(_quote_identifier(year) for year in year_columns)
    malformed_values = int(
        connection.execute(
            f"""
            with observations as (
              unpivot imf_weo_source
              on {quoted_years}
              into name source_year value source_value
            )
            select count(*)
            from observations
            where nullif(trim(source_value), '') is not null
              and try_cast(replace(trim(source_value), ',', '') as double) is null
            """
        ).fetchone()[0]
    )
    if malformed_values > 0:
        raise ValueError(
            f"IMF WEO workbook contains {malformed_values} malformed observations"
        )


def _create_normalized_series_source(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create or replace temp table imf_weo_series_source as
        select
          cast(try_cast("PUBLICATION_DATE" as timestamp) as date) as vintage_date,
          trim("SERIES_CODE") as series_code,
          upper(trim("COUNTRY.ID")) as country_iso3,
          trim("COUNTRY") as country_name,
          trim("INDICATOR.ID") as indicator_code,
          trim("INDICATOR") as indicator_name,
          coalesce(trim("INDICATOR.Description"), '') as indicator_description,
          trim("FREQUENCY") as frequency,
          nullif(trim("SCALE"), '') as scale,
          nullif(trim("UNIT"), '') as unit,
          try_cast("COUNTRY_UPDATE_DATE" as date) as country_update_date,
          nullif(trim("METHODOLOGY.ID"), '') as methodology,
          nullif(trim("METHODOLOGY_NOTES"), '') as methodology_notes,
          nullif(try_cast("LATEST_ACTUAL_ANNUAL_DATA" as usmallint), 0) as latest_actual_year,
          nullif(trim("HISTORICAL_DATA_SOURCE"), '') as historical_data_source,
          nullif(trim("BASE_YEAR"), '') as base_year,
          nullif(trim("START_END_MONTHS_OF_REPORTING_YEAR"), '') as reporting_year_months,
          nullif(trim("CHAIN_WEIGHTED"), '') as chain_weighted,
          nullif(trim("BASIS_OF_PROJECTIONS"), '') as basis_of_projections,
          nullif(trim("VALUATION"), '') as valuation,
          nullif(trim("PRICES_SECTOR_HARMONIZED_PRICES"), '') as harmonized_prices,
          nullif(trim("LABOR_SECTOR_EMPLOYMENT_TYPE"), '') as employment_type,
          nullif(trim("FISCAL_SECTOR_GENERAL_GOVERNMENT_COMPOSITION"), '')
            as government_composition,
          nullif(trim("FISCAL_SECTOR_VALUATION_OF_DEBT"), '') as debt_valuation,
          nullif(trim("FISCAL_SECTOR_INSTRUMENTS_INCLUDED_IN_GROSS_AND_NET_DEBT"), '')
            as debt_instruments,
          nullif(trim("TRADE_SECTOR_OIL_COVERAGE"), '') as oil_coverage,
          nullif(trim("PRIMARY_DOMESTIC_CURRENCY"), '') as primary_domestic_currency
        from imf_weo_source
        """
    )


def _source_vintage_date(connection: duckdb.DuckDBPyConnection) -> Any:
    row = connection.execute(
        """
        select
          min(try_cast("PUBLICATION_DATE" as timestamp)),
          max(try_cast("PUBLICATION_DATE" as timestamp)),
          min(try_cast("UPDATE_DATE" as timestamp)),
          max(try_cast("UPDATE_DATE" as timestamp)),
          count(distinct trim("DATASET"))
        from imf_weo_source
        """
    ).fetchone()
    if row[0] is None or row[2] is None:
        raise ValueError(
            "IMF WEO workbook has invalid publication or update timestamps"
        )
    if row[0] != row[1] or row[2] != row[3] or int(row[4]) != 1:
        raise ValueError("IMF WEO workbook contains inconsistent vintage metadata")
    return row[0].date()


def _replace_vintage(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
    vintage_date: Any,
    year_columns: tuple[str, ...],
) -> None:
    qualified_vintages = _qualified(tables.IMF_WEO_VINTAGES_TABLE)
    qualified_series = _qualified(tables.IMF_WEO_SERIES_TABLE)
    qualified_observations = _qualified(tables.IMF_WEO_OBSERVATIONS_TABLE)
    connection.execute(
        f"delete from {qualified_observations} where vintage_date = ?", [vintage_date]
    )
    connection.execute(
        f"delete from {qualified_series} where vintage_date = ?", [vintage_date]
    )
    connection.execute(
        f"delete from {qualified_vintages} where vintage_date = ?", [vintage_date]
    )

    connection.execute(
        f"""
        insert into {qualified_vintages} ({", ".join(tables.IMF_WEO_VINTAGES_COLUMNS)})
        select
          cast(try_cast("PUBLICATION_DATE" as timestamp) as date),
          strftime(cast(try_cast("PUBLICATION_DATE" as timestamp) as date), '%Y-%m'),
          regexp_extract(trim("DATASET"), '^([^\\(]+)', 1),
          regexp_extract(trim("DATASET"), '\\(([^\\)]+)\\)', 1),
          try_cast("PUBLICATION_DATE" as timestamp),
          try_cast("UPDATE_DATE" as timestamp),
          ?, ?, ?, ?, try_cast(? as timestamp)
        from imf_weo_source
        limit 1
        """,
        [
            local_snapshot.source_url,
            local_snapshot.source_object_key,
            local_snapshot.source_payload_hash,
            local_snapshot.source_run_id,
            local_snapshot.pulled_at,
        ],
    )
    connection.execute(
        f"""
        insert into {qualified_series} ({", ".join(tables.IMF_WEO_SERIES_COLUMNS)})
        select {", ".join(tables.IMF_WEO_SERIES_COLUMNS)}
        from imf_weo_series_source
        """
    )

    quoted_years = ", ".join(_quote_identifier(year) for year in year_columns)
    connection.execute(
        f"""
        insert into {qualified_observations} (
          {", ".join(tables.IMF_WEO_OBSERVATIONS_COLUMNS)}
        )
        with source_observations as (
          unpivot imf_weo_source
          on {quoted_years}
          into name source_year value source_value
        ), normalized as (
          select
            cast(try_cast("PUBLICATION_DATE" as timestamp) as date) as vintage_date,
            upper(trim("COUNTRY.ID")) as country_iso3,
            trim("INDICATOR.ID") as indicator_code,
            try_cast(source_year as usmallint) as year,
            try_cast(replace(trim(source_value), ',', '') as double) as value,
            nullif(trim("SCALE"), '') as scale,
            nullif(try_cast("LATEST_ACTUAL_ANNUAL_DATA" as usmallint), 0)
              as latest_actual_year
          from source_observations
          where nullif(trim(source_value), '') is not null
        )
        select
          vintage_date,
          country_iso3,
          indicator_code,
          year,
          value,
          value * case scale
            when 'Billions' then 1000000000.0
            when 'Millions' then 1000000.0
            when 'Thousands' then 1000.0
            else 1.0
          end as value_base,
          latest_actual_year is null or year > latest_actual_year as is_estimate
        from normalized
        """
    )


def _validate_vintage(
    *,
    connection: duckdb.DuckDBPyConnection,
    vintage_date: Any,
) -> dict[str, int | str]:
    qualified_series = _qualified(tables.IMF_WEO_SERIES_TABLE)
    qualified_observations = _qualified(tables.IMF_WEO_OBSERVATIONS_TABLE)
    duplicate_observations = int(
        connection.execute(
            f"""
            select count(*)
            from (
              select country_iso3, indicator_code, year
              from {qualified_observations}
              where vintage_date = ?
              group by country_iso3, indicator_code, year
              having count(*) > 1
            )
            """,
            [vintage_date],
        ).fetchone()[0]
    )
    if duplicate_observations > 0:
        raise ValueError(
            f"IMF WEO vintage has {duplicate_observations} duplicate observations"
        )

    series_row = connection.execute(
        f"""
        select
          count(*) as series,
          count(distinct country_iso3) as countries,
          count(distinct indicator_code) as indicators
        from {qualified_series}
        where vintage_date = ?
        """,
        [vintage_date],
    ).fetchone()
    observation_row = connection.execute(
        f"""
        select
          count(*) as observations,
          count_if(is_estimate) as estimates,
          min(year) as min_year,
          max(year) as max_year
        from {qualified_observations}
        where vintage_date = ?
        """,
        [vintage_date],
    ).fetchone()
    if int(series_row[0]) == 0 or int(observation_row[0]) == 0:
        raise ValueError("IMF WEO normalized vintage has no series or observations")
    return {
        "vintage": vintage_date.strftime("%Y-%m"),
        "countries": int(series_row[1]),
        "indicators": int(series_row[2]),
        "series": int(series_row[0]),
        "observations": int(observation_row[0]),
        "estimates": int(observation_row[1]),
        "min_year": int(observation_row[2]),
        "max_year": int(observation_row[3]),
    }


def _validate_contracts(connection: duckdb.DuckDBPyConnection) -> None:
    for table_name, contract in (
        (tables.IMF_WEO_VINTAGES_TABLE, tables.IMF_WEO_VINTAGES_CONTRACT),
        (tables.IMF_WEO_SERIES_TABLE, tables.IMF_WEO_SERIES_CONTRACT),
        (tables.IMF_WEO_OBSERVATIONS_TABLE, tables.IMF_WEO_OBSERVATIONS_CONTRACT),
    ):
        validate_duckdb_table_contract(
            connection,
            schema=tables.IMF_WEO_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )


def _qualified(table: str) -> str:
    return (
        f"{_quote_identifier(tables.IMF_WEO_DUCKDB_SCHEMA)}.{_quote_identifier(table)}"
    )


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while chunk := file_handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()
