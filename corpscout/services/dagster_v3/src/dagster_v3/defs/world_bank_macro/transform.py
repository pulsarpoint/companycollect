from __future__ import annotations

import hashlib
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import zipfile

import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.duckdb.schema_contract import (
    create_duckdb_table_from_contract,
    validate_duckdb_table_contract,
)
from dagster_v3.defs.world_bank_macro import tables
from dagster_v3.defs.world_bank_macro.source import (
    INDICATOR_BUNDLES,
    WORLD_BANK_RAW_BUCKET,
)


@dataclass(frozen=True)
class LocalObservationFile:
    bundle: str
    path: Path
    source_url: str
    source_object_key: str
    source_payload_hash: str


@dataclass(frozen=True)
class LocalSnapshot:
    country_catalog_path: Path
    observation_files: tuple[LocalObservationFile, ...]
    source_run_id: str
    pulled_at: str
    start_year: int
    end_year: int


@contextmanager
def local_snapshot_files(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
) -> Iterator[LocalSnapshot]:
    files = manifest.get("files")
    if not isinstance(files, list):
        raise ValueError("World Bank snapshot manifest has no files list")

    with tempfile.TemporaryDirectory(prefix="world_bank_duckdb_input_") as temp_dir:
        directory = Path(temp_dir)
        country_catalog_path: Path | None = None
        observations: list[LocalObservationFile] = []

        for index, file_entry in enumerate(files):
            if not isinstance(file_entry, dict):
                raise ValueError("World Bank snapshot file entry is not an object")
            object_key = str(file_entry.get("object_key") or "")
            expected_hash = str(file_entry.get("sha256") or "")
            kind = str(file_entry.get("kind") or "")
            if object_key == "" or len(expected_hash) != 64:
                raise ValueError(
                    "World Bank snapshot file is missing object key or hash"
                )

            stored_path = directory / f"stored_{index}{Path(object_key).suffix}"
            object_store.download_file(
                object_key,
                stored_path,
                bucket=WORLD_BANK_RAW_BUCKET,
            )
            actual_hash = _file_sha256(stored_path)
            if actual_hash != expected_hash:
                raise ValueError(
                    f"World Bank S3 object {object_key} hash mismatch: "
                    f"expected {expected_hash}, got {actual_hash}"
                )

            if kind == "country_catalog":
                if country_catalog_path is not None:
                    raise ValueError(
                        "World Bank snapshot contains multiple country catalogs"
                    )
                country_catalog_path = stored_path
                continue
            if kind != "observations":
                raise ValueError(f"Unknown World Bank snapshot file kind {kind!r}")

            bundle = str(file_entry.get("bundle") or "")
            data_member = _data_csv_member(file_entry)
            csv_path = directory / f"observations_{bundle}.csv"
            with zipfile.ZipFile(stored_path) as archive:
                with (
                    archive.open(data_member) as source_file,
                    csv_path.open("wb") as target,
                ):
                    shutil.copyfileobj(source_file, target)
            observations.append(
                LocalObservationFile(
                    bundle=bundle,
                    path=csv_path,
                    source_url=str(file_entry.get("source_url") or ""),
                    source_object_key=object_key,
                    source_payload_hash=expected_hash,
                )
            )

        if country_catalog_path is None:
            raise ValueError("World Bank snapshot contains no country catalog")
        if len(observations) != len(INDICATOR_BUNDLES):
            raise ValueError(
                f"World Bank snapshot contains {len(observations)} observation archives, "
                f"expected {len(INDICATOR_BUNDLES)}"
            )

        yield LocalSnapshot(
            country_catalog_path=country_catalog_path,
            observation_files=tuple(sorted(observations, key=lambda item: item.bundle)),
            source_run_id=str(manifest.get("run_id") or ""),
            pulled_at=str(manifest.get("retrieved_at") or ""),
            start_year=int(manifest.get("start_year") or 0),
            end_year=int(manifest.get("end_year") or 0),
        )


def ensure_world_bank_duckdb_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(f"create schema if not exists {tables.WORLD_BANK_DUCKDB_SCHEMA}")
    create_duckdb_table_from_contract(
        connection,
        schema=tables.WORLD_BANK_DUCKDB_SCHEMA,
        table=tables.WORLD_BANK_MACRO_TABLE,
        contract=tables.WORLD_BANK_DUCKDB_CONTRACT,
    )


def replace_world_bank_macro_observations(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
    minimum_country_count: int,
) -> dict[str, int | str]:
    if minimum_country_count <= 0:
        raise ValueError("minimum_country_count must be positive")
    if local_snapshot.source_run_id == "" or local_snapshot.pulled_at == "":
        raise ValueError("World Bank snapshot is missing run or retrieval metadata")

    connection.execute("begin transaction")
    try:
        ensure_world_bank_duckdb_schema(connection)
        _create_country_catalog(
            connection=connection,
            country_catalog_path=local_snapshot.country_catalog_path,
        )
        discovered_country_count = int(
            connection.execute(
                "select count(*) from world_bank_countries where is_country"
            ).fetchone()[0]
        )
        if discovered_country_count < minimum_country_count:
            raise ValueError(
                f"World Bank catalog contains {discovered_country_count} countries; "
                f"expected at least {minimum_country_count}"
            )

        _create_source_file_catalog(
            connection=connection,
            local_snapshot=local_snapshot,
        )
        _create_indicator_registry(connection)
        _create_source_rows(
            connection=connection,
            observation_files=local_snapshot.observation_files,
        )
        _validate_source_indicators(connection)

        qualified_table = (
            f"{tables.WORLD_BANK_DUCKDB_SCHEMA}.{tables.WORLD_BANK_MACRO_TABLE}"
        )
        connection.execute(f"delete from {qualified_table}")
        connection.execute(
            f"""
            insert into {qualified_table} ({", ".join(tables.WORLD_BANK_MACRO_COLUMNS)})
            select
                countries.country_code,
                countries.country_iso3,
                countries.country_name,
                countries.region,
                countries.income_group,
                source_rows.indicator_code,
                source_rows.indicator_name,
                source_rows.year,
                source_rows.value,
                'world_bank' as source,
                'WDI' as source_dataset,
                source_files.source_updated_date,
                source_files.source_url,
                source_files.source_object_key,
                source_files.source_payload_hash,
                source_files.source_run_id,
                source_files.pulled_at
            from world_bank_source_rows as source_rows
            inner join world_bank_countries as countries
              on countries.country_iso3 = source_rows.country_iso3
             and countries.is_country
            inner join world_bank_source_files as source_files
              on source_files.filename = source_rows.filename
            inner join world_bank_indicators as indicators
              on indicators.indicator_code = source_rows.indicator_code
            where source_rows.year between ? and ?
              and source_rows.value is not null
              and isfinite(source_rows.value)
            """,
            [local_snapshot.start_year, local_snapshot.end_year],
        )
        counts = _validate_normalized_table(
            connection=connection,
            qualified_table=qualified_table,
            discovered_country_count=discovered_country_count,
        )
        validate_duckdb_table_contract(
            connection,
            schema=tables.WORLD_BANK_DUCKDB_SCHEMA,
            table=tables.WORLD_BANK_MACRO_TABLE,
            contract=tables.WORLD_BANK_DUCKDB_CONTRACT,
        )
        connection.execute("commit")
        return counts
    except Exception:
        connection.execute("rollback")
        raise


def _create_country_catalog(
    *,
    connection: duckdb.DuckDBPyConnection,
    country_catalog_path: Path,
) -> None:
    connection.execute(
        """
        create or replace temp table world_bank_countries as
        with document as (
          select json(content) as payload
          from read_text(?)
        ), entries as (
          select item.value as entry
          from document,
               json_each(json_extract(document.payload, '$[1]')) as item
        )
        select
          lower(trim(json_extract_string(entry, '$.iso2Code'))) as country_code,
          upper(trim(json_extract_string(entry, '$.id'))) as country_iso3,
          trim(json_extract_string(entry, '$.name')) as country_name,
          coalesce(trim(json_extract_string(entry, '$.region.value')), '') as region,
          coalesce(trim(json_extract_string(entry, '$.incomeLevel.value')), '') as income_group,
          json_extract_string(entry, '$.region.id') <> 'NA' as is_country
        from entries
        where regexp_matches(json_extract_string(entry, '$.iso2Code'), '^[A-Za-z]{2}$')
          and regexp_matches(json_extract_string(entry, '$.id'), '^[A-Za-z]{3}$')
        """,
        [str(country_catalog_path)],
    )
    duplicates = int(
        connection.execute(
            """
            select count(*)
            from (
              select country_code
              from world_bank_countries
              where is_country
              group by country_code
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicates > 0:
        raise ValueError("World Bank country catalog contains duplicate ISO-2 codes")


def _create_source_file_catalog(
    *,
    connection: duckdb.DuckDBPyConnection,
    local_snapshot: LocalSnapshot,
) -> None:
    connection.execute(
        """
        create or replace temp table world_bank_source_files (
          filename varchar not null,
          bundle varchar not null,
          source_url varchar not null,
          source_object_key varchar not null,
          source_payload_hash varchar not null,
          source_run_id varchar not null,
          pulled_at timestamp not null,
          source_updated_date date not null
        )
        """
    )
    source_dates: set[str] = set()
    rows: list[tuple[Any, ...]] = []
    for observation_file in local_snapshot.observation_files:
        source_updated_date = str(
            connection.execute(
                """
                select regexp_extract(
                  content,
                  '"Last Updated Date","([0-9]{4}-[0-9]{2}-[0-9]{2})"',
                  1
                )
                from read_text(?)
                """,
                [str(observation_file.path)],
            ).fetchone()[0]
        )
        if source_updated_date == "":
            raise ValueError(
                f"World Bank CSV for {observation_file.bundle} has no update date"
            )
        source_dates.add(source_updated_date)
        rows.append(
            (
                str(observation_file.path),
                observation_file.bundle,
                observation_file.source_url,
                observation_file.source_object_key,
                observation_file.source_payload_hash,
                local_snapshot.source_run_id,
                local_snapshot.pulled_at,
                source_updated_date,
            )
        )
    if len(source_dates) != 1:
        raise ValueError(
            "World Bank observation bundles have different source update dates: "
            + ", ".join(sorted(source_dates))
        )
    connection.executemany(
        "insert into world_bank_source_files values (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _create_indicator_registry(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create or replace temp table world_bank_indicators (
          indicator_code varchar primary key
        )
        """
    )
    connection.executemany(
        "insert into world_bank_indicators values (?)",
        [
            (indicator,)
            for bundle in INDICATOR_BUNDLES
            for indicator in bundle.indicators
        ],
    )


def _create_source_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    observation_files: tuple[LocalObservationFile, ...],
) -> None:
    placeholders = ", ".join("?" for _ in observation_files)
    connection.execute(
        f"""
        create or replace temp table world_bank_source_rows as
        select
          upper(trim("Country Code")) as country_iso3,
          trim("Country Name") as country_name,
          trim("Indicator Code") as indicator_code,
          trim("Indicator Name") as indicator_name,
          try_cast(nullif(trim("Year"), '') as usmallint) as year,
          try_cast(nullif(trim("Value"), '') as double) as value,
          filename
        from read_csv(
          [{placeholders}],
          header = true,
          skip = 4,
          all_varchar = true,
          union_by_name = true,
          filename = true
        )
        """,
        [str(observation_file.path) for observation_file in observation_files],
    )


def _validate_source_indicators(connection: duckdb.DuckDBPyConnection) -> None:
    unknown_indicators = [
        str(row[0])
        for row in connection.execute(
            """
            select distinct source_rows.indicator_code
            from world_bank_source_rows as source_rows
            left join world_bank_indicators as indicators using (indicator_code)
            where indicators.indicator_code is null
              and source_rows.indicator_code <> ''
            order by source_rows.indicator_code
            """
        ).fetchall()
    ]
    if unknown_indicators:
        raise ValueError(
            "World Bank source returned unrequested indicators: "
            + ", ".join(unknown_indicators)
        )


def _validate_normalized_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    qualified_table: str,
    discovered_country_count: int,
) -> dict[str, int | str]:
    duplicate_count = int(
        connection.execute(
            f"""
            select count(*)
            from (
              select country_code, indicator_code, year
              from {qualified_table}
              group by country_code, indicator_code, year
              having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_count > 0:
        raise ValueError(
            f"World Bank normalized table has {duplicate_count} duplicate keys"
        )

    row = connection.execute(
        f"""
        select
          count(*) as rows,
          count(distinct country_code) as observed_countries,
          count(distinct indicator_code) as indicators,
          min(year) as min_year,
          max(year) as max_year,
          min(source_updated_date) as minimum_source_date,
          max(source_updated_date) as maximum_source_date
        from {qualified_table}
        """
    ).fetchone()
    row_count = int(row[0])
    if row_count == 0:
        raise ValueError("World Bank normalized table has no observations")
    if row[5] != row[6]:
        raise ValueError("World Bank normalized rows have multiple source update dates")
    return {
        "discovered_countries": discovered_country_count,
        "observed_countries": int(row[1]),
        "indicators": int(row[2]),
        "rows": row_count,
        "min_year": int(row[3]),
        "max_year": int(row[4]),
        "source_updated_date": row[5].isoformat(),
    }


def _data_csv_member(file_entry: dict[str, Any]) -> str:
    members = file_entry.get("members")
    if not isinstance(members, list):
        raise ValueError("World Bank observation archive has no members list")
    data_members = [
        str(member)
        for member in members
        if Path(str(member)).name.startswith("API_Download_")
        and Path(str(member)).name.endswith("_LIST.csv")
    ]
    if len(data_members) != 1:
        raise ValueError(
            f"World Bank observation archive has {len(data_members)} data members"
        )
    return data_members[0]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while chunk := file_handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()
