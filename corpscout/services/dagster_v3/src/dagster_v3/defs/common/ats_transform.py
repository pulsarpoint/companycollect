from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from dagster_v3.defs.common.ats_source import parse_datetime


@dataclass(frozen=True)
class SnapshotTableNames:
    schema: str
    boards: str
    board_company_links: str
    board_snapshots: str
    versions: str
    events: str
    current: str
    locations: str
    compensations: str


def register_snapshot_files(
    connection: Any,
    snapshot_files: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    rows = [
        {
            **dict(board),
            "configured_at": parse_datetime(str(board["configured_at"])),
            "retrieved_at": parse_datetime(str(board["retrieved_at"])),
        }
        for board in snapshot_files
    ]
    if not rows:
        raise ValueError("ATS snapshot manifest contains no boards")
    connection.register("ats_source_files_arrow", pa.Table.from_pylist(rows))
    connection.execute(
        "create or replace temp table ats_source_files as "
        "select * from ats_source_files_arrow"
    )
    return tuple(str(row["local_path"]) for row in rows)


def replace_snapshot_tables(
    *,
    connection: Any,
    names: SnapshotTableNames,
) -> dict[str, int]:
    _validate_normalized_tables(connection)
    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {names.schema}")
        _replace_registry_tables(connection, names)
        _replace_job_tables(connection, names)
        connection.execute("commit")
    except BaseException:
        connection.execute("rollback")
        raise

    table_names = (
        names.boards,
        names.board_company_links,
        names.board_snapshots,
        names.versions,
        names.events,
        names.current,
        names.locations,
        names.compensations,
    )
    return {
        table: int(
            connection.execute(
                f"select count(*) from {names.schema}.{table}"
            ).fetchone()[0]
        )
        for table in table_names
    }


def _replace_registry_tables(connection: Any, names: SnapshotTableNames) -> None:
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.boards} as
        select
            cast(provider_board_id as varchar) as provider_board_id,
            cast(board_token as varchar) as board_token,
            cast(display_name as varchar) as display_name,
            cast(board_url as varchar) as board_url,
            cast(enabled as utinyint) as enabled,
            cast(configured_at as timestamptz) as configured_at
        from ats_source_files
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.board_company_links} as
        select
            cast(provider_board_id as varchar) as provider_board_id,
            cast(company_id as varchar) as company_id,
            'reviewed_board_owner'::varchar as match_method,
            cast(evidence_url as varchar) as evidence_url,
            cast(configured_at as timestamptz) as reviewed_at
        from ats_source_files
        where company_id is not null and trim(company_id) != ''
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.board_snapshots} as
        select
            sha256(concat_ws('|', provider_board_id, source_run_id,
                source_object_key))
                as snapshot_uid,
            cast(provider_board_id as varchar) as provider_board_id,
            cast(source_run_id as varchar) as source_run_id,
            cast(source_url as varchar) as source_url,
            cast(source_object_key as varchar) as source_object_key,
            cast(retrieved_at as timestamptz) as retrieved_at,
            cast(http_status as usmallint) as http_status,
            cast(job_count as ubigint) as job_count
        from ats_source_files
        """
    )


def _replace_job_tables(connection: Any, names: SnapshotTableNames) -> None:
    content_hash = """
        sha256(concat_ws(chr(31),
            provider_board_id,
            source_job_ad_id,
            company_id,
            title_original,
            description_html_original,
            description_text_original,
            detected_language,
            employer_name,
            department_name,
            team_name,
            employment_type,
            workplace_type,
            cast(is_remote as varchar),
            coalesce(cast(publication_at as varchar), ''),
            coalesce(cast(application_deadline as varchar), ''),
            coalesce(cast(source_updated_at as varchar), ''),
            job_url,
            apply_url
        ))
    """
    connection.execute(
        f"""
        create or replace temp table ats_job_states as
        select
            provider_board_id::varchar as provider_board_id,
            source_job_ad_id::varchar as source_job_ad_id,
            coalesce(company_id, '')::varchar as company_id,
            {content_hash} as content_hash,
            coalesce(title_original, '')::varchar as title_original,
            coalesce(description_html_original, '')::varchar
                as description_html_original,
            coalesce(description_text_original, '')::varchar
                as description_text_original,
            coalesce(detected_language, '')::varchar as detected_language,
            coalesce(employer_name, '')::varchar as employer_name,
            coalesce(department_name, '')::varchar as department_name,
            coalesce(team_name, '')::varchar as team_name,
            coalesce(employment_type, '')::varchar as employment_type,
            coalesce(workplace_type, '')::varchar as workplace_type,
            cast(coalesce(is_remote, false) as utinyint) as is_remote,
            cast(publication_at as timestamptz) as publication_at,
            cast(application_deadline as timestamptz) as application_deadline,
            cast(source_updated_at as timestamptz) as source_updated_at,
            coalesce(job_url, '')::varchar as job_url,
            coalesce(apply_url, '')::varchar as apply_url,
            source_url::varchar as source_url,
            source_object_key::varchar as source_object_key,
            source_run_id::varchar as source_run_id,
            cast(retrieved_at as timestamptz) as retrieved_at
        from ats_jobs
        qualify row_number() over (
            partition by provider_board_id, source_job_ad_id
            order by source_updated_at desc nulls last, job_url
        ) = 1
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.current} as
        select * from ats_job_states
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.versions} as
        select
            sha256(concat_ws('|', provider_board_id, source_job_ad_id, content_hash))
                as version_uid,
            *
        from ats_job_states
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.events} as
        select
            ''::varchar as event_uid,
            ''::varchar as provider_board_id,
            ''::varchar as source_job_ad_id,
            ''::varchar as company_id,
            cast(null as timestamptz) as event_at,
            cast(null as timestamptz) as effective_at,
            ''::varchar as event_type,
            0::utinyint as is_active,
            0::utinyint as is_estimated,
            ''::varchar as source_run_id,
            cast(null as timestamptz) as retrieved_at
        where false
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.locations} as
        select
            sha256(concat_ws('|', version.version_uid, cast(location_index as varchar),
                coalesce(city, ''), coalesce(region, ''), coalesce(country_code, ''),
                coalesce(street_address, ''), coalesce(postal_code, '')))
                as location_uid,
            version.version_uid,
            location.provider_board_id,
            location.source_job_ad_id,
            version.company_id,
            cast(location_index as usmallint) as location_index,
            coalesce(city, '')::varchar as city,
            coalesce(region, '')::varchar as region,
            upper(coalesce(country_code, ''))::varchar as country_code,
            coalesce(street_address, '')::varchar as street_address,
            coalesce(postal_code, '')::varchar as postal_code,
            cast(latitude as double) as latitude,
            cast(longitude as double) as longitude,
            version.retrieved_at
        from ats_locations as location
        inner join {names.schema}.{names.versions} as version
            using (provider_board_id, source_job_ad_id)
        """
    )
    connection.execute(
        f"""
        create or replace table {names.schema}.{names.compensations} as
        select
            sha256(concat_ws('|', version.version_uid, coalesce(currency, ''),
                coalesce(interval, ''), coalesce(cast(minimum_amount as varchar), ''),
                coalesce(cast(maximum_amount as varchar), ''),
                coalesce(compensation_text, ''))) as compensation_uid,
            version.version_uid,
            compensation.provider_board_id,
            compensation.source_job_ad_id,
            version.company_id,
            upper(coalesce(currency, ''))::varchar as currency,
            coalesce(interval, '')::varchar as interval,
            cast(minimum_amount as double) as minimum_amount,
            cast(maximum_amount as double) as maximum_amount,
            coalesce(compensation_text, '')::varchar as compensation_text,
            version.retrieved_at
        from ats_compensations as compensation
        inner join {names.schema}.{names.versions} as version
            using (provider_board_id, source_job_ad_id)
        """
    )


def _validate_normalized_tables(connection: Any) -> None:
    expected = {
        "ats_jobs": {"provider_board_id", "source_job_ad_id", "company_id"},
        "ats_locations": {
            "provider_board_id",
            "source_job_ad_id",
            "location_index",
        },
        "ats_compensations": {"provider_board_id", "source_job_ad_id"},
    }
    for table, required_columns in expected.items():
        columns = {
            str(row[1])
            for row in connection.execute(f"pragma table_info('{table}')").fetchall()
        }
        missing = required_columns - columns
        if missing:
            raise ValueError(
                f"Normalized source table {table} is missing columns: "
                + ", ".join(sorted(missing))
            )
