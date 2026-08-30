from collections.abc import Mapping, Sequence
from typing import Any

from dagster_v3.defs.common.ats_transform import (
    SnapshotTableNames,
    register_snapshot_files,
    replace_snapshot_tables,
)
from dagster_v3.defs.sweden_greenhouse import tables

SWEDEN_LOCATION_PATTERN = (
    "stockholm|sweden|gothenburg|göteborg|malmö|malmo|lund|uppsala|"
    "västerås|vasteras|linköping|linkoping"
)


def replace_greenhouse_snapshot_tables(
    *, connection: Any, snapshot_files: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    paths = register_snapshot_files(connection, snapshot_files)
    connection.execute(
        """
        create or replace temp table ats_greenhouse_rows as
        select job.*, source.*
        from read_json_auto(?, filename=true, union_by_name=true) as payload
        cross join unnest(payload.jobs) as item(job)
        inner join ats_source_files as source
            on source.local_path = payload.filename
        where regexp_matches(lower(coalesce(job.location.name, '')), ?)
        """,
        [list(paths), SWEDEN_LOCATION_PATTERN],
    )
    connection.execute(
        """
        create or replace temp table ats_jobs as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            company_id,
            coalesce(title, '') as title_original,
            coalesce(content, '') as description_html_original,
            regexp_replace(coalesce(content, ''), '<[^>]+>', ' ', 'g')
                as description_text_original,
            coalesce(language, '') as detected_language,
            coalesce(company_name, display_name) as employer_name,
            coalesce(list_extract(departments, 1).name, '') as department_name,
            ''::varchar as team_name,
            ''::varchar as employment_type,
            ''::varchar as workplace_type,
            contains(lower(coalesce(location.name, '')), 'remote') as is_remote,
            try_cast(first_published as timestamptz) as publication_at,
            try_cast(application_deadline as timestamptz) as application_deadline,
            try_cast(updated_at as timestamptz) as source_updated_at,
            coalesce(absolute_url, '') as job_url,
            coalesce(absolute_url, '') as apply_url,
            source_url,
            source_object_key,
            source_run_id,
            retrieved_at
        from ats_greenhouse_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_locations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            1::usmallint as location_index,
            coalesce(location.name, '')::varchar as city,
            ''::varchar as region,
            'SE'::varchar as country_code,
            ''::varchar as street_address,
            ''::varchar as postal_code,
            cast(null as double) as latitude,
            cast(null as double) as longitude
        from ats_greenhouse_rows
        """
    )
    _create_empty_compensations(connection)
    return replace_snapshot_tables(connection=connection, names=_table_names())


def _create_empty_compensations(connection: Any) -> None:
    connection.execute(
        """
        create or replace temp table ats_compensations (
            provider_board_id varchar,
            source_job_ad_id varchar,
            currency varchar,
            interval varchar,
            minimum_amount double,
            maximum_amount double,
            compensation_text varchar
        )
        """
    )


def _table_names() -> SnapshotTableNames:
    return SnapshotTableNames(
        schema=tables.DUCKDB_SCHEMA,
        boards=tables.BOARDS_TABLE,
        board_company_links=tables.BOARD_COMPANY_LINKS_TABLE,
        board_snapshots=tables.BOARD_SNAPSHOTS_TABLE,
        versions=tables.VERSIONS_TABLE,
        events=tables.EVENTS_TABLE,
        current=tables.CURRENT_TABLE,
        locations=tables.LOCATIONS_TABLE,
        compensations=tables.COMPENSATIONS_TABLE,
    )
