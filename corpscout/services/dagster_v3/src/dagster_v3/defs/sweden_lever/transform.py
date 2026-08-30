from collections.abc import Mapping, Sequence
from typing import Any

from dagster_v3.defs.common.ats_transform import (
    SnapshotTableNames,
    register_snapshot_files,
    replace_snapshot_tables,
)
from dagster_v3.defs.sweden_lever import tables


def replace_lever_snapshot_tables(
    *, connection: Any, snapshot_files: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    paths = register_snapshot_files(connection, snapshot_files)
    connection.execute(
        """
        create or replace temp table ats_lever_rows as
        select job.*, source.*
        from read_json_auto(?, filename=true, union_by_name=true) as job
        inner join ats_source_files as source
            on source.local_path = job.filename
        where upper(coalesce(job.country, '')) = 'SE'
        """,
        [list(paths)],
    )
    connection.execute(
        """
        create or replace temp table ats_jobs as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            company_id,
            coalesce(text, '') as title_original,
            coalesce(description, '') as description_html_original,
            coalesce(descriptionPlain, descriptionBodyPlain, '')
                as description_text_original,
            ''::varchar as detected_language,
            display_name as employer_name,
            coalesce(categories.department, '') as department_name,
            coalesce(categories.team, '') as team_name,
            coalesce(categories.commitment, '') as employment_type,
            coalesce(workplaceType, '') as workplace_type,
            lower(coalesce(workplaceType, '')) = 'remote' as is_remote,
            to_timestamp(cast(createdAt as double) / 1000.0) as publication_at,
            cast(null as timestamptz) as application_deadline,
            to_timestamp(cast(createdAt as double) / 1000.0) as source_updated_at,
            coalesce(hostedUrl, '') as job_url,
            coalesce(applyUrl, '') as apply_url,
            source_url,
            source_object_key,
            source_run_id,
            retrieved_at
        from ats_lever_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_locations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            1::usmallint as location_index,
            coalesce(categories.location, '')::varchar as city,
            ''::varchar as region,
            'SE'::varchar as country_code,
            ''::varchar as street_address,
            ''::varchar as postal_code,
            cast(null as double) as latitude,
            cast(null as double) as longitude
        from ats_lever_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_compensations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            coalesce(salaryRange.currency, '')::varchar as currency,
            coalesce(salaryRange.interval, '')::varchar as interval,
            try_cast(salaryRange.min as double) as minimum_amount,
            try_cast(salaryRange.max as double) as maximum_amount,
            ''::varchar as compensation_text
        from ats_lever_rows
        where salaryRange is not null
        """
    )
    return replace_snapshot_tables(connection=connection, names=_table_names())


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
