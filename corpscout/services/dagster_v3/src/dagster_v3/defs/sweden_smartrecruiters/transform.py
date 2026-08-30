from collections.abc import Mapping, Sequence
from typing import Any

from dagster_v3.defs.common.ats_transform import (
    SnapshotTableNames,
    register_snapshot_files,
    replace_snapshot_tables,
)
from dagster_v3.defs.sweden_smartrecruiters import tables


def replace_smartrecruiters_snapshot_tables(
    *, connection: Any, snapshot_files: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    paths = register_snapshot_files(connection, snapshot_files)
    connection.execute(
        """
        create or replace temp table ats_smartrecruiters_rows as
        select job.*, source.*
        from read_json_auto(?, filename=true, union_by_name=true) as payload
        cross join unnest(payload.content) as item(job)
        inner join ats_source_files as source
            on source.local_path = payload.filename
        where lower(coalesce(job.location.country, '')) = 'se'
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
            coalesce(name, '') as title_original,
            concat_ws('\n',
                coalesce(jobAd.sections.companyDescription.text, ''),
                coalesce(jobAd.sections.jobDescription.text, ''),
                coalesce(jobAd.sections.qualifications.text, ''),
                coalesce(jobAd.sections.additionalInformation.text, '')
            ) as description_html_original,
            regexp_replace(concat_ws('\n',
                coalesce(jobAd.sections.companyDescription.text, ''),
                coalesce(jobAd.sections.jobDescription.text, ''),
                coalesce(jobAd.sections.qualifications.text, ''),
                coalesce(jobAd.sections.additionalInformation.text, '')
            ), '<[^>]+>', ' ', 'g') as description_text_original,
            coalesce(language.code, '') as detected_language,
            coalesce(company.name, display_name) as employer_name,
            coalesce(department.label, '') as department_name,
            coalesce(function.label, '') as team_name,
            coalesce(typeOfEmployment.label, '') as employment_type,
            case
                when coalesce(location.remote, false) then 'remote'
                when coalesce(location.hybrid, false) then 'hybrid'
                else 'on-site'
            end as workplace_type,
            coalesce(location.remote, false) as is_remote,
            try_cast(releasedDate as timestamptz) as publication_at,
            cast(null as timestamptz) as application_deadline,
            try_cast(releasedDate as timestamptz) as source_updated_at,
            coalesce(applyUrl, '') as job_url,
            coalesce(applyUrl, '') as apply_url,
            source_url,
            source_object_key,
            source_run_id,
            retrieved_at
        from ats_smartrecruiters_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_locations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            1::usmallint as location_index,
            coalesce(location.city, '')::varchar as city,
            coalesce(location.region, '')::varchar as region,
            upper(coalesce(location.country, ''))::varchar as country_code,
            coalesce(location.address, '')::varchar as street_address,
            ''::varchar as postal_code,
            try_cast(location.latitude as double) as latitude,
            try_cast(location.longitude as double) as longitude
        from ats_smartrecruiters_rows
        """
    )
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
