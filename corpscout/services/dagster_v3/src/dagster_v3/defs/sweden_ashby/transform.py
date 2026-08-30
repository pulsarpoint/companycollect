from collections.abc import Mapping, Sequence
from typing import Any

from dagster_v3.defs.common.ats_transform import (
    SnapshotTableNames,
    register_snapshot_files,
    replace_snapshot_tables,
)
from dagster_v3.defs.sweden_ashby import tables

SWEDEN_LOCATION_PATTERN = (
    "stockholm|sweden|gothenburg|göteborg|malmö|malmo|lund|uppsala|"
    "västerås|vasteras|linköping|linkoping"
)


def replace_ashby_snapshot_tables(
    *, connection: Any, snapshot_files: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    paths = register_snapshot_files(connection, snapshot_files)
    connection.execute(
        """
        create or replace temp table ats_ashby_rows as
        select job.*, source.*
        from read_json_auto(?, filename=true, union_by_name=true) as payload
        cross join unnest(payload.jobs) as item(job)
        inner join ats_source_files as source
            on source.local_path = payload.filename
        where coalesce(job.isListed, true)
          and (
              lower(coalesce(job.address.postalAddress.addressCountry, '')) = 'sweden'
              or regexp_matches(lower(coalesce(job.location, '')), ?)
          )
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
            coalesce(descriptionHtml, '') as description_html_original,
            coalesce(descriptionPlain, '') as description_text_original,
            ''::varchar as detected_language,
            display_name as employer_name,
            coalesce(department, '') as department_name,
            coalesce(team, '') as team_name,
            coalesce(employmentType, '') as employment_type,
            coalesce(workplaceType, '') as workplace_type,
            coalesce(isRemote, false) as is_remote,
            try_cast(publishedAt as timestamptz) as publication_at,
            cast(null as timestamptz) as application_deadline,
            try_cast(publishedAt as timestamptz) as source_updated_at,
            coalesce(jobUrl, '') as job_url,
            coalesce(applyUrl, '') as apply_url,
            source_url,
            source_object_key,
            source_run_id,
            retrieved_at
        from ats_ashby_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_locations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            1::usmallint as location_index,
            coalesce(address.postalAddress.addressLocality, location, '')::varchar
                as city,
            coalesce(address.postalAddress.addressRegion, '')::varchar as region,
            'SE'::varchar as country_code,
            coalesce(json_extract_string(
                to_json(address.postalAddress), '$.streetAddress'), '')::varchar
                as street_address,
            coalesce(json_extract_string(
                to_json(address.postalAddress), '$.postalCode'), '')::varchar
                as postal_code,
            cast(null as double) as latitude,
            cast(null as double) as longitude
        from ats_ashby_rows
        """
    )
    connection.execute(
        """
        create or replace temp table ats_compensations as
        select
            provider_board_id,
            cast(id as varchar) as source_job_ad_id,
            ''::varchar as currency,
            ''::varchar as interval,
            cast(null as double) as minimum_amount,
            cast(null as double) as maximum_amount,
            coalesce(
                nullif(cast(compensation.scrapeableCompensationSalarySummary
                    as varchar), 'null'),
                nullif(cast(compensation.compensationTierSummary as varchar), 'null'),
                '')::varchar
                as compensation_text
        from ats_ashby_rows
        where coalesce(
            nullif(cast(compensation.scrapeableCompensationSalarySummary
                as varchar), 'null'),
            nullif(cast(compensation.compensationTierSummary as varchar), 'null'),
            '') != ''
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
