from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from dagster_v3.defs.sweden_jobtech_links import tables


@dataclass(frozen=True)
class SnapshotProvenance:
    snapshot_uid: str
    snapshot_date: date
    catalog_url: str
    source_url: str
    archive_object_key: str
    archive_sha256: str
    archive_etag: str
    archive_size_bytes: int
    raw_member_path: str
    raw_member_size_bytes: int
    source_last_modified_at: datetime | None
    source_run_id: str
    retrieved_at: datetime


@dataclass(frozen=True)
class LoadedSnapshot:
    provenance: SnapshotProvenance
    raw_row_count: int
    platsbanken_row_count: int
    external_row_count: int
    external_provider_count: int


def _qualified(table: str) -> str:
    return f"{tables.DUCKDB_SCHEMA}.{table}"


def initialize_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    """Replace the partition-local raw staging table before a replay."""
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    for table in (
        tables.SNAPSHOTS_TABLE,
        tables.VERSIONS_TABLE,
        tables.OBSERVATIONS_TABLE,
        tables.LOCATIONS_TABLE,
        tables.ENRICHMENTS_TABLE,
    ):
        connection.execute(f"drop table if exists {_qualified(table)}")
    connection.execute(
        f"""
        create or replace table {_qualified(tables.RAW_EXTERNAL_TABLE)} (
            snapshot_uid varchar not null,
            snapshot_date date not null,
            observed_at timestamptz not null,
            provider varchar not null,
            source_identifier varchar not null,
            jobtech_links_id varchar not null,
            source_run_id varchar not null,
            source_object_key varchar not null,
            source_url varchar not null,
            source_retrieved_at timestamptz not null,
            source_line_number ubigint not null,
            source_payload_hash varchar not null,
            raw_payload json not null
        )
        """
    )


def append_snapshot_jsonl(
    *,
    connection: duckdb.DuckDBPyConnection,
    jsonl_path: Path,
    provenance: SnapshotProvenance,
) -> LoadedSnapshot:
    """Load one archive with DuckDB's JSON reader and retain external ads."""
    (
        raw_row_count,
        platsbanken_row_count,
        external_row_count,
        identified_external_count,
        provider_count,
    ) = connection.execute(
        """
            with source_rows as (
                select
                    lower(trim(coalesce(json_extract_string(
                        json, '$.originalJobPosting.scraper'
                    ), ''))) as provider,
                    trim(coalesce(json_extract_string(
                        json, '$.originalJobPosting.identifier'
                    ), '')) as source_identifier
                from read_json_objects(?, format = 'newline_delimited')
            )
            select
                count(*)::ubigint,
                count(*) filter (where provider = ?)::ubigint,
                count(*) filter (where provider <> ?)::ubigint,
                count(*) filter (
                    where provider <> ? and provider <> '' and source_identifier <> ''
                )::ubigint,
                count(distinct provider) filter (where provider <> ?)::ubigint
            from source_rows
        """,
        [str(jsonl_path), *([tables.PLATSBANKEN_PROVIDER] * 4)],
    ).fetchone()
    if raw_row_count == 0:
        raise ValueError(f"JobTech Links snapshot {jsonl_path.name} contains no rows")
    if identified_external_count != external_row_count:
        raise ValueError(
            f"JobTech Links snapshot {provenance.snapshot_date} contains "
            f"{external_row_count - identified_external_count} external rows "
            "without a stable provider identity"
        )

    connection.execute(
        f"""
        insert into {_qualified(tables.RAW_EXTERNAL_TABLE)}
        with source_rows as (
            select
                row_number() over ()::ubigint as source_line_number,
                lower(sha256(cast(json as varchar))) as source_payload_hash,
                lower(trim(coalesce(json_extract_string(
                    json, '$.originalJobPosting.scraper'
                ), ''))) as provider,
                trim(coalesce(json_extract_string(
                    json, '$.originalJobPosting.identifier'
                ), '')) as source_identifier,
                trim(coalesce(json_extract_string(json, '$.id'), ''))
                    as jobtech_links_id,
                cast(json as json) as raw_payload
            from read_json_objects(?, format = 'newline_delimited')
        )
        select
            ?::varchar,
            ?::date,
            cast(?::date as timestamptz),
            provider,
            source_identifier,
            jobtech_links_id,
            ?::varchar,
            ?::varchar,
            ?::varchar,
            ?::timestamptz,
            source_line_number,
            source_payload_hash,
            raw_payload
        from source_rows
        where provider <> ?
          and provider <> ''
          and source_identifier <> ''
        """,
        [
            str(jsonl_path),
            provenance.snapshot_uid,
            provenance.snapshot_date,
            provenance.snapshot_date,
            provenance.source_run_id,
            provenance.archive_object_key,
            provenance.source_url,
            provenance.retrieved_at,
            tables.PLATSBANKEN_PROVIDER,
        ],
    )
    return LoadedSnapshot(
        provenance=provenance,
        raw_row_count=int(raw_row_count),
        platsbanken_row_count=int(platsbanken_row_count),
        external_row_count=int(external_row_count),
        external_provider_count=int(provider_count),
    )


def replace_snapshot_catalog(
    connection: duckdb.DuckDBPyConnection,
    snapshots: list[LoadedSnapshot],
) -> None:
    """Replace the small archive catalog without row-by-row inserts."""
    rows = [
        {
            **asdict(snapshot.provenance),
            "raw_row_count": snapshot.raw_row_count,
            "platsbanken_row_count": snapshot.platsbanken_row_count,
            "external_row_count": snapshot.external_row_count,
            "external_provider_count": snapshot.external_provider_count,
        }
        for snapshot in snapshots
    ]
    if not rows:
        raise ValueError("Cannot create an empty JobTech Links snapshot catalog")
    relation_name = "jobtech_links_snapshot_catalog_arrow"
    connection.register(relation_name, pa.Table.from_pylist(rows))
    try:
        connection.execute(
            f"""
            create or replace table {_qualified(tables.SNAPSHOTS_TABLE)} as
            select
                snapshot_uid::varchar as snapshot_uid,
                snapshot_date::date as snapshot_date,
                catalog_url::varchar as catalog_url,
                source_url::varchar as source_url,
                archive_object_key::varchar as archive_object_key,
                archive_sha256::varchar as archive_sha256,
                archive_etag::varchar as archive_etag,
                archive_size_bytes::ubigint as archive_size_bytes,
                raw_member_path::varchar as raw_member_path,
                raw_member_size_bytes::ubigint as raw_member_size_bytes,
                raw_row_count::ubigint as raw_row_count,
                platsbanken_row_count::ubigint as platsbanken_row_count,
                external_row_count::ubigint as external_row_count,
                external_provider_count::usmallint as external_provider_count,
                source_last_modified_at::timestamptz as source_last_modified_at,
                source_run_id::varchar as source_run_id,
                retrieved_at::timestamptz as retrieved_at
            from {relation_name}
            order by snapshot_date, snapshot_uid
            """
        )
    finally:
        connection.unregister(relation_name)


def build_normalized_tables(*, connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Build serving-ready normalized tables with set-based DuckDB SQL."""
    _build_parsed_rows(connection)
    _build_versions(connection)
    _build_observations(connection)
    _build_locations(connection)
    _build_enrichments(connection)

    count_tables = {
        "snapshots": tables.SNAPSHOTS_TABLE,
        "raw_external_rows": tables.RAW_EXTERNAL_TABLE,
        "versions": tables.VERSIONS_TABLE,
        "observations": tables.OBSERVATIONS_TABLE,
        "locations": tables.LOCATIONS_TABLE,
        "enrichments": tables.ENRICHMENTS_TABLE,
    }
    counts = {
        name: int(
            connection.execute(f"select count(*) from {_qualified(table)}").fetchone()[
                0
            ]
        )
        for name, table in count_tables.items()
    }
    counts["external_providers"] = int(
        connection.execute(
            f"select count(distinct provider) from {_qualified(tables.RAW_EXTERNAL_TABLE)}"
        ).fetchone()[0]
    )
    return {
        "snapshots": counts["snapshots"],
        "raw_external_rows": counts["raw_external_rows"],
        "external_providers": counts["external_providers"],
        "versions": counts["versions"],
        "observations": counts["observations"],
        "locations": counts["locations"],
        "enrichments": counts["enrichments"],
    }


def _build_parsed_rows(connection: duckdb.DuckDBPyConnection) -> None:
    raw = _qualified(tables.RAW_EXTERNAL_TABLE)
    connection.execute(
        f"""
        create or replace temporary table jobtech_links_parsed as
        with extracted as (
            select
                *,
                lower(sha256(provider || chr(0) || source_identifier))
                    as source_job_ad_uid,
                coalesce(json_extract_string(raw_payload, '$.hashsum'), '')
                    as source_hashsum,
                try_cast(json_extract_string(raw_payload, '$.firstSeen') as timestamptz)
                    as source_first_seen_at,
                try_cast(json_extract_string(
                    raw_payload, '$.originalJobPosting.datePosted'
                ) as timestamptz) as publication_at,
                try_cast(json_extract_string(
                    raw_payload, '$.date_to_display_as_publish_date'
                ) as timestamptz) as display_publication_at,
                coalesce(
                    try_cast(json_extract_string(
                        raw_payload, '$.application_deadline'
                    ) as timestamptz),
                    try_cast(json_extract_string(
                        raw_payload, '$.originalJobPosting.validThrough'
                    ) as timestamptz)
                ) as application_deadline,
                coalesce(try_cast(json_extract_string(raw_payload, '$.isValid') as boolean), false)
                    as is_valid,
                coalesce(json_extract_string(raw_payload, '$.originalJobPosting.url'), '')
                    as canonical_url,
                coalesce(json_extract_string(raw_payload, '$.originalJobPosting.title'), '')
                    as headline_original,
                coalesce(json_extract_string(raw_payload, '$.brief_description'), '')
                    as brief_description_original,
                coalesce(json_extract_string(raw_payload, '$.detected_language'), '')
                    as detected_language,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.hiringOrganization.name'
                ), '') as employer_name,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.hiringOrganization.url'
                ), '') as employer_url,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.hiringOrganization.logo'
                ), '') as employer_logo_url,
                case json_type(json_extract(
                    raw_payload, '$.originalJobPosting.employmentType'
                ))
                    when 'ARRAY' then from_json(
                        json_extract(raw_payload, '$.originalJobPosting.employmentType'),
                        '["VARCHAR"]'
                    )
                    when 'VARCHAR' then [json_extract_string(
                        raw_payload, '$.originalJobPosting.employmentType'
                    )]
                    else []::varchar[]
                end as employment_types,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.jobLocationType'
                ), '') as workplace_type,
                try_cast(json_extract_string(
                    raw_payload, '$.originalJobPosting.totalJobOpenings'
                ) as uinteger) as number_of_vacancies,
                ''::varchar as occupation_concept_id,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.relevantOccupation.name'
                ), '') as occupation_label_original,
                coalesce(json_extract_string(raw_payload, '$.ssyk_lvl4'), '')
                    as ssyk_level4_code,
                json_extract(raw_payload, '$.workplace_addresses') as locations_json,
                json_extract(
                    raw_payload,
                    '$.text_enrichments_results.enrichedbinary_result.enriched_candidates'
                ) as accepted_enrichments_json
            from {raw}
        ), serving as (
            select
                *,
                coalesce(source_first_seen_at, observed_at) as version_at,
                case json_type(json_extract(
                    raw_payload, '$.originalJobPosting.experienceRequirements'
                ))
                    when 'VARCHAR' then coalesce(json_extract_string(
                        raw_payload, '$.originalJobPosting.experienceRequirements'
                    ), '')
                    else coalesce(cast(json_extract(
                        raw_payload, '$.originalJobPosting.experienceRequirements'
                    ) as varchar), '')
                end as experience_requirements_original,
                case json_type(json_extract(raw_payload, '$.originalJobPosting.skills'))
                    when 'VARCHAR' then coalesce(json_extract_string(
                        raw_payload, '$.originalJobPosting.skills'
                    ), '')
                    else coalesce(cast(json_extract(
                        raw_payload, '$.originalJobPosting.skills'
                    ) as varchar), '')
                end as skills_original,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.qualifications'
                ), '') as qualifications_original,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.responsibilities'
                ), '') as responsibilities_original,
                case json_type(json_extract(
                    raw_payload, '$.originalJobPosting.educationRequirements'
                ))
                    when 'VARCHAR' then coalesce(json_extract_string(
                        raw_payload, '$.originalJobPosting.educationRequirements'
                    ), '')
                    else coalesce(cast(json_extract(
                        raw_payload, '$.originalJobPosting.educationRequirements'
                    ) as varchar), '')
                end as education_requirements_original,
                case json_type(json_extract(
                    raw_payload, '$.originalJobPosting.jobBenefits'
                ))
                    when 'VARCHAR' then coalesce(json_extract_string(
                        raw_payload, '$.originalJobPosting.jobBenefits'
                    ), '')
                    else coalesce(cast(json_extract(
                        raw_payload, '$.originalJobPosting.jobBenefits'
                    ) as varchar), '')
                end as job_benefits_original,
                coalesce(json_extract_string(
                    raw_payload, '$.originalJobPosting.workHours'
                ), '') as work_hours_original
            from extracted
        ), hashed as (
            select
                *,
                lower(sha256(concat_ws(chr(0),
                    source_hashsum,
                    coalesce(cast(source_first_seen_at as varchar), ''),
                    coalesce(cast(publication_at as varchar), ''),
                    coalesce(cast(display_publication_at as varchar), ''),
                    coalesce(cast(application_deadline as varchar), ''),
                    cast(is_valid as varchar), canonical_url, headline_original,
                    brief_description_original, detected_language, employer_name,
                    employer_url, employer_logo_url, cast(employment_types as varchar),
                    workplace_type, coalesce(cast(number_of_vacancies as varchar), ''),
                    occupation_concept_id, occupation_label_original, ssyk_level4_code,
                    experience_requirements_original, skills_original,
                    qualifications_original, responsibilities_original,
                    education_requirements_original, job_benefits_original,
                    work_hours_original, coalesce(cast(locations_json as varchar), ''),
                    coalesce(cast(accepted_enrichments_json as varchar), '')
                ))) as normalized_payload_hash
            from serving
        )
        select
            *,
            lower(sha256(source_job_ad_uid || chr(0) || normalized_payload_hash))
                as version_uid
        from hashed
        """
    )


def _build_versions(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        create or replace table {_qualified(tables.VERSIONS_TABLE)} as
        select
            version_uid,
            source_job_ad_uid,
            provider,
            source_identifier,
            jobtech_links_id,
            source_hashsum,
            version_at,
            source_first_seen_at,
            publication_at,
            display_publication_at,
            application_deadline,
            is_valid::utinyint as is_valid,
            canonical_url,
            headline_original,
            brief_description_original,
            detected_language,
            employer_name,
            employer_url,
            employer_logo_url,
            employment_types,
            workplace_type,
            number_of_vacancies,
            occupation_concept_id,
            occupation_label_original,
            ssyk_level4_code,
            experience_requirements_original,
            skills_original,
            qualifications_original,
            responsibilities_original,
            education_requirements_original,
            job_benefits_original,
            work_hours_original,
            snapshot_uid,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            source_retrieved_at as ingested_at
        from jobtech_links_parsed
        qualify row_number() over (
            partition by version_uid
            order by snapshot_date, source_line_number
        ) = 1
        """
    )


def _build_observations(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        create or replace table {_qualified(tables.OBSERVATIONS_TABLE)} as
        select
            lower(sha256(snapshot_uid || chr(0) || source_job_ad_uid))
                as observation_uid,
            snapshot_uid,
            snapshot_date,
            observed_at,
            source_job_ad_uid,
            version_uid,
            provider,
            source_identifier,
            jobtech_links_id,
            source_run_id,
            source_line_number,
            source_retrieved_at as ingested_at
        from jobtech_links_parsed
        qualify row_number() over (
            partition by snapshot_uid, source_job_ad_uid
            order by source_line_number
        ) = 1
        """
    )


def _build_locations(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        create or replace table {_qualified(tables.LOCATIONS_TABLE)} as
        select
            lower(sha256(
                parsed.version_uid || chr(0) || cast(cast(location.key as integer) + 1 as varchar)
            )) as location_uid,
            parsed.version_uid,
            parsed.source_job_ad_uid,
            parsed.provider,
            (cast(location.key as integer) + 1)::usmallint as location_index,
            coalesce(json_extract_string(location.value, '$.municipality_concept_id'), '')
                as municipality_concept_id,
            coalesce(json_extract_string(location.value, '$.municipality'), '')
                as municipality_name_original,
            coalesce(json_extract_string(location.value, '$.region_concept_id'), '')
                as region_concept_id,
            coalesce(json_extract_string(location.value, '$.region'), '')
                as region_name_original,
            coalesce(json_extract_string(location.value, '$.country_concept_id'), '')
                as country_concept_id,
            coalesce(json_extract_string(location.value, '$.country'), '')
                as country_name_original,
            parsed.version_at,
            parsed.source_run_id,
            parsed.source_retrieved_at as ingested_at
        from jobtech_links_parsed as parsed,
             lateral json_each(coalesce(parsed.locations_json, '[]'::json)) as location
        qualify row_number() over (
            partition by parsed.version_uid, location_index
            order by parsed.snapshot_date, parsed.source_line_number
        ) = 1
        """
    )


def _build_enrichments(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        create or replace table {_qualified(tables.ENRICHMENTS_TABLE)} as
        with enrichment_arrays(enrichment_type, json_path) as (
            values
                ('occupation', '$.occupations'),
                ('competency', '$.competencies'),
                ('trait', '$.traits'),
                ('geo', '$.geos')
        ), expanded as (
            select
                parsed.*,
                enrichment_arrays.enrichment_type,
                enrichment.key as enrichment_index,
                coalesce(json_extract_string(enrichment.value, '$.concept_label'), '')
                    as concept_label_original,
                coalesce(json_extract_string(enrichment.value, '$.term'), '')
                    as matched_term_original,
                coalesce(try_cast(json_extract_string(
                    enrichment.value, '$.term_misspelled'
                ) as boolean), false) as term_misspelled
            from jobtech_links_parsed as parsed
            cross join enrichment_arrays
            cross join lateral json_each(coalesce(
                json_extract(parsed.accepted_enrichments_json, json_path),
                '[]'::json
            )) as enrichment
        )
        select
            lower(sha256(concat_ws(chr(0),
                version_uid, enrichment_type, concept_label_original,
                matched_term_original, cast(term_misspelled as varchar), enrichment_index
            ))) as enrichment_uid,
            version_uid,
            source_job_ad_uid,
            provider,
            enrichment_type,
            concept_label_original,
            matched_term_original,
            term_misspelled::utinyint as term_misspelled,
            version_at,
            source_run_id,
            source_retrieved_at as ingested_at
        from expanded
        qualify row_number() over (
            partition by enrichment_uid
            order by snapshot_date, source_line_number
        ) = 1
        """
    )
