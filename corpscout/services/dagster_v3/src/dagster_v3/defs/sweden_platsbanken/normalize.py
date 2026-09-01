import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.sweden_company.identity import sweden_identity_sql
from dagster_v3.defs.sweden_platsbanken import tables

_REQUIREMENT_PATHS = (
    ("must_have", "skill", "$.must_have.skills"),
    ("must_have", "language", "$.must_have.languages"),
    ("must_have", "work_experience", "$.must_have.work_experiences"),
    ("must_have", "education", "$.must_have.education"),
    ("must_have", "education_level", "$.must_have.education_level"),
    ("nice_to_have", "skill", "$.nice_to_have.skills"),
    ("nice_to_have", "language", "$.nice_to_have.languages"),
    ("nice_to_have", "work_experience", "$.nice_to_have.work_experiences"),
    ("nice_to_have", "education", "$.nice_to_have.education"),
    ("nice_to_have", "education_level", "$.nice_to_have.education_level"),
    ("must_have", "driving_license", "$.driving_license"),
)

_REQUIREMENT_VALUE_STRUCTURE = {
    "concept_id": "VARCHAR",
    "label": "VARCHAR",
    "legacy_ams_taxonomy_id": "VARCHAR",
    "weight": "FLOAT",
}
_REQUIREMENTS_STRUCTURE = {
    "must_have": {
        "skills": [_REQUIREMENT_VALUE_STRUCTURE],
        "languages": [_REQUIREMENT_VALUE_STRUCTURE],
        "work_experiences": [_REQUIREMENT_VALUE_STRUCTURE],
        "education": [_REQUIREMENT_VALUE_STRUCTURE],
        "education_level": [_REQUIREMENT_VALUE_STRUCTURE],
    },
    "nice_to_have": {
        "skills": [_REQUIREMENT_VALUE_STRUCTURE],
        "languages": [_REQUIREMENT_VALUE_STRUCTURE],
        "work_experiences": [_REQUIREMENT_VALUE_STRUCTURE],
        "education": [_REQUIREMENT_VALUE_STRUCTURE],
        "education_level": [_REQUIREMENT_VALUE_STRUCTURE],
    },
    "driving_license": [_REQUIREMENT_VALUE_STRUCTURE],
}


def replace_raw_jsonl_table(
    *,
    connection: Any,
    jsonl_path: Path,
    raw_table: str,
    record_kind: str,
    source_run_id: str,
    source_object_key: str,
    source_url: str,
    retrieved_at: datetime,
    allow_empty: bool = False,
) -> int:
    """Load a JSONL source file as auditable JSON rows using DuckDB's reader."""
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    qualified = f"{tables.DUCKDB_SCHEMA}.{raw_table}"
    connection.execute(
        f"""
        create or replace table {qualified} as
        select
            cast(? as varchar) as record_kind,
            cast(? as varchar) as source_run_id,
            cast(? as varchar) as source_object_key,
            cast(? as varchar) as source_url,
            cast(? as timestamptz) as source_retrieved_at,
            row_number() over ()::ubigint as source_line_number,
            lower(sha256(cast(json as varchar))) as source_payload_hash,
            cast(json as json) as raw_payload
        from read_json_objects(?, format='newline_delimited')
        """,
        [
            record_kind,
            source_run_id,
            source_object_key,
            source_url,
            retrieved_at,
            str(jsonl_path),
        ],
    )
    row_count = _count(connection, qualified)
    if row_count == 0 and not allow_empty:
        raise ValueError(f"{source_object_key} produced zero JSONL records")
    return row_count


def append_raw_jsonl_table(
    *,
    connection: Any,
    jsonl_path: Path,
    raw_table: str,
    record_kind: str,
    source_run_id: str,
    source_object_key: str,
    source_url: str,
    retrieved_at: datetime,
) -> int:
    """Append one archive member to an existing raw table in set-based SQL."""
    qualified = f"{tables.DUCKDB_SCHEMA}.{raw_table}"
    before = _count(connection, qualified)
    connection.execute(
        f"""
        insert into {qualified}
        select
            cast(? as varchar),
            cast(? as varchar),
            cast(? as varchar),
            cast(? as varchar),
            cast(? as timestamptz),
            row_number() over ()::ubigint,
            lower(sha256(cast(json as varchar))),
            cast(json as json)
        from read_json_objects(?, format='newline_delimited')
        """,
        [
            record_kind,
            source_run_id,
            source_object_key,
            source_url,
            retrieved_at,
            str(jsonl_path),
        ],
    )
    return _count(connection, qualified) - before


def build_normalized_tables(
    *,
    connection: Any,
    raw_table: str,
    versions_table: str,
    events_table: str,
    requirements_table: str,
    contacts_table: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build versioned job facts, lifecycle events, and requirement facts."""
    raw = f"{tables.DUCKDB_SCHEMA}.{raw_table}"
    versions = f"{tables.DUCKDB_SCHEMA}.{versions_table}"
    events = f"{tables.DUCKDB_SCHEMA}.{events_table}"
    requirements = f"{tables.DUCKDB_SCHEMA}.{requirements_table}"
    contacts = f"{tables.DUCKDB_SCHEMA}.{contacts_table}"
    employer_identity = sweden_identity_sql(
        "json_extract_string(raw_payload, '$.employer.organization_number')"
    )

    connection.execute(
        f"""
        create or replace table {versions} as
        with extracted as (
            select
                coalesce(
                    nullif(json_extract_string(raw_payload, '$.original_id'), ''),
                    json_extract_string(raw_payload, '$.id'),
                    ''
                ) as source_job_ad_id,
                coalesce(json_extract_string(raw_payload, '$.id'), '')
                    as source_record_id,
                coalesce(json_extract_string(raw_payload, '$.original_id'), '')
                    as source_original_id,
                coalesce(json_extract_string(raw_payload, '$.external_id'), '')
                    as source_external_id,
                coalesce(
                    to_timestamp(
                        try_cast(json_extract_string(raw_payload, '$.timestamp') as double)
                        / 1000.0
                    ),
                    source_retrieved_at
                ) as version_at,
                record_kind as version_kind,
                cast(
                    coalesce(
                        try_cast(json_extract_string(raw_payload, '$.removed') as boolean),
                        false
                    ) as utinyint
                ) as is_removed,
                {_stockholm_timestamp("$.publication_date")} as publication_at,
                {_stockholm_timestamp("$.last_publication_date")}
                    as last_publication_at,
                {_stockholm_timestamp("$.application_deadline")}
                    as application_deadline,
                {_stockholm_timestamp("$.removed_date")} as removed_at,
                {employer_identity} as employer_org_number,
                coalesce(json_extract_string(raw_payload, '$.employer.name'), '')
                    as employer_name,
                coalesce(json_extract_string(raw_payload, '$.employer.workplace'), '')
                    as employer_workplace,
                coalesce(json_extract_string(raw_payload, '$.employer.url'), '')
                    as employer_url,
                coalesce(json_extract_string(raw_payload, '$.application_details.email'), '')
                    as application_email,
                coalesce(json_extract_string(raw_payload, '$.application_details.url'), '')
                    as application_url,
                coalesce(json_extract_string(raw_payload, '$.application_details.other'), '')
                    as application_other,
                coalesce(json_extract_string(raw_payload, '$.application_details.reference'), '')
                    as application_reference,
                coalesce(json_extract_string(raw_payload, '$.application_details.information'), '')
                    as application_information,
                cast(
                    try_cast(
                        json_extract_string(raw_payload, '$.application_details.via_af')
                        as boolean
                    ) as utinyint
                ) as application_via_af,
                coalesce(json_extract_string(raw_payload, '$.employer.email'), '')
                    as employer_email,
                coalesce(json_extract_string(raw_payload, '$.employer.phone_number'), '')
                    as employer_phone,
                coalesce(json_extract_string(raw_payload, '$.headline'), '')
                    as headline_original,
                coalesce(json_extract_string(raw_payload, '$.description.text'), '')
                    as description_text_original,
                coalesce(json_extract_string(raw_payload, '$.detected_language'), '')
                    as detected_language,
                coalesce(json_extract_string(raw_payload, '$.webpage_url'), '')
                    as webpage_url,
                try_cast(
                    json_extract_string(raw_payload, '$.number_of_vacancies') as ubigint
                ) as number_of_vacancies,
                {_taxonomy("employment_type", "concept_id")}
                    as employment_type_concept_id,
                {_taxonomy("employment_type", "label")}
                    as employment_type_label_original,
                {_taxonomy("salary_type", "concept_id")} as salary_type_concept_id,
                {_taxonomy("salary_type", "label")}
                    as salary_type_label_original,
                coalesce(json_extract_string(raw_payload, '$.salary_description'), '')
                    as salary_description_original,
                {_taxonomy("duration", "concept_id")} as duration_concept_id,
                {_taxonomy("duration", "label")} as duration_label_original,
                {_taxonomy("working_hours_type", "concept_id")}
                    as working_hours_concept_id,
                {_taxonomy("working_hours_type", "label")}
                    as working_hours_label_original,
                try_cast(json_extract_string(raw_payload, '$.scope_of_work.min') as real)
                    as scope_min,
                try_cast(json_extract_string(raw_payload, '$.scope_of_work.max') as real)
                    as scope_max,
                cast(
                    try_cast(json_extract_string(raw_payload, '$.experience_required') as boolean)
                    as utinyint
                ) as experience_required,
                cast(
                    try_cast(json_extract_string(raw_payload, '$.access_to_own_car') as boolean)
                    as utinyint
                ) as access_to_own_car,
                cast(
                    try_cast(json_extract_string(raw_payload, '$.driving_license_required') as boolean)
                    as utinyint
                ) as driving_license_required,
                {_taxonomy("occupation", "concept_id")} as occupation_concept_id,
                {_taxonomy("occupation", "label")} as occupation_label_original,
                {_taxonomy("occupation_group", "concept_id")}
                    as occupation_group_concept_id,
                {_taxonomy("occupation_group", "label")}
                    as occupation_group_label_original,
                {_taxonomy("occupation_field", "concept_id")}
                    as occupation_field_concept_id,
                {_taxonomy("occupation_field", "label")}
                    as occupation_field_label_original,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.municipality_code'), '')
                    as municipality_code,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.municipality_concept_id'), '')
                    as municipality_concept_id,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.municipality'), '')
                    as municipality_name_original,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.region_code'), '')
                    as region_code,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.region_concept_id'), '')
                    as region_concept_id,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.region'), '')
                    as region_name_original,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.country_code'), '')
                    as country_code,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.country_concept_id'), '')
                    as country_concept_id,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.country'), '')
                    as country_name_original,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.street_address'), '')
                    as street_address,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.postcode'), '')
                    as postcode,
                coalesce(json_extract_string(raw_payload, '$.workplace_address.city'), '')
                    as city,
                try_cast(json_extract_string(raw_payload, '$.workplace_address.coordinates[0]') as double)
                    as longitude,
                try_cast(json_extract_string(raw_payload, '$.workplace_address.coordinates[1]') as double)
                    as latitude,
                coalesce(json_extract_string(raw_payload, '$.source_type'), '')
                    as source_type,
                source_url,
                source_object_key,
                source_run_id,
                source_line_number,
                source_payload_hash,
                source_retrieved_at as ingested_at
            from {raw}
        ), classified as (
            select
                *,
                case
                    when employer_org_number = '' then 'missing_org_number'
                    when length(employer_org_number) = 10 then 'eligible'
                    when length(employer_org_number) = 12
                         and (
                             employer_org_number like '19%'
                             or employer_org_number like '20%'
                         ) then 'person_keyed'
                    else 'invalid_or_foreign_org_number'
                end as match_eligibility
            from extracted
        )
        select
            lower(sha256(concat_ws(
                '|', source_job_ad_id, cast(version_at as varchar), source_payload_hash
            ))) as version_uid,
            source_job_ad_id,
            source_record_id,
            source_original_id,
            source_external_id,
            version_at,
            version_kind,
            is_removed,
            publication_at,
            last_publication_at,
            application_deadline,
            removed_at,
            employer_org_number,
            match_eligibility,
            employer_name,
            employer_workplace,
            employer_url,
            application_email,
            application_url,
            application_other,
            application_reference,
            application_information,
            application_via_af,
            employer_email,
            employer_phone,
            headline_original,
            description_text_original,
            detected_language,
            webpage_url,
            number_of_vacancies,
            employment_type_concept_id,
            employment_type_label_original,
            salary_type_concept_id,
            salary_type_label_original,
            salary_description_original,
            duration_concept_id,
            duration_label_original,
            working_hours_concept_id,
            working_hours_label_original,
            scope_min,
            scope_max,
            experience_required,
            access_to_own_car,
            driving_license_required,
            occupation_concept_id,
            occupation_label_original,
            occupation_group_concept_id,
            occupation_group_label_original,
            occupation_field_concept_id,
            occupation_field_label_original,
            municipality_code,
            municipality_concept_id,
            municipality_name_original,
            region_code,
            region_concept_id,
            region_name_original,
            country_code,
            country_concept_id,
            country_name_original,
            street_address,
            postcode,
            city,
            longitude,
            latitude,
            source_type,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from classified
        where source_job_ad_id != ''
          and (
              publication_at is not null
              or headline_original != ''
              or employer_org_number != ''
          )
        """
    )

    connection.execute(_events_sql(raw=raw, versions=versions, events=events))
    connection.execute(
        _requirements_sql(raw=raw, versions=versions, requirements=requirements)
    )
    connection.execute(_contacts_sql(raw=raw, versions=versions, contacts=contacts))

    counts = {
        "versions": _count(connection, versions),
        "events": _count(connection, events),
        "requirements": _count(connection, requirements),
        "contacts": _count(connection, contacts),
    }
    if log is not None:
        log("Built Platsbanken normalized tables: %s", counts)
    return counts


def _events_sql(*, raw: str, versions: str, events: str) -> str:
    employer_identity = sweden_identity_sql(
        "json_extract_string(raw_payload, '$.employer.organization_number')"
    )
    return f"""
    create or replace table {events} as
    with raw_events as (
        select
            coalesce(
                nullif(json_extract_string(raw_payload, '$.original_id'), ''),
                json_extract_string(raw_payload, '$.id'),
                ''
            ) as source_job_ad_id,
            coalesce(json_extract_string(raw_payload, '$.id'), '')
                as source_record_id,
            coalesce(
                to_timestamp(
                    try_cast(json_extract_string(raw_payload, '$.timestamp') as double)
                    / 1000.0
                ),
                source_retrieved_at
            ) as version_at,
            cast(
                coalesce(
                    try_cast(json_extract_string(raw_payload, '$.removed') as boolean),
                    false
                ) as utinyint
            ) as is_removed,
            {_stockholm_timestamp("$.publication_date")} as publication_at,
            {_stockholm_timestamp("$.removed_date")} as removed_at,
            {employer_identity} as employer_org_number,
            record_kind,
            source_payload_hash,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            source_retrieved_at as ingested_at
        from {raw}
    ), effective_events as (
        select
            *,
            case
                when is_removed = 1 then coalesce(removed_at, version_at)
                when record_kind = 'archive_record' and publication_at is not null
                    then publication_at
                else version_at
            end as effective_at
        from raw_events
    ), record_events as (
        select
            lower(sha256(concat_ws(
                '|', source_job_ad_id,
                case
                    when is_removed = 1 then 'removed'
                    when record_kind = 'archive_record' then 'archive_record'
                    when record_kind = 'snapshot' then 'snapshot_seen'
                    else 'upsert'
                end,
                cast(effective_at as varchar),
                source_payload_hash
            ))) as event_uid,
            source_job_ad_id,
            source_record_id,
            version_at as event_at,
            effective_at,
            case
                when is_removed = 1 then 'removed'
                when record_kind = 'archive_record' then 'archive_record'
                when record_kind = 'snapshot' then 'snapshot_seen'
                else 'upsert'
            end as event_type,
            cast(is_removed = 0 as utinyint) as is_active,
            case
                when is_removed = 1 and record_kind = 'archive_record'
                    then 'removed_date'
                when is_removed = 1 then 'removed_event'
                else ''
            end as active_to_basis,
            0::utinyint as is_estimated,
            employer_org_number,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from effective_events
        where source_job_ad_id != ''
    ), published_events as (
        select
            lower(sha256(concat_ws(
                '|', source_job_ad_id, 'published', cast(publication_at as varchar)
            ))) as event_uid,
            source_job_ad_id,
            source_record_id,
            version_at as event_at,
            publication_at as effective_at,
            'published' as event_type,
            1::utinyint as is_active,
            '' as active_to_basis,
            0::utinyint as is_estimated,
            employer_org_number,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from {versions}
        where publication_at is not null
    ), historical_end_events as (
        select
            lower(sha256(concat_ws(
                '|', source_job_ad_id,
                case when removed_at is not null then 'removed' else 'scheduled_end' end,
                cast(
                    coalesce(
                        removed_at,
                        least(last_publication_at, application_deadline),
                        last_publication_at,
                        application_deadline
                    ) as varchar
                )
            ))) as event_uid,
            source_job_ad_id,
            source_record_id,
            version_at as event_at,
            coalesce(
                removed_at,
                least(last_publication_at, application_deadline),
                last_publication_at,
                application_deadline
            ) as effective_at,
            case when removed_at is not null then 'removed' else 'scheduled_end' end
                as event_type,
            0::utinyint as is_active,
            case
                when removed_at is not null then 'removed_date'
                when last_publication_at is not null then 'last_publication_date'
                else 'application_deadline'
            end as active_to_basis,
            cast(removed_at is null as utinyint) as is_estimated,
            employer_org_number,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from {versions}
        where version_kind = 'archive_record'
          and is_removed = 0
          and coalesce(
              removed_at,
              least(last_publication_at, application_deadline),
              last_publication_at,
              application_deadline
          ) is not null
    )
    select * from record_events
    union all by name
    select * from published_events
    union all by name
    select * from historical_end_events
    """


def _requirements_sql(*, raw: str, versions: str, requirements: str) -> str:
    requirement_lists = ",\n".join(
        f"""
            list_transform(
                parsed_requirements.{path.removeprefix("$.")},
                item -> struct_pack(
                    requirement_level := '{level}',
                    requirement_type := '{requirement_type}',
                    concept_id := coalesce(item.concept_id, ''),
                    label_original := coalesce(item.label, ''),
                    legacy_ams_taxonomy_id := coalesce(
                        item.legacy_ams_taxonomy_id,
                        ''
                    ),
                    weight := item.weight
                )
            )
        """
        for level, requirement_type, path in _REQUIREMENT_PATHS
    )
    requirements_structure = json.dumps(
        _REQUIREMENTS_STRUCTURE,
        separators=(",", ":"),
    )
    return f"""
    create or replace table {requirements} as
    with parsed as (
        select
            v.version_uid,
            v.source_job_ad_id,
            v.source_url,
            v.source_object_key,
            v.source_run_id,
            v.source_line_number,
            v.ingested_at,
            from_json(r.raw_payload, '{requirements_structure}')
                as parsed_requirements
        from {raw} as r
        inner join {versions} as v
            on v.source_run_id = r.source_run_id
           and v.source_object_key = r.source_object_key
           and v.source_line_number = r.source_line_number
    ), requirement_items as (
        select
            version_uid,
            source_job_ad_id,
            unnest(list_concat(
{requirement_lists}
            )) as requirement,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from parsed
    )
    select
        lower(sha256(concat_ws(
            '|',
            version_uid,
            requirement.requirement_level,
            requirement.requirement_type,
            requirement.concept_id,
            coalesce(cast(requirement.weight as varchar), '')
        ))) as requirement_uid,
        version_uid,
        source_job_ad_id,
        requirement.requirement_level as requirement_level,
        requirement.requirement_type as requirement_type,
        requirement.concept_id as concept_id,
        requirement.label_original as label_original,
        requirement.legacy_ams_taxonomy_id as legacy_ams_taxonomy_id,
        requirement.weight as weight,
        source_url,
        source_object_key,
        source_run_id,
        source_line_number,
        ingested_at
    from requirement_items
    where requirement.concept_id != ''
    """


def _contacts_sql(*, raw: str, versions: str, contacts: str) -> str:
    return f"""
    create or replace table {contacts} as
    with contact_items as (
        select
            v.version_uid,
            v.source_job_ad_id,
            v.version_at,
            cast(contact.key as usmallint) as contact_index,
            contact.value as contact_value,
            v.source_url,
            v.source_object_key,
            v.source_run_id,
            v.source_line_number,
            v.ingested_at
        from {raw} as r
        inner join {versions} as v
            on v.source_run_id = r.source_run_id
           and v.source_object_key = r.source_object_key
           and v.source_line_number = r.source_line_number
        cross join json_each(r.raw_payload, '$.application_contacts') as contact
    ), normalized as (
        select
            version_uid,
            source_job_ad_id,
            version_at,
            contact_index,
            coalesce(json_extract_string(contact_value, '$.name'), '') as name,
            coalesce(json_extract_string(contact_value, '$.description'), '')
                as description,
            coalesce(json_extract_string(contact_value, '$.email'), '') as email,
            coalesce(json_extract_string(contact_value, '$.telephone'), '')
                as telephone,
            coalesce(json_extract_string(contact_value, '$.contact_type'), '')
                as contact_type,
            source_url,
            source_object_key,
            source_run_id,
            source_line_number,
            ingested_at
        from contact_items
    )
    select
        lower(sha256(concat_ws(
            '|', version_uid, 'application_contact', cast(contact_index as varchar)
        ))) as contact_uid,
        version_uid,
        source_job_ad_id,
        version_at,
        contact_index,
        name,
        description,
        email,
        telephone,
        contact_type,
        source_url,
        source_object_key,
        source_run_id,
        source_line_number,
        ingested_at
    from normalized
    """


def _taxonomy(field: str, attribute: str) -> str:
    return (
        "coalesce("
        f"json_extract_string(raw_payload, '$.{field}.{attribute}'), "
        f"json_extract_string(raw_payload, '$.{field}[0].{attribute}'), "
        "''"
        ")"
    )


def _stockholm_timestamp(path: str) -> str:
    return (
        "timezone('Europe/Stockholm', "
        f"try_strptime(json_extract_string(raw_payload, '{path}'), "
        "'%Y-%m-%dT%H:%M:%S'))"
    )


def _count(connection: Any, qualified_table: str) -> int:
    return int(
        connection.execute(f"select count(*) from {qualified_table}").fetchone()[0]
    )
