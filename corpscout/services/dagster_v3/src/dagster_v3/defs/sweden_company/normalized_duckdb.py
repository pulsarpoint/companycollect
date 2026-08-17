from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.identity import sweden_identity_sql

BOLAGSVERKET_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_line_number",
    "source_payload_hash",
    "organisationsidentitet",
    "organisationsnamn",
    "organisationsform",
    "namnskyddslopnummer",
    "registreringsland",
    "avregistreringsdatum",
    "avregistreringsorsak",
    "pagandeAvvecklingsEllerOmstruktureringsforfarande",
    "registreringsdatum",
    "verksamhetsbeskrivning",
    "postadress",
)


SCB_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_line_number",
    "source_payload_hash",
    "PeOrgNr",
    "Namn",
    "Foretagsnamn",
    "FtgStat",
    "JEStat",
    "JurForm",
    "COAdress",
    "Gatuadress",
    "PostNr",
    "PostOrt",
    "RegDatKtid",
    "Reklamsparrtyp",
    "Ng1",
    "Ng2",
    "Ng3",
    "Ng4",
    "Ng5",
)


def replace_sweden_company_normalized_tables(
    *,
    connection: Any,
    loaded_at: datetime,
) -> dict[str, int]:
    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        _validate_required_columns(connection)
        _replace_company_registry_states_table(
            connection=connection, loaded_at=loaded_at
        )
        _replace_company_proceedings_table(connection=connection, loaded_at=loaded_at)
        _replace_companies_table(connection=connection, loaded_at=loaded_at)
        _replace_company_addresses_table(connection=connection, loaded_at=loaded_at)
        _replace_company_industry_states_table(
            connection=connection, loaded_at=loaded_at
        )
        _replace_company_industry_codes_table(
            connection=connection, loaded_at=loaded_at
        )

        counts = {
            "companies": _table_count(connection, "companies"),
            "company_addresses": _table_count(connection, "company_addresses"),
            "company_registry_states": _table_count(
                connection, "company_registry_states"
            ),
            "company_proceedings": _table_count(connection, "company_proceedings"),
            "company_industry_states": _table_count(
                connection, "company_industry_states"
            ),
            "company_industry_codes": _table_count(
                connection, "company_industry_codes"
            ),
            "bolagsverket_company_count": _bolagsverket_company_count(connection),
            "scb_company_count": _scb_company_count(connection),
            "companies_with_sni_count": _companies_with_sni_count(connection),
            "unknown_sni_count": _unknown_sni_count(connection),
        }
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return counts


def _replace_company_registry_states_table(
    *, connection: Any, loaded_at: datetime
) -> None:
    bolagsverket_identity = sweden_identity_sql("organisationsidentitet")
    scb_identity = sweden_identity_sql("PeOrgNr")
    connection.execute(
        f"""
        create or replace table sweden_company.company_registry_states as
        with bolagsverket_source as (
            select
                *,
                {bolagsverket_identity} as company_id,
                row_number() over (
                    partition by {bolagsverket_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as company_rank
            from sweden_company.bolagsverket_raw
            where {bolagsverket_identity} != ''
        ),
        scb_source as (
            select
                *,
                {scb_identity} as company_id,
                row_number() over (
                    partition by {scb_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as company_rank
            from sweden_company.scb_raw
            where {scb_identity} != ''
        ),
        candidates as (
            select
                company_id,
                'bolagsverket' as source,
                organisationsidentitet as company_id_raw,
                nullif(trim(split_part(organisationsnamn, '$', 1)), '') as legal_name,
                organisationsnamn as legal_name_raw,
                null::varchar as alternate_name,
                nullif(trim(organisationsform), '') as legal_form_code,
                null::varchar as source_status_code,
                null::varchar as source_secondary_status_code,
                case
                    when nullif(trim(avregistreringsdatum), '') is not null
                        then 'inactive'
                    else 'active'
                end as derived_status,
                nullif(trim(avregistreringsorsak), '') as status_reason,
                try_strptime(
                    nullif(trim(registreringsdatum), ''), '%Y-%m-%d'
                )::date as incorporation_date,
                try_strptime(
                    nullif(trim(avregistreringsdatum), ''), '%Y-%m-%d'
                )::date as dissolution_date,
                nullif(trim(verksamhetsbeskrivning), '') as activity_description,
                nullif(trim(namnskyddslopnummer), '') as name_protection_sequence,
                nullif(trim(registreringsland), '') as registration_country_code,
                null::varchar as marketing_block_code,
                nullif(
                    trim(pagandeAvvecklingsEllerOmstruktureringsforfarande), ''
                ) as proceedings_raw,
                source_run_id,
                source_record_id,
                source_payload_hash
            from bolagsverket_source
            where company_rank = 1

            union all

            select
                company_id,
                'scb' as source,
                PeOrgNr as company_id_raw,
                nullif(trim(Namn), '') as legal_name,
                null::varchar as legal_name_raw,
                nullif(trim(Foretagsnamn), '') as alternate_name,
                nullif(trim(JurForm), '') as legal_form_code,
                nullif(trim(FtgStat), '') as source_status_code,
                nullif(trim(JEStat), '') as source_secondary_status_code,
                null::varchar as derived_status,
                null::varchar as status_reason,
                try_strptime(nullif(trim(RegDatKtid), ''), '%Y%m%d')::date
                    as incorporation_date,
                null::date as dissolution_date,
                null::varchar as activity_description,
                null::varchar as name_protection_sequence,
                null::varchar as registration_country_code,
                nullif(trim(Reklamsparrtyp), '') as marketing_block_code,
                null::varchar as proceedings_raw,
                source_run_id,
                source_record_id,
                source_payload_hash
            from scb_source
            where company_rank = 1
        ),
        states as (
            select
                *,
                cast(1 as utinyint) as has_company,
                sha256(concat_ws(
                    chr(31),
                    coalesce(company_id_raw, ''),
                    coalesce(legal_name, ''),
                    coalesce(legal_name_raw, ''),
                    coalesce(alternate_name, ''),
                    coalesce(legal_form_code, ''),
                    coalesce(source_status_code, ''),
                    coalesce(source_secondary_status_code, ''),
                    coalesce(derived_status, ''),
                    coalesce(status_reason, ''),
                    coalesce(cast(incorporation_date as varchar), ''),
                    coalesce(cast(dissolution_date as varchar), ''),
                    coalesce(activity_description, ''),
                    coalesce(name_protection_sequence, ''),
                    coalesce(registration_country_code, ''),
                    coalesce(marketing_block_code, ''),
                    coalesce(proceedings_raw, ''),
                    '1'
                )) as state_fingerprint
            from candidates
        )
        select
            * exclude (state_fingerprint),
            ? as updated_from_raw_at,
            has_company,
            state_fingerprint,
            state_fingerprint as observation_fingerprint,
            ? as observed_at
        from states
        """,
        [loaded_at, loaded_at],
    )


def _replace_company_proceedings_table(*, connection: Any, loaded_at: datetime) -> None:
    connection.execute(
        """
        create or replace table sweden_company.company_proceedings as
        with expanded as (
            select
                company_id,
                source,
                nullif(trim(proceeding), '') as raw_proceeding,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_registry_states,
                unnest(string_split(coalesce(proceedings_raw, ''), '|'))
                    as proceedings(proceeding)
            where source = 'bolagsverket'
        ),
        parsed as (
            select
                company_id,
                source,
                nullif(trim(split_part(raw_proceeding, '$', 1)), '')
                    as proceeding_code,
                try_strptime(
                    nullif(trim(split_part(raw_proceeding, '$', 2)), ''),
                    '%Y-%m-%d'
                )::date as effective_date,
                raw_proceeding,
                source_run_id,
                source_record_id,
                source_payload_hash
            from expanded
            where raw_proceeding is not null
        ),
        identified as (
            select
                *,
                sha256(concat_ws(
                    chr(31),
                    coalesce(proceeding_code, ''),
                    coalesce(cast(effective_date as varchar), ''),
                    coalesce(raw_proceeding, '')
                )) as proceeding_identity
            from parsed
        ),
        deduplicated as (
            select *
            from identified
            qualify row_number() over (
                partition by company_id, source, proceeding_identity
                order by source_record_id, source_payload_hash
            ) = 1
        )
        select
            company_id,
            source,
            proceeding_code,
            effective_date,
            raw_proceeding,
            proceeding_identity,
            source_run_id,
            source_record_id,
            source_payload_hash,
            ? as updated_from_raw_at,
            cast(1 as utinyint) as has_proceeding,
            proceeding_identity as proceeding_fingerprint,
            proceeding_identity as observation_fingerprint,
            ? as observed_at
        from deduplicated
        """,
        [loaded_at, loaded_at],
    )


def _replace_companies_table(*, connection: Any, loaded_at: datetime) -> None:
    connection.execute(
        """
        create or replace table sweden_company.companies as
        with bolagsverket as (
            select *
            from sweden_company.company_registry_states
            where source = 'bolagsverket'
        ),
        scb as (
            select *
            from sweden_company.company_registry_states
            where source = 'scb'
        ),
        company_ids as (
            select company_id from bolagsverket
            union
            select company_id from scb
        )
        select
            ids.company_id,
            ids.company_id as registration_number,
            b.company_id_raw as bolagsverket_company_id_raw,
            s.company_id_raw as scb_company_id_raw,
            coalesce(
                b.legal_name,
                s.legal_name
            ) as legal_name,
            b.legal_name_raw,
            coalesce(
                b.legal_form_code,
                s.legal_form_code
            ) as legal_form_code,
            coalesce(b.derived_status, 'active') as status,
            b.status_reason,
            coalesce(b.incorporation_date, s.incorporation_date) as incorporation_date,
            b.dissolution_date,
            b.activity_description,
            coalesce(b.source_run_id, s.source_run_id) as source_run_id,
            b.source_record_id as bolagsverket_source_record_id,
            s.source_record_id as scb_source_record_id,
            b.source_payload_hash as bolagsverket_source_payload_hash,
            s.source_payload_hash as scb_source_payload_hash,
            ? as updated_from_raw_at
        from company_ids ids
        left join bolagsverket b on b.company_id = ids.company_id
        left join scb s on s.company_id = ids.company_id
        """,
        [loaded_at],
    )


def _replace_company_addresses_table(*, connection: Any, loaded_at: datetime) -> None:
    bolagsverket_identity = sweden_identity_sql("organisationsidentitet")
    scb_identity = sweden_identity_sql("PeOrgNr")
    connection.execute(
        f"""
        create or replace table sweden_company.company_addresses as
        with bolagsverket_source as (
            select
                *,
                {bolagsverket_identity} as company_id,
                row_number() over (
                    partition by {bolagsverket_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as address_rank
            from sweden_company.bolagsverket_raw
            where {bolagsverket_identity} != ''
        ),
        bolagsverket_addresses as (
            select
                company_id,
                'postal' as address_type,
                'bolagsverket' as source,
                postadress as raw_address,
                nullif(trim(split_part(postadress, '$', 1)), '') as street_address,
                nullif(trim(split_part(postadress, '$', 2)), '') as care_of,
                nullif(trim(split_part(postadress, '$', 4)), '') as postal_code,
                nullif(trim(split_part(postadress, '$', 3)), '') as post_town,
                case
                    when nullif(trim(split_part(postadress, '$', 5)), '') is null then null
                    else split_part(trim(split_part(postadress, '$', 5)), '-', 1)
                end as country_code,
                source_run_id,
                source_record_id,
                source_payload_hash
            from bolagsverket_source
            where address_rank = 1
        ),
        scb_source as (
            select
                *,
                {scb_identity} as company_id,
                row_number() over (
                    partition by {scb_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as address_rank
            from sweden_company.scb_raw
            where {scb_identity} != ''
        ),
        scb_address_parts as (
            select
                *,
                lower(trim(PostOrt)) = 'utlandet' as registry_marks_foreign,
                regexp_matches(
                    coalesce(Gatuadress, ''),
                    '(^|[[:space:]])PL[[:space:]]{{2,}}'
                ) as has_polish_country_marker,
                regexp_matches(
                    coalesce(Gatuadress, ''),
                    '(^|[[:space:]])[0-9]{{2}}-[0-9]{{3}}[[:space:]]+.+$'
                ) as has_embedded_polish_postcode,
                regexp_extract(
                    coalesce(Gatuadress, ''),
                    '(^|[[:space:]])([0-9]{{2}}-[0-9]{{3}})[[:space:]]+.+$',
                    2
                ) as embedded_polish_postcode,
                trim(regexp_extract(
                    coalesce(Gatuadress, ''),
                    '(^|[[:space:]])[0-9]{{2}}-[0-9]{{3}}[[:space:]]+(.+)$',
                    2
                )) as embedded_polish_post_town
            from scb_source
        ),
        scb_addresses as (
            select
                company_id,
                'visiting_or_postal' as address_type,
                'scb' as source,
                case
                    when nullif(trim(Gatuadress), '') is not null
                        and (
                            nullif(trim(PostNr), '') is not null
                            or nullif(trim(PostOrt), '') is not null
                        )
                        then concat(
                            trim(Gatuadress),
                            ', ',
                            trim(concat_ws(' ', nullif(trim(PostNr), ''), nullif(trim(PostOrt), '')))
                        )
                    else trim(concat_ws(
                        ' ',
                        nullif(trim(Gatuadress), ''),
                        nullif(trim(PostNr), ''),
                        nullif(trim(PostOrt), '')
                    ))
                end as raw_address,
                case
                    when registry_marks_foreign and has_embedded_polish_postcode
                        then nullif(trim(regexp_replace(
                            Gatuadress,
                            '[[:space:]]+(PL[[:space:]]+)?[0-9]{{2}}-[0-9]{{3}}[[:space:]]+.+$',
                            ''
                        )), '')
                    else nullif(trim(Gatuadress), '')
                end as street_address,
                nullif(trim(COAdress), '') as care_of,
                case
                    when registry_marks_foreign and has_embedded_polish_postcode
                        then nullif(embedded_polish_postcode, '')
                    else nullif(trim(PostNr), '')
                end as postal_code,
                case
                    when registry_marks_foreign and has_embedded_polish_postcode
                        then nullif(embedded_polish_post_town, '')
                    else nullif(trim(PostOrt), '')
                end as post_town,
                case
                    when not registry_marks_foreign then 'SE'
                    when has_polish_country_marker or has_embedded_polish_postcode
                        then 'PL'
                    else null
                end as country_code,
                source_run_id,
                source_record_id,
                source_payload_hash
            from scb_address_parts
            where address_rank = 1
        ),
        candidates as (
            select * from bolagsverket_addresses
            union all
            select * from scb_addresses
        ),
        address_states as (
            select
                *,
                cast(
                    coalesce(trim(raw_address), '') != ''
                    or coalesce(trim(street_address), '') != ''
                    or coalesce(trim(care_of), '') != ''
                    or coalesce(trim(postal_code), '') != ''
                    or coalesce(trim(post_town), '') != ''
                    as utinyint
                ) as has_address
            from candidates
        )
        select
            company_id,
            address_type,
            source,
            raw_address,
            street_address,
            care_of,
            postal_code,
            post_town,
            country_code,
            source_run_id,
            source_record_id,
            source_payload_hash,
            ? as updated_from_raw_at,
            has_address,
            sha256(concat_ws(
                chr(31),
                coalesce(raw_address, ''),
                coalesce(street_address, ''),
                coalesce(care_of, ''),
                coalesce(postal_code, ''),
                coalesce(post_town, ''),
                coalesce(country_code, ''),
                cast(has_address as varchar)
            )) as address_fingerprint,
            sha256(concat_ws(
                chr(31),
                coalesce(raw_address, ''),
                coalesce(street_address, ''),
                coalesce(care_of, ''),
                coalesce(postal_code, ''),
                coalesce(post_town, ''),
                coalesce(country_code, ''),
                cast(has_address as varchar)
            )) as observation_fingerprint,
            ? as observed_at
        from address_states
        """,
        [loaded_at, loaded_at],
    )


def _replace_company_industry_states_table(
    *,
    connection: Any,
    loaded_at: datetime,
) -> None:
    scb_identity = sweden_identity_sql("PeOrgNr")
    connection.execute(
        f"""
        create or replace table sweden_company.company_industry_states as
        with scb_source as (
            select
                *,
                {scb_identity} as company_id,
                row_number() over (
                    partition by {scb_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as company_rank
            from sweden_company.scb_raw
            where {scb_identity} != ''
        ),
        candidates as (
            select
                company_id,
                'scb' as source,
                nullif(trim(Ng1), '') as ng1_code,
                nullif(trim(Ng2), '') as ng2_code,
                nullif(trim(Ng3), '') as ng3_code,
                nullif(trim(Ng4), '') as ng4_code,
                nullif(trim(Ng5), '') as ng5_code,
                source_run_id,
                source_record_id,
                source_payload_hash
            from scb_source
            where company_rank = 1
        ),
        states as (
            select
                *,
                cast(
                    ng1_code is not null
                    or ng2_code is not null
                    or ng3_code is not null
                    or ng4_code is not null
                    or ng5_code is not null
                    as utinyint
                ) as has_industry,
                sha256(concat_ws(
                    chr(31),
                    coalesce(ng1_code, ''),
                    coalesce(ng2_code, ''),
                    coalesce(ng3_code, ''),
                    coalesce(ng4_code, ''),
                    coalesce(ng5_code, ''),
                    cast(
                        ng1_code is not null
                        or ng2_code is not null
                        or ng3_code is not null
                        or ng4_code is not null
                        or ng5_code is not null
                        as varchar
                    )
                )) as state_fingerprint
            from candidates
        )
        select
            * exclude (state_fingerprint),
            ? as updated_from_raw_at,
            has_industry,
            state_fingerprint,
            state_fingerprint as observation_fingerprint,
            ? as observed_at
        from states
        """,
        [loaded_at, loaded_at],
    )


def _replace_company_industry_codes_table(
    *,
    connection: Any,
    loaded_at: datetime,
) -> None:
    connection.execute(
        """
        create or replace table sweden_company.company_industry_codes as
        with candidates as (
            select
                company_id,
                1 as sequence,
                true as is_primary,
                ng1_code as sni_code,
                'Ng1' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_industry_states
            union all
            select
                company_id,
                2 as sequence,
                false as is_primary,
                ng2_code as sni_code,
                'Ng2' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_industry_states
            union all
            select
                company_id,
                3 as sequence,
                false as is_primary,
                ng3_code as sni_code,
                'Ng3' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_industry_states
            union all
            select
                company_id,
                4 as sequence,
                false as is_primary,
                ng4_code as sni_code,
                'Ng4' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_industry_states
            union all
            select
                company_id,
                5 as sequence,
                false as is_primary,
                ng5_code as sni_code,
                'Ng5' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from sweden_company.company_industry_states
        )
        select
            company_id,
            sequence,
            is_primary,
            sni_code,
            left(sni_code, 4) as nace_rev2_class_code,
            source_field,
            source_run_id,
            source_record_id,
            source_payload_hash,
            ? as updated_from_raw_at
        from candidates
        where company_id != ''
            and sni_code ~ '^[0-9]{5}$'
            and sni_code != '00000'
        """,
        [loaded_at],
    )


def _validate_required_columns(connection: Any) -> None:
    required_tables = (
        "bolagsverket_raw",
        "scb_raw",
    )
    missing_tables = [
        table_name
        for table_name in required_tables
        if not _table_exists(connection, table_name)
    ]
    if missing_tables:
        raise ValueError(
            "Sweden company raw tables missing required raw tables: "
            + ", ".join(missing_tables)
        )

    missing_columns: list[str] = []
    for table_name, required_columns in (
        ("bolagsverket_raw", BOLAGSVERKET_REQUIRED_COLUMNS),
        ("scb_raw", SCB_REQUIRED_COLUMNS),
    ):
        existing_columns = _table_columns(connection, table_name)
        missing_columns.extend(
            f"{table_name}.{column}"
            for column in required_columns
            if column not in existing_columns
        )
    if missing_columns:
        raise ValueError(
            "Sweden company raw tables missing required columns: "
            + ", ".join(missing_columns)
        )


def _table_exists(connection: Any, table_name: str) -> bool:
    value = connection.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ? and table_name = ?
        """,
        [tables.DLT_DATASET_NAME, table_name],
    ).fetchone()[0]
    return int(value) > 0


def _table_columns(connection: Any, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = ? and table_name = ?
        """,
        [tables.DLT_DATASET_NAME, table_name],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _table_count(connection: Any, table_name: str) -> int:
    value = connection.execute(
        f"select count(*) from {tables.DLT_DATASET_NAME}.{table_name}"
    ).fetchone()[0]
    return int(value)


def _bolagsverket_company_count(connection: Any) -> int:
    bolagsverket_identity = sweden_identity_sql("organisationsidentitet")
    return _scalar_count(
        connection,
        f"""
        select count(distinct {bolagsverket_identity})
        from sweden_company.bolagsverket_raw
        where {bolagsverket_identity} != ''
        """,
    )


def _scb_company_count(connection: Any) -> int:
    scb_identity = sweden_identity_sql("PeOrgNr")
    return _scalar_count(
        connection,
        f"""
        select count(distinct {scb_identity})
        from sweden_company.scb_raw
        where {scb_identity} != ''
        """,
    )


def _companies_with_sni_count(connection: Any) -> int:
    return _scalar_count(
        connection,
        """
        select count(distinct company_id)
        from sweden_company.company_industry_codes
        """,
    )


def _unknown_sni_count(connection: Any) -> int:
    return _scalar_count(
        connection,
        """
        with candidates as (
            select nullif(trim(Ng1), '') as sni_code from sweden_company.scb_raw
            union all
            select nullif(trim(Ng2), '') as sni_code from sweden_company.scb_raw
            union all
            select nullif(trim(Ng3), '') as sni_code from sweden_company.scb_raw
            union all
            select nullif(trim(Ng4), '') as sni_code from sweden_company.scb_raw
            union all
            select nullif(trim(Ng5), '') as sni_code from sweden_company.scb_raw
        )
        select count(*)
        from candidates
        where sni_code is not null
            and (
                sni_code = '00000'
                or sni_code !~ '^[0-9]{5}$'
            )
        """,
    )


def _scalar_count(connection: Any, sql: str) -> int:
    value = connection.execute(sql).fetchone()[0]
    return int(value)
