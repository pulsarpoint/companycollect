from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_company import tables

BOLAGSVERKET_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "organisationsidentitet",
    "organisationsnamn",
    "organisationsform",
    "avregistreringsdatum",
    "avregistreringsorsak",
    "registreringsdatum",
    "verksamhetsbeskrivning",
    "postadress",
)

SCB_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "PeOrgNr",
    "Namn",
    "JurForm",
    "COAdress",
    "Gatuadress",
    "PostNr",
    "PostOrt",
    "RegDatKtid",
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
        _replace_companies_table(connection=connection, loaded_at=loaded_at)
        _replace_company_addresses_table(connection=connection, loaded_at=loaded_at)
        _replace_company_industry_codes_table(connection=connection, loaded_at=loaded_at)

        counts = {
            tables.COMPANIES_TABLE: _table_count(connection, tables.COMPANIES_TABLE),
            tables.COMPANY_ADDRESSES_TABLE: _table_count(
                connection, tables.COMPANY_ADDRESSES_TABLE
            ),
            tables.COMPANY_INDUSTRY_CODES_TABLE: _table_count(
                connection, tables.COMPANY_INDUSTRY_CODES_TABLE
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


def _replace_companies_table(*, connection: Any, loaded_at: datetime) -> None:
    dataset = tables.DLT_DATASET_NAME
    connection.execute(
        f"""
        create or replace table {dataset}.{tables.COMPANIES_TABLE} as
        with bolagsverket_source as (
            select
                *,
                regexp_replace(
                    coalesce(organisationsidentitet, ''),
                    '[^0-9]',
                    '',
                    'g'
                ) as company_id,
                row_number() over (
                    partition by regexp_replace(
                        coalesce(organisationsidentitet, ''),
                        '[^0-9]',
                        '',
                        'g'
                    )
                    order by source_record_id
                ) as company_rank
            from {dataset}.{tables.BOLAGSVERKET_RAW_TABLE}
            where regexp_replace(
                coalesce(organisationsidentitet, ''),
                '[^0-9]',
                '',
                'g'
            ) != ''
        ),
        bolagsverket as (
            select * exclude (company_rank)
            from bolagsverket_source
            where company_rank = 1
        ),
        scb_source as (
            select
                *,
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                row_number() over (
                    partition by regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g')
                    order by source_record_id
                ) as company_rank
            from {dataset}.{tables.SCB_RAW_TABLE}
            where regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') != ''
        ),
        scb as (
            select * exclude (company_rank)
            from scb_source
            where company_rank = 1
        ),
        company_ids as (
            select company_id from bolagsverket
            union
            select company_id from scb
        )
        select
            ids.company_id,
            ids.company_id as registration_number,
            b.organisationsidentitet as bolagsverket_company_id_raw,
            s.PeOrgNr as scb_company_id_raw,
            coalesce(
                nullif(trim(split_part(b.organisationsnamn, '$', 1)), ''),
                nullif(trim(s.Namn), '')
            ) as legal_name,
            b.organisationsnamn as legal_name_raw,
            coalesce(
                nullif(trim(b.organisationsform), ''),
                nullif(trim(s.JurForm), '')
            ) as legal_form_code,
            case
                when b.company_id is not null
                    and nullif(trim(b.avregistreringsdatum), '') is not null
                    then 'inactive'
                else 'active'
            end as status,
            case
                when b.company_id is not null then nullif(trim(b.avregistreringsorsak), '')
                else null
            end as status_reason,
            coalesce(
                try_strptime(nullif(trim(b.registreringsdatum), ''), '%Y-%m-%d')::date,
                try_strptime(nullif(trim(s.RegDatKtid), ''), '%Y%m%d')::date
            ) as incorporation_date,
            try_strptime(nullif(trim(b.avregistreringsdatum), ''), '%Y-%m-%d')::date
                as dissolution_date,
            case
                when b.company_id is not null then nullif(trim(b.verksamhetsbeskrivning), '')
                else null
            end as activity_description,
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
    dataset = tables.DLT_DATASET_NAME
    connection.execute(
        f"""
        create or replace table {dataset}.{tables.COMPANY_ADDRESSES_TABLE} as
        with bolagsverket_addresses as (
            select
                regexp_replace(
                    coalesce(organisationsidentitet, ''),
                    '[^0-9]',
                    '',
                    'g'
                ) as company_id,
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
                source_payload_hash,
                ? as updated_from_raw_at
            from {dataset}.{tables.BOLAGSVERKET_RAW_TABLE}
            where nullif(trim(postadress), '') is not null
                and regexp_replace(
                    coalesce(organisationsidentitet, ''),
                    '[^0-9]',
                    '',
                    'g'
                ) != ''
        ),
        scb_addresses as (
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
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
                nullif(trim(Gatuadress), '') as street_address,
                nullif(trim(COAdress), '') as care_of,
                nullif(trim(PostNr), '') as postal_code,
                nullif(trim(PostOrt), '') as post_town,
                'SE' as country_code,
                source_run_id,
                source_record_id,
                source_payload_hash,
                ? as updated_from_raw_at
            from {dataset}.{tables.SCB_RAW_TABLE}
            where regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') != ''
                and (
                    nullif(trim(COAdress), '') is not null
                    or nullif(trim(Gatuadress), '') is not null
                    or nullif(trim(PostNr), '') is not null
                    or nullif(trim(PostOrt), '') is not null
                )
        )
        select * from bolagsverket_addresses
        union all
        select * from scb_addresses
        """,
        [loaded_at, loaded_at],
    )


def _replace_company_industry_codes_table(
    *,
    connection: Any,
    loaded_at: datetime,
) -> None:
    dataset = tables.DLT_DATASET_NAME
    connection.execute(
        f"""
        create or replace table {dataset}.{tables.COMPANY_INDUSTRY_CODES_TABLE} as
        with candidates as (
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                1 as sequence,
                true as is_primary,
                nullif(trim(Ng1), '') as sni_code,
                'Ng1' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {dataset}.{tables.SCB_RAW_TABLE}
            union all
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                2 as sequence,
                false as is_primary,
                nullif(trim(Ng2), '') as sni_code,
                'Ng2' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {dataset}.{tables.SCB_RAW_TABLE}
            union all
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                3 as sequence,
                false as is_primary,
                nullif(trim(Ng3), '') as sni_code,
                'Ng3' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {dataset}.{tables.SCB_RAW_TABLE}
            union all
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                4 as sequence,
                false as is_primary,
                nullif(trim(Ng4), '') as sni_code,
                'Ng4' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {dataset}.{tables.SCB_RAW_TABLE}
            union all
            select
                regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') as company_id,
                5 as sequence,
                false as is_primary,
                nullif(trim(Ng5), '') as sni_code,
                'Ng5' as source_field,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {dataset}.{tables.SCB_RAW_TABLE}
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
            and sni_code ~ '^[0-9]{{5}}$'
            and sni_code != '00000'
        """,
        [loaded_at],
    )


def _validate_required_columns(connection: Any) -> None:
    required_tables = (
        tables.BOLAGSVERKET_RAW_TABLE,
        tables.SCB_RAW_TABLE,
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
        (tables.BOLAGSVERKET_RAW_TABLE, BOLAGSVERKET_REQUIRED_COLUMNS),
        (tables.SCB_RAW_TABLE, SCB_REQUIRED_COLUMNS),
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
    return _scalar_count(
        connection,
        f"""
        select count(distinct regexp_replace(
            coalesce(organisationsidentitet, ''),
            '[^0-9]',
            '',
            'g'
        ))
        from {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE}
        where regexp_replace(coalesce(organisationsidentitet, ''), '[^0-9]', '', 'g') != ''
        """,
    )


def _scb_company_count(connection: Any) -> int:
    return _scalar_count(
        connection,
        f"""
        select count(distinct regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g'))
        from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
        where regexp_replace(coalesce(PeOrgNr, ''), '[^0-9]', '', 'g') != ''
        """,
    )


def _companies_with_sni_count(connection: Any) -> int:
    return _scalar_count(
        connection,
        f"""
        select count(distinct company_id)
        from {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE}
        """,
    )


def _unknown_sni_count(connection: Any) -> int:
    return _scalar_count(
        connection,
        f"""
        with candidates as (
            select nullif(trim(Ng1), '') as sni_code from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            union all
            select nullif(trim(Ng2), '') as sni_code from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            union all
            select nullif(trim(Ng3), '') as sni_code from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            union all
            select nullif(trim(Ng4), '') as sni_code from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            union all
            select nullif(trim(Ng5), '') as sni_code from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
        )
        select count(*)
        from candidates
        where sni_code is not null
            and (
                sni_code = '00000'
                or sni_code !~ '^[0-9]{{5}}$'
            )
        """,
    )


def _scalar_count(connection: Any, sql: str) -> int:
    value = connection.execute(sql).fetchone()[0]
    return int(value)
