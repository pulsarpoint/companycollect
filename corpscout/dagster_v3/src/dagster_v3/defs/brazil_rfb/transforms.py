from __future__ import annotations

import os
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables

STATUS_EN_BY_CODE = {
    "01": "Null",
    "02": "Active",
    "03": "Suspended",
    "04": "Unfit",
    "08": "Closed",
}
COMPANY_SIZE_EN_BY_CODE = {
    "00": "Not informed",
    "01": "Micro",
    "03": "Small",
    "05": "Other",
}
DEFAULT_DUCKDB_THREADS = "4"
DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE = "100GiB"
DEFAULT_DUCKDB_TEMP_DIRECTORY = Path("data/brazil_rfb_duckdb_tmp")


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _case_map(column: str, mapping: dict[str, str]) -> str:
    cases = " ".join(
        f"when {_sql_literal(code)} then {_sql_literal(label)}"
        for code, label in mapping.items()
    )
    return f"case {column} {cases} else '' end"


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    clean_value = value.strip()
    return clean_value if clean_value else None


def apply_brazil_rfb_duckdb_runtime_settings(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    memory_limit = _env_value("BRAZIL_RFB_DUCKDB_MEMORY_LIMIT")
    temp_directory = Path(
        _env_value("BRAZIL_RFB_DUCKDB_TEMP_DIRECTORY")
        or DEFAULT_DUCKDB_TEMP_DIRECTORY
    )
    max_temp_directory_size = (
        _env_value("BRAZIL_RFB_DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
        or DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE
    )
    threads = _env_value("BRAZIL_RFB_DUCKDB_THREADS") or DEFAULT_DUCKDB_THREADS

    temp_directory.mkdir(parents=True, exist_ok=True)
    connection.execute("set preserve_insertion_order = false")
    connection.execute(f"set temp_directory = {_sql_literal(temp_directory)}")
    connection.execute(
        f"set max_temp_directory_size = {_sql_literal(max_temp_directory_size)}"
    )
    connection.execute(f"set threads = {int(threads)}")
    if memory_limit is not None:
        connection.execute(f"set memory_limit = {_sql_literal(memory_limit)}")


def build_brazil_rfb_companies_and_establishments(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    status_en = _case_map("e.situacao_cadastral", STATUS_EN_BY_CODE)
    size_en = _case_map("emp.porte", COMPANY_SIZE_EN_BY_CODE)

    with duckdb.connect(str(database_path)) as connection:
        apply_brazil_rfb_duckdb_runtime_settings(connection)
        connection.execute(f"create schema if not exists {dataset}")
        connection.execute(
            f"""
            create or replace table {dataset}.{tables.ESTABLISHMENTS_TABLE} as
            select
                'BR' as country_iso2,
                'brazil_rfb' as source_slug,
                {_sql_literal(source_run_id)} as source_run_id,
                concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) as source_record_id,
                concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) as cnpj,
                e.cnpj_basico,
                e.cnpj_ordem,
                e.cnpj_dv,
                case when e.identificador_matriz_filial = '1' then 1 else 0 end as is_headquarters,
                coalesce(trim(e.nome_fantasia), '') as trade_name,
                coalesce(e.situacao_cadastral, '') as status_code,
                {status_en} as status_en,
                try_strptime(nullif(e.data_situacao_cadastral, ''), '%Y%m%d')::date as status_date,
                coalesce(e.motivo_situacao_cadastral, '') as status_reason_code,
                try_strptime(nullif(e.data_inicio_atividade, ''), '%Y%m%d')::date as activity_start_date,
                coalesce(e.cnae_fiscal_principal, '') as primary_cnae_code,
                coalesce(e.cnae_fiscal_secundaria, '') as secondary_cnae_codes,
                coalesce(e.tipo_logradouro, '') as street_type,
                coalesce(e.logradouro, '') as street_name,
                coalesce(e.numero, '') as street_number,
                coalesce(e.complemento, '') as address_complement,
                coalesce(e.bairro, '') as district,
                coalesce(e.cep, '') as postal_code,
                coalesce(e.uf, '') as state,
                coalesce(e.municipio, '') as municipality_code,
                coalesce(m.description_pt, '') as municipality_name,
                coalesce(e.ddd_1, '') as ddd_1,
                coalesce(e.telefone_1, '') as telefone_1,
                coalesce(e.ddd_2, '') as ddd_2,
                coalesce(e.telefone_2, '') as telefone_2,
                coalesce(e.ddd_fax, '') as ddd_fax,
                coalesce(e.fax, '') as fax,
                coalesce(e.correio_eletronico, '') as correio_eletronico,
                coalesce(e.situacao_especial, '') as situacao_especial,
                coalesce(e.data_situacao_especial, '') as data_situacao_especial,
                now() as resolved_at
            from {dataset}.{tables.RAW_TABLE_BY_FAMILY["estabelecimentos"]} e
            left join {dataset}.{tables.RAW_TABLE_BY_FAMILY["municipios"]} m
                on m.code = e.municipio
            """
        )
        connection.execute(
            f"""
            create or replace table {dataset}.{tables.COMPANIES_TABLE} as
            with ranked_establishments as (
                select
                    *,
                    row_number() over (
                        partition by cnpj_basico
                        order by is_headquarters desc, (status_code = '02') desc, cnpj_ordem, cnpj
                    ) as rn
                from {dataset}.{tables.ESTABLISHMENTS_TABLE}
            ),
            picked as (
                select * from ranked_establishments where rn = 1
            ),
            simples_current as (
                select cnpj_basico, opcao_simples, opcao_mei
                from {dataset}.{tables.RAW_TABLE_BY_FAMILY["simples"]}
            )
            select
                'BR' as country_iso2,
                'brazil_rfb' as source_slug,
                {_sql_literal(source_run_id)} as source_run_id,
                emp.cnpj_basico as source_record_id,
                emp.cnpj_basico,
                p.cnpj as headquarters_cnpj,
                coalesce(trim(emp.razao_social), '') as legal_name,
                coalesce(p.trade_name, '') as trade_name,
                coalesce(emp.natureza_juridica, '') as legal_nature_code,
                coalesce(n.description_pt, '') as legal_nature_description_pt,
                coalesce(emp.porte, '') as company_size_code,
                {size_en} as company_size_en,
                try_cast(replace(replace(emp.capital_social, '.', ''), ',', '.') as decimal(18, 2))
                    as share_capital_amount_original,
                p.status_code,
                p.status_en,
                case when p.status_code = '02' then 1 else 0 end as is_active,
                p.status_date,
                p.activity_start_date,
                p.street_type,
                p.street_name,
                p.street_number,
                p.address_complement,
                p.district,
                p.postal_code,
                p.state,
                p.municipality_code,
                p.municipality_name,
                case when s.opcao_simples = 'S' then 1 else 0 end as is_simples,
                case when s.opcao_mei = 'S' then 1 else 0 end as is_mei,
                now() as resolved_at
            from {dataset}.{tables.RAW_TABLE_BY_FAMILY["empresas"]} emp
            left join picked p on p.cnpj_basico = emp.cnpj_basico
            left join {dataset}.{tables.RAW_TABLE_BY_FAMILY["naturezas"]} n
                on n.code = emp.natureza_juridica
            left join simples_current s on s.cnpj_basico = emp.cnpj_basico
            """
        )
        companies = int(
            connection.execute(
                f"select count(*) from {dataset}.{tables.COMPANIES_TABLE}"
            ).fetchone()[0]
        )
        establishments = int(
            connection.execute(
                f"select count(*) from {dataset}.{tables.ESTABLISHMENTS_TABLE}"
            ).fetchone()[0]
        )
        active_companies = int(
            connection.execute(
                f"select count(*) from {dataset}.{tables.COMPANIES_TABLE} where is_active = 1"
            ).fetchone()[0]
        )

    if companies == 0:
        raise ValueError("Brazil RFB company transform produced no rows")
    return {
        "companies": companies,
        "establishments": establishments,
        "active_companies": active_companies,
    }
