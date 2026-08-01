"""Raw PNCP JSON to typed contract candidates, in set-based DuckDB SQL.

Each line is kept whole as JSON and every field is read out by key, rather than
letting DuckDB infer a schema from the file. The payload has four nested objects
(``orgaoEntidade``, ``unidadeOrgao`` and their sub-rogated twins) whose inferred
struct shape would vary with whichever rows a partition happens to contain, so a
month with no sub-rogated buyer would produce a different schema from one that
has them. Reading by key is uniform across every partition.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_pncp import tables

# tipoPessoa is a two-letter code whose values differ by one character, so the
# comparison is exact. The same shape of trap as Sweden's "Inte direktivstyrd"
# containing "direktivstyrd" -- a prefix or LIKE match would classify natural
# persons as companies.
_ELIGIBILITY_SQL = f"""
        case
            when supplier_person_type = '{tables.PERSON_TYPE_NATURAL_PERSON}'
                then 'natural_person'
            when supplier_cnpj = '' then 'missing_supplier_id'
            when length(supplier_cnpj) != 14 then 'invalid_supplier_id'
            when supplier_person_type != '{tables.PERSON_TYPE_LEGAL_ENTITY}'
                then 'unknown_person_type'
            else 'eligible'
        end
"""

_CANDIDATES_BUILD_TABLE = "_br_pncp_contract_candidates_build"


def load_raw_pages(
    *,
    connection: Any,
    page_dir: Path,
    source_run_id: str,
    source_retrieved_at: datetime,
    raw_table: str = tables.RAW_TABLE,
) -> int:
    """Load a partition's page files into the raw table, one row per contract."""
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {tables.DUCKDB_SCHEMA}.{raw_table} as
        select
            cast(? as varchar) as source_run_id,
            row_number() over ()::ubigint as source_line_number,
            cast(? as timestamp) as source_retrieved_at,
            json
        from read_json_objects(?, format='newline_delimited')
        """,
        [source_run_id, source_retrieved_at, f"{page_dir}/page-*.jsonl"],
    )
    return int(
        connection.execute(
            f"select count() from {tables.DUCKDB_SCHEMA}.{raw_table}"
        ).fetchone()[0]
    )


def build_contract_candidates(
    *,
    connection: Any,
    source_run_id: str,
    resolved_at: datetime,
    raw_table: str = tables.RAW_TABLE,
    candidates_table: str = tables.CANDIDATES_TABLE,
    partition: str | None = None,
) -> dict[str, int]:
    """Project raw JSON into the candidate columns, typed and normalised."""
    connection.execute(
        f"""
        create or replace temp table {_CANDIDATES_BUILD_TABLE} as
        with extracted as (
            select
                source_run_id,
                source_line_number,
                source_retrieved_at,
                coalesce(json ->> '$.numeroControlePNCP', '') as numero_controle_pncp,
                coalesce(json ->> '$.numeroControlePncpCompra', '')
                    as numero_controle_pncp_compra,
                try_cast(json ->> '$.anoContrato' as usmallint) as ano_contrato,
                try_cast(json ->> '$.sequencialContrato' as uinteger)
                    as sequencial_contrato,
                coalesce(json ->> '$.numeroContratoEmpenho', '')
                    as numero_contrato_empenho,
                try_cast(json ->> '$.numeroRetificacao' as usmallint)
                    as numero_retificacao,
                coalesce(json ->> '$.processo', '') as processo,
                -- Kept verbatim (§7a): this is the field as PNCP published it.
                -- For the live API that is a nested id/nome object, so the text
                -- form is the whole object -- which is why the parsed pair below
                -- exists rather than replacing it.
                coalesce(json ->> '$.tipoContrato', '') as tipo_contrato,
                coalesce(json ->> '$.categoriaProcesso', '') as categoria_processo,
                -- Split out so the domain is groupable and filterable, and so a
                -- reader is never shown the blob. `->>` on a nested path returns
                -- NULL when the value is a plain string (older snapshots, and
                -- this module's own earlier test fixture), so fall back to the
                -- whole value for the name and leave the id absent.
                try_cast(json ->> '$.tipoContrato.id' as usmallint)
                    as tipo_contrato_id,
                coalesce(
                    json ->> '$.tipoContrato.nome',
                    json ->> '$.tipoContrato',
                    ''
                ) as tipo_contrato_name,
                try_cast(json ->> '$.categoriaProcesso.id' as usmallint)
                    as categoria_processo_id,
                coalesce(
                    json ->> '$.categoriaProcesso.nome',
                    json ->> '$.categoriaProcesso',
                    ''
                ) as categoria_processo_name,
                coalesce(json ->> '$.objetoContrato', '') as objeto_contrato,
                coalesce(json ->> '$.informacaoComplementar', '')
                    as informacao_complementar,
                -- Dates arrive as ISO datetimes ("2025-06-01T00:00:13"); the
                -- date is the meaningful part for every one of them.
                try_cast(json ->> '$.dataPublicacaoPncp' as date)
                    as data_publicacao_pncp,
                try_cast(json ->> '$.dataAssinatura' as date) as data_assinatura,
                try_cast(json ->> '$.dataVigenciaInicio' as date)
                    as data_vigencia_inicio,
                try_cast(json ->> '$.dataVigenciaFim' as date) as data_vigencia_fim,
                try_cast(json ->> '$.dataAtualizacaoGlobal' as timestamp)
                    as data_atualizacao_global,
                -- Punctuation is stripped: a CNPJ is 14 digits however it is
                -- written, and the register stores it unpunctuated.
                coalesce(regexp_replace(
                    coalesce(json ->> '$.niFornecedor', ''), '[^0-9]', '', 'g'
                ), '') as supplier_cnpj,
                coalesce(json ->> '$.nomeRazaoSocialFornecedor', '') as supplier_name,
                coalesce(json ->> '$.tipoPessoa', '') as supplier_person_type,
                coalesce(json ->> '$.codigoPaisFornecedor', '') as supplier_country_code,
                coalesce(regexp_replace(
                    coalesce(json ->> '$.niFornecedorSubContratado', ''),
                    '[^0-9]', '', 'g'
                ), '') as subcontractor_cnpj,
                coalesce(json ->> '$.nomeFornecedorSubContratado', '')
                    as subcontractor_name,
                coalesce(json ->> '$.tipoPessoaSubContratada', '')
                    as subcontractor_person_type,
                coalesce(regexp_replace(
                    coalesce(json ->> '$.orgaoEntidade.cnpj', ''), '[^0-9]', '', 'g'
                ), '') as buyer_cnpj,
                coalesce(json ->> '$.orgaoEntidade.razaoSocial', '') as buyer_name,
                coalesce(json ->> '$.orgaoEntidade.poderId', '') as buyer_power_id,
                coalesce(json ->> '$.orgaoEntidade.esferaId', '') as buyer_sphere_id,
                coalesce(json ->> '$.unidadeOrgao.codigoUnidade', '') as buyer_unit_code,
                coalesce(json ->> '$.unidadeOrgao.nomeUnidade', '') as buyer_unit_name,
                coalesce(json ->> '$.unidadeOrgao.ufSigla', '') as buyer_state_code,
                -- IBGE municipality code: Brazil's standard geographic key, and
                -- the only field on this endpoint that cannot be derived from
                -- what we already keep. Without it every join to population, GDP
                -- or regional data goes through fuzzy matching on municipioNome.
                -- String, not an integer: it is an identifier, not a quantity.
                coalesce(json ->> '$.unidadeOrgao.codigoIbge', '')
                    as buyer_municipality_ibge_code,
                coalesce(json ->> '$.unidadeOrgao.municipioNome', '')
                    as buyer_municipality,
                -- All five value fields. Which one is *the* contract value is
                -- decided in the view against real data, not here: the API
                -- documents none of them.
                try_cast(json ->> '$.valorInicial' as decimal(38, 2)) as valor_inicial,
                try_cast(json ->> '$.valorParcela' as decimal(38, 2)) as valor_parcela,
                try_cast(json ->> '$.valorGlobal' as decimal(38, 2)) as valor_global,
                try_cast(json ->> '$.valorAcumulado' as decimal(38, 2))
                    as valor_acumulado,
                try_cast(json ->> '$.numeroParcelas' as uinteger) as numero_parcelas,
                try_cast(json ->> '$.receita' as boolean)::tinyint
                    as is_revenue_contract,
                try_cast(json ->> '$.emendaParlamentar' as boolean)::tinyint
                    as parliamentary_amendment,
                try_cast(json ->> '$.frutoAdesao' as boolean)::tinyint as from_adhesion,
                try_cast(json ->> '$.temRemanejamento' as boolean)::tinyint
                    as has_reallocation
            from {tables.DUCKDB_SCHEMA}.{raw_table}
        )
        select
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            lower(sha256(concat_ws(
                '|', numero_controle_pncp, supplier_cnpj,
                cast(source_line_number as varchar)
            ))) as source_record_id,
            -- PNCP publishes an address per contract, unlike Sweden's bulk CSV.
            -- Empty rather than a broken guess when a component is missing.
            case
                when buyer_cnpj = '' or ano_contrato is null
                     or sequencial_contrato is null then ''
                else 'https://pncp.gov.br/app/contratos/' || buyer_cnpj || '/'
                     || cast(ano_contrato as varchar) || '/'
                     || cast(sequencial_contrato as varchar)
            end as source_url,
            numero_controle_pncp,
            numero_controle_pncp_compra,
            ano_contrato,
            sequencial_contrato,
            numero_contrato_empenho,
            numero_retificacao,
            processo,
            tipo_contrato,
            categoria_processo,
            tipo_contrato_id,
            tipo_contrato_name,
            categoria_processo_id,
            categoria_processo_name,
            objeto_contrato,
            informacao_complementar,
            data_publicacao_pncp,
            data_assinatura,
            data_vigencia_inicio,
            data_vigencia_fim,
            data_atualizacao_global,
            supplier_cnpj,
            -- The company base is the first 8 digits. Both are kept: rolling up
            -- is computable from the establishment, never the reverse.
            case when length(supplier_cnpj) = 14
                 then substr(supplier_cnpj, 1, 8) else '' end as supplier_cnpj_basico,
            supplier_name,
            supplier_person_type,
            supplier_country_code,
            subcontractor_cnpj,
            subcontractor_name,
            subcontractor_person_type,
            buyer_cnpj,
            buyer_name,
            buyer_power_id,
            buyer_sphere_id,
            buyer_unit_code,
            buyer_unit_name,
            buyer_state_code,
            buyer_municipality,
            buyer_municipality_ibge_code,
            valor_inicial,
            valor_parcela,
            valor_global,
            valor_acumulado,
            numero_parcelas,
            is_revenue_contract,
            parliamentary_amendment,
            from_adhesion,
            has_reallocation,
            {_ELIGIBILITY_SQL} as match_eligibility,
            source_retrieved_at,
            cast(? as timestamp) as resolved_at
        from extracted
        """,
        [source_run_id, resolved_at],
    )
    row = connection.execute(
        f"""
        select
            count(),
            count(*) filter (where match_eligibility = 'eligible'),
            count(*) filter (where match_eligibility = 'natural_person'),
            count(*) filter (where match_eligibility = 'missing_supplier_id'),
            count(*) filter (where match_eligibility = 'invalid_supplier_id'),
            count(*) filter (where source_url = ''),
            count(*) filter (where data_publicacao_pncp is null)
        from {_CANDIDATES_BUILD_TABLE}
        """
    ).fetchone()
    counts = {
        "candidate_rows": int(row[0]),
        "eligible_rows": int(row[1]),
        "natural_person_rows": int(row[2]),
        "missing_supplier_ids": int(row[3]),
        "invalid_supplier_ids": int(row[4]),
        "rows_without_source_url": int(row[5]),
        "malformed_publication_dates": int(row[6]),
    }
    qualified = f"{tables.DUCKDB_SCHEMA}.{candidates_table}"
    if partition is None:
        connection.execute(
            f"create or replace table {qualified} as "
            f"select * from {_CANDIDATES_BUILD_TABLE}"
        )
    else:
        _replace_candidate_partition(
            connection=connection,
            qualified_table=qualified,
            candidates_table=candidates_table,
            partition=partition,
        )
    return counts


def _replace_candidate_partition(
    *,
    connection: Any,
    qualified_table: str,
    candidates_table: str,
    partition: str,
) -> None:
    """Atomically replace one durable monthly candidates partition."""
    derived_partitions = [
        str(value)
        for (value,) in connection.execute(
            f"select distinct coalesce("
            f"strftime(data_publicacao_pncp, '%Y%m'), '197001') as part "
            f"from {_CANDIDATES_BUILD_TABLE} order by part"
        ).fetchall()
    ]
    if not derived_partitions:
        raise ValueError(
            f"Brazil PNCP produced no candidates for partition {partition}; "
            f"refusing to replace its durable DuckDB rows"
        )
    unexpected = [value for value in derived_partitions if value != partition]
    if unexpected:
        raise ValueError(
            f"Brazil PNCP normalised partition {partition}, but its rows belong "
            f"to {', '.join(unexpected)}; refusing to persist a misfiled month"
        )

    connection.execute(
        f"alter table {_CANDIDATES_BUILD_TABLE} add column "
        f"{tables.CANDIDATE_PARTITION_COLUMN} varchar"
    )
    connection.execute(
        f"update {_CANDIDATES_BUILD_TABLE} set {tables.CANDIDATE_PARTITION_COLUMN} = ?",
        [partition],
    )

    existing_columns = _table_columns(connection, candidates_table)
    if not existing_columns:
        connection.execute(
            f"create table {qualified_table} as select * from {_CANDIDATES_BUILD_TABLE}"
        )
        return

    if tables.CANDIDATE_PARTITION_COLUMN not in existing_columns:
        connection.execute(
            f"alter table {qualified_table} add column "
            f"{tables.CANDIDATE_PARTITION_COLUMN} varchar"
        )
        connection.execute(
            f"update {qualified_table} set {tables.CANDIDATE_PARTITION_COLUMN} = "
            "coalesce(strftime(data_publicacao_pncp, '%Y%m'), '197001')"
        )
        existing_columns[tables.CANDIDATE_PARTITION_COLUMN] = "VARCHAR"

    for column, column_type in _table_columns(
        connection,
        _CANDIDATES_BUILD_TABLE,
        schema="main",
        catalog="temp",
    ).items():
        if column not in existing_columns:
            connection.execute(
                f"alter table {qualified_table} add column {column} {column_type}"
            )

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {qualified_table} where "
            f"{tables.CANDIDATE_PARTITION_COLUMN} = ?",
            [partition],
        )
        connection.execute(
            f"insert into {qualified_table} by name "
            f"select * from {_CANDIDATES_BUILD_TABLE}"
        )
    except Exception:
        connection.execute("rollback")
        raise
    connection.execute("commit")


def _table_columns(
    connection: Any,
    table_name: str,
    *,
    schema: str = tables.DUCKDB_SCHEMA,
    catalog: str | None = None,
) -> dict[str, str]:
    catalog_filter = "" if catalog is None else "and table_catalog = ?"
    params = [schema, table_name] if catalog is None else [schema, table_name, catalog]
    return {
        str(column): str(data_type)
        for column, data_type in connection.execute(
            f"""
            select column_name, data_type
            from information_schema.columns
            where table_schema = ? and table_name = ?
              {catalog_filter}
            order by ordinal_position
            """,
            params,
        ).fetchall()
    }
