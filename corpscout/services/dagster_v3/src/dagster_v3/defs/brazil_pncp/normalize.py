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


def load_raw_pages(
    *,
    connection: Any,
    page_dir: Path,
    source_run_id: str,
    source_retrieved_at: datetime,
) -> int:
    """Load a partition's page files into the raw table, one row per contract."""
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} as
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
            f"select count() from {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
        ).fetchone()[0]
    )


def build_contract_candidates(
    *,
    connection: Any,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    """Project raw JSON into the candidate columns, typed and normalised."""
    connection.execute(
        f"""
        create or replace table {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} as
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
                coalesce(json ->> '$.tipoContrato', '') as tipo_contrato,
                coalesce(json ->> '$.categoriaProcesso', '') as categoria_processo,
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
            from {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}
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
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
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
        from {qualified}
        """
    ).fetchone()
    return {
        "candidate_rows": int(row[0]),
        "eligible_rows": int(row[1]),
        "natural_person_rows": int(row[2]),
        "missing_supplier_ids": int(row[3]),
        "invalid_supplier_ids": int(row[4]),
        "rows_without_source_url": int(row[5]),
        "malformed_publication_dates": int(row[6]),
    }
