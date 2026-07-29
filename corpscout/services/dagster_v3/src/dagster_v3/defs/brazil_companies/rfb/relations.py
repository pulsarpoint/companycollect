"""RFB Socios -> company relation edges.

One row per company-to-partner edge. `related_entity_kind` discriminates the
far end: '1' company, '2' natural person, '3' foreign. Person names and masked
CPFs are stored exactly as RFB publishes them -- the publisher performs the
masking, we add nothing and never attempt to reverse it. See
docs/brazil_rfb_socios-design.md section 6.

Verbatim: no joins, no resolution, no vocabulary mapping. `related_tax_id` is
NOT resolved against br_companies here, so a partner pointing at a company we
have not ingested stays visible instead of silently becoming empty.
"""

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_companies.rfb import tables
from dagster_v3.defs.brazil_companies.rfb.duckdb_attach import (
    attached_read_only_database,
)


def _blank(column: str) -> str:
    """Coalesce to '' -- a non-nullable ClickHouse String must never see NULL."""
    return f"coalesce(nullif(trim({column}), ''), '')"


def build_brazil_rfb_company_relations(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    snapshot_year_month: str,
    socios_database_path: str | Path,
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    raw_table = tables.RAW_TABLE_BY_FAMILY["socios"]
    target = f"{dataset}.{tables.COMPANY_RELATIONS_TABLE}"

    connection.execute(f"create schema if not exists {dataset}")
    with attached_read_only_database(
        connection,
        database_path=socios_database_path,
        alias="socios_db",
    ) as socios_alias:
        connection.execute(
            f"""
            create or replace table {target} as
            select
                'BR' as country_iso2,
                'brazil_rfb' as source_slug,
                cast(? as varchar) as source_run_id,
                lower(sha256(concat_ws(
                    '|',
                    {_blank('s.cnpj_basico')},
                    {_blank('s.identificador_socio')},
                    {_blank('s.cnpj_cpf_socio')},
                    {_blank('s.nome_socio_razao_social')},
                    {_blank('s.qualificacao_socio')}
                ))) as source_record_id,
                cast(? as varchar) as snapshot_year_month,
                {_blank('s.cnpj_basico')} as cnpj_basico,
                {_blank('s.identificador_socio')} as related_entity_kind,
                {_blank('s.nome_socio_razao_social')} as related_name,
                {_blank('s.cnpj_cpf_socio')} as related_tax_id,
                {_blank('s.qualificacao_socio')} as relation_code,
                try_strptime(nullif(trim(s.data_entrada_sociedade), ''), '%Y%m%d')::date
                    as relation_since,
                {_blank('s.data_entrada_sociedade')} as relation_since_key,
                {_blank('s.pais')} as related_country,
                {_blank('s.representante_legal')} as representative_tax_id,
                {_blank('s.nome_representante')} as representative_name,
                {_blank('s.qualificacao_representante')} as representative_code,
                {_blank('s.faixa_etaria')} as age_band,
                now() as resolved_at
            from {socios_alias}.{dataset}.{raw_table} as s
            """,
            [source_run_id, snapshot_year_month],
        )

    row_count = int(
        connection.execute(f"select count(*) from {target}").fetchone()[0]
    )
    if row_count == 0:
        raise ValueError(
            "Brazil RFB Socios produced no company relations; "
            "refusing to publish an empty edge table"
        )
    return {"company_relations": row_count}
