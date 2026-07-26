CREATE DATABASE IF NOT EXISTS corpscout;

-- Partition the contracts table by publication month.
--
-- The ingest asset is monthly-partitioned, so re-running a month must replace
-- that month rather than insert a second copy of it. Without a partition key
-- there is nothing to replace: the duplicates would sit there until a merge
-- happened to collapse them, and only queries using FINAL would be correct in
-- the meantime. With one, a re-run is an atomic REPLACE PARTITION, which is the
-- pattern the rest of this repo already uses.
--
-- toYYYYMM(data_publicacao_pncp) rather than a separate partition column,
-- because the fetch is *by* publication date: every row returned for month M
-- has its publication date in M by definition, so the row's own date and the
-- partition it was fetched under cannot disagree. A row with no publication
-- date is coalesced to the epoch, landing in an obviously wrong 197001
-- partition rather than being hidden inside a real month -- allow_nullable_key
-- is off repo-wide, so the expression has to be non-nullable, and a visible
-- bogus partition is better than a silent misfiling.
--
-- The daily update job returns contracts from any month, and each lands in the
-- partition its publication date says -- so an amendment to a 2024-03 contract
-- updates the 2024-03 partition, which is where that contract lives.
--
-- A partition key cannot be ALTERed, so the table is recreated. It is empty:
-- this migration lands before any backfill, and nothing is lost.
DROP TABLE IF EXISTS corpscout.br_pncp_contracts;

CREATE TABLE IF NOT EXISTS corpscout.br_pncp_contracts
(
    company_id String,
    company_match_status LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_url String,
    numero_controle_pncp String,
    numero_controle_pncp_compra String,
    ano_contrato Nullable(UInt16),
    sequencial_contrato Nullable(UInt32),
    numero_contrato_empenho String,
    numero_retificacao Nullable(UInt16),
    processo String,
    tipo_contrato LowCardinality(String),
    categoria_processo LowCardinality(String),
    objeto_contrato String,
    informacao_complementar String,
    data_publicacao_pncp Nullable(Date),
    data_assinatura Nullable(Date),
    data_vigencia_inicio Nullable(Date),
    data_vigencia_fim Nullable(Date),
    data_atualizacao_global DateTime64(3, 'UTC'),
    supplier_cnpj String,
    supplier_cnpj_basico String,
    supplier_name String,
    supplier_person_type LowCardinality(String),
    supplier_country_code LowCardinality(String),
    subcontractor_cnpj String,
    subcontractor_name String,
    subcontractor_person_type LowCardinality(String),
    buyer_cnpj String,
    buyer_name String,
    buyer_power_id LowCardinality(String),
    buyer_sphere_id LowCardinality(String),
    buyer_unit_code String,
    buyer_unit_name String,
    buyer_state_code LowCardinality(String),
    buyer_municipality String,
    valor_inicial Nullable(Decimal(38, 2)),
    valor_parcela Nullable(Decimal(38, 2)),
    valor_global Nullable(Decimal(38, 2)),
    valor_acumulado Nullable(Decimal(38, 2)),
    numero_parcelas Nullable(UInt32),
    is_revenue_contract Nullable(UInt8),
    parliamentary_amendment Nullable(UInt8),
    from_adhesion Nullable(UInt8),
    has_reallocation Nullable(UInt8),
    match_eligibility LowCardinality(String),
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    valor_global_usd Nullable(Decimal(38, 2)),
    fx_rate_to_usd Nullable(Decimal(24, 10)),
    fx_rate_date Nullable(Date),
    fx_source LowCardinality(String)
)
ENGINE = ReplacingMergeTree(data_atualizacao_global)
PARTITION BY toYYYYMM(ifNull(data_publicacao_pncp, toDate('1970-01-01')))
ORDER BY (company_id, numero_controle_pncp, supplier_cnpj);
