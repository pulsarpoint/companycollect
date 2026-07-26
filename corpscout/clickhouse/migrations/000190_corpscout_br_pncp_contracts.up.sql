CREATE DATABASE IF NOT EXISTS corpscout;

-- Brazilian government contracts from PNCP, the national procurement portal.
-- One row per contract per supplier, which is the grain the endpoint returns --
-- there is no separate winners table, so this is shaped like Sweden's UHM
-- rather than TED's winners/notices pair.
--
-- ReplacingMergeTree on data_atualizacao_global because contracts are amended
-- after publication: the payload carries numeroRetificacao, and a refresh job
-- re-reads /contratos/atualizacao. Later versions of a contract replace earlier
-- ones rather than accumulating.
--
-- All five value fields are stored. The API documents none of them -- the
-- schema types valorInicial, valorParcela, valorGlobal, valorAcumulado and
-- numeroParcelas with no description -- so which is "the" contract value is a
-- decision for the view, made against real data, not one baked in at ingest.
--
-- supplier_cnpj is the 14-digit CNPJ that signed, supplier_cnpj_basico its
-- 8-digit company base. Both are kept: 97.96% of Brazilian companies have one
-- establishment, but the 2.04% that have more are the large organisations most
-- likely to win public contracts, and one has 9,999 of them.
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
ORDER BY (company_id, numero_controle_pncp, supplier_cnpj);
