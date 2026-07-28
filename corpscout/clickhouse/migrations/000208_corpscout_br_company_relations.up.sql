CREATE DATABASE IF NOT EXISTS corpscout;

-- How a Brazilian company is connected to something else.
--
-- One row per partner edge from RFB's Socios file. The far end is a company, a
-- natural person, or a foreign entity, and `related_entity_kind` says which --
-- one edge model rather than separate people and ownership tables, because a
-- connection is a connection whichever kind sits at the other end.
--
-- This is the only Brazilian source that answers who controls a company and
-- what else they control. CVM's shareholder data covers 1,230 companies --
-- this covers the register.
--
-- Person names and masked CPFs are stored exactly as RFB publishes them. RFB
-- performs the masking itself as part of an open-transparency dataset. We add
-- nothing and never attempt to reverse it. Redaction is a view concern.
--
-- `related_tax_id` holds a CNPJ when kind is 1 and a MASKED CPF when kind is 2,
-- discriminated by related_entity_kind. Any join to br_companies must carry
-- that predicate or it silently matches nothing. `related_tax_id` is also the
-- full 14-digit CNPJ, while br_companies.cnpj_basico is only the 8-digit CNPJ
-- root -- the join is substr(related_tax_id, 1, 8) = br_companies.cnpj_basico,
-- not a direct equality, or it silently matches nothing too.
CREATE TABLE IF NOT EXISTS corpscout.br_company_relations
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_year_month LowCardinality(String),
    cnpj_basico String,
    related_entity_kind LowCardinality(String),
    related_name String,
    related_tax_id String,
    relation_code LowCardinality(String),
    relation_since Nullable(Date32),
    related_country String,
    representative_tax_id String,
    representative_name String,
    representative_code LowCardinality(String),
    age_band LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code);
