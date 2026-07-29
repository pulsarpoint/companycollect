CREATE DATABASE IF NOT EXISTS corpscout;

-- Connection history: one row per SPELL, not per snapshot.
--
-- 000208 created this table with snapshot semantics and the export replaced it
-- wholesale each run, so the ninth run destroyed what the eighth learned. RFB
-- republishes the full register monthly and its mirror eventually drops old
-- months, so a discarded snapshot is gone permanently. Ownership and control
-- changing over time is the signal worth having.
--
-- Dropped and recreated rather than altered: the table is deployed with ZERO
-- rows (nothing has ever been materialized), so there is no data to migrate,
-- and the sort key changes -- which ALTER cannot do.
--
-- relation_code is IN the key deliberately. A partner becoming an administrator
-- closes one row and opens another, because that control shift is precisely
-- what this table exists to show. Holding it in a mutable column would hide it.
--
-- relation_since_key is in the key because RFB publishes no departures but DOES
-- publish re-entries: data_entrada_sociedade carries a NEW entry date when
-- someone rejoins, so a second spell is detectable from a single snapshot
-- rather than depending on us having observed the gap.
--
-- start_at and end_at have DIFFERENT precision. start_at is authoritative, from
-- the source's own entry date. end_at means "gone by this snapshot" -- never
-- "left on this date" -- because the source never says when a relationship
-- ended. Its precision is exactly the run cadence.
DROP TABLE IF EXISTS corpscout.br_company_relations;

CREATE TABLE IF NOT EXISTS corpscout.br_company_relations
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    cnpj_basico String,
    related_entity_kind LowCardinality(String),
    related_tax_id String,
    relation_code LowCardinality(String),
    relation_since_key String,
    related_name String,
    related_country String,
    age_band LowCardinality(String),
    representative_tax_id String,
    representative_name String,
    representative_code LowCardinality(String),
    relation_since Nullable(Date32),
    first_seen_snapshot LowCardinality(String),
    last_seen_snapshot LowCardinality(String),
    start_at Nullable(Date32),
    end_at Nullable(Date32),
    is_current UInt8,
    observations UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    cnpj_basico,
    related_entity_kind,
    related_tax_id,
    relation_code,
    relation_since_key
);

-- Which months are in the history, so the merge can refuse an out-of-order
-- snapshot and a reader can tell what the history is made of. Without it the
-- ordering guard has nothing to compare against and a gap is invisible.
CREATE TABLE IF NOT EXISTS corpscout.br_company_relations_snapshots
(
    snapshot_year_month LowCardinality(String),
    merged_at DateTime64(3, 'UTC'),
    source_run_id String,
    edges_in_snapshot UInt64,
    spells_opened UInt64,
    spells_closed UInt64,
    spells_total UInt64
)
ENGINE = MergeTree
ORDER BY snapshot_year_month;
