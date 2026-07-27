CREATE DATABASE IF NOT EXISTS corpscout;

-- What kind of entity a legal form denotes, normalised across registers.
--
-- Brreg and Bolagsverket register legal ENTITIES, not companies: a municipality,
-- a ministry and a hospital trust each hold an organisation number and sit in
-- the same table as a hairdresser. Keeping them is right -- dropping public
-- bodies would break the link from a procurement buyer to the entity that
-- issued the tender, and every buyer is one. What it costs is that "company"
-- becomes the wrong word for part of the data with no way to tell which part.
--
-- This is that missing axis. One row per (country, legal form), so a page can
-- say "Government agency" instead of implying a business, and a count can
-- exclude them deliberately rather than silently.
--
-- A mapping table rather than a column on each register, because the register
-- tables already store the raw code: deriving here means no re-materialization
-- of 1.2M Norwegian or 3.4M Swedish rows when a classification is corrected.
--
-- Scale, measured 2026-07-27: 2,216 public-sector entities of 1,167,141 in
-- Norway (0.19%) and 747 of 3,407,809 in Sweden (0.02%). Small everywhere, and
-- concentrated exactly where it misleads.

CREATE TABLE IF NOT EXISTS corpscout.company_entity_types
(
    country_code LowCardinality(String),
    legal_form_code String,
    -- government | municipality | region | public_body | company | sole_trader
    -- | association | foundation | cooperative | estate | organisational_unit
    -- | other
    entity_type LowCardinality(String),
    entity_type_label String,
    -- What the register itself calls this form, in its own language where that
    -- is all it publishes. Kept so a classification can be checked against the
    -- source rather than trusted.
    source_label String,
    -- The filterable flag. True only for government, municipality, region and
    -- public_body -- NOT for organisational_unit, whose parent decides and
    -- which the form alone does not settle.
    is_public_sector UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_code, legal_form_code);
