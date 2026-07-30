CREATE DATABASE IF NOT EXISTS corpscout;

-- The EU Common Procurement Vocabulary (CPV 2008), Regulation (EC) No 213/2008.
--
-- Every EU-directive procurement notice carries a CPV code and nothing else --
-- the registers publish `45213100`, never "Construction work for commercial
-- buildings". Without this table a reader is shown eight digits that name
-- nothing, and the backoffice can only label the 45 divisions.
--
-- Loaded by the cpv_vocabulary Dagster assets, which own the data. This file
-- owns only the schema. 9,454 codes, English labels.
CREATE TABLE IF NOT EXISTS corpscout.cpv_vocabulary
(
    code String,
    label_en String,
    -- How many leading digits carry meaning. CPV is read left to right and
    -- trailing zeros mean "no more detail given", so this is both the node's
    -- depth and the length of the prefix every descendant shares -- which is
    -- what makes selecting a node select its whole subtree.
    significant_digits UInt8,
    -- The nearest ancestor that is itself a code, '' for a division. Not
    -- simply one digit shorter: the vocabulary skips levels, so a computed
    -- parent would dangle.
    parent_code String,
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY code;
