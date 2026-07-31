CREATE DATABASE IF NOT EXISTS corpscout;

-- INSEE's nomenclature des categories juridiques, the official French legal
-- form list -- levels I, II and III, 309 codes as published.
--
-- fr_companies stores a bare code and nothing else -- `5499`, never "Societe a
-- responsabilite limitee". Unlike Latvia or Estonia there is no label column
-- beside it, so before this table 1.93M French companies displayed a
-- four-digit number and there was no original text to fall back to or to
-- translate. The nomenclature supplies that label via the code, which is why
-- France gains a fallback without re-ingesting 29.7M Sirene rows.
--
-- Loaded by the france_legal_forms Dagster assets from INSEE's SPARQL
-- endpoint, which own the data. This file owns only the schema.
CREATE TABLE IF NOT EXISTS corpscout.fr_legal_forms
(
    code String,
    -- 1, 2 or 3 -- INSEE's niveau, which is also the code's digit count
    -- (1, 2 and 4 respectively).
    level UInt8,
    label_fr String,
    -- INSEE's own skos:broader, not a computed prefix. '' at level I.
    parent_code String,
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY code;
