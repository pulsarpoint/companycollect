CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.text_classifications
(
    source_table        LowCardinality(String),
    source_column       LowCardinality(String),
    source_text         String,
    source_text_hash    UInt64,
    nace_code           String,
    nace_candidates     Array(String),
    confidence          Float32,
    method              LowCardinality(String),
    model               LowCardinality(String),
    classifier_version  LowCardinality(String),
    version             UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash);

CREATE OR REPLACE VIEW corpscout.lv_companies_nace AS
SELECT
    c.*,
    cls.nace_code AS nace_code,
    cls.confidence AS nace_confidence,
    ifNull(n.label, '') AS nace_label
FROM corpscout.lv_companies AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(nace_code, version) AS nace_code,
        argMax(confidence, version) AS confidence
    FROM corpscout.text_classifications
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS cls ON cls.source_text_hash = cityHash64(ifNull(c.activity_text_original, ''))
LEFT JOIN (
    SELECT code, argMax(label, resolved_at) AS label
    FROM corpscout.nace_category_embeddings
    WHERE classification_version = 'NACE_REV_2_1'
    GROUP BY code
) AS n ON n.code = cls.nace_code;
