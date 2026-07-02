CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.lv_companies
    ADD COLUMN IF NOT EXISTS activity_text_original Nullable(String);

CREATE OR REPLACE VIEW corpscout.lv_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_text_en
FROM corpscout.lv_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_text_original, ''));
