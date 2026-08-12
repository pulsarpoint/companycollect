DROP VIEW IF EXISTS corpscout.lv_companies_translated;
DROP VIEW IF EXISTS corpscout.lv_companies_current;
DROP VIEW IF EXISTS corpscout.lv_company_addresses_current;
DROP TABLE IF EXISTS corpscout.lv_company_addresses;

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
      AND source_lang = 'lv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_text_original, ''));
