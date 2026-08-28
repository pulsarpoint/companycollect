CREATE DATABASE IF NOT EXISTS corpscout;

-- Faithful recreate of the translated view (000252's rendering, preserved verbatim). The
-- _retired serving view is NOT recreated: it was 000338's transitional swap artifact with no
-- readers -- roll forward instead.
CREATE OR REPLACE VIEW corpscout.se_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_description_en,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(sr.label_en, '') AS status_reason_label_en
FROM corpscout.se_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_companies'
      AND source_column = 'activity_description'
      AND source_lang = 'sv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''))
LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'legal_form'
    GROUP BY code
) AS lf ON lf.code = ifNull(c.legal_form_code, '')
LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
) AS sr ON sr.code = ifNull(c.status_reason, '');
