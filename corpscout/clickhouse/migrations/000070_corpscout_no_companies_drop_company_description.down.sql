CREATE DATABASE IF NOT EXISTS corpscout;

-- corpscout.companies was dropped in migration 000061, so only corpscout.no_companies remains.
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS company_description_original Nullable(String);

CREATE OR REPLACE VIEW corpscout.no_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '')  AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '')  AS company_description_en,
    ifNull(lf.translated_text, '')  AS legal_form_description_en
FROM corpscout.no_companies AS c
LEFT JOIN ( SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'articles_purpose_original'
    GROUP BY source_text_hash ) AS ap  ON ap.source_text_hash  = cityHash64(c.articles_purpose_original)
LEFT JOIN ( SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'
    GROUP BY source_text_hash ) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN ( SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'company_description_original'
    GROUP BY source_text_hash ) AS cd  ON cd.source_text_hash  = cityHash64(c.company_description_original)
LEFT JOIN ( SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'legal_form_description_original'
    GROUP BY source_text_hash ) AS lf  ON lf.source_text_hash  = cityHash64(c.legal_form_description_original);
