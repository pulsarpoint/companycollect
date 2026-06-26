CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS legal_form_description_language;
ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS legal_form_description_en;
ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS legal_form_description_translated_at;
ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS legal_form_description_translation_provider;
ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS legal_form_description_translation_model;

CREATE OR REPLACE VIEW corpscout.no_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en,
    ifNull(lf.translated_text, '') AS legal_form_description_en
FROM corpscout.no_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'articles_purpose'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'activity_text'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'company_description'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'legal_form_description'
    GROUP BY source_text_hash
) AS lf ON lf.source_text_hash = cityHash64(c.legal_form_description_original);
