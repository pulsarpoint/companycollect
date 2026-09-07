CREATE DATABASE IF NOT EXISTS corpscout;

-- Translations keyed on the register tables (spec 2026-09-07-se-translated-source-views).
-- Additive: the spine key ('corpscout.se_companies', 'activity_description') stays until
-- basic-info slice 5 retires corpscout.se_companies, and se_companies_serving (000347) and the
-- old se_company_info_scb artifact still read it.
--
-- 1. Bolagsverket: every spine-key row copied under the register table's name. `version`
--    is the translation's own stamp (the loader writes int(time.time())) and is preserved,
--    so <column>_translated_at below does not move and the basic-info change scan does
--    not revisit every company. Register and spine texts are byte-identical (0 of
--    2,855,016 differ on 2026-09-07), so the cityHash64 keys carry over unchanged.
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)
SELECT
    'corpscout.se_bolagsverket_companies', source_column, source_text_hash, source_lang, target_lang,
    translated_text, provider, model, version
FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';

-- 2. Ratsit: seeded from the same rows wherever a Ratsit business description is the same
--    text (65,080 of 66,910 distinct texts on 2026-09-07), so only the texts Ratsit alone
--    has ever reach the LLM.
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)
SELECT
    'corpscout.se_ratsit_company', 'business_description', source_text_hash, source_lang, target_lang,
    translated_text, provider, model, version
FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'
  AND source_text_hash IN (
      SELECT cityHash64(ifNull(business_description, ''))
      FROM corpscout.se_ratsit_company
      WHERE ifNull(business_description, '') != ''
  );

-- 3. The views: lv_companies_translated's shape plus the translation stamp, which the
--    basic-info extractors fold into observed_at so a translation that lands after an
--    extraction re-selects the company. Under join_use_nulls = 0 an untranslated row's
--    stamp is the zero DateTime64, so readers test <column>_en != '' instead. The empty-text
--    hash is excluded so a row without text can never borrow another row's translation.
CREATE OR REPLACE VIEW corpscout.se_bolagsverket_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_description_en,
    act.translated_at AS activity_description_translated_at
FROM corpscout.se_bolagsverket_companies AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, version) AS translated_text,
        toDateTime64(max(version), 3, 'UTC') AS translated_at
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_bolagsverket_companies'
      AND source_column = 'activity_description'
      AND source_lang = 'sv' AND target_lang = 'en'
      AND source_text_hash != cityHash64('')
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''));

CREATE OR REPLACE VIEW corpscout.se_ratsit_company_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS business_description_en,
    act.translated_at AS business_description_translated_at
FROM corpscout.se_ratsit_company AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, version) AS translated_text,
        toDateTime64(max(version), 3, 'UTC') AS translated_at
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_ratsit_company'
      AND source_column = 'business_description'
      AND source_lang = 'sv' AND target_lang = 'en'
      AND source_text_hash != cityHash64('')
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.business_description, ''));
