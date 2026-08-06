CREATE DATABASE IF NOT EXISTS corpscout;

-- ReplacingMergeTree only replaces rows with the same sorting key. The old
-- key omitted the language pair, so adding a second target language could
-- replace the existing English translation during a merge. Build the corrected
-- table beside the live one, copy every row, then swap names atomically.
DROP TABLE IF EXISTS corpscout.text_translations_multilingual;
DROP TABLE IF EXISTS corpscout.text_translations_before_multilingual;

CREATE TABLE corpscout.text_translations_multilingual
(
    source_table      LowCardinality(String),
    source_column     LowCardinality(String),
    source_text_hash  UInt64,
    source_lang       LowCardinality(String),
    target_lang       LowCardinality(String),
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash, source_lang, target_lang);

INSERT INTO corpscout.text_translations_multilingual
SELECT
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang,
    translated_text,
    provider,
    model,
    version
FROM corpscout.text_translations;

RENAME TABLE
    corpscout.text_translations TO corpscout.text_translations_before_multilingual,
    corpscout.text_translations_multilingual TO corpscout.text_translations;

-- Existing serving views must select one explicit language pair. Grouping only
-- by source_text_hash would make a newly added language nondeterministically
-- replace the English text returned by these views.
CREATE OR REPLACE VIEW corpscout.no_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(lf.translated_text, '') AS legal_form_description_en
FROM corpscout.no_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies'
      AND source_column = 'articles_purpose_original'
      AND source_lang = 'no'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies'
      AND source_column = 'activity_text_original'
      AND source_lang = 'no'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies'
      AND source_column = 'legal_form_description_original'
      AND source_lang = 'no'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS lf ON lf.source_text_hash = cityHash64(c.legal_form_description_original);

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

CREATE OR REPLACE VIEW corpscout.br_pncp_contracts_translated AS
SELECT
    c.*,
    ifNull(obj.translated_text, '') AS objeto_contrato_en
FROM corpscout.br_pncp_contracts AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.br_pncp_contracts'
      AND source_column = 'objeto_contrato'
      AND source_lang = 'pt'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS obj ON obj.source_text_hash = cityHash64(c.objeto_contrato);

CREATE OR REPLACE VIEW corpscout.br_government_contracts_translated AS
SELECT
    v.*,
    ifNull(obj.translated_text, '') AS title_en
FROM corpscout.br_government_contracts AS v
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.br_pncp_contracts'
      AND source_column = 'objeto_contrato'
      AND source_lang = 'pt'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS obj ON obj.source_text_hash = cityHash64(v.title);

CREATE OR REPLACE VIEW corpscout.br_cnae_categories_translated AS
SELECT
    c.*,
    ifNull(t.translated_text, '') AS description_en
FROM corpscout.br_cnae_categories AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.br_cnae_categories'
      AND source_column = 'description_pt'
      AND source_lang = 'pt'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.description_pt);

CREATE OR REPLACE VIEW corpscout.fr_legal_forms_translated AS
SELECT
    f.*,
    ifNull(t.translated_text, '') AS label_en
FROM corpscout.fr_legal_forms AS f
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.fr_legal_forms'
      AND source_column = 'label_fr'
      AND source_lang = 'fr'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label_fr);

CREATE OR REPLACE VIEW corpscout.company_entity_types_translated AS
SELECT
    e.*,
    ifNull(t.translated_text, '') AS source_label_en
FROM corpscout.company_entity_types AS e
LEFT JOIN (
    SELECT
        source_text_hash,
        source_lang,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.company_entity_types'
      AND source_column = 'source_label'
      AND target_lang = 'en'
    GROUP BY source_text_hash, source_lang
) AS t
    ON t.source_text_hash = cityHash64(e.source_label)
   AND t.source_lang = multiIf(
       e.country_code = 'SE', 'sv',
       e.country_code = 'NO', 'no',
       e.country_code = 'FI', 'fi',
       e.country_code = 'BR', 'pt',
       ''
   );

CREATE OR REPLACE VIEW corpscout.lv_legal_forms_translated AS
WITH forms AS (
    SELECT DISTINCT legal_form_code AS code, legal_form_text AS label
    FROM corpscout.lv_companies
    WHERE legal_form_code != '' AND legal_form_text != ''
)
SELECT f.code AS code, f.label AS label, ifNull(t.translated_text, '') AS label_en
FROM forms AS f
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'legal_form_text'
      AND source_lang = 'lv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label);

CREATE OR REPLACE VIEW corpscout.ee_legal_forms_translated AS
WITH forms AS (
    SELECT DISTINCT legal_form_original AS code, legal_form_original AS label
    FROM corpscout.ee_companies
    WHERE legal_form_original != ''
)
SELECT f.code AS code, f.label AS label, ifNull(t.translated_text, '') AS label_en
FROM forms AS f
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.ee_companies'
      AND source_column = 'legal_form_original'
      AND source_lang = 'et'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label);

CREATE OR REPLACE VIEW corpscout.sk_legal_forms_translated AS
WITH forms AS (
    SELECT DISTINCT legal_form_code AS code, legal_form_original AS label
    FROM corpscout.sk_companies
    WHERE legal_form_code != '' AND legal_form_original != ''
)
SELECT f.code AS code, f.label AS label, ifNull(t.translated_text, '') AS label_en
FROM forms AS f
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.sk_companies'
      AND source_column = 'legal_form_original'
      AND source_lang = 'sk'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label);

CREATE OR REPLACE VIEW corpscout.cz_legal_forms_translated AS
SELECT
    f.*,
    ifNull(t.translated_text, '') AS label_en
FROM corpscout.cz_legal_forms AS f
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.cz_legal_forms'
      AND source_column = 'label_cs'
      AND source_lang = 'cs'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label_cs);
