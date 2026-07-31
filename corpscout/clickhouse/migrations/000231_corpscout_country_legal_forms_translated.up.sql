CREATE DATABASE IF NOT EXISTS corpscout;

-- Per-country legal-form dimensions, for Latvia, Estonia and Slovakia.
--
-- These three registers publish the legal form as a label on the company row
-- itself, so unlike France they need no separate reference source. What they
-- did need was a place for the English to live other than a column stamped at
-- ingest: lv_companies.legal_form_description_en was written from a Python
-- dict during the load, and when that dict was re-keyed in June the 118,008
-- already-loaded rows kept the old, wrong values. Editing the map changed
-- nothing until the whole register was downloaded again.
--
-- Reading English from text_translations instead makes a correction a
-- translation load rather than a re-ingest. These views are the read side --
-- a few dozen rows each, which the backoffice caches and decodes in memory
-- rather than joining into every company query.
--
-- argMax orders on (provider = 'static', version) so hand-curated wording
-- outranks a machine translation whichever ran more recently.

CREATE OR REPLACE VIEW corpscout.lv_legal_forms_translated AS
WITH forms AS (
    SELECT DISTINCT legal_form_code AS code, legal_form_text AS label
    FROM corpscout.lv_companies
    WHERE legal_form_code != '' AND legal_form_text != ''
)
SELECT f.code AS code, f.label AS label, ifNull(t.translated_text, '') AS label_en
FROM forms AS f
LEFT JOIN (
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'legal_form_text'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label);

-- Estonia's register publishes no code, only the Estonian term, so the label
-- is its own key. Lookups are by label and the backoffice column reads
-- legal_form_original directly.
CREATE OR REPLACE VIEW corpscout.ee_legal_forms_translated AS
WITH forms AS (
    SELECT DISTINCT legal_form_original AS code, legal_form_original AS label
    FROM corpscout.ee_companies
    WHERE legal_form_original != ''
)
SELECT f.code AS code, f.label AS label, ifNull(t.translated_text, '') AS label_en
FROM forms AS f
LEFT JOIN (
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.ee_companies'
      AND source_column = 'legal_form_original'
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
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.sk_companies'
      AND source_column = 'legal_form_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label);
