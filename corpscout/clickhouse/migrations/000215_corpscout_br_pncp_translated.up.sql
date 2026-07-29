CREATE DATABASE IF NOT EXISTS corpscout;

-- English contract objects for Brazil.
--
-- objeto_contrato is the one string that says what public money actually bought,
-- and it is Portuguese-only while the backoffice labels every field in English
-- by design. brazil_pncp_translation_load fills text_translations, and these two
-- views expose the result, following no_companies_translated exactly:
-- argMax(translated_text, version) per source_text_hash, LEFT JOINed on
-- cityHash64 of the source column, and ifNull to '' so an untranslated row is
-- empty rather than missing.
--
-- Two views because they have different consumers and different row sets:
--
--   br_pncp_contracts_translated      every contract, for the detail page
--   br_government_contracts_translated the register view, for the contracts table
--
-- They are NOT interchangeable. br_government_contracts filters to
-- company_match_status = 'exact', so it omits 3,283 contracts (2.8%) including
-- every award to a natural person. The detail page must read the first, or those
-- contracts would show an untranslated object even once translated.
--
-- Joining the register view on cityHash64(title) is safe because its title is
-- `CAST(objeto_contrato, 'String')` with no trim, case change or collapse:
-- verified 2026-07-30 that cityHash64(title) = cityHash64(objeto_contrato) on
-- all 112,943 rows, 0 mismatches. Any future transformation of that expression
-- silently breaks this join, which is why the column is cast and nothing more.

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
    GROUP BY source_text_hash
) AS obj ON obj.source_text_hash = cityHash64(v.title);
