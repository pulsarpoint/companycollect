-- Preserve translations written after the migration under the shadow-table
-- name, and restore the exact pre-migration table. No translation rows are
-- deleted by the rollback itself.
DROP TABLE IF EXISTS corpscout.text_translations_multilingual;

RENAME TABLE
    corpscout.text_translations TO corpscout.text_translations_multilingual,
    corpscout.text_translations_before_multilingual TO corpscout.text_translations;
