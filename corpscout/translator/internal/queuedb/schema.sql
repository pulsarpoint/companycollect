-- Translator queue schema. Single source of truth: embedded via go:embed and
-- executed by CreateTables; everything is IF NOT EXISTS (idempotent).
-- source_text_hash is a decimal-string uint64 (SQLite integers are signed
-- 64-bit; half of all cityHash64 values exceed 2^63).

CREATE TABLE IF NOT EXISTS input_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    source_language_name TEXT NOT NULL,
    target_language_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

CREATE TABLE IF NOT EXISTS output_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

CREATE TABLE IF NOT EXISTS failed_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    error_message TEXT NOT NULL,
    failed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

-- Early-termination indexes: batch queries walk created_at order and stop at
-- their LIMIT; without these, every batch pays a full scan of input_items.
-- idx_input_created is widened to (created_at, source_lang, target_lang)
-- because the pair-pick query orders by all three columns: a single-column
-- index on created_at alone cannot satisfy that ORDER BY, so SQLite would
-- full-scan and temp-B-tree-sort the whole table per batch; the extra
-- columns let SQLite walk the index directly and stop at LIMIT 1.
CREATE INDEX IF NOT EXISTS idx_input_created ON input_items (created_at, source_lang, target_lang);
CREATE INDEX IF NOT EXISTS idx_input_pair_created ON input_items (source_lang, target_lang, created_at);

-- The single definition of "pending": in input, not yet translated, not failed.
CREATE VIEW IF NOT EXISTS pending_items AS
SELECT i.*
FROM input_items AS i
WHERE NOT EXISTS (
    SELECT 1 FROM output_items AS o
    WHERE o.source_table = i.source_table AND o.source_column = i.source_column
      AND o.source_text_hash = i.source_text_hash
      AND o.source_lang = i.source_lang AND o.target_lang = i.target_lang
)
AND NOT EXISTS (
    SELECT 1 FROM failed_items AS f
    WHERE f.source_table = i.source_table AND f.source_column = i.source_column
      AND f.source_text_hash = i.source_text_hash
      AND f.source_lang = i.source_lang AND f.target_lang = i.target_lang
);
