CREATE DATABASE IF NOT EXISTS corpscout;

-- The legal-form label becomes part of the Swedish company-info merge, and the owner's
-- 2026-08-23 decision is that BOTH languages are published: the Swedish name is the
-- official term (Bolagsverket organisationsform for the -ORGFO codes, SCB juridisk form
-- for the numeric ones) and the English label is the gloss beside it.
--
-- corpscout.se_code_labels is a curated FIXTURE, seeded by the sweden_company
-- se_code_labels_clickhouse asset from an in-repo dictionary, so the official Swedish name
-- is one more column of that dictionary rather than a translation job: an LLM asked to
-- render "AB-ORGFO" would guess, and the register's own terminology is not negotiable.
--
-- DEFAULT '' rather than Nullable: a code either has a curated Swedish name or it does not
-- (the status-reason codes have no published list to curate from and keep ''), and every
-- consumer already reads a label through ifNull. Rows seeded before this migration read as
-- '' until the asset runs again, which is one INSERT -- ReplacingMergeTree(version) plus
-- argMax(version) in the consumers make the newer rows win without a delete.

ALTER TABLE corpscout.se_code_labels
    ADD COLUMN IF NOT EXISTS label_sv String DEFAULT '' AFTER label_en;
