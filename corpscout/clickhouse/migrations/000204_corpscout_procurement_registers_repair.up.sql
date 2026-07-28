CREATE DATABASE IF NOT EXISTS corpscout;

-- Repair: 000199 was edited after it had already been applied.
--
-- b285033e added retrieval_method to that migration's CREATE TABLE, which is
-- the right column to have -- how a register arrives is not always "we call an
-- API", and Hilma is a CSV a human exports by hand, so its freshness is
-- whenever someone last did that. The mistake was where it was added.
--
-- golang-migrate records applied versions. A database whose ledger already
-- reached 199 will never re-run that file, and CREATE TABLE IF NOT EXISTS
-- would be inert even if it did. So "just re-deploy 000199" exits clean and
-- changes nothing, which is the worst shape a defect can take: the remedy that
-- looks obvious reports success while the column stays missing.
--
-- The ledger is forward-only. A migration that has shipped is history, not a
-- draft, and the only way to change what it built is another migration.
--
-- Idempotent by construction, so this is correct whichever state a given
-- database is in: where 000199 ran before the edit it adds the column, and
-- where it ran after -- or has not run yet -- IF NOT EXISTS makes it a no-op.
ALTER TABLE corpscout.procurement_registers
    ADD COLUMN IF NOT EXISTS retrieval_method String AFTER api_or_download_url;
