CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000406 as far as ClickHouse allows. The view owns its MergeTree (the engine is
-- declared inside it), so one DROP VIEW removes the definition and the data together. The
-- two new tables go with it -- the response table first, because it is the evidence a queue
-- row points at, and nothing else in the database references either of them.
--
-- THE PAIR TABLE KEEPS request_id AND THAT IS DELIBERATE. The column is in
-- corpscout.se_company_person_match's SORTING KEY, and ClickHouse cannot shrink a sorting
-- key: undoing it would mean rebuilding a live table inside a down migration, which this
-- ledger does not do. The column is harmless to every reader that does not name it -- it
-- stores String's zero value on every row the match asset wrote -- and a re-applied 000406
-- finds it already there, because the ALTER is written IF NOT EXISTS.
--
-- The STATE table's column is not in a key, so it can go and does.

DROP VIEW IF EXISTS corpscout.se_company_person_match_gap;

DROP TABLE IF EXISTS corpscout.llm_response_se_company_person;

DROP TABLE IF EXISTS corpscout.llm_queue_se_company_person;

ALTER TABLE corpscout.se_company_person_match_state
    DROP COLUMN IF EXISTS request_id;
