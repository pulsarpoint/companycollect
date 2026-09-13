CREATE DATABASE IF NOT EXISTS corpscout;

-- THE LLM-ENHANCEMENT PATTERN, FIRST USE (spec 2026-09-13 sections 3 and 4, slice 1). An
-- LLM pass over a source table is a QUEUE, a RESPONSE table and four steps -- queue, run,
-- apply, clean up -- and every one of them belongs to the table it enhances. Per source
-- table X that needs LLM augmentation there are exactly two tables, llm_queue_<X> and
-- llm_response_<X>, mapped to X's own unit id. Nothing generic goes into the schema: a
-- shared llm_queue would need a string entity discriminator in its sort key, an unconstrained
-- unit id, and one table's retention policy imposed on every consumer.
--
-- THESE ARE THE ONLY TWO corpscout TABLES WHOSE NAMES DO NOT START WITH THEIR ENTITY'S
-- PREFIX. That is the pattern's name, not an oversight: llm_<step>_<source table> reads as
-- "the LLM queue OF se_company_person". Every string match on a table name in this repo
-- compares whole names, so nothing is broken by the new prefix.
--
-- THE UNIT IS THE COMPANY, because that is what a person-match prompt is built over: all of
-- a company's people go into one prompt, since identity is decided by comparing them with
-- each other. The queue therefore holds company ids, not person keys -- even when the
-- reviewer reached it from a person row.

-- The unit ids of one request and nothing else. No version column: within a request a
-- company appears once (both minters de-duplicate before inserting) and every column beside
-- the key is identical for every row of a request, so there is nothing for a version to
-- choose between. request_id leads the sort key because every read is "this request's
-- companies".
CREATE TABLE IF NOT EXISTS corpscout.llm_queue_se_company_person
(
    request_id String,
    company_id String,
    queued_at DateTime64(3, 'UTC'),
    queued_by String,
    note String DEFAULT '',
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree
ORDER BY (request_id, company_id);

-- One row per company per request, the newest answer winning. Nothing downstream reads it:
-- it is the paid evidence, and the apply step's input. Two columns go beyond the owner's
-- list and both earn their place -- `candidates` is the list length the answer was produced
-- for, which is the apply's sanity check and the cost readout, and `source_run_id` is the
-- Dagster run that wrote the row, which is how a Requests page links to the last run without
-- a tag query against Dagster.
--
-- RE-RUNNING THE SAME REQUEST UNDER A DIFFERENT PROMPT VERSION REPLACES THAT REQUEST'S
-- RESPONSES. A request holds exactly one effective answer per company, the most recent
-- run's. To compare two prompts, queue two requests.
CREATE TABLE IF NOT EXISTS corpscout.llm_response_se_company_person
(
    request_id String,
    company_id String,
    provider LowCardinality(String),
    model LowCardinality(String),
    prompt_version LowCardinality(String),
    input_hash FixedString(64),
    candidates UInt16,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    raw_response String,
    error String DEFAULT '',
    attempts UInt8,
    source_run_id String,
    responded_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(responded_at)
ORDER BY (request_id, company_id);

-- WHY THE PAIR TABLE'S SORT KEY HAS TO GROW. Without request_id in it, a v2 pair row
-- REPLACES the v1 row for the same candidate pair, so a v2 that scores a pair lower than v1
-- silently unmerges a person the moment the next fold runs. With it, v1's rows (request_id
-- empty) and every request's rows coexist as distinct rows, and the fold takes the MAXIMUM
-- confidence across them. It is also what makes a revert exact: deleting a request's pair
-- rows cannot touch another request's.
--
-- ONE STATEMENT ON PURPOSE. ClickHouse extends a sorting key only with a column added by the
-- SAME ALTER, and the column must take the type's zero value so that every stored row's new
-- key component is identical and no part has to be re-sorted -- metadata only, no rewrite.
-- Two separate statements are refused.
--
-- AND NO DEFAULT CLAUSE. `ADD COLUMN request_id String DEFAULT ''` with a MODIFY ORDER BY is
-- refused on 26.5 with code 36, BAD_ARGUMENTS, "Newly added column request_id has a default
-- expression, so adding expressions that use it to the sorting key is forbidden". Without
-- the clause the column still stores String's zero value, which is the empty string this
-- design wants -- it is the DEFAULT EXPRESSION, not the value, that a key column may not
-- carry.
ALTER TABLE corpscout.se_company_person_match
    ADD COLUMN IF NOT EXISTS request_id String,
    MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id);

-- The state table's sort key is NOT extended: it is one row per company by design -- the
-- certification the fold joins on -- and an apply REPLACES it. request_id here records which
-- request certified the company, which is what a revert deletes by. The column is not in a
-- key, so it keeps its explicit DEFAULT.
ALTER TABLE corpscout.se_company_person_match_state
    ADD COLUMN IF NOT EXISTS request_id String DEFAULT '';

-- THE MATCH-GAP VIEW (spec section 5): one row per company that still carries a
-- deterministic call-name or double-surname gap no stored match at or above 0.8 has closed.
-- It is what turns "re-send everything" into a named, affordable request.
--
-- DERIVED, NOT WRITTEN. Nothing inserts into it. The fold, the normalizer, the reviewer
-- rules, the precedence and the match asset are untouched by this migration. The view is
-- recomputed whole on every refresh, so it cannot drift from the four tables it reads -- it
-- can only LAG them, by at most one hour.
--
-- THE REFRESHABLE FORM THIS REPO USES (000326, 000335, 000391, 000392, 000402): the engine
-- is declared INSIDE the view, so corpscout.se_company_person_match_gap IS the MergeTree
-- readers query and there is no separate target table to keep in step. The down file's DROP
-- VIEW takes that inner table with it.
--
-- REFRESH AT :30. se_company_person_role rebuilds at :20 and se_companies_serving at :45 for
-- 13 to 15 minutes -- :30 is the free half of the hour.
--
-- CREATED EMPTY, FIRST BUILD BY HAND. This is the heaviest hourly refresh the person entity
-- owns: two ARRAY JOINs over 1.27M active persons, a self-join of about 5.8M member rows and
-- an anti-join. The migrate client's read_timeout is 300 seconds, so EMPTY skips the initial
-- refresh, this CREATE returns in milliseconds, and the ledger can never be left dirty by a
-- dropped client. The controller then runs SYSTEM REFRESH VIEW
-- corpscout.se_company_person_match_gap and polls system.view_refreshes. Until that lands
-- the view answers with zero rows, which every reader treats as "no gap".
--
-- ALSO BOUNDS THE REFRESH ITSELF: the SELECT carries the same trailing SETTINGS every
-- serving refresh has carried since 000347/000391 -- grace_hash spill joins, external
-- group-by and sort, and a 12 GiB max_memory_usage. If the first build exceeds ten minutes
-- the refresh moves to every 6 hours rather than the cap growing.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- person/tables.py::build_se_company_person_match_gap_sql(), drift-pinned by dagster_v3
-- tests/test_se_company_person_match_gap_view.py.

CREATE MATERIALIZED VIEW corpscout.se_company_person_match_gap
REFRESH EVERY 1 HOUR OFFSET 30 MINUTE
ENGINE = MergeTree
ORDER BY (company_id)
EMPTY
AS WITH
members AS (
    SELECT
        p.company_id AS company_id,
        p.person_key AS person_key,
        p.birth_year AS birth_year,
        p.sources AS sources,
        arrayStringConcat(n.last_tokens, ' ') AS surname,
        arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))) AS given_set,
        arrayStringConcat(arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))), ' ') AS given,
        n.last_tokens AS last_tokens
    FROM corpscout.se_company_person AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    INNER JOIN corpscout.se_company_person_normalized AS n FINAL
      ON n.company_id = p.company_id AND n.normalized_id = member_id
    WHERE p.active = 1 AND n.parse_status = 'ok'
      AND n.source IN ('bolagsverket', 'esef', 'wikidata', 'ratsit')
),
member_person AS (
    SELECT p.company_id AS company_id, member_id AS normalized_id, p.person_key AS person_key
    FROM corpscout.se_company_person AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    WHERE p.active = 1
),
matched AS (
    SELECT m.company_id AS company_id, m.members_a AS members_a, m.members_b AS members_b
    FROM corpscout.se_company_person_match AS m FINAL
    INNER JOIN (
        SELECT company_id, input_hash
        FROM corpscout.se_company_person_match_state FINAL
        WHERE error = ''
    ) AS s ON s.company_id = m.company_id AND s.input_hash = m.input_hash
    WHERE m.confidence >= 0.8
),
matched_left AS (
    SELECT company_id, member_a, members_b FROM matched ARRAY JOIN members_a AS member_a
),
matched_ids AS (
    SELECT company_id, member_a, member_b FROM matched_left ARRAY JOIN members_b AS member_b
),
matched_pairs AS (
    SELECT DISTINCT
        ka.company_id AS company_id,
        least(ka.person_key, kb.person_key) AS person_key_a,
        greatest(ka.person_key, kb.person_key) AS person_key_b
    FROM matched_ids AS mi
    INNER JOIN member_person AS ka
      ON ka.company_id = mi.company_id AND ka.normalized_id = mi.member_a
    INNER JOIN member_person AS kb
      ON kb.company_id = mi.company_id AND kb.normalized_id = mi.member_b
    WHERE ka.person_key != kb.person_key
),
call_name AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.surname = b.surname
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND hasAll(a.given_set, b.given_set)
      AND length(a.given_set) > length(b.given_set)
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
double_surname AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.given = b.given
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND length(a.last_tokens) = 2
      AND length(b.last_tokens) = 1
      AND has(a.last_tokens, b.last_tokens[1])
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
gap AS (
    SELECT company_id, person_key_a, person_key_b, 1 AS is_call_name, 0 AS is_double_surname
    FROM call_name
    UNION ALL
    SELECT company_id, person_key_a, person_key_b, 0 AS is_call_name, 1 AS is_double_surname
    FROM double_surname
)
SELECT
    g.company_id AS company_id,
    toUInt32(countIf(g.is_call_name = 1)) AS call_name_pairs,
    toUInt32(countIf(g.is_double_surname = 1)) AS double_surname_pairs,
    now64(3, 'UTC') AS computed_at
FROM gap AS g
LEFT ANTI JOIN matched_pairs AS m
  ON m.company_id = g.company_id
 AND m.person_key_a = g.person_key_a
 AND m.person_key_b = g.person_key_b
GROUP BY g.company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;
