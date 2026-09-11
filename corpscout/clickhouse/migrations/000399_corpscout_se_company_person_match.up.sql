CREATE DATABASE IF NOT EXISTS corpscout;

-- THE LLM IDENTITY MATCHING PHASE (spec 2026-09-11 section 3.4, slice 1). Two tables between
-- the normalized layer and the fold: the scored pairs, and one state row per matched company.
--
-- WHY A PAIR TABLE AND NOT A PER-PERSON LIST. The model answers with unordered pairs, so the
-- two directions of one claim cannot disagree. candidate_a < candidate_b is imposed by the
-- parser, not by the engine, and the sort key is that ordered pair, so a re-match of the same
-- pair replaces its row instead of adding a second.
--
-- WHY input_hash IS ON BOTH TABLES. It is NOT in this table's sort key, so it does not
-- version anything: a re-match of the SAME pair replaces that pair's row, keeping the newer
-- input's confidence, reason and hash. What the column does is certify. The fold reads only
-- the pairs whose input_hash equals the company's state-row hash, so a row left behind by an
-- input the company no longer has -- a pair the new candidate list no longer contains, or one
-- the model stopped scoring -- is simply never read again, without anything deleting it. The
-- state table is one row per company (ReplacingMergeTree ORDER BY company_id), which is what
-- makes that comparison a single hash.
--
-- WHY raw_response IS STORED. The same reason the basic-info observation cache stores it: a
-- paid answer is evidence, and a parse that changes must be re-readable against the exact text
-- the model returned. At about 1 KB per company it is a hundred megabytes for all 124,646
-- multi-source companies.
--
-- error IS THE RETRY SWITCH. A company whose call failed or whose answer did not parse keeps a
-- state row with error set and the raw text, and the change scan re-sends it because the reuse
-- read takes only rows with error = ''. The run itself never fails on one company.
--
-- NO SERVING VIEW IS TOUCHED, so this migration has no SYSTEM STOP VIEW, no MODIFY QUERY and
-- no refresh window to avoid -- unlike 000396 and 000398, which both re-pointed
-- corpscout.se_companies_serving.

-- One row per unordered candidate pair the model scored (spec 3.4). members_a and members_b
-- are the normalized_ids the two candidates stand for, which is what the fold unions.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match
(
    company_id String,
    candidate_a FixedString(64),
    candidate_b FixedString(64),
    members_a Array(FixedString(64)),
    members_b Array(FixedString(64)),
    source_a LowCardinality(String),
    source_b LowCardinality(String),
    name_a String,
    name_b String,
    -- Float64, not Float32: the fold admits a pair with `confidence >= MATCH_THRESHOLD` and
    -- that comparison is inclusive. Float32 would store 0.8 as 0.800000011920929 and a
    -- threshold of 0.7 as 0.69999998807907104, so a pair scored at exactly the threshold
    -- would be kept or dropped by the storage format rather than by the constant.
    confidence Float64,
    reason String,
    model LowCardinality(String),
    prompt_version LowCardinality(String),
    input_hash FixedString(64),
    matched_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(matched_at)
ORDER BY (company_id, candidate_a, candidate_b);

-- One row per matched company (spec 3.4): the change scan's memory, the fold's fifth
-- watermark, and the record of what the call cost and what it said.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match_state
(
    company_id String,
    input_hash FixedString(64),
    candidates UInt16,
    sources UInt8,
    pairs UInt16,
    model LowCardinality(String),
    prompt_version LowCardinality(String),
    prompt_tokens UInt32,
    completion_tokens UInt32,
    raw_response String,
    error String DEFAULT '',
    source_run_id String,
    matched_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(matched_at)
ORDER BY (company_id);
