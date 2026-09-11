# SE company person — LLM identity matching — design

Date: 2026-09-11. Owner decision (chat, 2026-09-11): cross-source identity for the person
entity is decided by an LLM, not by more name rules; precedence keeps deciding the spelling
and the attributes; the matching phase sits after normalization and runs only for companies
whose people come from more than one source; the LLM receives the normalized people with
everything we hold (name, age or birth year, roles, years, source) and answers, per person,
which other people are possibly the same person with a confidence.

## 1. Purpose

After Ratsit slice 2 the person entity holds 1,321,187 active persons, but the fold's K3
identity (equal first and last tokens, middle tokens by unique minimal superset) keeps apart
the same human when sources spell the name differently:

- call names: Ratsit delivers every registered given name (`Erik Bo Bengtsson`, born 1966),
  ESEF and Bolagsverket the call name (`Bo Bengtsson`) — 32,390 pairs in 31,360 companies
  (32,214 beside Bolagsverket, 176 beside ESEF; Swedbank 5020177753 has 89 Ratsit and 50
  ESEF persons and 7 merges);
- double surnames split differently (`Anna Ek Svensson` as first `anna` / middle `ek` /
  last `svensson` from a full name, `Ek Svensson` as the last tokens from first+last
  columns) — 3,793 pairs in 3,666 companies;
- and the long tail no rule reaches: hyphens, transliterations, a married name, initials.

An LLM reads a company's people list once and scores the pairs. The fold merges the pairs
above a threshold as it merges exact names today; the birth-year guard stays absolute; a
reviewer split still wins; precedence is untouched.

Out of scope: matching across companies (a person id across the register), matching
reviewer-created persons (the reviewer merges by hand), changing the normalizer's tokens,
and any change to the address or basic-info entities.

## 2. Facts the design rests on

- Population (prod 2026-09-11): 124,646 companies have normalized `ok` rows from at least
  two machine sources (124,283 Ratsit + Bolagsverket, 356 with ESEF). Grouping rows per
  source by normalized name gives 492,464 candidates: median 3 per company, p95 8, p99 12,
  maximum 159, two companies above 100. One call per company at roughly 500 prompt and 100
  completion tokens is ~60M prompt tokens on the first run (the ESEF people extraction spent
  17.4M on deepseek-v4-flash); later runs touch only companies whose candidate list changed.
- Normalized layer: `se_company_person_normalized` (000396) keyed `(company_id, source,
  slot)`, `normalized_id = sha256(suggestion_id \n normalizer_version)`, tokens
  `first_tokens`/`middle_tokens`/`last_tokens`, `display_name`, `birth_year`, `role_code`,
  `role_year`, `data`, `normalized_at` (one stamp per normalize run). The normalize change
  scan compares `suggestion_id`, not a timestamp.
- Fold: `identity_sets_before_split` (`fold.py:191`) is a union-find over candidate pairs
  from `by_name` (equal first+last tokens) and `by_qid`, each pair admitted through
  `_matches` (birth-year conflict vetoes, QID equality wins, else K3 middles); then
  `_split_sets` splits a set holding two birth years; then `apply_rules` (merge by person
  keys, split by slots, resolved through the previous members), then hide. Keys hash the
  most complete member's tokens with a discriminator. The change scan of `batch.py`
  bumps a company's "newest" stamp from four watermarks (normalized, rules, company
  precedence, global precedence) against `folded_at`.
- Rules: `se_company_person_rule` (kind hide|merge|split, `person_keys` or `slots`,
  `active`, `created_by = 'backoffice'`); a Reset writes an inactive version. The backoffice
  already has a `merge` intent (tick persons, note) that writes a merge rule by person keys,
  and a `split` by member slots.
- Backoffice People tab: the loader runs six FINAL reads per company (main, history,
  normalized, raw, rules, precedence) in one `Promise.all`; members are index-parallel
  arrays zipped by `membersOf()`; `MemberEntry` shows source, slot, precedence and
  re-fold badges, name, birth year and `data` as JSON.
- Highest migration: 000398; `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`
  lists every file; apply with `make -s -C corpscout clickhouse-migrate-up-one`.

## 3. The matching phase

### 3.1 Placement

A new asset `se_company_person_match` (module `person/match.py`, group `se_company_person`)
runs after `se_company_person_normalize` in `se_company_person_extract_job` and the weekly's
run config, with `deps=[se_company_person_normalize]`. It is the second phase of
normalization in the pipeline's sense — every company is normalized, then matched, then
folded — but its own asset because LLM calls take hours on the first run, need their own
concurrency and retries, and must resume after a failure without re-normalizing anything.
Config: `changed_only: bool = True`, `company_ids: list[str]`, `page_size: int = 500`
(companies per page; the page's results are written before the next page starts, so a
killed run resumes by its own change scan), `concurrency: int = 8` (LLM calls in flight),
`max_companies: int = 5_000_000`, plus the profile fields of section 3.5. The weekly's
`WEEKLY_RUN_CONFIG` gains the match op's config with `provider`, `model` and `changed_only`
spelled out (an explicit run config, so the no-default rule for ad-hoc materializations
still holds), and `se_company_person_extract_job`'s selection gains the asset.

### 3.2 Candidates

For a company, the candidates are its normalized rows with `parse_status = 'ok'` from the
machine sources (`bolagsverket`, `esef`, `wikidata`, `ratsit`; reviewer rows never go to the
LLM — a reviewer merges by hand), grouped per source by `(first_tokens, middle_tokens,
last_tokens)` — the exact-name identity the fold already applies within a source. One
candidate per group carries:

| field | value |
| --- | --- |
| `id` | the smallest `normalized_id` of the group; `members` = every `normalized_id` in it |
| `source` | the source |
| `name` | the group's `display_name` as delivered (the longest when they differ) |
| `given`, `surname` | `first_tokens + middle_tokens` joined, `last_tokens` joined |
| `birth_year` | any non-NULL `birth_year` in the group |
| `age` | `data.age` when present (Ratsit) |
| `roles` | the distinct `(role_code, role_year)` pairs, sorted, at most 20 |
| `external` | `data.external = 'true'` (Ratsit's Extern roles) |

A company is in scope when its candidates span at least two sources. The candidate list,
serialized deterministically (sorted by source then id), is hashed to `input_hash`
(sha256); the change scan sends a company when its `input_hash` differs from the stored one
or no state row exists.

### 3.3 The call

One request per company (`concurrency` in flight). System prompt (versioned as
`PROMPT_VERSION = "se-person-match-v1"`): the task is to find pairs of candidates that are
the same physical person, with Swedish naming explained — several registered given names
of which one is the call name (`Erik Bo Bengtsson` is `Bo Bengtsson`), double surnames with
or without a hyphen, a maiden or married surname change is NOT the same person unless the
given names, birth year and roles all agree, initials, transliterations (`Björn`/`Bjorn`),
and that a differing birth year or age means different people. The user message is the
candidate list as JSON with short ordinal ids (`c0`..`cN` in the serialized order; the 64-char
normalized ids never reach the model, and the parser maps the ordinals back). The answer is JSON only: `{"pairs": [{"a": id, "b": id,
"confidence": 0-1, "reason": "..."}]}` — pairs, not per-candidate lists, so the two
directions cannot disagree; the parser accepts both `a`/`b` orders and keeps the higher
confidence when a pair is repeated. Sanity rules applied after parsing: unknown ids,
self-pairs and confidences outside `[0, 1]` are dropped and counted; a pair whose candidates
carry different birth years is stored with `confidence = 0` and reason `birth-year
conflict` (the guard is ours, not the model's).

Model and client (section 3.5): `deepseek-v4-flash` on `https://api.deepseek.com` through
the `se_company/info.py` profile, temperature 0, `response_format json_object`, thinking
disabled for the deepseek provider (as the ESEF pass does), `max_tokens` scaled with the candidate count (at least 4,000; a
reasoning model's reasoning counts against it, and a 150-candidate company answers dozens of pairs), timeout 120 s, the SDK's two retries on transport
errors and 429; a company whose call still fails or answers malformed JSON gets a state row
with `error`, the raw text and the usage, and is retried on the next run only when the error
was transient (`rate_limited:`, `http_error:`) or the input hash moved — a malformed answer or
an over-cap company is sticky for the same input, so it is neither re-paid nor re-folded every
run. The run never fails on one company, but a page whose error share exceeds one half after
twenty attempts raises (a provider outage must not produce a green run that re-sends
everything next week); the asset's retry policy supplies the backoff.

### 3.4 Tables (migration 000399)

`corpscout.se_company_person_match` — the scored pairs, one row per unordered pair:

```
company_id       String
candidate_a      FixedString(64)   -- normalized_id, a < b
candidate_b      FixedString(64)
members_a        Array(FixedString(64))
members_b        Array(FixedString(64))
source_a         LowCardinality(String)
source_b         LowCardinality(String)
name_a           String
name_b           String
confidence       Float64
reason           String
model            LowCardinality(String)
prompt_version   LowCardinality(String)
input_hash       FixedString(64)
matched_at       DateTime64(3, 'UTC')
CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
ENGINE = ReplacingMergeTree(matched_at) ORDER BY (company_id, candidate_a, candidate_b)
```

`corpscout.se_company_person_match_state` — one row per matched company, the change scan's
and the fold watermark's source of truth:

```
company_id String, input_hash FixedString(64), candidates UInt16, sources UInt8, pairs UInt16,
model LowCardinality(String), prompt_version LowCardinality(String),
prompt_tokens UInt32, completion_tokens UInt32, raw_response String, error String DEFAULT '',
source_run_id String, matched_at DateTime64(3, 'UTC')
CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
ENGINE = ReplacingMergeTree(matched_at) ORDER BY (company_id)
```

(`raw_response` keeps the model's exact text as the basic-info observation cache does; at
~1 KB per company it is a hundred megabytes for the whole population.)

A re-match of a company writes its new pairs and its new state row; a re-scored pair replaces
its row (`input_hash` is not in the ORDER BY) and what supersedes the previous input is the
fold's join on the state row's `input_hash`. A run killed between the pairs and the state row of
one page leaves pairs the fold cannot see until the next match run (the runbook re-runs the
match before folding after an interruption). `EXPECTED_MIGRATIONS` gains `000399_corpscout_se_company_person_match`.
Down: drop both tables.

### 3.5 Client and profile

`person/match.py` reuses `se_company/info.py`: `LlmProfileConfig` (provider, model,
base_url, temperature, max_tokens, prompt_version, concurrency), `build_llm_client(profile,
timeout_seconds=...)` (the OpenAI-compatible client with the `{PROVIDER}_API_KEY` env
variable read at call time, never through run config or a Dagster resource) and
`map_ordered` (the ordered thread pool). The match asset's config is a
`PersonMatchProfile(LlmProfileConfig)` that, like `LlmSuggestionProfile` and the ESEF
passes, requires `provider` and `model` explicitly (a bare Materialize fails validation
rather than spending on a default) and pins `prompt_version` to `PROMPT_VERSION` (a
mismatch refuses to run). Usage is read from `response.usage`; the asset's metadata carries
`companies`, `pages`, `called`, `reused` (unchanged input hash), `pairs`, `pairs_above_threshold`,
`errors`, `prompt_tokens`, `completion_tokens`. `DEEPSEEK_API_KEY` already lives in the prod
host's `.env` (the ESEF people run used it); ansible verifies the file and never copies
secrets.

## 4. The fold consumes the matches

- `batch.py` reads, for a page's companies, the pairs with `confidence >= MATCH_THRESHOLD`
  whose `input_hash` equals the company's current state row (`match_pairs_sql()`), and adds
  a fifth watermark `match_watermarks_sql()` = `max(matched_at)` per company from the state
  table, bumped into `newest` like the rule and precedence stamps. `MATCH_THRESHOLD = 0.8`
  is a constant in `fold.py` beside `FOLD_VERSION`, which becomes `se-person-fold-v2`.
- `fold_company_persons(...)` takes `matches: Sequence[MatchPair]` (`members_a`,
  `members_b`, `confidence`, `reason`). `identity_sets_before_split` unions every member
  of `a` with every member of `b` for each pair — after the `by_name`/`by_qid` pairs, through
  the same `_years_conflict` veto (a pair whose two sides carry different birth years is
  ignored; the stored row already says `confidence = 0` for that case, this is the second
  lock), before `_split_sets` (a set that still ends up holding two birth years is split as
  today) and before `apply_rules` (a reviewer merge or split, and a hide, keep the last
  word: `apply_rules` runs on the LLM-joined sets, so a split rule pinning the Ratsit slot
  pulls it back out, and a Reset of that split lets the match apply again).
- A published person whose set was joined by at least one match carries, in `data`, the key
  `llm_match`: `{"pairs": [{"a": name_a, "b": name_b, "confidence": 0.93, "reason": "..."}],
  "model": ..., "prompt_version": ...}` written by the fold after `merge_member_data`
  (member data never contains it; the backoffice treats it as fold-owned, read-only, and
  `RESERVED_DATA_KEYS` stays the reviewer's three). `_COMPARED` is unchanged, so the added
  key changes the row and the history records the merge as `updated`/`withdrawn`+`created`
  exactly like a name-driven re-key.
- Keys: the joined set re-keys through `canonical_tokens` (the most complete member —
  Ratsit's full name), so the ESEF person's key is withdrawn and a new key created; the
  fold already handles that (hide rules resolve through previous members).
- `se_company_person_fold_companies` (the backoffice's Fold now) keeps its order normalize →
  fold; it does not call the LLM (a targeted fold must stay seconds). The People tab's
  possible-matches panel (section 5) is the reviewer's way to merge a pair the LLM scored
  below the threshold.

## 5. Backoffice

- Loader: a seventh FINAL read, `PERSON_MATCH_SQL` over `se_company_person_match` joined to
  the company's state row on `input_hash`, returning every pair with `confidence >= 0.5`.
- Person card: a member whose `normalized_id` is in a pair at or above the threshold shows
  a `matched by LLM · 0.93` badge beside the precedence badge, with the reason on hover; the
  card's `llm_match` key is rendered as a small list, not as raw JSON.
- A "Possible matches" panel under the persons list: the pairs between 0.5 and the
  threshold whose two candidates sit in different published persons, each as `name_a ↔
  name_b · 0.64 · reason` with a `Merge` button that submits the existing `merge` intent
  with the two person keys pre-filled and the note `LLM match 0.64: <reason>` (the reviewer's
  merge rule then outranks the threshold on the next fold). Pairs already inside one person
  are not listed.
- The People list gets no new column; the Source filter is unchanged.

## 6. Tests

- `tests/test_se_company_person_match.py`: candidate building (grouping per source by
  tokens, birth year and roles aggregation, deterministic serialization and `input_hash`,
  companies with one source excluded), the prompt (system text carries the naming rules, the
  user message is the sorted JSON), parsing (both id orders, duplicates keep the max,
  unknown ids and self-pairs dropped and counted, out-of-range confidence dropped, the
  birth-year lock writes `confidence = 0`), and the run loop with a fake client (pages,
  reuse by hash, one failing company recorded with `error` and the run continuing, usage
  summed).
- `tests/test_se_company_person_fold.py`: a call-name pair (`erik bo bengtsson` from ratsit,
  `bo bengtsson` from esef) merges into one person with the Ratsit spelling, the birth year
  and `data.llm_match`; a pair with conflicting birth years does not; a split rule on the
  Ratsit slot undoes the merge; a match below the threshold does nothing; the same input on
  a second fold is `unchanged`.
- `tests/test_se_company_person_fold_clickhouse_local.py`: the new watermark and the pair
  read are pinned by name; a fold round with a match row moves the person.
- `tests/test_clickhouse_migrations.py`: `000399` registered.
- Backoffice vitest: loader stub for the match query, the badge, the panel and its merge
  submission.

## 7. Prod run

1. Migrate 000399; deploy.
2. Sample gate: `se_company_person_match` with `company_ids` = 2,000 companies drawn from
   the call-name and double-surname gap lists plus 500 random multi-source companies plus
   Swedbank 5020177753 and the slice 2 spot-check companies; read the pairs: the share of
   known call-name pairs scored ≥ 0.8 (expect well above 90%), the share of random pairs of
   different surnames scored ≥ 0.8 (expect ~0), a hand check of twenty reasons, tokens per
   company. The owner reads the sample before step 3; a threshold change is a constant
   edit and a re-fold, not a re-match.
3. The full match: `changed_only: true, page_size: 500, concurrency: 8` over the 124,646
   companies (hours; the asset resumes by its own change scan if interrupted). Readouts:
   state rows, errors, pairs by confidence band, prompt and completion tokens.
4. Fold backfill over the 64 buckets (the match watermark selects the matched companies).
   Readouts: multi-source persons before → after (82,944 → ?), the call-name gap count
   (32,390 → ?), the double-surname count (3,793 → ?), `withdrawn`/`created` sums, persons
   with `data.llm_match`, Swedbank's ratsit+esef merges (7 → ?), a spot check of ten
   merges with their reasons.
5. The serving refresh; the People tab smoke; the shipped record.

## 8. Risks and rulings

- Precision over recall: the threshold is 0.8 and the birth-year lock is ours. A wrong
  merge costs a reviewer one Split; a missed merge costs a possible-match click.
- Cost: ~60M prompt tokens once, then deltas; the sample gate runs first. If a single
  company's candidate list is huge (max 159 today), the prompt is still a few thousand
  tokens; a hard cap of 400 candidates skips the company with `error = 'too many
  candidates'` rather than truncating silently.
- The extract job grows by the match asset; the weekly stays STOPPED. The match asset has
  its own pool `se_company_person_match` (limit 1) so two runs never race on the same
  companies.
- Reviewer-created persons are never LLM input; a reviewer person that duplicates a source
  person is merged by the reviewer's own merge rule as today.
- The stored pairs name candidates by `normalized_id`; a re-normalization (new normalizer
  version) changes the ids, the candidate hashes change, and every multi-source company is
  re-matched on the next run — the price of a normalizer bump, stated here so it is not a
  surprise.
- Same-source pairs are allowed (a source spelling the same person two ways across
  filings); the fold treats them like any pair.
- Weekly cost is birthday-driven: Ratsit's `data.age` moves a company's extractor state hash
  once a year per person, which re-stamps its slots, mints new normalized ids and a new input
  hash — expect roughly 5-8% of multi-source companies re-sent per weekly run. The
  birth-year guard inside the split honours the LLM pairs (a year-less row paired to a
  1980-born member stays with that member, never falls to the smallest year).
- `confidence` is stored as Float64 and the threshold is inclusive, so a threshold edit is
  exact.

## 9. Names

Module `se_company/person/match.py`; asset `se_company_person_match`; pool
`se_company_person_match`; tables `se_company_person_match`, `se_company_person_match_state`
(migration `000399_corpscout_se_company_person_match`); constants `PROMPT_VERSION =
"se-person-match-v1"`, `MATCH_THRESHOLD = 0.8`, `POSSIBLE_MATCH_FLOOR = 0.5`, `FOLD_VERSION =
"se-person-fold-v2"`; `data` key `llm_match`; backoffice `PERSON_MATCH_SQL`, "Possible
matches" panel.

## 10. Slices

1. The matching phase and the fold: migration 000399, `person/match.py` with the candidate
   builder, prompt, parser, state and pairs writers, the asset and job wiring; the fold's
   pair read, watermark, union, `llm_match` and version; tests; prod: migrate, deploy, the
   sample gate, the full match, the 64-bucket fold, readouts, record.
2. The backoffice: the match read, the member badge, the `llm_match` rendering, the
   possible-matches panel with its merge; tests; owner smoke.

Each slice is one plan executed with subagent-driven development, reviewed, merged and run
on prod before the next; shipped records are appended here.
