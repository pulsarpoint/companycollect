# Sweden company-person corrections and suggestions — design

Date: 2026-08-21. Status: draft for review. Owner: dagster_v3 `company_people` (compute) and
backoffice `/admin/se/people` (review).

## 1. Decision and scope

Direction agreed on 2026-08-21 after the SE people curation audit:

- **Dagster owns computation** of Sweden company people (`se_company_person_draft` →
  `se_company_person` → `se_company_person_role`). The backoffice DuckDB Draft 1/Draft 2
  rebuild is not the publish path and will be retired (sub-project 4).
- **Humans intervene through an append-only ledger**, never by editing published rows.
  The pipeline applies ledger decisions as input on every run, so a decision survives
  rebuilds, has provenance, and can be undone. This mirrors `country_person_correction` +
  `country_person_correction_sensor` (`identity.py`), which already works.
- **LLM output is a suggestion**, recorded as data. Publishing is a deterministic function
  of *(drafts, suggestions, corrections)*. Re-running a model never loses a review.

This spec covers **sub-project 1** only:

1. Two new ClickHouse tables: `se_company_person_correction` (ledger) and
   `se_company_person_suggestion` (model output), plus writer grants and two provenance
   columns on `se_company_person`.
2. Precedence and staleness rules in `normalization.py` and `roles.py`.
3. A Dagster sensor that re-runs only the companies touched by new ledger rows.
4. A backoffice write path and one person review page that appends ledger rows.

Follow-on sub-projects, each with its own spec:

- **2.** Role-string classification pass (the 653 `other` strings) and keeping role-less
  signatories as person evidence instead of dropping them.
- **3.** Person grain: one row per (company, person) with a role timeline; the 11.7k
  name-key collisions become the review queue fed by this ledger.
- **4.** Re-point the admin wizard at ClickHouse; retire the DuckDB/SQLite drafts, the local
  `person_profile_llm_response`, and the backoffice Temporal worker (its three workflows are
  covered by the Dagster assets — Dagster is the only producer of suggestion rows; a
  per-person "re-run now" is a synchronous backoffice call or a scoped Dagster run);
  schedule the Dagster chain, add freshness checks, apply migration 000294, commit the
  outstanding admin work.

Out of scope here: cross-company person identity (no personnummer in SE filings — stays
"same name", never "same person"), changing the LLM provider or prompt, authentication.

## 2. Tables (migration 000295)

### 2.1 `corpscout.se_company_person_correction`

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction
(
    correction_id            UUID,
    company_id               String,                 -- 10-digit orgnr; every decision is company-scoped
    correction_kind          LowCardinality(String), -- see §3
    subject_person_id        UUID,                   -- person the decision is about ('undo' repeats the superseded row's subject)
    target_person_id         Nullable(UUID),         -- merge target / reassignment destination
    draft_ids                Array(UUID),            -- observations the decision binds to (split, reassign, set_role, remove_role)
    payload                  String,                 -- JSON object; shape per kind in §3; '{}' when unused
    evidence_hash            FixedString(64),        -- subject's se_company_person.draft_set_hash at review time; all-zero when not applicable
    reason                   String,
    decided_by               String,                 -- 'backoffice' until the backoffice has users
    supersedes_correction_id Nullable(UUID),
    created_at               DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, subject_person_id, created_at, correction_id);
```

Append-only. "Current" decisions are derived (§4.1); rows are never updated or deleted.

### 2.2 `corpscout.se_company_person_suggestion`

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion
(
    suggestion_id    UUID,
    company_id       String,
    person_id        UUID,                 -- deterministic id the suggestion was resolved to (§4.2)
    input_hash       FixedString(64),      -- SHA256 of the exact request payload
    draft_ids        Array(UUID),          -- observations the suggestion covers
    suggestion       String,               -- validated JSON: {name, description, draft_ids}
    raw_response     String,               -- model text as returned (for audit; may be '')
    model_provider   LowCardinality(String),
    model_name       String,
    prompt_version   String,
    prompt_tokens    UInt32,
    completion_tokens UInt32,
    source_run_id    String,
    created_at       DateTime64(3, 'UTC'),

    CONSTRAINT valid_suggestion CHECK isValidJSON(suggestion)
)
ENGINE = MergeTree
ORDER BY (company_id, person_id, input_hash, created_at);
```

One row per model call per person. The same `(person_id, input_hash)` may appear more than
once (re-runs, model changes); the newest `created_at` is the current suggestion. The
backoffice's SQLite `person_profile_llm_response` is the same idea and is superseded by
this table in sub-project 4.

### 2.3 Provenance on `se_company_person`

```sql
ALTER TABLE corpscout.se_company_person
    ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] AFTER draft_ids,
    ADD COLUMN IF NOT EXISTS correction_set_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(arrayStringConcat(
            arrayMap(id -> toString(id), arraySort(correction_ids)), '\n'
        )))) AFTER draft_set_hash,
    ADD COLUMN IF NOT EXISTS suggestion_id Nullable(UUID) AFTER correction_set_hash;
```

`correction_ids` lists every ledger row applied to the published profile;
`suggestion_id` points at the suggestion that was published (NULL for deterministic
copies). `model_provider` keeps naming the generator; a reviewed profile is recognisable
by `notEmpty(correction_ids)`. `se_company_person_role` gets the same `correction_ids`
column.

### 2.4 Grants

```sql
GRANT INSERT ON corpscout.se_company_person_correction TO corpscout_person_correction_writer;
GRANT INSERT ON corpscout.se_company_person_suggestion TO corpscout_person_correction_writer;
```

The backoffice writer account (`CLICKHOUSE_WRITE_USER`, provisioned by
`pnpm provision:clickhouse-writer`) already holds this role; no new credentials. Dagster
writes both tables with its own account.

## 3. Correction kinds

| kind | subject / target / draft_ids | payload | effect |
|---|---|---|---|
| `merge_persons` | subject = person A, target = person B | `{}` | A's drafts publish under B's `person_id`; A is no longer published |
| `split_person` | subject = person, `draft_ids` = drafts to pull out | `{"name": "…"}` required | listed drafts get a new deterministic `person_id` from that name (§4.2); the rest stays |
| `reassign_draft` | subject = current person, target = destination, `draft_ids` = exactly one | `{}` | one observation moves |
| `override_field` | subject = person | `{"name": "…"}` and/or `{"description": "…" \| null}` | wins over suggestion and deterministic values |
| `approve_suggestion` | subject = person | `{"suggestion_id": "…"}` | that suggestion is the published profile while its `input_hash` is current |
| `reject_suggestion` | subject = person | `{"suggestion_id": "…"}` | suggestion never publishes; deterministic fallback; pipeline re-prompts only when evidence changes |
| `set_role` | subject = person, `draft_ids` = role-bearing drafts | `{"role_code": "…", "fiscal_year": 2023 \| null}` | overrides the mapped `role_code` for those drafts |
| `remove_role` | subject = person, `draft_ids` = role-bearing drafts | `{}` | those drafts produce no role row |
| `undo` | `supersedes_correction_id` = row being undone; subject = that row's subject | `{}` | the superseded row is ignored from then on |

`payload` keys outside the table above are rejected by the backoffice before insert and
ignored by the pipeline (logged as `invalid_correction_count`). `role_code` must be an
active row of `company_person_role_type` at apply time; otherwise the correction is stale
(§4.3).

## 4. Pipeline semantics (`normalization.py`, `roles.py`)

### 4.1 Effective corrections

For a company, the effective ledger is: all rows for that `company_id`, minus any row named
by a later row's `supersedes_correction_id`, ordered by `(created_at, correction_id)`.
Computed in one CTE per company batch; the table is small. Applied in kind order:

1. `merge_persons`, `reassign_draft`, `split_person` — change which drafts belong to which
   `person_id`.
2. `approve_suggestion`, `reject_suggestion` — choose the profile source.
3. `override_field` — final field values.
4. `set_role`, `remove_role` — role rows.

Within a kind, later rows win. Precedence for a field: **correction > approved or current
suggestion > deterministic**.

### 4.2 Identity under corrections

`_person_id(company_id, name)` stays the deterministic id. Corrections refer to those ids.
After a `merge_persons`, the pipeline keeps emitting B and never A; a later observation that
would deterministically hash to A is redirected to B as long as the merge is effective
(the merge row is included in B's `correction_ids`). `split_person` assigns the listed
drafts `_person_id(company_id, payload.name)`; if that id collides with an existing person
in the company, the drafts join that person instead of creating a new one.

### 4.3 Staleness

A correction is **stale** and not applied when:

- `evidence_hash` is non-zero and differs from the subject's current `draft_set_hash`
  (`override_field`, `approve_suggestion`, `reject_suggestion`, `merge_persons`, `set_role`,
  `remove_role`); or
- any listed `draft_ids` no longer exist in the company's drafts (`split_person`,
  `reassign_draft`, `set_role`, `remove_role`); or
- `approve_suggestion` names a suggestion whose `input_hash` no longer matches the current
  request for that person; or
- `set_role.role_code` is not active in `company_person_role_type`.

Stale rows are counted in asset metadata (`stale_correction_count`) and listed by a
backoffice query (`correction.evidence_hash != person.draft_set_hash`) so the reviewer can
re-decide. They are never silently applied and never deleted.

### 4.4 Idempotency

`company_status.is_unchanged` becomes

```
published.draft_ids = drafts.draft_ids
AND published.correction_set_hash = effective_correction_set_hash
```

so a new ledger row is itself "evidence changed" for exactly that company. Writes keep the
existing stage → validate → insert path; `_profile_changed` compares name, description,
draft_ids, correction_ids and suggestion_id.

### 4.5 Suggestions

Every LLM response the pipeline accepts (after the existing pydantic contract and repair
loop) is inserted into `se_company_person_suggestion` before it is used. Publishing
behaviour for multi-source companies does **not** change: absent a correction, the newest
suggestion for the current `input_hash` publishes (status today). `reject_suggestion`
blocks it; `approve_suggestion` pins it. Single-source companies never call the model and
have no suggestion rows.

`model_provider` is taken from settings rather than the hardcoded `"deepseek"`; a
`prompt_version` bump invalidates `input_hash` by construction (the version is part of the
request payload).

### 4.6 Sensor and job

- `se_company_person_review_job`: asset selection `se_company_person_clickhouse`,
  `se_company_person_role_draft_clickhouse`, `se_company_person_role_clickhouse` (no draft
  import).
- `se_company_person_correction_sensor` (60 s, RUNNING): cursor =
  `count():argMax(correction_id)` like `country_person_correction_cursor`; on advance,
  one `RunRequest` with `company_ids` = distinct `company_id` of ledger rows newer than the
  previous cursor's `created_at`, `run_key = f"se-company-person-correction:{cursor}"`.
- Until sub-project 4 schedules the full chain, this sensor is the only automatic trigger.

## 5. Backoffice

### 5.1 Write path

`app/lib/clickhouse.server.ts`: `chInsertSeCompanyPersonCorrections(values)` using the
writer client, same shape as `chInsertPersonCorrections`. Validation before insert, in a
pure module `app/lib/se-person-corrections.ts` (client-safe, unit-tested):

- `correction_kind` in §3; `company_id` is 10 digits; `payload` keys allowed for the kind;
  `draft_ids` cardinality per kind (exactly one for `reassign_draft`, ≥1 for split/roles,
  empty otherwise); `target_person_id` required for merge/reassign and ≠ subject;
  `role_code` active in `company_person_role_type` (server-side lookup).
- `evidence_hash` is filled by the server from the person's current `draft_set_hash` at
  form-render time and echoed back in the form; the action re-reads it and refuses the
  write with "the evidence changed while you were reviewing" if they differ.
- `decided_by = 'backoffice'` (matches the identity ledger until auth exists).

### 5.2 Person review page

Route `admin/se/people/person/:companyId/:personId` (ClickHouse-backed; the DuckDB pages
stay untouched until sub-project 4). Loader reads, for the pair:

- `se_company_person` row (name, description, draft_ids, correction_ids, suggestion_id,
  model provenance);
- its drafts from `se_company_person_draft` (source, name, role, fiscal year, payload);
- role rows from `se_company_person_role`;
- suggestions ordered newest first, with `is_current = input_hash matches`;
- ledger rows for the person (subject or target) with `is_current` derived as in
  `people.server.ts` (argMax per subject), and `is_stale` per §4.3.

Actions (one `intent` each, all append one row): approve suggestion, reject suggestion,
override name/description, merge into (search active people in the same company), reassign
one draft to another person, set/remove role for selected drafts, undo a listed correction.
After a write the page shows "Saved — Dagster will re-run company {id} within a minute" and
polls the person row's `updated_at`.

No "run Dagster now" button in this sub-project; the sensor is the trigger.

### 5.3 Navigation

Draft 2 rows and the company Management section link to the review page by
`(company_id, person_id)`. The existing `llm-input/:draftTwoId` page is unchanged.

## 6. Error handling

- Backoffice insert failure (writer credentials, CH down): form error, nothing written,
  no partial state possible (single-row inserts).
- Pipeline: invalid or stale corrections never abort a run; they are counted and logged
  with ids. A correction that references an unknown `person_id` is stale.
- The existing `RuntimeError("made no publish progress")` is relaxed for sensor-triggered
  runs: a company whose only change is a stale correction produces zero writes legitimately;
  the run reports `skipped_company_count` instead of failing.
- Sensor: a run for companies that no longer have drafts completes with zero writes.

## 7. Testing

- Migration contract test (`tests/test_clickhouse_migrations.py` pattern): column order of
  the two tables and the three added columns; grants present.
- `normalization.py` unit tests with an in-memory fake client: each kind in §3 applied to a
  fixture company; precedence order; staleness for every rule in §4.3; `is_unchanged`
  flips on a new ledger row and not on an unrelated company's row; undo chain.
- `roles.py`: `set_role`/`remove_role` against the year-scoped role rows; inactive
  `role_code` is stale.
- Sensor test: cursor advances per appended row; `company_ids` limited to touched
  companies.
- Backoffice: vitest for the pure validator (every kind, every rejection); integration
  test for the loader query against the real ClickHouse (project convention); component
  test for the review page actions building the right row; a writer test that the insert
  uses the write client and refuses when `evidence_hash` moved.
- End-to-end check on the server after deploy: append one `override_field` for a known
  person, observe the sensor run, confirm `se_company_person.correction_ids` contains it
  and the name changed; append `undo`, confirm it reverts.

## 8. Open items (decided here, change if wrong)

- Ledger row `decided_by` stays a free string; no user table yet.
- `raw_response` is stored (audit value outweighs size: ~3 KB × a few hundred rows).
- Suggestions from the backoffice's own interactive LLM runs are not written to
  `se_company_person_suggestion` in this sub-project; that migration is part of
  sub-project 4.
