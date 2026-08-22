# Enrichment observations and review queue — design

Date: 2026-08-22. Status: draft for review. Owners: dagster_v3 (consumers, policy,
ledgers), backoffice (queue UI), producer services (agents, LLM passes).

Companion specs: `2026-08-21-se-company-person-corrections-design.md` (sub-project 1 —
first concrete instance of the ledger side of this design).

## 1. Problem

Automatic producers — the multi-source LLM pass today, AI agents tomorrow — will look for
records that are not fully enriched (company fields, people, technology/web records) and
propose values for them, continuously and in bursts of several rows per second. Most
proposals are good enough to accept automatically; some need a person. Decisions, whether
automatic or human, must survive pipeline rebuilds, be attributable, and be undoable.

Three things must therefore be kept apart, because they have different volumes, lifecycles
and stores:

| thing | volume | lifecycle | right store |
|---|---|---|---|
| **Observation** — what a producer saw and proposed | large, fast, per country | immutable | ClickHouse, append-only, batched |
| **Review item** — a proposal that needs a person | small (bounded by reviewer capacity) | pending → decided → gone | Postgres, hot, transactional |
| **Decision** — what was accepted or refused, on which evidence | small, human- or policy-paced | immutable | ClickHouse, append-only ledger |

Published data is derived from observations + decisions by the owning Dagster asset, which
remains the only writer of published ClickHouse tables.

## 2. Principles

1. **Producers write observations, never published rows.** An agent's output is evidence
   with a confidence, exactly like a Wikidata claim or a filing. It has no status.
2. **Policy is evaluated by the consumer, not the producer.** The Dagster asset that owns an
   entity type decides, per observation, whether the auto-approval policy accepts it; if not
   it opens a review item. Producers stay dumb and replaceable; thresholds live in one place.
3. **Queue size is bounded by construction.** Only contested proposals enter Postgres, and
   a decided item leaves the queue. History never accumulates in the queue table.
4. **Decisions are input, not edits.** The pipeline re-reads decisions on every run;
   staleness is detected by comparing the evidence hash the decider saw with current
   evidence. Published rows carry the ids of the decisions applied to them.
5. **Per-country grain for country-keyed entities.** Observation tables and decision
   ledgers are one per (country, entity type), matching how source tables and assets are
   organised and keeping write load, size and blast radius isolated. Domain-keyed entities
   (technology, web) are global. Cross-country reads use `merge()` like the rest of the
   repository.
6. **Insert discipline.** ClickHouse cost is parts per second. Producers batch per call and
   use a writer with `async_insert = 1, wait_for_async_insert = 1,
   async_insert_busy_timeout_ms = 1000`. One-row loops are a bug.

## 3. Components

### 3.1 Enrichment observation tables (ClickHouse)

One table per (country, entity type): `{cc}_{entity}_enrichment_observation`, e.g.
`se_company_enrichment_observation`, `se_company_person_enrichment_observation`,
`no_company_enrichment_observation`; domain-keyed: `domain_enrichment_observation`.
All are generated from one DDL template so a column-contract test can pin them; per-country
tables may add identity columns their register needs.

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_enrichment_observation
(
    observation_id    UUID,
    company_id        String,                      -- 10-digit orgnr (per-country identity column)
    aspect            LowCardinality(String),      -- 'description' | 'primary_domain' | 'employees' | …
    proposed          String,                      -- JSON, aspect-specific shape
    evidence          String,                      -- JSON: source refs, URLs, draft ids, quotes
    evidence_hash     FixedString(64),             -- hash of the evidence the producer used
    input_hash        FixedString(64),             -- hash of the exact producer input (prompt, model, evidence)
    confidence        Float32,                     -- 0..1; producers must calibrate per aspect
    producer          LowCardinality(String),      -- 'agent:web-enrich@3' | 'dagster:se_company_person' | …
    model_provider    LowCardinality(String),
    model_name        String,
    prompt_version    String,
    prompt_tokens     UInt32,
    completion_tokens UInt32,
    source_run_id     String,
    created_at        DateTime64(3, 'UTC'),

    CONSTRAINT valid_proposed CHECK isValidJSON(proposed),
    CONSTRAINT valid_evidence CHECK isValidJSON(evidence),
    CONSTRAINT confidence_range CHECK confidence >= 0 AND confidence <= 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (company_id, aspect, input_hash, created_at);
```

- `input_hash` is the idempotency key: a consumer reuses an observation whose input hash
  matches the current request instead of re-running the producer. Producers should look
  before they leap (skip when an observation with the same `input_hash` exists).
- Retention: partitions older than N months can be dropped per table once every observation
  in them is either superseded by a newer `input_hash` or referenced by a decision; N is a
  per-table setting, default 24.
- `se_company_person_suggestion` from sub-project 1 is renamed
  `se_company_person_enrichment_observation` and is the first instance of this template
  (its `suggestion` column is `proposed`, `person_id` is its entity column, `draft_ids`
  its evidence key).

### 3.2 Auto-approval policy (Postgres, read by Dagster)

```sql
CREATE TABLE auto_approval_policy (
  id               serial PRIMARY KEY,
  country_code     text,                      -- NULL = any
  entity_type      text NOT NULL,             -- 'company' | 'company_person' | 'domain'
  aspect           text NOT NULL,             -- '*' allowed
  producer_pattern text NOT NULL,             -- SQL LIKE on producer, e.g. 'agent:web-enrich@%'
  min_confidence   real NOT NULL,
  requires_no_conflict boolean NOT NULL DEFAULT true, -- reject auto-approval when a current value disagrees
  enabled          boolean NOT NULL DEFAULT true,
  created_by       text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);
```

The consuming asset loads enabled policies at run start. For each new observation:
`accepted` if any policy matches (country, entity, aspect, producer) with
`confidence >= min_confidence` and, when `requires_no_conflict`, the proposed value does not
contradict a published value that itself came from a decision; otherwise `contested`.
Accepted observations are applied directly and recorded in the decision ledger with
`decided_by = 'policy:<id>'` so the audit trail is uniform. Contested ones open a review
item. The multi-source person merge at high confidence is one policy row; lowering a
threshold is a policy edit, not a deploy.

### 3.3 Review queue (Postgres)

```sql
CREATE TYPE review_status AS ENUM ('pending', 'claimed', 'approved', 'rejected', 'expired');

CREATE TABLE review_item (
  id               bigserial PRIMARY KEY,
  country_code     text,                      -- NULL for domain-keyed entities
  entity_type      text NOT NULL,
  entity_key       jsonb NOT NULL,            -- {"company_id":"5560125220"} | {"person_id":"…"} | {"domain":"…"}
  aspect           text NOT NULL,
  observation_table text NOT NULL,            -- which *_enrichment_observation row this came from
  observation_id   uuid NOT NULL,
  proposed         jsonb NOT NULL,            -- copied for display; the observation row is the source of truth
  current_value    jsonb,                     -- what is published now, for side-by-side review
  evidence_hash    text NOT NULL,
  confidence       real,
  producer         text NOT NULL,
  reason           text NOT NULL,             -- why it is contested: 'below_threshold' | 'conflict' | 'structural' | 'no_policy'
  status           review_status NOT NULL DEFAULT 'pending',
  claimed_by       text, claimed_at timestamptz,
  decided_by       text, decided_at timestamptz, decision_note text,
  exported_at      timestamptz,               -- set when the decision reached the ClickHouse ledger
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (observation_table, observation_id)
);
CREATE INDEX review_item_queue ON review_item (status, country_code, entity_type, created_at);
CREATE INDEX review_item_entity ON review_item USING gin (entity_key);

CREATE TABLE review_event (                    -- append-only audit of status changes
  id bigserial PRIMARY KEY,
  review_item_id bigint NOT NULL REFERENCES review_item(id),
  from_status review_status, to_status review_status NOT NULL,
  actor text NOT NULL, note text, created_at timestamptz NOT NULL DEFAULT now()
);
```

Lifecycle: Dagster inserts `pending`; a reviewer claims (optional, 30-minute lease),
decides; the decision is exported to the ClickHouse ledger by the next sensor-triggered run
(outbox: `exported_at` set after the ledger insert is confirmed); rows with `exported_at`
older than 7 days are deleted. `expired` is set by the consumer when the item's
`evidence_hash` no longer matches current evidence — the reviewer sees it, nothing is
applied.

Human-initiated structural corrections (merge, split, reassign, override) do not need the
queue: the entity's review page appends directly to the ledger, as sub-project 1 specifies.
The queue exists for producer-initiated proposals.

### 3.4 Decision ledgers (ClickHouse, per country)

One append-only ledger per (country, entity type): `{cc}_{entity}_correction`, the shape
already defined for `se_company_person_correction` in sub-project 1, extended with:

```sql
    observation_id   Nullable(UUID),     -- the observation accepted/rejected, when there is one
    review_item_id   Nullable(Int64),    -- the queue item, when a person decided
    confidence       Nullable(Float32),  -- copied from the observation for analytics
```

`decided_by` is `'policy:<id>'`, `'backoffice'` (later a user id), or `'producer:<name>'`
for a producer-side assertion that needs no review (rare; must be allow-listed). The
ledger is what the pipeline re-reads; the queue is never read by the pipeline after export.

### 3.5 Application in the owning asset

Per entity, per run:

1. Load current observations for the selected scope (`input_hash`-deduplicated, newest per
   `(entity, aspect, input_hash)`), live decisions from the ledger (superseded rows removed),
   and enabled policies.
2. Evaluate policy for observations without a decision → accept (write policy decision) or
   open a review item (skip if one exists for that observation).
3. Compute the published value per aspect with precedence
   **human decision > policy decision > deterministic source rule**; a decision whose
   `evidence_hash` no longer matches the current evidence is stale: skipped, counted,
   surfaced. A rejected observation is never reconsidered unless its `input_hash` changes.
4. Publish through the asset's existing stage → validate → insert path; published rows carry
   `correction_ids` (decision ids) and the `observation_id` they derive from.
5. Emit metadata: `accepted_observation_count`, `contested_observation_count`,
   `stale_decision_count`, `applied_decision_count`.

Idempotency: the entity's change detector includes the set of live decision ids and the set
of observation `input_hash`es it consumed, so a new observation or decision re-selects
exactly that entity.

### 3.6 Triggers

- Producers: their own cadence; they write observations and stop.
- `{entity}_observation_sensor` (Dagster, 60 s): cursor on `max(created_at)` of the
  observation table; scoped run over touched entities.
- `{entity}_decision_sensor`: cursor on `max(decided_at)` of `review_item` where
  `status IN ('approved','rejected') AND exported_at IS NULL`, plus the ClickHouse ledger
  cursor from sub-project 1; scoped run that exports the decision and applies it.
- Scheduled full runs stay as they are; sensors only shorten the loop.

### 3.7 Backoffice

- `/admin/review` — queue list: filters by country, entity type, aspect, producer, reason;
  claim, approve, reject with a note; side-by-side proposed vs current vs evidence.
- `/admin/review/:id` — one item, with a link to the entity's page.
- `/admin/settings/policies` — list/edit `auto_approval_policy`; every edit is recorded
  (`created_by`, new row; disable rather than delete).
- Entity pages (company, person, domain) show pending items and applied decisions for that
  entity, and keep their direct structural actions.

Backoffice talks to Postgres with `BACKOFFICE_POSTGRES_URL` (app role); migrations with the
owner role; Dagster with `BACKOFFICE_POSTGRES_DAGSTER_URL`. All three are already defined in
`.env.example` and unused; the database lives on the central Corpscout Postgres.

## 4. Data flow

```
producers (agents, LLM passes) ──batch──▶ CH {cc}_{entity}_enrichment_observation
                                                   │ observation sensor
                                                   ▼
                                  Dagster owning asset: policy evaluation
                                     accepted ──▶ CH {cc}_{entity}_correction (policy decision)
                                     contested ─▶ PG review_item (pending)
                                                   │ human decides in backoffice
                                                   ▼ decision sensor
                                  Dagster: export decision ──▶ CH ledger; apply ──▶ published tables
```

ClickHouse is written only by producers (observation tables, batched) and Dagster
(everything else). Postgres is written by Dagster (open items, policy decisions are not
stored there) and the backoffice (claims, decisions, policies).

## 5. Grain and naming

| entity | key | observation table | ledger | review item `entity_key` |
|---|---|---|---|---|
| company (country-keyed) | `company_id` | `{cc}_company_enrichment_observation` | `{cc}_company_correction` | `{"company_id"}` |
| company person | `person_id` | `{cc}_company_person_enrichment_observation` | `{cc}_company_person_correction` | `{"person_id"}` |
| domain / technology | `root_domain` | `domain_enrichment_observation` | `domain_correction` | `{"domain"}` |

Not per aspect, not per producer — those are columns. Adding a country = generating the two
tables from the template and registering the entity type's asset; nothing in the queue or
policy tables changes.

## 6. Producer contract

A producer (agent service, LLM pass, script) must:

- write only to observation tables, in batches, through an async-insert writer;
- set `producer` to a stable name with a version suffix, `input_hash` from its exact input,
  `evidence_hash` from the evidence it used, `confidence` calibrated for the aspect;
- check for an existing observation with the same `input_hash` before doing expensive work;
- never write to published tables, the ledger, or the queue.

Sizing guidance: one agent at 5 rows/s sustained is ~18k rows/hour and, with async inserts,
~1 part/s — comfortable. Ten agents should share one writer process or an ingestion
endpoint rather than each holding a ClickHouse connection.

## 7. Existing flows and how they map

- `se_company_person_correction` + `se_company_person_suggestion` (sub-project 1) — the
  ledger is already this design; the suggestion table is renamed to
  `se_company_person_enrichment_observation`. No queue needed yet: the only producer is
  Dagster itself and the only reviewer acts from the person page.
- `country_person_correction` — stays; a ledger for a cross-country resolver.
- `company_domains` — review state lives on the published row (`review_status`,
  `reviewed_evidence_fingerprint`). It keeps working; migrating it to observation + ledger
  is optional and would be done when a second domain producer appears.
- Backoffice DuckDB drafts, SQLite responses, Temporal worker — retired (sub-project 4).

## 8. Error handling

- Producer insert failure: producer retries its batch; duplicates are harmless because
  consumers dedupe on `input_hash`.
- Policy table unreachable at run start: the run proceeds with **no** auto-approval (all
  new observations contested) and reports `policy_unavailable = true`; it never guesses.
- Queue insert failure for a contested observation: the observation stays unconsumed and is
  retried next run (the `UNIQUE (observation_table, observation_id)` constraint makes
  retries idempotent).
- Decision export failure: `exported_at` stays NULL; the decision sensor retries; the queue
  row is never deleted before export is confirmed.
- Stale decision: skipped, counted, shown on the item and entity pages; never deleted.

## 9. Testing

- DDL template contract test per generated observation table and ledger (columns in order,
  engine, partition, constraints).
- Policy evaluation unit tests: match by country/entity/aspect/producer pattern,
  threshold, `requires_no_conflict`, disabled rows, unavailable table.
- Consumer unit tests with fake clients: accept path writes a policy decision and applies;
  contested path opens exactly one item per observation; stale decision skipped; precedence
  human > policy > deterministic; `input_hash` reuse.
- Sensor tests: cursors advance per observation / per decided item; scoped run config.
- Backoffice: queue list filters and claim/approve/reject transitions (integration against
  a test Postgres), policy editing creates rows and never deletes.
- End to end on the server: one agent batch → observation rows → accepted ones published
  within a sensor cycle; one contested item → approve in the queue → applied; reject →
  never applied; expire by changing evidence.

## 10. Open decisions

- Claim lease length (30 min proposed) and whether claims are required at all for a
  single-reviewer team (proposed: optional).
- Retention N for observation partitions (24 months proposed).
- Whether policy decisions should also be mirrored into `review_event` for one audit view
  (proposed: no — the ClickHouse ledger is the audit; the queue audit covers human steps).
- Backoffice Postgres migration tooling (proposed: plain numbered SQL files applied with the
  owner role, mirroring the ClickHouse migration convention).

## 11. Sequencing

1. **Sub-project 1** (person corrections) proceeds as planned, with the suggestion table
   renamed to the observation name.
2. **Sub-project 5a — observation template + policy + consumer changes** for one entity
   type (company person is the natural first, since its consumer already exists).
3. **Sub-project 5b — review queue** (Postgres schema, export sensor, backoffice queue and
   policy pages), started when the first producer other than Dagster is ready, so the
   envelope is designed against a real payload.
4. Further entity types (company fields, domains) add a table pair and an asset, nothing
   else.
