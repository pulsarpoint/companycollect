# SE People Experiment — design (Phase C of the people cleanup)

> Status: DRAFT for owner review. Follows the owner decisions of 2026-08-27:
> Sweden only; no central cross-country people tables; per-source `se_` tables
> for Swedish-official extractions; multi-country source tables
> (`esef_document_people`, `wikidata_company_people`) stay source-scoped and are
> READ filtered to SE, never copied into global tables. `company_people_all` and
> `se_company_person_draft_legacy` were retired 2026-08-27 (migration 000328).
> Phase B (retiring `country_person*` + the public people pages) executes only
> after this experiment produces a serving-worthy replacement.

## 1. Goal

Settle, on Sweden only, the three unsolved questions of the people model:

1. **Identity** — who is "the same person" across observations, given no Swedish
   source carries a public person id (Bolagsverket XBRL signatories have split
   names only; ESEF people are LLM-extracted full names; Wikidata has QIDs for a
   tiny minority). Today's key — `first_token|last_token` of the name, scoped to
   one company — has 11,713 known collisions and silently merges distinct people.
2. **Connections** — the person↔company edge: grain, role vocabulary, validity.
3. **Update semantics** — how profiles/descriptions change over time without
   rewriting history, and how human review interacts with model suggestions.

Explicit non-goals: cross-country identity, cross-company identity (a person id
never spans companies in this experiment), personnummer acquisition, any change
to the public pages (Phase B's concern).

## 2. What already exists and is kept

- **Sources** (unchanged): `se_financial_report_signatories` (Bolagsverket XBRL
  signatures; split first/last name, `role_original`+`role_kind`, `fiscal_year`,
  `signatory_kind`, semantic hashes from migration 000289),
  `esef_document_people` (full name, `role_category` enum, `effective_from/to`,
  organization-as-description, filtered `country_code='SE'`),
  `wikidata_company_people` + `wikidata_persons` (full name, QID, birth_year,
  image/external URLs, P-code roles, start/end dates; bridged to orgnr/LEI).
- **Role taxonomy**: `company_person_role_type` (25 codes, migrations
  000290/000294/000319) + the three per-source maps in code. Kept as-is; the
  serving layer must start USING it (today's dbt management model ships raw
  per-source role strings — that mismatch ends here).
- **Connections table**: `se_company_person_role`, one row per
  `(company_id, person_id, role_code, fiscal_year)` since migration 000293.
  This grain is confirmed correct and stays.
- **Review machinery** (sub-project 1, migrations 000295/000296): append-only
  `se_company_person_correction` ledger, `se_company_person_enrichment_observation`
  with `input_hash` reuse, the correction sensor, the backoffice person review
  page. All kept; they are the experiment's review channel.

## 3. What changes — three moves

### 3.1 Per-source artifacts replace the draft inbox

Following the `se_company_<datatype>_<source>` envelope proven by `info` and
`address` (spec 2026-08-22, `se_company/common.py` helpers — `publish_with_stage`
with `new_versions_only`, evidence hashes, ledger sensor factory):

| New table | Reads | Payload columns (typed, beyond the envelope) |
|---|---|---|
| `se_company_person_bolagsverket` | `se_financial_report_signatories` | first_name, last_name, role_original, role_kind, signatory_kind, fiscal_year, statement_key, person_seq |
| `se_company_person_esef` | `esef_document_people` WHERE country='SE' | full_name, role, role_category, organization, status, effective_from, effective_to, confidence, source_document_id |
| `se_company_person_wikidata` | `wikidata_company_people`+`wikidata_persons` via orgnr/LEI bridge | full_name, person_wikidata_id, role_property, start_date, end_date, birth_year, description, image_url, external_url |

Envelope: `company_id, source_record_uid, observed_at, source_run_id,
evidence_hash MATERIALIZED SHA256('se-company-person-<source>-v1\n'||payload)`,
`ReplacingMergeTree(observed_at) ORDER BY (company_id, source_record_uid)`,
`CHECK match(company_id,'^[0-9]{10}$'|12-digit sole traders)` — identical to the
info/address artifact shape. Hand-written asset per source in
`defs/se_company/` (scb.py-style modules; no factories, per the standing rule).

The current `se_company_person_draft` collector (draft.py) is retired once the
artifacts feed normalization — it is the same three reads with a less regular
envelope. `draft.py`'s hashing discipline (semantic profile/role hashes) carries
over via `evidence_hash`.

### 3.2 The identity experiment (the actual research)

`person_id` stays company-scoped: `SHA256('se-company-person-v2\n{company_id}\n{identity_key}')`.
The experiment is what `identity_key` should be. Three candidate rules, evaluated
against each other on the full corpus before one is adopted:

- **K1 (baseline, today's)**: `first_token|last_token`, casefolded.
- **K2 (full-name)**: all tokens casefolded, whitespace-normalized, diacritics
  preserved (å/ä/ö are distinguishing in Swedish names). Middle names split
  people that K1 merges; misspellings split people that K1 also splits.
- **K3 (K2 + reconciliation pass)**: K2 keys, then a deterministic merge step
  inside one company: two K2 keys merge iff (a) one's token set is a strict
  superset of the other's with identical first+last tokens (middle-name
  presence/absence), or (b) a Wikidata QID observation links them. Anything
  else that K1 would have merged but K3 keeps apart becomes a
  **collision-review candidate** row for the backoffice — never auto-merged.

Evaluation (a one-off analysis asset writing a report table, not serving):
per-rule person counts, merge/split diff vs K1 (the 11,713 collisions
classified: genuinely distinct vs same-person variants, sampled for manual
inspection through the review page), and role-assignment stability. **The owner
picks the rule from the numbers**; the expectation is K3.

Identity versioning: the chosen rule's version lives in the person_id hash
domain (`-v2`). A future rule change is a new hash domain + full re-resolution,
exactly like a matcher `policy_version` bump — never an in-place mutation.

### 3.3 Update semantics — formalized, not invented

Already mostly built; the experiment pins the contract:

- **Observations are append-only** (artifacts); the final
  `se_company_person` row is derived state keyed `(company_id, person_id)` with
  `draft→artifact` provenance arrays and `ReplacingMergeTree(updated_at)`.
- **Change detection** by `evidence_set_hash` over the contributing artifact
  evidence hashes (framework-standard): a person re-resolves only when a
  contributing observation changed.
- **Descriptions**: deterministic single-source copy where only one source
  contributes; the LLM merges only multi-source conflicts, writing to
  `se_company_person_enrichment_observation` (reused by `input_hash`);
  suggestions become live only via the correction ledger or explicit
  auto-approval policy — the info-pilot pattern verbatim, including the
  `execute` gate (UI materialize = preview) and backoffice-triggered runs.
- **Corrections outrank recomputation**: ledger rows are applied after
  derivation with staleness marked by evidence hash (already implemented).

## 4. Retirements this unlocks (in order)

1. `se_company_person_draft` collector + table (after artifacts feed
   normalization; table dropped with the standard gates).
2. The backoffice **DuckDB Draft 1/2 + SQLite + Temporal person worker**
   (`/admin/se/people` re-points to the ClickHouse model; role mappings move
   from SQLite to the Python maps that Dagster already owns).
3. **Phase B**: `country_person*` (5 tables, daily pipeline, sensors,
   corrections) + the public people pages; the dbt management model drops the
   `country_person_match` join and switches `role_kind` to the canonical
   taxonomy, reading `se_company_person`/`se_company_person_role` for SE.

## 5. Operational shape

Group `se_company_person_<source>` artifacts (weekly, riding each source's
existing refresh), final assets in group `se_company` with the correction
sensor, NO eager automation on the final (owner rule: materialization from the
backoffice), schedules ship STOPPED until the identity rule is chosen and the
first full resolution validates. clickhouse-local test harness per the
established pattern; every SQL executed under both `join_use_nulls` settings.

## 6. Open questions for the owner (answers wanted at spec review)

1. Identity rule expectation confirmed (K3) — or should the experiment also
   price an LLM-assisted merge pass for the collision candidates?
2. May `se_company_person_draft` be retired as part of this work, or must it
   run in parallel until the first full re-resolution is reviewed?
3. Does the company People tab switch to the new model immediately on first
   resolution, or after a review sample?
