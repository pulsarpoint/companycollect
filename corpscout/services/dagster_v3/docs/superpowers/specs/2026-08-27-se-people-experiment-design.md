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

### 3.1 Per-source SE VIEWS replace the draft inbox — no copies, no duplicated history

**Owner decision (2026-08-27): history and evidence live in the ORIGINAL source
tables only.** Sweden gets a uniform *shape*, not a copy. The evidence keys
already exist upstream: migration 000289's MATERIALIZED
`person_profile_hash` / `person_role_hash` on all four source tables are the
per-observation evidence hashes this experiment builds on.

Three plain ClickHouse VIEWS (one migration; a refreshable MV is unnecessary at
this scale — ESEF ~6.6k rows, Wikidata ~12k; the Bolagsverket view is a column
projection the optimizer pushes down):

| View | Over | Projects (uniform person-observation shape) |
|---|---|---|
| `se_company_person_bolagsverket` | `se_financial_report_signatories` | company_id, source_record_uid, person_profile_hash, person_role_hash, full_name (concat), first_name, last_name, role_original, role_kind, signatory_kind, fiscal_year |
| `se_company_person_esef` | `esef_document_people` WHERE country='SE' | company_id, source_record_uid, person_profile_hash, person_role_hash, full_name, role, role_category, organization, status, effective_from, effective_to, confidence |
| `se_company_person_wikidata` | `wikidata_company_people` + `wikidata_persons` joined via the orgnr/LEI bridge | company_id (normalized in the view; invalid ids filtered out), source_record_uid, person_profile_hash, person_role_hash, full_name, person_wikidata_id, role_property, start_date, end_date, birth_year, description, image_url, external_url |

Each view is pinned by a drift test comparing its stored SQL against the Python
builder (the `se_address_geocodes_served` pattern), and every view runs under
both `join_use_nulls` settings in the clickhouse-local harness.

**Consequences, accepted deliberately:**
- The final's `evidence_set_hash` is computed over the CURRENT upstream rows'
  000289 hashes. When an upstream row is replaced in place (ESEF re-enrichment
  under a new prompt, Wikidata weekly rebuild), the hash changes, the person
  re-resolves, and any pending correction goes stale-by-hash for re-review —
  exactly the already-shipped sub-project-1 behavior. The pre-change payload is
  not retained on the Swedish side; if pre-replacement history is ever wanted,
  that is an UPSTREAM table concern, decided per source, not a per-country copy.
- The `se_company_person_draft` inbox retires with nothing replacing it: the
  originals are the observation store, the views are the read contract, and the
  final's provenance arrays hold `source_record_uids` + the 000289 hashes
  observed at resolution time.

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

1. `se_company_person_draft` collector + table (after the views feed
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

## 6. Owner decisions (2026-08-27, spec review)

1. **Identity/merging**: deterministic K3 is the base rule (still validated by
   the §3.2 evaluation numbers). Person MERGING beyond the deterministic rule —
   the collision candidates — is **LLM-assisted, triggered from the backoffice**
   exactly like ESEF extraction: the Dagster merge asset takes the **LLM as a
   run parameter** (named run-config profiles, `<PROVIDER>_API_KEY` from host
   env, `execute` gate so a bare UI Materialize is a preview — the info-pilot /
   ESEF pattern verbatim). Never scheduled; never eager.
2. **`se_company_person_draft` is retired IN THIS WORK** — no parallel run.
   Collector, jobs, and table go with the standard drop gates once
   normalization reads the views.
3. **The company People tab switches to the new model immediately at first
   resolution** — no review-sample gate.
