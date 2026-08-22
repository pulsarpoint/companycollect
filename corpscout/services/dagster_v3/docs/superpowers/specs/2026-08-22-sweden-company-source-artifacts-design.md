# Sweden company data: per-source artifacts and per-datatype finals — design

Date: 2026-08-22. Status: draft for review. Owner: dagster_v3, new package `defs/se_company/`.

Companion specs: `2026-08-21-se-company-person-corrections-design.md` (people — the first
datatype, built by hand; stays in `company_people/` for now) and
`2026-08-22-enrichment-observations-and-review-queue-design.md` (observations, policy, review
queue — what the finals plug into).

## 1. Decision

Company information for a country is assembled in three explicit layers, each a Dagster asset
group, each writing only its own tables:

| layer | where | writes | reads |
|---|---|---|---|
| **Source** (exists) | one folder per source: `esef_filings/`, `wikidata/`, `sweden_financial/`, `sweden_company/` (SCB register), … | the source's own tables (`esef_*`, `wikidata_*`, `se_companies`, …) | the outside world |
| **Country artifacts** (new) | `defs/se_company/<source>.py`, group `se_company_<source>` | `se_company_<datatype>_<source>` | the source layer's published assets |
| **Country finals** (new) | `defs/se_company/<datatype>.py`, group `se_company` | `se_company_<datatype>` (+ its ledger/observation tables) | the artifacts of that datatype |

`company_serving` / `companies_all` (the cross-country serving layer, frozen until 15 countries)
read the finals; they are not changed by this design.

**Simplest possible:** plain `@dg.asset` functions, one per table, written by hand, each with a
docstring that states its input asset(s), its output table, what it does and what triggers it.
No factories, no spec objects. Shared code is three small helpers in `se_company/common.py`.
Discipline is enforced by naming, the table envelope, and tests that iterate a tuple of table
names — not by a framework.

## 2. Folder

```
defs/se_company/
  README.md        this design, condensed: layers, naming, envelope, how to add a source/datatype
  common.py        publish_with_stage(), ledger_sensor(), reuse_or_call() — the only shared code
  scb.py           group se_company_scb          ← sweden_company_companies_clickhouse (register = company_id universe)
  esef.py          group se_company_esef         ← esef_document_company_information_clickhouse (+ esef_source_documents)
  wikidata.py      group se_company_wikidata     ← wikidata_companies (+ company_identifier links)
  bolagsverket.py  group se_company_bolagsverket ← sweden_financial_company_source_records_clickhouse
  commoncrawl.py   group se_company_commoncrawl  ← company_domains (confirmed links) + commoncrawl_* company_info   (later)
  info.py          group se_company              → se_company_info        (merge asset, ledger, sensor)
  financial.py     group se_company              → se_company_financial   (precedence pick, no ledger, no LLM)
```

Deliberately **not** in this folder, documented in the README as "lives elsewhere":
- `address` — the existing resolution/geocoding pipeline in `sweden_company/` already produces the
  final address tables; it is listed as the `address` datatype with its owning module.
- `people` — `company_people/` (`se_company_person*`); planned move to `se_company/people.py`,
  not in this round.
- The SCB register ingest itself (`sweden_company/assets.py`, `se_companies`) stays where it is:
  it is the source layer for `scb.py`.

## 3. Naming

- Artifact table: `se_company_<datatype>_<source>`; asset: `<table>_clickhouse`; group:
  `se_company_<source>`.
- Final table: `se_company_<datatype>`; asset `<table>_clickhouse`; group `se_company`.
- Ledger / observation tables of a final: `se_company_<datatype>_correction`,
  `se_company_<datatype>_enrichment_observation` (same shape as the people ones, migration 000295).
- Datatype names are singular nouns: `info`, `address`, `people`, `financial`, `domain`.
- Source names match the source folder: `scb`, `esef`, `wikidata`, `bolagsverket`, `commoncrawl`.

Datatype before source, so one datatype's sources sort together and
`merge('corpscout', '^se_company_info_')` is the union for free.

## 4. Table envelope

Every artifact table starts with the same five columns, then the source's own typed payload.
Engine `ReplacingMergeTree(observed_at)`, `ORDER BY (company_id, source_record_uid)` — rows are
versions; the newest per record wins; history is kept.

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_esef
(
    company_id        String,                       -- 10-digit orgnr, validated
    source_record_uid String,                       -- the source's stable record key (filing id, QID, …)
    observed_at       DateTime64(3, 'UTC'),         -- when this version was processed from the source
    source_run_id     String,
    evidence_hash     FixedString(64) MATERIALIZED  -- semantic hash of the payload columns below
        lower(hex(SHA256(concat('se-company-info-esef-v1\n', legal_name, '\n', ifNull(description, ''), '\n', ...)))),
    -- payload: ESEF's own shape
    legal_name        String,
    description       Nullable(String),
    fiscal_year       UInt16,
    ...
    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);
```

Every final table carries, after its typed merged columns: `source_record_uids Array(String)`,
`evidence_hashes Array(String)` (64-char lowercase hex — `Array(String)` rather than
`Array(FixedString(64))` so a short value can never be NUL-padded into a wrong hash), `correction_ids Array(UUID)`,
`suggestion_id Nullable(UUID)`, `model_provider`, `model_name`, `prompt_version`,
`source_run_id`, `resolved_at`; engine `ReplacingMergeTree(resolved_at)`, `ORDER BY (company_id)`.
Change detection for a company = any artifact `observed_at` > final `resolved_at`, or the live
ledger set ≠ `correction_ids`.

## 5. An artifact asset (the shape every source file repeats)

```python
"""Swedish company artifacts extracted from ESEF filings.

Input (source layer): esef_document_company_information_clickhouse joined to
esef_source_documents — the published, typed ESEF payload (not the generic
company_source_records provenance layer, which is an audit trail rather than a typed input).
This module keeps SE issuers and writes one artifact table per datatype with the standard
envelope followed by ESEF's own typed columns.

Assets
  se_company_info_esef_clickhouse       → corpscout.se_company_info_esef       (name, description, activity text per filing)
  se_company_financial_esef_clickhouse  → corpscout.se_company_financial_esef  (metrics per fiscal year)
Downstream: info.py, financial.py.
"""

@dg.asset(
    name="se_company_info_esef_clickhouse",
    deps=[dg.AssetKey("esef_document_company_information_clickhouse")],
    group_name="se_company_esef",
    kinds={"clickhouse", "python"},
    metadata={"table": "corpscout.se_company_info_esef"},
    description="Name, description and activity text as reported in each Swedish ESEF filing; "
                "a new version is appended only when the evidence hash changes.",
)
def se_company_info_esef_clickhouse(context, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    """Select SE rows from the source table → stage → validate envelope → insert new versions → counts."""
    assert_clickhouse_tables_exist(clickhouse, database="corpscout",
                                   tables=("esef_company_source_records", "se_company_info_esef"))
    counts = publish_with_stage(
        clickhouse,
        target="se_company_info_esef",
        select_sql=SE_COMPANY_INFO_ESEF_SQL,          # module constant, text-tested
        new_versions_only=("company_id", "source_record_uid", "evidence_hash"),
        validate="trim(company_id) = '' OR trim(source_record_uid) = ''",
    )
    return dg.MaterializeResult(metadata={**counts, "table": "corpscout.se_company_info_esef"})
```

Rules for a source file (README):
1. It may read only the source layer's published assets (declared in `deps`) and write only
   `se_company_<datatype>_<source>` tables.
2. Each asset appends new versions only; it never rewrites history or touches a final.
3. The SELECT is a module constant so a text test and the clickhouse-local harness can run it.

## 6. A final asset (the shape every datatype file repeats)

```python
"""Final Swedish company information, one row per company, merged from the per-source artifacts.

Inputs: se_company_info_scb (identity and legal name — authoritative), se_company_info_esef,
se_company_info_wikidata, later se_company_info_commoncrawl.
Rules (merge_company_info, pure function, unit-tested) — refined 2026-08-22: only the
description is merged; every other column is copied from its owning source as-is.
  legal_name, legal form, status, dates, industry ← SCB always.
  wikidata_id ← Wikidata; lei ← ESEF.
  description  ← exactly one source has one: copy it (no model call);
                 two or more: the model writes one description from all of them,
                 published with description_source = 'llm' and description_sources /
                 description_source_record_uids naming every contributor. No
                 agreement heuristic — several sources always go to the model.
Ledger: se_company_info_correction (override_field / approve_suggestion / reject_suggestion / undo)
        wins over everything; stale by evidence_hash, as for people.
LLM:    conflicts only; request cached by input_hash in se_company_info_enrichment_observation;
        policy (sub-project 5) decides auto-accept vs review_item.
Trigger: any artifact moved since resolved_at; se_company_info_correction_sensor; weekly schedule.
"""

@dg.asset(
    name="se_company_info_clickhouse",
    deps=[dg.AssetKey("se_company_info_scb_clickhouse"),
          dg.AssetKey("se_company_info_esef_clickhouse"),
          dg.AssetKey("se_company_info_wikidata_clickhouse")],
    group_name="se_company",
    kinds={"clickhouse", "python", "llm"},
    metadata={"table": "corpscout.se_company_info"},
    description="One merged information row per Swedish company with full provenance.",
)
def se_company_info_clickhouse(context, config: SECompanyScopeConfig, clickhouse) -> dg.MaterializeResult:
    companies = changed_companies(clickhouse, artifacts=SE_COMPANY_INFO_ARTIFACTS,
                                  final="se_company_info", ledger="se_company_info_correction",
                                  scope=config.company_ids)
    rows = load_artifact_rows(clickhouse, companies, SE_COMPANY_INFO_ARTIFACTS)
    outcomes = [merge_company_info(company, rows[company]) for company in companies]
    outcomes = apply_ledger(outcomes, load_corrections(clickhouse, companies, "se_company_info_correction"))
    outcomes = resolve_conflicts_with_llm(outcomes, reuse_or_call, COMPANY_INFO_PROMPT)
    accepted, contested = apply_policy(outcomes)
    publish_with_stage(clickhouse, target="se_company_info", rows=to_final_rows(accepted))
    open_review_items(contested)
    return dg.MaterializeResult(metadata=outcome_counts(outcomes))
```

Rules for a final file:
1. It reads only its datatype's artifacts, ledger and observation table; writes only its final,
   its observation rows and review items.
2. Deterministic rules first; the LLM sees only flagged conflicts; corrections win; nothing aborts
   a run (stale/invalid are counted and logged with ids).
3. `financial.py` is the exception: a precedence pick per metric and year (source priority,
   recency, completeness) — **no ledger, no LLM**; figures are never merged by a model.

## 7. `common.py` — the only shared code

- `publish_with_stage(clickhouse, *, target, select_sql=None, rows=None, validate, new_versions_only=None)` —
  stage table → validate → insert (optionally only rows whose `(keys…)` are not yet in the
  target) → drop stage → shrink guard on the published table. Today this block is copied in
  `normalization.py`, `roles.py`, `sweden_financial/history.py`, `officers.py`.
- `ledger_sensor(*, name, table, job, review_assets)` — the tuple-cursor sensor from
  `company_people/corrections.py`, parameterised by ledger table and job.
- `reuse_or_call(input_hash, stored, call)` — the suggestion-reuse step from
  `company_people/normalization.py`.

Nothing else is shared. If a fourth copy of something appears, extract it then.

## 8. Tests (the enforcement)

In `tests/test_se_company_layout.py`, driven by two tuples declared in `se_company/README.md`-adjacent
code (`se_company/tables.py`): `SE_COMPANY_ARTIFACT_TABLES`, `SE_COMPANY_FINAL_TABLES`.

- Envelope contract: each artifact migration's column list starts with the five envelope
  columns in order; `evidence_hash` is `MATERIALIZED`; engine/ORDER BY as §4.
- Provenance contract: each final migration carries the §4 provenance columns.
- Definitions contract: for every artifact table an asset `<table>_clickhouse` exists with
  `group_name = se_company_<source>` and at least one `deps` entry in the source layer; for every
  final, an asset in group `se_company`, a ledger sensor (except `financial`), a freshness leaf in
  `CLICKHOUSE_LEAVES`, and a schedule or membership in one.
- Rule tests: `merge_company_info` and friends are pure functions with table-driven cases
  (agree / single source / conflict / missing / correction wins / stale correction).
- Executed SQL: the clickhouse-local harness (`tests/test_se_company_person_clickhouse_local.py`
  pattern) parametrised over the artifact SELECTs and each final's publish path.

## 9. Pilot: `info`

1. Migrations: `se_company_info_scb`, `se_company_info_esef`, `se_company_info_wikidata`,
   `se_company_info`, `se_company_info_correction`, `se_company_info_enrichment_observation`
   (ledger/observation DDL identical to 000295 with the person columns replaced by
   `company_id` only), plus writer grants.
2. `scb.py`, `esef.py`, `wikidata.py` with the `info` artifacts only.
3. `info.py` with the merge asset, `merge_company_info`, ledger sensor, weekly schedule,
   freshness leaf.
4. Backoffice: the company page's information section reads `se_company_info`; the review
   page pattern from sub-project 1 is reused for `info` corrections (override description,
   approve/reject suggestion, undo).
5. Then `financial.py` (precedence view over `se_company_financial_esef` /
   `_bolagsverket`), then `commoncrawl.py`, then move people.

## 10. Out of scope

Cross-country unification (frozen), addresses (already resolved elsewhere), moving the people
pipeline, the Postgres review queue itself (sub-project 5b — `info` conflicts go to the same
ledger/observation mechanism as people until the queue exists).
