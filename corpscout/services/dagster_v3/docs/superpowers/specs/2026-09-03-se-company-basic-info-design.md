# SE company basic info: entity, suggestions, fold, history

Date: 2026-09-03. Status: approved in brainstorming, awaiting the owner's review of this file.

Supersedes the field-registry design of 2026-09-02 (`2026-09-02-se-company-field-registry-design.md`). Nothing from that design or its five plans reached production; its 62 commits were reverted on this design's branch before slice 0 (commit `ea39cd40`), so the code base starts from the deployed state: the old `se_company_info` publisher, its sensor and weekly, the enum-validated admin page, migrations ending at 000372.

## 1. Why

The field registry stored every company attribute as one long row per company, field and source, folded by generated SQL into a wide row. It covered scalar info well and nothing else: addresses, industries and people each kept their own half-model (per-source tables plus a correction ledger, or observations plus a `_current` table), with no shared shape and no history. The owner's review on 2026-09-03: the abstraction did not match how the domain is thought about.

The replacement is one shape for every company entity, applied here to basic info first:

- **source tables**, one per source, in the source's own shape (they exist);
- **one suggestions table** per entity, in the entity's shape, one current row per company and source, the reviewer being a source like any other;
- **one main table** per entity, written by a per-company fold function in Python, with per-field provenance;
- **one history table** per entity, appended only when the folded row changes.

Decisions taken in brainstorming: one entity at a time, basic info first; the fold is Python in Dagster, batched, triggered by hand for now (no sensor); descriptions belong to basic info and the LLM is a source with its own precedence; precedence is a number per field and source; suggestions keep one current row per company and source; the main row carries a `<column>_source` beside every value; basic info carries identity, legal facts and descriptions only.

## 2. Scope

In scope: the three tables, the precedence export, the six extractors, the fold assets, the admin info page and its three reviewer actions, the pipeline sheet, the cutover with parity, the retirement of the old publisher and of the field-registry code, and the later switch of every `se_companies` reader to the new main table.

Out of scope, later entities on the same shape: addresses, industries, people, financials, domains (website), jobs, listings, proceedings. Also out of scope: the suggestions sensor (postponed until the manual fold has proven itself), public country views, other countries.

## 3. Tables

All in database `corpscout`, ClickHouse 26.5, created by golang-migrate migrations (first line `CREATE DATABASE IF NOT EXISTS corpscout;`, no `;` inside comments, last line a statement).

### 3.1 Source layer (slice 0)

Principle, decided by the owner: a source table holds what that source provides, in the source's own organisation, named per source; nothing merges sources; the entity's suggestion extractors pull from them. Wikidata (`wikidata_companies`), ESEF (`esef_document_company_information`) and Ratsit (`se_ratsit_company`) already satisfy this and are read as they are. LLM answers stay in `se_company_info_enrichment_observation`.

The two register sources get their own tables, both new, both written by the existing register loads straight from the normalised DuckDB layer (`company_registry_states` split by source, without the derived status):

- `se_scb_companies`: one row per company, the whole SCB register record: the SCB identifier, company name, legal-form code, the two SCB status codes, registration date, the five SNI code columns, the address columns, the marketing-block flag. English column names where the name is a plain rename, SCB's own codes untouched, no derived status. The `m*` previous-value twins are dropped: the S3 snapshots keep every file as delivered.
- `se_bolagsverket_companies`: one row per company, the whole Bolagsverket record: identity, name-protection sequence, registration country, name, legal form, deregistration date and reason, the pending-proceedings field, registration date, activity description, postal address.
- Both `ReplacingMergeTree(observed_at) ORDER BY company_id`, replaced only when the source record changes, with `source_run_id`, `source_record_id`, `source_payload_hash` as provenance. When a source stops delivering a company the publisher appends a tombstone row (`has_company = 0`, values NULL, empty record id and hash, the delivery's `source_run_id` and `observed_at`); a company that returns is inserted again, so readers take `FINAL` rows `WHERE has_company = 1` (owner decision 2026-09-03, after the slice-0 review). Dates the sources deliver as text keep a verbatim twin (`registration_date_raw`, `deregistration_date_raw`): Date32 starts at 1900-01-01 and 631 Bolagsverket registration dates are older, so the typed column is NULL for them and the twin keeps the value (same decision). No history table: S3 is the archive, as for every other source.

Where source codes become entity values (SCB status codes to `status`, Bolagsverket's legal-form text to a code, dates parsed) is the suggestion extractor of that source, one per source, not a merge asset. Amended 2026-09-04 (owner decision after the slice-2 prod run): the entity's `legal_form_code` is SCB's *juridisk form* two-digit code, Sweden's national standard; the Bolagsverket extractor maps its organisationsform tokens (`AB-ORGFO` to 49, `E-ORGFO` to 10, `HB-`/`KB-ORGFO` to 31, ...) through `legal_form.py`, an unknown token passes through as a visible gap, and the raw token stays in `se_bolagsverket_companies`. Before this, half the main table carried tokens and half SCB codes in one column.

Deleted in this design: `se_company_registry_observations` and `se_company_registry_current` (slice 0, after the domain-suggestions dbt model reads the union of the two new tables), `se_companies` and its builder (slice 5, after every reader moved to the main table), the derived artifacts `se_company_info_scb`, `se_company_info_esef`, `se_company_info_wikidata` (slice 4, with the old publisher). Addresses, industries and proceedings keep their tables and assets untouched until their entity's turn, although the SCB row now carries address and SNI columns too.

### 3.2 `se_company_basic_info_suggestion`

One current row per company and source.

```
company_id            String
source                LowCardinality(String)   -- scb, bolagsverket, wikidata, esef, ratsit, llm, reviewer
source_record_uid     String                   -- the source row this came from; the observation id for llm; '' for reviewer
observed_at           DateTime64(3, 'UTC')     -- when the source observed it; the decision instant for reviewer
legal_name            Nullable(String)
legal_form_code       Nullable(String)
status                Nullable(String)
incorporation_date    Nullable(Date32)
lei                   Nullable(String)
wikidata_id           Nullable(String)
description           Nullable(String)
description_language  Nullable(String)         -- language of `description`
description_sv        Nullable(String)
decided_by            Nullable(String)         -- reviewer rows only
note                  Nullable(String)         -- reviewer rows only
suggested_at          DateTime64(3, 'UTC')     -- version
source_run_id         String
extractor_version     LowCardinality(String)
ENGINE = ReplacingMergeTree(suggested_at) ORDER BY (company_id, source)
CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
```

NULL in a value column means "this source has no opinion", never "this source says empty". An extractor inserts a row only when the source's current record carries a newer `observed_at` than the current suggestion row for that company and source (or no suggestion row exists), so an unchanged daily refresh writes nothing; a source change outside the basic-info fields writes an identical suggestion row, which the fold then finds unchanged (owner decision 2026-09-03: no content hash, the source timestamp is the change signal, and each extractor must use a timestamp that is monotonic per source record). A source with several records per company (ESEF filings) contributes its newest record. A reviewer decision is a new version of the reviewer row; a release sets that one field back to NULL in a new version.

### 3.3 `se_company_basic_info`

One row per published company.

```
company_id                  String
legal_name                  String              legal_name_source           LowCardinality(String)
legal_form_code             Nullable(String)    legal_form_code_source      LowCardinality(String)
status                      LowCardinality(String)  status_source           LowCardinality(String)
incorporation_date          Nullable(Date32)    incorporation_date_source   LowCardinality(String)
lei                         Nullable(String)    lei_source                  LowCardinality(String)
wikidata_id                 Nullable(String)    wikidata_id_source          LowCardinality(String)
description                 Nullable(String)    description_source          LowCardinality(String)
description_language        Nullable(String)
description_sv              Nullable(String)    description_sv_source       LowCardinality(String)
folded_at                   DateTime64(3, 'UTC')  -- version
fold_version                LowCardinality(String)
source_run_id               String
ENGINE = ReplacingMergeTree(folded_at) ORDER BY company_id
```

A `_source` column is `''` when the field has no value; `status` itself is `''` then (the column is not nullable, as on the old table), every other value column is NULL. Publish rule: a company gets a row only when SCB or Bolagsverket supplies its legal name; otherwise the fold writes nothing and any earlier row stands. The legal-form labels (`legal_form_label_en`/`_sv`) are not stored; the serving view joins `se_code_labels` as today.

### 3.4 `se_company_basic_info_history`

Append-only, one row per change of the main row.

```
company_id, every column of se_company_basic_info, changed_fields Array(String)
ENGINE = MergeTree ORDER BY (company_id, folded_at)
```

Written by the fold when the folded row differs from the current main row, including the first publish (`changed_fields` = every non-NULL field).

### 3.5 `se_company_basic_info_precedence`

The precedence table as exported from code: `field, source, precedence UInt32, exported_at` (ReplacingMergeTree(exported_at) ORDER BY (field, source)). Read by the backoffice for display and validation. Never edited in ClickHouse.

## 4. Precedence

In Python, `dagster_v3.defs.se_company.basic_info.precedence`:

```python
BASIC_INFO_PRECEDENCE: dict[str, dict[str, int]] = {
    "legal_name":         {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200},
    "legal_form_code":    {"reviewer": 10000, "scb": 1000, "bolagsverket": 900},
    "status":             {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300},
    "incorporation_date": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "wikidata": 200},
    "lei":                {"reviewer": 10000, "esef": 1000},
    "wikidata_id":        {"reviewer": 10000, "wikidata": 1000},
    "description":        {"reviewer": 10000, "llm": 2000, "esef": 800, "wikidata": 600, "bolagsverket": 400, "ratsit": 300},
    "description_sv":     {"reviewer": 10000, "llm": 2000, "bolagsverket": 400, "ratsit": 300},
}
```

The numbers are the owner's to adjust in review; gaps leave room for new sources. A source absent from a field's map cannot supply that field. `description_language` is not in the map: it follows the winning `description` row.

Amended 2026-09-04 (slice 2): the register text comes from Bolagsverket's `activity_description`, so the `description` and `description_sv` maps name `bolagsverket` where they named `scb`.

## 5. The fold

`fold_basic_info(company_id, suggestions) -> BasicInfoRow | None`, a pure function:

1. Per field: among the suggestion rows whose value is not NULL and whose source has a precedence for the field, the highest precedence wins; ties go to the newest `observed_at`, then the smaller `source_record_uid`.
2. `description_language` is copied from the row that won `description`.
3. If no SCB or Bolagsverket row supplies `legal_name`, return None.
4. The row carries `fold_version` (a module constant, bumped when the logic changes) and the run id.

Batch layer, `fold_companies(client, company_ids, *, changed_only)`:

1. Read the current suggestion rows for the set (`argMax` per company and source over `suggested_at`), grouped by company.
2. With `changed_only`, keep the companies whose newest `suggested_at` is later than their main row's `folded_at`, or that have no main row.
3. Fold in memory, read the current main rows for the set, compare values and sources.
4. Insert a new main row for every folded company (so `folded_at` advances and the changed-only selection converges); append one history row per company whose values or sources changed (owner decision 2026-09-04).

Pages of 20,000 companies keep memory bounded; the 3.5M backfill is 64 partition runs of a few minutes each.

## 6. Dagster

Group `se_company_basic_info`, everything manual for now.

- Slice 0, in the `sweden_company` module: `sweden_company_scb_companies_clickhouse` and `sweden_company_bolagsverket_companies_clickhouse` export the two source tables from the normalised DuckDB layer on the register job's existing cadence; the `sweden_company_profile_history_clickhouse` asset stops writing the registry observation tables (it keeps writing proceedings until that entity's turn).
- `se_basic_info_suggestions_<source>` for scb, bolagsverket, wikidata, esef, ratsit, llm: plan 2's extractors reshaped to write one wide suggestion row per company. Per-company watermark scan, staged publish with a hash anti-join, config `execute` (preview by default), `company_ids`, `max_companies`, `since`. The LLM extractor keeps its observation cache, its preview counts (`would_reuse_count`, `would_call_model_count`) and its required model profile.
- `se_company_basic_info_fold`: 64 static partitions on `cityHash64(company_id) % 64`, `BackfillPolicy.multi_run(max_partitions_per_run=1)`, one pool; config `changed_only` default true.
- `se_company_basic_info_fold_companies`: unpartitioned, config `company_ids` required; the targeted fold, later the sensor's job.
- `se_company_basic_info_precedence_clickhouse`: exports the precedence table.
- Job `se_company_basic_info_extract_job` (the six extractors, LLM with run config) with schedule `se_company_basic_info_weekly` defined STOPPED. No sensor.

Deleted at cutover: `info.py`'s publisher with its jobs, weekly and field-value sensor (the file keeps only the LLM helpers the extractor imports) and the three artifact assets. The field-registry code and its migrations were already reverted before slice 0. The `sweden_company` asset that builds `se_companies` is deleted in the spine slice.

## 7. Backoffice

Admin info page reads: the main row with its sources, every current suggestion row (reviewer first), the history as a timeline, and the precedence table for "why this value".

Reviewer actions: Use this, Edit, Release. Each reads the current reviewer row, changes one field, inserts a new version with `decided_by = 'backoffice'` and `note`. Release sets the field to NULL. The validator takes fields and sources from the exported precedence table (plus `reviewer`).

After a decision the page shows the reviewer row and a "fold pending" marker when the reviewer row is newer than `folded_at`. A Fold now button launches `se_company_basic_info_fold_companies` for the company through the existing Dagster launch helper; the page reloads when the run finishes.

Pipeline sheet: extract job, fold partitions, suggestions per source, pending folds, LLM preview counts. Every reader of `se_companies` moves to `se_company_basic_info` in the spine slice.

## 8. Cutover

1. Apply the new migrations (four tables). The field-registry migrations never existed on this branch (reverted before slice 0); slice 0 took 000373 to 000375, so the entity tables start at 000376.
2. Run the six extractors by hand, one at a time, LLM last with a preview first.
3. Fold all 64 partitions from the UI backfill.
4. Migrate the existing reviewer decisions from `se_company_info_field_value` into reviewer suggestion rows (a one-off asset), then fold those companies.
5. Parity: an asset check on the fold compares `se_company_basic_info` with the untouched `se_company_info` per company: legal facts equal; a copied description equal; an LLM description equal to the stored observation; a decided row equal to its old text; companies published before and not now counted. Go or no-go.
6. Switch the serving view and the admin info page to the new tables, deploy, smoke one company through Use this, Fold now, Release.
7. Retire: delete the old publisher (`info.py`'s asset, scan, jobs, weekly and sensor, keeping the LLM helpers the extractor imports) and the three `se_company_info_*` artifact assets from Dagster; drop `se_company_info`, the artifacts and `se_company_info_field_value` by gated migrations once nothing reads them.

Rollback before step 6 is doing nothing. After step 6 it is switching the view and page back; the old table is untouched until step 7.

## 9. Testing

- The fold is table-driven pure-function tests: one case per field for precedence, ties, NULL-as-no-opinion, the publish rule, `description_language`, `fold_version`.
- The batch layer against the scripted fake client: changed-only selection, the rewrite of every folded main row, change-only history rows and `changed_fields`.
- One clickhouse-local harness (both `join_use_nulls`) runs extract, fold and history end to end with a hand-written expected main row.
- Extractor SQL pinned as text and executed in the harness; the LLM extractor's preview counts pinned.
- Backoffice: the page, the three actions, the validator and Fold now under vitest with one shared fixture; a live test under `VITEST_LIVE=1` after cutover step 1.
- The parity check executed once on prod at cutover step 5.

## 10. Slices

One plan each, executed in order with subagent-driven development:

0. Source tables: `se_scb_companies` and `se_bolagsverket_companies` with their export assets, the domain-suggestions dbt model moved to their union, the registry observation tables retired (gated drop migrations after the new tables are filled on prod).
   Done 2026-09-03: migrations 000373/000374 applied, both assets materialized on prod
   (1,818,909 SCB and 2,855,218 Bolagsverket companies, equal to the retired projection),
   the domain-suggestions model rebuilt on their union, and the registry pair dropped by
   000375. Slice 1's `se_basic_info_suggestions_scb` reads
   `corpscout.se_scb_companies` and `se_basic_info_suggestions_bolagsverket` reads
   `corpscout.se_bolagsverket_companies`, both one row per company, both
   `ReplacingMergeTree(observed_at) ORDER BY company_id` — read them with `FINAL` or
   `argMax(..., observed_at)`, and only rows `WHERE has_company = 1`: the publisher
   appends a `has_company = 0` tombstone when a source stops delivering a company and
   inserts the company again when it returns. A date Date32 cannot hold (before 1900)
   is NULL in the typed column and verbatim in its `*_raw` twin (`registration_date_raw`,
   `deregistration_date_raw`; 631 Bolagsverket companies, oldest 1826-01-01). Neither
   carries a `source_record_uid`: the extractor computes the suggestion's
   `source_record_uid` itself, from `source_record_id` and `source_payload_hash`, the way
   migration 000257's DEFAULT expression did
   (`lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', <'sweden_scb'|'sweden_bolagsverket'>, '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)))))`).
   The extractors do the interpreting: SCB's `source_status_code` (`FtgStat`) becomes
   `status`, Bolagsverket's `deregistration_date` implies its own, and
   `registration_date` becomes `incorporation_date`. Neither source table has, or will
   get, a `derived_status`. Parked for slice 1: a partial non-empty delivery would
   tombstone every missing company (only the empty-stage refusal guards the publisher);
   add a `max_removed_fraction` config to the export assets before the fold depends on
   `has_company`.
1. Tables, precedence, fold function, the two fold assets, the precedence export.
   Built 2026-09-03 (plan `2026-09-03-se-basic-info-1-tables-fold.md`): migrations
   000376-000379, package `dagster_v3.defs.se_company.basic_info` (`tables`, `precedence`,
   `fold`, `batch`, `assets`). Slice 2's extractors insert through
   `tables.SUGGESTION_INSERT_COLUMNS` and write a row only when the source's current
   record has a newer `observed_at` than the current suggestion row (`FINAL`) or none
   exists; `observed_at` must be the source's own, monotonic per record, since ties break
   on it. The fold assets exist but the suggestion table is empty until slice 2 runs.
   Resolved 2026-09-04: every folded company is rewritten, so `changed_only` converges
   without a watermark table. Memory per page is bounded only by `page_size` (default
   20,000 companies, up to seven suggestion rows each, descriptions included); lower
   `page_size` in the asset config if a run presses the host.
2. The six extractors, reading the source layer of section 3.1.
   Built 2026-09-04: `basic_info/extract.py` (change scan on the source `observed_at`,
   keyset paging, `INSERT ... SELECT`), the five SQL extractors and the LLM extractor (new
   prompt version `se-company-basic-info-description-v1`, observation cache reused from the
   first run on), job `se_company_basic_info_extract_job`, schedule
   `se_company_basic_info_weekly` STOPPED. Slice 3 reads suggestion rows through `FINAL`;
   the reviewer row is written by the backoffice with `source = 'reviewer'`, `observed_at`
   = the decision instant, `source_record_uid = ''`. The LLM gate compares the newest
   non-llm, non-reviewer text against the llm row's `suggested_at`, so a reused answer
   clears the scope; reviewer rows never trigger it.
   Amended 2026-09-04 (review fixes): the scope query is unpaged and `scope_pages` runs it
   once into a scratch table (`corpscout._tmp_basic_info_scope_<uuid>`) that the scan
   keyset-pages and drops, so each register and the suggestion table are read once per run
   instead of once per page. Bolagsverket's `observed_at` is the later of the register
   row's stamp and its translation's (`text_translations.version`), so a company whose
   Swedish text is translated after its last extraction is visited again instead of keeping
   the Swedish text on the English-facing `description`.
   Run on prod 2026-09-04: the five SQL extractors executed and converged (second preview
   0): suggestion rows bolagsverket 2,855,218, scb 1,818,909, ratsit 83,696, wikidata
   3,104, esef 393 (Bolagsverket text translated to English for 92.1% of 2,855,016 rows).
   The 64-bucket fold published 3,523,558 companies (1 unpublished: no register legal
   name), history 3,523,558 first-publish rows; a power cut during the backfill cost one
   bucket its merged parts, repaired by deleting the 20,000 orphan history rows and
   re-folding bucket_16 with `changed_only = false`. The LLM extractor's preview reported
   78,579 eligible companies (all paid calls under the new prompt version), the spend
   decision is the owner's and was still open when this was written. The legal-form
   follow-up (section 3.1 amendment) re-extracts Bolagsverket after this record.
3. The backoffice page, actions, Fold now, pipeline sheet.
4. Cutover (owner-gated prod steps) and retirement of the old publisher, the field-registry code and the three `se_company_info_*` artifacts.
5. The spine switch: every `se_companies` reader to `se_company_basic_info`, then the `se_companies` builder and table go.
6. The sensor, as its own later spec.

## 11. Names

Source tables `se_scb_companies`, `se_bolagsverket_companies` (assets `sweden_company_scb_companies_clickhouse`, `sweden_company_bolagsverket_companies_clickhouse`). Entity tables `se_company_basic_info_suggestion`, `se_company_basic_info`, `se_company_basic_info_history`, `se_company_basic_info_precedence`. Assets `se_basic_info_suggestions_<source>`, `se_company_basic_info_fold`, `se_company_basic_info_fold_companies`, `se_company_basic_info_precedence_clickhouse`. Job `se_company_basic_info_extract_job`, schedule `se_company_basic_info_weekly`. Module `dagster_v3.defs.se_company.basic_info`. Sources `scb`, `bolagsverket`, `wikidata`, `esef`, `ratsit`, `llm`, `reviewer`.
