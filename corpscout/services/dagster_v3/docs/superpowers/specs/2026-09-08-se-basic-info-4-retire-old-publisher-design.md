# SE basic info, slice 4: retire the old publisher and its tables

Date: 2026-09-08. Owner asked for the cleanup of the old info tables the same day; this is
basic-info slice 4 of the 2026-09-03 design ("Cutover and retirement of the old publisher,
the field-registry code and the three `se_company_info_*` artifacts"), plus the one piece
that design deferred to slice 5 but slice 4 cannot avoid: the serving view's spine.

## Goal

Drop `se_company_info`, `se_company_info_scb`, `se_company_info_esef`,
`se_company_info_wikidata` and `se_company_info_field_value` from prod and the code, after
every live reader has moved to `se_company_basic_info`. Keep
`se_company_info_enrichment_observation`.

## Facts the design rests on (prod and repo, 2026-09-08)

| Table | Rows | Disk | Last write | Verdict |
|---|---|---|---|---|
| `se_company_info` | 3,779,090 | 1.32 GiB | 2026-09-07 09:42 | drop |
| `se_company_info_scb` | 3,749,662 | 971 MiB | 2026-09-07 06:59 | drop |
| `se_company_info_esef` | 675 | 630 KiB | 2026-09-07 06:50 | drop |
| `se_company_info_wikidata` | 3,119 | 397 KiB | 2026-08-22 | drop |
| `se_company_info_field_value` | 2 | 4 KiB | 2026-09-02 | drop |
| `se_company_info_enrichment_observation` | 2,296 | 1.7 MiB | 2026-09-07 09:42 | **keep** |

- The observation table is the basic-info LLM extractor's paid-call cache:
  `basic_info/llm.py` imports `SE_COMPANY_INFO_OBSERVATION`, `OBSERVATION_COLUMNS` and
  `OBSERVATION_FLUSH_ROWS` from `se_company/info.py` and reads and writes the table through
  `common.py`'s observation helpers. It stays, under its name; a rename is a separate
  decision.
- The old publisher is still alive on prod: schedule `se_company_info_weekly` (Mondays
  06:50 UTC) and sensor `se_company_info_field_value_sensor` are RUNNING and rewrote the
  tables on 2026-09-07. Both must be stopped before the code that defines them goes.
- `se_companies_serving` (migration 000347, cadence changed by 000366 to
  `REFRESH EVERY 1 HOUR OFFSET 45 MINUTE`) has `se_company_info` as its spine and also joins
  `se_companies` and the spine-keyed `text_translations`. It backs the admin companies list,
  the geocoding list, and the company_markets and technology_catalog assets read it too.
- The backoffice company header (`se-company-shell.server.ts`, `SHELL_INFO_SQL`) reads
  `se_company_info` for name, legal form, status and incorporation date; the same column
  names exist on `se_company_basic_info` (both `status` LowCardinality(String),
  `incorporation_date` Nullable(Date32)). Its fallback `SHELL_REGISTER_SQL` reads
  `se_companies` and stays until slice 5.
- The old pipeline page `/admin/se/companies/pipeline` is routed but not in the sidebar. It
  reads the old tables and launches `se_company_info_job` and `se_company_info_review_job`.
- `se-company-info.server.ts` and `se-info-field-values.ts` have no importers left except
  each other and `chInsertSeCompanyInfoFieldValues` in `clickhouse.server.ts`.
  `se-company-info-payload.ts` stays: the ESEF tab pages and the description card import it.
- The public country detail page reads `se_companies` directly, not the old info table:
  slice 5, untouched here.
- Old `se_company/scb.py` holds two artifacts: the info one (`se_company_info_scb_clickhouse`,
  goes) and the address one (`se_company_address_scb_clickhouse`, `ADDRESS_TABLE`,
  `SE_COMPANY_ADDRESS_SCB_*`), which `address_legacy.py` and three address tests import.
  The address half stays.
- `se_company/info_rules.py`: `address_rules.py` and `address_legacy.py` import
  `ArtifactRow`, `_text` and `evidence_set_hash_for`. Those stay; `InfoOutcome`,
  `merge_company_info`, `FieldValueRow`, `_live_field_values`, `_apply_description_value`,
  `apply_field_values` and the `DESCRIPTION_PRIORITY`/`INFO_VALUE_*` constants go.
- `se_company/info.py`: only `basic_info/llm.py` imports from it, exactly
  `OBSERVATION_COLUMNS`, `OBSERVATION_FLUSH_ROWS`, `SE_COMPANY_INFO_OBSERVATION`,
  `LlmProfileConfig`, `build_llm_client`, `map_ordered`, `parse_description_suggestion`.
  The 2026-09-03 design says the file keeps only the LLM helpers the extractor imports.
- `common/clickhouse_checks.py` lists four freshness leaves for the old assets.
- The old publisher's status came from Bolagsverket's deregistration; the fold's now does
  too (precedence amended 2026-09-08). The full re-fold runs concurrently with this work
  and does not gate it: the serving view refreshes hourly from whatever the main table holds.
- Ledger policy (2026-09-03): an unused table is dropped by hand on the server and its DDL
  leaves the migration file, which stays for history; `EMPTIED_MIGRATIONS` in
  `tests/test_clickhouse_migrations.py` lists files with nothing left to declare.
  `tests/se_company_ddl.py` reads table DDL from the creating migration; the address tests
  use it for 000307's tables and keep working once 000297 holds only the observation table.

## Design

### 1. Stop the old publisher on prod

Before any deploy: `stopRunningSchedule` for `se_company_info_weekly` and `stopSensor` for
`se_company_info_field_value_sensor` over GraphQL (both take the instigator's `id`; the
selectors are `{repositoryLocationName: "dagster_v3", repositoryName: "__repository__",
scheduleName | sensorName}`). Owner-named prod action.

### 2. Serving view re-based (migration 000391)

`se_companies_serving` keeps its exact column list, cadence and staged swap; its inputs
change. Builder `sweden_company/companies_current.py:build_se_companies_serving_sql`:

- Spine: `corpscout.se_company_basic_info AS i FINAL`. `se_company_info`, `se_companies` and
  the `text_translations` join disappear from the view.
- Register columns from `corpscout.se_bolagsverket_companies AS b FINAL` (`has_company = 1`),
  LEFT-joined on `company_id`:
  `status_reason` = `ifNull(b.deregistration_reason, '')`,
  `bolagsverket_source_record_uid` = the extractor's record-uid expression over
  `b.source_record_id` and `b.source_payload_hash` (`''` without a register row; the SHA
  expression moves to one shared constant the extractor and the builder both use),
  `updated_from_raw_at` = `ifNull(b.observed_at, toDateTime64(0, 3, 'UTC'))`.
- Labels from `corpscout.se_code_labels` (`code_type = 'legal_form'`, `argMax` by version,
  both `label_en` and `label_sv`) joined on `ifNull(i.legal_form_code, '')`; the
  `status_reason` label join stays as it is, keyed on the register's reason.
- Descriptions from the main row: `activity_description` = `ifNull(i.description_sv, '')`,
  `activity_description_en` = `if(ifNull(i.description_language, '') = 'en',
  ifNull(i.description, ''), '')`, `has_description` = `i.description IS NOT NULL`,
  `desc_esef` = `i.description_source = 'esef'`, `desc_wikidata` =
  `i.description_source = 'wikidata'`, `has_lei` = `i.lei IS NOT NULL`, `has_wikidata` =
  `i.wikidata_id IS NOT NULL`. `legal_name`, `status`, `legal_form_code` read the main row.
- Every presence and market arm, the address CTEs, the ORDER BY and the SETTINGS are
  unchanged.

Migration `000391_corpscout_se_companies_serving_basic_info` is the 000347 recipe verbatim,
five statements: `CREATE DATABASE`, `SYSTEM STOP VIEW`, `CREATE MATERIALIZED VIEW
corpscout.se_companies_serving_next REFRESH EVERY 1 HOUR OFFSET 45 MINUTE ENGINE = MergeTree
ORDER BY company_id AS <builder render>`, `SYSTEM WAIT VIEW ..._next`, `RENAME TABLE`
serving to `_retired` and `_next` to serving. Down: rename back, `SYSTEM START VIEW`, drop
the discarded render. `tests/test_se_companies_serving_mv.py` pins 000391 (cadence
assertion becomes the hourly one; the "no INNER JOIN se_company_info" pin becomes "no
`se_company_info`, no `se_companies`, no `text_translations` in the body");
`tests/test_se_companies_serving_sql.py` seeds `se_company_basic_info` (000377) and
`se_bolagsverket_companies` (000374) instead of the old spine pair and keeps every
behaviour assertion.

### 3. Backoffice

- `se-company-shell.server.ts`: `SHELL_INFO_SQL` reads `corpscout.se_company_basic_info`
  (same columns). Nothing else changes in the shell.
- Delete: `routes/admin-se-companies-pipeline.ts` and its test, `components/admin/
  se-company-info-pipeline.tsx`, `lib/se-company-info-pipeline.server.ts` and its test,
  `lib/se-company-info.server.ts`, `lib/se-info-field-values.ts`,
  `chInsertSeCompanyInfoFieldValues` in `clickhouse.server.ts`, the `SE_COMPANY_INFO_*`
  constants in `dagster.server.ts`, the `se/companies/pipeline` entry in `routes.ts`.
- Comments naming `se_company_info` in `se-company-info-filters.ts` and
  `se-company-geocoding-list.server.ts` name the main table instead.
- `npm run typecheck` and `npx vitest run` clean. Deploy is the merge to main; the owner runs
  the backoffice locally.

### 4. Dagster

- `se_company/info.py` keeps: `LlmProfileConfig`, `DEFAULT_LLM_PROFILE`,
  `llm_api_key_variable`, `build_llm_client`, `DescriptionSuggestion`,
  `parse_description_suggestion`, `map_ordered`, `OBSERVATION_COLUMNS`,
  `OBSERVATION_FLUSH_ROWS`, `SE_COMPANY_INFO_OBSERVATION`, and the token-average helpers
  if the extractor's preview uses them (it does not today; they go). Everything else goes:
  the change scan, artifact reads, field values, the page resolver, the publisher,
  `se_company_info_clickhouse`, both jobs, the sensor, the schedule, the module `defs`.
- Delete `se_company/esef.py` and `se_company/wikidata.py` (old artifacts); delete the info
  half of `se_company/scb.py` (constants, `SE_COMPANY_INFO_SCB_*`, the asset) and keep the
  address half.
- Trim `se_company/info_rules.py` to `ArtifactRow`, `evidence_set_hash_for`, `_text`,
  `_date`, `_newest`.
- `common/clickhouse_checks.py`: remove the four `se_company_info*` leaves and their comment.
- Tests: delete `test_se_company_info.py`, `test_se_company_info_clickhouse_local.py`,
  `test_se_company_esef.py`, `test_se_company_wikidata.py`, `test_se_company_scb.py`; move
  the kept-helper tests (`parse_description_suggestion` shape and truncation, the API key by
  provider, `map_ordered` concurrency) to a new `tests/test_se_company_llm_support.py`;
  trim `test_se_company_info_rules.py` to the `evidence_set_hash_for` test; adjust the
  freshness-check and definitions tests that enumerate the old leaves or jobs.
- `uv run dg check defs` and the unit suite green.

### 5. Ledger edits (code only, nothing runs on prod)

Per the policy, the DDL of the dropped tables leaves the files: 000297 keeps
`CREATE DATABASE` and the observation table only; 000299, 000300, 000301, 000304, 000306,
000365 and 000371 keep `CREATE DATABASE` only and join `EMPTIED_MIGRATIONS`. 000298 (grants
on the observation table) and 000372 (drops the correction ledger) are untouched. The three
migration-text tests on 000365, 000371 and 000372 are deleted or reduced to what the files
still say.

### 6. Drop (owner-run)

`corpscout/clickhouse/operations/se_company_info_retire.md`: a gated script. Preconditions
checked inline before the drops: the two instigators are STOPPED, `se_companies_serving`'s
definition no longer names `se_company_info`, the deployed code location has no
`se_company_info_clickhouse` asset, and the row counts of the five tables are recorded.
Then `DROP TABLE` for the five, never the observation table.

## Rollout order

1. Stop the schedule and the sensor (section 1).
2. Merge the branch; deploy dagster_v3 (light_sync, defs-state refresh first if any dbt
   commit landed since the last refresh); backoffice is live on merge.
3. Apply 000391 on prod (one full view rebuild, about three minutes under the bounded
   settings); verify the view's row count equals `se_company_basic_info`'s, the companies
   list, the geocoding list and a company header.
4. Run the drop script (section 6).

## Out of scope

- Renaming the observation table.
- The public country detail page, `SHELL_REGISTER_SQL`, the company_serving dbt models and
  the domain-suggestions model: every remaining `se_companies` reader is slice 5.
- The `economically_active` field from SCB's `Företagsstatus` (agreed follow-up).
- Any change to the fold, precedence or the Info tab.
