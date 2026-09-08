# SE basic info, slice 5: retire the `se_companies` spine

Date: 2026-09-08, after slice 4 landed on prod. Basic-info slice 5 of the 2026-09-03 design:
"every `se_companies` reader to `se_company_basic_info`, then the `se_companies` builder and
table go." Also the spine-keyed translation rows (kept in the 2026-09-07 translated-views
design until this slice).

## Facts (prod and repo, 2026-09-08)

- `se_companies` and `se_company_basic_info` cover exactly the same 3,523,558 companies;
  `registration_number` equals `company_id` on every spine row.
- The spine is the last table of the old `sweden_company` projection. Its readers:

| Reader | Spine columns used | Successor |
|---|---|---|
| `sweden_uhm_procurement/clickhouse.py` (LEFT ANY JOIN on `company_id`) | `company_id` | `se_company_basic_info` |
| `sweden_platsbanken/clickhouse.py` (`SELECT company_id ... FINAL WHERE length = 10`) | `company_id` | `se_company_basic_info` |
| `company_serving/publish.py` (anchor and orphan checks) | `company_id`, count | `se_company_basic_info` |
| `company_signals/rules.py` (`companies_table`) | `company_id` | `se_company_basic_info` |
| `company_identifier/rules.py` (`register_table`) and `assets.py` upstream keys | `company_id` | `se_company_basic_info`, upstream `se_company_basic_info_fold` |
| `company_domain_suggestions/inputs.py`, `dbt_run.py`, `assets.py` | `company_id`, `legal_name` | `se_company_basic_info` |
| dbt `stg_se_company_match_features` (live view) | `company_id`, `legal_name` | `se_company_basic_info` |
| dbt `company_serving` anchor CTEs (7 models) and `company_serving_tests` macro | `company_id` | `se_company_basic_info` |
| dbt `company_section_item_source_links_build` | both source record uids, payload hashes, record ids, `updated_from_raw_at`, `source_run_id`, `registration_number` | `se_bolagsverket_companies` and `se_scb_companies` FINAL (`has_company = 1`), uid by a dbt macro |
| `sweden_ratsit/assets.py` (`RATSIT_ACTIVE_COMPANIES_TABLE`, `status = 'active'`) | `company_id`, `status` | `se_company_basic_info` |
| backoffice `address-companies`, `address-quality` | `registration_number`, `legal_name`, `status` | `se_company_basic_info` (`company_id`) |
| backoffice `company-domains`, `technologies`, `se-ratsit-results` | `legal_name` | `se_company_basic_info` |
| backoffice `se-company-shell` fallback (`SHELL_REGISTER_SQL`) | name, form, status, date | `se_company_basic_info` covers every company, so the fallback reads the same table; the `published` flag stays true for every row that exists |
| backoffice `countries.ts` SE profile | `companiesTable`, `legal_name`, `status = 'active'`, search expr | `se_company_basic_info` |
| backoffice `countries.ts` SE `companyShellQuery` (`c.*` of the spine) | the whole row | a projection over `se_company_basic_info` and `se_bolagsverket_companies` that keeps the same output names |

- Dagster assets that declare the spine builder as a dependency: uhm procurement,
  ratsit scan dispatch, platsbanken, `COMPANY_IDENTIFIER_UPSTREAM_ASSET_KEYS`, the wikidata
  registry seed spec in `sweden_company/tables.py`, the freshness leaf, the
  `identities_normalized` check, the `sweden_company_refresh_job` selection.
- The public detail page renders the shell row generically. `fields.tsx` sorts
  `status_source`, `status_observed_at`, `status_conflict`, `updated_from_raw_at` into its
  lineage bucket; `registration_number`, `status_reason`, `dissolution_date`,
  `legal_name_registration_date`, `activity_description_original`, `registration_date` are
  content fields today.
- Register tables carry: `se_bolagsverket_companies.deregistration_date`,
  `deregistration_reason`, `registration_date`, `legal_name_raw`, `source_record_id`,
  `source_payload_hash`, `observed_at`; `se_scb_companies.source_record_id`,
  `source_payload_hash`, `observed_at`, `registration_date`.
- Ledger: `se_companies` is created by 000084 (with `se_company_addresses` and
  `se_industries`, which stay), altered by 000244 (one ALTER among many other tables) and
  000281 (spine only). `text_translations` holds 2,176,663 rows under the spine key.
- The noise in the prod query log (`DESCRIBE se_company_ratsit_crawl_results` every five
  minutes) comes from the owner's Linux workstation, not from prod; out of scope.

## Design

### 1. One successor per column class

- Anchor, name, status, form, date: `corpscout.se_company_basic_info FINAL`.
  `legal_name` is a non-null String there, so `ifNull(legal_name, '')` becomes `legal_name`;
  `registration_number` becomes `company_id`; `status = 'active'` is unchanged.
- Register record identity and stamps: the register tables. A dbt macro
  `se_register_record_uid(source_slug, alias)` renders the extractor's
  company-source-record hash (`common.bolagsverket_record_uid_sql` generalised to
  `register_record_uid_sql(source_slug, alias)` in Python; the two are pinned equal by a
  test that renders both for `sweden_bolagsverket`).
- Public SE shell (`countries.ts`): `SELECT` from `se_company_basic_info AS i FINAL` LEFT JOIN
  `se_bolagsverket_companies AS b FINAL (has_company = 1)` projecting, under the old names,
  `company_id`, `registration_number` (= `company_id`), `legal_name`, `legal_form_code`,
  `status`, `status_source`, `registration_date` (= `i.incorporation_date`),
  `dissolution_date` (= `b.deregistration_date`), `status_reason`
  (= `b.deregistration_reason`), `activity_description_original` (= `i.description_sv`),
  `legal_name_raw` (= `b.legal_name_raw`), `source_run_id`, `updated_from_raw_at`
  (= `i.folded_at`), and the six `__shell_*` fields. Dropped: `legal_name_registration_date`,
  `status_observed_at`, `status_conflict`, the four source record id/hash columns and the
  two source record uids: lineage rows the page files under its lineage bucket or never
  showed as content.

### 2. Dagster

- Dependencies on `sweden_company_companies_clickhouse` move to
  `se_company_basic_info_fold` (an unpartitioned asset may depend on a partitioned one;
  these deps are lineage, nothing auto-materialises on them).
- `identities_normalized` re-targets `sweden_company_scb_companies_clickhouse` and
  counts on `se_scb_companies`, where the ids it guards originate.
- Deleted: `sweden_company_companies_clickhouse`, `export_sweden_company_clickhouse_companies`,
  `COMPANIES_TABLE_CH`/`QUALIFIED_COMPANIES_TABLE`, the job selection entry, the freshness
  leaf. The DuckDB `companies_current` table the exporter read stays in the normalised
  layer untouched (nothing else needs to change for it, and the DuckDB layer is rebuilt
  weekly anyway).
- `company_signals/rules.py` and `company_identifier/rules.py` name the main table;
  `company_serving/publish.py` reconciles against it; `sweden_ratsit` selects active
  companies from it.
- `sweden_uhm_procurement_job` selects the awards export's upstream chain minus the fold's
  own chain (`AssetSelection.assets(fold).upstream()`), so the monthly job never plans the
  partitioned fold or its extractors; the contract test that no schedule reaches the fold
  assets stays green.

### 3. dbt

- `stg_se_company_match_features`: companies CTE from the main table.
- `company_serving`: every anchor CTE and the tests macro from the main table;
  `company_section_item_source_links_build` builds its register source records from the two
  register tables with the macro, and joins ESEF registry ids on `company_id`.
- The dbt sources declare `se_company_basic_info`, `se_bolagsverket_companies`,
  `se_scb_companies`; `se_companies` leaves the source lists.

### 4. Backoffice

- Every query in the reader table above re-pointed; `countries.ts` SE shell rewritten per
  section 1; `SHELL_REGISTER_SQL` reads the main table too (kept as the fallback shape so
  the header's `published` logic is untouched); the countries and tabs tests re-pinned.
- `se_company_basic_info` is a ReplacingMergeTree the fold rewrites in place, so a bare
  read sees every unmerged version (9,997,901 rows for 3,523,558 companies on prod). The
  SE profile sets `companiesTableFinal: true` and every generic reader of `companiesTable`
  (list, count, stats, facets, markets, procurements, financial aggregates, overview,
  facts) goes through `companiesFrom(country)`, which appends `FINAL` for that flag.
- The SE list's id column is `company_id` (the spine's `registration_number` is gone).

### 5. Ledger and drops

- Per the policy, 000084 loses the `se_companies` CREATE (its two other tables stay),
  000244 loses its `ALTER TABLE corpscout.se_companies` statement, 000281 is emptied and
  listed in `EMPTIED_MIGRATIONS`. Tests that read those files adjust.
- Owner-run, after the deploy and the dbt rebuilds have run green:
  `DROP TABLE corpscout.se_companies` and
  `ALTER TABLE corpscout.text_translations DELETE WHERE source_table = 'corpscout.se_companies'`
  (2,176,663 rows, lightweight delete), gated on: no view or MV names `se_companies`, the
  deployed code location has no `sweden_company_companies_clickhouse` asset, and the
  translation scan's field names the register table.

## Rollout order

1. Merge; deploy dagster_v3 (defs-state refresh: the dbt projects change, so the two
   `dbt parse` runs and `dg utils refresh-defs-state` are mandatory before light_sync).
2. Materialise the dbt-backed assets that rebuild from the new sources: the
   company_domain_suggestions staging (recreates the `stg_se_company_match_features` view)
   and the company_serving publish job (rebuilds the seven models and its anchor checks
   against the main table).
3. Backoffice is live on merge; smoke the public SE company page, the address list, the
   domains review queue and the technologies page.
4. Run the drop script.

## Out of scope

- The `economically_active` field (next).
- Renaming the observation cache.
- Any change to what the basic-info fold publishes.
