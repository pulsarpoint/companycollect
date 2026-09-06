# Sweden data sources — overview and state of processing

Last updated: 2026-08-19. This is the map of everything we ingest for Sweden:
what we download, how each pipeline processes it, what is live in ClickHouse
today, the known gaps, and what should be done next. Deep detail lives in each
module's own design doc — this is the map, not the spec:

- `src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`
- `src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md`
- plans: `docs/superpowers/plans/2026-07-19-{company-people,sweden-audits,sweden-incremental-exports,sweden-text-translations}.md`

## Source summary

| Module | Publisher / dataset | Acquisition | Cadence | ClickHouse tables (rows @ 2026-07-20) | Auth / license |
|---|---|---|---|---|---|
| `sweden_company` | Bolagsverket high-value datasets: SCB/FDB company bulk + Bolagsverket legal-register bulk (two full ZIP snapshots) | automated full snapshot | source refreshes ~weekly; schedule Mon 06:15 (STOPPED by default, enable in UI) | `se_companies` 3.41M, `se_company_addresses` 4.40M, `se_industries` 2.45M | none / open data |
| `sweden_financial` | Bolagsverket årsredovisningar bulk ZIPs (inline-XBRL annual reports, archive years 2020–2026) | automated; backfill by year partition + weekly current refresh | weekly Sat 06:45 (RUNNING) | `se_financial_reports`, `se_financial_facts`, unified reported/comparative `se_bolagsverket_financial_metrics`, `se_financial_report_signatories`, `se_company_audits` | none / open data |
| `esef_filings` | filings.xbrl.org ESEF reports for Swedish LEI issuers | automated cross-country ESEF flow | source-owned schedule | `esef_filings`, `esef_facts`, `esef_financial_metrics` | public filings |
| cross-source consumers | — | derived from the tables above | daily / with financial refresh | `companies_all` (SE included), `company_people_all` 5.26M SE rows, `se_company_financials_latest` 570k | — |

Entity key everywhere: the 10-digit **organisationsnummer** (`company_id` /
`registration_number`).

Both sources live on the same host,
`https://vardefulla-datamangder.bolagsverket.se/` — Bolagsverket's public
S3-style open-data listing. No API keys, no rate-limit contract.

## 1. sweden_company — company register (the spine)

Two full ZIP snapshots, refreshed upstream roughly weekly:

| dataset | file | format |
|---|---|---|
| SCB/FDB company bulk | `scb/scb_bulkfil.zip` | tab-separated text |
| Bolagsverket legal register | `bolagsverket/bolagsverket_bulkfil.zip` | semicolon-separated text |

Chain: `sweden_company_raw_snapshot_s3` (HEAD-based skip on unchanged
`Last-Modified`, ZIPs into `source-sweden-company` bucket, run-scoped +
date-latest manifests) → `sweden_company_raw_duckdb` (raw tables with exact
source columns as varchar + provenance: `source_run_id`, line number,
`source_payload_hash`, `raw_record`) → `sweden_company_normalized_duckdb`
(companies / source-specific registry states / typed proceedings / addresses /
complete SNI states / industry codes; Bolagsverket preferred over SCB for legal
identity) → five ClickHouse publish assets (migration-owned tables, atomic
current-snapshot swaps, and change-aware historical observations).

DuckDB file: `data/sweden_company_source.duckdb`, schema `sweden_company`.

What it provides:

- **`se_companies`** — one row per organisation: `legal_name` (normalized),
  `legal_name_raw` (the packed source string — see caveats), legal form code,
  status + status reason, incorporation/dissolution dates,
  `activity_description` (Swedish free text from SCB).
- **`se_company_addresses`** — append-only parsed Bolagsverket and SCB address observations; current rows are served from the atomically refreshed `se_company_addresses_current` snapshot
  fallback/enrichment addresses.
- **`se_industries`** — SCB `Ng1`..`Ng5` 5-digit SNI codes with derived 4-digit
  `nace_rev2_class_code` (the 5th digit is Sweden-specific detail, so we do not
  call the 5-digit value NACE).
- **`se_scb_companies`** — one row per company with the whole SCB register
  record in SCB's own organisation: raw `FtgStat` / `JEStat`, legal form, both
  name fields, registration date, the five SNI codes, the address columns and
  the marketing block. No derived status: interpreting SCB's codes belongs to
  the basic-info suggestion extractors.
- **`se_bolagsverket_companies`** — one row per company with the whole
  Bolagsverket register record: identity, name-protection sequence,
  registration country, both name forms, legal form, registration and
  deregistration dates with the deregistration reason, the pending-proceedings
  field, activity description and the packed postal address.
- **`se_company_proceeding_observations`** — typed Bolagsverket liquidation,
  bankruptcy, and restructuring procedures with raw values retained;
  `se_company_proceedings_current` contains the currently reported set.
- **`se_company_industry_observations`** — append-only full `Ng1`..`Ng5` SNI
  state, with `se_company_industry_current` as the current projection.

The history publishers compare stable logical fingerprints. Unchanged weekly
snapshots append nothing; changes append observations; and removed records,
procedures, or classifications append tombstones before the physical current
snapshot is swapped. Source ZIP archives remain the lossless recovery layer for
snapshots captured before these typed history tables were introduced.

Known gaps in this module:

- **No canonical registry contacts/domains.** The source carries only
  unstructured contact candidates. The separate `company_domain_suggestions`
  pipeline now builds reviewable Sweden domain candidates from Common Crawl
  identifiers, organization/domain names, and distinctive officers, with
  country/industry/web-presence boosts. Those suggestions are intentionally not
  canonical website/email/phone facts until a later review boundary accepts one.
- **`legal_name_raw` is a packed multi-name string** (name records concatenated
  with type markers). The normalized `legal_name` is what the UI shows; human-
  readable secondary names are extracted and shown in the detail page's
  secondary-names section. The raw value is deliberately kept out of the main
  record view.
- **`activity_description` is Swedish-only** — see the translations section
  under "What should be done next".
- The weekly refresh schedule ships `STOPPED` by default; the live state is
  managed in the Dagster UI.

## 2. sweden_financial — annual reports (inline XBRL)

Bolagsverket publishes every filed annual report as bulk ZIP archives under
`arsredovisningar/<year>/…`. One outer ZIP contains many nested ZIPs; each
nested ZIP holds one company's report as inline-XBRL XHTML. Archive years run
2020–2026 (report periods inside reach back to 2017 — a filing lands in the
archive year it was *published*, not the fiscal year it covers).

Chain (year-partitioned backfill + non-partitioned weekly current refresh):

1. **Raw archives → S3** (`sweden_financial_backfill_raw_archives_s3` /
   `…_current_raw_archives_s3`): outer ZIPs stored under deterministic keys in
   `source-sweden-financial`, keyed by year, archive name, and upstream
   `LastModified`; unchanged archives are reused, and each run writes an
   archive sync manifest.
2. **XHTML catalog → DuckDB** (`…_report_xhtml_catalog_duckdb`): every nested
   report XHTML extracted into deterministic S3 keys
   (`sweden_financial/report_xhtml/year=…/company_id=…/…/report.xhtml`) and
   cataloged in the per-year DuckDB file
   `data/sweden_financial/sweden_financial_source_<year>.duckdb`. The current
   variant reprocesses only archives the sync manifest marked as downloaded.
3. **Parse → DuckDB** (`…_parsed_reports_duckdb`): each XHTML parsed into
   `sweden_financial.reports` (one row per report), `sweden_financial.facts`
   (one row per inline-XBRL fact — lossless: numeric, date, text, context,
   unit, currency, dimensions), and `sweden_financial.parse_errors` (a bad
   document never blocks its partition).
4. **Export → ClickHouse** — **scoped incremental upserts, never full-table
   replaces** (architecture decision after the 2026-07-19 incident, when a
   host holding only one year's DuckDB file full-replaced the seven-year
   facts table). The backfill pair
   (`sweden_financial_backfill_reports_clickhouse` + facts twin) is
   year-partitioned and upserts its year file's full archive scope. The
   current pair (`…_current_reports_clickhouse` + facts twin) is
   non-partitioned and **reconciling** (2026-07-20 design): it diffs the
   local active-year file against ClickHouse per `source_archive_key` (row
   counts; facts via the `statement_key` join) and upserts exactly the
   missing/mismatched archives — an empty diff is a clean no-op, so weekly
   and yearly materializations are order-independent by construction. Both
   delete exactly their own scope — reports by `source_archive_key` array
   param, facts by `statement_key` staged through a per-run Memory table so
   hundreds of thousands of keys travel as data blocks, never query text —
   with `mutations_sync = 1` (skipped when the pre-count is 0, the
   steady-state new-archive case), then insert. A run structurally cannot
   touch rows outside its own scope.
5. **Derived wave** (full rebuilds from ClickHouse facts, stage + exchange with
   shrink guard):
   - **`se_bolagsverket_financial_observations`** — source-owned observations,
     one row per mapped fact and represented fiscal year. Filing/context/concept
     identity, original value, dimensions, precision and FX provenance are
     preserved; reported and comparative assertions coexist. Conflicts are
     quality flags, not filters, and no canonical or cross-source value is
     selected here.
   - **`se_bolagsverket_financial_metrics`** — one resolved row per filing and
     represented fiscal year. Reported years carry the full standard metric set;
     comparative years backfill revenue and total assets only. The latter are
     retained only when overlapping revenue agrees with a directly reported
     year, and `observation_kind` plus `source_fiscal_year` preserve their
     provenance.
   - **`se_financial_report_signatories`** — people reconstructed from the signature
     blocks (`UnderskriftHandling*` / `Arsredovisning*` /
     `Faststallelseintyg*` concept triples): 5.43M rows ≈ 3.10M board /
     2.03M certification signers / 0.30M auditors across ~570k companies.
     Deterministic argMax tiebreaks; names indexed with `ngrambf_v1` for
     search.
   - **`se_company_audits`** — audit opinions (`Revisionsberattelse*`
     concepts): ~304k standard, 743 modified (`AvvikerStandardutformning`, a
     distress signal), ~91k firm-name-only rows; Swedish prose opinion dates
     ("15 maj 2024") parsed to `Date32` with 100% coverage on the verified
     year.

Provenance is first-class: `se_financial_facts_with_source` joins every fact to
its filing and exposes both the official Bolagsverket outer-archive URL and the
exact extracted XHTML URI — this backs the "open source document" buttons in
the backoffice facts drill-down.

### Financial serving boundary

The Financials page does not combine standalone Bolagsverket values with
consolidated ESEF values. It reads two same-shape views independently:

- `se_financials_bolagsverket_current` selects one Bolagsverket filing row per
  company/year and retains filed versus comparative provenance.
- `se_financials_esef_current` resolves Swedish ESEF issuers through
  `company_identifier` and composes amendments per metric.

Both expose the canonical presentation aliases and source-record UIDs. The UI
shows only sources available for the company and lets the user switch the
selected source without creating a cross-source winner.

### Jobs and schedules

| job | contents | trigger |
|---|---|---|
| `sweden_financial_backfill_job` | raw + catalog + parse, year partitions | manual backfill |
| `sweden_financial_current_year_job` | full weekly chain: sync + catalog + parse + reconciling reports/facts exports (non-partitioned) | `sweden_financial_current_year_weekly`, Sat 06:45 Europe/Belgrade, RUNNING |
| `sweden_financial_backfill_clickhouse_job` | backfill reports+facts export pair | manual, after parse |
| `sweden_financial_context_period_backfill_job` | cached XHTML reparse + FX + scoped reports/facts exports, one archive-year partition per run | manual schema/data rollout |
| `sweden_financial_current_clickhouse_job` | reconciling current export pair (manual; safe any time -- stateless diff vs ClickHouse) | manual |
| `sweden_financial_clickhouse_job` | derived wave: source observations, Bolagsverket metrics, officers, audits | after exports |

Operational notes:

- A backfill `2026` export and the weekly current writer share the 2026 DuckDB
  file; running both concurrently fails loudly on the DuckDB cross-process
  lock — sequence them.
- The `archive_ingest_complete` check lives on the metrics asset (the derived
  wave), not on the incremental exports.
- `clickhouse_driver` %-escaping: on code paths that pass a params dict,
  literal `%` in SQL must be `%%`; the no-params paths are unescaped —
  documented per-module, keep it that way when editing `clickhouse.py`.

## 3. Cross-source consumers

- **`companies_all`** — SE rows are part of the unified cross-country search
  spine (name/status/industry), refreshed daily.
- **`company_people_all`** — country-tagged person layer built from
  `PEOPLE_SOURCES` (currently SE officers only): 5.26M SE rows, ~550k distinct
  normalized names. Daily schedule 07:45 UTC, RUNNING. This remains an
  observation/search table; names are not identities.
- **Country people identity** — `country_person`,
  `country_person_observation`, `country_person_identifier`, and
  `country_person_match` are all partitioned and keyed within `country_iso2`.
  The current SE resolver preserves 5.43M source observations and produces
  1.51M country-person profiles. Published identifiers are exact evidence;
  otherwise, same-company/name groups are explicitly provisional and
  ambiguous duplicates remain separate. Reviewed reassign, split, merge, and
  undo decisions are appended to the country-partitioned
  `country_person_correction` ledger. The resolver applies the latest decision
  after automatic matching and keeps merged IDs as redirect tombstones. A
  running sensor starts a refresh when the ledger advances; the daily 08:00
  UTC schedule remains the reconciliation fallback.
- **`se_company_financials_latest`** — latest-filing projection per company
  (570k rows) feeding the cross-country latest-financials layer.
- **Backoffice surfaces** (`corpscout/services/backoffice`): company detail at
  `/company/se/:id` (registry record, secondary names, management with people
  links + audit line with modified-opinion badge, industries, financials table
  combining metrics with comparative history), facts drill-down at
  `/company/se/:id/facts/:year` with per-fact source-document links (SigV4
  proxy to the object store), `/people` identity search, and
  `/country/:country/person/:id` combined/raw person evidence pages. Legacy
  `/person/:name` bookmarks redirect to a search because a name is not an ID.

## Storage map

| layer | location | contents |
|---|---|---|
| S3 (RustFS) | `source-sweden-company` | registry ZIP snapshots + manifests |
| S3 (RustFS) | `source-sweden-financial` | outer report archives, extracted per-report XHTML, archive sync manifests |
| DuckDB | `data/sweden_company_source.duckdb` | raw + normalized registry staging |
| DuckDB | `data/sweden_financial/sweden_financial_source_<year>.duckdb` (2020–2026) | XHTML catalog + parsed reports/facts per archive year |
| ClickHouse | `corpscout.se_*` tables (migrations `corpscout/clickhouse/migrations/`) | published layer, see counts above |

DuckDB files and S3 raw snapshots are rebuildable cache; ClickHouse + the
Dagster Postgres are the backup scope (see `docs/deployment-runbook.md`).

Row counts, `se_financial_reports` by report period (@ 2026-07-20): 2017: 2,
2018: 275, 2019: 71.7k, 2020: 159.2k, 2021: 240.4k, 2022: 331.4k,
2023: 413.8k, 2024: 484.1k, 2025: 493.0k, 2026: 13.1k. Facts follow the same
shape (2025: 64.3M). Sparse early years are expected: the bulk archives start
at publication year 2020, so pre-2020 periods appear only via late filings.

## Known caveats and data-quality notes

- **One company can file more than once per fiscal year** (corrections);
  metrics pick one canonical row per filing, the facts layer keeps everything.
  Multi-year statement keys do not exist (verified).
- **Officers come from signature blocks only** — the people listed are those
  who signed the annual report (board, CEO, certifier, auditor) at signing
  time. This is not the full Bolagsverket role registry (näringslivsregistret,
  which is a paid API): resignations mid-year, deputies who never sign, and
  non-signing roles are invisible.
- **Comparative history is reconstructed, not filed** — history rows carry
  `net_result`/`equity` as NULL where the comparative columns don't include
  them; UI marks comparative rows.
- **Modified audit opinions are rare and meaningful** (743 of ~396k) — surfaced
  with a destructive badge in the UI.
- **Zero-revenue years in the UI** usually mean the filing genuinely reported
  no revenue concept (dormant/holding companies), not a parse gap — the facts
  drill-down shows exactly what was filed.
- All derived aggregations use **deterministic argMax tuple tiebreaks** (e.g.
  `argMax(x, (role_kind != 'unknown', statement_key))`) — never bare
  `any()`/`argMax` on non-unique keys; two real non-determinism bugs were
  fixed this way (officers roles, audit firm names).

## What is missing / what should be done next

The items below are data-quality and completeness work on the sources we
already ingest. Proposed *new* sources (Skatteverket status flags,
Kronofogden, SCB establishments/group structure, Bolagsverket events, PRV,
press releases…) and their recommended order live in
`sweden-new-data-sources-proposal.md`.

Ordered roughly by value-for-effort:

1. **Translations (planned, not executed)** —
   `docs/superpowers/plans/2026-07-19-sweden-text-translations.md`. Swedish
   text is untranslated everywhere today (`text_translations` has zero `se_*`
   rows): facts concept keys, `activity_description` in `se_companies`, audit
   opinion kinds. The plan follows the Norway pattern: distinct-text
   translation via the Go translator service (sole writer of
   `text_translations`, keyed on `cityHash64(text)`), plus `<table>_translated`
   views. Task 1 needs the Go-side source registration (user involvement).
2. **Surface proceedings in the UI** — typed liquidation, bankruptcy, and
   restructuring procedures are now stored historically in
   `se_company_proceeding_observations`; add the current procedure set and its
   effective dates to the company detail status area.
3. **Dividends** — proposed/decided dividend concepts are in the facts;
   extract into metrics or a small dedicated table.
4. **Wider metrics concepts** — the long-form facts hold many mappable
   concepts (depreciation, financial income/expense, inventory, long-term
   liabilities…) that the stable metric projection does not yet include.
5. **Registry contact candidates** — build
   `sweden_company_contact_candidates_duckdb` so SE companies get
   domains/emails like other countries.
6. **Search integration** — officer names and secondary names are not in the
   `companies_all` search index yet; both are extracted and queryable but only
   reachable through detail pages / `/people`.
7. **Audit-firm → orgnr linkage** — audit firm names are strings; linking them
   to their own `se_companies` rows would make auditor portfolios queryable.
8. **POIT gazette** (Post- och Inrikes Tidningar) — announcements
   (liquidations, mergers, reconstructions) as an event stream; separate
   source, not yet investigated in depth.
9. **LEI / GLEIF linkage** — connect `se_companies` to the existing `gleif`
   module for group hierarchies. Note (2026-07-20): the cross-country
   `defs/esef_filings/` module now builds `esef_entity_registry_map` from
   `gleif_lei_records`, giving any Swedish LEI filer a registry-id match today
   (SE normalizer: digits-only org number) — but only for companies that also
   file ESEF (listed issuers). This item is still open for `se_companies`
   broadly (unlisted companies have no ESEF filing to match through).
10. **Stronger same-country identity evidence** — the reviewed correction
    workflow is now in place, while the automatic resolver still deliberately
    refuses to merge same-name people across companies without a source
    identifier. Evaluate additional evidence only against the reviewed ledger;
    do not introduce cross-country automatic linkage.
11. **Full role registry** — if signature-block officers prove too narrow,
    Bolagsverket's näringslivsregistret (paid) is the upgrade path.
