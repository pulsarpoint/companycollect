# SE company financial entity: design

Owner-approved in conversation 2026-09-11 (the decisions of section 2 and sections 3 to 11 agreed
one by one). Fourth entity on the basic-info shape after `se_company_basic_info` (2026-09-03),
`se_company_address` (2026-09-06) and `se_company_person` (2026-09-09): per-source suggestions in
the entity's shape, a Python fold into one main table with history, rules and precedence, and a
backoffice tab. Sweden only. Unlike the address and person entities there is no stored normalized
layer: the sources already deliver typed numbers, so unit scaling and field mapping happen in the
extractors.

## 1. Why

Swedish financial figures exist three times today and are never combined: Bolagsverket's annual
accounts (`se_bolagsverket_financial_metrics`, 6.47M rows, 580k companies, exact SEK, 2020 to
2026), Ratsit's per-report periods (`se_ratsit_financial_periods`, 3.14M rows, 720k companies,
MSEK rounded to a tenth, 2000 to 2026, standalone and consolidated) and ESEF's consolidated IFRS
metrics (1.3k company-years, 404 companies, through the LEI map). Ratsit and Bolagsverket share
2.01M company-years and Ratsit alone covers another 1.09M, mostly before 2020 and for companies
that never filed digitally. The backoffice and the public financials page read two same-shape
views behind a source switcher, show Ratsit nowhere, attribute no value to a source, keep no
history and offer the reviewer nothing to decide. The owner's ask (2026-09-11): represent every
source's opinion on a value, pick one by precedence or by a reviewer's choice, present it with the
source it came from, and show every other source that had an opinion on that cell.

## 2. Scope and decisions

- **Row identity**: one row per company, accounting scope and period end,
  `(company_id, scope, period_end)`. Scope is `standalone` or `consolidated`; a group figure never
  merges with a legal-entity figure. Period end is exact: Bolagsverket and ESEF are fully dated,
  and only 7,503 of Ratsit's 3.14M rows lack dates and get Dec 31 of their fiscal year, flagged.
  `fiscal_year`, `period_start` and `period_months` are folded fields, not keys, so a changed
  fiscal-year end keeps its two short periods apart (2,796 Bolagsverket years carry two filings,
  235k Ratsit periods are not twelve months).
- **Sources**: `bolagsverket` (reported filings), `bolagsverket_comparative` (the prior-year
  column a later filing restates; its own source so a restatement shows beside the original and a
  reviewer can prefer it), `esef` (consolidated IFRS), `ratsit` (both scopes), plus `reviewer` and
  `reviewer_draft`. Wikidata's employee counts and SCB, which carries no figures, are not sources
  in this version.
- **Fields**: the union of every figure a source publishes, 20 monetary fields and `employees`.
  Ratios (equity ratio, margins, per-employee figures) are left out because the reader computes
  them from the folded figures, and Ratsit's `average_salary` is left out until Ratsit states its
  unit. Every monetary field carries an original and a USD twin and a per-field source.
- **Precedence**: Ratsit first (owner decision 2026-09-11: "I think Ratsit information is pretty
  accurate"; the measured disagreement with Bolagsverket is rounding, not error), then the two
  registers, then restatements. Numbers in section 5.
- **Rules**: a reviewer precedence rule is scoped to one period or to every period of the company;
  a period can be hidden.
- **Readers**: full cutover in this project (owner decision 2026-09-11: "all that can be just
  removed, there is no need for a slow cut off"). The admin Financial tab and the public financials
  page render the entity, every Swedish reader of the old presentation objects is re-pointed, the
  two per-source views and the Sweden-only financial components are deleted. Section 10.
- **Slice 0 first**: Ratsit's source table gains USD twins through its own asset before the entity
  reads it (owner: "we should first add asset that will update ratsit currency conversion").
- Out of scope: other countries, the Bolagsverket and ESEF parsing pipelines, the per-year facts
  and ESEF document readers (they stay as source readers the entity links to), a sensor or a
  fold-aware weekly, and USD twins on the other Ratsit tables.

## 3. Slice 0: Ratsit financial USD conversion

The currency standard (data-source-guidelines section 7) wants every monetary figure a source
publishes stored with its USD twin in the source table. `se_ratsit_financial_periods` violates it:
it carries 20 monetary columns and no USD. This step fixes the source layer so the entity, and any
other reader, finds ready pairs.

**Table change, migration 000400.** `se_ratsit_financial_periods` gains a `<column>_usd
Nullable(Decimal(38, 6))` twin after each of its 20 monetary columns: the 18 `*_amount` lines
(`revenue_amount` through `dividend_amount`, `balance_sheet_total_amount` included, for fidelity)
and the two per-employee MSEK figures (`personnel_cost_per_employee_usd`,
`revenue_per_employee_usd`). Plus `fx_rate_to_usd Nullable(Decimal(38, 12))`, `fx_rate_date
Nullable(Date32)`, `fx_source LowCardinality(String) DEFAULT ''`. Existing columns keep their names
and values; `average_salary` gets no twin (unit unknown), the percent ratios are not money. The
normalizer's INSERT lists its columns explicitly and is untouched: new columns default to NULL.

**Asset `se_ratsit_financial_periods_usd`** in `defs/sweden_ratsit`, unpartitioned, depending on
the `se_ratsit_financial_periods` spec key and on `exchange_rates_v2_clickhouse` with an
`AllPartitionMapping`. Each run:

1. Reads the distinct rate dates still pending: rows `FINAL` with `fx_rate_to_usd IS NULL` and at
   least one monetary value, rate date `ifNull(period_end, makeDate32(fiscal_year, 12, 31))`.
2. Resolves SEK to USD for those dates through the shared `ExchangeRateClient.usd_rates`, batched
   at 50 like the Bolagsverket step; the client picks the latest ECB date at or before the
   requested one and reports it as `fx_rate_date`.
3. Writes the resolved rates into a per-run `Join(ANY, LEFT, rate_date)` table
   `corpscout._tmp_ratsit_fx_<run>` and runs one `ALTER TABLE ... UPDATE` restricted to
   `fx_rate_to_usd IS NULL AND <rate date> IN (SELECT rate_date FROM <join table>)`, with
   `mutations_sync = 2`. USD is `multiplyDecimal(<amount> * <scale>, joinGet(<join table>,
   'fx_rate', <rate date>), 6)` where the scale is `multiIf(monetary_unit = 'MSEK', 1000000,
   monetary_unit = 'TSEK', 1000, 1)` for the 18 amount lines and a fixed 1000000 for the two
   per-employee MSEK figures. The join table name is always database-qualified (a mutation has no
   default database). Then drops the join table.
4. Reports `rows_pending`, `rate_dates_needed`, `rates_found`, `rows_converted` and
   `rows_still_without_rate`. Config `execute` defaults to false (preview counts only), as the
   entity extractors do.

Proven on 2026-09-11 against ClickHouse 26.5 in clickhouse-local: the mutation fills the twins and
the three fx columns, scales MSEK and TSEK, derives the rate date for undated rows and leaves rows
without a rate untouched. The step is idempotent and re-runnable: a row is converted once, a row
whose rate does not exist yet (a period end after the newest ECB date) is picked up on a later
run, and the normalizer never overwrites USD because it only inserts rows for new report versions.
`_amount` stays Ratsit's rounded published figure in its published unit and `_usd` is the
full-unit dollar value: the guideline's asymmetry, scaling applied before FX.

Job `se_ratsit_financial_usd_job`, manual. The entity's Ratsit extractor depends on this asset and
the entity's extract job selects it first.

## 4. Tables

All in database `corpscout`, ClickHouse 26.5, created by golang-migrate migrations (first line
`CREATE DATABASE IF NOT EXISTS corpscout;`, no `;` inside comments, last line a statement). Entity
tables in migration 000401. Numbers are checked against main and the prod ledger (399 on
2026-09-11) at each merge.

The 20 monetary fields, in DDL order: `revenue`, `operating_costs`, `operating_result`,
`result_after_financial_items`, `net_result`, `ebitda`, `total_assets`, `fixed_assets`,
`current_assets`, `cash_and_bank`, `equity`, `share_capital`, `untaxed_reserves`, `provisions`,
`liabilities`, `long_term_liabilities`, `current_liabilities`, `personnel_expenses`,
`wages_and_salaries`, `dividend`. Each is a pair `<field>_amount_original` and
`<field>_amount_usd`, both `Nullable(Decimal(38, 6))`.

### 4.1 `se_company_financial_suggestion`

One current row per company, source and period.

```
company_id          String
source              LowCardinality(String)  -- bolagsverket, bolagsverket_comparative, esef, ratsit, reviewer, reviewer_draft
period_key          String                  -- '<scope>:<period_end>', e.g. 'standalone:2023-12-31'
suggestion_id       String                  -- lineage id, minted by the extractor from one clock read
suggested_at        DateTime64(3, 'UTC')    -- version
source_record_uid   String                  -- statement key; ESEF fxo_id; 'ratsit:<company>:<report index>:<period index>'; '' for reviewer rows
scope               LowCardinality(String)  -- standalone | consolidated
period_end          Date32
period_end_derived  UInt8 DEFAULT 0         -- 1 when the source had no date and Dec 31 of the fiscal year was used
period_start        Nullable(Date32)
fiscal_year         Nullable(UInt16)
period_months       Nullable(UInt16)
filing_fiscal_year  Nullable(UInt16)        -- the filing that carried the figures; differs from fiscal_year on restated rows
currency            LowCardinality(Nullable(String))
amount_scale        UInt32 DEFAULT 1        -- the unit the source published in: 1, 1000 or 1000000
<field>_amount_original, <field>_amount_usd  x 20
employees           Nullable(UInt64)
fx_rate_to_usd      Nullable(Decimal(38, 12))
fx_rate_date        Nullable(Date32)
fx_source           LowCardinality(String) DEFAULT ''
decided_by          LowCardinality(String) DEFAULT ''   -- reviewer rows
note                String DEFAULT ''                   -- reviewer rows
source_run_id       String
extractor_version   LowCardinality(String)
ENGINE = ReplacingMergeTree(suggested_at) ORDER BY (company_id, source, period_key)
CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
CHECK scope IN ('standalone', 'consolidated')
CHECK period_key = concat(scope, ':', toString(period_end))
CHECK amount_scale IN (1, 1000, 1000000)
```

Every `_amount_original` is in the source's currency at full units: Ratsit's 57.1 MSEK lands as
57,100,000 with `amount_scale = 1000000` saying the figure was published in millions. USD twins and
the fx columns are copied from the source table, never converted here. NULL in a value column means
"this source has no opinion", never "this source says zero". A tombstone is a row whose 20
figures and `employees` are all NULL; an extractor writes one per period a source stops delivering
(a live row always carries at least one figure or an employee count). The reviewer sources use the
same period keys as the pipeline sources, so adding a period is a reviewer row at a new key, and
`reviewer_draft` holds typed values the reviewer has not activated, unranked everywhere.

### 4.2 `se_company_financial` (main)

One row per company, scope and period end.

```
company_id          String
scope               LowCardinality(String)
period_end          Date32
period_key          String                  -- the suggestion key of this period, for rules and the backoffice
period_start        Nullable(Date32)        period_start_source   LowCardinality(String)
fiscal_year         Nullable(UInt16)        fiscal_year_source    LowCardinality(String)
period_months       Nullable(UInt16)        period_months_source  LowCardinality(String)
currency            LowCardinality(String)  currency_source       LowCardinality(String)   -- '' when no source names one
<field>_amount_original, <field>_amount_usd, <field>_source   x 20
employees           Nullable(UInt64)        employees_source      LowCardinality(String)
sources             Array(String)           -- every source that won at least one field, sorted
active              UInt8                   -- 0 when hidden by a rule or withdrawn
inactive_reason     LowCardinality(String)  -- '', hidden, withdrawn
folded_at           DateTime64(3, 'UTC')    -- version
fold_version        LowCardinality(String)
source_run_id       String
ENGINE = ReplacingMergeTree(folded_at) ORDER BY (company_id, scope, period_end)
```

A `_source` column is `''` when the field has no value. There are no row-level fx columns: each
figure's USD twin is its own winner's conversion, and one rate per row would be wrong the moment
two sources share a row; the backoffice shows the rate behind any cell from the suggestion row.

### 4.3 `se_company_financial_history`

Every column of the main table plus `changed_fields Array(String)`, `changed_at DateTime64(3,
'UTC')`, `change_kind LowCardinality(String)` (created, updated, hidden, withdrawn, reactivated)
and `fold_run_id String`. `ENGINE = MergeTree ORDER BY (company_id, scope, period_end,
changed_at)`. Written only on change, the first publish included (`changed_fields` = every field
with a value).

### 4.4 `se_company_financial_precedence`

The basic-info shape with the period scope.

```
company_id   String                   -- '' for the global rows exported from code
period_key   String                   -- '' = every period of the company; otherwise one period (its scope is inside the key)
field        LowCardinality(String)
source       LowCardinality(String)
precedence   UInt32
removed      UInt8 DEFAULT 0          -- 1 = a released rule (the newest version wins)
decided_by   LowCardinality(String) DEFAULT ''   -- 'code' for exports, 'backoffice' for decisions
note         String DEFAULT ''
decided_at   DateTime64(3, 'UTC')     -- version
ENGINE = ReplacingMergeTree(decided_at) ORDER BY (company_id, period_key, field, source)
```

The export writes only `company_id = ''` rows and never touches a decision; the backoffice writes
only company rows, one version per decision; a release is a new version with `removed = 1`.

### 4.5 `se_company_financial_rule`

The address shape, one action.

```
company_id   String
period_key   String
action       LowCardinality(String)   -- hide
removed      UInt8 DEFAULT 0
decided_by   LowCardinality(String) DEFAULT ''
note         String DEFAULT ''
decided_at   DateTime64(3, 'UTC')     -- version
ENGINE = ReplacingMergeTree(decided_at) ORDER BY (company_id, period_key, action)
```

### 4.6 Kept as is

Every source table: `se_bolagsverket_financial_metrics` and its observations, facts and reports,
`esef_financial_metrics` and the `se_esef_*` views, `se_ratsit_financial_periods` (with the slice 0
columns) and `se_ratsit_financial_reports`. The cross-country `se_company_financials_latest` keeps
its name and schema (frozen until fifteen countries) and is rebuilt from the entity (section 10).

## 5. Precedence

In Python, `dagster_v3.defs.se_company.financial.precedence`, one map per field, scope-agnostic:
Bolagsverket and ESEF never meet because they never share a scope.

| Field group | Order |
|---|---|
| period_start, fiscal_year, period_months, currency, revenue, total_assets | reviewer 20000, ratsit 1000, bolagsverket 900, esef 900, bolagsverket_comparative 800 |
| operating_result, net_result, equity, liabilities, employees | reviewer 20000, ratsit 1000, bolagsverket 900, esef 900 |
| cash_and_bank, personnel_expenses | reviewer 20000, bolagsverket 900, esef 900 |
| current_assets, current_liabilities | reviewer 20000, ratsit 1000, bolagsverket 900 |
| wages_and_salaries | reviewer 20000, bolagsverket 900 |
| operating_costs, result_after_financial_items, ebitda, fixed_assets, share_capital, untaxed_reserves, provisions, long_term_liabilities, dividend | reviewer 20000, ratsit 1000 |

The comparative source appears only where Bolagsverket restates a figure, revenue and total
assets. A source absent from a field's map cannot supply that field; a company rule can rank it in.
Precedence only decides when several sources have an opinion on the same cell: for the 1.09M
company-years only Ratsit covers and the nine figures only Ratsit carries, the order never matters.
Accepted consequence of Ratsit first: where the exact SEK figure exists, the main row shows
Ratsit's figure rounded to a tenth of a million, and `amount_scale` on the winning suggestion row
lets the reader say so. The numbers are the owner's to adjust in review; a change is a full re-fold
(`changed_only: false` over all 64 buckets) after the precedence export.

Rules resolve most-specific first: an active period rule `(company_id, period_key, field,
source)`, else the company-wide rule (empty `period_key`), else the global number. "Use this"
writes 10000; a value the reviewer typed and activated ranks 20000; `reviewer_draft` has no rank
anywhere, so the fold and every watermark ignore it.

## 6. The fold

`fold_financial(company_id, period_key, suggestions, rules, hidden) -> FinancialRow | None`, a
pure function over one period's current suggestion rows (tombstones excluded):

1. **Currency first.** Among rows with a currency, the highest effective precedence wins and sets
   the row's currency. Ties, which only a company rule can create, go to the smaller source name,
   then the smaller `source_record_uid`.
2. **Money gated by currency.** For each of the 20 figures, only rows whose currency equals the
   row's currency compete; the winner supplies its `_amount_original` and `_amount_usd` together,
   never mixed. A row without a currency can never supply money (the 15 ESEF rows with a blank
   currency contribute employees and dates only).
3. **Employees and the period fields** (`period_start`, `fiscal_year`, `period_months`) compete
   without the gate.
4. `sources` is the sorted set of winning sources. `hidden` (an active hide rule for the period)
   makes the row fold normally and land with `active = 0, inactive_reason = 'hidden'`.
5. **Publish rule.** A row is returned when any field has a winner. With no live suggestion at all
   the function returns None; the batch layer turns that into a withdrawal when a main row exists.
6. The row carries `fold_version` (a module constant, bumped when the logic changes) and the run id.

Accepted consequences of the exact key: Ratsit's undated rows merge with a register period that
really ends Dec 31 and otherwise stand as their own row, flagged derived; two sources disagreeing
on the end date by a day produce two rows. The slice 3 prod readout counts companies with two
standalone periods ending within seven days of each other, so a merge rule can be designed later if
the number warrants it.

**Batch layer** `fold_companies(client, company_ids, *, changed_only, page_size)`, per page:

1. Read the current suggestion rows for the page's companies (`argMax` over `suggested_at` per
   company, source and period key), the companies' active precedence rules and hide rules,
   grouped by company and period.
2. With `changed_only`, keep a company when its newest `suggested_at`, its newest rule or hide
   decision (`decided_at`, removed versions included) is later than its newest `folded_at`, or it
   has no main row. Selection is per company: all of a company's periods fold together. The global
   precedence export never selects a company on its own (the person ruling 7).
3. Fold every period; read the company's current main rows; compare values and sources per
   period. A period with a main row and no live suggestion gets a `withdrawn` version that keeps
   the last values with `active = 0`; a hidden period whose rule was released comes back as
   `reactivated`.
4. Insert a new main row for every folded period so `folded_at` advances and the selection
   converges (the basic-info decision of 2026-09-04); append one history row per period whose
   values, sources or activity changed, with its `change_kind`.

Pages of 5,000 companies (config, not 20,000): a company carries about 13 suggestion rows across
its sources and periods and each row is 65 values wide, so a page is about 65k rows in memory. The
64-bucket backfill is about 12k companies per bucket, three pages each. Reads use the family's
`max_query_size` settings for id-bound statements.

## 7. Extractors

All four use `basic_info/extract.py`'s `SuggestionTarget` and `define_suggestion_asset`, as the
address and person entities do, with the person entity's per-company state-hash change scan
instead of a timestamp: all three source tables are rebuilt whole (Bolagsverket metrics swap
tables weekly, ESEF metrics rebuild entirely, Ratsit re-normalizes per scan), so "newer than last
time" would re-extract everything every week. The scan hashes what a source delivers now for a
company against the company's stored live rows, visits only companies whose hashes differ or that
one side lacks, writes the live rows and one tombstone per period the source stopped delivering,
and converges after one pass. That machinery lives in `person/suggestions.py` today; slice 2 lifts
it into a shared module `se_company/state_scan.py` parameterized by the target's columns and
switches the person extractors to it, with the person entity's extractor tests as the regression
net. Every extractor joins the published universe, `se_company_basic_info FINAL`, as the person
extractors do, and stamps `suggestion_id` and `suggested_at` from one `WITH (SELECT now64(3,
'UTC')) AS stamp` (two `now64()` calls in one statement were measured to differ).

- **bolagsverket** (`financial/bolagsverket.py`): `se_bolagsverket_financial_metrics FINAL WHERE
  observation_kind = 'reported'`, period key `standalone:<report_period_end>`. Where a period has
  two statements (14 groups today, identical figures archived twice) the one with more non-NULL
  figures wins, then the smaller `statement_key`. Maps the twelve register metrics
  (`operating_profit_loss` to `operating_result`, `profit_loss` to `net_result`, the rest by
  name), copies the USD twins and the fx columns, `period_months` as the rounded month count
  between the two dates, `filing_fiscal_year` from `source_fiscal_year`, `amount_scale` 1,
  `source_record_uid` from the metrics row. Version `bolagsverket-financial-v1`.
- **bolagsverket_comparative**: comparative rows of the same table, one per represented period,
  from the newest restating filing (greatest `source_fiscal_year`, then the smaller statement
  key). Revenue and total assets only, whichever the row carries; `filing_fiscal_year` names the
  filing that restated them.
- **esef** (`financial/esef.py`): `esef_financial_metrics FINAL` joined to the `se_esef_filings`
  view on `(lei, period_end, fxo_id)` for the register-verified `company_id`, scope
  `consolidated_ifrs` mapped to `consolidated`; any other scope value is counted and skipped. Per
  period the newest version (the numeric suffix of `fxo_id`) wins field by field with older
  versions filling its gaps, today's view logic (61 amended periods), and names the newest
  `fxo_id` as `source_record_uid`. Eight fields plus employees (`operating_profit` to
  `operating_result`, `profit_loss` to `net_result`, `cash` to `cash_and_bank`), Decimal128(2)
  cast to the entity type; a blank currency becomes NULL; `amount_scale` 1.
- **ratsit** (`financial/ratsit.py`): `se_ratsit_financial_periods FINAL` for each company's
  latest report (`argMax(result_sha256, normalized_at)` over `se_ratsit_financial_reports`),
  depending on the slice 0 asset. Scope `company` maps to `standalone`, `consolidated` stays. A
  missing period end becomes `makeDate32(fiscal_year, 12, 31)` with `period_end_derived = 1`; a
  row with neither is counted and skipped. Among duplicate periods (577 today) the longer
  `period_months` wins, then the greater report and period index. Originals are the published
  figure times the unit scale (`monetary_unit` MSEK 1000000, TSEK 1000, SEK 1), `amount_scale`
  records that scale, `_usd` comes from the slice 0 twins, `currency` is SEK, `employment_only`
  periods contribute employees alone, `balance_sheet_total_amount` is ignored in favour of
  `total_assets_amount` per Ratsit's own field rule. Seventeen fields plus employees.
  `source_record_uid` is `ratsit:<company>:<report index>:<period index>`, stable across
  re-scans.

Each extractor reports counts per outcome (inserted, tombstoned, skipped by reason, unchanged) and
takes the family's config: `execute` (false = preview), `company_ids`, `max_companies`,
`page_size`.

## 8. Dagster

Group `se_company_financial`, package `dagster_v3.defs.se_company.financial` with the family's
split: `tables`, `precedence`, `fold`, `batch`, `suggestions` (the target and the shared SQL
shapes), `bolagsverket`, `esef`, `ratsit`, `assets`, `jobs`. Everything manual, no sensor.

- `se_company_financial_suggestions_bolagsverket`, `_bolagsverket_comparative`, `_esef`,
  `_ratsit`: each depends on its source's final asset, `se_bolagsverket_financial_metrics_clickhouse`,
  `esef_financial_metrics_clickhouse` and `se_ratsit_financial_periods_usd`. Sources never depend
  on the fold (the lineage ruling of 2026-09-08).
- `se_company_financial_fold`: 64 static partitions on `cityHash64(company_id) % 64`,
  `BackfillPolicy.multi_run(max_partitions_per_run=1)`, its own serial pool
  `se_company_financial_fold`; config `changed_only` (default true) and `page_size` (default 5,000).
- `se_company_financial_fold_companies`: unpartitioned, config `company_ids` required,
  `changed_only` default false; the backoffice's Fold now target.
- `se_company_financial_precedence_clickhouse`: exports the global rows, idempotent; run before
  the first fold because its `decided_at` is a watermark.
- Job `se_company_financial_extract_job` selecting `se_ratsit_financial_periods_usd` and the four
  extractors; schedule `se_company_financial_weekly` defined STOPPED at a `(minute, hour)` the
  cron uniqueness test accepts, run config `execute: true`.
- The cross-country `company_financials_latest` asset's Sweden leg depends on
  `se_company_financial_fold` (section 10).

## 9. Backoffice and the public page

The admin Financial tab (`/admin/se/company/:companyId/financial`, route
`admin-se-company-financial.tsx`) becomes the entity workspace, in the family's split:
`app/lib/se-company-financial-entity.server.ts` (one read over the five tables into a detail
object, the reviewer writes, the targeted-fold launch), `app/lib/se-financial-fields.ts`
(client-safe catalog: the 20 figures, employees, the period fields, sources, scopes, validation),
`app/lib/se-financial-decision-form.ts` (intents), `app/lib/se-financial-tables.ts`,
`app/components/admin/se-financial-workspace.tsx`, `app/components/admin/se-financial-edit-sheet.tsx`,
`app/components/admin/fold-run-poller.tsx` reused unchanged. The grid and the read-only sources
panel are one shared component, `app/components/financials/se-financial-grid.tsx`, that the public
page renders without actions.

**Layout.** A scope switch at the top, standalone and consolidated, each with its period count.
Left, two thirds: a statement-shaped grid, periods as columns newest first, fields as rows. Every
cell shows the folded value in the row's currency, formatted, a short source badge, and a "custom
order" mark where a rule decided it. Column headers carry period end, fiscal year, months,
currency, the derived-date flag and the period's source set. Hidden and withdrawn periods are
greyed behind a "show hidden" toggle. Beneath the grid: a collapsed history card per period and
the fold footer (`folded_at`, `fold_version`, run id). Right, one third, sticky: the panel for the
selected cell (search parameters `period` and `field`, default the newest period's revenue). It
lists every source's suggestion for that cell ordered by effective precedence: original value, USD
twin, "published in millions" when `amount_scale` says so, "restated in the <filing_fiscal_year>
filing" on comparative rows, the derived-date flag, `source_record_uid` as a link where a reader
page exists (the per-year facts page for a Bolagsverket statement, the ESEF document page for an
fxo_id), and `suggested_at`. The winner is marked active, sources without an opinion sit greyed at
the bottom, a source in another currency shows "not eligible: EUR row vs SEK" instead of a button.

**Actions**, each an append, never an edit of a published row:

- *Use this*: a period rule at 10000 for (company, period, field, source); with the "for every
  period" checkbox a company-wide rule (empty period key). Shown on every eligible row, the active
  one included.
- *Reset to default*: retires the field's active rules at the same two scopes (one `removed = 1`
  version per rule, note "reset to default" plus the reviewer's) and clears a typed reviewer value
  for that cell (a new reviewer-row version with the field NULL).
- *Edit value*: a right-side sheet with a decimal input in full units of the row's currency for a
  figure, an integer input for employees, date inputs bounded at the Date32 floor for the period
  fields, a select for the currency. Saving writes a new `reviewer_draft` version at that period
  key with the field set. The panel shows the draft as a "Draft" row with *Activate* (a new
  `reviewer` version with the field copied from the draft and a draft version with it cleared,
  then the targeted fold) and *Discard* (clears it in the draft).
- *Add period*: a draft at a new key (scope and period end required; any figures; currency
  required when money is typed); Activate publishes every typed field at once.
- *Hide period* and *Unhide*: a rule-table version with `removed` 0 or 1.
- *Fold now*: launches `se_company_financial_fold_companies` for the company through the existing
  Dagster launch helper; the workspace polls the run and reloads.
- The fold-pending marker considers reviewer rows, company rules and hide rules newer than the
  company's newest `folded_at`, never the global export.

Validation lives in `se-financial-fields.ts` and runs without a server import: decimal strings
that fit `Decimal(38, 6)`, employees a non-negative integer, dates between 1900-01-01 and today,
scope and source from the catalog, a note that allows line breaks and refuses other control
characters, as the other entities do.

**Public page.** `company-financials.tsx`'s Sweden branch renders the shared grid and read-only
panel over the same loader (`getCompanyFinancialDetail` for `se` returns the entity detail),
keeps the filing-status summary above and the report-documents table below. The per-year facts
route and the ESEF document routes stay as they are. The admin companies financial list page
(`admin-se-companies-financial.tsx`, a scaffold) stays as it is.

## 10. Readers and retirement

Owner decision 2026-09-11: no parallel run. In the cutover slice every Swedish reader of the old
presentation objects moves to the entity and the old objects go.

Re-pointed to `se_company_financial` (active rows, `FINAL`):

- `company_financials_latest/sql.py`'s Sweden leg: latest standalone period per company from the
  entity (revenue, net result, total assets, equity, employees with their USD twins,
  `years_count = uniqExact(fiscal_year)`), so `se_company_financials_latest` keeps its
  cross-country contract; `UPSTREAM_KEYS["se"]` becomes the fold.
- `company_serving/dbt/models/company_section_presence_current_build.sql`'s financials leg and
  `publish.py`'s financials reconciliation: one presence row per company with an active entity
  row, keyed by company as today.
- `sweden_company/companies_current.py`: `has_financial` becomes "has an active entity row"
  through one table constant, the per-source flags `fin_bolagsverket` and `fin_esef` become
  `has(sources, ...)` over the same rows, `fin_reports` stays on `se_financial_reports` (filed
  reports are a source fact, not a presentation). `se_companies_serving` is re-pointed with
  `ALTER TABLE ... MODIFY QUERY` in the cutover migration (000402), the person slice 4 recipe.
- `se_annual_report_filing_status_current` (migration 000282): its first leg reads the entity's
  newest active standalone period end instead of `se_company_financials_latest`, re-issued in
  000402; the `se_annual_report_filing_observations` leg stays (empty today).
- Backoffice: `company-sections.server.ts`'s financials presence, `queries.server.ts`'s
  `getStructuredFinancialFilingFallback`, `countries.ts`'s Sweden `financialsByYear`
  (`se_bolagsverket_financial_metrics` today) read the entity's active standalone rows;
  `financialsLatest` keeps `se_company_financials_latest`.

Deleted: the views `se_financials_bolagsverket_current` and `se_financials_esef_current` (their
only readers were the backoffice's source-view queries), by an owner-run drop script under the
ledger policy (`corpscout/clickhouse/operations/se_financial_views_retirement_{precheck,drops,
postcheck}.sql`, the address and person retirements as precedent; views cannot be UNDROPped, so
the precheck gates on zero readers in `system.tables` view queries) with the historical migration
files 000286 and 000364 emptied of the view DDL and `EMPTIED_MIGRATIONS` extended; the backoffice
Sweden-only financial components `se-financials-view.tsx`, `financial-source-switcher.tsx`,
`financial-source-overview.tsx`, `financial-comparison-table.tsx`, `financial-kpi-strip.tsx`,
`financial-trend-chart.tsx` and whatever of `copy.ts`, `metrics.ts` and `formatters.ts` only they
import (`revenue-bar-chart`, `top-companies-table` and `methodology-note` serve the country
overview and stay); `countries.ts`'s Sweden `financialSources` block;
`queries.server.ts`'s `SWEDEN_FINANCIAL_SOURCE_VIEWS`, `getSwedenFinancialSourceRows`,
`buildSwedenFinancialSources` and the Sweden branch of `getCompanyFinancialDetail`; the tests that
pin them (`queries-financials.test.ts` and `countries.test.ts` where they name the views,
`test_sweden_financial_source_views_migration.py`); the "Financial serving boundary" paragraph of
`sweden_financial-design.md` and the views section of `sweden-data-sources.md` rewritten to name
the entity.

## 11. Testing

- Pure fold tests (`tests/test_se_company_financial_fold.py`): precedence per group, the currency
  gate, period and company-wide rules and their resolution order, hide, withdrawal and
  reactivation, ties, the USD twin travelling with its original, `sources`, `changed_fields`.
- clickhouse-local tests for every extractor's SQL with fixtures for the measured edge cases: a
  duplicated Bolagsverket statement, a restated year with two restating filings, an ESEF
  amendment that drops a metric, an EUR filer, a blank ESEF currency, an undated Ratsit row, two
  Ratsit periods with one end date, an `employment_only` period, a tombstone; and for the batch
  SQL under both `join_use_nulls` settings, as the person tests do.
- The slice 0 mutation test (the 2026-09-11 proof made permanent) and unit tests for the
  rate-date derivation and the count report.
- `tests/se_company_ddl.py` pins the five entity tables and the extended Ratsit table with exactly
  one creator each; `test_clickhouse_migrations.py`'s emptied list gains 000286 and 000364;
  `test_se_companies_serving_sql.py` follows the re-point.
- Backoffice vitest for the parser, the loader and the workspace; a smoke on localhost:5183 with
  5567081699, which all three sources cover (standalone: Ratsit 60.3 MSEK over Bolagsverket
  59,016,040 for 2023; consolidated: ESEF 1,296,506,000).
- Prod readouts after the fold: rows per scope, companies, source distribution per field,
  currency distribution, undated-derived rows, companies with two standalone periods ending within
  seven days, and `se_company_financials_latest` row count against the 579,766 it holds today.

## 12. Slices

0. Ratsit USD: migration 000400, `se_ratsit_financial_periods_usd` and its job, tests; prod
   preview and execute over 3.1M rows with the spot check (57.1 MSEK near 5.4M USD for 5567081699
   in 2023 at the ECB rate of that date). Code complete 2026-09-11 on branch se-financial-entity (Tasks 1 to 5 of plan 2026-09-11-se-company-financial-0-ratsit-usd.md); prod rollout pending.
1. Tables and precedence: migration 000401 with the five tables, `financial/tables.py`,
   `precedence.py` with the export asset, DDL tests; prod apply and export.
2. Extractors: `state_scan.py` lifted from the person package with person switched to it, the
   four extractors, `suggestions.py`, the extract job and the stopped weekly; prod runs with counts
   per source and per skip reason (expected order of magnitude: 3.05M Bolagsverket periods, the
   restated periods, 1.3k ESEF, 3.1M Ratsit).
3. Fold: `fold.py`, `batch.py`, the two fold assets; prod 64-bucket backfill and the readouts of
   section 11.
4. Cutover: the admin workspace, the shared grid on the public page, every re-point of section 10
   with migration 000402, the deletions, the owner-run view drops, the smoke.

Later, separate specs: a fold-aware weekly, USD twins on the other Ratsit tables, a merge rule for
near-identical period ends if the readout warrants it, Wikidata employees as a source.

## 13. Names

Tables `se_company_financial_suggestion`, `se_company_financial`, `se_company_financial_history`,
`se_company_financial_precedence`, `se_company_financial_rule`. Package
`dagster_v3.defs.se_company.financial` (`tables`, `precedence`, `fold`, `batch`, `suggestions`,
`bolagsverket`, `esef`, `ratsit`, `assets`, `jobs`) and the shared
`dagster_v3.defs.se_company.state_scan`. Assets `se_company_financial_suggestions_<source>`,
`se_company_financial_fold`, `se_company_financial_fold_companies`,
`se_company_financial_precedence_clickhouse`; job `se_company_financial_extract_job`; schedule
`se_company_financial_weekly`. Ratsit: asset `se_ratsit_financial_periods_usd`, job
`se_ratsit_financial_usd_job`. Migrations 000400 (Ratsit USD columns), 000401 (entity tables),
000402 (cutover re-points). Backoffice `app/lib/se-company-financial-entity.server.ts`,
`app/lib/se-financial-fields.ts`, `app/lib/se-financial-decision-form.ts`,
`app/lib/se-financial-tables.ts`, `app/components/admin/se-financial-workspace.tsx`,
`app/components/admin/se-financial-edit-sheet.tsx`,
`app/components/financials/se-financial-grid.tsx`; routes `admin-se-company-financial.tsx` and
`company-financials.tsx` (Sweden branch). Operations
`corpscout/clickhouse/operations/se_financial_views_retirement_{precheck,drops,postcheck}.sql`.
