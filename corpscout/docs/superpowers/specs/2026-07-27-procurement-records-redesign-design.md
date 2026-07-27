# Procurement register records redesign

2026-07-27. Approved in brainstorming with Goran.

## Problem

The records section on `/procurements/:source` renders every column the register publishes in a
hand-rolled table with two ad-hoc filters (country text box, date range). For TED that is 40+
columns, most of which are FX bookkeeping, estimated/framework value variants, or load plumbing.
The country filter is a free-text box on a register that only contains three countries. Buyer
identity is shown as a raw org number even though most buyers exist in the company register.

## Decisions

- Reuse the existing shadcn data-table stack (`app/components/data-table/`) with dynamic columns,
  not a bespoke table and not the full companies "unified" infrastructure.
- One generic column-curation rule set for all registers, not per-source lists.
- The record detail page (`/procurements/:source/:key`) continues to show every column, including
  the ones hidden from the table. Its purpose is "what does this register publish"; the table's
  purpose is scanning records.
- Buyers get links to the existing company detail page when their national ID matches
  `companies_all`. No dedicated buyer pages.

## Design

### 1. Column curation — `app/lib/procurement-columns.ts`

One shared, ordered rule set deciding which columns the records table hides:

- exact: `source_slug`, `source_run_id`, `partition_key`, `resolved_at`
- prefix/pattern: `fx_*`, `estimated_*` and `framework_*` value groups
  (`*_amount_original`, `*_amount_usd`, `*_currency` variants included)

Exported as a predicate `isHiddenTableColumn(name: string): boolean` plus
`visibleColumns(columns: string[]): string[]`. Unit-tested against the real column names of all
five registers. Every register (TED, Doffin, Hilma, PNCP, UHM) inherits the rules; new registers
get them for free. The detail page does not use this module.

### 2. Records table — shadcn data table

`procurement-source.tsx` builds `ColumnDef[]` at render time from `visibleColumns(...)` and renders
through the shared `DataTable`. Changes to shared components are limited to parameterizing the
empty-state text (currently hardcoded "No companies found."). Server-side pagination stays in the
loader (tables exceed 100k rows); the existing pagination component is reused. Money columns
(via `formatMoneyField`) render right-aligned with `tabular-nums`. The key column keeps its link to
the record detail page.

### 3. Filter sheet — `ProcurementFilterSheet`

A Sheet-based filter panel modeled on `filter-sidebar.tsx`. Sections render only when the selected
table has the backing columns:

- **Country** — shadcn `Select` of all 27 EU countries by full name; only countries with rows in
  the current table are enabled (TED: Sweden, Finland, Norway), the rest greyed out as
  "not loaded". Static EU list (iso2 + name) in code; the enabled set comes from the data.
- **Publication date** — from/to (existing behavior, moved into the sheet).
- **Buyer** — case-insensitive name search over `buyer_name`.
- **Winner** — search over winner name or org number.
- **Award result / notice type** — dropdowns populated from distinct values in the table.
- **Value (USD)** — min/max over the USD-normalized award amount.

All filter state lives in URL search params. The loader translates params to parameterized WHERE
clauses: column names validated against the table's real columns (`columnsOf`), values bound as
ClickHouse query parameters, never interpolated.

### 4. Buyer and winner company links

The loader batch-resolves the current page's `buyer_national_id` and winner org-number values with
one `IN` query against `companies_all`. Matches render the name as a link to the existing company
detail page; non-matches stay plain text. Buyers are predominantly government institutions whose
org numbers exist in the national registers (measured: SE 996/1015 unique buyers matched, NO
633/668, FI 383/893). The Finland rate is suspiciously low — check business-ID normalization
(`1234567-8` formatting) during implementation.

## Testing

- Unit: hidden-column rules against real register schemas; filter-param → WHERE builder incl.
  rejection of unknown columns.
- Existing vitest setup; `npm run typecheck`.
- Browser: TED (multi-country, all filters) and one single-country register (sheet omits country
  or shows it fixed), record detail page still shows all columns.

## Out of scope

- Dedicated buyer aggregation pages (possible follow-up).
- Facet counts / saved views like the companies unified table.
- Any change to the country contract views or detail pages beyond the buyer/winner links.
