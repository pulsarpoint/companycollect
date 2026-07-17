# backoffice

Internal explorer for CompanyCollect data. React Router v8 (SSR) + shadcn/ui,
reading directly from ClickHouse (read-only).

## Setup

```bash
pnpm install
cp .env.example .env   # fill CLICKHOUSE_PASSWORD from corpscout/.env
pnpm dev               # http://localhost:5183
```

## Commands

- `pnpm dev` — dev server (port 5183)
- `pnpm build` / `pnpm start` — production build / serve (port 3000)
- `pnpm typecheck` — react-router typegen + tsc
- `pnpm test` — vitest (integration tests hit the real ClickHouse from .env)

## Structure

- `app/lib/countries.ts` — static registry: one entry per country, maps URL
  code → ClickHouse table/columns/features. Add new countries here.
- `app/lib/clickhouse.server.ts` — server-only ClickHouse client (`chQuery`).
- `app/lib/queries.server.ts` — per-country stats and company search.
- `app/routes.ts` — `/` picker → `/:country` layout → overview, companies.

## Structure

- `/companies` — ALL countries in one list (name, industry, country).
  Default order is registry order (fast); sorting by name does a cross-
  country top-N merge and takes ~10s over 116M rows (a materialized
  `companies_all` table in dagster is the planned fix). Country is a filter
  (`f_country=ee`), alongside status/legal form/place/size/industry —
  a filter only includes countries that can answer it (e.g. size → Brazil).
- `/company/{country_code}/{id}` — the company detail page.
- `app/lib/unified.server.ts` — cross-country UNION search + merged facets.
- `app/lib/countries.ts` — the per-country registry: list columns, filters,
  detail queries. The per-country query layer (`queries.server.ts`) remains
  the engine for detail pages and the live-schema test sweeps.

## Companies table (Legacy: per-country layer)

The per-country layer powers the detail page and test sweeps. URL-driven state
on `/{country}/companies` (no longer in the unified dashboard but available
if needed): `?q=` name search, `?sort=` column key + `?dir=asc|desc`
(whitelisted against `countries.ts` column config; unknown values fall back to
name asc), `?page=`, `?pageSize=25|50|100`. The industry column is populated
by a second per-page lookup (`industryQuery` in `countries.ts`) and is not
sortable by design — sorting happens on base-table columns only so the 30–70M-row
countries stay fast. Add columns per country in `countries.ts` (`columns`),
never by editing SQL in the route.

### Filters

The Filters sheet offers one searchable multi-select per categorical column
(`filterable: true` in `countries.ts`) plus Industry (canonical NACE English
labels via `nace_categories`; `industryFacetQuery`/`industryFilterExpr` per
country — Latvia has none because `lv_companies_nace` is unpopulated).
Selected values live in the URL as repeated `f_<key>=` params and are applied
server-side by the loader. Option lists are cached in-process for 24h
(`facets.server.ts`) — typeahead searches the cache (diacritic-insensitive,
prefix-first), never ClickHouse per keystroke.

### Company detail

`/company/{country_code}/{id}` — identity header, overview (all list columns +
industry), and per-country sections declared in `countries.ts` (`detail`):
financials (no, fi, ee, lv, gb, br — canonical yearly metrics, USD chart via
recharts), contacts and domains (no, fi, ee, lv, cz, br). se/sk have no
financial metrics materialized yet (pipeline gap) and fr has no detail data —
those pages show identity/overview only. All section queries bind the id as
`{id:String}` and live in the registry, never in routes.

The Contact & location card shows contacts, addresses (per-country
`addressQuery` in `countries.ts` — Norway's `no_company_addresses` is wired
but awaits its first dagster materialization; Finland has no address data
yet), and a leaflet mini map. Coordinates come from Latvia's stored
lat/long where present; otherwise the address is geocoded server-side via
Nominatim (1 req/s, results — including misses — cached permanently in
`.cache/geocode.sqlite` via node:sqlite).

Fidelity rule: the detail page shows every column of the company row
("Company record" card; lineage fields collapsed under "Source & lineage")
and country-specific sections render full source shapes — Norway shows the
complete Brønnøysund statement (all P&L/balance/filing fields; any column a
future migration adds lands in "Other fields" automatically). Never trim a
country's data to fit a generic UI — add a country component instead
(`app/components/detail/countries/`, wired via COUNTRY_FINANCIALS in the
detail route).

NO/LV record cards join their `*_companies_translated` tables (LEFT JOIN —
never a table switch; the translated tables are missing base columns).
Industries render as a shared section (all rows, canonical NACE English).
Norway statement amounts carry their currency code; USD values shown with a
leading `≈` are derived in the UI as `original × fx_rate_to_usd` where the
pipeline left the stored USD NULL.

## Financials section

Three routes power financial analytics:

- **`/financials`** — Global overview: total revenue by country (bar chart + table),
  top 15 NACE divisions (across all NACE-enabled countries), and top 25 companies.
- **`/financials/country/{code}`** — Country page (no/fi/ee/lv/gb/br/se/sk): total
  companies, revenue, and latest fiscal year; industry breakdown (NACE divisions
  for countries with mapping, or unmapped bucket); top companies for the country.
- **`/financials/industry/{division}`** — Division (2-digit NACE) page: revenue and
  company count by country, top companies across all countries in that industry.

### Sums vs. lists: Norwegian NUF exclusion

Norwegian foreign-branch companies (legal form NUF) file the foreign parent's full
accounts — real corporate data, but not Norway-earned. Revenue sums and company
counts **exclude** NUF rows via `financialsAggregates.sumExclusionExpr`, but **lists**
(top companies table, top divisions) **keep** them badged with `excluded_from_sums: true`
so editors can see the data exists and understand why it's absent from aggregates.

### Unmapped bucket

Companies with financial data but no NACE mapping are explicitly counted as "Unmapped"
and included in country pages (for countries with NACE support). The unmapped count is
computed as total companies minus sum of all mapped divisions; revenue is the residual
(totals minus mapped). Unmapped is never dropped — if a company has financials, it
contributes to the country's total revenue, appearing either in a named division or
in the Unmapped bucket.

### NACE-breakdown countries vs. totals-only

**NACE-breakdown countries** (no/se/ee/gb/sk) have a registry `financialsAggregates.nace`
entry joining their industries table, yielding division (2-digit) breakdowns and feeding
the global industry view:

- **NO** (Norway): `no_industries` + `nace_normalized_code` + primary filter.
- **SE** (Sweden): `se_industries` + `nace_rev2_class_code` (REV2 to current via category fallback).
- **EE** (Estonia): `ee_industries` + `nace_normalized_code` + primary filter.
- **GB** (United Kingdom): `gb_industries` + `nace_normalized_code` + primary filter.
- **SK** (Slovakia): `sk_industries` + `nace_normalized_code` + primary filter.

**Totals-only countries** (fi/lv/br) have `financialsLatest` tables but no nace config:

- **FI** (Finland): TOL2008 source codes exist in `fi_industries`; mapping to
  canonical NACE not yet built. Once added, add a `financialsAggregates.nace` entry
  to unlock industry breakdown (both country page and global industry routes).
- **LV** (Latvia): NACE classifier not yet run; `lv_companies_nace` remains unpopulated.
  Once classifier lands, add `financialsAggregates.nace` to enable breakdowns.
- **BR** (Brazil): CNAE→NACE mapping stub exists (`br_cnae_to_nace` table) for industry
  labels, but no aggregates config. Once aggregates logic is added, instantiate
  `financialsAggregates.nace` to activate divisions (estimated ~100 NACE classes;
  layout fits existing charts).

Each country auto-upgrades when its mapping arrives — simply add the registry entry
in `countries.ts` and the division view activates without UI changes.

### Methodology & caveats

- **Latest filed year per company**: Financial aggregates use each company's most
  recent filing (`max(fiscal_year)` in `financialsLatest` tables), not a snapshot
  year. A company filing FY 2024 in June 2026 contributes at the 2024 rate; another
  filing FY 2023 contributes at its own 2023 rate. `latest_fiscal_year` shown in UI
  is the max across all companies in scope (country/division/global).
- **USD at period-end rates**: Revenue converted via period-end FX rates stored during
  pipeline materialization. Aggregates sum the already-converted USD values; no
  secondary normalization occurs. Rates vary by company filing date — sums are not
  anchored to a single rate.
- **Standalone vs. group accounts**: Some filings include standalone company results
  (and group results separately). Aggregates sum whichever metric is recorded in the
  `revenue_amount_usd` column; no deduplication across group/standalone occurs at this
  layer. Group-level double-counting is possible if a parent and subsidiary both file.

## Rules

- Read-only: `SELECT` only, no writes to ClickHouse.
- User input goes through ClickHouse query params (`{name:String}`), never
  string interpolation. Identifiers may only come from `countries.ts`.
