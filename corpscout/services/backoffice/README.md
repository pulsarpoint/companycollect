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

## Companies table

URL-driven state on `/{country}/companies`:
`?q=` name search, `?sort=` column key + `?dir=asc|desc` (whitelisted against
`countries.ts` column config; unknown values fall back to name asc),
`?page=`, `?pageSize=25|50|100`. The industry column is populated by a second
per-page lookup (`industryQuery` in `countries.ts`) and is not sortable by
design — sorting happens on base-table columns only so the 30–70M-row
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

`/{country}/companies/{id}` — identity header, overview (all list columns +
industry), and per-country sections declared in `countries.ts` (`detail`):
financials (no, fi, ee, lv, gb, br — canonical yearly metrics, USD chart via
recharts), contacts and domains (no, fi, ee, lv, cz, br). se/sk have no
financial metrics materialized yet (pipeline gap) and fr has no detail data —
those pages show identity/overview only. All section queries bind the id as
`{id:String}` and live in the registry, never in routes.

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

## Rules

- Read-only: `SELECT` only, no writes to ClickHouse.
- User input goes through ClickHouse query params (`{name:String}`), never
  string interpolation. Identifiers may only come from `countries.ts`.
