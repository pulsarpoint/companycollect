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

## Rules

- Read-only: `SELECT` only, no writes to ClickHouse.
- User input goes through ClickHouse query params (`{name:String}`), never
  string interpolation. Identifiers may only come from `countries.ts`.
