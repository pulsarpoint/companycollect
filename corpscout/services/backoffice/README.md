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

## Rules

- Read-only: `SELECT` only, no writes to ClickHouse.
- User input goes through ClickHouse query params (`{name:String}`), never
  string interpolation. Identifiers may only come from `countries.ts`.
