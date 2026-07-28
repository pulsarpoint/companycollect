# backoffice — agent guide

React Router 8 (framework mode) + TypeScript + shadcn/ui + TanStack Table, reading ClickHouse via
`app/lib/clickhouse.server.ts`. Commands from this directory: `npm run typecheck`, `npx vitest run`,
`npm run dev`.

## Company identity

**A bare `company_id` is not unique — always match and join on `(country_code, company_id)`.**
National org-number formats collide across registers: a Czech IČO has been observed equalling a
Brazilian id, and Danish CVR / Finnish business ids share digit-lengths with Swedish org numbers.
`companies_all` is keyed by the composite pair and company pages live at `/company/:country/:id`.

- Cross-register matching goes through `matchCompanies` (all register hits per id) +
  `pickCompanyMatch` (`app/lib/company-match.ts`), which links only when the row's country selects
  exactly one candidate, or a single unambiguous candidate exists. Never link on a bare-id hit.
- Buyers in a single-country register may fall back to the register's own country (they are that
  country's authorities); winners/suppliers may not (awards go to foreign companies).
- If a single-string global id is ever needed at an edge (exports, external APIs), derive it
  (`SE:5560125220` style) at serialization — do not migrate stored ids.

## Route modules and `.server` files

Route components must not use values from `~/lib/*.server` modules in the component body — React
Router refuses server-only modules in the client bundle and the page's hydration breaks. Pure
helpers used by components live in client-safe modules (`~/lib/procurement-paths`,
`~/lib/company-match`, `~/lib/money`); `import type` from a `.server` module is fine.
