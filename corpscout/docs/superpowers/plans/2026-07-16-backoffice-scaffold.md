# Backoffice Explorer Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold a server-side-rendered React Router v7 backoffice app at `corpscout/services/backoffice` that reads company data per country directly from ClickHouse — country picker on `/`, per-country dashboard shell at `/:country` with an overview page and a searchable, paginated companies list.

**Architecture:** React Router v7 framework mode (SSR on by default). All ClickHouse access happens server-side in route loaders through a `.server.ts` module using the official `@clickhouse/client`; the browser never talks to ClickHouse. A static, hand-written country registry (`app/lib/countries.ts`) is the single source of truth for which countries exist and how each one maps onto its ClickHouse tables/columns (schemas differ per country). UI is shadcn/ui (latest, Tailwind CSS v4) with the `dashboard-01` block providing the sidebar/dashboard shell.

**Tech Stack:** React Router v7 (framework mode, SSR), TypeScript, Tailwind CSS v4, shadcn/ui + dashboard-01 block, `@clickhouse/client`, Vitest, pnpm.

## Global Constraints

- App lives at `corpscout/services/backoffice` (all paths below are relative to that unless absolute).
- React Router v7 framework mode with SSR enabled (`ssr: true`, the default) — do not switch to SPA mode.
- shadcn/ui latest with Tailwind CSS v4; dashboard shell comes from the `dashboard-01` block.
- No authentication of any kind for now.
- The app connects **directly to ClickHouse** (HTTP interface, `http://companycollect:8123`, database `corpscout`) and is **strictly read-only** — only `SELECT` statements, ever.
- ClickHouse access only from server code (`*.server.ts` modules and loaders). Never import the ClickHouse client into client-rendered code.
- All user-supplied values go through ClickHouse **query params** (`{name:String}` syntax) — never string-interpolate user input into SQL. Table/column names may be interpolated only from the static country registry.
- Countries are a static object array in code (no countries table lookup).
- Package manager: pnpm. Node.js 20+.
- Path alias `~/*` → `./app/*` (matches the repo's frontend convention).
- Dev server on port **5183** (5173 is taken by pulsarprotectproweb).
- Integration tests run against the real ClickHouse instance (repo convention — no SQL-string-assertion fakes).
- Conventional Commits for every commit.
- URL scheme: `/` (country picker) → `/:country` (dashboard layout) → `/:country` index (overview), `/:country/companies` (list). `:country` is a lowercase ISO2 code.

## Ground truth (verified 2026-07-16 against live ClickHouse)

Countries with a companies table, their key columns, and row counts:

| code | table | id column | name column | active expr | rows |
|------|-------|-----------|-------------|-------------|------|
| no | `no_companies` | `org_number` | `name` | `is_active = 1` | 1.17M |
| fi | `fi_companies` | `business_id` | `name` | `is_active = 1` | 460k |
| se | `se_companies` | `registration_number` | `legal_name` | `status = 'active'` | 4.1M |
| ee | `ee_companies` | `reg_code` | `name` | `is_active = 1` | 373k |
| lv | `lv_companies` | `regcode` | `legal_name` | `is_active = 1` | 485k |
| gb | `gb_companies` | `company_number` | `name` | `is_active = 1` | 5.7M |
| fr | `fr_companies` | `siren` | `name` | `is_active = 1` | 29.7M |
| br | `br_companies` | `cnpj_basico` | `legal_name` | `is_active = 1` | 68.6M |
| cz | `cz_companies` | `ico` | `name` | `is_active = 1` | 3.5M |
| sk | `sk_companies` | `ico` | `name` | `is_active = 1` | 2.2M |

Auxiliary tables vary per country (e.g. `fi_*` has addresses/contacts/tax registrations, `se_*` has financial facts/reports, `br_*` has establishments/CVM filings). The registry records these as `features` flags so future per-country pages can differ; this scaffold only builds Overview + Companies for every country.

Technology data (`commoncrawl_page_technologies`, 794M rows; `commoncrawl_domains`, 119M rows) is domain-keyed, not country-keyed — it is out of scope for this scaffold and will be wired in a later plan.

---

### Task 1: Scaffold the React Router v7 project

**Files:**
- Create: `corpscout/services/backoffice/` (entire tree via generator)
- Modify: `vite.config.ts` (dev port), `tsconfig.json` (verify `~/*` alias), `.gitignore`

**Interfaces:**
- Produces: a running SSR app with `pnpm dev` on port 5183, `pnpm typecheck` green. Route modules live in `app/routes/`, route table in `app/routes.ts`.

- [ ] **Step 1: Generate the project**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services
pnpm dlx create-react-router@latest backoffice --no-git-init --package-manager pnpm --install --yes
```

Expected: a `backoffice/` directory containing `app/`, `react-router.config.ts`, `vite.config.ts`, `package.json`, Tailwind preconfigured.

- [ ] **Step 2: Pin the dev port and confirm the alias**

In `vite.config.ts`, add a `server` block so the full file looks like:

```ts
import { reactRouter } from "@react-router/dev/vite";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  server: { port: 5183 },
  plugins: [tailwindcss(), reactRouter(), tsconfigPaths()],
});
```

(Keep whatever plugin list the generator produced; only add `server: { port: 5183 }` and keep `tsconfigPaths` if present.)

In `tsconfig.json`, confirm `compilerOptions.paths` maps `"~/*": ["./app/*"]`. If the template generated a different alias, change it to `~/*` and update any generated imports.

- [ ] **Step 3: Confirm SSR is on**

`react-router.config.ts` must be:

```ts
import type { Config } from "@react-router/dev/config";

export default {
  ssr: true,
} satisfies Config;
```

- [ ] **Step 4: Verify it runs**

```bash
cd backoffice
pnpm typecheck
pnpm dev
```

Expected: typecheck passes; dev server serves the template welcome page at `http://localhost:5183`. Stop the dev server after checking (curl the page: `curl -s http://localhost:5183 | head -5` should return HTML).

- [ ] **Step 5: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): scaffold react router v7 ssr app"
```

---

### Task 2: shadcn/ui init + dashboard-01 block

**Files:**
- Create: `components.json`, `app/components/ui/*`, `app/components/*` (dashboard-01 components), `app/lib/utils.ts` (all generated by shadcn CLI)
- Modify: `app/app.css` (theme variables added by CLI)

**Interfaces:**
- Produces: shadcn components importable as `~/components/ui/<name>`; dashboard-01 pieces (`app-sidebar.tsx`, `site-header.tsx`, `section-cards.tsx`, `nav-main.tsx`, etc.) in `app/components/`.

- [ ] **Step 1: Initialize shadcn**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice
pnpm dlx shadcn@latest init
```

Pick: base color **neutral**. The CLI detects React Router + Tailwind v4. After it runs, open `components.json` and make sure the aliases use `~`:

```json
{
  "aliases": {
    "components": "~/components",
    "utils": "~/lib/utils",
    "ui": "~/components/ui",
    "lib": "~/lib",
    "hooks": "~/hooks"
  }
}
```

(If the CLI wrote `@/…`, edit `components.json` to the values above and re-check `app/lib/utils.ts` imports.)

- [ ] **Step 2: Add the dashboard-01 block**

```bash
pnpm dlx shadcn@latest add dashboard-01
```

Expected: components like `app/components/app-sidebar.tsx`, `site-header.tsx`, `section-cards.tsx`, `chart-area-interactive.tsx`, `data-table.tsx`, `nav-main.tsx`, `nav-user.tsx`, plus `app/components/ui/*` primitives and a demo `app/routes/dashboard/…` or `app/dashboard/` page with `data.json` (location varies by CLI version — keep whatever it generates for now; Task 6 adapts the shell and Task 9 deletes unused demo files).

- [ ] **Step 3: Verify**

```bash
pnpm typecheck
```

Expected: PASS. If the block generated a demo route that collides with routing, don't wire it into `app/routes.ts` — components compile standalone.

- [ ] **Step 4: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add shadcn with dashboard-01 block"
```

---

### Task 3: ClickHouse server client + Vitest setup

**Files:**
- Create: `app/lib/clickhouse.server.ts`, `.env`, `.env.example`, `vitest.config.ts`, `tests/setup.ts`, `tests/clickhouse.server.test.ts`
- Modify: `package.json` (deps + `test` script), `.gitignore` (ensure `.env` ignored)

**Interfaces:**
- Produces: `chQuery<T>(sql: string, params?: Record<string, unknown>): Promise<T[]>` — runs a SELECT with ClickHouse named query params, returns rows as objects (`JSONEachRow`).

- [ ] **Step 1: Install dependencies**

```bash
pnpm add @clickhouse/client
pnpm add -D vitest dotenv
```

- [ ] **Step 2: Environment files**

`.env.example`:

```bash
# ClickHouse read-only access (HTTP interface). Copy to .env and fill in.
CLICKHOUSE_URL=http://companycollect:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=change-me
CLICKHOUSE_DATABASE=corpscout
```

`.env`: same keys with the real password (copy `CLICKHOUSE_PASSWORD` from `corpscout/.env`).

Ensure `.gitignore` contains `.env` (the template usually has it; add if missing).

- [ ] **Step 3: Vitest config**

`vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [tsconfigPaths()],
  test: {
    setupFiles: ["./tests/setup.ts"],
    include: ["tests/**/*.test.ts", "app/**/*.test.ts"],
  },
});
```

`tests/setup.ts`:

```ts
import "dotenv/config";
```

Add to `package.json` scripts:

```json
"test": "vitest run"
```

- [ ] **Step 4: Write the failing test**

`tests/clickhouse.server.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";

// Integration test: runs against the real ClickHouse instance from .env.
describe("chQuery", () => {
  it("runs a parameterized SELECT and returns typed rows", async () => {
    const rows = await chQuery<{ answer: number }>(
      "SELECT {a:UInt8} + {b:UInt8} AS answer",
      { a: 40, b: 2 },
    );
    expect(rows).toEqual([{ answer: 42 }]);
  });

  it("reads from the corpscout database by default", async () => {
    const rows = await chQuery<{ db: string }>("SELECT currentDatabase() AS db");
    expect(rows).toEqual([{ db: "corpscout" }]);
  });
});
```

- [ ] **Step 5: Run test to verify it fails**

```bash
pnpm test
```

Expected: FAIL — cannot resolve `~/lib/clickhouse.server`.

- [ ] **Step 6: Implement the client module**

`app/lib/clickhouse.server.ts`:

```ts
import { createClient, type ClickHouseClient } from "@clickhouse/client";

let client: ClickHouseClient | undefined;

function getClient(): ClickHouseClient {
  if (!client) {
    client = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username: process.env.CLICKHOUSE_USER ?? "default",
      password: process.env.CLICKHOUSE_PASSWORD ?? "",
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: 30_000,
    });
  }
  return client;
}

/**
 * Runs a read-only SELECT against ClickHouse and returns rows as objects.
 * User-supplied values MUST be passed via `params` (ClickHouse named query
 * params, e.g. `{q:String}` in the SQL) — never interpolated into `sql`.
 */
export async function chQuery<T>(
  sql: string,
  params?: Record<string, unknown>,
): Promise<T[]> {
  const result = await getClient().query({
    query: sql,
    query_params: params,
    format: "JSONEachRow",
  });
  return result.json<T>();
}
```

- [ ] **Step 7: Run test to verify it passes**

```bash
pnpm test
```

Expected: PASS (2 tests).

- [ ] **Step 8: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add server-only clickhouse client with integration tests"
```

---

### Task 4: Static country registry

**Files:**
- Create: `app/lib/countries.ts`, `app/lib/countries.test.ts`

**Interfaces:**
- Produces:
  - `type CountryFeature = "financials" | "industries" | "contacts" | "domains"`
  - `interface CountryConfig { code: string; name: string; flag: string; companiesTable: string; idColumn: string; nameColumn: string; activeExpr: string; approxCompanies: string; features: CountryFeature[] }`
  - `const COUNTRIES: CountryConfig[]`
  - `function getCountry(code: string): CountryConfig | undefined` (case-insensitive)

- [ ] **Step 1: Write the failing test**

`app/lib/countries.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { COUNTRIES, getCountry } from "~/lib/countries";

describe("country registry", () => {
  it("contains all ten countries with unique lowercase ISO2 codes", () => {
    const codes = COUNTRIES.map((c) => c.code);
    expect(codes).toEqual([...new Set(codes)]);
    expect(codes.every((c) => /^[a-z]{2}$/.test(c))).toBe(true);
    expect(codes.sort()).toEqual(
      ["br", "cz", "ee", "fi", "fr", "gb", "lv", "no", "se", "sk"].sort(),
    );
  });

  it("resolves countries case-insensitively", () => {
    expect(getCountry("no")?.name).toBe("Norway");
    expect(getCountry("NO")?.name).toBe("Norway");
    expect(getCountry("xx")).toBeUndefined();
  });

  it("maps Sweden to its status-based active expression", () => {
    const se = getCountry("se");
    expect(se?.companiesTable).toBe("se_companies");
    expect(se?.nameColumn).toBe("legal_name");
    expect(se?.activeExpr).toBe("status = 'active'");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pnpm test countries
```

Expected: FAIL — cannot resolve `~/lib/countries`.

- [ ] **Step 3: Implement the registry**

`app/lib/countries.ts`:

```ts
export type CountryFeature = "financials" | "industries" | "contacts" | "domains";

export interface CountryConfig {
  /** Lowercase ISO2 code, used as the URL segment /:country. */
  code: string;
  name: string;
  flag: string;
  /** ClickHouse table holding the canonical company rows. */
  companiesTable: string;
  /** Column holding the national registry identifier. */
  idColumn: string;
  /** Column holding the display name. */
  nameColumn: string;
  /** SQL boolean expression selecting active companies. */
  activeExpr: string;
  /** Human-readable approximate row count, shown on the picker card. */
  approxCompanies: string;
  /** Which auxiliary data families exist for this country. */
  features: CountryFeature[];
}

export const COUNTRIES: CountryConfig[] = [
  { code: "no", name: "Norway", flag: "🇳🇴", companiesTable: "no_companies", idColumn: "org_number", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "1.2M", features: ["financials", "industries", "contacts", "domains"] },
  { code: "fi", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies", idColumn: "business_id", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "460k", features: ["financials", "industries", "contacts", "domains"] },
  { code: "se", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies", idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'", approxCompanies: "4.1M", features: ["financials", "industries"] },
  { code: "ee", name: "Estonia", flag: "🇪🇪", companiesTable: "ee_companies", idColumn: "reg_code", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "373k", features: ["financials", "industries", "contacts", "domains"] },
  { code: "lv", name: "Latvia", flag: "🇱🇻", companiesTable: "lv_companies", idColumn: "regcode", nameColumn: "legal_name", activeExpr: "is_active = 1", approxCompanies: "485k", features: ["financials", "contacts", "domains"] },
  { code: "gb", name: "United Kingdom", flag: "🇬🇧", companiesTable: "gb_companies", idColumn: "company_number", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "5.7M", features: ["financials", "industries"] },
  { code: "fr", name: "France", flag: "🇫🇷", companiesTable: "fr_companies", idColumn: "siren", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "29.7M", features: ["industries"] },
  { code: "br", name: "Brazil", flag: "🇧🇷", companiesTable: "br_companies", idColumn: "cnpj_basico", nameColumn: "legal_name", activeExpr: "is_active = 1", approxCompanies: "68.6M", features: ["financials", "contacts", "domains"] },
  { code: "cz", name: "Czechia", flag: "🇨🇿", companiesTable: "cz_companies", idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "3.5M", features: ["industries", "contacts", "domains"] },
  { code: "sk", name: "Slovakia", flag: "🇸🇰", companiesTable: "sk_companies", idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "2.2M", features: ["financials", "industries"] },
];

export function getCountry(code: string): CountryConfig | undefined {
  const normalized = code.toLowerCase();
  return COUNTRIES.find((c) => c.code === normalized);
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pnpm test countries
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add static country registry"
```

---

### Task 5: Country query functions

**Files:**
- Create: `app/lib/queries.server.ts`, `tests/queries.server.test.ts`

**Interfaces:**
- Consumes: `chQuery` (Task 3), `CountryConfig` (Task 4).
- Produces:
  - `interface CountryStats { total: number; active: number }`
  - `interface CompanyRow { id: string; name: string; active: 0 | 1 }`
  - `interface CompanySearchResult { rows: CompanyRow[]; total: number; page: number; pageSize: number }`
  - `getCountryStats(country: CountryConfig): Promise<CountryStats>`
  - `searchCompanies(country: CountryConfig, opts: { q?: string; page?: number; pageSize?: number }): Promise<CompanySearchResult>`

- [ ] **Step 1: Write the failing integration test**

`tests/queries.server.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { getCountryStats, searchCompanies } from "~/lib/queries.server";

// Integration tests against the real ClickHouse. Estonia is the smallest
// dataset (~373k rows), so queries stay fast.
const ee = getCountry("ee")!;

describe("getCountryStats", () => {
  it("returns positive totals with active <= total", async () => {
    const stats = await getCountryStats(ee);
    expect(stats.total).toBeGreaterThan(100_000);
    expect(stats.active).toBeGreaterThan(0);
    expect(stats.active).toBeLessThanOrEqual(stats.total);
  });
});

describe("searchCompanies", () => {
  it("returns a first page of rows with id and name", async () => {
    const result = await searchCompanies(ee, { page: 1, pageSize: 10 });
    expect(result.rows).toHaveLength(10);
    expect(result.total).toBeGreaterThan(100_000);
    for (const row of result.rows) {
      expect(row.id).toBeTruthy();
      expect(row.name).toBeTruthy();
    }
  });

  it("filters by case-insensitive name substring", async () => {
    const result = await searchCompanies(ee, { q: "grupp", pageSize: 10 });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.total).toBeLessThan(400_000);
    for (const row of result.rows) {
      expect(row.name.toLowerCase()).toContain("grupp");
    }
  });

  it("paginates without overlap", async () => {
    const p1 = await searchCompanies(ee, { page: 1, pageSize: 5 });
    const p2 = await searchCompanies(ee, { page: 2, pageSize: 5 });
    const ids1 = new Set(p1.rows.map((r) => r.id));
    expect(p2.rows.some((r) => ids1.has(r.id))).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pnpm test queries
```

Expected: FAIL — cannot resolve `~/lib/queries.server`.

- [ ] **Step 3: Implement the query functions**

`app/lib/queries.server.ts`:

```ts
import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

export interface CountryStats {
  total: number;
  active: number;
}

export interface CompanyRow {
  id: string;
  name: string;
  active: 0 | 1;
}

export interface CompanySearchResult {
  rows: CompanyRow[];
  total: number;
  page: number;
  pageSize: number;
}

const MAX_PAGE_SIZE = 100;

export async function getCountryStats(country: CountryConfig): Promise<CountryStats> {
  // Table/column identifiers come from the static registry, never from users.
  const rows = await chQuery<{ total: string; active: string }>(
    `SELECT count() AS total, countIf(${country.activeExpr}) AS active
     FROM ${country.companiesTable}`,
  );
  const row = rows[0];
  return { total: Number(row.total), active: Number(row.active) };
}

export async function searchCompanies(
  country: CountryConfig,
  opts: { q?: string; page?: number; pageSize?: number },
): Promise<CompanySearchResult> {
  const page = Math.max(1, Math.trunc(opts.page ?? 1));
  const pageSize = Math.min(MAX_PAGE_SIZE, Math.max(1, Math.trunc(opts.pageSize ?? 50)));
  const q = (opts.q ?? "").trim();

  const where = q ? `WHERE ${country.nameColumn} ILIKE {pattern:String}` : "";
  const params = q ? { pattern: `%${q}%` } : undefined;

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );

  const rows = await chQuery<CompanyRow>(
    `SELECT
       ${country.idColumn} AS id,
       ${country.nameColumn} AS name,
       toUInt8(${country.activeExpr}) AS active
     FROM ${country.companiesTable}
     ${where}
     ORDER BY ${country.nameColumn}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  return { rows, total: Number(countRows[0].total), page, pageSize };
}
```

Note: ClickHouse returns `count()` as a JSON string (UInt64) — hence the `Number(...)` conversions. `LIMIT`/`OFFSET` are computed from clamped integers, safe to inline.

- [ ] **Step 4: Run test to verify it passes**

```bash
pnpm test queries
```

Expected: PASS (4 tests). If the `grupp` search assertion fails because Estonia's data changed, pick another common substring (`"ehitus"`) — keep the shape of the assertions.

- [ ] **Step 5: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add country stats and company search queries"
```

---

### Task 6: Route table + home page (country picker)

**Files:**
- Create: `app/routes/home.tsx`
- Modify: `app/routes.ts`, `app/root.tsx` (title only, if the template put branding there)
- Delete: the template's welcome route (e.g. `app/routes/home.tsx` template content and `app/welcome/` folder if generated)

**Interfaces:**
- Consumes: `COUNTRIES` (Task 4), shadcn `Card` components (Task 2).
- Produces: route table used by Tasks 7–8: `/` → `routes/home.tsx`, `/:country` layout → `routes/country.tsx` with children `routes/country-overview.tsx` (index) and `routes/country-companies.tsx` (`companies`).

- [ ] **Step 1: Define the route table**

`app/routes.ts`:

```ts
import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route(":country", "routes/country.tsx", [
    index("routes/country-overview.tsx"),
    route("companies", "routes/country-companies.tsx"),
  ]),
] satisfies RouteConfig;
```

(Tasks 7–8 create `country.tsx`, `country-overview.tsx`, `country-companies.tsx`; typecheck will fail until then, so within THIS task keep only `index("routes/home.tsx")` and add the `:country` subtree in Task 7. Concretely, at the end of this task `app/routes.ts` is:)

```ts
import { type RouteConfig, index } from "@react-router/dev/routes";

export default [index("routes/home.tsx")] satisfies RouteConfig;
```

- [ ] **Step 2: Ensure the card component exists**

```bash
pnpm dlx shadcn@latest add card badge
```

(No-op if dashboard-01 already installed them.)

- [ ] **Step 3: Implement the home page**

Replace `app/routes/home.tsx` entirely:

```tsx
import { Link } from "react-router";
import type { Route } from "./+types/home";
import { COUNTRIES } from "~/lib/countries";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export function meta(_: Route.MetaArgs) {
  return [{ title: "CompanyCollect Backoffice" }];
}

export default function Home() {
  return (
    <main className="mx-auto max-w-5xl px-6 py-12">
      <h1 className="text-3xl font-semibold tracking-tight">
        CompanyCollect Backoffice
      </h1>
      <p className="text-muted-foreground mt-2">
        Select a country to explore its company data.
      </p>
      <div className="mt-8 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {COUNTRIES.map((country) => (
          <Link key={country.code} to={`/${country.code}`}>
            <Card className="hover:bg-accent/50 h-full transition-colors">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className="text-2xl">{country.flag}</span>
                  {country.name}
                </CardTitle>
                <CardDescription>
                  ~{country.approxCompanies} companies
                </CardDescription>
                <div className="mt-2 flex flex-wrap gap-1">
                  {country.features.map((f) => (
                    <Badge key={f} variant="secondary">
                      {f}
                    </Badge>
                  ))}
                </div>
              </CardHeader>
            </Card>
          </Link>
        ))}
      </div>
    </main>
  );
}
```

Delete the template's `app/welcome/` directory (and any unused template assets) if it exists.

- [ ] **Step 4: Verify**

```bash
pnpm typecheck && pnpm dev
```

Expected: `http://localhost:5183/` renders a grid of 10 country cards. `curl -s http://localhost:5183/ | grep -c 'Norway'` returns at least 1 (SSR — the name is in the initial HTML). Stop the dev server.

- [ ] **Step 5: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add country picker home page"
```

---

### Task 7: Country layout route with dashboard sidebar

**Files:**
- Create: `app/routes/country.tsx`, `app/routes/country-overview.tsx` (stub — fleshed out in this task), `app/components/country-sidebar.tsx`
- Modify: `app/routes.ts` (add the `:country` subtree)

**Interfaces:**
- Consumes: `getCountry` (Task 4), `getCountryStats` (Task 5), dashboard-01 components (Task 2: `~/components/ui/sidebar`, `~/components/ui/card`).
- Produces: layout loader returning `{ country: CountryConfig }`; child routes access it via `useRouteLoaderData("routes/country")`. Overview page at `/:country`.

- [ ] **Step 1: Extend the route table**

`app/routes.ts`:

```ts
import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route(":country", "routes/country.tsx", [
    index("routes/country-overview.tsx"),
    route("companies", "routes/country-companies.tsx"),
  ]),
] satisfies RouteConfig;
```

Create a placeholder `app/routes/country-companies.tsx` so typecheck passes until Task 8 (this placeholder is REPLACED in Task 8):

```tsx
export default function CountryCompanies() {
  return null;
}
```

- [ ] **Step 2: Country sidebar component**

`app/components/country-sidebar.tsx` — a trimmed adaptation of dashboard-01's `app-sidebar.tsx` (no user menu, no auth):

```tsx
import { Link, NavLink } from "react-router";
import { Building2, LayoutDashboard } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "~/components/ui/sidebar";

export function CountrySidebar({ country }: { country: CountryConfig }) {
  const items = [
    { title: "Overview", to: `/${country.code}`, icon: LayoutDashboard, end: true },
    { title: "Companies", to: `/${country.code}/companies`, icon: Building2, end: false },
  ];

  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild className="h-auto">
              <Link to="/">
                <span className="text-xl">{country.flag}</span>
                <span className="font-semibold">{country.name}</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {items.map((item) => (
                <SidebarMenuItem key={item.to}>
                  <NavLink to={item.to} end={item.end}>
                    {({ isActive }) => (
                      <SidebarMenuButton isActive={isActive}>
                        <item.icon />
                        <span>{item.title}</span>
                      </SidebarMenuButton>
                    )}
                  </NavLink>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
```

- [ ] **Step 3: Layout route with param validation**

`app/routes/country.tsx`:

```tsx
import { data, Outlet } from "react-router";
import type { Route } from "./+types/country";
import { getCountry } from "~/lib/countries";
import { CountrySidebar } from "~/components/country-sidebar";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "~/components/ui/sidebar";
import { Separator } from "~/components/ui/separator";

export function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) {
    throw data(`Unknown country: ${params.country}`, { status: 404 });
  }
  return { country };
}

export default function CountryLayout({ loaderData }: Route.ComponentProps) {
  const { country } = loaderData;
  return (
    <SidebarProvider>
      <CountrySidebar country={country} />
      <SidebarInset>
        <header className="flex h-12 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mx-2 h-4" />
          <span className="text-sm font-medium">{country.name}</span>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4 md:p-6">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
```

- [ ] **Step 4: Overview page with stats cards**

`app/routes/country-overview.tsx`:

```tsx
import { useRouteLoaderData } from "react-router";
import type { Route } from "./+types/country-overview";
import type { loader as countryLoader } from "./country";
import { getCountry } from "~/lib/countries";
import { getCountryStats } from "~/lib/queries.server";
import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  return { stats: await getCountryStats(country) };
}

const nf = new Intl.NumberFormat("en-US");

export default function CountryOverview({ loaderData }: Route.ComponentProps) {
  const { stats } = loaderData;
  const parent = useRouteLoaderData<typeof countryLoader>("routes/country");
  const tiles = [
    { label: "Total companies", value: stats.total },
    { label: "Active", value: stats.active },
    { label: "Inactive", value: stats.total - stats.active },
  ];
  return (
    <>
      <h2 className="text-xl font-semibold">
        {parent?.country.name} overview
      </h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {tiles.map((tile) => (
          <Card key={tile.label}>
            <CardHeader>
              <CardDescription>{tile.label}</CardDescription>
              <CardTitle className="text-3xl tabular-nums">
                {nf.format(tile.value)}
              </CardTitle>
            </CardHeader>
          </Card>
        ))}
      </div>
    </>
  );
}
```

- [ ] **Step 5: Verify**

```bash
pnpm typecheck && pnpm test && pnpm dev
```

Expected: typecheck + tests PASS. In the browser: `http://localhost:5183/ee` shows the sidebar (Estonia header, Overview/Companies nav) and three stat cards with real numbers (~373k total). `http://localhost:5183/xx` returns a 404. `curl -s -o /dev/null -w '%{http_code}' http://localhost:5183/xx` → `404`. Stop the dev server.

- [ ] **Step 6: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add country layout with sidebar and overview stats"
```

---

### Task 8: Companies page — search + server-side pagination

**Files:**
- Modify: `app/routes/country-companies.tsx` (replace the Task 7 placeholder entirely)

**Interfaces:**
- Consumes: `searchCompanies` (Task 5), `getCountry` (Task 4), shadcn `Table`, `Input`, `Button`, `Badge` components.
- Produces: `/:country/companies?q=<search>&page=<n>` — SSR'd, URL-driven state (search + pagination survive reload/share).

- [ ] **Step 1: Ensure table/input components exist**

```bash
pnpm dlx shadcn@latest add table input button
```

(No-op for components dashboard-01 already installed.)

- [ ] **Step 2: Implement the page**

Replace `app/routes/country-companies.tsx` entirely:

```tsx
import { Form, Link, useSearchParams } from "react-router";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { searchCompanies } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const PAGE_SIZE = 50;

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const q = url.searchParams.get("q") ?? "";
  const page = Number(url.searchParams.get("page") ?? "1") || 1;

  const result = await searchCompanies(country, { q, page, pageSize: PAGE_SIZE });
  return { q, result };
}

const nf = new Intl.NumberFormat("en-US");

export default function CountryCompanies({ loaderData, params }: Route.ComponentProps) {
  const { q, result } = loaderData;
  const [searchParams] = useSearchParams();
  const lastPage = Math.max(1, Math.ceil(result.total / result.pageSize));

  function pageLink(page: number) {
    const next = new URLSearchParams(searchParams);
    next.set("page", String(page));
    return `?${next.toString()}`;
  }

  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Companies</h2>
        <Form method="get" className="flex gap-2">
          <Input
            type="search"
            name="q"
            defaultValue={q}
            placeholder="Search by name…"
            className="w-64"
          />
          <Button type="submit" variant="secondary">
            Search
          </Button>
        </Form>
      </div>

      <p className="text-muted-foreground text-sm">
        {nf.format(result.total)} companies
        {q ? ` matching “${q}”` : ""}
      </p>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-48">Registry ID</TableHead>
              <TableHead>Name</TableHead>
              <TableHead className="w-24">Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={3} className="text-muted-foreground h-24 text-center">
                  No companies found.
                </TableCell>
              </TableRow>
            ) : (
              result.rows.map((row) => (
                <TableRow key={`${row.id}-${row.name}`}>
                  <TableCell className="font-mono text-xs">{row.id}</TableCell>
                  <TableCell>{row.name}</TableCell>
                  <TableCell>
                    <Badge variant={row.active ? "default" : "outline"}>
                      {row.active ? "active" : "inactive"}
                    </Badge>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between">
        <span className="text-muted-foreground text-sm">
          Page {result.page} of {nf.format(lastPage)}
        </span>
        <div className="flex gap-2">
          {result.page <= 1 ? (
            <Button variant="outline" size="sm" disabled>
              Previous
            </Button>
          ) : (
            <Button asChild variant="outline" size="sm">
              <Link to={pageLink(result.page - 1)}>Previous</Link>
            </Button>
          )}
          {result.page >= lastPage ? (
            <Button variant="outline" size="sm" disabled>
              Next
            </Button>
          ) : (
            <Button asChild variant="outline" size="sm">
              <Link to={pageLink(result.page + 1)}>Next</Link>
            </Button>
          )}
        </div>
      </div>
    </>
  );
}
```

Note: `params` is available in `Route.ComponentProps` but unused here — remove it from the destructuring if the linter complains.

- [ ] **Step 3: Verify**

```bash
pnpm typecheck && pnpm dev
```

Expected in the browser (use Estonia — smallest dataset):
- `http://localhost:5183/ee/companies` — 50 rows, total ≈ 373k, pagination controls.
- Search `grupp` → filtered rows, total drops, page resets via the form (form GET drops the `page` param).
- `Next` → page 2, different rows, URL is `?page=2`.
- `curl -s 'http://localhost:5183/ee/companies?q=grupp' | grep -ci grupp` ≥ 1 (SSR).

Stop the dev server.

- [ ] **Step 4: Commit**

```bash
git add services/backoffice
git commit -m "feat(backoffice): add searchable paginated companies page"
```

---

### Task 9: Cleanup, README, final verification

**Files:**
- Delete: unused dashboard-01 demo files (e.g. `app/dashboard/` or demo route + `data.json`, `chart-area-interactive.tsx`, `data-table.tsx`, `nav-user.tsx`, `nav-documents.tsx`, `nav-secondary.tsx` — delete only files nothing imports; keep `~/components/ui/*`)
- Create: `README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: clean tree, documented setup.

- [ ] **Step 1: Remove dead demo files**

For each dashboard-01 demo file, check imports before deleting:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice
rg -l 'chart-area-interactive|nav-user|nav-documents|nav-secondary|data-table' app/
```

Delete any listed component that only appears in other demo files, then delete the demo page/`data.json`. Re-run `rg` until nothing outside the deleted set references them.

- [ ] **Step 2: Write the README**

`README.md`:

```markdown
# backoffice

Internal explorer for CompanyCollect data. React Router v7 (SSR) + shadcn/ui,
reading directly from ClickHouse (read-only).

## Setup

```bash
pnpm install
cp .env.example .env   # fill CLICKHOUSE_PASSWORD from corpscout/.env
pnpm dev               # http://localhost:5183
```

## Commands

- `pnpm dev` — dev server (port 5183)
- `pnpm build` / `pnpm start` — production build / serve
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
```

- [ ] **Step 3: Full verification**

```bash
pnpm typecheck && pnpm test && pnpm build
```

Expected: all PASS, build succeeds. Then `pnpm start` and spot-check `http://localhost:3000/` (react-router-serve default port) renders the picker; stop it.

- [ ] **Step 4: Commit**

```bash
git add services/backoffice
git commit -m "chore(backoffice): remove demo files and add README"
```

---

## Out of scope (future plans)

- Technology exploration pages (`commoncrawl_page_technologies` / `commoncrawl_domains` are domain-keyed; needs the company↔domain join design).
- Per-country extra pages (financials, industries, contacts, domains) — the `features` flags in the registry are the hook for these.
- Company detail page (`/:country/companies/:id`).
- Auth, deployment (ansible/systemd like the other services).
