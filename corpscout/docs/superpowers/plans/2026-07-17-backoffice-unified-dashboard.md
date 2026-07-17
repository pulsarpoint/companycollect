# Backoffice Unified Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the app into a standard dashboard: one app-wide sidebar (menu item: Companies), `/companies` listing ALL companies across the 10 countries (country becomes a filter alongside the existing facets), a 3-column table (name, industry, country), and details at `/company/{country_code}/{id}`.

**Architecture:** A unified query layer (`unified.server.ts`) builds a per-country UNION ALL from the existing registry: each included branch selects `country_code/id/name/active` with its own exprs, applies the SAME `f_<key>` filters via its own column exprs (a branch is included only if it can answer every active filter key — e.g. `f_size` restricts to BR), does per-branch top-N, and an outer ORDER/LIMIT merges. Default sort is registry order (`country, id` — measured 0.26s over 116.3M rows); name sort is offered but expensive (measured ~9.5s — accepted for a backoffice, and the logged dagster follow-up `companies_all` materialized table is the real fix). Facet options merge the existing per-country caches (counts summed by value); `country` is itself a facet with live counts. Industry stays a post-pagination per-country merge. The old per-country pages (picker, country layout/overview, per-country companies) are REMOVED; the detail page moves to `/company/:country/:id` unchanged internally.

**Tech Stack:** existing stack only. Reuses `DataTable`/`DataTablePagination`/`DataTableColumnHeader`/`FacetCombobox` internals, `facets.server` caches, url helpers.

## Global Constraints

- App: `corpscout/services/backoffice`. RR8 SSR, Base UI shadcn (`render` prop + `nativeButton={false}`), `chQuery`, registry-only SQL identifiers, `{param}` binding for all user values, read-only ClickHouse.
- URL scheme: `/` redirects to `/companies`; list state stays `?q=&page=&pageSize=&sort=&dir=&f_<key>=` (repeated params); detail = `/company/{country_code}/{id}` (id URL-encoded). Old routes are deleted (no redirects — internal tool).
- Unified sorts: `country` (default, asc) and `name` only; industry NOT sortable. `f_country` values are whitelisted against registry codes; a branch is included only if it can answer every active filter key (`filterable` column with that key, or `industryFilterExpr` for `industry`).
- Page clamp: `min(requested, lastPage, 400)` — the per-branch `LIMIT page*pageSize` merge bound caps at 40k rows/branch; totals unaffected.
- Empties-last sorting preserved (branch and outer level) exactly as the per-country list had.
- Table columns exactly: Name (links to detail), Industry (code+label, unsortable), Country (flag + name, sortable). Nothing else for now.
- Sidebar: dashboard shell at the root layout; ONE menu item "Companies" (active-state aware); header with SidebarTrigger. All pages render inside it.
- Per-country query layer (`searchCompanies`, per-country facets) REMAINS — it powers the registry test sweeps and the detail page; only its routes/UI are deleted.
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths. Dev server 5183 is USER-OWNED — never kill/restart; verify via HMR.

## Ground truth (measured live, 2026-07-17)

- Total companies across the 10 tables: **116,332,198**.
- Default-order union (per-branch `ORDER BY idColumn LIMIT 50`, outer merge): **0.26s**.
- Name-sorted union (per-branch name top-N): **8.7–9.5s** cold AND warm (sort CPU, not cache) — accepted as the explicit-sort cost; `companies_all` materialized table = dagster follow-up.
- Facet keys across the registry: `status` (10 countries), `legal_form` (8 — FI's was unflagged as empty, BR has none), `size` (BR), `place` (7), `industry` (9, not LV). Plus the new `country` facet.

---

### Task 1: Unified query layer + filters

**Files:**
- Create: `app/lib/unified.server.ts`
- Modify: `app/lib/filters.ts` (unified keys + parser)
- Modify: `app/lib/filters.test.ts`
- Create: `tests/unified.server.test.ts`

**Interfaces:**
- Produces (Tasks 3–4 rely on):

```ts
// filters.ts additions (isomorphic)
export const UNIFIED_FACET_KEYS: string[];      // ["country", ...union of filterable column keys..., "industry"]
export const UNIFIED_FACET_LABELS: Record<string, string>; // country→"Country", status→"Status", legal_form→"Legal form", place→"Place", size→"Size", industry→"Industry"
export function parseUnifiedFilters(searchParams: URLSearchParams): CompanyFilters; // f_<key> for UNIFIED_FACET_KEYS; country values whitelisted to registry codes; trim/dedupe/cap 50

// unified.server.ts
export interface UnifiedRow {
  country_code: string; id: string; name: string; active: 0 | 1;
  industry_code?: string | null; industry_label?: string | null;
}
export interface UnifiedSearchResult {
  rows: UnifiedRow[]; total: number; page: number; pageSize: number; sort: string; dir: SortDir;
}
export async function searchUnifiedCompanies(opts: {
  q?: string; page?: number; pageSize?: number; sort?: string | null; dir?: string | null; filters?: CompanyFilters;
}): Promise<UnifiedSearchResult>;
export async function getUnifiedFacetOptions(facetKey: string): Promise<FacetOption[]>; // throws Error("unknown facet: ...") on bad key
export async function searchUnifiedFacetOptions(facetKey: string, q: string): Promise<FacetOption[]>; // '' → top 200, typed → top 50
```

- [ ] **Step 1: Failing unit tests (filters)**

Append to `app/lib/filters.test.ts`:

```ts
import { parseUnifiedFilters, UNIFIED_FACET_KEYS, UNIFIED_FACET_LABELS } from "~/lib/filters";

describe("unified filters", () => {
  it("exposes country first and industry last among the keys", () => {
    expect(UNIFIED_FACET_KEYS[0]).toBe("country");
    expect(UNIFIED_FACET_KEYS).toContain("status");
    expect(UNIFIED_FACET_KEYS).toContain("industry");
    expect(Object.keys(UNIFIED_FACET_LABELS)).toEqual(expect.arrayContaining(UNIFIED_FACET_KEYS));
  });

  it("whitelists country values against the registry", () => {
    const sp = new URLSearchParams("f_country=ee&f_country=xx&f_country=no&f_status=A");
    expect(parseUnifiedFilters(sp)).toEqual({ country: ["ee", "no"], status: ["A"] });
  });

  it("ignores unknown filter keys", () => {
    const sp = new URLSearchParams("f_bogus=1&f_industry=6201");
    expect(parseUnifiedFilters(sp)).toEqual({ industry: ["6201"] });
  });
});
```

Run `pnpm test filters` — FAIL.

- [ ] **Step 2: Implement the filters additions**

Append to `app/lib/filters.ts`:

```ts
import { COUNTRIES } from "~/lib/countries";

const COLUMN_FACET_KEYS = [
  ...new Set(COUNTRIES.flatMap((c) => c.columns.filter((col) => col.filterable).map((col) => col.key))),
];
export const UNIFIED_FACET_KEYS = ["country", ...COLUMN_FACET_KEYS, "industry"];

export const UNIFIED_FACET_LABELS: Record<string, string> = {
  country: "Country",
  status: "Status",
  legal_form: "Legal form",
  place: "Place",
  size: "Size",
  industry: "Industry",
};

const COUNTRY_CODES = new Set(COUNTRIES.map((c) => c.code));

export function parseUnifiedFilters(searchParams: URLSearchParams): CompanyFilters {
  const filters: CompanyFilters = {};
  for (const key of UNIFIED_FACET_KEYS) {
    let values = [
      ...new Set(
        searchParams.getAll(`${FILTER_PREFIX}${key}`).map((v) => v.trim()).filter((v) => v !== ""),
      ),
    ].slice(0, 50);
    if (key === "country") values = values.filter((v) => COUNTRY_CODES.has(v));
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}
```

Run `pnpm test filters` → PASS.

- [ ] **Step 3: Failing integration tests (unified search + facets)**

`tests/unified.server.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { getUnifiedFacetOptions, searchUnifiedCompanies, searchUnifiedFacetOptions } from "~/lib/unified.server";
import { getFacetOptions } from "~/lib/facets.server";
import { getCountry } from "~/lib/countries";

describe("searchUnifiedCompanies", () => {
  it("default page: 50 rows, registry order, total spans all countries", async () => {
    const result = await searchUnifiedCompanies({});
    expect(result.rows).toHaveLength(50);
    expect(result.sort).toBe("country");
    expect(result.total).toBeGreaterThan(100_000_000);
    expect(result.rows[0].country_code).toBe("br"); // 'br' sorts first, 68.6M rows
    for (const row of result.rows) {
      expect(row).toHaveProperty("industry_code");
      expect(row).not.toHaveProperty("__ik");
    }
  }, 30_000);

  it("country filter restricts branches", async () => {
    const result = await searchUnifiedCompanies({ filters: { country: ["ee"] } });
    expect(result.rows.every((r) => r.country_code === "ee")).toBe(true);
    expect(result.total).toBeGreaterThan(300_000);
    expect(result.total).toBeLessThan(500_000);
  }, 30_000);

  it("capability exclusion: a size filter restricts to brazil implicitly", async () => {
    const sizes = await getFacetOptions(getCountry("br")!, "size");
    const result = await searchUnifiedCompanies({ filters: { size: [sizes[0].value] }, pageSize: 25 });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((r) => r.country_code === "br")).toBe(true);
  }, 60_000);

  it("column filter + country filter compose", async () => {
    const statuses = await getFacetOptions(getCountry("ee")!, "status");
    const result = await searchUnifiedCompanies({
      filters: { country: ["ee"], status: [statuses[0].value] },
      pageSize: 25,
    });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((r) => r.country_code === "ee")).toBe(true);
    expect(result.total).toBeLessThan(400_000);
  }, 30_000);

  it("q search hits across countries", async () => {
    const result = await searchUnifiedCompanies({ q: "petrobras", pageSize: 25 });
    expect(result.total).toBeGreaterThan(0);
    expect(result.rows.some((r) => r.country_code === "br")).toBe(true);
  }, 60_000);

  it("no matching branch returns empty", async () => {
    // size is BR-only; country ee cannot answer it
    const result = await searchUnifiedCompanies({ filters: { country: ["ee"], size: ["x"] } });
    expect(result.rows).toEqual([]);
    expect(result.total).toBe(0);
  });
});

describe("unified facets", () => {
  it("country facet lists all 10 with live counts", async () => {
    const options = await getUnifiedFacetOptions("country");
    expect(options).toHaveLength(10);
    const br = options.find((o) => o.value === "br");
    expect(br!.count).toBeGreaterThan(60_000_000);
    expect(br!.label).toBe("Brazil");
  }, 30_000);

  it("merged status options sum counts across countries", async () => {
    const options = await getUnifiedFacetOptions("status");
    expect(options.length).toBeGreaterThan(0);
    const counts = options.map((o) => o.count);
    expect([...counts].sort((a, b) => b - a)).toEqual(counts);
  }, 60_000);

  it("typeahead ranks merged options", async () => {
    const options = await searchUnifiedFacetOptions("country", "esto");
    expect(options[0]?.value).toBe("ee");
  }, 30_000);

  it("rejects unknown facet keys", async () => {
    await expect(getUnifiedFacetOptions("name")).rejects.toThrow(/unknown facet/i);
  });
});
```

Run `pnpm test unified` — FAIL (module not found).

- [ ] **Step 4: Implement `app/lib/unified.server.ts`**

```ts
import { chQuery } from "~/lib/clickhouse.server";
import {
  COUNTRIES,
  getCountry,
  PAGE_SIZES,
  type CountryConfig,
  type SortDir,
} from "~/lib/countries";
import { UNIFIED_FACET_KEYS, type CompanyFilters } from "~/lib/filters";
import { getFacetOptions, rankFacetOptions, type FacetOption } from "~/lib/facets.server";

export interface UnifiedRow {
  country_code: string;
  id: string;
  name: string;
  active: 0 | 1;
  industry_code?: string | null;
  industry_label?: string | null;
}

export interface UnifiedSearchResult {
  rows: UnifiedRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
}

const UNIFIED_SORTS = new Set(["country", "name"]);
/** Per-branch merge bound: each branch returns at most page*pageSize rows. */
const MAX_UNIFIED_PAGE = 400;

function canAnswer(c: CountryConfig, key: string): boolean {
  if (key === "industry") return Boolean(c.industryFilterExpr);
  return c.columns.some((col) => col.filterable && col.key === key);
}

function branchCountries(filters: CompanyFilters): CountryConfig[] {
  let list = COUNTRIES;
  const wanted = filters.country;
  if (wanted?.length) {
    const set = new Set(wanted);
    list = list.filter((c) => set.has(c.code));
  }
  const activeKeys = Object.keys(filters).filter(
    (k) => k !== "country" && (filters[k]?.length ?? 0) > 0,
  );
  return list.filter((c) => activeKeys.every((k) => canAnswer(c, k)));
}

function branchWhere(
  c: CountryConfig,
  q: string,
  filters: CompanyFilters,
  params: Record<string, unknown>,
): string {
  const conds: string[] = [];
  if (q) {
    conds.push(`${c.nameColumn} ILIKE {pattern:String}`);
    params.pattern = `%${q}%`;
  }
  for (const col of c.columns) {
    if (!col.filterable) continue;
    const values = filters[col.key];
    if (!values || values.length === 0) continue;
    conds.push(`${col.expr} IN {f_${col.key}:Array(String)}`);
    params[`f_${col.key}`] = values;
  }
  const industry = filters.industry;
  if (industry?.length && c.industryFilterExpr) {
    conds.push(c.industryFilterExpr);
    params.f_industry = industry;
  }
  return conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
}

export async function searchUnifiedCompanies(opts: {
  q?: string;
  page?: number;
  pageSize?: number;
  sort?: string | null;
  dir?: string | null;
  filters?: CompanyFilters;
}): Promise<UnifiedSearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const q = (opts.q ?? "").trim();
  const filters = opts.filters ?? {};
  const sort = UNIFIED_SORTS.has(opts.sort ?? "") ? (opts.sort as string) : "country";
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";
  const branches = branchCountries(filters);
  if (branches.length === 0) {
    return { rows: [], total: 0, page: 1, pageSize, sort, dir };
  }

  const params: Record<string, unknown> = {};
  const whereByCode = new Map(branches.map((c) => [c.code, branchWhere(c, q, filters, params)]));

  const countSql = branches
    .map((c) => `SELECT count() AS c FROM ${c.companiesTable} ${whereByCode.get(c.code)}`)
    .join(" UNION ALL ");
  const countRows = await chQuery<{ total: string }>(
    `SELECT sum(c) AS total FROM (${countSql})`,
    params,
  );
  const total = Number(countRows[0].total);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const requestedRaw = Number.isFinite(opts.page as number) ? Math.trunc(opts.page as number) : 1;
  const page = Math.min(Math.max(1, requestedRaw), lastPage, MAX_UNIFIED_PAGE);

  const dirSql = dir === "desc" ? "DESC" : "ASC";
  const branchSql = (c: CountryConfig) => {
    const ik = c.industryJoinKeyExpr ?? c.idColumn;
    const sortExpr = sort === "name" ? c.nameColumn : c.idColumn;
    return `SELECT '${c.code}' AS country_code, toString(${c.idColumn}) AS id, ${c.nameColumn} AS name,
      toUInt8(${c.activeExpr}) AS active, toString(${ik}) AS __ik
    FROM ${c.companiesTable} ${whereByCode.get(c.code)}
    ORDER BY coalesce(toString(${sortExpr}), '') = '' ASC, ${sortExpr} ${dirSql}, ${c.idColumn}
    LIMIT ${page * pageSize}`;
  };
  const outerSort =
    sort === "name"
      ? `coalesce(name, '') = '' ASC, name ${dirSql}, country_code, id`
      : `country_code ${dirSql}, id ${dirSql}`;

  const rows = await chQuery<UnifiedRow & { __ik?: string }>(
    `SELECT country_code, id, name, active, __ik
     FROM (${branches.map(branchSql).join(" UNION ALL ")})
     ORDER BY ${outerSort}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  // Per-country industry merge for just the visible page.
  const byCountry = new Map<string, (UnifiedRow & { __ik?: string })[]>();
  for (const row of rows) {
    const group = byCountry.get(row.country_code) ?? [];
    group.push(row);
    byCountry.set(row.country_code, group);
  }
  await Promise.all(
    [...byCountry.entries()].map(async ([code, group]) => {
      const country = getCountry(code)!;
      if (!country.industryQuery) {
        for (const row of group) {
          row.industry_code = null;
          row.industry_label = null;
          delete row.__ik;
        }
        return;
      }
      const ids = group.map((r) => r.__ik ?? "").filter((v) => v !== "");
      const industries = ids.length
        ? await chQuery<{ company_id: string; industry_code: string | null; industry_label: string | null }>(
            country.industryQuery,
            { ids },
          )
        : [];
      const byId = new Map(industries.map((i) => [i.company_id, i]));
      for (const row of group) {
        const hit = byId.get(row.__ik ?? "");
        row.industry_code = hit?.industry_code ?? null;
        row.industry_label = hit?.industry_label ?? null;
        delete row.__ik;
      }
    }),
  );

  return { rows, total, page, pageSize, sort, dir };
}

// ---------- Unified facets ----------

let countryFacetCache: { loadedAt: number; options: FacetOption[] } | null = null;
const COUNTRY_FACET_TTL_MS = 24 * 60 * 60 * 1000;

async function countryFacet(): Promise<FacetOption[]> {
  if (countryFacetCache && Date.now() - countryFacetCache.loadedAt < COUNTRY_FACET_TTL_MS) {
    return countryFacetCache.options;
  }
  const sql = COUNTRIES.map(
    (c) => `SELECT '${c.code}' AS value, count() AS cnt FROM ${c.companiesTable}`,
  ).join(" UNION ALL ");
  const rows = await chQuery<{ value: string; cnt: string }>(`SELECT value, cnt FROM (${sql}) ORDER BY cnt DESC`);
  const options = rows.map((r) => ({
    value: r.value,
    label: getCountry(r.value)?.name ?? r.value,
    count: Number(r.cnt),
  }));
  countryFacetCache = { loadedAt: Date.now(), options };
  return options;
}

export async function getUnifiedFacetOptions(facetKey: string): Promise<FacetOption[]> {
  if (!UNIFIED_FACET_KEYS.includes(facetKey)) throw new Error(`unknown facet: ${facetKey}`);
  if (facetKey === "country") return countryFacet();

  const countries = COUNTRIES.filter((c) => canAnswer(c, facetKey));
  const lists = await Promise.all(countries.map((c) => getFacetOptions(c, facetKey)));
  const merged = new Map<string, FacetOption>();
  for (const list of lists) {
    for (const option of list) {
      const existing = merged.get(option.value);
      if (existing) {
        existing.count += option.count;
        if (existing.label === existing.value && option.label !== option.value) {
          existing.label = option.label;
        }
      } else {
        merged.set(option.value, { ...option });
      }
    }
  }
  return [...merged.values()].sort((a, b) => b.count - a.count);
}

export async function searchUnifiedFacetOptions(facetKey: string, q: string): Promise<FacetOption[]> {
  const options = await getUnifiedFacetOptions(facetKey);
  const trimmed = q.trim();
  if (trimmed === "") return options.slice(0, 200);
  return rankFacetOptions(options, trimmed, 50);
}
```

NOTE: `'${c.code}'` interpolation is registry-only (lowercase ISO2, registry-tested `[a-z]{2}`) — never user input; `f_country` never reaches SQL text (it only selects branches).

- [ ] **Step 5: Verify + commit**

`pnpm test unified && pnpm test filters` → PASS; full `pnpm typecheck && pnpm test` green.

```bash
git add app/lib/unified.server.ts app/lib/filters.ts app/lib/filters.test.ts tests/unified.server.test.ts
git commit -m "feat(backoffice): unified cross-country company search and facets"
```

---

### Task 2: Dashboard shell, route restructure, detail move

**Files:**
- Create: `app/components/app-sidebar.tsx`
- Create: `app/routes/shell.tsx`
- Create: `app/routes/companies.tsx` (MINIMAL placeholder — Task 3 replaces)
- Modify: `app/routes.ts` (full rewrite)
- Modify: `app/routes/home.tsx` (redirect only)
- Modify: `app/routes/country-company-detail.tsx` (back-link)
- Modify: `app/components/detail/contact-location-card.tsx` (geocode path)
- Delete: `app/routes/country.tsx`, `app/routes/country-overview.tsx`, `app/routes/country-companies.tsx`, `app/routes/country-facet-options.ts`, `app/components/country-sidebar.tsx`, `app/components/data-table/company-columns.tsx`

**Interfaces:**
- Produces: the new route table (Tasks 3–4 fill in `companies.tsx` and `facet-options.ts`); detail reachable at `/company/:country/:id`; geocode at `/company/:country/geocode`.

- [ ] **Step 1: New route table**

`app/routes.ts`:

```ts
import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  layout("routes/shell.tsx", [
    index("routes/home.tsx"),
    route("companies", "routes/companies.tsx"),
    route("company/:country/:id", "routes/country-company-detail.tsx"),
    route("company/:country/geocode", "routes/country-geocode.ts"),
    route("facet-options", "routes/facet-options.ts"),
  ]),
] satisfies RouteConfig;
```

Task 4 creates `routes/facet-options.ts`; to keep THIS task's typecheck green, create it now as the thinnest valid resource route (Task 4 replaces the body):

```ts
// app/routes/facet-options.ts — filled in by the unified-filters task
export async function loader() {
  return { options: [] };
}
```

- [ ] **Step 2: Shell + sidebar**

`app/components/app-sidebar.tsx`:

```tsx
import { Link, NavLink } from "react-router";
import { Building2 } from "lucide-react";
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

const NAV_ITEMS = [{ title: "Companies", to: "/companies", icon: Building2 }];

export function AppSidebar() {
  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton render={<Link to="/companies" />} className="h-auto">
              <span className="font-semibold">CompanyCollect</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map((item) => (
                <SidebarMenuItem key={item.to}>
                  <NavLink to={item.to}>
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

(Adaptation note: mirror the prop usage that worked in the deleted `country-sidebar.tsx` — same `render`/`isActive` API on the installed Base UI sidebar.)

`app/routes/shell.tsx`:

```tsx
import { Outlet } from "react-router";
import { AppSidebar } from "~/components/app-sidebar";
import { Separator } from "~/components/ui/separator";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "~/components/ui/sidebar";

export default function Shell() {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-12 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mx-2 h-4" />
          <span className="text-sm font-medium">CompanyCollect Backoffice</span>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4 md:p-6">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
```

`app/routes/home.tsx` — replace entirely:

```tsx
import { redirect } from "react-router";

export function loader() {
  return redirect("/companies");
}
```

`app/routes/companies.tsx` — minimal placeholder:

```tsx
export default function Companies() {
  return <h2 className="text-xl font-semibold">Companies</h2>;
}
```

- [ ] **Step 3: Detail move + deletions**

- `app/routes/country-company-detail.tsx`: change the back Button's Link to `to="/companies"` and its label stays "Companies". Everything else unchanged (params are still `:country`/`:id`).
- `app/components/detail/contact-location-card.tsx`: geocode fetch path `/${country.code}/geocode` → `/company/${country.code}/geocode`.
- Delete the six files listed above. Then `rg -n 'country-sidebar|company-columns|country-overview|country-companies|country-facet-options' app/` must return nothing.

- [ ] **Step 4: Verify**

`pnpm typecheck && pnpm test` green (tests touch libs only). Dev server (HMR):

```bash
curl -s -o /dev/null -w '%{http_code}' http://localhost:5183/            # 302 (or 200 after redirect with -L)
curl -sL http://localhost:5183/ | grep -c 'CompanyCollect'               # >= 1 (shell + sidebar)
ID=$(curl -s "http://companycollect:8123/?user=default&password=password123" --data "SELECT reg_code FROM corpscout.ee_companies ORDER BY reg_code LIMIT 1")
curl -s "http://localhost:5183/company/ee/$ID" | grep -c 'Company record' # >= 1 (detail at the new URL)
curl -s -o /dev/null -w '%{http_code}' "http://localhost:5183/ee/companies"  # 404 (old URL gone)
```

- [ ] **Step 5: Commit**

```bash
git add app/routes.ts app/routes/shell.tsx app/routes/home.tsx app/routes/companies.tsx app/routes/facet-options.ts app/components/app-sidebar.tsx app/routes/country-company-detail.tsx app/components/detail/contact-location-card.tsx
git rm app/routes/country.tsx app/routes/country-overview.tsx app/routes/country-companies.tsx app/routes/country-facet-options.ts app/components/country-sidebar.tsx app/components/data-table/company-columns.tsx
git commit -m "feat(backoffice): dashboard shell and unified route structure"
```

---

### Task 3: Unified companies table page

**Files:**
- Create: `app/components/data-table/unified-columns.tsx`
- Modify: `app/routes/companies.tsx` (replace the placeholder entirely)

**Interfaces:**
- Consumes: `searchUnifiedCompanies`/`UnifiedRow` (Task 1), `parseUnifiedFilters` (Task 1), `DataTable`/`DataTablePagination`/`DataTableColumnHeader` (existing, generic), `getCountry`.
- Produces: the `/companies` page — 3 columns (Name → detail link, Industry, Country), search, sorting (name, country), pagination. Filter sidebar arrives in Task 4.

- [ ] **Step 1: Column builder**

`app/components/data-table/unified-columns.tsx`:

```tsx
import { Link } from "react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { getCountry, type SortDir } from "~/lib/countries";
import type { UnifiedRow } from "~/lib/unified.server";
import { DataTableColumnHeader } from "~/components/data-table/column-header";

const EMPTY = <span className="text-muted-foreground">—</span>;

export function buildUnifiedColumns(sort: string, dir: SortDir): ColumnDef<UnifiedRow, unknown>[] {
  return [
    {
      id: "name",
      header: () => (
        <DataTableColumnHeader label="Name" sortKey="name" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const s = row.original.name ?? "";
        if (s === "") return EMPTY;
        return (
          <Link
            to={`/company/${row.original.country_code}/${encodeURIComponent(row.original.id)}`}
            className="block max-w-[26rem] truncate font-medium underline-offset-2 hover:underline"
            title={s}
          >
            {s}
          </Link>
        );
      },
    },
    {
      id: "industry",
      header: () => <DataTableColumnHeader label="Industry" currentSort={sort} currentDir={dir} />,
      cell: ({ row }) => {
        const code = row.original.industry_code;
        const label = row.original.industry_label;
        if (!code && !label) return EMPTY;
        return (
          <span className="flex max-w-[22rem] items-baseline gap-1.5">
            {code ? <span className="text-muted-foreground font-mono text-xs">{code}</span> : null}
            {label ? (
              <span className="truncate" title={String(label)}>
                {label}
              </span>
            ) : null}
          </span>
        );
      },
    },
    {
      id: "country",
      header: () => (
        <DataTableColumnHeader label="Country" sortKey="country" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const country = getCountry(row.original.country_code);
        if (!country) return row.original.country_code;
        return (
          <span className="flex items-center gap-1.5 whitespace-nowrap">
            <span>{country.flag}</span>
            <span>{country.name}</span>
          </span>
        );
      },
    },
  ];
}
```

- [ ] **Step 2: The page**

Replace `app/routes/companies.tsx`:

```tsx
import { Form } from "react-router";
import type { Route } from "./+types/companies";
import { parseUnifiedFilters } from "~/lib/filters";
import { searchUnifiedCompanies } from "~/lib/unified.server";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildUnifiedColumns } from "~/components/data-table/unified-columns";

export function meta(_: Route.MetaArgs) {
  return [{ title: "Companies – CompanyCollect Backoffice" }];
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q") ?? "";
  const result = await searchUnifiedCompanies({
    q,
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
    filters: parseUnifiedFilters(url.searchParams),
  });
  return { q, result, filters: parseUnifiedFilters(url.searchParams) };
}

const nf = new Intl.NumberFormat("en-US");

export default function Companies({ loaderData }: Route.ComponentProps) {
  const { q, result } = loaderData;
  const columns = buildUnifiedColumns(result.sort, result.dir);
  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Companies</h2>
        <Form method="get" className="flex gap-2">
          <Input type="search" name="q" defaultValue={q} placeholder="Search by name…" className="w-64" />
          <Button type="submit" variant="secondary">
            Search
          </Button>
        </Form>
      </div>
      <p className="text-muted-foreground text-sm">
        {nf.format(result.total)} companies{q ? ` matching “${q}”` : ""}
      </p>
      <DataTable columns={columns} data={result.rows} />
      <DataTablePagination total={result.total} page={result.page} pageSize={result.pageSize} />
    </>
  );
}
```

(Note: `parseUnifiedFilters` called twice in the loader for clarity of the return — collapse to a single `const filters = ...` if you prefer; the returned `filters` is consumed by Task 4's sidebar/badges.)

- [ ] **Step 3: Verify**

`pnpm typecheck && pnpm test`, then against the dev server:

```bash
curl -s 'http://localhost:5183/companies' | grep -c 'Industry'                 # >= 1
curl -s 'http://localhost:5183/companies' | grep -c 'Brazil'                   # >= 1 (default order starts with br)
curl -s 'http://localhost:5183/companies?f_country=ee' | grep -c 'Estonia'     # >= 1 — filter works via URL even before the sidebar
curl -s 'http://localhost:5183/companies?q=petrobras' | grep -ci petrobras     # >= 1
# name sort is the accepted slow path (~10s):
time curl -s -o /dev/null 'http://localhost:5183/companies?sort=name'
```

Click a company name → `/company/{cc}/{id}` detail renders.

- [ ] **Step 4: Commit**

```bash
git add app/components/data-table/unified-columns.tsx app/routes/companies.tsx
git commit -m "feat(backoffice): unified companies table with country column"
```

---

### Task 4: Unified filter sidebar + facet route

**Files:**
- Modify: `app/routes/facet-options.ts` (real implementation)
- Modify: `app/components/data-table/filter-sidebar.tsx` (generalize to unified)
- Modify: `app/routes/companies.tsx` (wire sidebar + badges)

**Interfaces:**
- Consumes: `searchUnifiedFacetOptions`, `UNIFIED_FACET_KEYS`/`UNIFIED_FACET_LABELS`, `toggleFilterValue`/`clearAllFilters`/`useEffectiveSearchParams` (existing).
- Produces: `GET /facet-options?column=<key>&q=` → `{options}` (400 unknown key); the Filters sheet on `/companies` with Country/Status/Legal form/Place/Size/Industry comboboxes; active badges + Clear all.

- [ ] **Step 1: Facet route**

`app/routes/facet-options.ts` — replace the placeholder:

```ts
import type { Route } from "./+types/facet-options";
import { UNIFIED_FACET_KEYS } from "~/lib/filters";
import { searchUnifiedFacetOptions } from "~/lib/unified.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const column = url.searchParams.get("column") ?? "";
  const q = url.searchParams.get("q") ?? "";
  if (!UNIFIED_FACET_KEYS.includes(column)) {
    throw new Response(`Unknown facet column: ${column}`, { status: 400 });
  }
  return { options: await searchUnifiedFacetOptions(column, q) };
}
```

- [ ] **Step 2: Generalize the filter sidebar**

Rework `app/components/data-table/filter-sidebar.tsx` (its only former consumer was deleted in Task 2):
- `FacetCombobox` props become `{ facetKey, label, selected }`; the fetcher base becomes `/facet-options?column=${facetKey}` (no country in the path); everything else (debounce, pending-navigation-aware toggle, checkmarks, counts) stays byte-identical.
- `FilterSidebar` props become `{ filters: CompanyFilters }`; it maps `UNIFIED_FACET_KEYS` and passes `label={UNIFIED_FACET_LABELS[key] ?? key}`.
- Export a `facetLabel(key: string): string` helper returning `UNIFIED_FACET_LABELS[key] ?? key` (the badges import it).

- [ ] **Step 3: Wire into the page**

In `app/routes/companies.tsx`: render `<FilterSidebar filters={filters} />` next to the search Form, plus the active-badges row (identical markup to the deleted country-companies version — Badge per (key,value) with an X `Link to={toggleFilterValue(effectiveParams, key, value)}`, "Clear all" `Link to={clearAllFilters(effectiveParams)}`, both via `useEffectiveSearchParams()`; import `facetLabel` for badge labels; for `country` badges show the country NAME via `getCountry(value)?.name ?? value`).

- [ ] **Step 4: Verify**

`pnpm typecheck && pnpm test`, then:

```bash
curl -s 'http://localhost:5183/facet-options?column=country' | head -c 300     # 10 options with counts
curl -s 'http://localhost:5183/facet-options?column=status&q=reg' | head -c 300
curl -s -o /dev/null -w '%{http_code}' 'http://localhost:5183/facet-options?column=name'  # 400
```

Browser: Filters sheet shows Country (10 options, live counts) + Status/Legal form/Place/Size/Industry; selecting Estonia + a status narrows the table; badges show "Country: Estonia"; rapid multi-select still composes (pending-navigation hook untouched); Clear all resets.

- [ ] **Step 5: Commit**

```bash
git add app/routes/facet-options.ts app/components/data-table/filter-sidebar.tsx app/routes/companies.tsx
git commit -m "feat(backoffice): unified filter sidebar with country facet"
```

---

### Task 5: Cleanup, README, gate

**Files:**
- Modify: `README.md` (structure section rewrite)

- [ ] **Step 1: Dead-code sweep**

`rg -n 'parseFilters\(|filterableFacetKeys\(' app/ --glob '!*.test.*'` — both are now consumed only by lib code/tests (searchCompanies path). Confirm nothing in `app/routes` or components references them; they STAY (the per-country layer powers the detail page and the registry test sweeps — document that in the README). Any component/import that became orphaned in Tasks 2–4 gets removed (run `pnpm typecheck` with `noUnusedLocals` semantics via lint of imports — practically: `rg` for each deleted file's exports).

- [ ] **Step 2: README**

Rewrite the `## Companies table` intro and `## Structure` bullets:

```markdown
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
```

(Adjust surrounding stale references to per-country URLs anywhere else in the README.)

- [ ] **Step 3: Full gate**

`pnpm typecheck && pnpm test && pnpm build`, then `pnpm start` (port 3000, YOUR server — kill after):

```bash
curl -sL http://localhost:3000/ | grep -c 'Companies'          # shell + list render
curl -s 'http://localhost:3000/companies?f_country=no' | grep -c 'Norway'
ID=$(curl -s "http://companycollect:8123/?user=default&password=password123" --data "SELECT org_number FROM corpscout.no_companies ORDER BY org_number LIMIT 1")
curl -s "http://localhost:3000/company/no/$ID" | grep -c 'Company record'
```

Kill; port free.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(backoffice): document unified dashboard structure"
```

---

## Out of scope (logged)

- **`companies_all` materialized table in dagster** — the real fix for name-sort latency and deep pagination; also the natural place for normalized status/industry columns. Top priority follow-up if the unified list becomes the daily driver.
- Per-country overview dashboards (deleted with the old layout; revisit as a country filter view or `/countries` page if missed).
- Old-URL redirects (`/{cc}/companies…` → new scheme) — internal tool, skipped.
- Sidebar future items (Technologies, Domains, Pipelines…) — the shell is ready for them.
