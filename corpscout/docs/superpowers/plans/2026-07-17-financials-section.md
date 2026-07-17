# Backoffice /financials Section Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `/financials` section in the backoffice: a landing overview (revenue by country, top industries, top companies globally), `/financials/country/{code}` (revenue by NACE division + top companies for one country), and `/financials/industry/{division}` (revenue per country + top companies globally within one NACE division) — all computed live from the `xx_company_financials_latest` summary tables.

**Architecture:** Pure backoffice work (`corpscout/services/backoffice`) — no new pipeline. A new registry field `financialsAggregates` describes, per country, (a) how to join the latest-financials summary to that country's primary-NACE industry rows and (b) which rows to exclude from SUMS (Norwegian NUF foreign-branch filings). A new server module `financial-aggregates.server.ts` builds registry-driven aggregate queries (all identifiers from the registry, all user values via named params). Three new routes render tables + recharts bar charts inside the existing dashboard shell; the sidebar gains a second nav item.

**Tech Stack:** React Router 8 SSR, TypeScript, ClickHouse (read-only, named params), recharts (already a dependency via financials-section), vitest live-ClickHouse tests, shadcn/ui.

## Global Constraints

- Registry-driven SQL only: table/column identifiers come exclusively from `app/lib/countries.ts`; every user-influenced value binds via ClickHouse named params (`{x:String}` etc.); `clickhouse_settings: { readonly: "2" }` stays enforced by the shared client.
- **Sums exclude, lists keep**: rows matched by a country's `sumExclusionExpr` are excluded from every aggregate SUM/COUNT but remain eligible for top-companies LISTS, where they render with a "foreign branch — parent entity accounts" marker. (User decision 2026-07-17: NUF branches file the foreign parent's full accounts — AWS EMEA €19.4bn, Nike Retail B.V. €11.0bn — real data, not Norway-earned.)
- Industry grouping is at NACE **division** level (2-digit, `substring(code, 1, 2)`); labels from `corpscout.nace_categories` `level='division'`, preferring `is_current = 1` (NACE_REV_2_1, 87 rows) and falling back to REV_2 for codes only present there. Companies with financials but no NACE mapping aggregate into an explicit **"Unmapped"** bucket — never silently dropped.
- Every aggregate page carries the shared methodology note (exact copy, one component): "Latest filed year per company, converted to USD at period-end rates. Standalone (non-consolidated) accounts; totals may double-count corporate groups. Norway excludes foreign-branch (NUF) filings from sums."
- `TOP_COMPANIES_LIMIT = 25` and `TOP_DIVISIONS_LIMIT = 15` are named constants in the server module.
- Dev server on port 5183 is USER-OWNED — never touch it; visual checks on a throwaway `pnpm dev` port only, killed afterward.
- Tests run live against ClickHouse (no mocks), following `tests/unified.server.test.ts` conventions (generous timeouts, assert on real magnitudes with loose bands).
- Charts: the implementer of Task 3 MUST load the `dataviz` skill before writing chart code.
- Conventional Commits, explicit-path `git add` (shared tree carries unrelated WIP); trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## Ground truth (verified live, 2026-07-17)

- Summary tables `{code}_company_financials_latest` (one row/company; `company_id String`, `fiscal_year`, `revenue_amount_usd Nullable(Float64)`, …): no=425,365 · fi=21,315 · se=525,494 · ee=321,384 · lv=253,928 · gb=24,115 · br=1,218 · sk=1. The NO outlier (org 983096077) is already excluded via `quality_flag`.
- **Primary-NACE join coverage of summary companies** (probed): no 425,100/425,365 via `no_industries` (`org_number`, `nace_normalized_code`, `is_primary=1`); ee 272,704/321,384 (`ee_industries`, `reg_code`); gb 23,806/24,115 (`gb_industries`, `company_number`); sk 1/1 (`sk_industries`, `ico`); **se 417,119/525,494 via `se_industries.company_id` = '16' + summary company_id** — join expr `substring(toString(company_id), 3)` guarded by `startsWith(toString(company_id), '16')`, code column `nace_rev2_class_code` (e.g. '6820').
- **No NACE for**: fi (`fi_industries.nace_normalized_code` empty for ALL rows — `nace_mapping_status` = `unmapped_source_code_set`/`missing_source_code`; TOL2008→NACE mapping never built), lv (`lv_companies_nace` view has 485,380 rows but `nace_code` empty everywhere — classifier not yet run), br (CNAE→NACE = 3-row stub). These three get country pages WITHOUT the industry breakdown (totals + top companies + an honest note); they are excluded from `/financials/industry/*` and from the landing's top-divisions block.
- NACE code formats: NO '27510' (5-digit class), SE '6820' (4-digit class) — 2-digit division prefix works on both. EE carries mixed `NACE_REV_2`/`NACE_REV_2_1` rows; division codes are stable across revisions.
- `nace_categories`: `normalized_code`, `level` ('section'/'division'/'group'/'class'), `description_en` (label INCLUDES the code prefix, e.g. "01 Crop and animal production, …" — strip `^\d+ ` for display), `is_current` (1 = REV_2_1).
- **NUF materiality**: 3,423 NO summary companies are NUF (`no_companies.legal_form_code = 'NUF'`, column verified) carrying $65.4bn revenue — the exclusion is not cosmetic.
- Name enrichment pattern: summary tables carry no names; fetch per-country `SELECT idColumn, nameColumn WHERE idColumn IN {ids:Array(String)}` after the top-N is known (the unified layer's industry-enrichment pattern).
- `formatRevenueUsd(value, fiscalYear)` (compact `$1.2M (2024)`) is exported from `app/components/data-table/unified-columns.tsx` — reuse, don't duplicate.
- Sidebar: `app/components/app-sidebar.tsx` `NAV_ITEMS` currently has one item ("Companies") with a hardcoded `/company/` active-prefix special case — the logged follow-up "per-item active prefixes when item #2 lands" lands NOW.
- recharts is already installed (used by `financials-section.tsx`).

## Out of scope (logged)

- FI TOL2008→NACE mapping (the real fix is in dagster — TOL2008 is the Finnish NACE implementation, near-identity mapping); LV NACE classifier run; BR CNAE→NACE table. Each country auto-joins these pages once its registry `financialsAggregates.nace` entry can be added.
- GB overseas-company branch filings (check whether GB has a NUF-analog worth excluding — needs investigation).
- Drill-down below division level; revenue range filters; caching (live queries measured fast; revisit only if a page exceeds ~2s).

---

### Task 1: Registry config + aggregate query layer

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (type + entries for no/se/ee/gb/sk)
- Create: `corpscout/services/backoffice/app/lib/financial-aggregates.server.ts`
- Test: `corpscout/services/backoffice/tests/financial-aggregates.server.test.ts`

**Interfaces:**
- Produces (Tasks 2–3 consume):

```ts
// countries.ts
export type CountryFinancialsAggregates = {
  /** Primary-NACE join for the summary table; omit when no usable mapping. */
  nace?: {
    industriesTable: string;
    /** Expression on industriesTable yielding summary.company_id values. */
    companyKeyExpr: string;
    /** Expression yielding normalized NACE digits (class level). */
    naceCodeExpr: string;
    /** WHERE conjunct scoping to usable primary rows. */
    filterExpr: string;
  };
  /** Conjunct on summary alias `f` excluding rows from SUMS (lists keep them). */
  sumExclusionExpr?: string;
};
// added to CountryConfig: financialsAggregates?: CountryFinancialsAggregates;

// financial-aggregates.server.ts
export const TOP_COMPANIES_LIMIT = 25;
export const TOP_DIVISIONS_LIMIT = 15;
export type CountryTotals = { country_code: string; companies: number; revenue_usd: number | null; latest_fiscal_year: number | null };
export type DivisionRevenue = { division: string; label: string; companies: number; revenue_usd: number | null };
export type TopCompany = { country_code: string; company_id: string; name: string; revenue_usd: number | null; fiscal_year: number | null; industry_label: string | null; excluded_from_sums: boolean };
export function getDivisionLabels(): Promise<Map<string, string>>;          // 24h in-memory cache
export function getGlobalFinancialOverview(): Promise<{ countries: CountryTotals[]; topDivisions: DivisionRevenue[]; topCompanies: TopCompany[] }>;
export function getCountryFinancials(code: string): Promise<null | { totals: CountryTotals; divisions: DivisionRevenue[] | null; unmapped: DivisionRevenue | null; topCompanies: TopCompany[] }>;
export function getIndustryFinancials(division: string): Promise<null | { division: string; label: string; countries: CountryTotals[]; topCompanies: TopCompany[] }>;
```

- [ ] **Step 1: Registry entries.** Add the type above and, on the five NACE-capable countries:

```ts
// no (plus the NUF sum exclusion):
financialsAggregates: {
  nace: {
    industriesTable: "no_industries",
    companyKeyExpr: "toString(org_number)",
    naceCodeExpr: "nace_normalized_code",
    filterExpr: "is_primary = 1 AND nace_normalized_code != ''",
  },
  // NUF branches file the foreign parent's full accounts (AWS EMEA €19.4bn);
  // real data, not Norway-earned — excluded from sums, kept in lists.
  sumExclusionExpr: "f.company_id NOT IN (SELECT toString(org_number) FROM no_companies WHERE legal_form_code = 'NUF')",
},
// se:
financialsAggregates: {
  nace: {
    industriesTable: "se_industries",
    companyKeyExpr: "substring(toString(company_id), 3)",
    naceCodeExpr: "nace_rev2_class_code",
    filterExpr: "is_primary = 1 AND nace_rev2_class_code != '' AND startsWith(toString(company_id), '16')",
  },
},
// ee / gb / sk: same shape as no.nace with (ee_industries, toString(reg_code)),
// (gb_industries, toString(company_number)), (sk_industries, toString(ico)),
// all with naceCodeExpr "nace_normalized_code" and the same filterExpr as no.
```

fi/lv/br get NO `financialsAggregates` (no usable NACE — see Ground truth); fr/cz have no `financialsLatest` at all.

- [ ] **Step 2: Write the failing live tests** (`tests/financial-aggregates.server.test.ts`):

```ts
import { describe, expect, it } from "vitest";
import {
  getCountryFinancials,
  getDivisionLabels,
  getGlobalFinancialOverview,
  getIndustryFinancials,
} from "~/lib/financial-aggregates.server";

describe("division labels", () => {
  it("loads ~87 divisions with code-stripped labels", async () => {
    const labels = await getDivisionLabels();
    expect(labels.size).toBeGreaterThanOrEqual(85);
    expect(labels.get("62")).toMatch(/programming|computer/i);
    expect(labels.get("62")).not.toMatch(/^62 /);
  }, 30_000);
});

describe("getGlobalFinancialOverview", () => {
  it("covers all summary countries and puts Equinor in the global top", async () => {
    const overview = await getGlobalFinancialOverview();
    expect(overview.countries.map((c) => c.country_code).sort()).toEqual(
      ["br", "ee", "fi", "gb", "lv", "no", "se", "sk"],
    );
    const names = overview.topCompanies.map((c) => c.name);
    expect(names.some((n) => /EQUINOR/i.test(n))).toBe(true);
    expect(overview.topDivisions.length).toBeGreaterThan(5);
  }, 60_000);
});

describe("getCountryFinancials", () => {
  it("norway: NUF exclusion reduces the sum and divisions cover most companies", async () => {
    const no = await getCountryFinancials("no");
    expect(no).not.toBeNull();
    // 3,423 NUF companies carry ~$65bn — the excluded total must be well below
    // the naive sum (which would exceed $300bn with branches included).
    expect(no!.totals.revenue_usd).toBeGreaterThan(100e9);
    expect(no!.totals.companies).toBeLessThan(425_365); // NUF excluded from count too
    const mapped = no!.divisions!.reduce((s, d) => s + d.companies, 0);
    expect(mapped + (no!.unmapped?.companies ?? 0)).toBe(no!.totals.companies);
    // NUF companies still appear in lists when they rank (AWS EMEA does):
    const aws = no!.topCompanies.find((c) => /AMAZON WEB SERVICES/i.test(c.name));
    expect(aws?.excluded_from_sums).toBe(true);
  }, 60_000);

  it("finland has totals and top companies but no division breakdown", async () => {
    const fi = await getCountryFinancials("fi");
    expect(fi).not.toBeNull();
    expect(fi!.divisions).toBeNull();
    expect(fi!.topCompanies.length).toBeGreaterThan(10);
  }, 30_000);

  it("returns null for countries without financials", async () => {
    expect(await getCountryFinancials("fr")).toBeNull();
    expect(await getCountryFinancials("nope")).toBeNull();
  });
});

describe("getIndustryFinancials", () => {
  it("real estate (68) spans multiple countries with real revenue", async () => {
    const re = await getIndustryFinancials("68");
    expect(re).not.toBeNull();
    expect(re!.label).toMatch(/real estate/i);
    const codes = re!.countries.map((c) => c.country_code);
    expect(codes).toContain("no");
    expect(codes).toContain("se");
    expect(re!.topCompanies.length).toBeGreaterThan(10);
    expect(re!.topCompanies[0].revenue_usd).toBeGreaterThan(1e8);
  }, 60_000);

  it("rejects garbage division codes", async () => {
    expect(await getIndustryFinancials("9x")).toBeNull();
    expect(await getIndustryFinancials("999")).toBeNull();
  });
});
```

Run: `pnpm vitest run tests/financial-aggregates.server.test.ts` → FAIL (module missing).

- [ ] **Step 3: Implement `financial-aggregates.server.ts`.** Structure (complete the obvious mechanical parts; all SQL shapes below are the spec):

```ts
import { COUNTRIES, getCountry, type CountryConfig } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server"; // match the existing client import used by unified.server.ts

export const TOP_COMPANIES_LIMIT = 25;
export const TOP_DIVISIONS_LIMIT = 15;

const summaryCountries = () => COUNTRIES.filter((c) => c.financialsLatest);
const naceCountries = () => COUNTRIES.filter((c) => c.financialsLatest && c.financialsAggregates?.nace);
const exclusion = (c: CountryConfig) => c.financialsAggregates?.sumExclusionExpr;
// NOTE: no revenue-IS-NOT-NULL conjunct here — `sum()` skips NULLs on its own,
// and adding it would silently shrink the companies COUNT (NO has ~90k summary
// rows with fiscal data but no convertible revenue). Counts mean "companies
// with financial data (minus sum-exclusions)".
const sumWhere = (c: CountryConfig, extra?: string) => {
  const conds = [exclusion(c), extra].filter(Boolean);
  return conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
};
```

**Division labels** (24h module-level cache, facet-cache pattern): current revision wins, REV_2-only codes fall back:

```sql
SELECT code, any(label) AS label FROM (
  SELECT normalized_code AS code, description_en AS label
  FROM nace_categories WHERE level = 'division'
  ORDER BY is_current DESC
) GROUP BY code
```

Strip the leading code from labels in TS (`label.replace(/^\d+\s+/, "")`).

**Country totals** (one UNION over `summaryCountries()`):

```sql
SELECT '{code}' AS country_code, toUInt32(count()) AS companies,
  sum(f.revenue_amount_usd) AS revenue_usd, max(f.fiscal_year) AS latest_fiscal_year
FROM {summary} f
{sumWhere(c)}
```

**Divisions for one country** (only when `nace` present) — LEFT-join-free semi-aggregate; the unmapped bucket is `totals.companies - sum(mapped)`, computed in TS:

```sql
SELECT substring(i.code, 1, 2) AS division, toUInt32(count()) AS companies,
  sum(f.revenue_amount_usd) AS revenue_usd
FROM {summary} f
INNER JOIN (
  SELECT {companyKeyExpr} AS cid, any({naceCodeExpr}) AS code
  FROM {industriesTable} WHERE {filterExpr} GROUP BY cid
) i ON i.cid = f.company_id
{sumWhere(c)}
GROUP BY division ORDER BY revenue_usd DESC
```

(The inner `GROUP BY cid` guarantees ≤1 industry row per company so counts can't double.) Attach labels from the division map; unknown divisions keep the raw code as label.

**Top companies** (per country; used by country page, and by landing/industry pages as UNION branches): NOTE — no `sumWhere` here (lists keep excluded rows), but each row carries `excluded_from_sums`:

```sql
SELECT '{code}' AS country_code, f.company_id AS company_id,
  f.revenue_amount_usd AS revenue_usd, f.fiscal_year AS fiscal_year,
  {excludedExpr} AS excluded_from_sums
FROM {summary} f
WHERE f.revenue_amount_usd IS NOT NULL {AND division/industry scope when applicable}
ORDER BY f.revenue_amount_usd DESC
LIMIT {TOP_COMPANIES_LIMIT}
```

where `{excludedExpr}` is `toUInt8(NOT (${sumExclusionExpr}))` when the country has one, else `toUInt8(0)`. For the landing/industry pages, wrap the per-country branches in an outer `ORDER BY revenue_usd DESC LIMIT TOP_COMPANIES_LIMIT`.

**Name + industry enrichment** (after the top-N ids are known, grouped per country): names via `SELECT toString({idColumn}) AS id, {nameColumn} AS name FROM {companiesTable} WHERE {idColumn} IN {ids:Array(String)}`; industry labels via the country's existing `industryQuery` when defined (same contract the unified layer uses). Missing name → fall back to the id; missing industry → null. ClickHouse returns the `toUInt8` excluded column as `0/1` — convert to a real boolean during enrichment (`excluded_from_sums: Boolean(row.excluded_from_sums)`) so the `TopCompany` type and the `.toBe(true)` test assertions hold.

**Industry page scope**: validate `division` (`/^\d{2}$/` AND present in the division label map → else return null). Country branches only for `naceCountries()`; scope both sums and top-companies with:

```sql
f.company_id IN (
  SELECT {companyKeyExpr} FROM {industriesTable}
  WHERE {filterExpr} AND substring({naceCodeExpr}, 1, 2) = {division:String}
)
```

**Landing top divisions**: UNION the per-country division aggregates (nace countries only, sums-excluded), outer `GROUP BY division` summing revenue and companies, `ORDER BY revenue_usd DESC LIMIT TOP_DIVISIONS_LIMIT`.

- [ ] **Step 4: Run the tests to green; measure.** `pnpm vitest run tests/financial-aggregates.server.test.ts` → all pass. Log each page-level function's wall time in the test output (console.log is fine in tests); report any function over 2s.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/countries.ts corpscout/services/backoffice/app/lib/financial-aggregates.server.ts corpscout/services/backoffice/tests/financial-aggregates.server.test.ts
git commit -m "feat(backoffice): financial aggregate query layer with nace and nuf rules"
```

---

### Task 2: Routes, navigation, and tables

**Files:**
- Modify: `corpscout/services/backoffice/app/routes.ts`
- Modify: `corpscout/services/backoffice/app/components/app-sidebar.tsx`
- Create: `corpscout/services/backoffice/app/routes/financials.tsx`
- Create: `corpscout/services/backoffice/app/routes/financials-country.tsx`
- Create: `corpscout/services/backoffice/app/routes/financials-industry.tsx`
- Create: `corpscout/services/backoffice/app/components/financials/methodology-note.tsx`
- Create: `corpscout/services/backoffice/app/components/financials/top-companies-table.tsx`

**Interfaces:**
- Consumes Task 1's functions/types verbatim.
- Produces the three routes at `/financials`, `/financials/country/:country`, `/financials/industry/:division`; a shared `<TopCompaniesTable companies={TopCompany[]} showCountry={boolean} />`; `<MethodologyNote />` with the exact Global-Constraints copy. Task 3 adds charts into these pages.

- [ ] **Step 1: Routing.** In `routes.ts`, inside the shell layout: `route("financials", "routes/financials.tsx")`, `route("financials/country/:country", "routes/financials-country.tsx")`, `route("financials/industry/:division", "routes/financials-industry.tsx")`.

- [ ] **Step 2: Sidebar.** Add `{ label: "Financials", to: "/financials", icon: <appropriate lucide icon, e.g. ChartColumn> }` to `NAV_ITEMS` and generalize active-state per item: an item is active when `pathname === item.to || pathname.startsWith(item.to + "/")`, plus the existing Companies special case (`/company/` detail pages activate Companies). This closes the logged "per-item active prefixes" follow-up. Keep the single-anchor `render={<Link/>}` pattern (no nested buttons; `SidebarMenuButton` has no `nativeButton` prop).

- [ ] **Step 3: Landing (`financials.tsx`).** Loader: `getGlobalFinancialOverview()`. Meta title "Financials – CompanyCollect Backoffice". Render three cards: (1) **Revenue by country** table — flag + name (link `/financials/country/{code}`), companies-with-financials count, total revenue (compact USD), latest FY; (2) **Top industries** — division label (link `/financials/industry/{division}`), companies, revenue; (3) **Top companies** — `<TopCompaniesTable showCountry>`. `<MethodologyNote />` at the bottom.

- [ ] **Step 4: Country page.** Loader: 404 `Response` when `getCountryFinancials(params.country)` returns null. Header: flag + name + back link to `/financials`. Totals strip (companies, revenue, latest FY) using the small stat-card pattern from the dashboard. When `divisions` is non-null: division table (label linking to the industry page, companies, revenue) including the **Unmapped** row (muted) when present; when null: a muted note "Industry breakdown unavailable — no NACE mapping for this source yet." Top companies via the shared table (`showCountry={false}`, names link to `/company/{code}/{id}`); rows with `excluded_from_sums` get an outline badge "foreign branch — parent entity accounts".

- [ ] **Step 5: Industry page.** Loader: null → 404. Header: division code + label, back link. Countries table (flag+name linking to country page, companies, revenue). Top companies (`showCountry`). MethodologyNote.

- [ ] **Step 6: Shared components.** `TopCompaniesTable`: shadcn Table, right-aligned `tabular-nums` revenue via `formatRevenueUsd` (import from `unified-columns.tsx`), name cell links to `/company/{country_code}/{company_id}`, optional country column (flag + code), the excluded-badge described above. `MethodologyNote`: `text-muted-foreground text-xs` paragraph with the exact copy.

- [ ] **Step 7: Gate + commit.** `pnpm typecheck` clean; `pnpm test` green (existing suites unaffected). Throwaway dev server: all three pages render with real data; sidebar shows both items with correct active states; links round-trip (landing → country → industry → company detail). Kill the throwaway server.

```bash
git add corpscout/services/backoffice/app/routes.ts corpscout/services/backoffice/app/components/app-sidebar.tsx corpscout/services/backoffice/app/routes/financials.tsx corpscout/services/backoffice/app/routes/financials-country.tsx corpscout/services/backoffice/app/routes/financials-industry.tsx corpscout/services/backoffice/app/components/financials/methodology-note.tsx corpscout/services/backoffice/app/components/financials/top-companies-table.tsx
git commit -m "feat(backoffice): financials section routes and tables"
```

---

### Task 3: Charts

**Files:**
- Create: `corpscout/services/backoffice/app/components/financials/revenue-bar-chart.tsx`
- Modify: the three route files (add charts above their tables)

**Interfaces:**
- Consumes Task 1 types. Produces `<RevenueBarChart items={{ key, label, revenue_usd, href? }[]} />` — a horizontal recharts bar chart (labels readable at 15–20 rows, compact USD ticks, bar click/label navigates via href when given).

- [ ] **Step 1: LOAD THE `dataviz` SKILL FIRST** (mandatory), then implement `RevenueBarChart` client-side (recharts is client-safe here the same way `financials-section.tsx` uses it — mirror its import/mounting approach). Horizontal layout, `formatRevenueUsd`-style compact tick formatter, muted grid, single-series brand-neutral color per the skill, tooltip with full (non-compact) USD value + label.
- [ ] **Step 2: Wire in**: landing gets country-revenue chart + top-divisions chart; country page gets its division chart (top 15 + unmapped); industry page gets the per-country chart. Tables stay (charts summarize, tables link).
- [ ] **Step 3: Gate + commit.** `pnpm typecheck`; `pnpm test`; throwaway-server visual check of all three pages (charts render, tooltips work, clicking a division bar navigates). Kill the server.

```bash
git add corpscout/services/backoffice/app/components/financials/revenue-bar-chart.tsx corpscout/services/backoffice/app/routes/financials.tsx corpscout/services/backoffice/app/routes/financials-country.tsx corpscout/services/backoffice/app/routes/financials-industry.tsx
git commit -m "feat(backoffice): financials section charts"
```

---

### Task 4: README + final gate

**Files:**
- Modify: `corpscout/services/backoffice/README.md`

- [ ] **Step 1:** Document the section: the three routes, the sums-exclude/lists-keep NUF rule, the unmapped bucket, which countries have NACE breakdowns (no/se/ee/gb/sk) vs totals-only (fi/lv/br) and why, and the follow-ups that auto-upgrade them (FI TOL mapping, LV classifier, BR CNAE table → just add the registry `nace` entry).
- [ ] **Step 2:** Full gate: `pnpm typecheck && pnpm test` — all green.
- [ ] **Step 3:** Commit:

```bash
git add corpscout/services/backoffice/README.md
git commit -m "docs(backoffice): financials section notes"
```
