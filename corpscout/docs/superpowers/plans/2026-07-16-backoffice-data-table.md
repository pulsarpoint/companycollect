# Backoffice Companies Data Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the basic 3-column companies table with a shadcn/TanStack data table: ~7 columns per country (registry ID, name, **industry**, legal form, status, key date, place), server-side column sorting (URL-driven), and proper pagination controls (page-size selector, first/prev/next/last, totals).

**Architecture:** The static country registry grows a declarative per-country column model (`CompanyColumn[]`: key, label, SQL expr, sortable, render kind) plus a per-country industry lookup query. `searchCompanies` builds its SELECT and ORDER BY from that model (identifiers still only from the registry; sort key validated against the whitelist), pages first, then fetches primary industries for just the visible page via a second `IN {ids:Array(String)}` query and merges in JS — so industry display never slows pagination on the 30–70M-row tables, at the cost of the industry column being **unsortable by design**. The UI is TanStack Table in fully manual mode (sorting/pagination state parsed from the URL, header sort buttons and pagination controls are plain `<Link>`s) rendered with shadcn `ui/table`.

**Tech Stack:** React Router 8 (SSR), @tanstack/react-table (re-added — it was pruned in the scaffold cleanup because only the deleted demo used it; now it has a real consumer), shadcn/ui v4.13 (Base UI primitives — `render` prop, NOT `asChild`), ClickHouse via existing `chQuery`.

## Global Constraints

- App: `corpscout/services/backoffice`. Package manager pnpm. Node 22.22+.
- URL is the single source of truth for table state: `?q=&page=&pageSize=&sort=&dir=`. No client-side table state that isn't derived from the URL. Search submit resets page; sort change resets page; page-size change resets page.
- All user-supplied VALUES go through ClickHouse query params; SQL identifiers/expressions come ONLY from the static registry (`app/lib/countries.ts`). `sort` and `dir` from the URL are never interpolated — they select a whitelisted registry column (fallback `name`) and a whitelisted direction (`asc`|`desc`, fallback `asc`).
- ClickHouse access stays read-only (client enforces `readonly=2`) and server-only (`*.server.ts`).
- Integration tests run against the real ClickHouse (Estonia `ee` — smallest dataset); no mocks.
- Page sizes: exactly `[25, 50, 100]`; default 50 (also the fallback for invalid values). MAX_PAGE_SIZE stays 100.
- The industry column is NOT sortable (it comes from a post-pagination join) — every other column's sortability is declared per column in the registry.
- shadcn primitives are Base UI: use `render={<Link .../>}` on Button, never `asChild` (precedent: `app/components/country-sidebar.tsx`, `app/routes/country-companies.tsx`).
- Design direction (frontend-design): refined-utilitarian, dense but calm, coherent with the existing shadcn/Geist system — no restyle. Registry IDs in `font-mono text-xs text-muted-foreground`; numbers/dates `tabular-nums whitespace-nowrap`; names `font-medium` truncated with `title` tooltip; industry cell = muted mono code + truncated label; status as Badge (`default` when active, `outline` when not) carrying the country's own status text; table wrapped in `overflow-x-auto` with a `min-w` so the page never scrolls horizontally; missing values render as `—` (em dash), never blank or "null".
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only backoffice paths.

## Ground truth (verified live, 2026-07-16)

Industry sources per country — all queries return one row per company id, preferring `is_primary = 1`:

| country | source | join key (company side) | label source |
|---|---|---|---|
| no, fi, ee, gb, fr, cz, sk | `{cc}_industries` (uniform: `nace_normalized_code`, `description_en`, `description_original`, `is_primary`) | = idColumn | `coalesce(description_en, description_original, code)` — **EE has `description_en` all NULL; `description_original` is 100% populated** |
| se | `se_industries` (`company_id`, `sequence`, `is_primary`, `nace_rev2_class_code`, no description) | `company_id` (NOT `registration_number` → needs `industryJoinKeyExpr`) | JOIN `nace_categories` (1,047 current rows) on `normalized_code`; verified live: `6499` → "64.99 Other financial service activities…" |
| lv | `lv_companies_nace` (`nace_code`, `nace_label`; exactly 1 row per regcode, 485,380 = 485,380) | `regcode` | `nace_label` inline |
| br | `br_establishments` (`is_headquarters=1`, `primary_cnae_code`) → `br_cnae_to_nace` | `cnpj_basico` | NACE mapping covers almost nothing (14/2005 sample) → label falls back to the raw 7-digit CNAE code; one CNAE maps to MULTIPLE NACE rows → `LIMIT 1 BY` required |

Column inventory used below was verified against `system.columns` for all 10 `{cc}_companies` tables.

---

### Task 1: Registry column model + per-country columns and industry queries

**Files:**
- Modify: `app/lib/countries.ts` (add types + extend all 10 entries)
- Modify: `app/lib/countries.test.ts` (add invariants)

**Interfaces:**
- Consumes: nothing new.
- Produces (Tasks 2–4 rely on these exact names):
  - `type ColumnKind = "id" | "text" | "date" | "status"`
  - `type SortDir = "asc" | "desc"`
  - `interface CompanyColumn { key: string; label: string; expr: string; sortable: boolean; kind: ColumnKind }`
  - `CountryConfig` gains: `columns: CompanyColumn[]`, `industryQuery?: string`, `industryJoinKeyExpr?: string`
  - Helper `getSortColumn(country: CountryConfig, key: string | null): CompanyColumn` (returns the matching sortable column, else the `name` column)

- [ ] **Step 1: Write the failing tests**

Append to `app/lib/countries.test.ts`:

```ts
describe("company columns", () => {
  it("every country declares id and name columns with unique keys", () => {
    for (const c of COUNTRIES) {
      const keys = c.columns.map((col) => col.key);
      expect(keys, c.code).toEqual([...new Set(keys)]);
      expect(keys, c.code).toContain("id");
      expect(keys, c.code).toContain("name");
      expect(keys, c.code).not.toContain("industry"); // industry is virtual, merged post-query
      expect(keys, c.code).not.toContain("active"); // reserved, always selected
    }
  });

  it("every country has a sortable status column and a sortable name", () => {
    for (const c of COUNTRIES) {
      const status = c.columns.find((col) => col.kind === "status");
      expect(status, c.code).toBeDefined();
      expect(status?.sortable, c.code).toBe(true);
      expect(c.columns.find((col) => col.key === "name")?.sortable, c.code).toBe(true);
    }
  });

  it("every industry query is parameterized and returns the merge contract", () => {
    for (const c of COUNTRIES) {
      expect(c.industryQuery, c.code).toBeDefined();
      expect(c.industryQuery, c.code).toContain("{ids:Array(String)}");
      expect(c.industryQuery, c.code).toContain("AS company_id");
      expect(c.industryQuery, c.code).toContain("AS industry_code");
      expect(c.industryQuery, c.code).toContain("AS industry_label");
    }
  });

  it("getSortColumn whitelists: unknown or unsortable keys fall back to name", () => {
    const ee = getCountry("ee")!;
    expect(getSortColumn(ee, "status").key).toBe("status");
    expect(getSortColumn(ee, "id; DROP TABLE x").key).toBe("name");
    expect(getSortColumn(ee, null).key).toBe("name");
  });
});
```

Add `getSortColumn` to the import line at the top of the file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test countries`
Expected: FAIL — `getSortColumn` not exported / `columns` undefined.

- [ ] **Step 3: Extend the registry**

In `app/lib/countries.ts`, add after `CountryFeature`:

```ts
export type ColumnKind = "id" | "text" | "date" | "status";
export type SortDir = "asc" | "desc";

export interface CompanyColumn {
  /** Stable key: row field name, ?sort= value, and SQL alias. [a-z_]+ only. */
  key: string;
  /** Column header text. */
  label: string;
  /** SQL select expression. Registry-only — never user input. */
  expr: string;
  /** Sortable columns become ORDER BY candidates. Industry is never sortable. */
  sortable: boolean;
  /** Rendering hint for the UI. */
  kind: ColumnKind;
}
```

Extend `CountryConfig`:

```ts
export interface CountryConfig {
  // ... existing fields unchanged ...
  /** Visible list columns, in display order. Must include keys "id" and "name". */
  columns: CompanyColumn[];
  /**
   * SQL returning one industry row per visible company:
   * SELECT ... AS company_id, ... AS industry_code, ... AS industry_label
   * with the page's join-key values bound as {ids:Array(String)}.
   */
  industryQuery?: string;
  /** Company-table expression producing the industry join key. Defaults to idColumn. */
  industryJoinKeyExpr?: string;
}
```

Then extend each entry. The uniform-industry countries share the SQL shape (table + key differ); write each out fully — the engineer may read entries independently:

```ts
export const COUNTRIES: CountryConfig[] = [
  {
    code: "no", name: "Norway", flag: "🇳🇴", companiesTable: "no_companies",
    idColumn: "org_number", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "1.2M", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Org number", expr: "org_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(legal_form_description_original, legal_form_code)", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "lifecycle_status", sortable: true, kind: "status" },
      { key: "registered", label: "Registered", expr: "toString(registration_date)", sortable: true, kind: "date" },
      { key: "website", label: "Website", expr: "primary_website_host", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT org_number AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM no_industries
WHERE org_number IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY org_number`,
  },
  {
    code: "fi", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies",
    idColumn: "business_id", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "460k", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Business ID", expr: "business_id", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(legal_form_description_en, legal_form_description_original, legal_form_code)", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "lifecycle_status", sortable: true, kind: "status" },
      { key: "registered", label: "Registered", expr: "toString(registration_date)", sortable: true, kind: "date" },
      { key: "website", label: "Website", expr: "primary_website_url", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT business_id AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM fi_industries
WHERE business_id IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY business_id`,
  },
  {
    code: "se", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies",
    idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'",
    approxCompanies: "4.1M", features: ["financials", "industries"],
    industryJoinKeyExpr: "company_id",
    columns: [
      { key: "id", label: "Reg. number", expr: "registration_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_code", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "status", sortable: true, kind: "status" },
      { key: "registered", label: "Incorporated", expr: "toString(incorporation_date)", sortable: true, kind: "date" },
    ],
    industryQuery: `SELECT i.company_id AS company_id,
  i.nace_rev2_class_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.nace_rev2_class_code) AS industry_label
FROM se_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.company_id IN {ids:Array(String)}
ORDER BY i.is_primary DESC, i.sequence ASC
LIMIT 1 BY i.company_id`,
  },
  {
    code: "ee", name: "Estonia", flag: "🇪🇪", companiesTable: "ee_companies",
    idColumn: "reg_code", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "373k", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Reg. code", expr: "reg_code", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_en, ''), legal_form_original)", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "coalesce(nullIf(status_en, ''), status_original)", sortable: true, kind: "status" },
      { key: "registered", label: "First entry", expr: "toString(first_entry_date)", sortable: true, kind: "date" },
      { key: "place", label: "Location", expr: "location", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT reg_code AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM ee_industries
WHERE reg_code IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY reg_code`,
  },
  {
    code: "lv", name: "Latvia", flag: "🇱🇻", companiesTable: "lv_companies",
    idColumn: "regcode", nameColumn: "legal_name", activeExpr: "is_active = 1",
    approxCompanies: "485k", features: ["financials", "contacts", "domains"],
    columns: [
      { key: "id", label: "Reg. code", expr: "regcode", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_description_en, ''), legal_form_text)", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "status", sortable: true, kind: "status" },
      { key: "registered", label: "Registered", expr: "registered_date", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "coalesce(address_city_name, '')", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT regcode AS company_id,
  nace_code AS industry_code,
  coalesce(nullIf(nace_label, ''), nace_code) AS industry_label
FROM lv_companies_nace
WHERE regcode IN {ids:Array(String)}`,
  },
  {
    code: "gb", name: "United Kingdom", flag: "🇬🇧", companiesTable: "gb_companies",
    idColumn: "company_number", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "5.7M", features: ["financials", "industries"],
    columns: [
      { key: "id", label: "Company number", expr: "company_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Category", expr: "company_category", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "company_status", sortable: true, kind: "status" },
      { key: "registered", label: "Incorporated", expr: "toString(incorporation_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT company_number AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM gb_industries
WHERE company_number IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY company_number`,
  },
  {
    code: "fr", name: "France", flag: "🇫🇷", companiesTable: "fr_companies",
    idColumn: "siren", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "29.7M", features: ["industries"],
    columns: [
      { key: "id", label: "SIREN", expr: "siren", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_en", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "status_en", sortable: true, kind: "status" },
      { key: "registered", label: "Created", expr: "toString(creation_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT siren AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM fr_industries
WHERE siren IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY siren`,
  },
  {
    code: "br", name: "Brazil", flag: "🇧🇷", companiesTable: "br_companies",
    idColumn: "cnpj_basico", nameColumn: "legal_name", activeExpr: "is_active = 1",
    approxCompanies: "68.6M", features: ["financials", "contacts", "domains"],
    columns: [
      { key: "id", label: "CNPJ", expr: "cnpj_basico", sortable: true, kind: "id" },
      { key: "name", label: "Legal name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "trade_name", label: "Trade name", expr: "trade_name", sortable: false, kind: "text" },
      { key: "size", label: "Size", expr: "company_size_en", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "status_en", sortable: true, kind: "status" },
      { key: "registered", label: "Activity start", expr: "toString(activity_start_date)", sortable: true, kind: "date" },
      { key: "place", label: "Municipality", expr: "concat(municipality_name, ' / ', state)", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT e.cnpj_basico AS company_id,
  e.primary_cnae_code AS industry_code,
  coalesce(nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label
FROM br_establishments AS e
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.cnpj_basico IN {ids:Array(String)} AND e.is_headquarters = 1
ORDER BY e.primary_cnae_code != '' DESC
LIMIT 1 BY e.cnpj_basico`,
  },
  {
    code: "cz", name: "Czechia", flag: "🇨🇿", companiesTable: "cz_companies",
    idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "3.5M", features: ["industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "IČO", expr: "ico", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_en", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "if(is_active = 1, 'active', 'inactive')", sortable: true, kind: "status" },
      { key: "registered", label: "Established", expr: "toString(established_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT ico AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM cz_industries
WHERE ico IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY ico`,
  },
  {
    code: "sk", name: "Slovakia", flag: "🇸🇰", companiesTable: "sk_companies",
    idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "2.2M", features: ["financials", "industries"],
    columns: [
      { key: "id", label: "IČO", expr: "ico", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_en, ''), legal_form_original)", sortable: true, kind: "text" },
      { key: "status", label: "Status", expr: "if(is_active = 1, 'active', 'inactive')", sortable: true, kind: "status" },
      { key: "registered", label: "Established", expr: "toString(established_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT ico AS company_id,
  nace_normalized_code AS industry_code,
  coalesce(description_en, description_original, nace_normalized_code) AS industry_label
FROM sk_industries
WHERE ico IN {ids:Array(String)}
ORDER BY is_primary DESC
LIMIT 1 BY ico`,
  },
];
```

(Existing fields — code, name, flag, companiesTable, idColumn, nameColumn, activeExpr, approxCompanies, features — keep their current values exactly; the entries above restate them for completeness.)

Nullable-column note: `toString(Nullable(Date))` yields `NULL`-propagating strings; the UI renders null/empty as `—`. That is expected — do not add per-column null gymnastics beyond what's written here.

Add the helper at the bottom of the file:

```ts
export function getSortColumn(
  country: CountryConfig,
  key: string | null,
): CompanyColumn {
  const match = country.columns.find((c) => c.sortable && c.key === key);
  return match ?? country.columns.find((c) => c.key === "name")!;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test countries`
Expected: PASS (7 tests in the file).

- [ ] **Step 5: Run the full gate and commit**

Run: `pnpm typecheck && pnpm test`
Expected: typecheck green; 12 existing tests + new ones pass (Task 2 will adjust `queries.server` — at this point nothing consumes the new fields, so everything still passes).

```bash
git add app/lib/countries.ts app/lib/countries.test.ts
git commit -m "feat(backoffice): add per-country column model and industry queries"
```

---

### Task 2: Query layer — dynamic columns, sorting, industry merge

**Files:**
- Modify: `app/lib/queries.server.ts` (rework `searchCompanies`; keep `getCountryStats` untouched)
- Modify: `tests/queries.server.test.ts` (extend)

**Interfaces:**
- Consumes: `CompanyColumn`, `SortDir`, `getSortColumn`, `CountryConfig.columns/industryQuery/industryJoinKeyExpr` (Task 1); `chQuery` (existing).
- Produces (Tasks 3–4 rely on):
  - `type CompanyListRow = Record<string, string | number | null> & { active: 0 | 1 }` — keys are the registry column keys, plus `industry_code`/`industry_label` (string|null) when the country has an `industryQuery`.
  - `interface CompanySearchResult { rows: CompanyListRow[]; total: number; page: number; pageSize: number; sort: string; dir: SortDir }`
  - `searchCompanies(country: CountryConfig, opts: { q?: string; page?: number; pageSize?: number; sort?: string | null; dir?: string | null }): Promise<CompanySearchResult>`
  - `const PAGE_SIZES = [25, 50, 100] as const` (exported)

- [ ] **Step 1: Write the failing tests**

First, update the THREE existing tests that use non-whitelisted page sizes (the new membership rule maps any non-{25,50,100} value to 50, which would silently change their row counts):

- `"returns a first page of rows with id and name"`: change `pageSize: 10` → `pageSize: 25` and `toHaveLength(10)` → `toHaveLength(25)`.
- `"filters by case-insensitive name substring"`: change `pageSize: 10` → `pageSize: 25`.
- `"paginates without overlap"`: change both `pageSize: 5` → `pageSize: 25`.
- `"clamps out-of-range pages to the last page"`: change `pageSize: 10` → `pageSize: 25` (the `lastPage` recomputation inside the test already adapts).

(`"falls back to sane defaults on non-finite page inputs"` needs no change — `Infinity` fails the membership check and still yields 50.)

Then replace the import line and add tests in `tests/queries.server.test.ts` (keep all other existing tests; the shape assertions on `row.id`/`row.name` keep working because the column keys preserve them):

```ts
import { PAGE_SIZES, getCountryStats, searchCompanies } from "~/lib/queries.server";
```

```ts
describe("searchCompanies sorting and columns", () => {
  it("returns all declared column keys plus active and industry fields", async () => {
    const result = await searchCompanies(ee, { pageSize: 25 });
    expect(result.pageSize).toBe(25);
    const row = result.rows[0];
    for (const col of ee.columns) {
      expect(row, `column ${col.key}`).toHaveProperty(col.key);
    }
    expect(row).toHaveProperty("active");
    expect(row).toHaveProperty("industry_code");
    expect(row).toHaveProperty("industry_label");
  });

  it("merges a primary industry for most companies on a page", async () => {
    const result = await searchCompanies(ee, { pageSize: 50 });
    const withIndustry = result.rows.filter((r) => r.industry_label);
    // ~96% of EE companies have a primary industry; a 50-row page having zero would mean the merge is broken.
    expect(withIndustry.length).toBeGreaterThan(0);
  });

  it("sorts by a whitelisted column in both directions", async () => {
    const asc = await searchCompanies(ee, { sort: "id", dir: "asc", pageSize: 5 });
    const desc = await searchCompanies(ee, { sort: "id", dir: "desc", pageSize: 5 });
    expect(asc.sort).toBe("id");
    expect(asc.dir).toBe("asc");
    expect(desc.dir).toBe("desc");
    expect(asc.rows[0].id).not.toEqual(desc.rows[0].id);
    const ascIds = asc.rows.map((r) => String(r.id));
    expect([...ascIds].sort()).toEqual(ascIds);
  });

  it("falls back to name asc on unknown sort keys and directions", async () => {
    const result = await searchCompanies(ee, {
      sort: "industry_label; DROP TABLE x",
      dir: "sideways",
      pageSize: 5,
    });
    expect(result.sort).toBe("name");
    expect(result.dir).toBe("asc");
    expect(result.rows).toHaveLength(5);
  });

  it("accepts only whitelisted page sizes", async () => {
    const result = await searchCompanies(ee, { pageSize: 37 });
    expect(result.pageSize).toBe(50);
    expect(PAGE_SIZES).toEqual([25, 50, 100]);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test queries`
Expected: FAIL — `PAGE_SIZES` not exported; `sort`/`dir` not returned; rows lack `legal_form`/`industry_*` keys.

- [ ] **Step 3: Rework searchCompanies**

Replace `app/lib/queries.server.ts` content below `getCountryStats` (keep the file header, imports, `CountryStats`, `getCountryStats`, and `clampInt` exactly as they are; update imports to include the new registry types):

```ts
import { chQuery } from "~/lib/clickhouse.server";
import {
  getSortColumn,
  type CountryConfig,
  type SortDir,
} from "~/lib/countries";

export const PAGE_SIZES = [25, 50, 100] as const;

export type CompanyListRow = Record<string, string | number | null> & {
  active: 0 | 1;
};

export interface CompanySearchResult {
  rows: CompanyListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
}

interface IndustryRow {
  company_id: string;
  industry_code: string | null;
  industry_label: string | null;
}

export async function searchCompanies(
  country: CountryConfig,
  opts: {
    q?: string;
    page?: number;
    pageSize?: number;
    sort?: string | null;
    dir?: string | null;
  },
): Promise<CompanySearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const q = (opts.q ?? "").trim();
  const sortColumn = getSortColumn(country, opts.sort ?? null);
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";

  const where = q ? `WHERE ${country.nameColumn} ILIKE {pattern:String}` : "";
  const params = q ? { pattern: `%${q}%` } : undefined;

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );
  const total = Number(countRows[0].total);

  // Clamp the requested page to the real page range (count runs first on purpose).
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(requestedPage, lastPage);

  const joinKeyExpr = country.industryJoinKeyExpr ?? country.idColumn;
  const selectList = [
    ...country.columns.map((c) => `${c.expr} AS ${c.key}`),
    `toUInt8(${country.activeExpr}) AS active`,
    ...(country.industryQuery ? [`toString(${joinKeyExpr}) AS __industry_key`] : []),
  ].join(",\n       ");

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     ${where}
     ORDER BY ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  if (country.industryQuery) {
    const ids = rows.map((r) => r.__industry_key ?? "").filter((v) => v !== "");
    const industries = ids.length
      ? await chQuery<IndustryRow>(country.industryQuery, { ids })
      : [];
    const byId = new Map(industries.map((i) => [i.company_id, i]));
    for (const row of rows) {
      const hit = byId.get(row.__industry_key ?? "");
      row.industry_code = hit?.industry_code ?? null;
      row.industry_label = hit?.industry_label ?? null;
      delete row.__industry_key;
    }
  }

  return { rows, total, page, pageSize, sort: sortColumn.key, dir };
}
```

Safety invariants preserved (do not weaken them): `sortColumn.expr` and every select expression come from the registry; `dir` is a two-value whitelist mapped to literal `ASC`/`DESC`; `pageSize` is membership-checked against `PAGE_SIZES`; `q` and `ids` are query params.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test queries`
Expected: PASS. The pre-existing "clamps out-of-range pages" and "paginates without overlap" tests must still pass unchanged; the pre-existing test asserting `pageSize` behavior on `Infinity` now expects 50 via the membership check (verify it still holds — `PAGE_SIZES.includes(Infinity)` is false → 50).

- [ ] **Step 5: Full gate and commit**

Run: `pnpm typecheck && pnpm test`
Expected: all green. NOTE: `app/routes/country-companies.tsx` still compiles because it only reads `result.rows[n].id/name/active`, `total`, `page`, `pageSize` — all still present. If typecheck flags the route's `CompanyRow` import (removed type), fix the route import minimally to `CompanyListRow` — Task 4 replaces this file anyway.

```bash
git add app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "feat(backoffice): dynamic columns, server-side sorting and industry merge"
```

---

### Task 3: Data-table component kit (TanStack + shadcn)

**Files:**
- Create: `app/components/data-table/url.ts` (pure URL-state helpers)
- Create: `app/components/data-table/url.test.ts`
- Create: `app/components/data-table/data-table.tsx` (generic table shell)
- Create: `app/components/data-table/column-header.tsx` (sortable header control)
- Create: `app/components/data-table/pagination.tsx` (pagination bar)
- Modify: `package.json` (add `@tanstack/react-table`)

**Interfaces:**
- Consumes: `SortDir` (Task 1), shadcn `ui/table`, `ui/button`, `ui/select`, lucide icons.
- Produces (Task 4 relies on):
  - `tableSearch(current: URLSearchParams, patch: Partial<{ q: string; page: number; pageSize: number; sort: string; dir: SortDir }>): string` — returns a `?...` string; setting `sort`/`pageSize`/`q` deletes `page`; values equal to defaults (`page=1`, `dir=asc` handled by caller) are still written explicitly except deleted `page`.
  - `nextSortDir(currentSort: string, currentDir: SortDir, key: string): SortDir` — `asc` when switching columns, toggles when re-clicking.
  - `<DataTable columns={ColumnDef<T>[]} data={T[]} />` — renders shadcn table, `overflow-x-auto` wrapper, `—` for null/empty handled by cell renderers, built-in empty state row ("No companies found.").
  - `<DataTableColumnHeader label sortKey currentSort currentDir />` — non-sortable renders plain label; sortable renders a ghost Button-as-Link with ArrowUp/ArrowDown (active) or ChevronsUpDown (inactive).
  - `<DataTablePagination total page pageSize sort dir />` — totals text, page-size Select, Page X of Y, first/prev/next/last Link-buttons with disabled bounds.

- [ ] **Step 1: Install the dependency and ensure select exists**

```bash
pnpm add @tanstack/react-table
pnpm dlx shadcn@latest add select
```

(`select` may already exist from the dashboard-01 block — the add is a no-op then.)

- [ ] **Step 2: Write the failing URL-helper tests**

`app/components/data-table/url.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { nextSortDir, tableSearch } from "~/components/data-table/url";

describe("tableSearch", () => {
  it("preserves existing params and patches page", () => {
    const current = new URLSearchParams("q=grupp&sort=status&dir=desc&page=3");
    expect(tableSearch(current, { page: 4 })).toBe("?q=grupp&sort=status&dir=desc&page=4");
  });

  it("resets page when sort changes", () => {
    const current = new URLSearchParams("q=grupp&page=3");
    const s = tableSearch(current, { sort: "status", dir: "asc" });
    expect(s).toContain("sort=status");
    expect(s).toContain("dir=asc");
    expect(s).not.toContain("page=");
    expect(s).toContain("q=grupp");
  });

  it("resets page when pageSize changes", () => {
    const current = new URLSearchParams("page=9");
    expect(tableSearch(current, { pageSize: 100 })).toBe("?pageSize=100");
  });
});

describe("nextSortDir", () => {
  it("starts asc on a new column", () => {
    expect(nextSortDir("name", "asc", "status")).toBe("asc");
  });
  it("toggles on the same column", () => {
    expect(nextSortDir("status", "asc", "status")).toBe("desc");
    expect(nextSortDir("status", "desc", "status")).toBe("asc");
  });
});
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pnpm test url`
Expected: FAIL — cannot resolve `~/components/data-table/url`.

- [ ] **Step 4: Implement the URL helpers**

`app/components/data-table/url.ts`:

```ts
import type { SortDir } from "~/lib/countries";

export interface TablePatch {
  q?: string;
  page?: number;
  pageSize?: number;
  sort?: string;
  dir?: SortDir;
}

/**
 * Builds the next table URL search string from the current params and a patch.
 * Changing q, sort, or pageSize resets pagination (deletes page).
 */
export function tableSearch(current: URLSearchParams, patch: TablePatch): string {
  const next = new URLSearchParams(current);
  const resets = patch.q !== undefined || patch.sort !== undefined || patch.pageSize !== undefined;
  if (resets) next.delete("page");
  if (patch.q !== undefined) next.set("q", patch.q);
  if (patch.sort !== undefined) next.set("sort", patch.sort);
  if (patch.dir !== undefined) next.set("dir", patch.dir);
  if (patch.pageSize !== undefined) next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}

export function nextSortDir(currentSort: string, currentDir: SortDir, key: string): SortDir {
  if (currentSort !== key) return "asc";
  return currentDir === "asc" ? "desc" : "asc";
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pnpm test url`
Expected: PASS (5 tests).

- [ ] **Step 6: Implement the three components**

`app/components/data-table/data-table.tsx`:

```tsx
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export function DataTable<TData>({
  columns,
  data,
}: {
  columns: ColumnDef<TData, unknown>[];
  data: TData[];
}) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
  });

  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className="min-w-[56rem]">
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <TableHead key={header.id} className="whitespace-nowrap">
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.length === 0 ? (
            <TableRow>
              <TableCell
                colSpan={columns.length}
                className="text-muted-foreground h-24 text-center"
              >
                No companies found.
              </TableCell>
            </TableRow>
          ) : (
            table.getRowModel().rows.map((row) => (
              <TableRow key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="align-top">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  );
}
```

`app/components/data-table/column-header.tsx`:

```tsx
import { Link, useSearchParams } from "react-router";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import type { SortDir } from "~/lib/countries";
import { Button } from "~/components/ui/button";
import { nextSortDir, tableSearch } from "~/components/data-table/url";

export function DataTableColumnHeader({
  label,
  sortKey,
  currentSort,
  currentDir,
}: {
  label: string;
  /** undefined → not sortable, render plain label */
  sortKey?: string;
  currentSort: string;
  currentDir: SortDir;
}) {
  const [searchParams] = useSearchParams();
  if (!sortKey) {
    return <span className="text-muted-foreground text-xs font-medium uppercase tracking-wide">{label}</span>;
  }
  const isActive = currentSort === sortKey;
  const target = tableSearch(searchParams, {
    sort: sortKey,
    dir: nextSortDir(currentSort, currentDir, sortKey),
  });
  const Icon = !isActive ? ChevronsUpDown : currentDir === "asc" ? ArrowUp : ArrowDown;
  return (
    <Button
      variant="ghost"
      size="sm"
      className="-ml-2 h-7 gap-1 text-xs font-medium uppercase tracking-wide data-[active=true]:text-foreground"
      data-active={isActive}
      render={<Link to={target} preventScrollReset />}
    >
      {label}
      <Icon className="size-3.5" />
    </Button>
  );
}
```

`app/components/data-table/pagination.tsx`:

```tsx
import { Link, useSearchParams } from "react-router";
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";
import type { SortDir } from "~/lib/countries";
import { PAGE_SIZES } from "~/lib/queries.server";
import { Button } from "~/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { tableSearch } from "~/components/data-table/url";
import { useNavigate } from "react-router";

const nf = new Intl.NumberFormat("en-US");

export function DataTablePagination({
  total,
  page,
  pageSize,
}: {
  total: number;
  page: number;
  pageSize: number;
}) {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const lastPage = Math.max(1, Math.ceil(total / pageSize));

  function nav(target: number, disabled: boolean, icon: React.ReactNode, label: string) {
    if (disabled) {
      return (
        <Button variant="outline" size="icon-sm" disabled aria-label={label}>
          {icon}
        </Button>
      );
    }
    return (
      <Button
        variant="outline"
        size="icon-sm"
        aria-label={label}
        render={<Link to={tableSearch(searchParams, { page: target })} preventScrollReset />}
      >
        {icon}
      </Button>
    );
  }

  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <span className="text-muted-foreground text-sm tabular-nums">
        {nf.format(total)} companies
      </span>
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground text-sm">Rows per page</span>
          <Select
            value={String(pageSize)}
            onValueChange={(value: string) =>
              navigate(tableSearch(searchParams, { pageSize: Number(value) }), {
                preventScrollReset: true,
              })
            }
          >
            <SelectTrigger className="h-8 w-[4.5rem]" size="sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PAGE_SIZES.map((size) => (
                <SelectItem key={size} value={String(size)}>
                  {size}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <span className="text-sm tabular-nums">
          Page {nf.format(page)} of {nf.format(lastPage)}
        </span>
        <div className="flex items-center gap-1.5">
          {nav(1, page <= 1, <ChevronsLeft className="size-4" />, "First page")}
          {nav(page - 1, page <= 1, <ChevronLeft className="size-4" />, "Previous page")}
          {nav(page + 1, page >= lastPage, <ChevronRight className="size-4" />, "Next page")}
          {nav(lastPage, page >= lastPage, <ChevronsRight className="size-4" />, "Last page")}
        </div>
      </div>
    </div>
  );
}
```

API adaptation note (Base UI, shadcn v4.13): the exact prop names on the installed `Select`/`Button` may differ from the snippets (`onValueChange` vs `onChange`, `size="icon-sm"` vs `size="icon"` — check `app/components/ui/select.tsx` and `button.tsx` and adapt minimally; structure and behavior above are the spec). `PAGE_SIZES` importing from `queries.server.ts` into a component is safe: it's a `const` array (no server-only code executes at import in the client bundle) — BUT React Router build may still complain about importing a `.server.ts` module from client code. If it does, move `PAGE_SIZES` to `app/lib/countries.ts` (NOT server-only) and re-export from `queries.server.ts` for the tests; report the move.

- [ ] **Step 7: Verify and commit**

Run: `pnpm typecheck && pnpm test`
Expected: green (components compile; nothing renders them yet).

```bash
git add app/components/data-table package.json pnpm-lock.yaml app/components/ui
git commit -m "feat(backoffice): add data-table component kit with url-driven sort and pagination"
```

---

### Task 4: Rewire the companies route onto the data table

**Files:**
- Create: `app/components/data-table/company-columns.tsx` (ColumnDef builder from registry config)
- Modify: `app/routes/country-companies.tsx` (replace table + pagination with the kit)

**Interfaces:**
- Consumes: everything from Tasks 1–3; `searchCompanies` new signature.
- Produces: final `/:country/companies?q=&page=&pageSize=&sort=&dir=` behavior.

- [ ] **Step 1: Implement the column-def builder**

`app/components/data-table/company-columns.tsx`:

```tsx
import type { ColumnDef } from "@tanstack/react-table";
import type { CompanyColumn, CountryConfig, SortDir } from "~/lib/countries";
import type { CompanyListRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { DataTableColumnHeader } from "~/components/data-table/column-header";

const EMPTY = <span className="text-muted-foreground">—</span>;

function text(value: unknown) {
  const s = value == null ? "" : String(value);
  if (s === "") return EMPTY;
  return s;
}

function cellFor(col: CompanyColumn) {
  return ({ row }: { row: { original: CompanyListRow } }) => {
    const value = row.original[col.key];
    switch (col.kind) {
      case "id":
        return (
          <span className="text-muted-foreground font-mono text-xs whitespace-nowrap">
            {text(value)}
          </span>
        );
      case "date":
        return <span className="tabular-nums whitespace-nowrap">{text(value)}</span>;
      case "status":
        return (
          <Badge variant={row.original.active ? "default" : "outline"}>
            {text(value)}
          </Badge>
        );
      default:
        if (col.key === "name") {
          const s = value == null ? "" : String(value);
          return (
            <span className="block max-w-[22rem] truncate font-medium" title={s}>
              {s === "" ? EMPTY : s}
            </span>
          );
        }
        return <span className="block max-w-[14rem] truncate">{text(value)}</span>;
    }
  };
}

export function buildCompanyColumns(
  country: CountryConfig,
  sort: string,
  dir: SortDir,
): ColumnDef<CompanyListRow, unknown>[] {
  const defs: ColumnDef<CompanyListRow, unknown>[] = country.columns.map((col) => ({
    id: col.key,
    header: () => (
      <DataTableColumnHeader
        label={col.label}
        sortKey={col.sortable ? col.key : undefined}
        currentSort={sort}
        currentDir={dir}
      />
    ),
    cell: cellFor(col),
  }));

  if (country.industryQuery) {
    const industryDef: ColumnDef<CompanyListRow, unknown> = {
      id: "industry",
      header: () => (
        <DataTableColumnHeader label="Industry" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const code = row.original.industry_code;
        const label = row.original.industry_label;
        if (!code && !label) return EMPTY;
        return (
          <span className="flex max-w-[20rem] items-baseline gap-1.5">
            <span className="text-muted-foreground font-mono text-xs">{code}</span>
            <span className="truncate" title={label ? String(label) : undefined}>
              {label}
            </span>
          </span>
        );
      },
    };
    // Insert industry right after the name column.
    const nameIndex = defs.findIndex((d) => d.id === "name");
    defs.splice(nameIndex + 1, 0, industryDef);
  }

  return defs;
}
```

- [ ] **Step 2: Replace the route**

Replace `app/routes/country-companies.tsx` entirely:

```tsx
import { Form } from "react-router";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { searchCompanies } from "~/lib/queries.server";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildCompanyColumns } from "~/components/data-table/company-columns";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const result = await searchCompanies(country, {
    q: url.searchParams.get("q") ?? "",
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
  });
  return { q: url.searchParams.get("q") ?? "", result, countryCode: country.code };
}

export default function CountryCompanies({ loaderData, params }: Route.ComponentProps) {
  const { q, result } = loaderData;
  const country = getCountry(params.country)!;
  const columns = buildCompanyColumns(country, result.sort, result.dir);

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

      <DataTable columns={columns} data={result.rows} />

      <DataTablePagination
        total={result.total}
        page={result.page}
        pageSize={result.pageSize}
      />
    </>
  );
}
```

Notes: `getCountry` in the component is safe (static registry, isomorphic). The search `Form` carries only `q`, so submitting drops `page`/`sort`/`dir` — search resets to default sort page 1, which is the intended reset behavior.

- [ ] **Step 3: Verify in the dev server**

Run: `pnpm typecheck && pnpm test`, then `pnpm dev` (port 5183) and check (Estonia):

```bash
curl -s 'http://localhost:5183/ee/companies' | grep -c 'Industry'            # >= 1 (header present)
curl -s 'http://localhost:5183/ee/companies' | grep -c 'Rows per page'       # >= 1 (pagination bar)
# sorting actually changes order (first data row differs between directions):
A=$(curl -s 'http://localhost:5183/ee/companies?sort=id&dir=asc'  | grep -o 'font-mono[^<]*</span>' | head -1)
B=$(curl -s 'http://localhost:5183/ee/companies?sort=id&dir=desc' | grep -o 'font-mono[^<]*</span>' | head -1)
[ "$A" != "$B" ] && echo SORT-OK
curl -s 'http://localhost:5183/ee/companies?pageSize=25' | grep -c 'Page 1 of' # >= 1
curl -s 'http://localhost:5183/ee/companies?q=grupp' | grep -ci grupp          # >= 1
```

Also click through in a browser: sort arrows toggle, page-size select navigates, first/last buttons land correctly, industry column shows `code label` pairs. Then kill the dev server.

- [ ] **Step 4: Commit**

```bash
git add app/components/data-table/company-columns.tsx app/routes/country-companies.tsx
git commit -m "feat(backoffice): companies data table with sorting and rich columns"
```

---

### Task 5: Big-country smoke, README, full gate

**Files:**
- Modify: `README.md` (document table URL params + sorting rules)

**Interfaces:** consumes everything; produces the merge-ready state.

- [ ] **Step 1: Big-country smoke (manual, dev server)**

With `pnpm dev` running, time the heavy cases (France 29.7M, Brazil 68.6M):

```bash
time curl -s -o /dev/null 'http://localhost:5183/fr/companies?sort=status&dir=desc'
time curl -s -o /dev/null 'http://localhost:5183/br/companies?sort=registered&dir=desc'
time curl -s -o /dev/null 'http://localhost:5183/br/companies?q=petrobras'
```

Expected: each completes (a few seconds is acceptable for a backoffice; ClickHouse top-N handles ORDER BY + LIMIT without full sort). If any errors (e.g. memory limit on a sort column), report the exact ClickHouse error — do not silently drop the column; the fallback decision (make that column unsortable) is a one-line registry change but must be reported.

- [ ] **Step 2: README addition**

Add to `README.md` under Structure:

```markdown
## Companies table

URL-driven state on `/{country}/companies`:
`?q=` name search, `?sort=` column key + `?dir=asc|desc` (whitelisted against
`countries.ts` column config; unknown values fall back to name asc),
`?page=`, `?pageSize=25|50|100`. The industry column is populated by a second
per-page lookup (`industryQuery` in `countries.ts`) and is not sortable by
design — sorting happens on base-table columns only so the 30–70M-row
countries stay fast. Add columns per country in `countries.ts` (`columns`),
never by editing SQL in the route.
```

- [ ] **Step 3: Full gate**

Run: `pnpm typecheck && pnpm test && pnpm build`
Expected: all green. Then `pnpm start`, curl `http://localhost:3000/ee/companies?sort=status&dir=desc` → HTML contains "Industry" and "Rows per page"; kill, port free.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(backoffice): document companies table url state and sorting rules"
```

---

## Out of scope (explicitly)

- Sorting by industry (post-pagination join — would require joining before pagination; revisit with a materialized primary-industry column if ever needed).
- Column visibility toggles, row selection, CSV export, sticky first column.
- Industry filter facet (natural follow-up: `WHERE id IN (SELECT ... FROM {cc}_industries WHERE nace_normalized_code = ...)`).
- FR/BR name-search performance work (`ngrambf_v1` / lowercase materialized column) — separate, already-logged follow-up.
