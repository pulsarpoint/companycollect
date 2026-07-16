# Backoffice Facet Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add categorical filtering to `/:country/companies`: a filter sidebar (shadcn Sheet) with one server-searched multi-select combobox per filterable column — including **industry with canonical NACE English labels** — where selected values live in the URL and are applied by the loader.

**Architecture:** The registry gains `filterable: true` flags on categorical columns plus per-country industry facet/filter SQL. A server-side facet cache (`facets.server.ts`, in-memory Map + TTL) loads each facet's **full** value+count list from ClickHouse once (`GROUP BY expr ORDER BY count() DESC`), then a resource route (`/:country/facet-options?column=&q=`) serves diacritic-insensitive, prefix-ranked typeahead matches from that cache — the browser never triggers per-keystroke ClickHouse queries. Selected values ride the URL as repeated `f_<key>` params; `searchCompanies` translates them into `AND expr IN {f_key:Array(String)}` conditions (industry via a registry-defined semi-join). The typeahead text inside a combobox is ephemeral component state — only *selections* go in the URL.

**Tech Stack:** existing stack (React Router 8 SSR, shadcn v4.13 Base UI, `chQuery`) + shadcn `sheet`, `command` (cmdk), `popover` components. No new backend dependencies — the cache is a module-level Map (deliberately NOT SQLite: the data is tiny, derivable, and recomputed by ClickHouse in <1s; the cache sits behind two functions so a persistent store can replace it later without touching callers).

## Global Constraints

- App: `corpscout/services/backoffice`. pnpm, Node 22.22+, RR8 SSR, Base UI shadcn (`render` prop, never `asChild`).
- URL is the single source of truth for SELECTED filters: repeated params `f_<columnKey>=<value>` (e.g. `?f_status=active&f_status=inactive&f_industry=6201`). Any filter change deletes `page`. The typeahead query inside a combobox is ephemeral UI state and never appears in the URL.
- SQL safety unchanged: filter COLUMNS are whitelisted via the registry (`filterable` flag / `industryFilterExpr`); filter VALUES are always bound as `{f_<key>:Array(String)}` query params; facet SQL identifiers come only from the registry. Read-only ClickHouse.
- Facet cache: in-memory, per-process, key `"<country>:<facetKey>"`, TTL 24h, stores the FULL value list (capped `LIMIT 50000` in SQL). Display caps: empty query → top 200 by count; non-empty query → top 50 ranked matches.
- Typeahead ranking: case- and diacritic-insensitive (NFD + strip combining marks + lowercase) substring match; prefix matches rank before mid-string matches; ties by count descending. No fuzzy/typo-tolerance library in v1.
- Industry labels default to **canonical NACE English** from `nace_categories` (`is_current = 1`, `description_en`), falling back source `description_en` → source `description_original` → the code itself. This applies to BOTH the facet options and the table's industry column (existing `industryQuery` strings are updated in Task 5).
- Combobox fetches are debounced 200ms; cmdk client filtering disabled (`shouldFilter={false}`) — the server result IS the list.
- Filters must be applied server-side in the loader (SSR — filtered rows and totals in initial HTML).
- Integration tests: real ClickHouse; Estonia for behavior tests; the all-countries sweep extends the existing pattern.
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths.

## Ground truth (verified live, 2026-07-16)

- `nace_categories` (is_current=1): 1,047 rows, `normalized_code` lengths 1–4 (651 four-digit classes), `description_en` populated. This is the canonical English label source.
- Code formats per industry source: **no** = 5-digit national (join `substring(code,1,4)`); **fi** = `nace_normalized_code` is NULL everywhere — the real key is `source_industry_code` (5-digit TOL2008, Nullable; join `substring(code,1,4)`); **ee/gb/sk/se** = 4-digit (direct join); **cz** = mixed 2/3/4-digit (direct join works — `nace_categories` holds all levels); **fr** = 4-digit but ~6M rows with EMPTY code (facet must exclude `''`); **br** = 7-digit CNAE, NACE mapping table nearly empty (labels fall back to the CNAE code until the `brazil_comp_cnae` fixture is expanded); **lv** = `lv_companies_nace` is 100% EMPTY (`nace_code`/`nace_label` = `''` on all 485,380 rows) → **LV gets NO industry facet/filter** (fields omitted; sidebar simply won't show Industry for LV). Separately flagged to the user as a pipeline gap.
- Filterable columns per country (all `LowCardinality`-ish categorical): `status` (all 10), `legal_form` (all except br), `size` (br), `place` (ee, lv, gb, fr, br, cz, sk). Never filterable: `id`, `name`, `registered`, `website`, `trade_name`.

---

### Task 1: Registry `filterable` flags

**Files:**
- Modify: `app/lib/countries.ts` (one-word type addition + flags on existing entries)
- Modify: `app/lib/countries.test.ts`

**Interfaces:**
- Consumes: existing `CompanyColumn`, `COUNTRIES`.
- Produces: `CompanyColumn.filterable?: boolean` — set `filterable: true` on: every country's `status` column; every `legal_form` column (no, fi, se, ee, lv, gb, fr, cz, sk); br's `size`; every `place` column (ee, lv, gb, fr, br, cz, sk). All other columns get no flag (undefined = not filterable).

- [ ] **Step 1: Write the failing tests**

Append to `app/lib/countries.test.ts` inside the existing `describe("company columns", ...)`:

```ts
it("every country has a filterable status; only text/status kinds are filterable", () => {
  for (const c of COUNTRIES) {
    const filterable = c.columns.filter((col) => col.filterable);
    expect(filterable.length, c.code).toBeGreaterThan(0);
    expect(
      c.columns.find((col) => col.kind === "status")?.filterable,
      c.code,
    ).toBe(true);
    for (const col of filterable) {
      expect(["text", "status"], `${c.code}:${col.key}`).toContain(col.kind);
      expect(col.key, `${c.code}:${col.key}`).toMatch(/^[a-z_]+$/);
    }
  }
});

it("id, name, registered are never filterable", () => {
  for (const c of COUNTRIES) {
    for (const key of ["id", "name", "registered"]) {
      expect(
        c.columns.find((col) => col.key === key)?.filterable,
        `${c.code}:${key}`,
      ).toBeFalsy();
    }
  }
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test countries`
Expected: FAIL — no column has `filterable`.

- [ ] **Step 3: Implement**

In `app/lib/countries.ts`, extend `CompanyColumn`:

```ts
export interface CompanyColumn {
  // ... existing fields unchanged ...
  /** Filterable columns become facet candidates (categorical values only). */
  filterable?: boolean;
}
```

Then add `filterable: true` to exactly these existing column entries (no other edits):
- `status` in ALL 10 countries.
- `legal_form` in no, fi, se, ee, lv, gb, fr, cz, sk.
- `size` in br.
- `place` in ee, lv, gb, fr, br, cz, sk.

Example (Estonia's entries after the edit — apply the same pattern everywhere):

```ts
{ key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_en, ''), legal_form_original)", sortable: true, kind: "text", filterable: true },
{ key: "status", label: "Status", expr: "coalesce(nullIf(status_en, ''), status_original)", sortable: true, kind: "status", filterable: true },
{ key: "place", label: "Location", expr: "location", sortable: false, kind: "text", filterable: true },
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test countries` → PASS. Then `pnpm typecheck && pnpm test` → all green.

- [ ] **Step 5: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts
git commit -m "feat(backoffice): flag filterable categorical columns in registry"
```

---

### Task 2: Facet cache + typeahead ranking (`facets.server.ts`)

**Files:**
- Create: `app/lib/facets.server.ts`
- Create: `tests/facets.server.test.ts`

**Interfaces:**
- Consumes: `chQuery` (`~/lib/clickhouse.server`), `CountryConfig`/`CompanyColumn` (`~/lib/countries`).
- Produces (Tasks 3–5 rely on):
  - `interface FacetOption { value: string; label: string; count: number }` (for plain columns `label === value`)
  - `normalizeFacetText(s: string): string`
  - `rankFacetOptions(options: FacetOption[], q: string, limit: number): FacetOption[]`
  - `getFacetOptions(country: CountryConfig, facetKey: string): Promise<FacetOption[]>` — cached full list; throws `Error("unknown facet: ...")` for keys that aren't a filterable column (the `"industry"` facet key is added in Task 5)
  - `searchFacetOptions(country: CountryConfig, facetKey: string, q: string): Promise<FacetOption[]>` — `q` empty → top 200; else `rankFacetOptions(all, q, 50)`
  - `clearFacetCache(): void` (tests)

- [ ] **Step 1: Write the failing tests**

`tests/facets.server.test.ts`:

```ts
import { beforeEach, describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import {
  clearFacetCache,
  getFacetOptions,
  normalizeFacetText,
  rankFacetOptions,
  searchFacetOptions,
  type FacetOption,
} from "~/lib/facets.server";

const ee = getCountry("ee")!;

beforeEach(() => clearFacetCache());

describe("normalizeFacetText", () => {
  it("lowercases and strips diacritics", () => {
    expect(normalizeFacetText("Osaühing")).toBe("osauhing");
    expect(normalizeFacetText("São PAULO")).toBe("sao paulo");
    expect(normalizeFacetText("Řím")).toBe("rim");
  });
});

describe("rankFacetOptions", () => {
  const options: FacetOption[] = [
    { value: "Osaühing", label: "Osaühing", count: 100 },
    { value: "Aktsiaselts", label: "Aktsiaselts", count: 50 },
    { value: "Usaldusühing", label: "Usaldusühing", count: 10 },
  ];

  it("matches diacritic-insensitively anywhere in the string", () => {
    // Both contain "ühing" mid-string (no prefix match) → ordered by count desc.
    const hits = rankFacetOptions(options, "uhing", 50);
    expect(hits.map((o) => o.value)).toEqual(["Osaühing", "Usaldusühing"]);
  });

  it("ranks prefix matches before substring matches, ties by count desc", () => {
    const hits = rankFacetOptions(options, "usaldus", 50);
    expect(hits[0].value).toBe("Usaldusühing");
    const sub = rankFacetOptions(options, "ühing", 50);
    // both Osaühing (substring) and Usaldusühing (substring): count desc
    expect(sub.map((o) => o.value)).toEqual(["Osaühing", "Usaldusühing"]);
  });

  it("matches against the label too", () => {
    const labeled: FacetOption[] = [
      { value: "6201", label: "Computer programming activities", count: 5 },
    ];
    expect(rankFacetOptions(labeled, "programming", 50)).toHaveLength(1);
    expect(rankFacetOptions(labeled, "6201", 50)).toHaveLength(1);
  });

  it("respects the limit", () => {
    const many: FacetOption[] = Array.from({ length: 100 }, (_, i) => ({
      value: `v${i}`, label: `v${i}`, count: 100 - i,
    }));
    expect(rankFacetOptions(many, "v", 50)).toHaveLength(50);
  });
});

describe("facet cache against live ClickHouse (Estonia)", () => {
  it("loads status options with counts, sorted desc, no empties", async () => {
    const options = await getFacetOptions(ee, "status");
    expect(options.length).toBeGreaterThan(0);
    for (const o of options) {
      expect(o.value).not.toBe("");
      expect(o.count).toBeGreaterThan(0);
    }
    const counts = options.map((o) => o.count);
    expect([...counts].sort((a, b) => b - a)).toEqual(counts);
  });

  it("serves the second call from cache (same array reference)", async () => {
    const first = await getFacetOptions(ee, "status");
    const second = await getFacetOptions(ee, "status");
    expect(second).toBe(first);
  });

  it("rejects unknown or non-filterable facet keys", async () => {
    await expect(getFacetOptions(ee, "name")).rejects.toThrow(/unknown facet/i);
    await expect(getFacetOptions(ee, "id; DROP")).rejects.toThrow(/unknown facet/i);
  });

  it("searchFacetOptions: empty q caps at 200, typed q ranks matches", async () => {
    const top = await searchFacetOptions(ee, "legal_form", "");
    expect(top.length).toBeGreaterThan(0);
    expect(top.length).toBeLessThanOrEqual(200);
    const typed = await searchFacetOptions(ee, "legal_form", top[0].value.slice(0, 4));
    expect(typed.length).toBeGreaterThan(0);
    expect(typed.length).toBeLessThanOrEqual(50);
  });
});
```

Note on the first `rankFacetOptions` test: "uhing" is a substring of both "Osaühing" and "Usaldusühing" — the expected order is by count desc → `["Osaühing", "Usaldusühing"]`. Fix that assertion to match (the inline comment marks it); the point of the test is diacritic-insensitive substring matching.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test facets`
Expected: FAIL — cannot resolve `~/lib/facets.server`.

- [ ] **Step 3: Implement**

`app/lib/facets.server.ts`:

```ts
import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

export interface FacetOption {
  value: string;
  label: string;
  count: number;
}

const TTL_MS = 24 * 60 * 60 * 1000;
const EMPTY_Q_LIMIT = 200;
const TYPED_Q_LIMIT = 50;

const cache = new Map<string, { loadedAt: number; options: FacetOption[] }>();

export function clearFacetCache(): void {
  cache.clear();
}

/** Lowercase + strip diacritics so "Osaühing" matches "osauhing". */
export function normalizeFacetText(s: string): string {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

/**
 * Substring match on normalized value OR label; prefix matches first,
 * ties by count descending.
 */
export function rankFacetOptions(
  options: FacetOption[],
  q: string,
  limit: number,
): FacetOption[] {
  const needle = normalizeFacetText(q);
  const prefix: FacetOption[] = [];
  const substring: FacetOption[] = [];
  for (const option of options) {
    const value = normalizeFacetText(option.value);
    const label = normalizeFacetText(option.label);
    if (value.startsWith(needle) || label.startsWith(needle)) {
      prefix.push(option);
    } else if (value.includes(needle) || label.includes(needle)) {
      substring.push(option);
    }
  }
  // Input lists are already count-sorted, so group order is preserved.
  return [...prefix, ...substring].slice(0, limit);
}

function facetSql(country: CountryConfig, facetKey: string): string {
  const column = country.columns.find(
    (c) => c.filterable && c.key === facetKey,
  );
  if (!column) throw new Error(`unknown facet: ${facetKey}`);
  // Identifiers/expressions come from the static registry only.
  return `SELECT toString(${column.expr}) AS value,
       toString(${column.expr}) AS label,
       count() AS cnt
FROM ${country.companiesTable}
WHERE toString(${column.expr}) != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`;
}

export async function getFacetOptions(
  country: CountryConfig,
  facetKey: string,
): Promise<FacetOption[]> {
  const cacheKey = `${country.code}:${facetKey}`;
  const hit = cache.get(cacheKey);
  if (hit && Date.now() - hit.loadedAt < TTL_MS) return hit.options;

  const rows = await chQuery<{ value: string; label: string; cnt: string }>(
    facetSql(country, facetKey),
  );
  const options: FacetOption[] = rows.map((r) => ({
    value: r.value,
    label: r.label,
    count: Number(r.cnt),
  }));
  cache.set(cacheKey, { loadedAt: Date.now(), options });
  return options;
}

export async function searchFacetOptions(
  country: CountryConfig,
  facetKey: string,
  q: string,
): Promise<FacetOption[]> {
  const options = await getFacetOptions(country, facetKey);
  const trimmed = q.trim();
  if (trimmed === "") return options.slice(0, EMPTY_Q_LIMIT);
  return rankFacetOptions(options, trimmed, TYPED_Q_LIMIT);
}
```

(Nullable exprs: `toString(NULL)` propagates NULL, `NULL != ''` is not true → excluded by the WHERE. Task 5 extends `facetSql`/`getFacetOptions` for the `"industry"` facet key.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test facets` → PASS. Then `pnpm typecheck && pnpm test` → green.

- [ ] **Step 5: Commit**

```bash
git add app/lib/facets.server.ts tests/facets.server.test.ts
git commit -m "feat(backoffice): facet option cache with server-side typeahead ranking"
```

---

### Task 3: URL filter parsing + WHERE composition in `searchCompanies`

**Files:**
- Create: `app/lib/filters.ts` (isomorphic — also used by UI)
- Create: `app/lib/filters.test.ts`
- Modify: `app/lib/queries.server.ts` (`searchCompanies` gains `filters`)
- Modify: `tests/queries.server.test.ts`

**Interfaces:**
- Consumes: `CountryConfig` (Task 1 flags).
- Produces:
  - `const FILTER_PREFIX = "f_"`
  - `type CompanyFilters = Record<string, string[]>`
  - `filterableFacetKeys(country: CountryConfig): string[]` — filterable column keys (Task 5 appends `"industry"`)
  - `parseFilters(searchParams: URLSearchParams, country: CountryConfig): CompanyFilters` — reads repeated `f_<key>` params for whitelisted keys only; trims, drops empties, dedupes, caps 50 values/filter
  - `searchCompanies(country, { ..., filters?: CompanyFilters })` — result unchanged in shape; `total` reflects filters.

- [ ] **Step 1: Write the failing unit tests**

`app/lib/filters.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { filterableFacetKeys, parseFilters } from "~/lib/filters";

const ee = getCountry("ee")!;

describe("filterableFacetKeys", () => {
  it("lists only filterable column keys", () => {
    const keys = filterableFacetKeys(ee);
    expect(keys).toContain("status");
    expect(keys).toContain("legal_form");
    expect(keys).not.toContain("id");
    expect(keys).not.toContain("name");
  });
});

describe("parseFilters", () => {
  it("reads repeated params for whitelisted keys only", () => {
    const sp = new URLSearchParams(
      "f_status=Registered&f_status=Deleted&f_name=hack&f_bogus=x&q=grupp",
    );
    expect(parseFilters(sp, ee)).toEqual({
      status: ["Registered", "Deleted"],
    });
  });

  it("trims, drops empties, dedupes", () => {
    const sp = new URLSearchParams("f_status=+A+&f_status=A&f_status=");
    expect(parseFilters(sp, ee)).toEqual({ status: ["A"] });
  });

  it("returns empty object when nothing matches", () => {
    expect(parseFilters(new URLSearchParams("q=x&page=2"), ee)).toEqual({});
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm test filters` — FAIL: cannot resolve `~/lib/filters`.

- [ ] **Step 3: Implement `app/lib/filters.ts`**

```ts
import type { CountryConfig } from "~/lib/countries";

export const FILTER_PREFIX = "f_";
const MAX_VALUES_PER_FILTER = 50;

export type CompanyFilters = Record<string, string[]>;

/** Facet keys this country supports. Task 5 appends "industry". */
export function filterableFacetKeys(country: CountryConfig): string[] {
  return country.columns.filter((c) => c.filterable).map((c) => c.key);
}

/** Extracts whitelisted f_<key> params. Unknown keys are ignored, never errors. */
export function parseFilters(
  searchParams: URLSearchParams,
  country: CountryConfig,
): CompanyFilters {
  const filters: CompanyFilters = {};
  for (const key of filterableFacetKeys(country)) {
    const values = [
      ...new Set(
        searchParams
          .getAll(`${FILTER_PREFIX}${key}`)
          .map((v) => v.trim())
          .filter((v) => v !== ""),
      ),
    ].slice(0, MAX_VALUES_PER_FILTER);
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}
```

- [ ] **Step 4: Unit tests green, then write the failing integration tests**

Run: `pnpm test filters` → PASS. Append to `tests/queries.server.test.ts`:

```ts
describe("searchCompanies with filters", () => {
  it("applies a single-value filter to rows and total", async () => {
    const unfiltered = await searchCompanies(ee, { pageSize: 25 });
    const statusOptions = await getFacetOptions(ee, "status");
    const top = statusOptions[0].value;
    const filtered = await searchCompanies(ee, {
      pageSize: 25,
      filters: { status: [top] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThanOrEqual(unfiltered.total);
    for (const row of filtered.rows) {
      expect(String(row.status)).toBe(top);
    }
  });

  it("multi-value filter is a union (IN)", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    if (statusOptions.length < 2) return; // data-dependent guard
    const [a, b] = statusOptions;
    const fa = await searchCompanies(ee, { filters: { status: [a.value] } });
    const fb = await searchCompanies(ee, { filters: { status: [b.value] } });
    const both = await searchCompanies(ee, {
      filters: { status: [a.value, b.value] },
    });
    expect(both.total).toBe(fa.total + fb.total);
  });

  it("filters compose with q search", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    const result = await searchCompanies(ee, {
      q: "grupp",
      filters: { status: [statusOptions[0].value] },
    });
    for (const row of result.rows) {
      expect(String(row.name).toLowerCase()).toContain("grupp");
      expect(String(row.status)).toBe(statusOptions[0].value);
    }
  });

  it("ignores filter keys not in the registry whitelist", async () => {
    const result = await searchCompanies(ee, {
      pageSize: 25,
      filters: { "bogus; DROP": ["x"], name: ["y"] } as never,
    });
    expect(result.rows.length).toBe(25); // filters silently ignored
  });
});
```

Add imports at the top of the file: `import { getFacetOptions } from "~/lib/facets.server";` and `beforeEach`-style cache clearing is NOT needed here (warm cache is fine). Run `pnpm test queries` — the new block FAILS (`filters` not accepted).

- [ ] **Step 5: Implement in `app/lib/queries.server.ts`**

Extend the `searchCompanies` options type with `filters?: CompanyFilters` (import the type from `~/lib/filters`), and replace the WHERE construction:

```ts
  const conds: string[] = [];
  const params: Record<string, unknown> = {};
  if (q) {
    conds.push(`${country.nameColumn} ILIKE {pattern:String}`);
    params.pattern = `%${q}%`;
  }
  for (const column of country.columns) {
    if (!column.filterable) continue;
    const values = opts.filters?.[column.key];
    if (!values || values.length === 0) continue;
    // Column expr from registry; values bound as an Array(String) param.
    conds.push(`${column.expr} IN {f_${column.key}:Array(String)}`);
    params[`f_${column.key}`] = values;
  }
  const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
```

Use `params` (pass `Object.keys(params).length ? params : undefined` or just `params` — chQuery accepts an empty record) for BOTH the count query and the rows query (replace the old `params`/`where` variables wholesale). Everything else (clamping, ORDER BY, industry merge) stays untouched. Column keys are `[a-z_]+` (registry-tested in Task 1), so `f_<key>` is always a valid ClickHouse param name.

- [ ] **Step 6: Run to verify green**

Run: `pnpm test queries` → PASS (all, including pre-existing). Then `pnpm typecheck && pnpm test` → green.

- [ ] **Step 7: Commit**

```bash
git add app/lib/filters.ts app/lib/filters.test.ts app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "feat(backoffice): url filter parsing and filtered company search"
```

---

### Task 4: Resource route + filter sidebar UI

**Files:**
- Create: `app/routes/country-facet-options.ts` (resource route)
- Create: `app/components/data-table/filter-sidebar.tsx`
- Modify: `app/routes.ts` (add the resource route)
- Modify: `app/components/data-table/url.ts` + `app/components/data-table/url.test.ts` (toggle/clear helpers)
- Modify: `app/routes/country-companies.tsx` (wire sidebar + active-filter badges + pass filters to searchCompanies)

**Interfaces:**
- Consumes: `searchFacetOptions` (Task 2), `parseFilters`/`filterableFacetKeys`/`FILTER_PREFIX`/`CompanyFilters` (Task 3), shadcn `sheet`/`command`/`popover` components.
- Produces:
  - `GET /:country/facet-options?column=<key>&q=<text>` → `{ options: FacetOption[] }`; 400 on unknown column, 404 on unknown country.
  - `toggleFilterValue(current: URLSearchParams, key: string, value: string): string` — adds/removes one `f_<key>` value, deletes `page`.
  - `clearAllFilters(current: URLSearchParams): string` — removes every `f_*` param, deletes `page`.
  - `<FilterSidebar country filters />` and active-filter badges on the companies page.

- [ ] **Step 1: Install shadcn components**

```bash
pnpm dlx shadcn@latest add sheet command popover
```

(No-ops where already present. `command` brings the `cmdk` dependency.)

- [ ] **Step 2: Failing tests for the URL helpers**

Append to `app/components/data-table/url.test.ts`:

```ts
import { clearAllFilters, toggleFilterValue } from "~/components/data-table/url";

describe("toggleFilterValue", () => {
  it("adds a value and resets page", () => {
    const current = new URLSearchParams("q=grupp&page=3");
    const s = toggleFilterValue(current, "status", "Registered");
    expect(s).toContain("f_status=Registered");
    expect(s).toContain("q=grupp");
    expect(s).not.toContain("page=");
  });

  it("removes an already-selected value, keeps siblings", () => {
    const current = new URLSearchParams("f_status=A&f_status=B");
    const s = toggleFilterValue(current, "status", "A");
    expect(s).toContain("f_status=B");
    expect(s).not.toContain("f_status=A");
  });
});

describe("clearAllFilters", () => {
  it("removes every f_* param and page, keeps the rest", () => {
    const current = new URLSearchParams(
      "q=x&sort=status&f_status=A&f_legal_form=B&page=2",
    );
    const s = clearAllFilters(current);
    expect(s).toBe("?q=x&sort=status");
  });
});
```

Run `pnpm test url` — FAIL (not exported).

- [ ] **Step 3: Implement the helpers**

Append to `app/components/data-table/url.ts`:

```ts
import { FILTER_PREFIX } from "~/lib/filters";

/** Adds or removes one facet value; any filter change resets pagination. */
export function toggleFilterValue(
  current: URLSearchParams,
  key: string,
  value: string,
): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  const param = `${FILTER_PREFIX}${key}`;
  const values = next.getAll(param);
  next.delete(param);
  const remaining = values.filter((v) => v !== value);
  if (remaining.length === values.length) remaining.push(value);
  for (const v of remaining) next.append(param, v);
  return `?${next.toString()}`;
}

export function clearAllFilters(current: URLSearchParams): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  for (const key of [...next.keys()]) {
    if (key.startsWith(FILTER_PREFIX)) next.delete(key);
  }
  return `?${next.toString()}`;
}
```

Run `pnpm test url` → PASS.

- [ ] **Step 4: Resource route**

`app/routes/country-facet-options.ts`:

```ts
import type { Route } from "./+types/country-facet-options";
import { getCountry } from "~/lib/countries";
import { searchFacetOptions } from "~/lib/facets.server";
import { filterableFacetKeys } from "~/lib/filters";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const column = url.searchParams.get("column") ?? "";
  const q = url.searchParams.get("q") ?? "";

  if (!filterableFacetKeys(country).includes(column)) {
    throw new Response(`Unknown facet column: ${column}`, { status: 400 });
  }

  return { options: await searchFacetOptions(country, column, q) };
}
```

Register it in `app/routes.ts` as a child of the country layout:

```ts
route(":country", "routes/country.tsx", [
  index("routes/country-overview.tsx"),
  route("companies", "routes/country-companies.tsx"),
  route("facet-options", "routes/country-facet-options.ts"),
]),
```

- [ ] **Step 5: Filter sidebar component**

`app/components/data-table/filter-sidebar.tsx`:

```tsx
import { useRef, useState } from "react";
import { useFetcher, useNavigate, useSearchParams } from "react-router";
import { Check, ListFilter } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";
import { filterableFacetKeys } from "~/lib/filters";
import type { FacetOption } from "~/lib/facets.server";
import { toggleFilterValue } from "~/components/data-table/url";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
} from "~/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "~/components/ui/popover";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";

const nf = new Intl.NumberFormat("en-US");

function facetLabel(country: CountryConfig, key: string): string {
  return country.columns.find((c) => c.key === key)?.label ?? key;
}

function FacetCombobox({
  country,
  facetKey,
  selected,
}: {
  country: CountryConfig;
  facetKey: string;
  selected: string[];
}) {
  const fetcher = useFetcher<{ options: FacetOption[] }>();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [open, setOpen] = useState(false);
  const debounce = useRef<ReturnType<typeof setTimeout>>(undefined);
  const base = `/${country.code}/facet-options?column=${facetKey}`;

  function onOpenChange(next: boolean) {
    setOpen(next);
    if (next) fetcher.load(base);
  }

  function onQueryChange(q: string) {
    clearTimeout(debounce.current);
    debounce.current = setTimeout(() => {
      fetcher.load(`${base}&q=${encodeURIComponent(q)}`);
    }, 200);
  }

  const options = fetcher.data?.options ?? [];

  return (
    <div className="space-y-1.5">
      <p className="text-sm font-medium">{facetLabel(country, facetKey)}</p>
      <Popover open={open} onOpenChange={onOpenChange}>
        <PopoverTrigger
          render={
            <Button variant="outline" size="sm" className="w-full justify-between font-normal" />
          }
        >
          {selected.length > 0 ? `${selected.length} selected` : "Any"}
          <ListFilter className="text-muted-foreground size-3.5" />
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start">
          <Command shouldFilter={false}>
            <CommandInput
              placeholder="Type to search…"
              onValueChange={onQueryChange}
            />
            <CommandList>
              <CommandEmpty>
                {fetcher.state === "idle" ? "No options." : "Loading…"}
              </CommandEmpty>
              {options.map((option) => {
                const isSelected = selected.includes(option.value);
                return (
                  <CommandItem
                    key={option.value}
                    value={option.value}
                    onSelect={() =>
                      navigate(
                        toggleFilterValue(searchParams, facetKey, option.value),
                        { preventScrollReset: true },
                      )
                    }
                  >
                    <Check
                      className={`size-4 ${isSelected ? "opacity-100" : "opacity-0"}`}
                    />
                    {option.label !== option.value ? (
                      <span className="text-muted-foreground font-mono text-xs">
                        {option.value}
                      </span>
                    ) : null}
                    <span className="flex-1 truncate" title={option.label}>
                      {option.label}
                    </span>
                    <span className="text-muted-foreground text-xs tabular-nums">
                      {nf.format(option.count)}
                    </span>
                  </CommandItem>
                );
              })}
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  );
}

export function FilterSidebar({
  country,
  filters,
}: {
  country: CountryConfig;
  filters: CompanyFilters;
}) {
  const activeCount = Object.values(filters).reduce((n, v) => n + v.length, 0);
  return (
    <Sheet>
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilter className="size-4" />
        Filters
        {activeCount > 0 ? <Badge variant="secondary">{activeCount}</Badge> : null}
      </SheetTrigger>
      <SheetContent side="right" className="w-96 overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter companies</SheetTitle>
        </SheetHeader>
        <div className="space-y-4 px-4 pb-6">
          {filterableFacetKeys(country).map((key) => (
            <FacetCombobox
              key={key}
              country={country}
              facetKey={key}
              selected={filters[key] ?? []}
            />
          ))}
        </div>
      </SheetContent>
    </Sheet>
  );
}
```

Base UI adaptation note (binding, same as prior tasks): the exact `render`-prop placement on `SheetTrigger`/`PopoverTrigger` and Command prop names may differ in the installed components — read `app/components/ui/sheet.tsx`, `popover.tsx`, `command.tsx` and adapt minimally (structure/behavior above is the spec: trigger button with count badge, sheet from the right, one combobox per facet with search input, check-marked multi-select, counts right-aligned).

- [ ] **Step 6: Wire the companies route**

In `app/routes/country-companies.tsx`:
- Loader: parse and apply filters, and return them:

```ts
import { parseFilters } from "~/lib/filters";
// inside loader, after `const url = ...`:
const filters = parseFilters(url.searchParams, country);
const result = await searchCompanies(country, {
  q: url.searchParams.get("q") ?? "",
  page: Number(url.searchParams.get("page") ?? "1") || 1,
  pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
  sort: url.searchParams.get("sort"),
  dir: url.searchParams.get("dir"),
  filters,
});
return { q: url.searchParams.get("q") ?? "", result, filters };
```

- Component: render `<FilterSidebar country={country} filters={filters} />` next to the search form, and an active-filter badge row between the header and the table:

```tsx
{Object.entries(filters).flatMap(([key, values]) =>
  values.map((value) => (
    <Badge key={`${key}:${value}`} variant="secondary" className="gap-1">
      <span className="text-muted-foreground">{facetLabel(country, key)}:</span>
      {value}
      <Link
        to={toggleFilterValue(searchParams, key, value)}
        preventScrollReset
        aria-label={`Remove ${value}`}
      >
        <X className="size-3" />
      </Link>
    </Badge>
  )),
)}
{Object.keys(filters).length > 0 ? (
  <Link to={clearAllFilters(searchParams)} preventScrollReset className="text-muted-foreground text-xs underline">
    Clear all
  </Link>
) : null}
```

(Wrap in a `flex flex-wrap items-center gap-2` div rendered only when filters exist; import `X` from lucide, `useSearchParams`, the url helpers, and export `facetLabel` from `filter-sidebar.tsx`.)

- [ ] **Step 7: Verify**

`pnpm typecheck && pnpm test`, then `pnpm dev` and:

```bash
# facet options endpoint (JSON):
curl -s 'http://localhost:5183/ee/facet-options?column=status' | head -c 300
curl -s 'http://localhost:5183/ee/facet-options?column=status&q=reg' | head -c 300
curl -s -o /dev/null -w '%{http_code}' 'http://localhost:5183/ee/facet-options?column=bogus'   # 400
# pick a real status value from the first curl, then:
curl -s 'http://localhost:5183/ee/companies?f_status=<VALUE>' | grep -c 'Clear all'   # >= 1
# filtered total < unfiltered total (compare the "companies" counts in the two HTML outputs)
```

Browser: open Filters sheet, type in a combobox (options narrow as you type, one debounced request per pause — check the network tab), select two statuses, see badges + filtered table; remove via badge X; Clear all. Kill the dev server.

- [ ] **Step 8: Commit**

```bash
git add app/routes/country-facet-options.ts app/components/data-table/filter-sidebar.tsx app/routes.ts app/components/data-table/url.ts app/components/data-table/url.test.ts app/routes/country-companies.tsx app/components/ui package.json pnpm-lock.yaml
git commit -m "feat(backoffice): filter sidebar with server-searched facet comboboxes"
```

---

### Task 5: Industry facet + filter with canonical NACE English labels

**Files:**
- Modify: `app/lib/countries.ts` (2 new `CountryConfig` fields; new SQL for 9 countries; UPDATE existing `industryQuery` labels)
- Modify: `app/lib/countries.test.ts`
- Modify: `app/lib/facets.server.ts` (industry facet support)
- Modify: `app/lib/filters.ts` (append `"industry"` to facet keys)
- Modify: `app/lib/queries.server.ts` (industry WHERE)
- Modify: `tests/queries.server.test.ts`, `tests/facets.server.test.ts`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `CountryConfig.industryFacetQuery?: string` — no params; returns `value, label, cnt` (label = canonical NACE English)
  - `CountryConfig.industryFilterExpr?: string` — SQL boolean expr containing exactly the param `{f_industry:Array(String)}`
  - `filterableFacetKeys` includes `"industry"` when `industryFilterExpr` is set
  - LV has NEITHER field (its NACE table is empty) — the sidebar shows no Industry facet for LV.

Label rule everywhere (facet AND the existing display `industryQuery`): `coalesce(nullIf(nace_categories.description_en, ''), source description_en, source description_original, code)`. Join key: direct `normalized_code = code` for ee/gb/fr/cz/sk/se; `normalized_code = substring(code, 1, 4)` for no/fi (5-digit national codes); br keeps its CNAE mapping fallback.

- [ ] **Step 1: Write the failing registry tests**

Append to `app/lib/countries.test.ts`:

```ts
describe("industry facet and filter", () => {
  it("every country except lv has industryFacetQuery and industryFilterExpr", () => {
    for (const c of COUNTRIES) {
      if (c.code === "lv") {
        expect(c.industryFacetQuery, c.code).toBeUndefined();
        expect(c.industryFilterExpr, c.code).toBeUndefined();
        continue;
      }
      expect(c.industryFacetQuery, c.code).toContain(" AS value");
      expect(c.industryFacetQuery, c.code).toContain(" AS label");
      expect(c.industryFacetQuery, c.code).toContain(" AS cnt");
      expect(c.industryFilterExpr, c.code).toContain("{f_industry:Array(String)}");
    }
  });

  it("display industryQuery prefers canonical nace english labels", () => {
    // Every non-lv, non-br industryQuery joins nace_categories for the label.
    for (const c of COUNTRIES) {
      if (c.code === "lv" || c.code === "br") continue;
      expect(c.industryQuery, c.code).toContain("nace_categories");
    }
  });
});
```

Run `pnpm test countries` — FAIL.

- [ ] **Step 2: Registry SQL**

In `app/lib/countries.ts` add the two fields to `CountryConfig`:

```ts
  /** Facet options SQL: value, label (canonical NACE English), cnt. No params. */
  industryFacetQuery?: string;
  /** Boolean WHERE expr filtering companies by industry; binds {f_industry:Array(String)}. */
  industryFilterExpr?: string;
```

Per-country values. Uniform direct-join countries — ee, gb, fr, cz, sk — use this pattern (Estonia shown; substitute table + key per country: gb→`gb_industries`/`company_number`, fr→`fr_industries`/`siren`, cz→`cz_industries`/`ico`, sk→`sk_industries`/`ico`):

```ts
industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM ee_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
industryFilterExpr: `reg_code IN (SELECT reg_code FROM ee_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
```

Norway (5-digit → substring join):

```ts
industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM no_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(i.nace_normalized_code, 1, 4) AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
industryFilterExpr: `org_number IN (SELECT org_number FROM no_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
```

Finland (`nace_normalized_code` is NULL in live data — key on `source_industry_code`, 5-digit TOL):

```ts
industryFacetQuery: `SELECT coalesce(i.source_industry_code, '') AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM fi_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.is_primary = 1 AND coalesce(i.source_industry_code, '') != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
industryFilterExpr: `business_id IN (SELECT business_id FROM fi_industries WHERE is_primary = 1 AND coalesce(source_industry_code, '') IN {f_industry:Array(String)})`,
```

Sweden:

```ts
industryFacetQuery: `SELECT i.nace_rev2_class_code AS value,
  coalesce(nullIf(any(n.description_en), ''), value) AS label,
  count() AS cnt
FROM se_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_rev2_class_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
industryFilterExpr: `company_id IN (SELECT company_id FROM se_industries WHERE is_primary = 1 AND nace_rev2_class_code IN {f_industry:Array(String)})`,
```

Brazil (CNAE codes; NACE mapping nearly empty — labels fall back to the code until the mapping fixture is expanded, then improve automatically):

```ts
industryFacetQuery: `SELECT e.primary_cnae_code AS value,
  coalesce(nullIf(any(m.nace_description_en), ''), value) AS label,
  count() AS cnt
FROM br_establishments AS e
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.is_headquarters = 1 AND e.primary_cnae_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
industryFilterExpr: `cnpj_basico IN (SELECT cnpj_basico FROM br_establishments WHERE is_headquarters = 1 AND primary_cnae_code IN {f_industry:Array(String)})`,
```

Latvia: add NEITHER field.

**Also update the DISPLAY `industryQuery` strings** to the same canonical-label rule (user requirement: NACE English by default). For ee/gb/fr/cz/sk (direct join; Estonia shown, substitute table+key):

```ts
industryQuery: `SELECT i.reg_code AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM ee_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.reg_code IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.reg_code`,
```

Norway: same but join on `substring(i.nace_normalized_code, 1, 4)`. Finland: key/code on `coalesce(i.source_industry_code, '')` and join on `substring(coalesce(i.source_industry_code, ''), 1, 4)`:

```ts
industryQuery: `SELECT i.business_id AS company_id,
  coalesce(i.source_industry_code, '') AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.source_industry_code, '') AS industry_label
FROM fi_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.business_id IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.business_id`,
```

Sweden and Brazil `industryQuery`: unchanged (SE already canonical; BR keeps CNAE fallback). Latvia `industryQuery`: unchanged.

Run `pnpm test countries` → PASS.

- [ ] **Step 3: Industry support in facets + filters + query layer**

`app/lib/filters.ts` — extend `filterableFacetKeys`:

```ts
export function filterableFacetKeys(country: CountryConfig): string[] {
  const keys = country.columns.filter((c) => c.filterable).map((c) => c.key);
  if (country.industryFilterExpr) keys.push("industry");
  return keys;
}
```

`app/lib/facets.server.ts` — in `getFacetOptions`, branch before the column lookup:

```ts
  let sql: string;
  if (facetKey === "industry") {
    if (!country.industryFacetQuery) throw new Error(`unknown facet: ${facetKey}`);
    sql = country.industryFacetQuery;
  } else {
    sql = facetSql(country, facetKey);
  }
  const rows = await chQuery<{ value: string; label: string; cnt: string }>(sql);
```

`app/lib/queries.server.ts` — in the conds loop section, after the column filters:

```ts
  const industryValues = opts.filters?.industry;
  if (industryValues?.length && country.industryFilterExpr) {
    conds.push(country.industryFilterExpr);
    params.f_industry = industryValues;
  }
```

(The sidebar from Task 4 needs no code change: it maps over `filterableFacetKeys`, which now yields `industry`, and `facetLabel` falls back to the key — improve it to `key === "industry" ? "Industry" : ...` in `filter-sidebar.tsx`.)

- [ ] **Step 4: Failing-then-green integration tests**

Append to `tests/facets.server.test.ts`:

```ts
describe("industry facet (Estonia)", () => {
  it("serves industry options with canonical english labels", async () => {
    const options = await getFacetOptions(ee, "industry");
    expect(options.length).toBeGreaterThan(0);
    const labeled = options.filter((o) => o.label !== o.value);
    // Most 4-digit EE codes resolve in nace_categories → english label
    expect(labeled.length).toBeGreaterThan(options.length / 2);
  });
});
```

Append to `tests/queries.server.test.ts`:

```ts
describe("industry filter (Estonia)", () => {
  it("filters companies by primary industry code", async () => {
    const options = await getFacetOptions(ee, "industry");
    const top = options[0];
    const filtered = await searchCompanies(ee, {
      filters: { industry: [top.value] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThan(400_000);
    for (const row of filtered.rows) {
      expect(row.industry_code).toBe(top.value);
    }
  });
});
```

Run RED first (before Step 3's code if you sequence strictly — otherwise verify they fail by commenting nothing and trusting the missing-field failures from Step 1's RED), then `pnpm test` → all green, `pnpm typecheck` clean.

- [ ] **Step 5: Verify labels in the running app**

`pnpm dev`, then `curl -s 'http://localhost:5183/ee/companies' | rg -o 'Installation of|Retail sale|Computer programming' | head -3` — the industry COLUMN now shows English text (previously Estonian). Spot-check `/no/companies` and `/fi/companies` render industry labels in English. Check `/ee/facet-options?column=industry&q=software` returns matches. Kill the server.

- [ ] **Step 6: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts app/lib/facets.server.ts app/lib/filters.ts app/lib/queries.server.ts tests/queries.server.test.ts tests/facets.server.test.ts app/components/data-table/filter-sidebar.tsx
git commit -m "feat(backoffice): industry facet filter with canonical nace english labels"
```

---

### Task 6: All-countries smoke, README, full gate

**Files:**
- Modify: `tests/queries.server.test.ts` (extend the all-countries sweep)
- Modify: `README.md`

- [ ] **Step 1: Extend the all-countries sweep**

In the existing `describe("searchCompanies across all countries")`, add a second `it.each` (60s timeout — BR's facet GROUP BY scans 68M rows once, then caches):

```ts
it.each(COUNTRIES.map((c) => [c.code, c] as const))(
  "%s: first facet loads and filters companies",
  async (_code, country) => {
    const keys = filterableFacetKeys(country).filter((k) => k !== "industry");
    const facetKey = keys[0];
    const options = await getFacetOptions(country, facetKey);
    expect(options.length).toBeGreaterThan(0);
    const filtered = await searchCompanies(country, {
      pageSize: 25,
      filters: { [facetKey]: [options[0].value] },
    });
    expect(filtered.total).toBeGreaterThan(0);
  },
  60_000,
);

it.each(
  COUNTRIES.filter((c) => c.industryFilterExpr).map((c) => [c.code, c] as const),
)(
  "%s: industry facet loads and filters companies",
  async (_code, country) => {
    const options = await getFacetOptions(country, "industry");
    expect(options.length).toBeGreaterThan(0);
    const filtered = await searchCompanies(country, {
      pageSize: 25,
      filters: { industry: [options[0].value] },
    });
    expect(filtered.total).toBeGreaterThan(0);
  },
  120_000,
);
```

Add the needed imports (`filterableFacetKeys` from `~/lib/filters`). Run `pnpm test queries` — report per-country timings; a ClickHouse error on any country is a real registry bug: report it verbatim, do not skip the country.

- [ ] **Step 2: README**

Add under the `## Companies table` section:

```markdown
### Filters

The Filters sheet offers one searchable multi-select per categorical column
(`filterable: true` in `countries.ts`) plus Industry (canonical NACE English
labels via `nace_categories`; `industryFacetQuery`/`industryFilterExpr` per
country — Latvia has none because `lv_companies_nace` is unpopulated).
Selected values live in the URL as repeated `f_<key>=` params and are applied
server-side by the loader. Option lists are cached in-process for 24h
(`facets.server.ts`) — typeahead searches the cache (diacritic-insensitive,
prefix-first), never ClickHouse per keystroke.
```

- [ ] **Step 3: Full gate**

`pnpm typecheck && pnpm test && pnpm build`, then `pnpm start` and:

```bash
curl -s 'http://localhost:3000/ee/facet-options?column=status' | head -c 200
curl -s 'http://localhost:3000/ee/companies?f_status=<top value from previous curl>' | grep -c 'Clear all'
```

Kill; port free.

- [ ] **Step 4: Commit**

```bash
git add tests/queries.server.test.ts README.md
git commit -m "test(backoffice): all-countries facet smoke and filter docs"
```

---

## Out of scope (logged)

- Contextual facet counts (counts recomputed under current q/other filters).
- Filtering by "empty" values (rows where the column is `''`/NULL can't be selected).
- Typo-tolerant fuzzy scoring (swap into `rankFacetOptions` if substring proves insufficient).
- Persistent/shared facet cache (SQLite/Redis) — only needed with multiple server processes.
- **LV NACE pipeline gap**: `lv_companies_nace` is fully unpopulated — needs investigation in the data pipeline, separate from this app.
- **BR CNAE→NACE mapping expansion** (dagster `brazil_comp_cnae` fixture) — industry labels for BR improve automatically once done.
