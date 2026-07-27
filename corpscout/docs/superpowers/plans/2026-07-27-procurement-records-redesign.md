# Procurement Register Records Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the records section of `/procurements/:source` as a shadcn data table with a filter sheet (country dropdown, buyer/winner search, enum and USD-value filters) and curated columns, with buyers/winners linked to company pages.

**Architecture:** The register pages keep server-side pagination in the loader (tables exceed 100k rows). A shared column-curation module hides load-plumbing and FX/estimate columns from the table only — the record detail page still shows everything. The table renders through the existing shared `DataTable` (TanStack + shadcn) with `ColumnDef[]` built at render time from the register's visible columns. A new Sheet-based `ProcurementFilterSheet` writes URL search params; the loader translates them into parameterized ClickHouse WHERE clauses (column names validated against the table's real columns, values always bound, never interpolated). The loader batch-resolves buyer/winner org ids against `companies_all` so cells can link to `/company/:country/:id`.

**Tech Stack:** React Router 8 (framework mode), TanStack Table via existing `DataTable`, shadcn/ui (`Sheet`, `Select`), ClickHouse via `chQuery`, vitest.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-27-procurement-records-redesign-design.md`

## Global Constraints

- Working dir for all commands: `corpscout/services/backoffice`. Tests: `npx vitest run <file>`. Typecheck: `npm run typecheck`.
- Never interpolate user input into SQL. Values go through ClickHouse bound params (`{name:String}`); column names must come from `columnsOf(table)` or hardcoded candidate lists.
- Route components must not import `~/lib/procurements.server` for anything used in the component body — server-only. Pure helpers go in client-safe `~/lib/*` modules (see `procurement-paths.ts` precedent).
- Commit after each task **by explicit path** (`git add <files>`), Conventional Commits, and end commit messages with: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- The record detail page (`app/routes/procurement-record.tsx`) is out of scope and must not change.
- Follow existing component idioms: `Button` with `render={<Link …/>}` + `nativeButton={false}`, `useEffectiveSearchParams()` + `tableSearch()` for pagination URLs.

---

### Task 1: Column curation module

**Files:**
- Create: `app/lib/procurement-columns.ts`
- Test: `app/lib/procurement-columns.test.ts`

**Interfaces:**
- Produces: `isHiddenTableColumn(name: string): boolean` and `visibleColumns(columns: string[]): string[]` — used by Task 6 (route) and nothing else.

- [ ] **Step 1: Write the failing test**

```ts
// app/lib/procurement-columns.test.ts
import { describe, expect, it } from "vitest";
import { isHiddenTableColumn, visibleColumns } from "./procurement-columns";

describe("isHiddenTableColumn", () => {
  it("hides load plumbing on every register", () => {
    for (const name of ["source_slug", "source_run_id", "partition_key", "resolved_at"]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("hides FX bookkeeping", () => {
    for (const name of ["fx_rate_to_usd", "fx_rate_date", "fx_source"]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("hides estimated and framework value groups (TED, Doffin, Hilma)", () => {
    for (const name of [
      "estimated_value_amount_original",
      "estimated_value_amount_usd",
      "estimated_value_currency",
      "notice_estimated_value_amount_usd",
      "lot_estimated_value_amount_original",
      "framework_maximum_amount_original",
      "framework_maximum_amount_usd",
      "framework_maximum_currency",
      "framework_total_maximum_amount_usd",
      "framework_total_approximate_amount_original",
      "framework_value_reestimated_amount_usd",
    ]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("keeps realized values, identity and buyer/winner columns", () => {
    for (const name of [
      "value_amount_original",
      "value_amount_usd",
      "value_currency",
      "awarded_amount_usd",
      "total_value_amount_usd",
      "procurement_value_amount_original",
      "valor_global",
      "valor_global_usd",
      "buyer_name",
      "buyer_national_id",
      "winner_name",
      "winner_org_number",
      "publication_date",
      "notice_type",
      "award_result",
      "source_record_id",
      "source_url",
      "company_id",
    ]) {
      expect(isHiddenTableColumn(name)).toBe(false);
    }
  });
});

describe("visibleColumns", () => {
  it("filters while preserving order", () => {
    expect(
      visibleColumns(["doffin_id", "source_slug", "buyer_name", "fx_source", "value_amount_usd"]),
    ).toEqual(["doffin_id", "buyer_name", "value_amount_usd"]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run app/lib/procurement-columns.test.ts`
Expected: FAIL — cannot resolve `./procurement-columns`.

- [ ] **Step 3: Write the implementation**

```ts
// app/lib/procurement-columns.ts
/** Which register columns the records TABLE hides. The record detail page
 * deliberately shows everything a register publishes; the table is for
 * scanning, so load plumbing, FX bookkeeping, and the estimated/framework
 * value variants move to the detail page only. One shared rule set — every
 * register (TED, Doffin, Hilma, PNCP, UHM) inherits it. */

const HIDDEN_EXACT = new Set([
  "source_slug",
  "source_run_id",
  "partition_key",
  "resolved_at",
]);

const HIDDEN_PATTERN = /^fx_|^framework_|estimated_value/;

export function isHiddenTableColumn(name: string): boolean {
  return HIDDEN_EXACT.has(name) || HIDDEN_PATTERN.test(name);
}

export function visibleColumns(columns: string[]): string[] {
  return columns.filter((name) => !isHiddenTableColumn(name));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run app/lib/procurement-columns.test.ts`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add app/lib/procurement-columns.ts app/lib/procurement-columns.test.ts
git commit -m "feat(backoffice): shared column curation for procurement record tables"
```

---

### Task 2: EU/EEA country list

**Files:**
- Create: `app/lib/eu-countries.ts`
- Test: `app/lib/eu-countries.test.ts`

**Interfaces:**
- Produces: `EU_EEA_COUNTRIES: { iso2: string; name: string }[]` (alphabetical by name) — used by the filter sheet (Task 5). EEA members are included because TED carries Norway, which is not EU; a pure EU-27 list would grey out a country that has data.

- [ ] **Step 1: Write the failing test**

```ts
// app/lib/eu-countries.test.ts
import { describe, expect, it } from "vitest";
import { EU_EEA_COUNTRIES } from "./eu-countries";

describe("EU_EEA_COUNTRIES", () => {
  it("contains the EU-27 plus EEA (30 entries) with unique iso2 codes", () => {
    expect(EU_EEA_COUNTRIES).toHaveLength(30);
    expect(new Set(EU_EEA_COUNTRIES.map((c) => c.iso2)).size).toBe(30);
  });

  it("includes the loaded TED countries and sorts by name", () => {
    const codes = EU_EEA_COUNTRIES.map((c) => c.iso2);
    for (const code of ["SE", "FI", "NO"]) expect(codes).toContain(code);
    const names = EU_EEA_COUNTRIES.map((c) => c.name);
    expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b)));
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run app/lib/eu-countries.test.ts`
Expected: FAIL — cannot resolve `./eu-countries`.

- [ ] **Step 3: Write the implementation**

```ts
// app/lib/eu-countries.ts
/** EU-27 plus the three EEA EFTA states, for the procurement country filter.
 * EEA is included because TED carries Norwegian notices; a strict EU list
 * would grey out a country that has data. Sorted by English name. */
export const EU_EEA_COUNTRIES: { iso2: string; name: string }[] = [
  { iso2: "AT", name: "Austria" },
  { iso2: "BE", name: "Belgium" },
  { iso2: "BG", name: "Bulgaria" },
  { iso2: "HR", name: "Croatia" },
  { iso2: "CY", name: "Cyprus" },
  { iso2: "CZ", name: "Czechia" },
  { iso2: "DK", name: "Denmark" },
  { iso2: "EE", name: "Estonia" },
  { iso2: "FI", name: "Finland" },
  { iso2: "FR", name: "France" },
  { iso2: "DE", name: "Germany" },
  { iso2: "GR", name: "Greece" },
  { iso2: "HU", name: "Hungary" },
  { iso2: "IS", name: "Iceland" },
  { iso2: "IE", name: "Ireland" },
  { iso2: "IT", name: "Italy" },
  { iso2: "LV", name: "Latvia" },
  { iso2: "LI", name: "Liechtenstein" },
  { iso2: "LT", name: "Lithuania" },
  { iso2: "LU", name: "Luxembourg" },
  { iso2: "MT", name: "Malta" },
  { iso2: "NL", name: "Netherlands" },
  { iso2: "NO", name: "Norway" },
  { iso2: "PL", name: "Poland" },
  { iso2: "PT", name: "Portugal" },
  { iso2: "RO", name: "Romania" },
  { iso2: "SK", name: "Slovakia" },
  { iso2: "SI", name: "Slovenia" },
  { iso2: "ES", name: "Spain" },
  { iso2: "SE", name: "Sweden" },
];
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run app/lib/eu-countries.test.ts`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add app/lib/eu-countries.ts app/lib/eu-countries.test.ts
git commit -m "feat(backoffice): EU/EEA country list for procurement filters"
```

---

### Task 3: Filter model and WHERE builder in procurements.server.ts

**Files:**
- Modify: `app/lib/procurements.server.ts` (SourceQuery at ~line 147, listSourceRecords at ~line 156)
- Test: `app/lib/procurement-filter.test.ts`

**Interfaces:**
- Consumes: existing `dateColumn(columns)` / `countryColumn(columns)` helpers in the same file.
- Produces (exported from `procurements.server.ts`):
  - `interface SourceQuery { table?, country?, from?, to?, buyer?, winner?, noticeType?, awardResult?, valueMin?, valueMax?, limit?, offset? }` (all optional; valueMin/valueMax are `number`)
  - `interface FilterColumns { date: string | null; country: string | null; buyerName: string | null; winnerName: string | null; winnerId: string | null; noticeType: string | null; awardResult: string | null; usdValue: string | null }`
  - `filterColumns(columns: string[]): FilterColumns`
  - `buildSourceFilter(columns: string[], query: SourceQuery): { where: string[]; params: Record<string, unknown> }` — pure, unit-testable; `listSourceRecords` uses it.

The pure helpers live in `procurements.server.ts` (they never reach the client — only the loader uses them), but the unit test imports them directly, which vitest allows because the test runs in Node.

- [ ] **Step 1: Write the failing test**

```ts
// app/lib/procurement-filter.test.ts
import { describe, expect, it } from "vitest";
import { buildSourceFilter, filterColumns } from "./procurements.server";

const TED_NOTICES = [
  "publication_number", "country_iso2", "publication_date", "notice_type",
  "buyer_name", "buyer_national_id", "total_value_amount_usd", "fx_source",
];
const DOFFIN = [
  "doffin_id", "country_code", "publication_date", "notice_type", "award_result",
  "buyer_name", "buyer_org_number", "winner_name", "winner_org_number",
  "value_amount_usd",
];
const TED_WINNERS = [
  "publication_number", "winner_name", "winner_national_id", "awarded_amount_usd",
];

describe("filterColumns", () => {
  it("discovers per-table filter columns", () => {
    const ted = filterColumns(TED_NOTICES);
    expect(ted).toEqual({
      date: "publication_date",
      country: "country_iso2",
      buyerName: "buyer_name",
      winnerName: null,
      winnerId: null,
      noticeType: "notice_type",
      awardResult: null,
      usdValue: "total_value_amount_usd",
    });
    const winners = filterColumns(TED_WINNERS);
    expect(winners.winnerName).toBe("winner_name");
    expect(winners.winnerId).toBe("winner_national_id");
    expect(winners.usdValue).toBe("awarded_amount_usd");
    expect(winners.buyerName).toBeNull();
  });
});

describe("buildSourceFilter", () => {
  it("builds clauses only for columns the table has, with bound params", () => {
    const { where, params } = buildSourceFilter(DOFFIN, {
      country: "NO",
      from: "2026-01-01",
      buyer: "kommune",
      winner: "consult",
      noticeType: "award",
      awardResult: "won",
      valueMin: 1000,
      valueMax: 500000,
    });
    expect(where).toEqual([
      "upper(country_code) = upper({country:String})",
      "publication_date >= toDate({from:String})",
      "positionCaseInsensitiveUTF8(buyer_name, {buyer:String}) > 0",
      "(positionCaseInsensitiveUTF8(winner_name, {winner:String}) > 0 OR winner_org_number = {winner:String})",
      "notice_type = {noticeType:String}",
      "award_result = {awardResult:String}",
      "value_amount_usd >= {valueMin:Float64}",
      "value_amount_usd <= {valueMax:Float64}",
    ]);
    expect(params).toMatchObject({
      country: "NO",
      from: "2026-01-01",
      buyer: "kommune",
      winner: "consult",
      noticeType: "award",
      awardResult: "won",
      valueMin: 1000,
      valueMax: 500000,
    });
  });

  it("ignores filters whose backing column is absent", () => {
    const { where } = buildSourceFilter(TED_WINNERS, {
      country: "SE",
      buyer: "city",
      noticeType: "award",
    });
    expect(where).toEqual([]);
  });

  it("ignores empty and non-finite values", () => {
    const { where } = buildSourceFilter(DOFFIN, {
      buyer: "",
      winner: "  ",
      valueMin: Number.NaN,
    });
    expect(where).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run app/lib/procurement-filter.test.ts`
Expected: FAIL — `buildSourceFilter`/`filterColumns` not exported.

- [ ] **Step 3: Implement in procurements.server.ts**

Replace the existing `SourceQuery` interface and the WHERE-building section of `listSourceRecords` with:

```ts
export interface SourceQuery {
  table?: string;
  country?: string;
  from?: string;
  to?: string;
  buyer?: string;
  winner?: string;
  noticeType?: string;
  awardResult?: string;
  valueMin?: number;
  valueMax?: number;
  limit?: number;
  offset?: number;
}

export interface FilterColumns {
  date: string | null;
  country: string | null;
  buyerName: string | null;
  winnerName: string | null;
  winnerId: string | null;
  noticeType: string | null;
  awardResult: string | null;
  usdValue: string | null;
}

function firstPresent(columns: string[], candidates: string[]): string | null {
  for (const candidate of candidates) {
    if (columns.includes(candidate)) return candidate;
  }
  return null;
}

/** Column discovery per register table. Candidate lists, like dateColumn's,
 * because the registers publish the same concepts under different names. */
export function filterColumns(columns: string[]): FilterColumns {
  return {
    date: dateColumn(columns),
    country: countryColumn(columns),
    buyerName: firstPresent(columns, ["buyer_name", "buyer_name_fi", "buyer_unit_name"]),
    winnerName: firstPresent(columns, ["winner_name"]),
    winnerId: firstPresent(columns, [
      "winner_org_number",
      "winner_national_id",
      "winner_business_id",
    ]),
    noticeType: firstPresent(columns, ["notice_type", "procedure_type"]),
    awardResult: firstPresent(columns, ["award_result"]),
    usdValue: firstPresent(columns, [
      "value_amount_usd",
      "awarded_amount_usd",
      "total_value_amount_usd",
      "procurement_value_amount_usd",
      "valor_global_usd",
    ]),
  };
}

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

export function buildSourceFilter(
  columns: string[],
  query: SourceQuery,
): { where: string[]; params: Record<string, unknown> } {
  const cols = filterColumns(columns);
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  const country = nonEmpty(query.country);
  if (cols.country && country) {
    where.push(`upper(${cols.country}) = upper({country:String})`);
    params.country = country;
  }
  const from = nonEmpty(query.from);
  if (cols.date && from) {
    where.push(`${cols.date} >= toDate({from:String})`);
    params.from = from;
  }
  const to = nonEmpty(query.to);
  if (cols.date && to) {
    where.push(`${cols.date} <= toDate({to:String})`);
    params.to = to;
  }
  const buyer = nonEmpty(query.buyer);
  if (cols.buyerName && buyer) {
    where.push(`positionCaseInsensitiveUTF8(${cols.buyerName}, {buyer:String}) > 0`);
    params.buyer = buyer;
  }
  const winner = nonEmpty(query.winner);
  if ((cols.winnerName || cols.winnerId) && winner) {
    const parts: string[] = [];
    if (cols.winnerName) {
      parts.push(`positionCaseInsensitiveUTF8(${cols.winnerName}, {winner:String}) > 0`);
    }
    if (cols.winnerId) parts.push(`${cols.winnerId} = {winner:String}`);
    where.push(parts.length > 1 ? `(${parts.join(" OR ")})` : parts[0]);
    params.winner = winner;
  }
  const noticeType = nonEmpty(query.noticeType);
  if (cols.noticeType && noticeType) {
    where.push(`${cols.noticeType} = {noticeType:String}`);
    params.noticeType = noticeType;
  }
  const awardResult = nonEmpty(query.awardResult);
  if (cols.awardResult && awardResult) {
    where.push(`${cols.awardResult} = {awardResult:String}`);
    params.awardResult = awardResult;
  }
  if (cols.usdValue && query.valueMin != null && Number.isFinite(query.valueMin)) {
    where.push(`${cols.usdValue} >= {valueMin:Float64}`);
    params.valueMin = query.valueMin;
  }
  if (cols.usdValue && query.valueMax != null && Number.isFinite(query.valueMax)) {
    where.push(`${cols.usdValue} <= {valueMax:Float64}`);
    params.valueMax = query.valueMax;
  }
  return { where, params };
}
```

Then rewrite `listSourceRecords` to use it (keeping its signature, adding `filters: FilterColumns` to the return):

```ts
export async function listSourceRecords(
  register: ProcurementRegister,
  query: SourceQuery = {},
): Promise<SourceRecords & { filters: FilterColumns }> {
  const table = assertKnownTable(register, query.table ?? register.notice_table);
  const columns = await columnsOf(table);
  const cols = filterColumns(columns);

  const { where, params } = buildSourceFilter(columns, query);
  params.limit = Math.min(query.limit ?? 50, 200);
  params.offset = Math.max(query.offset ?? 0, 0);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const order = cols.date ? `ORDER BY ${cols.date} DESC` : "";

  const [rows, counted] = await Promise.all([
    chQuery<SourceRow>(
      `SELECT * FROM ${table} ${filter} ${order}
       LIMIT {limit:UInt32} OFFSET {offset:UInt32}`,
      params,
    ),
    chQuery<{ total: string }>(
      `SELECT toString(count()) AS total FROM ${table} ${filter}`,
      params,
    ),
  ]);

  return { columns, rows, total: Number(counted[0]?.total ?? 0), filters: cols };
}
```

Delete the old `dateColumn`/`countryColumn` return fields (`dateColumn: dateCol, countryColumn: countryCol`) — Task 6 updates the route, which is the only caller (verify with `rg -n "listSourceRecords" app/`).

- [ ] **Step 4: Run tests and typecheck**

Run: `npx vitest run app/lib/procurement-filter.test.ts` — Expected: PASS (4 tests).
Run: `npm run typecheck` — Expected: FAIL in `app/routes/procurement-source.tsx` only (uses removed `dateColumn`/`countryColumn` fields). That is Task 6's file; if anything ELSE fails, fix it here. To keep the tree green for review, apply the minimal route patch now: in `procurement-source.tsx`, replace uses of `records.dateColumn`/`records.countryColumn` with `records.filters.date`/`records.filters.country`. Re-run typecheck — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/procurements.server.ts app/lib/procurement-filter.test.ts app/routes/procurement-source.tsx
git commit -m "feat(backoffice): parameterized multi-filter WHERE builder for register tables"
```

---

### Task 4: Enum options, active countries, and company matching (server)

**Files:**
- Modify: `app/lib/procurements.server.ts` (append at end)

**Interfaces:**
- Consumes: `chQuery`, `assertKnownTable`, `columnsOf`, `filterColumns` (Task 3).
- Produces:
  - `getFilterOptions(register, table): Promise<{ noticeTypes: string[]; awardResults: string[]; activeCountries: string[] }>` — activeCountries are UPPERCASE ISO2 codes present in the table (`country_iso2` is already ISO2; Doffin's `country_code` likewise; tables without a country column return `[]`).
  - `matchCompanies(ids: string[]): Promise<Record<string, { country_code: string; company_id: string }>>` — keys are the input ids that matched `companies_all`.

No pure logic here, so no unit test — verified in the browser in Task 7. (The ClickHouse client is not available in vitest.)

- [ ] **Step 1: Implement**

Append to `app/lib/procurements.server.ts`:

```ts
/** Distinct values for the sheet's enum dropdowns plus which countries have
 * rows. One grouped query per column, bounded: these are LowCardinality
 * columns with a handful of values. */
export async function getFilterOptions(
  register: ProcurementRegister,
  table?: string,
): Promise<{ noticeTypes: string[]; awardResults: string[]; activeCountries: string[] }> {
  const safeTable = assertKnownTable(register, table ?? register.notice_table);
  const cols = filterColumns(await columnsOf(safeTable));

  async function distinct(column: string | null): Promise<string[]> {
    if (!column) return [];
    const rows = await chQuery<{ v: string }>(
      `SELECT DISTINCT ${column} AS v FROM ${safeTable}
       WHERE ${column} != '' ORDER BY v LIMIT 100`,
    );
    return rows.map((r) => r.v);
  }

  const [noticeTypes, awardResults, activeCountries] = await Promise.all([
    distinct(cols.noticeType),
    distinct(cols.awardResult),
    distinct(cols.country).then((codes) => codes.map((c) => c.toUpperCase())),
  ]);
  return { noticeTypes, awardResults, activeCountries };
}

/** Which of these org ids exist in the company register, and where their
 * company pages live. Buyers are mostly public institutions, but their org
 * numbers are in the national registers (SE ~98%, NO ~95% measured), so the
 * company page doubles as the buyer page. */
export async function matchCompanies(
  ids: string[],
): Promise<Record<string, { country_code: string; company_id: string }>> {
  const unique = [...new Set(ids.filter((id) => id !== ""))];
  if (unique.length === 0) return {};
  const rows = await chQuery<{ company_id: string; country_code: string }>(
    `SELECT company_id, any(country_code) AS country_code
     FROM companies_all
     WHERE company_id IN {ids:Array(String)}
     GROUP BY company_id`,
    { ids: unique },
  );
  return Object.fromEntries(
    rows.map((r) => [r.company_id, { country_code: r.country_code, company_id: r.company_id }]),
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `npm run typecheck` — Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add app/lib/procurements.server.ts
git commit -m "feat(backoffice): filter options and company matching for register pages"
```

---

### Task 5: Shared component tweaks + ProcurementFilterSheet

**Files:**
- Modify: `app/components/data-table/data-table.tsx` (empty-state text, line ~55)
- Modify: `app/components/data-table/pagination.tsx` (label noun, line ~59)
- Create: `app/components/procurements/filter-sheet.tsx`

**Interfaces:**
- Consumes: `EU_EEA_COUNTRIES` (Task 2), `FilterColumns` shape (Task 3) passed as plain props (NOT imported from the server module — pass `filters`/options data from the loader).
- Produces:
  - `DataTable` gains optional `emptyText?: string` (default `"No results."`); companies callers pass `"No companies found."` explicitly.
  - `DataTablePagination` gains optional `itemsLabel?: string` (default `"companies"` to leave companies pages untouched).
  - `ProcurementFilterSheet` component with props:

```ts
export interface ProcurementFilterValues {
  country: string;      // ISO2 or ""
  from: string;
  to: string;
  buyer: string;
  winner: string;
  noticeType: string;
  awardResult: string;
  valueMin: string;     // raw input strings; loader parses
  valueMax: string;
}

export function ProcurementFilterSheet(props: {
  values: ProcurementFilterValues;
  available: {          // which sections to render (from loader's filters)
    country: boolean;
    date: boolean;
    buyer: boolean;
    winner: boolean;
    noticeType: boolean;
    awardResult: boolean;
    usdValue: boolean;
  };
  options: { noticeTypes: string[]; awardResults: string[]; activeCountries: string[] };
  table: string;        // preserved as hidden input so filters stay on the selected table
}): JSX.Element;
```

- [ ] **Step 1: Parameterize the shared components**

In `data-table.tsx`, change the signature and empty cell:

```ts
export function DataTable<TData>({
  columns,
  data,
  emptyText = "No results.",
}: {
  columns: ColumnDef<TData, unknown>[];
  data: TData[];
  emptyText?: string;
}) {
```

and replace `No companies found.` with `{emptyText}`. Then find companies callers (`rg -n "<DataTable" app/routes/`) and add `emptyText="No companies found."` to each so their copy is unchanged.

In `pagination.tsx`, add `itemsLabel = "companies"` to the props and replace the literal in `{nf.format(total)} companies` with `{nf.format(total)} {itemsLabel}`.

- [ ] **Step 2: Typecheck**

Run: `npm run typecheck` — Expected: PASS.

- [ ] **Step 3: Build the filter sheet**

```tsx
// app/components/procurements/filter-sheet.tsx
import { Form } from "react-router";
import { ListFilter } from "lucide-react";
import { EU_EEA_COUNTRIES } from "~/lib/eu-countries";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";

export interface ProcurementFilterValues {
  country: string;
  from: string;
  to: string;
  buyer: string;
  winner: string;
  noticeType: string;
  awardResult: string;
  valueMin: string;
  valueMax: string;
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label className="text-sm font-medium">{label}</Label>
      {children}
    </div>
  );
}

/** Sheet-based filters for one register table, mirroring the companies
 * FilterSidebar interaction. Sections render only when the selected table
 * has the backing column. Submitting navigates via GET so every filter
 * lives in the URL; page resets implicitly because no `page` input is kept. */
export function ProcurementFilterSheet({
  values,
  available,
  options,
  table,
}: {
  values: ProcurementFilterValues;
  available: {
    country: boolean;
    date: boolean;
    buyer: boolean;
    winner: boolean;
    noticeType: boolean;
    awardResult: boolean;
    usdValue: boolean;
  };
  options: { noticeTypes: string[]; awardResults: string[]; activeCountries: string[] };
  table: string;
}) {
  const activeCount = Object.values(values).filter((v) => v !== "").length;
  const active = new Set(options.activeCountries);

  function enumSelect(name: string, value: string, choices: string[]) {
    return (
      <Select name={name} defaultValue={value === "" ? undefined : value}>
        <SelectTrigger className="w-full" size="sm">
          <SelectValue placeholder="Any" />
        </SelectTrigger>
        <SelectContent>
          {choices.map((choice) => (
            <SelectItem key={choice} value={choice}>
              {choice}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }

  return (
    <Sheet>
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilter className="size-4" />
        Filters
        {activeCount > 0 ? <Badge variant="secondary">{activeCount}</Badge> : null}
      </SheetTrigger>
      <SheetContent side="right" className="w-96 overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter records</SheetTitle>
        </SheetHeader>
        <Form method="get" className="space-y-4 px-4 pb-6">
          <input type="hidden" name="table" value={table} />
          {available.country ? (
            <Field label="Country">
              <Select name="country" defaultValue={values.country === "" ? undefined : values.country}>
                <SelectTrigger className="w-full" size="sm">
                  <SelectValue placeholder="Any" />
                </SelectTrigger>
                <SelectContent>
                  {EU_EEA_COUNTRIES.map((c) => (
                    <SelectItem key={c.iso2} value={c.iso2} disabled={!active.has(c.iso2)}>
                      {c.name}
                      {active.has(c.iso2) ? "" : " (not loaded)"}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
          ) : null}
          {available.date ? (
            <>
              <Field label="Published from">
                <Input type="date" name="from" defaultValue={values.from} />
              </Field>
              <Field label="Published to">
                <Input type="date" name="to" defaultValue={values.to} />
              </Field>
            </>
          ) : null}
          {available.buyer ? (
            <Field label="Buyer name contains">
              <Input name="buyer" defaultValue={values.buyer} placeholder="e.g. kommun" />
            </Field>
          ) : null}
          {available.winner ? (
            <Field label="Winner name or org number">
              <Input name="winner" defaultValue={values.winner} placeholder="name or org number" />
            </Field>
          ) : null}
          {available.noticeType ? (
            <Field label="Notice type">{enumSelect("noticeType", values.noticeType, options.noticeTypes)}</Field>
          ) : null}
          {available.awardResult ? (
            <Field label="Award result">{enumSelect("awardResult", values.awardResult, options.awardResults)}</Field>
          ) : null}
          {available.usdValue ? (
            <div className="grid grid-cols-2 gap-2">
              <Field label="Min value (USD)">
                <Input type="number" name="valueMin" defaultValue={values.valueMin} min="0" />
              </Field>
              <Field label="Max value (USD)">
                <Input type="number" name="valueMax" defaultValue={values.valueMax} min="0" />
              </Field>
            </div>
          ) : null}
          <SheetFooter className="px-0">
            <Button type="submit">Apply filters</Button>
            <Button
              type="submit"
              variant="ghost"
              name="clear"
              value="1"
              formAction="?"
              formMethod="get"
            >
              Clear all
            </Button>
          </SheetFooter>
        </Form>
      </SheetContent>
    </Sheet>
  );
}
```

Note on "Clear all": a GET form to `?` with only the buttons' own values submits every named field too; the loader treats `clear=1` as "ignore all other filter params" — Task 6 implements that. Empty `Select` values simply don't submit (no `name` submitted when nothing chosen and `defaultValue` unset), and empty text inputs submit `""` which the loader's `nonEmpty` drops.

Check `~/components/ui/input.tsx` and `label.tsx` exist (`ls app/components/ui/ | rg "input|label"`); if `label.tsx` is missing, use a plain `<p className="text-sm font-medium">` instead of `Label` (match FacetCombobox's pattern).

- [ ] **Step 4: Typecheck**

Run: `npm run typecheck` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/components/data-table/data-table.tsx app/components/data-table/pagination.tsx app/components/procurements/filter-sheet.tsx
git commit -m "feat(backoffice): procurement filter sheet and shared table parameterization"
```

---

### Task 6: Rewrite the records section of procurement-source.tsx

**Files:**
- Modify: `app/routes/procurement-source.tsx`

**Interfaces:**
- Consumes: `visibleColumns` (Task 1), `listSourceRecords`+`SourceQuery`+`filters` (Task 3), `getFilterOptions`+`matchCompanies` (Task 4), `ProcurementFilterSheet` (Task 5), `DataTable`/`DataTablePagination`, `formatMoneyField` (`~/lib/money`), `sourceSlugToPath` (`~/lib/procurement-paths`).
- Produces: the finished page. URL params: `table`, `page`, `pageSize`, `country`, `from`, `to`, `buyer`, `winner`, `noticeType`, `awardResult`, `valueMin`, `valueMax`, `clear`.

- [ ] **Step 1: Update the loader**

Replace the current loader with:

```ts
export async function loader({ params, request }: Route.LoaderArgs) {
  const register = await getRegisterByPath(params.source);
  if (!register) throw new Response("Source not found", { status: 404 });

  const url = new URL(request.url);
  const q = (name: string) =>
    url.searchParams.get("clear") === "1" ? "" : (url.searchParams.get(name) ?? "");
  const num = (name: string) => {
    const parsed = Number.parseFloat(q(name));
    return Number.isFinite(parsed) ? parsed : undefined;
  };
  const table = url.searchParams.get("table") ?? undefined;
  const page = Math.max(1, Number.parseInt(q("page") || "1", 10) || 1);
  const pageSize = Math.min(200, Math.max(10, Number.parseInt(q("pageSize") || "50", 10) || 50));

  const query: SourceQuery = {
    table,
    country: q("country"),
    from: q("from"),
    to: q("to"),
    buyer: q("buyer"),
    winner: q("winner"),
    noticeType: q("noticeType"),
    awardResult: q("awardResult"),
    valueMin: num("valueMin"),
    valueMax: num("valueMax"),
    limit: pageSize,
    offset: (page - 1) * pageSize,
  };

  const [records, filterOptions, coverage, counts] = await Promise.all([
    listSourceRecords(register, query),
    getFilterOptions(register, table),
    getCoverage(register),
    countRowsByTable(register),
  ]);

  // Batch-resolve buyer/winner ids on this page to company pages.
  const idColumns = ["buyer_national_id", "buyer_org_number", "buyer_business_id",
    "winner_national_id", "winner_org_number", "winner_business_id", "buyer_cnpj",
  ].filter((c) => records.columns.includes(c));
  const ids = records.rows.flatMap((row) =>
    idColumns.map((c) => String(row[c] ?? "")).filter((v) => v !== ""),
  );
  const companyLinks = await matchCompanies(ids);

  return {
    register,
    records,
    filterOptions,
    coverage,
    counts,
    companyLinks,
    idColumns,
    page,
    pageSize,
    table: records.columns.length > 0 ? (table ?? register.notice_table) : register.notice_table,
    query,
  };
}
```

Keep the existing `getCoverage`/`counts` calls as they are in the current file (adapt names to what the file actually uses — check the current loader; `countRows` may be the real name). Import `getFilterOptions`, `matchCompanies`, `type SourceQuery` from `~/lib/procurements.server`.

- [ ] **Step 2: Build the ColumnDef factory and swap the table**

In the component file (NOT inside the component render), add:

```tsx
import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { ProcurementFilterSheet } from "~/components/procurements/filter-sheet";
import { visibleColumns } from "~/lib/procurement-columns";
import { formatMoneyField } from "~/lib/money";

const ID_TO_NAME_COLUMN: Record<string, string> = {
  buyer_national_id: "buyer_name",
  buyer_org_number: "buyer_name",
  buyer_business_id: "buyer_name",
  buyer_cnpj: "buyer_name",
  winner_national_id: "winner_name",
  winner_org_number: "winner_name",
  winner_business_id: "winner_name",
};

function cellText(column: string, value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.length > 0 ? value.join(", ") : "—";
  if (typeof value === "object") return JSON.stringify(value);
  const money = formatMoneyField(column, value);
  if (money !== null) return money;
  const text = String(value);
  return text === "" ? "—" : text;
}

function buildColumns(args: {
  columns: string[];
  keyColumn: string;
  path: string;
  companyLinks: Record<string, { country_code: string; company_id: string }>;
}): ColumnDef<SourceRow, unknown>[] {
  const { columns, keyColumn, path, companyLinks } = args;
  return visibleColumns(columns).map((column) => ({
    id: column,
    accessorFn: (row: SourceRow) => row[column],
    header: column,
    cell: ({ row }) => {
      const value = row.original[column];
      const text = cellText(column, value);
      if (column === keyColumn && text !== "—") {
        return (
          <Link
            to={`/procurements/${path}/${encodeURIComponent(text)}`}
            className="underline underline-offset-2"
          >
            {text}
          </Link>
        );
      }
      // Buyer/winner names link to the matched company page.
      const idColumn = Object.entries(ID_TO_NAME_COLUMN).find(
        ([, nameCol]) => nameCol === column,
      );
      if (idColumn) {
        for (const [idCol, nameCol] of Object.entries(ID_TO_NAME_COLUMN)) {
          if (nameCol !== column) continue;
          const match = companyLinks[String(row.original[idCol] ?? "")];
          if (match) {
            return (
              <Link
                to={`/company/${match.country_code.toLowerCase()}/${encodeURIComponent(match.company_id)}`}
                className="underline underline-offset-2"
              >
                {text}
              </Link>
            );
          }
        }
      }
      const isMoney = formatMoneyField(column, value) !== null;
      return (
        <span
          className={`block max-w-[22rem] truncate ${isMoney ? "text-right tabular-nums" : ""}`}
          title={text}
        >
          {text}
        </span>
      );
    },
  }));
}
```

Then in the component, replace the entire hand-rolled `<Table>…</Table>` block, the old country/date `<Form>`, and the old pagination footer with:

```tsx
<div className="flex items-center justify-between gap-2">
  <ProcurementFilterSheet
    values={{
      country: query.country ?? "",
      from: query.from ?? "",
      to: query.to ?? "",
      buyer: query.buyer ?? "",
      winner: query.winner ?? "",
      noticeType: query.noticeType ?? "",
      awardResult: query.awardResult ?? "",
      valueMin: query.valueMin != null ? String(query.valueMin) : "",
      valueMax: query.valueMax != null ? String(query.valueMax) : "",
    }}
    available={{
      country: records.filters.country !== null,
      date: records.filters.date !== null,
      buyer: records.filters.buyerName !== null,
      winner: records.filters.winnerName !== null || records.filters.winnerId !== null,
      noticeType: records.filters.noticeType !== null,
      awardResult: records.filters.awardResult !== null,
      usdValue: records.filters.usdValue !== null,
    }}
    options={filterOptions}
    table={table}
  />
</div>
<DataTable
  columns={buildColumns({ columns: records.columns, keyColumn, path, companyLinks })}
  data={records.rows}
  emptyText="No records match these filters."
/>
<DataTablePagination total={records.total} page={page} pageSize={pageSize} itemsLabel="records" />
```

Keep the existing table-picker (the per-table tabs/links with row counts) untouched above the filter row. `keyColumn` stays `register.notice_key_column` as in the current file.

`DataTablePagination` navigates with `tableSearch(searchParams, { page })`, which preserves all other params — filters survive paging. Verify `tableSearch` handles the `pageSize` param the same way (it does for companies; no change expected).

- [ ] **Step 3: Typecheck and unit tests**

Run: `npm run typecheck` — Expected: PASS.
Run: `npx vitest run` — Expected: all suites PASS.

- [ ] **Step 4: Commit**

```bash
git add app/routes/procurement-source.tsx
git commit -m "feat(backoffice): shadcn data table, filter sheet, and company links on register pages"
```

---

### Task 7: Browser verification

**Files:** none (verification only). Fix-forward anything found, amend the relevant task's commit style (`fix(backoffice): …`).

- [ ] **Step 1: Start the dev server**

Run from `corpscout/services/backoffice`: `npm run dev -- --port 5199` (background). Wait for "Local: http://localhost:5199".

- [ ] **Step 2: TED page (multi-country, all filters)**

Open `http://localhost:5199/procurements/ted?table=ted_notice_winners` in the browser (Playwright MCP):
- Table shows curated columns only — assert `fx_source`, `source_slug`, `source_run_id`, `partition_key`, `resolved_at`, `estimated_value_amount_usd` are NOT column headers; `winner_name`, `awarded_amount_usd` ARE.
- Money cells formatted (`1,234,567.89` style), right-aligned.
- Open Filters sheet: Country select lists 30 countries by full name, only Sweden/Finland/Norway enabled.
- Pick Sweden + a value range → Apply → URL contains `country=SE&valueMin=…`; row count shrinks; pagination preserves filters.
- A winner with a matched org number renders as a link to `/company/se/<orgnr>` (or `fi`/`no`); click one and confirm the company page loads.
- Zero console errors (hydration intact).

- [ ] **Step 3: Single-country register**

Open `http://localhost:5199/procurements/norway-doffin` (or `brazil-pncp` if Doffin is still empty):
- Sheet omits the Country section when the table has no country column (or shows it with only that country enabled).
- Enum dropdowns populated from real distinct values.
- Record detail page (`/procurements/<src>/<key>`) still shows ALL columns including the hidden ones.

- [ ] **Step 4: Full test suite + typecheck one last time**

Run: `npx vitest run && npm run typecheck` — Expected: PASS.

- [ ] **Step 5: Stop the dev server, close the browser tab.**

---

## Self-Review Notes

- Spec coverage: curation → Task 1; shadcn table → Tasks 5/6; filter sheet incl. country dropdown → Tasks 2/3/4/5/6; buyer/winner links → Tasks 4/6; detail page untouched → global constraint; FI business-ID normalization check → Task 7 observation (if FI match rate in links looks broken, file it as follow-up rather than scope-creeping).
- Type consistency: `FilterColumns` produced in Task 3, consumed by Tasks 4/6; `ProcurementFilterValues` strings only; `matchCompanies` returns `Record<string, {country_code, company_id}>` consumed in Task 6's `buildColumns`.
- Known judgment calls: `positionCaseInsensitiveUTF8` for name search (ClickHouse-native, index-friendly enough at 100k rows); "Clear all" via `clear=1` param handled in the loader's `q()`.
