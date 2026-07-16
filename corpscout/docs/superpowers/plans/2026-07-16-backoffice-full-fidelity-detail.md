# Backoffice Full-Fidelity Detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show ALL available data on the company detail page — the full company record (every column), a Norway-specific full financial statement section (every field of `no_financial_statements`), an automatic renderer for any field not explicitly placed — plus the empties-last sort fix for the Finland list.

**Architecture:** Governing principle (user, 2026-07-16): **fidelity first, specific-first** — never drop a field because no polished UI exists for it; a plain field grid is acceptable; build sections per-country against that country's own shape and extract shared components only when a pattern repeats. Mechanically: `getCompanyDetail` gains a full `SELECT *` company `record` plus country-shaped `statements` rows (new registry `detail.statementsQuery`); a small field-rendering kit (humanize/format/lineage-split/FieldGrid) renders arbitrary records; Norway gets a dedicated statement component with explicit income/balance/metadata groups **and a computed "rest" group so any unplaced or future column auto-renders** — a test enforces that nothing can silently drop. Countries with a `statementsQuery` but no dedicated component fall back to auto-rendered field grids (fidelity by default).

**Tech Stack:** existing stack only (RR8 SSR, Base UI shadcn, `chQuery`). No new dependencies.

## Global Constraints

- App: `corpscout/services/backoffice`. pnpm, RR8 SSR, Base UI shadcn (`render` prop + `nativeButton={false}` on Button-as-Link).
- **Fidelity rule (binding):** every column of the fetched rows must end up rendered somewhere — visible grid, the collapsed "Source & lineage" block, or the "Other fields" group. Dropping a field is a defect. Tests enforce the partition.
- `id` bound as `{id:String}`; SQL/identifiers from the registry only; read-only ClickHouse.
- Lineage fields (shown, but collapsed): keys starting `source_` EXCEPT `source_url` (user-useful link), plus exactly `country_iso2`, `source_system`, `resolved_at`, `updated_from_raw_at`, `name_normalized`, `xml_object_key`, `xml_sha256`, `xml_size_bytes`. Everything else is a visible field.
- Value formatting: `null`/`''` → em dash; UInt8 flags on keys starting `is_`/`has_`/`opted_` → "yes"/"no"; finite numbers → `en-US` grouped format; strings starting `http://`/`https://` render as links; everything else `String(value)`.
- Empties-last sorting (list page): the rows query orders by `coalesce(toString(<sortExpr>), '') = '' ASC` FIRST, then the existing `<sortExpr> <dir>, <idColumn>` — empty/NULL sort-key rows go last regardless of direction. Count/pagination semantics unchanged.
- Integration tests: real ClickHouse; ids picked dynamically (existence-guarded, ORDER BY for determinism).
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths.

## Ground truth (verified live, 2026-07-16)

- `no_financial_statements` (312,733 rows ≈ 1 filing per company, mostly FY2024/2025): 51 columns — full P&L chain (`operating_revenue/operating_costs/operating_result/net_financial_items/pretax_result/net_result` × original+usd), balance sheet (`total_assets/current_assets/fixed_assets/equity/total_debt/current_liabilities/long_term_liabilities` × original+usd), metadata (`accounts_type` e.g. 'SELSKAP', `is_parent_company`, `statement_layout`, `accounting_rules`, `liquidation_accounts`, `is_not_audited`, `opted_out_audit`, `is_small_enterprise`, `journal_number`, `filing_id`, `period_start_date`, `period_end_date`, `fiscal_year`, `currency`, `legal_name`, `legal_form_code`, `last_submitted_accounts_year`, `fx_rate_to_usd`, `fx_rate_date`, `fx_source`, `source_url`) + lineage. Field coverage 69–98%. The current canonical card shows only 4 of these metrics — the reported defect.
- `fi_companies`: exactly **1,205 rows with `name = ''`** (no names in `fi_names` either; genuinely nameless registry stubs, their industry rows are also all-NULL) — under the default name-asc sort they fill the first ~25 pages. 458,995 companies have real names.
- Companies tables carry more columns than the list config shows (e.g. `ee_companies.address/company_url/source_url`, `no_companies.articles_purpose_original/activity_text_original`) — the full-record section surfaces all of them.

---

### Task 1: Field-rendering kit

**Files:**
- Create: `app/components/detail/fields.tsx`
- Create: `app/components/detail/fields.test.ts`

**Interfaces:**
- Produces (Tasks 2–3 rely on):
  - `humanizeFieldKey(key: string): string` — `"operating_revenue_amount_usd"` → `"Operating revenue amount USD"` (underscores → spaces, first letter capitalized, tokens in ACRONYMS upper-cased: `id, usd, url, vat, eu, fx, cnpj, cvm, nace, sni, ico, siren`)
  - `formatFieldValue(key: string, value: unknown): string | null` — null/`''` → `null` (caller renders the dash); `is_`/`has_`/`opted_`-prefixed keys with value 0/1 (number or "0"/"1") → `"no"`/`"yes"`; finite numbers → `en-US` grouped; else `String(value)`
  - `isLineageKey(key: string): boolean` — per the Global Constraints lineage rule
  - `splitFields(record: Record<string, unknown>): { visible: [string, unknown][]; lineage: [string, unknown][] }` — preserves key order
  - `<FieldGrid fields={[string, unknown][]} />` — dl grid; dash for null-formatted values; http(s) string values rendered as `<a target="_blank" rel="noreferrer">`

- [ ] **Step 1: Write the failing unit tests**

`app/components/detail/fields.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  formatFieldValue,
  humanizeFieldKey,
  isLineageKey,
  splitFields,
} from "~/components/detail/fields";

describe("humanizeFieldKey", () => {
  it("titles snake_case and upcases acronyms", () => {
    expect(humanizeFieldKey("operating_revenue_amount_usd")).toBe(
      "Operating revenue amount USD",
    );
    expect(humanizeFieldKey("business_id")).toBe("Business ID");
    expect(humanizeFieldKey("source_url")).toBe("Source URL");
    expect(humanizeFieldKey("name")).toBe("Name");
  });
});

describe("formatFieldValue", () => {
  it("maps empty to null (caller renders dash)", () => {
    expect(formatFieldValue("name", null)).toBeNull();
    expect(formatFieldValue("name", "")).toBeNull();
  });
  it("renders flag keys as yes/no", () => {
    expect(formatFieldValue("is_parent_company", 0)).toBe("no");
    expect(formatFieldValue("is_not_audited", 1)).toBe("yes");
    expect(formatFieldValue("opted_out_audit", "1")).toBe("yes");
  });
  it("groups numbers, passes strings through", () => {
    expect(formatFieldValue("total_assets_amount_original", 663788)).toBe("663,788");
    expect(formatFieldValue("accounts_type", "SELSKAP")).toBe("SELSKAP");
  });
});

describe("isLineageKey / splitFields", () => {
  it("classifies lineage vs visible, keeps source_url visible", () => {
    expect(isLineageKey("source_run_id")).toBe(true);
    expect(isLineageKey("source_url")).toBe(false);
    expect(isLineageKey("resolved_at")).toBe(true);
    expect(isLineageKey("name_normalized")).toBe(true);
    expect(isLineageKey("name")).toBe(false);
  });
  it("splits a record preserving order", () => {
    const { visible, lineage } = splitFields({
      name: "X", source_run_id: "r1", source_url: "https://a", resolved_at: "t",
    });
    expect(visible.map(([k]) => k)).toEqual(["name", "source_url"]);
    expect(lineage.map(([k]) => k)).toEqual(["source_run_id", "resolved_at"]);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm test fields` — FAIL: cannot resolve `~/components/detail/fields`.

- [ ] **Step 3: Implement**

`app/components/detail/fields.tsx`:

```tsx
const ACRONYMS = new Set(["id", "usd", "url", "vat", "eu", "fx", "cnpj", "cvm", "nace", "sni", "ico", "siren"]);

export function humanizeFieldKey(key: string): string {
  const words = key.split("_").map((w) => (ACRONYMS.has(w) ? w.toUpperCase() : w));
  const joined = words.join(" ");
  return joined.charAt(0).toUpperCase() + joined.slice(1);
}

const FLAG_PREFIXES = ["is_", "has_", "opted_"];
const nf = new Intl.NumberFormat("en-US");

export function formatFieldValue(key: string, value: unknown): string | null {
  if (value == null || value === "") return null;
  if (
    FLAG_PREFIXES.some((p) => key.startsWith(p)) &&
    (value === 0 || value === 1 || value === "0" || value === "1")
  ) {
    return Number(value) === 1 ? "yes" : "no";
  }
  if (typeof value === "number" && Number.isFinite(value)) return nf.format(value);
  return String(value);
}

const LINEAGE_EXACT = new Set([
  "country_iso2", "source_system", "resolved_at", "updated_from_raw_at",
  "name_normalized", "xml_object_key", "xml_sha256", "xml_size_bytes",
]);

export function isLineageKey(key: string): boolean {
  if (key === "source_url") return false;
  if (key.startsWith("source_")) return true;
  return LINEAGE_EXACT.has(key);
}

export function splitFields(record: Record<string, unknown>): {
  visible: [string, unknown][];
  lineage: [string, unknown][];
} {
  const visible: [string, unknown][] = [];
  const lineage: [string, unknown][] = [];
  for (const [key, value] of Object.entries(record)) {
    (isLineageKey(key) ? lineage : visible).push([key, value]);
  }
  return { visible, lineage };
}

const EMPTY = <span className="text-muted-foreground">—</span>;

export function FieldGrid({ fields }: { fields: [string, unknown][] }) {
  if (fields.length === 0) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
      {fields.map(([key, value]) => {
        const formatted = formatFieldValue(key, value);
        const isLink =
          typeof formatted === "string" &&
          (formatted.startsWith("http://") || formatted.startsWith("https://"));
        return (
          <div key={key} className="flex flex-col gap-0.5">
            <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              {humanizeFieldKey(key)}
            </dt>
            <dd className="text-sm break-words tabular-nums">
              {formatted === null ? (
                EMPTY
              ) : isLink ? (
                <a href={formatted} target="_blank" rel="noreferrer" className="underline underline-offset-2">
                  {formatted}
                </a>
              ) : (
                formatted
              )}
            </dd>
          </div>
        );
      })}
    </dl>
  );
}
```

- [ ] **Step 4: Run to verify green**

Run: `pnpm test fields` → PASS; `pnpm typecheck` → clean.

- [ ] **Step 5: Commit**

```bash
git add app/components/detail/fields.tsx app/components/detail/fields.test.ts
git commit -m "feat(backoffice): field rendering kit for full-fidelity records"
```

---

### Task 2: Full company record on the detail page

**Files:**
- Modify: `app/lib/queries.server.ts` (`CompanyDetail` gains `record`)
- Modify: `tests/queries.server.test.ts`
- Modify: `app/components/detail/detail-sections.tsx` (replace `OverviewSection` with `CompanyRecordSection`)
- Modify: `app/routes/country-company-detail.tsx` (render the new section)

**Interfaces:**
- Consumes: field kit (Task 1).
- Produces: `CompanyDetail.record: Record<string, unknown>` — the FULL row (`SELECT *`); `<CompanyRecordSection country company record />` — visible FieldGrid (+ industry entry) with a native `<details>` "Source & lineage" block for lineage fields.

- [ ] **Step 1: Write the failing integration test**

Append inside the existing `describe("getCompanyDetail (Estonia)")` in `tests/queries.server.test.ts`:

```ts
it("returns the full company record with every table column", async () => {
  const page = await searchCompanies(ee, { pageSize: 1 });
  const detail = await getCompanyDetail(ee, String(page.rows[0].id));
  const keys = Object.keys(detail!.record);
  // ee_companies has 24 columns — the record must carry them all,
  // including columns the list config never shows:
  expect(keys.length).toBeGreaterThanOrEqual(20);
  expect(keys).toContain("address");
  expect(keys).toContain("company_url");
  expect(keys).toContain("source_url");
  expect(detail!.record.reg_code).toBe(String(page.rows[0].id));
});
```

Run: `pnpm test queries` — FAIL (`record` missing).

- [ ] **Step 2: Implement the record fetch**

In `app/lib/queries.server.ts`: add `record: Record<string, unknown>;` to `CompanyDetail`, and in `getCompanyDetail` fetch it in the same parallel batch as the sections. Restructure the tail of the function:

```ts
  const recordPromise = chQuery<Record<string, unknown>>(
    `SELECT * FROM ${country.companiesTable} WHERE ${country.idColumn} = {id:String} LIMIT 1`,
    { id },
  );
  const sectionsPromise = Promise.all([
    country.detail?.financialsQuery
      ? chQuery<FinancialYearRow>(country.detail.financialsQuery, { id })
      : Promise.resolve([]),
    country.detail?.contactsQuery
      ? chQuery<ContactRow>(country.detail.contactsQuery, { id })
      : Promise.resolve([]),
    country.detail?.domainsQuery
      ? chQuery<DomainRow>(country.detail.domainsQuery, { id })
      : Promise.resolve([]),
  ]);
  recordPromise.catch(() => {});
  sectionsPromise.catch(() => {});
```

(then the existing industry fetch/merge, then `const [records, [financials, contacts, domains]] = await Promise.all([recordPromise, sectionsPromise]);` and return `{ company, record: records[0] ?? {}, financials, contacts, domains }`). NOTE: the two `.catch(() => {})` no-op handlers close the unhandled-rejection window already logged from the previous review — the later `await` still surfaces real errors. Keep them.

- [ ] **Step 3: Replace the overview section**

In `app/components/detail/detail-sections.tsx`, DELETE `OverviewSection` (and its now-unused imports if any) and add:

```tsx
import { FieldGrid, splitFields } from "~/components/detail/fields";
import type { CompanyListRow } from "~/lib/queries.server";

export function CompanyRecordSection({
  company,
  record,
}: {
  company: CompanyListRow;
  record: Record<string, unknown>;
}) {
  const { visible, lineage } = splitFields(record);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Company record</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <FieldGrid
          fields={[
            ...visible,
            ["industry", [company.industry_code, company.industry_label].filter(Boolean).join(" ") || null],
          ]}
        />
        {lineage.length > 0 ? (
          <details>
            <summary className="text-muted-foreground cursor-pointer text-xs font-medium uppercase tracking-wide">
              Source &amp; lineage
            </summary>
            <div className="pt-3">
              <FieldGrid fields={lineage} />
            </div>
          </details>
        ) : null}
      </CardContent>
    </Card>
  );
}
```

In `app/routes/country-company-detail.tsx`: replace `<OverviewSection country={country} company={company} />` with `<CompanyRecordSection company={company} record={detail.record} />` (drop the `OverviewSection` import; drop the now-unused `country` prop pass — `country` is still used elsewhere in the file).

- [ ] **Step 4: Verify**

`pnpm test queries` → PASS; `pnpm typecheck && pnpm test` → all green. Dev server: `curl -s http://localhost:5183/ee/companies/<any id from the list> | grep -c 'Company record'` ≥ 1 and `grep -c 'Source &amp; lineage\|Source & lineage'` ≥ 1. Kill server.

- [ ] **Step 5: Commit**

```bash
git add app/lib/queries.server.ts tests/queries.server.test.ts app/components/detail/detail-sections.tsx app/routes/country-company-detail.tsx
git commit -m "feat(backoffice): full-fidelity company record section"
```

---

### Task 3: Norway full financial statement section

**Files:**
- Modify: `app/lib/countries.ts` (add `statementsQuery` to `CountryDetailConfig` + Norway's query)
- Modify: `app/lib/countries.test.ts`
- Modify: `app/lib/queries.server.ts` (`CompanyDetail.statements`)
- Modify: `tests/queries.server.test.ts`
- Create: `app/components/detail/countries/no-financials.tsx`
- Modify: `app/routes/country-company-detail.tsx` (country section map)

**Interfaces:**
- Produces:
  - `CountryDetailConfig.statementsQuery?: string` — `{id:String}` → FULL country-shaped rows (`SELECT *`), newest first, ALL filings (no per-year dedup — parent AND company accounts both show)
  - `CompanyDetail.statements: Record<string, unknown>[]`
  - `<NoFinancialsSection statements />` and a `COUNTRY_FINANCIALS` map in the route: `{ no: NoFinancialsSection }`; a country with a specific component renders it INSTEAD of the generic `FinancialsSection`; a country with `statementsQuery` but no component gets `<StatementsFallback statements />` (auto FieldGrid per row — fidelity default).

- [ ] **Step 1: Failing registry + query tests**

Append to `app/lib/countries.test.ts` (inside `describe("detail config")`):

```ts
it("norway declares a full statements query; others do not yet", () => {
  for (const c of COUNTRIES) {
    if (c.code === "no") {
      expect(c.detail?.statementsQuery).toContain("{id:String}");
      expect(c.detail?.statementsQuery).toContain("no_financial_statements");
    } else {
      expect(c.detail?.statementsQuery, c.code).toBeUndefined();
    }
  }
});
```

Append to `tests/queries.server.test.ts`:

```ts
describe("getCompanyDetail statements (Norway)", () => {
  it("returns full statement rows for a company with filings", async () => {
    const no = getCountry("no")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT org_number AS id FROM no_financial_statements
       WHERE operating_costs_amount_original IS NOT NULL
         AND org_number IN (SELECT org_number FROM no_companies)
       ORDER BY org_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(no, row.id);
    expect(detail!.statements.length).toBeGreaterThan(0);
    const stmt = detail!.statements[0];
    // full-fidelity: raw table columns present, not the canonical subset
    expect(stmt).toHaveProperty("operating_costs_amount_original");
    expect(stmt).toHaveProperty("accounts_type");
    expect(stmt).toHaveProperty("is_parent_company");
  });

  it("countries without statementsQuery return an empty array", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const detail = await getCompanyDetail(ee, String(page.rows[0].id));
    expect(detail!.statements).toEqual([]);
  });
});
```

Run both — FAIL.

- [ ] **Step 2: Registry + query layer**

`app/lib/countries.ts` — add to `CountryDetailConfig`:

```ts
  /**
   * {id:String} → FULL country-shaped statement rows (SELECT *), newest
   * first, ALL filings (no per-year dedup). Rendered by a country-specific
   * component when one exists, else by the auto field-grid fallback.
   */
  statementsQuery?: string;
```

Norway's `detail` block gains:

```ts
statementsQuery: `SELECT * FROM no_financial_statements
WHERE org_number = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 40`,
```

`app/lib/queries.server.ts` — `CompanyDetail` gains `statements: Record<string, unknown>[];`; add to the parallel batch in `getCompanyDetail`:

```ts
  const statementsPromise = country.detail?.statementsQuery
    ? chQuery<Record<string, unknown>>(country.detail.statementsQuery, { id })
    : Promise.resolve([]);
  statementsPromise.catch(() => {});
```

and include it in the final `Promise.all` + returned object.

Run the two test files → PASS.

- [ ] **Step 3: The Norway component (with the fidelity-guarantee "rest" group)**

`app/components/detail/countries/no-financials.tsx`:

```tsx
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { FieldGrid, formatFieldValue, isLineageKey } from "~/components/detail/fields";

const INCOME_KEYS = [
  "operating_revenue_amount_original", "operating_revenue_amount_usd",
  "operating_costs_amount_original", "operating_costs_amount_usd",
  "operating_result_amount_original", "operating_result_amount_usd",
  "net_financial_items_amount_original", "net_financial_items_amount_usd",
  "pretax_result_amount_original", "pretax_result_amount_usd",
  "net_result_amount_original", "net_result_amount_usd",
];
const BALANCE_KEYS = [
  "total_assets_amount_original", "total_assets_amount_usd",
  "fixed_assets_amount_original", "fixed_assets_amount_usd",
  "current_assets_amount_original", "current_assets_amount_usd",
  "equity_amount_original", "equity_amount_usd",
  "total_debt_amount_original", "total_debt_amount_usd",
  "long_term_liabilities_amount_original", "long_term_liabilities_amount_usd",
  "current_liabilities_amount_original", "current_liabilities_amount_usd",
];
const META_KEYS = [
  "period_start_date", "period_end_date", "accounts_type", "is_parent_company",
  "statement_layout", "accounting_rules", "liquidation_accounts",
  "is_not_audited", "opted_out_audit", "is_small_enterprise",
  "journal_number", "filing_id", "last_submitted_accounts_year",
  "legal_name", "legal_form_code",
  "fx_rate_to_usd", "fx_rate_date", "fx_source", "source_url",
];
const HEADER_KEYS = ["fiscal_year", "currency", "org_number"];
const PLACED = new Set([...INCOME_KEYS, ...BALANCE_KEYS, ...META_KEYS, ...HEADER_KEYS]);

/** Exported for the fidelity test: keys a statement row may contain that are
 * neither placed in a group nor lineage end up in the "Other fields" grid. */
export function restKeys(row: Record<string, unknown>): string[] {
  return Object.keys(row).filter((k) => !PLACED.has(k) && !isLineageKey(k));
}

function pick(row: Record<string, unknown>, keys: string[]): [string, unknown][] {
  return keys.filter((k) => k in row).map((k) => [k, row[k]]);
}

export function NoFinancialsSection({
  statements,
}: {
  statements: Record<string, unknown>[];
}) {
  if (statements.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financial statements (Brønnøysund)</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {statements.map((row, i) => (
          <div key={`${row.fiscal_year}-${row.filing_id ?? i}`} className="space-y-4">
            <p className="text-sm font-semibold">
              {formatFieldValue("fiscal_year", row.fiscal_year) ?? "?"}
              {" · "}
              {formatFieldValue("accounts_type", row.accounts_type) ?? "—"}
              {Number(row.is_parent_company) === 1 ? " (parent/group accounts)" : ""}
              {" · "}
              {String(row.currency ?? "")}
            </p>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Income statement</p>
              <FieldGrid fields={pick(row, INCOME_KEYS)} />
            </div>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Balance sheet</p>
              <FieldGrid fields={pick(row, BALANCE_KEYS)} />
            </div>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Filing details</p>
              <FieldGrid fields={pick(row, META_KEYS)} />
            </div>
            {restKeys(row).length > 0 ? (
              <div>
                <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Other fields</p>
                <FieldGrid fields={pick(row, restKeys(row))} />
              </div>
            ) : null}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

/** Fallback for countries with statementsQuery but no dedicated component. */
export function StatementsFallback({
  statements,
}: {
  statements: Record<string, unknown>[];
}) {
  if (statements.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financial statements</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {statements.map((row, i) => (
          <FieldGrid key={i} fields={Object.entries(row).filter(([k]) => !isLineageKey(k))} />
        ))}
      </CardContent>
    </Card>
  );
}
```

Add the fidelity-guarantee test to `tests/queries.server.test.ts` (inside the Norway statements describe; import `restKeys` from `~/components/detail/countries/no-financials`):

```ts
it("every statement column is placed in a group, lineage, or rest (fidelity guarantee)", async () => {
  const no = getCountry("no")!;
  const [row] = await chQuery<{ id: string }>(
    `SELECT org_number AS id FROM no_financial_statements
     WHERE org_number IN (SELECT org_number FROM no_companies)
     ORDER BY org_number LIMIT 1`,
  );
  const detail = await getCompanyDetail(no, row.id);
  const rest = restKeys(detail!.statements[0]);
  // Today every column is explicitly grouped; a future migration adding a
  // column makes it appear in "Other fields" automatically — never dropped.
  expect(rest).toEqual([]);
});
```

- [ ] **Step 4: Route wiring — country section map**

In `app/routes/country-company-detail.tsx`:

```tsx
import { NoFinancialsSection, StatementsFallback } from "~/components/detail/countries/no-financials";
import type { ComponentType } from "react";

const COUNTRY_FINANCIALS: Record<
  string,
  ComponentType<{ statements: Record<string, unknown>[] }>
> = {
  no: NoFinancialsSection,
};
```

and replace the `<FinancialsSection financials={detail.financials} />` line with:

```tsx
{(() => {
  const Specific = COUNTRY_FINANCIALS[country.code];
  if (Specific) return <Specific statements={detail.statements} />;
  if (detail.statements.length > 0) return <StatementsFallback statements={detail.statements} />;
  return <FinancialsSection financials={detail.financials} />;
})()}
```

(Norway renders the full statement section instead of the generic card; every other country is unchanged today.)

- [ ] **Step 5: Verify**

`pnpm typecheck && pnpm test` → green. Dev server: `curl -s http://localhost:5183/no/companies/926056522 | grep -c 'Income statement'` ≥ 1, `grep -c 'Operating costs'` ≥ 1, `grep -c 'Balance sheet'` ≥ 1. An EE company still shows the generic Financials card. Kill server.

- [ ] **Step 6: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts app/lib/queries.server.ts tests/queries.server.test.ts app/components/detail/countries/no-financials.tsx app/routes/country-company-detail.tsx
git commit -m "feat(backoffice): norway full financial statement section"
```

---

### Task 4: Empties-last sorting on the companies list

**Files:**
- Modify: `app/lib/queries.server.ts` (`searchCompanies` ORDER BY)
- Modify: `tests/queries.server.test.ts`

- [ ] **Step 1: Failing integration tests**

Append to `tests/queries.server.test.ts`:

```ts
describe("empties-last sorting (Finland has 1,205 nameless registry stubs)", () => {
  const fi = getCountry("fi")!;

  it("default name-asc page 1 starts with real names, not empty stubs", async () => {
    const result = await searchCompanies(fi, { pageSize: 25 });
    for (const row of result.rows) {
      expect(String(row.name)).not.toBe("");
    }
  });

  it("empties stay last under desc too", async () => {
    const result = await searchCompanies(fi, { sort: "name", dir: "desc", pageSize: 25 });
    for (const row of result.rows) {
      expect(String(row.name)).not.toBe("");
    }
  });

  it("total still counts the stubs (ordering only, no filtering)", async () => {
    const result = await searchCompanies(fi, { pageSize: 25 });
    expect(result.total).toBeGreaterThan(460_000); // 460,200 incl. the 1,205 stubs
  });
});
```

Run: `pnpm test queries` — the first two FAIL (page 1 is currently all empty names).

- [ ] **Step 2: Implement**

In `searchCompanies` (`app/lib/queries.server.ts`), change the rows query's ORDER BY from:

```
ORDER BY ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
```

to:

```
ORDER BY coalesce(toString(${sortColumn.expr}), '') = '' ASC, ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
```

(one added leading term; the count query is untouched — totals include stubs).

- [ ] **Step 3: Verify green + no regressions**

`pnpm test queries` → PASS (including the all-countries sweeps and the sort-direction tests — the EE `sorts by a whitelisted column` test still passes because EE ids/names are never empty). Full `pnpm typecheck && pnpm test` → green. Dev server spot-check: `http://localhost:5183/fi/companies` page 1 shows real company names; `?page=9200` (the tail) shows the stubs. Kill server.

- [ ] **Step 4: Commit**

```bash
git add app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "fix(backoffice): sort empty values last in company lists"
```

---

### Task 5: Gate, README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README**

Add under `### Company detail`:

```markdown
Fidelity rule: the detail page shows every column of the company row
("Company record" card; lineage fields collapsed under "Source & lineage")
and country-specific sections render full source shapes — Norway shows the
complete Brønnøysund statement (all P&L/balance/filing fields; any column a
future migration adds lands in "Other fields" automatically). Never trim a
country's data to fit a generic UI — add a country component instead
(`app/components/detail/countries/`, wired via COUNTRY_FINANCIALS in the
detail route).
```

- [ ] **Step 2: Full gate**

`pnpm typecheck && pnpm test && pnpm build`, then `pnpm start`:

```bash
curl -s 'http://localhost:3000/no/companies/926056522' | grep -c 'Income statement'   # >= 1
curl -s 'http://localhost:3000/fi/companies' | grep -c ' Oy'                           # >= 1 — page 1 now shows real Finnish company names (most end in "Oy")
```

Kill; port free.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(backoffice): document full-fidelity detail rule"
```

---

## Out of scope (logged)

- Country-specific sections for EE/LV/GB/FI/BR financials depth (next passes, one country at a time — EE first candidate); BR establishments/sanctions; FI name history; SE facts.
- FI legal-form column stays empty on every page until the upstream backfill (already-logged pipeline gap).
- USD/original value pairing polish in the NO layout (side-by-side columns instead of separate grid cells) — iterate after it ships.
