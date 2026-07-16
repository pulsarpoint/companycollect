# Backoffice Company Detail Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/:country/companies/:id` — a company detail page with an identity header, overview, financials (per-year canonical metrics + a revenue/result chart), contacts, and domains sections, reached by clicking a company name in the list.

**Architecture:** The registry gains a per-country `detail` config: up to three SQL strings (`financialsQuery`, `contactsQuery`, `domainsQuery`), each parameterized `{id:String}` and returning a CANONICAL row shape so the UI is fully generic — one detail page renders whatever sections the country declares (the `features`-flags idea, finally load-bearing). `getCompanyDetail` fetches the company row (list-column exprs + industry, reusing the existing `industryQuery`) and the declared sections in parallel. Financial amounts are `toFloat64(...)`-wrapped in SQL so ClickHouse Decimals arrive as JSON numbers. The chart uses the already-installed `recharts` via the kept `ui/chart.tsx` wrapper.

**Tech Stack:** existing stack (React Router 8 SSR, Base UI shadcn, `chQuery`) + `recharts`/`ui/chart.tsx` (already in the tree since the dashboard-01 install; currently unused).

## Global Constraints

- App: `corpscout/services/backoffice`. pnpm, Node 22.22+, RR8 SSR, Base UI shadcn (`render` prop, never `asChild`).
- URL: `/:country/companies/:id` (`:id` = the registry idColumn value, URL-encoded in links). Unknown country OR unknown id → 404.
- SQL safety unchanged: `id` is ALWAYS bound as `{id:String}`; all identifiers/SQL from the static registry only; read-only.
- Every `detail.*Query` returns the canonical shape for its section (defined in Task 1); the UI is generic and renders only sections whose query exists AND returned rows. Missing values render `—`.
- Financial amounts must be JSON numbers: wrap every amount column in `toFloat64(...)` (Nullable propagates; `toString(fiscal_year)` for years).
- Chart: revenue and net result in USD by fiscal year (USD series are cross-country comparable); the per-year table shows original-currency values with the currency code. Chart renders client-side (recharts measures the DOM); the table is the SSR-verifiable part.
- Integration tests: real ClickHouse; Estonia for behavior tests (ids picked dynamically from the data, never hardcoded).
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths.

## Ground truth (verified live, 2026-07-16)

**Financial sources** (canonical section available for 6 countries):

| country | table (rows) | key | notes |
|---|---|---|---|
| no | `no_financial_statements` (313k) | `org_number` | wide; `operating_revenue_*` is the revenue column; possibly >1 filing/year → `LIMIT 1 BY fiscal_year` |
| fi | `fi_financial_metrics` (19k) | `business_id` | no `fiscal_year` column → `toYear(period_end)`; `currency_original`; `profit_loss_*` = net result; has `employees` |
| ee | `ee_financial_metrics` (1.5M) | `reg_code` | canonical wide shape |
| lv | `lv_financial_metrics` (2.0M) | `regcode` | canonical wide + `employees` |
| gb | `gb_financial_metrics` (24k) | `company_number` | canonical wide; SPARSE coverage (iXBRL subset) — most GB companies have no rows; section simply stays empty |
| br | `br_cvm_financial_metrics` (904k) | `cnpj_basico` | LONG format (`metric_name`/`amount_*`), listed companies only; values are lowercase: `period_type='annual'`, `consolidation_type` ∈ {'consolidated','individual'} (prefer consolidated); `is_latest_version` is always 1 (pre-deduped) |

**NO financials for:** se (`se_financial_metrics` is EMPTY — 245M raw `se_financial_facts` + 1.85M reports exist but metrics were never materialized → **pipeline gap, flagged to user**), sk (`sk_financial_metrics` has 1 row → same), cz/fr (no tables).

**Contacts** — canonical shape `(registry_id, contact_type, contact_value, is_current, ...)`, types email/mobile/phone/website/fax: `no_company_contacts` (113k), `fi_company_contacts` (119k), `ee_company_contacts` (638k), `lv_company_contacts` (2.6k), `cz_company_contacts` (6.8k), `br_company_contacts` (119M — point lookup by registry_id, fine). None for se/gb/fr/sk.

**Domains** — canonical shape `(registry_id, domain, website_url, domain_source, confidence, is_current, is_primary)`: `no_company_domains` (113k), `fi_company_domains` (119k), `ee_company_domains` (68k), `lv_company_domains` (1.6k), `cz_company_domains` (4.6k), `br_company_domains` (858k). None for se/gb/fr/sk.

Per-country `detail` config: **no/fi/ee/lv** = financials+contacts+domains; **br** = financials(CVM)+contacts+domains; **gb** = financials only; **cz** = contacts+domains only; **se/sk/fr** = no `detail` at all (page shows identity/overview/industry only).

---

### Task 1: Registry `detail` config

**Files:**
- Modify: `app/lib/countries.ts`
- Modify: `app/lib/countries.test.ts`

**Interfaces:**
- Produces (Tasks 2–5 rely on):

```ts
export interface CountryDetailConfig {
  /** {id:String} → canonical financial rows (see FinancialYearRow in queries.server). */
  financialsQuery?: string;
  /** {id:String} → { contact_type, contact_value } rows. */
  contactsQuery?: string;
  /** {id:String} → { domain, website_url, domain_source, confidence, is_primary } rows. */
  domainsQuery?: string;
}
// CountryConfig gains: detail?: CountryDetailConfig;
```

Canonical financial row columns every `financialsQuery` MUST alias (numbers via `toFloat64`, `NULL` where the source lacks the concept): `fiscal_year` (String), `currency` (String), `revenue_amount_original`, `revenue_amount_usd`, `net_result_amount_original`, `net_result_amount_usd`, `total_assets_amount_usd`, `equity_amount_usd`, `employees`.

- [ ] **Step 1: Write the failing tests**

Append to `app/lib/countries.test.ts`:

```ts
describe("detail config", () => {
  const FIN = ["no", "fi", "ee", "lv", "gb", "br"];
  const CONTACTS = ["no", "fi", "ee", "lv", "cz", "br"];
  const DOMAINS = ["no", "fi", "ee", "lv", "cz", "br"];
  const NONE = ["se", "sk", "fr"];

  it("declares detail sections exactly per data availability", () => {
    for (const c of COUNTRIES) {
      if (NONE.includes(c.code)) {
        expect(c.detail, c.code).toBeUndefined();
        continue;
      }
      expect(!!c.detail?.financialsQuery, c.code).toBe(FIN.includes(c.code));
      expect(!!c.detail?.contactsQuery, c.code).toBe(CONTACTS.includes(c.code));
      expect(!!c.detail?.domainsQuery, c.code).toBe(DOMAINS.includes(c.code));
    }
  });

  it("every detail query is parameterized and canonical", () => {
    for (const c of COUNTRIES) {
      for (const q of [c.detail?.financialsQuery, c.detail?.contactsQuery, c.detail?.domainsQuery]) {
        if (!q) continue;
        expect(q, c.code).toContain("{id:String}");
      }
      if (c.detail?.financialsQuery) {
        for (const col of [
          "AS fiscal_year", "AS currency",
          "AS revenue_amount_original", "AS revenue_amount_usd",
          "AS net_result_amount_original", "AS net_result_amount_usd",
          "AS total_assets_amount_usd", "AS equity_amount_usd", "AS employees",
        ]) {
          expect(c.detail.financialsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
      if (c.detail?.contactsQuery) {
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_type");
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_value");
      }
      if (c.detail?.domainsQuery) {
        for (const col of ["AS domain", "AS website_url", "AS domain_source", "AS confidence", "AS is_primary"]) {
          expect(c.detail.domainsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
    }
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm test countries` — FAIL (`detail` undefined everywhere / missing type).

- [ ] **Step 3: Implement**

Add `CountryDetailConfig` (as in Interfaces above) to `app/lib/countries.ts`, add `detail?: CountryDetailConfig;` to `CountryConfig`, then add per-country `detail` blocks.

Contacts pattern — identical for no/fi/ee/lv/cz/br, substitute the table (`no_company_contacts`, `fi_company_contacts`, `ee_company_contacts`, `lv_company_contacts`, `cz_company_contacts`, `br_company_contacts`):

```ts
contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM ee_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
```

Domains pattern — identical for no/fi/ee/lv/cz/br, substitute the table:

```ts
domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM ee_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
```

Financials — Estonia (same shape for lv with `regcode`+`lv_financial_metrics`+real `employees`; gb with `company_number`+`gb_financial_metrics`, `NULL AS employees`):

```ts
financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM ee_financial_metrics
WHERE reg_code = {id:String}
ORDER BY fiscal_year DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
```

(For lv replace the `NULL AS employees` line with `toFloat64(employees) AS employees`.)

Norway (`no_financial_statements`; possibly multiple filings per year → keep newest):

```ts
financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(operating_revenue_amount_original) AS revenue_amount_original,
  toFloat64(operating_revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM no_financial_statements
WHERE org_number = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
```

Finland (`fi_financial_metrics`; year from `period_end`, `currency_original`, `profit_loss_*` = net result):

```ts
financialsQuery: `SELECT toString(toYear(period_end)) AS fiscal_year, currency_original AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(profit_loss_amount_original) AS net_result_amount_original,
  toFloat64(profit_loss_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees
FROM fi_financial_metrics
WHERE business_id = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
```

Brazil (`br_cvm_financial_metrics`; long format → pivot; annual only, consolidated preferred over individual per (year, metric)):

```ts
financialsQuery: `SELECT toString(fy) AS fiscal_year, any(cur) AS currency,
  anyIf(orig, metric = 'revenue') AS revenue_amount_original,
  anyIf(usd, metric = 'revenue') AS revenue_amount_usd,
  anyIf(orig, metric = 'net_income') AS net_result_amount_original,
  anyIf(usd, metric = 'net_income') AS net_result_amount_usd,
  anyIf(usd, metric = 'total_assets') AS total_assets_amount_usd,
  anyIf(usd, metric = 'equity') AS equity_amount_usd,
  NULL AS employees
FROM (
  SELECT toYear(period_end_date) AS fy, metric_name AS metric,
    toFloat64(amount_original) AS orig, toFloat64(amount_usd) AS usd, currency AS cur
  FROM br_cvm_financial_metrics
  WHERE cnpj_basico = {id:String} AND period_type = 'annual'
  ORDER BY consolidation_type = 'consolidated' DESC
  LIMIT 1 BY fy, metric
)
GROUP BY fy
ORDER BY fy DESC
LIMIT 20`,
```

se/sk/fr: do NOT add a `detail` field at all.

- [ ] **Step 4: Run to verify green**

Run: `pnpm test countries` → PASS, then `pnpm typecheck && pnpm test` → all green (nothing consumes `detail` yet).

- [ ] **Step 5: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts
git commit -m "feat(backoffice): per-country company detail queries in registry"
```

---

### Task 2: `getCompanyDetail` query function

**Files:**
- Modify: `app/lib/queries.server.ts`
- Modify: `tests/queries.server.test.ts`

**Interfaces:**
- Consumes: `chQuery`, `CountryConfig.detail` (Task 1), existing `industryQuery`/`industryJoinKeyExpr`, `CompanyListRow`.
- Produces (Tasks 3–4 rely on):

```ts
export interface FinancialYearRow {
  fiscal_year: string; currency: string;
  revenue_amount_original: number | null; revenue_amount_usd: number | null;
  net_result_amount_original: number | null; net_result_amount_usd: number | null;
  total_assets_amount_usd: number | null; equity_amount_usd: number | null;
  employees: number | null;
}
export interface ContactRow { contact_type: string; contact_value: string }
export interface DomainRow {
  domain: string; website_url: string | null; domain_source: string;
  confidence: number | null; is_primary: 0 | 1;
}
export interface CompanyDetail {
  company: CompanyListRow;      // list-column keys + active + industry_code/industry_label
  financials: FinancialYearRow[];
  contacts: ContactRow[];
  domains: DomainRow[];
}
export async function getCompanyDetail(country: CountryConfig, id: string): Promise<CompanyDetail | null>
```

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/queries.server.test.ts` (imports: add `getCompanyDetail` to the queries.server import; `chQuery` from `~/lib/clickhouse.server`):

```ts
describe("getCompanyDetail (Estonia)", () => {
  it("returns null for an unknown id", async () => {
    const detail = await getCompanyDetail(ee, "does-not-exist-000");
    expect(detail).toBeNull();
  });

  it("returns company row with industry for a real id", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const id = String(page.rows[0].id);
    const detail = await getCompanyDetail(ee, id);
    expect(detail).not.toBeNull();
    expect(String(detail!.company.id)).toBe(id);
    expect(detail!.company).toHaveProperty("name");
    expect(detail!.company).toHaveProperty("active");
    expect(detail!.company).toHaveProperty("industry_code");
    expect(detail!.company).not.toHaveProperty("__industry_key");
  });

  it("returns canonical financials for a company that has them", async () => {
    const [row] = await chQuery<{ id: string }>(
      "SELECT reg_code AS id FROM ee_financial_metrics WHERE revenue_amount_usd IS NOT NULL LIMIT 1",
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.financials.length).toBeGreaterThan(0);
    const year = detail!.financials[0];
    expect(typeof year.fiscal_year).toBe("string");
    expect(typeof year.revenue_amount_usd).toBe("number"); // toFloat64 → JSON number, not string
    const years = detail!.financials.map((f) => f.fiscal_year);
    expect([...years].sort().reverse()).toEqual(years); // newest first
  });

  it("returns contacts and domains for companies that have them", async () => {
    const [c] = await chQuery<{ id: string }>(
      "SELECT registry_id AS id FROM ee_company_contacts WHERE is_current = 1 LIMIT 1",
    );
    const withContacts = await getCompanyDetail(ee, c.id);
    expect(withContacts!.contacts.length).toBeGreaterThan(0);
    expect(withContacts!.contacts[0]).toHaveProperty("contact_type");
    expect(withContacts!.contacts[0]).toHaveProperty("contact_value");

    const [d] = await chQuery<{ id: string }>(
      "SELECT registry_id AS id FROM ee_company_domains WHERE is_current = 1 LIMIT 1",
    );
    const withDomains = await getCompanyDetail(ee, d.id);
    expect(withDomains!.domains.length).toBeGreaterThan(0);
    expect(withDomains!.domains[0].domain).toBeTruthy();
  });
});
```

NOTE: the contacts/domains tests double as verification that `registry_id` in the canonical tables equals the companies idColumn value — if `getCompanyDetail(ee, c.id)` returned null, that assumption is broken and must be reported, not patched around.

- [ ] **Step 2: Run to verify failure**

Run: `pnpm test queries` — FAIL: `getCompanyDetail` not exported.

- [ ] **Step 3: Implement**

Append to `app/lib/queries.server.ts` (types from Interfaces above, then):

```ts
export async function getCompanyDetail(
  country: CountryConfig,
  id: string,
): Promise<CompanyDetail | null> {
  const joinKeyExpr = country.industryJoinKeyExpr ?? country.idColumn;
  const selectList = [
    ...country.columns.map((c) => `${c.expr} AS ${c.key}`),
    `toUInt8(${country.activeExpr}) AS active`,
    ...(country.industryQuery ? [`toString(${joinKeyExpr}) AS __industry_key`] : []),
  ].join(",\n       ");

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     WHERE ${country.idColumn} = {id:String}
     LIMIT 1`,
    { id },
  );
  if (rows.length === 0) return null;
  const company = rows[0];

  if (country.industryQuery) {
    const key = company.__industry_key ?? "";
    const industries = key
      ? await chQuery<{ company_id: string; industry_code: string | null; industry_label: string | null }>(
          country.industryQuery,
          { ids: [key] },
        )
      : [];
    company.industry_code = industries[0]?.industry_code ?? null;
    company.industry_label = industries[0]?.industry_label ?? null;
    delete company.__industry_key;
  }

  const [financials, contacts, domains] = await Promise.all([
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

  return { company, financials, contacts, domains };
}
```

- [ ] **Step 4: Run to verify green**

Run: `pnpm test queries` → PASS; `pnpm typecheck && pnpm test` → all green.

- [ ] **Step 5: Commit**

```bash
git add app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "feat(backoffice): company detail query with parallel section fetches"
```

---

### Task 3: Detail route, identity/overview/contacts/domains UI, row-click navigation

**Files:**
- Create: `app/routes/country-company-detail.tsx`
- Create: `app/components/detail/detail-sections.tsx`
- Modify: `app/routes.ts`
- Modify: `app/components/data-table/company-columns.tsx` (name cell → Link)

**Interfaces:**
- Consumes: `getCompanyDetail` (Task 2), `getCountry`, shadcn `Card`/`Badge`/`Table`, `facetLabel` idea NOT reused — column labels come from `country.columns`.
- Produces: the detail page shell; Task 4 adds the financials section into the same page (a `<FinancialsSection>` placeholder slot is NOT used — Task 4 edits this file; keep the financials block out entirely in this task).

- [ ] **Step 1: Route registration**

In `app/routes.ts`, add inside the `:country` children (order matters — `companies/:id` after `companies`):

```ts
route(":country", "routes/country.tsx", [
  index("routes/country-overview.tsx"),
  route("companies", "routes/country-companies.tsx"),
  route("companies/:id", "routes/country-company-detail.tsx"),
  route("facet-options", "routes/country-facet-options.ts"),
]),
```

- [ ] **Step 2: Sections component**

`app/components/detail/detail-sections.tsx`:

```tsx
import type { CompanyListRow, ContactRow, DomainRow } from "~/lib/queries.server";
import type { CountryConfig } from "~/lib/countries";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export const EMPTY = <span className="text-muted-foreground">—</span>;

function value(v: unknown) {
  const s = v == null ? "" : String(v);
  return s === "" ? EMPTY : s;
}

export function OverviewSection({
  country,
  company,
}: {
  country: CountryConfig;
  company: CompanyListRow;
}) {
  const fields = country.columns.filter((c) => c.key !== "name");
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Overview</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
          {fields.map((col) => (
            <div key={col.key} className="flex flex-col gap-0.5">
              <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                {col.label}
              </dt>
              <dd className={col.kind === "id" ? "font-mono text-sm" : "text-sm"}>
                {value(company[col.key])}
              </dd>
            </div>
          ))}
          <div className="flex flex-col gap-0.5">
            <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              Industry
            </dt>
            <dd className="text-sm">
              {company.industry_code || company.industry_label ? (
                <span className="flex items-baseline gap-1.5">
                  {company.industry_code ? (
                    <span className="text-muted-foreground font-mono text-xs">
                      {company.industry_code}
                    </span>
                  ) : null}
                  {company.industry_label ? <span>{company.industry_label}</span> : null}
                </span>
              ) : (
                EMPTY
              )}
            </dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  );
}

export function ContactsSection({ contacts }: { contacts: ContactRow[] }) {
  if (contacts.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contacts</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-1.5">
          {contacts.map((c, i) => (
            <li key={`${c.contact_type}-${c.contact_value}-${i}`} className="flex items-baseline gap-2 text-sm">
              <Badge variant="outline" className="w-20 justify-center">
                {c.contact_type}
              </Badge>
              <span className="break-all">{c.contact_value}</span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

export function DomainsSection({ domains }: { domains: DomainRow[] }) {
  if (domains.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Domains</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-1.5">
          {domains.map((d) => (
            <li key={d.domain} className="flex flex-wrap items-baseline gap-2 text-sm">
              <span className="font-medium">{d.domain}</span>
              {d.is_primary ? <Badge>primary</Badge> : null}
              <span className="text-muted-foreground text-xs">
                {d.domain_source}
                {d.confidence != null ? ` · ${Math.round(d.confidence * 100)}%` : ""}
              </span>
              {d.website_url ? (
                <a
                  href={d.website_url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-muted-foreground truncate text-xs underline"
                >
                  {d.website_url}
                </a>
              ) : null}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 3: The route**

`app/routes/country-company-detail.tsx`:

```tsx
import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/country-company-detail";
import { getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  ContactsSection,
  DomainsSection,
  OverviewSection,
} from "~/components/detail/detail-sections";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const detail = await getCompanyDetail(country, params.id);
  if (!detail) throw new Response("Company not found", { status: 404 });
  return { detail };
}

export function meta({ data, params }: Route.MetaArgs) {
  const name = data?.detail.company.name;
  return [{ title: name ? `${name} – CompanyCollect Backoffice` : `Company ${params.id}` }];
}

export default function CompanyDetail({ loaderData, params }: Route.ComponentProps) {
  const { detail } = loaderData;
  const country = getCountry(params.country)!;
  const { company } = detail;
  const status = country.columns.find((c) => c.kind === "status");

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={`/${country.code}/companies`} />}
        >
          <ArrowLeft className="size-4" />
          Companies
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-2xl font-semibold">{String(company.name ?? "")}</h2>
        {status ? (
          <Badge variant={company.active ? "default" : "outline"}>
            {String(company[status.key] ?? (company.active ? "active" : "inactive"))}
          </Badge>
        ) : null}
        <span className="text-muted-foreground font-mono text-sm">
          {String(company.id)}
        </span>
      </div>

      <OverviewSection country={country} company={company} />
      <ContactsSection contacts={detail.contacts} />
      <DomainsSection domains={detail.domains} />
    </div>
  );
}
```

- [ ] **Step 4: Row-click navigation**

In `app/components/data-table/company-columns.tsx`, the `name` special case inside `cellFor` gains a Link (import `Link` from `react-router`; `buildCompanyColumns` already receives `country`, thread it into `cellFor(col, country)`):

```tsx
if (col.key === "name") {
  const s = value == null ? "" : String(value);
  if (s === "") return EMPTY;
  return (
    <Link
      to={`/${country.code}/companies/${encodeURIComponent(String(row.original.id))}`}
      className="block max-w-[22rem] truncate font-medium underline-offset-2 hover:underline"
      title={s}
    >
      {s}
    </Link>
  );
}
```

- [ ] **Step 5: Verify**

`pnpm typecheck && pnpm test`, then `pnpm dev`:

```bash
ID=$(curl -s 'http://localhost:5183/ee/facet-options?column=status' >/dev/null; curl -s 'http://localhost:5183/ee/companies' | grep -o 'companies/[0-9]*"' | head -1 | grep -o '[0-9]*')
curl -s "http://localhost:5183/ee/companies/$ID" | grep -c 'Overview'          # >= 1
curl -s "http://localhost:5183/ee/companies/$ID" | grep -c 'Industry'          # >= 1
curl -s -o /dev/null -w '%{http_code}' 'http://localhost:5183/ee/companies/nope-000'  # 404
```

Browser: click a company name in /ee/companies → detail page; back button returns to the list. Kill the dev server.

- [ ] **Step 6: Commit**

```bash
git add app/routes/country-company-detail.tsx app/components/detail/detail-sections.tsx app/routes.ts app/components/data-table/company-columns.tsx
git commit -m "feat(backoffice): company detail page with overview, contacts and domains"
```

---

### Task 4: Financials section with chart

**Files:**
- Create: `app/components/detail/financials-section.tsx`
- Modify: `app/routes/country-company-detail.tsx` (render the section)

**Interfaces:**
- Consumes: `FinancialYearRow` (Task 2), `ui/chart.tsx` (`ChartContainer`, `ChartTooltip`, `ChartTooltipContent` — already in the tree from the dashboard-01 install) + `recharts` (`BarChart`, `Bar`, `XAxis`, `CartesianGrid`), shadcn `Card`/`Table`.
- Produces: `<FinancialsSection financials={FinancialYearRow[]} />` — renders null when empty.

- [ ] **Step 1: Implement the section**

`app/components/detail/financials-section.tsx`:

```tsx
import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";
import type { FinancialYearRow } from "~/lib/queries.server";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const chartConfig = {
  revenue: { label: "Revenue (USD)", color: "var(--chart-1)" },
  result: { label: "Net result (USD)", color: "var(--chart-2)" },
} satisfies ChartConfig;

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function money(v: number | null) {
  return v == null ? <span className="text-muted-foreground">—</span> : nf.format(v);
}

export function FinancialsSection({ financials }: { financials: FinancialYearRow[] }) {
  if (financials.length === 0) return null;
  // Chart wants oldest → newest, left to right.
  const chartData = [...financials]
    .reverse()
    .map((f) => ({ year: f.fiscal_year, revenue: f.revenue_amount_usd, result: f.net_result_amount_usd }));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financials</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <ChartContainer config={chartConfig} className="h-56 w-full">
          <BarChart data={chartData}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="year" tickLine={false} axisLine={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Bar dataKey="revenue" fill="var(--color-revenue)" radius={3} />
            <Bar dataKey="result" fill="var(--color-result)" radius={3} />
          </BarChart>
        </ChartContainer>

        <div className="overflow-x-auto">
          <Table className="min-w-[40rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead>Currency</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Net result</TableHead>
                <TableHead className="text-right">Total assets (USD)</TableHead>
                <TableHead className="text-right">Equity (USD)</TableHead>
                <TableHead className="text-right">Employees</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {financials.map((f) => (
                <TableRow key={f.fiscal_year}>
                  <TableCell className="tabular-nums">{f.fiscal_year}</TableCell>
                  <TableCell>{f.currency}</TableCell>
                  <TableCell className="text-right tabular-nums">{money(f.revenue_amount_original)}</TableCell>
                  <TableCell className="text-right tabular-nums">{money(f.net_result_amount_original)}</TableCell>
                  <TableCell className="text-right tabular-nums">{money(f.total_assets_amount_usd)}</TableCell>
                  <TableCell className="text-right tabular-nums">{money(f.equity_amount_usd)}</TableCell>
                  <TableCell className="text-right tabular-nums">{money(f.employees)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
```

API adaptation note (binding): `ui/chart.tsx` is the kept dashboard-01 wrapper — read it before finalizing; if its exports/`ChartConfig` shape differ from the snippet, adapt minimally (structure = spec: a two-series USD bar chart by year with tooltip, above an original-currency table) and report the adaptation. `--chart-1`/`--chart-2` CSS vars come from the shadcn theme in `app.css` — verify they exist; if named differently, use the theme's actual chart color vars.

- [ ] **Step 2: Wire into the route**

In `app/routes/country-company-detail.tsx`: `import { FinancialsSection } from "~/components/detail/financials-section";` and render `<FinancialsSection financials={detail.financials} />` between `<OverviewSection .../>` and `<ContactsSection .../>`.

- [ ] **Step 3: Verify**

`pnpm typecheck && pnpm test`, then `pnpm dev`:

```bash
# find an EE company with financials, open its page:
ID=$(curl -s "http://companycollect:8123/?user=default&password=password123" --data "SELECT reg_code FROM corpscout.ee_financial_metrics WHERE revenue_amount_usd IS NOT NULL LIMIT 1")
curl -s "http://localhost:5183/ee/companies/$ID" | grep -c 'Financials'        # >= 1
curl -s "http://localhost:5183/ee/companies/$ID" | grep -c 'Total assets'      # >= 1 (SSR table)
```

Browser: the chart renders with two series and a tooltip on an EE company with financials, and on a NO company (`/no/companies` → pick one via search for a large firm). A company with no financials shows no Financials card at all. Kill the dev server.

- [ ] **Step 4: Commit**

```bash
git add app/components/detail/financials-section.tsx app/routes/country-company-detail.tsx
git commit -m "feat(backoffice): financials section with usd chart and yearly table"
```

---

### Task 5: All-countries detail smoke, README, full gate

**Files:**
- Modify: `tests/queries.server.test.ts`
- Modify: `README.md`

- [ ] **Step 1: All-countries detail sweep**

Append inside `describe("searchCompanies across all countries")`:

```ts
it.each(COUNTRIES.map((c) => [c.code, c] as const))(
  "%s: company detail loads for a first-page id",
  async (_code, country) => {
    const page = await searchCompanies(country, { pageSize: 1 });
    const id = String(page.rows[0].id);
    const detail = await getCompanyDetail(country, id);
    expect(detail).not.toBeNull();
    expect(String(detail!.company.id)).toBe(id);
  },
  60_000,
);

it.each(
  COUNTRIES.filter((c) => c.detail?.financialsQuery).map((c) => [c.code, c] as const),
)(
  "%s: financials registry SQL is valid against live schema",
  async (_code, country) => {
    // Execute the registry SQL directly with a synthetic id: zero rows expected,
    // but a schema/SQL error surfaces as a ClickHouse exception → test failure.
    const rows = await chQuery(country.detail!.financialsQuery!, { id: "0" });
    expect(Array.isArray(rows)).toBe(true);
  },
  60_000,
);
```

Same pattern for `contactsQuery`/`domainsQuery`:

```ts
it.each(
  COUNTRIES.filter((c) => c.detail?.contactsQuery || c.detail?.domainsQuery).map(
    (c) => [c.code, c] as const,
  ),
)(
  "%s: contacts/domains registry SQL is valid against live schema",
  async (_code, country) => {
    for (const q of [country.detail?.contactsQuery, country.detail?.domainsQuery]) {
      if (!q) continue;
      const rows = await chQuery(q, { id: "0" });
      expect(Array.isArray(rows)).toBe(true);
    }
  },
  60_000,
);
```

Add needed imports (`chQuery` from `~/lib/clickhouse.server` if not already imported in this file). Any ClickHouse error here is a registry bug — report it verbatim, do not skip the country.

- [ ] **Step 2: README**

Add under `## Companies table`:

```markdown
### Company detail

`/{country}/companies/{id}` — identity header, overview (all list columns +
industry), and per-country sections declared in `countries.ts` (`detail`):
financials (no, fi, ee, lv, gb, br — canonical yearly metrics, USD chart via
recharts), contacts and domains (no, fi, ee, lv, cz, br). se/sk have no
financial metrics materialized yet (pipeline gap) and fr has no detail data —
those pages show identity/overview only. All section queries bind the id as
`{id:String}` and live in the registry, never in routes.
```

- [ ] **Step 3: Full gate**

`pnpm typecheck && pnpm test && pnpm build`, then `pnpm start`:

```bash
ID=$(curl -s 'http://localhost:3000/ee/companies' | grep -o 'companies/[0-9]*"' | head -1 | grep -o '[0-9]*')
curl -s "http://localhost:3000/ee/companies/$ID" | grep -c 'Overview'   # >= 1
```

Kill; port free.

- [ ] **Step 4: Commit**

```bash
git add tests/queries.server.test.ts README.md
git commit -m "test(backoffice): all-countries company detail smoke and docs"
```

---

## Out of scope (logged)

- Country-specialty sections (BR establishments/sanctions/debts, FI name history/tax registrations, SE financial facts) — second wave, after the SE/SK metrics pipeline gaps are resolved.
- Back-link preserving list state (filters/sort/page) — currently returns to the default list; follow-up (history-back or serialized return-to param).
- Technologies section (company → domains → `commoncrawl_page_technologies`) — the domains section is its future home; needs the rollup design.
- **Pipeline gaps for the user:** `se_financial_metrics` empty (245M raw facts + 1.85M reports exist, metrics never materialized); `sk_financial_metrics` has 1 row; plus previously logged LV NACE, FI legal-form, BR CNAE mapping.
