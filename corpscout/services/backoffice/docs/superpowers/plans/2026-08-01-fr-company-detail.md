# France company detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A French company page shows its government contracts, its filed
accounts with the ratio suite INPI publishes, and its Wikidata entry.

**Architecture:** France declares five queries in a new `app/lib/detail-queries/fr.ts`.
Three use config keys that already exist, so contracts and Wikidata need SQL
only. Two new keys carry the extended financial metrics and the contract
summary, rendered by a new France-specific section and an optional prop on the
shared contracts section.

**Tech Stack:** React Router 8 (framework mode), TypeScript, shadcn/ui,
ClickHouse over HTTP (`app/lib/clickhouse.server.ts`), vitest.

**Spec:** `docs/superpowers/specs/2026-08-01-fr-company-detail-design.md`

## Global Constraints

- **All commands run from `corpscout/services/backoffice`.**
- **Gate every task with `npm run typecheck && npx vitest run`.** Both must pass
  before the commit step.
- **Commit by explicit path.** The working tree carries unrelated in-flight WIP;
  never `git add -A`.
- **Tests execute against live ClickHouse**, following
  `tests/public-contracts.queries.test.ts`. Asserting on SQL strings alone does
  not count as coverage.
- **A null figure is WITHHELD, not zero.** 23.5% of French filings are
  `Partiellement confidentiel`. Never render `0` or `€0` for a null.
- **Route components must not use values from `~/lib/*.server` modules in the
  component body** — React Router refuses server-only modules in the client
  bundle. `import type` from a `.server` module is fine (see backoffice CLAUDE.md).
- **France publishes no per-winner contract value.** `value_amount_original` and
  `value_amount_usd` are NULL for all 721,161 rows; the notice totals are
  populated for all of them. Never divide a notice total by its winners.
- **France has no equity or total assets.** Both columns are NULL for all
  1,586,046 rows of `fr_company_financials_latest`. Do not join that table.

## Verified facts

Every query below was run against live ClickHouse on 2026-08-01 and returned
the shape stated here.

| fact | value |
| --- | --- |
| `fr_financial_metrics` | 6,542,232 rows, 1,586,046 companies, 100% EUR |
| ratio fill | revenue/EBITDA 100%, financial autonomy 99.96%, debt ratio 99.93%, interest coverage 76.5% |
| duplicate `(siren, fiscal_year)` | 41,055 pairs, across balance types C (4,924,259) / S (1,586,394) / K (31,579) |
| confidentiality | Public 4,951,478 · Partiellement confidentiel 1,538,303 · RAPCAC 50,273 · Publication simplifiee 2,178 |
| `fr_government_contracts` | 721,161 rows, 99,287 companies, sole source `france_decp_procurement` |
| contracts per company | median 2, p90 15, p99 69, **max 7,035**, 616 companies over 100 |
| `wikidata_company_identifiers` `fr_siren` | 169 |

**Test seeds**, all confirmed to exist:

| siren | name | why |
| --- | --- | --- |
| `055800296` | FNAC DARTY | 8 financial years, 7 contracts, **and** a Wikidata match (`Q47088340`) — exercises all three sections |
| `531615169` | — | 13 fiscal years with duplicate balance types — the dedup case |
| `330714569` | — | has contracts and a summary row |

## File Structure

| file | responsibility |
| --- | --- |
| `app/lib/detail-queries/fr.ts` | **new** — France's five SQL strings, nothing else. Pure string consts, no imports. |
| `app/lib/countries.ts` | two new optional keys on `CountryDetailConfig`; France's `detail` block references the consts |
| `app/lib/queries.server.ts` | `FrFinancialRow` + `ContractSummaryRow` types, two promises, two `CompanyDetail` fields |
| `app/components/detail/countries/fr-financials.tsx` | **new** — the France financials section plus its pure formatting helpers |
| `app/components/detail/countries/fr-financials.test.ts` | **new** — unit tests for those helpers |
| `app/components/detail/public-contracts-section.tsx` | optional `summary` prop |
| `app/routes/country-company-detail.tsx` | render `FrFinancialsSection`, pass `summary` |
| `tests/fr-detail.queries.test.ts` | **new** — live-ClickHouse tests for all five of France's queries |

**One deviation from the spec, deliberate.** The spec named
`tests/fr-financials.queries.test.ts` and a summary assertion added to
`tests/public-contracts.queries.test.ts`. This plan puts all five of France's
query tests in one `tests/fr-detail.queries.test.ts` instead: the summary is
not a `publicContractsQuery`, so asserting it in that file would test one
country inside a file that is otherwise parameterised over all of them. France
still joins that file's parameterised run automatically — its `withContracts`
filter is derived from which countries declare the query, so Task 1 earns that
coverage without editing it.

---

### Task 1: Contracts and Wikidata

France declares three queries whose config keys and loaders **already exist**.
When this task lands, `/company/fr/055800296` shows a contracts table and a
Wikidata card with no plumbing changes at all.

**Files:**
- Create: `app/lib/detail-queries/fr.ts`
- Create: `tests/fr-detail.queries.test.ts`
- Modify: `app/lib/countries.ts`

**Interfaces:**
- Consumes: `PublicContractRow`, `WikidataCompanyRow`, `WikidataPersonRow` from `~/lib/queries.server` (all already defined)
- Produces: `FR_PUBLIC_CONTRACTS_QUERY`, `FR_WIKIDATA_QUERY`, `FR_WIKIDATA_PEOPLE_QUERY` from `~/lib/detail-queries/fr`

- [ ] **Step 1: Write the failing test**

Create `tests/fr-detail.queries.test.ts`:

```ts
// Live ClickHouse integration tests for France's company-detail queries.
// Own file, so unrelated in-flight source work does not collide here.
import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";

/** FNAC DARTY. Chosen because one company exercises all three sections:
 * 8 filed fiscal years, 7 contract wins, and a Wikidata match (Q47088340). */
const FNAC = "055800296";

describe("France public contracts", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.publicContractsQuery).toBeTruthy();
  });

  it("reads France's own contracts view", () => {
    expect(getCountry("fr")!.detail!.publicContractsQuery).toContain(
      "FROM fr_government_contracts",
    );
  });

  it("returns nothing for an id that cannot exist", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.publicContractsQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("returns canonical rows for a company with wins", async () => {
    const rows = await chQuery<{
      source: string;
      amount_original: number | null;
      notice_amount_original: number | null;
    }>(getCountry("fr")!.detail!.publicContractsQuery!, { id: FNAC });
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].source).toBe("france_decp_procurement");
    // DECP publishes the notice total, never a per-winner split. The section
    // labels the notice figure rather than passing it off as this company's
    // share -- if per-winner values ever appear, this assertion is the signal
    // to revisit that labelling, not to delete the check.
    expect(rows.every((r) => r.amount_original == null)).toBe(true);
    expect(rows.some((r) => r.notice_amount_original != null)).toBe(true);
  });
});

describe("France Wikidata", () => {
  it("declares both queries", () => {
    expect(getCountry("fr")?.detail?.wikidataQuery).toBeTruthy();
    expect(getCountry("fr")?.detail?.wikidataPeopleQuery).toBeTruthy();
  });

  it("matches FNAC DARTY on its siren", async () => {
    const rows = await chQuery<{ wikidata_id: string; official_name: string }>(
      getCountry("fr")!.detail!.wikidataQuery!,
      { id: FNAC },
    );
    expect(rows.length).toBe(1);
    expect(rows[0].wikidata_id).toBe("Q47088340");
  });

  it("returns nothing for an unmatched company", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.wikidataQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("people query executes against the live schema", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.wikidataPeopleQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });
});
```

- [ ] **Step 2: Run it and watch it fail**

Run: `npx vitest run tests/fr-detail.queries.test.ts`
Expected: FAIL — `expect(undefined).toBeTruthy()`, because France declares none
of these queries yet.

- [ ] **Step 3: Create the SQL module**

Create `app/lib/detail-queries/fr.ts`:

```ts
/**
 * France's company-detail queries.
 *
 * Held here rather than inline in countries.ts because that file is 1,482
 * lines and already mostly SQL literals; France adds five more. The config
 * still lives beside every other country's -- only the strings moved.
 *
 * Plain string consts with no imports, so this module is safe on both sides of
 * the server boundary (countries.ts is imported by route components).
 */

/**
 * Contract wins, in the canonical PublicContractRow shape.
 *
 * DECP publishes no per-winner value: value_amount_original and
 * value_amount_usd are NULL for all 721,161 French rows, while the notice
 * totals are populated for all of them. Both are selected, and
 * PublicContractsSection renders the notice figure LABELLED as the whole
 * procurement rather than as this company's share. Never divide it by the
 * number of winners -- DECP does not say how it was split.
 */
export const FR_PUBLIC_CONTRACTS_QUERY = `SELECT
  source_slug AS source,
  concat(source_notice_id, if(source_lot_id = '', '', concat(':', source_lot_id))) AS notice_ref,
  coalesce(toString(publication_date), '') AS contract_date,
  buyer_name,
  title,
  toFloat64(value_amount_original) AS amount_original,
  toFloat64(value_amount_usd) AS amount_usd,
  value_currency AS currency,
  toFloat64(notice_value_amount_original) AS notice_amount_original,
  toFloat64(notice_value_amount_usd) AS notice_amount_usd,
  notice_value_currency AS notice_currency,
  source_url
FROM fr_government_contracts
WHERE company_id = {id:String}
ORDER BY publication_date DESC NULLS LAST, contract_id
LIMIT 100`;

/**
 * Wikidata enrichment, matched on the SIREN (P1616) with an LEI fallback.
 *
 * Coverage is thin -- 169 companies carry an fr_siren identifier and 168 are
 * reachable through an FR-jurisdiction LEI, almost entirely the same set. The
 * section hides itself when unmatched, which is why wiring it at this coverage
 * still pays: it is right for the handful of large companies that do match
 * (FNAC DARTY, Q47088340) and invisible everywhere else.
 */
export const FR_WIKIDATA_QUERY = `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'FR'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
) AS my_lei
SELECT w.wikidata_id AS wikidata_id,
  w.wikidata_url AS wikidata_url,
  coalesce(w.company_description, '') AS description,
  coalesce(w.official_name, '') AS official_name,
  coalesce(toString(w.inception_date), '') AS inception_date,
  w.employee_count AS employee_count,
  coalesce(toString(w.employee_count_point_in_time), '') AS employee_count_as_of,
  coalesce(w.industry_label, '') AS industry_label,
  coalesce(w.legal_form_label, '') AS legal_form_label,
  coalesce(w.headquarters_label, '') AS headquarters,
  coalesce(w.headquarters_country_label, '') AS headquarters_country,
  coalesce(w.logo_image_url, '') AS logo_url,
  toUInt8(w.has_current_listing) AS has_current_listing,
  coalesce(l.listings, '') AS listings,
  coalesce(s.websites, '') AS websites,
  coalesce(li.linkedin, '') AS linkedin_id
FROM wikidata_companies AS w
LEFT JOIN (
  SELECT wikidata_id,
    arrayStringConcat(groupUniqArray(concat(exchange_name, ': ', ticker)), ' | ') AS listings
  FROM wikidata_company_listings
  WHERE is_current AND ticker != ''
  GROUP BY wikidata_id
) AS l ON l.wikidata_id = w.wikidata_id
LEFT JOIN (
  SELECT wikidata_id,
    arrayStringConcat(groupUniqArray(website_url), ' ') AS websites
  FROM wikidata_company_websites
  GROUP BY wikidata_id
) AS s ON s.wikidata_id = w.wikidata_id
LEFT JOIN (
  SELECT wikidata_id, any(identifier_value) AS linkedin
  FROM wikidata_company_identifiers
  WHERE identifier_type = 'linkedin_company_id'
  GROUP BY wikidata_id
) AS li ON li.wikidata_id = w.wikidata_id
WHERE w.wikidata_id IN (
  SELECT wikidata_id FROM wikidata_company_identifiers
  WHERE (identifier_type = 'fr_siren'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
LIMIT 1`;

/** Company-anchored Wikidata people, same match rule as FR_WIKIDATA_QUERY. */
export const FR_WIKIDATA_PEOPLE_QUERY = `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'FR'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
) AS my_lei
SELECT p.person_wikidata_id AS person_wikidata_id,
  per.name AS name,
  coalesce(per.description, '') AS description,
  per.birth_year AS birth_year,
  coalesce(per.image_url, '') AS image_url,
  coalesce(per.wikidata_url, '') AS wikidata_url,
  p.role_label AS role_label,
  toUInt8(p.is_current) AS is_current,
  coalesce(toString(p.start_date), '') AS start_date,
  coalesce(toString(p.end_date), '') AS end_date
FROM wikidata_company_people AS p
JOIN wikidata_persons AS per ON per.person_wikidata_id = p.person_wikidata_id
WHERE p.company_wikidata_id IN (
  SELECT wikidata_id FROM wikidata_company_identifiers
  WHERE (identifier_type = 'fr_siren'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
ORDER BY p.is_current DESC, p.role_label, per.name
LIMIT 100`;
```

- [ ] **Step 4: Reference them from France's config**

In `app/lib/countries.ts`, add the import at the top with the other imports:

```ts
import {
  FR_PUBLIC_CONTRACTS_QUERY,
  FR_WIKIDATA_QUERY,
  FR_WIKIDATA_PEOPLE_QUERY,
} from "~/lib/detail-queries/fr";
```

Then in France's `detail:` block — the one containing `industriesQuery` and
`addressQuery` — add three entries beside them:

```ts
      publicContractsQuery: FR_PUBLIC_CONTRACTS_QUERY,
      wikidataQuery: FR_WIKIDATA_QUERY,
      wikidataPeopleQuery: FR_WIKIDATA_PEOPLE_QUERY,
```

- [ ] **Step 5: Run the tests to green**

Run: `npx vitest run tests/fr-detail.queries.test.ts`
Expected: PASS, 8 tests.

- [ ] **Step 6: Run the full gate**

Run: `npm run typecheck && npx vitest run`
Expected: typecheck clean; all tests pass. France now also joins the
parameterised run in `tests/public-contracts.queries.test.ts` automatically,
because its `withContracts` filter is derived from which countries declare the
query.

- [ ] **Step 7: Commit**

```bash
git add app/lib/detail-queries/fr.ts app/lib/countries.ts tests/fr-detail.queries.test.ts
git commit -m "feat(backoffice): France shows its contracts and Wikidata entry"
```

---

### Task 2: Load France's financial metrics

**Files:**
- Modify: `app/lib/detail-queries/fr.ts`
- Modify: `app/lib/countries.ts`
- Modify: `app/lib/queries.server.ts`
- Modify: `tests/fr-detail.queries.test.ts`

**Interfaces:**
- Consumes: `FR_PUBLIC_CONTRACTS_QUERY` etc. from Task 1
- Produces:
  - `FR_FINANCIAL_METRICS_QUERY` from `~/lib/detail-queries/fr`
  - `financialMetricsQuery?: string` on `CountryDetailConfig`
  - `export interface FrFinancialRow` in `~/lib/queries.server` with the exact fields listed in Step 3
  - `frFinancials: FrFinancialRow[]` on `CompanyDetail`

- [ ] **Step 1: Write the failing test**

Append to `tests/fr-detail.queries.test.ts`:

```ts
describe("France financial metrics", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.financialMetricsQuery).toBeTruthy();
  });

  it("returns nothing for an id that cannot exist", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.financialMetricsQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("returns one row per fiscal year, newest first", async () => {
    // 531615169 files under more than one balance type in the same year --
    // 41,055 (siren, fiscal_year) pairs do. Two rows for one year would put
    // the year in the table twice, and it is invisible until someone opens
    // exactly such a company.
    const rows = await chQuery<{ fiscal_year: string; balance_type: string }>(
      getCountry("fr")!.detail!.financialMetricsQuery!,
      { id: "531615169" },
    );
    expect(rows.length).toBeGreaterThan(1);
    expect(new Set(rows.map((r) => r.fiscal_year)).size).toBe(rows.length);
    const years = rows.map((r) => Number(r.fiscal_year));
    expect(years).toEqual([...years].sort((a, b) => b - a));
  });

  it("carries the ratio suite and the confidentiality status", async () => {
    const rows = await chQuery<{
      currency: string;
      confidentiality: string;
      revenue_original: number | null;
      ebitda_margin_percent: number | null;
      customer_payment_days: number | null;
    }>(getCountry("fr")!.detail!.financialMetricsQuery!, { id: "055800296" });
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].currency).toBe("EUR");
    expect(rows[0].confidentiality).not.toBe("");
    expect(rows.some((r) => r.ebitda_margin_percent != null)).toBe(true);
    expect(rows.some((r) => r.customer_payment_days != null)).toBe(true);
  });
});
```

- [ ] **Step 2: Run it and watch it fail**

Run: `npx vitest run tests/fr-detail.queries.test.ts -t "financial metrics"`
Expected: FAIL — `expect(undefined).toBeTruthy()`.

- [ ] **Step 3: Add the query**

Append to `app/lib/detail-queries/fr.ts`:

```ts
/**
 * Per-year filed accounts, with the ratio suite INPI publishes.
 *
 * ONE ROW PER FISCAL YEAR. 41,055 (siren, fiscal_year) pairs carry more than
 * one filing because a company can file under more than one balance type --
 * C complete (4,924,259 rows), S simplified (1,586,394), K consolidated
 * (31,579). LIMIT 1 BY takes one, ordered by an explicit priority: the
 * entity's own complete accounts first, then its simplified ones, and the
 * consolidated group last, because this is the entity's page and not the
 * group's.
 *
 * NOT argMin over the balance type. argMin on a tied key picks arbitrarily
 * between runs -- the defect that made Swedish contract counts flicker
 * between 301 and 299 from identical data.
 *
 * No equity and no total assets: fr_company_financials_latest carries those
 * columns for every country but France fills neither, NULL across all
 * 1,586,046 rows. Joining it would add two permanently empty columns.
 *
 * Currency is EUR for all 6,542,232 rows, so the *_usd twins are conversions
 * rather than a second reporting currency.
 */
export const FR_FINANCIAL_METRICS_QUERY = `SELECT
  toString(m.fiscal_year) AS fiscal_year,
  m.balance_type_code AS balance_type,
  m.confidentiality_status AS confidentiality,
  m.currency AS currency,
  toFloat64(m.revenue_amount_original) AS revenue_original,
  toFloat64(m.revenue_amount_usd) AS revenue_usd,
  toFloat64(m.gross_margin_amount_original) AS gross_margin_original,
  toFloat64(m.ebitda_amount_original) AS ebitda_original,
  toFloat64(m.ebit_amount_original) AS ebit_original,
  toFloat64(m.net_income_amount_original) AS net_income_original,
  toFloat64(m.net_income_amount_usd) AS net_income_usd,
  toFloat64(m.ebitda_margin_percent) AS ebitda_margin_percent,
  toFloat64(m.debt_ratio_percent) AS debt_ratio_percent,
  toFloat64(m.financial_autonomy_percent) AS financial_autonomy_percent,
  toFloat64(m.liquidity_ratio_percent) AS liquidity_ratio_percent,
  toFloat64(m.interest_coverage_percent) AS interest_coverage_percent,
  toFloat64(m.customer_payment_days) AS customer_payment_days,
  toFloat64(m.supplier_payment_days) AS supplier_payment_days,
  toFloat64(m.inventory_turnover_days) AS inventory_turnover_days
FROM fr_financial_metrics AS m
WHERE m.siren = {id:String}
ORDER BY m.fiscal_year DESC,
  multiIf(m.balance_type_code = 'C', 0, m.balance_type_code = 'S', 1, 2)
LIMIT 1 BY m.fiscal_year
LIMIT 15`;
```

- [ ] **Step 4: Add the config key**

In `app/lib/countries.ts`, inside `CountryDetailConfig`, directly after the
`financialsQuery` entry:

```ts
  /**
   * {id:String} → per-year financial metrics for registers that publish more
   * than the five canonical figures (see FrFinancialRow in queries.server).
   * Rendered by a country-specific section, NOT the generic FinancialsSection
   * -- a country declaring this one usually declares no financialsQuery.
   */
  financialMetricsQuery?: string;
```

Add the import beside the Task 1 imports:

```ts
import { FR_FINANCIAL_METRICS_QUERY } from "~/lib/detail-queries/fr";
```

and in France's `detail:` block:

```ts
      financialMetricsQuery: FR_FINANCIAL_METRICS_QUERY,
```

France declares **no** `financialsQuery`, so the generic `FinancialsSection`
stays empty for it and the route's existing three-way financials conditional is
untouched.

- [ ] **Step 5: Add the row type and the loader**

In `app/lib/queries.server.ts`, add the type beside the other row interfaces
(directly after `PublicContractRow` is a good home):

```ts
/**
 * One fiscal year of a French filing, with the ratio suite INPI publishes.
 *
 * Distinct from FinancialYearRow: France carries gross margin, EBITDA, EBIT
 * and fourteen ratio and working-capital-day columns that the canonical shape
 * has no room for, and carries neither equity nor total assets, which it has.
 *
 * A null figure means WITHHELD, not zero -- 23.5% of French filings are
 * partially confidential and may legally omit lines.
 */
export interface FrFinancialRow {
  fiscal_year: string;
  /** 'C' complete, 'S' simplified, 'K' consolidated. */
  balance_type: string;
  /** 'Public' | 'Partiellement confidentiel' | 'Partiellement confidentiel (RAPCAC)' | 'Publication simplifiee'. */
  confidentiality: string;
  currency: string;
  revenue_original: number | null;
  revenue_usd: number | null;
  gross_margin_original: number | null;
  ebitda_original: number | null;
  ebit_original: number | null;
  net_income_original: number | null;
  net_income_usd: number | null;
  ebitda_margin_percent: number | null;
  debt_ratio_percent: number | null;
  financial_autonomy_percent: number | null;
  liquidity_ratio_percent: number | null;
  interest_coverage_percent: number | null;
  customer_payment_days: number | null;
  supplier_payment_days: number | null;
  inventory_turnover_days: number | null;
}
```

Add the field to `CompanyDetail`, after `financials`:

```ts
  /** France's extended per-year metrics; empty for every other country. */
  frFinancials: FrFinancialRow[];
```

Add the promise beside the other optional ones (next to `taxRecordsPromise`):

```ts
  const frFinancialsPromise = country.detail?.financialMetricsQuery
    ? chQuery<FrFinancialRow>(country.detail.financialMetricsQuery, { id })
    : Promise.resolve([]);
```

Add the no-op guard with the others:

```ts
  frFinancialsPromise.catch(() => {});
```

Add it to the destructured `Promise.all`. The existing line ends
`… wikidataPeople, esefFilings] = await Promise.all([`; append one name to the
pattern and one promise to the argument list, in the same position:

```ts
  const [records, [financials, contacts, domains], statements, industries, addresses, taxRecords, publicContracts, secondaryNames, officers, auditRows, gleifRelationships, gleifEntityRows, wikidataRows, wikidataPeople, esefFilings, frFinancials] = await Promise.all([
```

and as the last entry of the array passed to `Promise.all`, after
`esefFilingsPromise`:

```ts
    frFinancialsPromise,
```

Appending rather than inserting keeps every existing pair aligned — a name and
a promise that drift out of step here type-check fine and return another
section's rows.

Then add to the returned object, after `financials`:

```ts
    frFinancials,
```

- [ ] **Step 6: Run the tests to green**

Run: `npx vitest run tests/fr-detail.queries.test.ts`
Expected: PASS, 12 tests.

- [ ] **Step 7: Run the full gate**

Run: `npm run typecheck && npx vitest run`
Expected: typecheck clean; all tests pass.

- [ ] **Step 8: Commit**

```bash
git add app/lib/detail-queries/fr.ts app/lib/countries.ts app/lib/queries.server.ts tests/fr-detail.queries.test.ts
git commit -m "feat(backoffice): load France's per-year metrics and ratios"
```

---

### Task 3: The France financials section

**Files:**
- Create: `app/components/detail/countries/fr-financials.tsx`
- Create: `app/components/detail/countries/fr-financials.test.ts`
- Modify: `app/routes/country-company-detail.tsx`

**Interfaces:**
- Consumes: `FrFinancialRow` and `CompanyDetail.frFinancials` from Task 2
- Produces, from `~/components/detail/countries/fr-financials`:
  - `FrFinancialsSection({ financials }: { financials: FrFinancialRow[] })`
  - `balanceLabel(code: string): string`
  - `isWithheld(row: FrFinancialRow): boolean`
  - `formatRatio(value: number | null, unit: "percent" | "days" | "ratio"): string`

- [ ] **Step 1: Write the failing test**

Create `app/components/detail/countries/fr-financials.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  balanceLabel,
  formatRatio,
  isWithheld,
} from "~/components/detail/countries/fr-financials";
import type { FrFinancialRow } from "~/lib/queries.server";

function row(over: Partial<FrFinancialRow> = {}): FrFinancialRow {
  return {
    fiscal_year: "2024",
    balance_type: "C",
    confidentiality: "Public",
    currency: "EUR",
    revenue_original: 121958,
    revenue_usd: 130714.58,
    gross_margin_original: 121958,
    ebitda_original: -21641,
    ebit_original: -21643,
    net_income_original: 1041528,
    net_income_usd: 1116309.71,
    ebitda_margin_percent: -17.745,
    debt_ratio_percent: 49.275,
    financial_autonomy_percent: 66.042,
    liquidity_ratio_percent: 478.205,
    interest_coverage_percent: -1122.212,
    customer_payment_days: 48.287,
    supplier_payment_days: 232.315,
    inventory_turnover_days: 0,
    ...over,
  };
}

describe("balanceLabel", () => {
  it("names the three filing bases", () => {
    expect(balanceLabel("C")).toBe("Complete");
    expect(balanceLabel("S")).toBe("Simplified");
    expect(balanceLabel("K")).toBe("Consolidated");
  });

  it("passes an unknown code through rather than inventing a name", () => {
    expect(balanceLabel("X")).toBe("X");
  });
});

describe("isWithheld", () => {
  it("is false for a public filing", () => {
    expect(isWithheld(row())).toBe(false);
  });

  it("is true for every non-public status", () => {
    // 23.5% of French filings are partially confidential and may legally omit
    // lines. A blank there means withheld, and the badge is what says so.
    expect(isWithheld(row({ confidentiality: "Partiellement confidentiel" }))).toBe(true);
    expect(
      isWithheld(row({ confidentiality: "Partiellement confidentiel (RAPCAC)" })),
    ).toBe(true);
    expect(isWithheld(row({ confidentiality: "Publication simplifiee" }))).toBe(true);
  });
});

describe("formatRatio", () => {
  // Values chosen to sit clear of a half-way point. 49.275 at one decimal is
  // decided by whether the double is a hair above or below .275, so asserting
  // on it tests the floating-point representation rather than the formatter.
  it("renders a percentage to one decimal", () => {
    expect(formatRatio(49.2, "percent")).toBe("49.2%");
  });

  it("rounds to one decimal", () => {
    expect(formatRatio(48.26, "days")).toBe("48.3 d");
  });

  it("renders days with a unit", () => {
    expect(formatRatio(48.3, "days")).toBe("48.3 d");
  });

  it("renders a bare ratio without a unit", () => {
    expect(formatRatio(1.42, "ratio")).toBe("1.42");
  });

  it("keeps a genuine zero visible", () => {
    // inventory_turnover_days is legitimately 0 for a service company. If this
    // renders as the withheld dash, the page claims the figure is missing.
    expect(formatRatio(0, "days")).toBe("0.0 d");
  });

  it("renders null as a dash, never as zero", () => {
    expect(formatRatio(null, "percent")).toBe("—");
    expect(formatRatio(null, "days")).toBe("—");
  });

  it("keeps negative figures signed", () => {
    expect(formatRatio(-17.7, "percent")).toBe("-17.7%");
  });
});
```

- [ ] **Step 2: Run it and watch it fail**

Run: `npx vitest run app/components/detail/countries/fr-financials.test.ts`
Expected: FAIL — cannot resolve `~/components/detail/countries/fr-financials`.

- [ ] **Step 3: Write the component**

Create `app/components/detail/countries/fr-financials.tsx`:

```tsx
import type { FrFinancialRow } from "~/lib/queries.server";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const rf = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});
const bf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

/** INPI's type de bilan. Unknown codes pass through rather than being renamed
 * into something that looks authoritative and is not. */
export function balanceLabel(code: string): string {
  if (code === "C") return "Complete";
  if (code === "S") return "Simplified";
  if (code === "K") return "Consolidated";
  return code;
}

/** Whether this filing may legally omit lines. 1,590,754 of 6,542,232 rows
 * are one of the three non-public statuses, so a blank figure on those is
 * WITHHELD rather than zero, and the badge is what tells a reader which. */
export function isWithheld(row: FrFinancialRow): boolean {
  return row.confidentiality !== "Public";
}

/** A null is a dash, never a zero -- and a genuine zero stays a zero. */
export function formatRatio(
  value: number | null,
  unit: "percent" | "days" | "ratio",
): string {
  if (value == null) return "—";
  if (unit === "percent") return `${rf.format(value)}%`;
  if (unit === "days") return `${rf.format(value)} d`;
  return bf.format(value);
}

function money(value: number | null) {
  return value == null ? (
    <span className="text-muted-foreground">—</span>
  ) : (
    nf.format(value)
  );
}

/** Original on top, USD beneath. Currency is EUR for every French row, so the
 * USD figure is a conversion rather than a second reporting currency. */
function MoneyPair({
  original,
  usd,
  currency,
}: {
  original: number | null;
  usd: number | null;
  currency: string;
}) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      <span>
        {money(original)}
        {original == null ? "" : ` ${currency}`}
      </span>
      <span className="text-muted-foreground text-xs">
        {usd == null ? "—" : `$${nf.format(usd)}`}
      </span>
    </div>
  );
}

const RATIOS: {
  key: keyof FrFinancialRow;
  label: string;
  unit: "percent" | "days" | "ratio";
}[] = [
  { key: "ebitda_margin_percent", label: "EBITDA margin", unit: "percent" },
  { key: "debt_ratio_percent", label: "Debt ratio", unit: "percent" },
  { key: "financial_autonomy_percent", label: "Financial autonomy", unit: "percent" },
  { key: "liquidity_ratio_percent", label: "Liquidity", unit: "percent" },
  { key: "interest_coverage_percent", label: "Interest coverage", unit: "percent" },
  { key: "customer_payment_days", label: "Customer payment", unit: "days" },
  { key: "supplier_payment_days", label: "Supplier payment", unit: "days" },
  { key: "inventory_turnover_days", label: "Inventory turnover", unit: "days" },
];

/**
 * France's filed accounts (INPI), with the ratio suite the register publishes.
 *
 * Its own section rather than the canonical FinancialsSection because France
 * carries gross margin, EBITDA, EBIT and fourteen ratio and working-capital
 * columns that the five-column shape has no room for -- and carries neither
 * equity nor total assets, which it has. Fill is essentially complete: revenue
 * and EBITDA 100%, financial autonomy 99.96%, debt ratio 99.93%.
 *
 * One row per fiscal year; where a company filed under two bases in one year
 * the query already picked one, and the basis is named on the row.
 */
export function FrFinancialsSection({
  financials,
}: {
  financials: FrFinancialRow[];
}) {
  if (financials.length === 0) return null;
  const anyWithheld = financials.some(isWithheld);

  return (
    <Card id="fr-financials">
      <CardHeader>
        <CardTitle className="text-base">Financials</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="overflow-x-auto">
          <Table className="min-w-[46rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead>Basis</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Gross margin</TableHead>
                <TableHead className="text-right">EBITDA</TableHead>
                <TableHead className="text-right">EBIT</TableHead>
                <TableHead className="text-right">Net income</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {financials.map((r) => (
                <TableRow key={r.fiscal_year}>
                  <TableCell className="tabular-nums whitespace-nowrap">
                    {r.fiscal_year}
                  </TableCell>
                  <TableCell className="whitespace-nowrap">
                    {balanceLabel(r.balance_type)}
                    {isWithheld(r) ? (
                      <span
                        className="text-muted-foreground ml-1 text-xs"
                        title={r.confidentiality}
                      >
                        · partly confidential
                      </span>
                    ) : null}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.revenue_original}
                      usd={r.revenue_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {money(r.gross_margin_original)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {money(r.ebitda_original)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {money(r.ebit_original)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.net_income_original}
                      usd={r.net_income_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        <div className="overflow-x-auto">
          <Table className="min-w-[46rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Ratio</TableHead>
                {financials.map((r) => (
                  <TableHead key={r.fiscal_year} className="text-right">
                    {r.fiscal_year}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {RATIOS.map((ratio) => (
                <TableRow key={ratio.key}>
                  <TableCell className="whitespace-nowrap">{ratio.label}</TableCell>
                  {financials.map((r) => (
                    <TableCell
                      key={r.fiscal_year}
                      className="text-right tabular-nums"
                    >
                      {formatRatio(r[ratio.key] as number | null, ratio.unit)}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        <p className="text-muted-foreground text-xs">
          Filed accounts from INPI. Equity and total assets are not published in
          this dataset.
          {anyWithheld
            ? " A company may legally restrict publication, so a blank figure on a partly confidential filing means withheld, not zero."
            : ""}
        </p>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 4: Run the tests to green**

Run: `npx vitest run app/components/detail/countries/fr-financials.test.ts`
Expected: PASS, 11 tests.

- [ ] **Step 5: Render it on the page**

In `app/routes/country-company-detail.tsx`, add the import beside the other
per-country section imports:

```ts
import { FrFinancialsSection } from "~/components/detail/countries/fr-financials";
```

and render it directly after the existing financials block — after the
`})()}` that closes the three-way conditional and before `<EsefSection …>`:

```tsx
      <FrFinancialsSection financials={detail.frFinancials} />
```

It returns `null` for every country but France, which declares the query.

- [ ] **Step 6: Run the full gate**

Run: `npm run typecheck && npx vitest run`
Expected: typecheck clean; all tests pass.

- [ ] **Step 7: Check it in the browser**

Run the dev server and open `/company/fr/055800296` (FNAC DARTY). Expected: a
Financials card with 8 fiscal years, a Basis column reading "Complete", a
ratio table whose EBITDA margin and payment-day rows carry values, and the
INPI footnote. Confirm no row shows `0` where the ratio table shows `—`.

- [ ] **Step 8: Commit**

```bash
git add app/components/detail/countries/fr-financials.tsx app/components/detail/countries/fr-financials.test.ts app/routes/country-company-detail.tsx
git commit -m "feat(backoffice): show France's filed accounts and ratio suite"
```

---

### Task 4: The contract summary header

**Files:**
- Modify: `app/lib/detail-queries/fr.ts`
- Modify: `app/lib/countries.ts`
- Modify: `app/lib/queries.server.ts`
- Modify: `app/components/detail/public-contracts-section.tsx`
- Modify: `app/routes/country-company-detail.tsx`
- Modify: `tests/fr-detail.queries.test.ts`

**Interfaces:**
- Consumes: everything from Tasks 1–3
- Produces:
  - `FR_CONTRACT_SUMMARY_QUERY` from `~/lib/detail-queries/fr`
  - `contractSummaryQuery?: string` on `CountryDetailConfig`
  - `export interface ContractSummaryRow` in `~/lib/queries.server`
  - `contractSummary: ContractSummaryRow | null` on `CompanyDetail`
  - `PublicContractsSection` gains `summary?: ContractSummaryRow | null`

- [ ] **Step 1: Write the failing test**

Append to `tests/fr-detail.queries.test.ts`:

```ts
describe("France contract summary", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.contractSummaryQuery).toBeTruthy();
  });

  it("returns nothing for a company with no awards", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.contractSummaryQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("summarises a company with awards", async () => {
    const rows = await chQuery<{
      award_count: number | string;
      total_value_usd: number | null;
      last_award_date: string;
      sources: string;
    }>(getCountry("fr")!.detail!.contractSummaryQuery!, { id: "055800296" });
    expect(rows.length).toBe(1);
    expect(Number(rows[0].award_count)).toBeGreaterThan(0);
    expect(rows[0].sources).toBe("france_decp_procurement");
    expect(rows[0].last_award_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    // France publishes no per-winner value, so the summary's USD total is NULL
    // for all 99,287 companies. The header must not print "0" for it.
    expect(rows[0].total_value_usd).toBeNull();
  });
});
```

- [ ] **Step 2: Run it and watch it fail**

Run: `npx vitest run tests/fr-detail.queries.test.ts -t "contract summary"`
Expected: FAIL — `expect(undefined).toBeTruthy()`.

- [ ] **Step 3: Add the query**

Append to `app/lib/detail-queries/fr.ts`:

```ts
/**
 * One row summarising a company's award history.
 *
 * total_value_usd is NULL for every French company -- public_award_value_usd
 * and public_award_valued_count are derived from the per-winner figure DECP
 * does not publish, so they are NULL and 0 across all 99,287 rows. It is
 * selected anyway because the header renders a value only when one exists, and
 * the other eight contract countries have the same table with figures in it.
 */
export const FR_CONTRACT_SUMMARY_QUERY = `SELECT
  toUInt32(public_award_count) AS award_count,
  toUInt32(public_award_valued_count) AS valued_count,
  toFloat64(public_award_value_usd) AS total_value_usd,
  coalesce(toString(public_award_last_date), '') AS last_award_date,
  arrayStringConcat(source_slugs, ', ') AS sources
FROM fr_government_contract_summary
WHERE company_id = {id:String}
LIMIT 1`;
```

- [ ] **Step 4: Add the config key**

In `app/lib/countries.ts`, inside `CountryDetailConfig`, directly after
`publicContractsQuery`:

```ts
  /**
   * {id:String} → ONE row from <cc>_government_contract_summary (see
   * ContractSummaryRow in queries.server), rendered as a header above the
   * contracts table. Optional: a country without it renders the table alone.
   */
  contractSummaryQuery?: string;
```

Extend the import and France's `detail:` block:

```ts
      contractSummaryQuery: FR_CONTRACT_SUMMARY_QUERY,
```

- [ ] **Step 5: Add the row type and the loader**

In `app/lib/queries.server.ts`, after `PublicContractRow`:

```ts
/**
 * A company's award history in one row, for the header above the contracts
 * table.
 *
 * total_value_usd and valued_count are null/0 wherever a register publishes no
 * per-winner figure -- which is every French company. The header shows a value
 * only when one exists: printing "total value: 0" would say the company won
 * nothing.
 */
export interface ContractSummaryRow {
  award_count: number | string;
  valued_count: number | string;
  total_value_usd: number | null;
  last_award_date: string;
  sources: string;
}
```

Add to `CompanyDetail`, after `publicContracts`:

```ts
  /** Award-history summary; null when the country declares no summary query
   * or the company has never won one. */
  contractSummary: ContractSummaryRow | null;
```

Add the promise beside `publicContractsPromise`:

```ts
  const contractSummaryPromise = country.detail?.contractSummaryQuery
    ? chQuery<ContractSummaryRow>(country.detail.contractSummaryQuery, { id })
    : Promise.resolve([]);
```

Add the guard:

```ts
  contractSummaryPromise.catch(() => {});
```

Append to the destructured `Promise.all` exactly as Task 2 did — one name at
the end of the pattern, one promise at the end of the array:

```ts
  const [records, [financials, contacts, domains], statements, industries, addresses, taxRecords, publicContracts, secondaryNames, officers, auditRows, gleifRelationships, gleifEntityRows, wikidataRows, wikidataPeople, esefFilings, frFinancials, contractSummaryRows] = await Promise.all([
```

```ts
    contractSummaryPromise,
```

Then return, after `publicContracts`:

```ts
    contractSummary: contractSummaryRows[0] ?? null,
```

- [ ] **Step 6: Add the header to the shared section**

In `app/components/detail/public-contracts-section.tsx`, import the type:

```ts
import type { ContractSummaryRow } from "~/lib/queries.server";
```

Change the signature and add the header. Replace:

```tsx
export function PublicContractsSection({
  contracts,
}: {
  contracts: PublicContractRow[];
}) {
  if (contracts.length === 0) return null;
```

with:

```tsx
const summaryNf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

export function PublicContractsSection({
  contracts,
  summary = null,
}: {
  contracts: PublicContractRow[];
  /** Award history in one line. Optional: a country that declares no
   * contractSummaryQuery renders exactly what it rendered before. */
  summary?: ContractSummaryRow | null;
}) {
  if (contracts.length === 0) return null;
```

Then, as the first child of `<CardContent className="space-y-4">`, add:

```tsx
        {summary ? (
          <div className="text-muted-foreground flex flex-wrap gap-x-6 gap-y-1 text-sm">
            <span>
              <span className="text-foreground tabular-nums">
                {summaryNf.format(Number(summary.award_count))}
              </span>{" "}
              awards
            </span>
            {summary.last_award_date === "" ? null : (
              <span>
                latest{" "}
                <span className="text-foreground tabular-nums">
                  {summary.last_award_date}
                </span>
              </span>
            )}
            {/* Only where the register publishes a per-winner figure. France
                publishes none, so printing a zero here would say this company
                won nothing. */}
            {summary.total_value_usd == null ? null : (
              <span>
                <span className="text-foreground tabular-nums">
                  ${summaryNf.format(summary.total_value_usd)}
                </span>{" "}
                across {summaryNf.format(Number(summary.valued_count))} valued
              </span>
            )}
            {summary.sources === "" ? null : <span>{summary.sources}</span>}
          </div>
        ) : null}
```

Note the table shows only the first 100 awards while the header counts them
all, which is the point of having it: FNAC DARTY's header says how many exist,
the table shows the newest.

- [ ] **Step 7: Pass it from the route**

In `app/routes/country-company-detail.tsx`, change:

```tsx
      <PublicContractsSection contracts={detail.publicContracts} />
```

to:

```tsx
      <PublicContractsSection
        contracts={detail.publicContracts}
        summary={detail.contractSummary}
      />
```

- [ ] **Step 8: Run the full gate**

Run: `npm run typecheck && npx vitest run`
Expected: typecheck clean; all tests pass, including the 4 new ones.

- [ ] **Step 9: Check it in the browser**

Open `/company/fr/055800296`. Expected: the Government contracts card carries a
header line reading `7 awards · latest <date> · france_decp_procurement` with
**no** dollar figure, and the table beneath shows the notice totals labelled as
whole-procurement values.

- [ ] **Step 10: Commit**

```bash
git add app/lib/detail-queries/fr.ts app/lib/countries.ts app/lib/queries.server.ts app/components/detail/public-contracts-section.tsx app/routes/country-company-detail.tsx tests/fr-detail.queries.test.ts
git commit -m "feat(backoffice): summarise a company's award history above its contracts"
```

---

## Self-Review

Before finishing, confirm:

1. **Nothing renders a null as zero.** Grep the new components for `?? 0` and
   `|| 0`. The ratio table, the money cells and the summary's USD figure must
   each omit or dash a null. This is the one class of bug that makes the page
   state something false.
2. **`/company/fr/055800296` shows all three sections** — contracts with a
   header, financials with ratios, and the Wikidata card for Q47088340.
3. **One row per fiscal year for `531615169`.** Open it and count: 13 years,
   13 rows, no year twice.
4. **No country but France changed.** Open a Finnish and a Brazilian company
   and confirm their sections render as before — `PublicContractsSection`'s new
   prop defaults to `null`, and `frFinancials` is empty everywhere else.
5. **France still declares no `financialsQuery`**, so the generic
   `FinancialsSection` stays empty for it and the route's three-way conditional
   was never touched.
