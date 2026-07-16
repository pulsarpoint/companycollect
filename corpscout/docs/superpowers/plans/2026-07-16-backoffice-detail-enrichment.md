# Backoffice Detail Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the English translations that already exist (NO/LV), add a shared Industries section (all industry rows, canonical NACE English), and render financial amounts with their currency plus `≈`-marked derived USD values where the pipeline left them NULL.

**Architecture:** Three additions under the fidelity/specific-first rules: (1) a registry `detail.recordQuery` override — NO and LV join their `*_companies_translated` tables (100% coverage, verified) via LEFT JOIN so the record card gains `_en` fields **without dropping** the base-table columns the translated tables are missing (`last_submitted_accounts_year` for NO; the VZD address fields for LV); (2) `detail.industriesQuery` per country (9 countries) feeding one shared `IndustriesSection` — the industries-table pattern repeats across countries, so it qualifies as shared; (3) the NO statements component pre-formats amount values: originals get the row's currency code appended, USD fields with stored NULL but available `fx_rate_to_usd` show a derived `≈` value (`original × fx`, rounded to 2) — 199,431 of 312,733 NO statements have the rate but NULL USD amounts.

**Tech Stack:** existing stack only. No new dependencies.

## Global Constraints

- App: `corpscout/services/backoffice`. RR8 SSR, Base UI shadcn, `chQuery`; `{id:String}` binding; registry-only SQL; read-only.
- FIDELITY RULE stands: the record card must render every base-table column PLUS the joined `_en` fields — a test asserts both a translated-only field AND a base-only field are present.
- Derived USD values are visually distinguished with a leading `≈ ` and computed as `round(original * fx_rate_to_usd, 2)` ONLY when the stored USD is NULL, the original is non-NULL, and the rate is non-NULL. Stored USD values render without the marker. Currency code (e.g. `NOK`) is appended to every `*_amount_original` value; ` USD` to every `*_amount_usd` value.
- Industries section: shared component (pattern repeats across 9 countries); renders ALL industry rows for the company (primary first), each with mono code, canonical English label, original-language description when present and different from the label, and a `primary` badge. Renders null when empty. LV gets no `industriesQuery` (its NACE table is empty — known gap).
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths.

## Ground truth (verified live, 2026-07-16)

- `no_companies_translated`: 1,167,098 rows = 100% of `no_companies`; adds `articles_purpose_en`, `activity_text_en`, `legal_form_description_en`; **lacks `last_submitted_accounts_year`** → JOIN, never a table switch. Example (926056522): "Consulting and advisory activities, and other activities that naturally fall under this."
- `lv_companies_translated`: 485,380 rows = 100% of `lv_companies`; adds `activity_text_en` (its `legal_form_description_en` also exists in the base table); **lacks the `vzd_address_*`/`address_city_name`/`address_municipality_name`/lat/long columns** → JOIN.
- Industry sources for the section (same tables/joins as the facet work): uniform `{cc}_industries` for no/fi/ee/gb/fr/cz/sk (no/fi label-join on `substring(code,1,4)`; fi keys on `source_industry_code`), se via `se_industries`+`nace_categories`, br via headquarters establishment CNAE (primary only). Verified example: 926056522 → `73110 · "73.11 Activities of advertising agencies"`.
- NO USD gap: `countIf(fx_rate_to_usd IS NOT NULL AND operating_revenue_amount_usd IS NULL AND operating_revenue_amount_original IS NOT NULL)` = 199,431 / 312,733 — per-field partial (user's example: current-assets USD present, total-assets USD NULL in the same row).
- NO contacts coverage ~10% (113k/1.17M) — **pipeline gap, out of scope here**, logged for dagster.

---

### Task 1: `recordQuery` override — translated record cards for NO and LV

**Files:**
- Modify: `app/lib/countries.ts` (field + two queries)
- Modify: `app/lib/countries.test.ts`
- Modify: `app/lib/queries.server.ts` (use the override)
- Modify: `tests/queries.server.test.ts`

**Interfaces:**
- Produces: `CountryDetailConfig.recordQuery?: string` — `{id:String}` → ONE full row (base `SELECT c.*` + joined `_en` columns). `getCompanyDetail` uses it when present, else the existing default `SELECT * ... LIMIT 1`.

- [ ] **Step 1: Failing tests**

Append to `app/lib/countries.test.ts` (inside `describe("detail config")`):

```ts
it("no and lv join their translated tables in recordQuery", () => {
  for (const c of COUNTRIES) {
    if (c.code === "no") {
      expect(c.detail?.recordQuery).toContain("no_companies_translated");
      expect(c.detail?.recordQuery).toContain("{id:String}");
      expect(c.detail?.recordQuery).toContain("c.*");
    } else if (c.code === "lv") {
      expect(c.detail?.recordQuery).toContain("lv_companies_translated");
      expect(c.detail?.recordQuery).toContain("{id:String}");
      expect(c.detail?.recordQuery).toContain("c.*");
    } else {
      expect(c.detail?.recordQuery, c.code).toBeUndefined();
    }
  }
});
```

Append to `tests/queries.server.test.ts`:

```ts
describe("translated record cards", () => {
  it("norway record carries _en fields AND base-only fields (fidelity both ways)", async () => {
    const no = getCountry("no")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT org_number AS id FROM no_companies_translated
       WHERE articles_purpose_en IS NOT NULL AND articles_purpose_en != ''
       ORDER BY org_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(no, row.id);
    expect(detail!.record).toHaveProperty("articles_purpose_en");
    expect(detail!.record).toHaveProperty("activity_text_en");
    expect(detail!.record).toHaveProperty("legal_form_description_en");
    expect(detail!.record).toHaveProperty("last_submitted_accounts_year"); // base-only column survives
    expect(String(detail!.record.articles_purpose_en)).not.toBe("");
  });

  it("latvia record carries activity_text_en AND base-only address fields", async () => {
    const lv = getCountry("lv")!;
    const page = await searchCompanies(lv, { pageSize: 1 });
    const detail = await getCompanyDetail(lv, String(page.rows[0].id));
    expect(detail!.record).toHaveProperty("activity_text_en");
    expect(detail!.record).toHaveProperty("address_city_name"); // base-only column survives
  });
});
```

Run `pnpm test countries` and `pnpm test queries` — the new tests FAIL.

- [ ] **Step 2: Implement**

`app/lib/countries.ts` — add to `CountryDetailConfig`:

```ts
  /**
   * Optional override for the detail record fetch: {id:String} → ONE row.
   * MUST select the base table's full row (c.*) — used to join translated
   * columns without dropping base-only fields (fidelity rule).
   */
  recordQuery?: string;
```

Norway's `detail` block gains:

```ts
recordQuery: `SELECT c.*, t.articles_purpose_en, t.activity_text_en, t.legal_form_description_en
FROM no_companies AS c
LEFT JOIN no_companies_translated AS t ON t.org_number = c.org_number
WHERE c.org_number = {id:String}
LIMIT 1`,
```

Latvia's `detail` block gains:

```ts
recordQuery: `SELECT c.*, t.activity_text_en
FROM lv_companies AS c
LEFT JOIN lv_companies_translated AS t ON t.regcode = c.regcode
WHERE c.regcode = {id:String}
LIMIT 1`,
```

`app/lib/queries.server.ts` — in `getCompanyDetail`, the record fetch becomes:

```ts
  const recordPromise = chQuery<Record<string, unknown>>(
    country.detail?.recordQuery ??
      `SELECT * FROM ${country.companiesTable} WHERE ${country.idColumn} = {id:String} LIMIT 1`,
    { id },
  );
```

(the `.catch(() => {})` guard and everything else stays).

- [ ] **Step 3: Verify**

`pnpm test countries && pnpm test queries` → PASS; full `pnpm typecheck && pnpm test` → green. Dev server: `curl -s http://localhost:5183/no/companies/926056522 | grep -c 'Consulting and advisory'` ≥ 1 (the English purpose renders). Kill server.

- [ ] **Step 4: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "feat(backoffice): translated record cards for norway and latvia"
```

---

### Task 2: Shared Industries section (9 countries)

**Files:**
- Modify: `app/lib/countries.ts` (`industriesQuery` per country)
- Modify: `app/lib/countries.test.ts`
- Modify: `app/lib/queries.server.ts` (`industries` in `CompanyDetail`)
- Modify: `tests/queries.server.test.ts`
- Create: `app/components/detail/industries-section.tsx`
- Modify: `app/routes/country-company-detail.tsx`

**Interfaces:**
- Produces:
  - `CountryDetailConfig.industriesQuery?: string` — `{id:String}` → rows `industry_code, description_original, industry_label, is_primary` (label = canonical NACE English with the usual fallbacks). Declared for no/fi/ee/gb/fr/cz/sk/se/br; NOT lv.
  - `interface IndustryDetailRow { industry_code: string; description_original: string; industry_label: string; is_primary: 0 | 1 }` and `CompanyDetail.industries: IndustryDetailRow[]`
  - `<IndustriesSection industries />` — shared component, renders null when empty; rendered between the record card and the financials section.

- [ ] **Step 1: Failing tests**

`app/lib/countries.test.ts` (inside `describe("detail config")`):

```ts
it("every country except lv declares industriesQuery with the canonical aliases", () => {
  for (const c of COUNTRIES) {
    if (c.code === "lv") {
      expect(c.detail?.industriesQuery).toBeUndefined();
      continue;
    }
    for (const alias of ["AS industry_code", "AS description_original", "AS industry_label", "AS is_primary"]) {
      expect(c.detail?.industriesQuery, `${c.code}: ${alias}`).toContain(alias);
    }
    expect(c.detail?.industriesQuery, c.code).toContain("{id:String}");
  }
});
```

`tests/queries.server.test.ts`:

```ts
describe("industries section", () => {
  it("estonia returns all industry rows with english labels, primary first", async () => {
    const [row] = await chQuery<{ id: string }>(
      `SELECT reg_code AS id FROM ee_industries
       WHERE is_primary = 1 AND reg_code IN (SELECT reg_code FROM ee_companies)
       ORDER BY reg_code LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.industries.length).toBeGreaterThan(0);
    expect(detail!.industries[0].is_primary).toBe(1);
    expect(detail!.industries[0].industry_label).toBeTruthy();
  });

  it("latvia returns an empty industries array", async () => {
    const lv = getCountry("lv")!;
    const page = await searchCompanies(lv, { pageSize: 1 });
    const detail = await getCompanyDetail(lv, String(page.rows[0].id));
    expect(detail!.industries).toEqual([]);
  });
});

it.each(
  COUNTRIES.filter((c) => c.detail?.industriesQuery).map((c) => [c.code, c] as const),
)(
  "%s: industriesQuery SQL is valid against live schema",
  async (_code, country) => {
    const rows = await chQuery(country.detail!.industriesQuery!, { id: "0" });
    expect(Array.isArray(rows)).toBe(true);
  },
  60_000,
);
```

(the `it.each` goes inside the existing all-countries describe block; imports already present). Run — FAIL.

- [ ] **Step 2: Registry SQL**

Add `industriesQuery` to `CountryDetailConfig`:

```ts
  /** {id:String} → all industry rows: industry_code, description_original, industry_label (canonical NACE English), is_primary. */
  industriesQuery?: string;
```

Uniform direct-join countries — ee/gb/fr/cz/sk (Estonia shown; substitute table + key: gb→`gb_industries`/`company_number`, fr→`fr_industries`/`siren`, cz→`cz_industries`/`ico`, sk→`sk_industries`/`ico`):

```ts
industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM ee_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.reg_code = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
```

Norway (5-digit → substring join):

```ts
industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM no_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = substring(i.nace_normalized_code, 1, 4) AND n.is_current = 1
WHERE i.org_number = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
```

Finland (keys on `source_industry_code`, substring join):

```ts
industriesQuery: `SELECT coalesce(i.source_industry_code, '') AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.source_industry_code, '') AS industry_label,
  i.is_primary AS is_primary
FROM fi_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.business_id = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
```

Sweden (no source description; NOTE: `se_industries` keys on `company_id` while the page `:id` is `registration_number`, hence the subselect):

```ts
industriesQuery: `SELECT i.nace_rev2_class_code AS industry_code,
  '' AS description_original,
  coalesce(nullIf(n.description_en, ''), i.nace_rev2_class_code) AS industry_label,
  i.is_primary AS is_primary
FROM se_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.company_id IN (SELECT company_id FROM se_companies WHERE registration_number = {id:String})
ORDER BY i.is_primary DESC, i.sequence
LIMIT 100`,
```

Brazil (headquarters primary CNAE only):

```ts
industriesQuery: `SELECT e.primary_cnae_code AS industry_code,
  '' AS description_original,
  coalesce(nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label,
  1 AS is_primary
FROM br_establishments AS e
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.cnpj_basico = {id:String} AND e.is_headquarters = 1 AND e.primary_cnae_code != ''
LIMIT 100`,
```

Latvia: nothing.

- [ ] **Step 3: Query layer + component + route**

`app/lib/queries.server.ts`:

```ts
export interface IndustryDetailRow {
  industry_code: string;
  description_original: string;
  industry_label: string;
  is_primary: 0 | 1;
}
```

`CompanyDetail` gains `industries: IndustryDetailRow[];`; add `industriesPromise` to the parallel batch (same `.catch(() => {})` guard pattern), include in the final `Promise.all` and the return.

`app/components/detail/industries-section.tsx`:

```tsx
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import type { IndustryDetailRow } from "~/lib/queries.server";

export function IndustriesSection({ industries }: { industries: IndustryDetailRow[] }) {
  if (industries.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Industries</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-2">
          {industries.map((row, i) => (
            <li key={`${row.industry_code}-${i}`} className="flex flex-wrap items-baseline gap-2 text-sm">
              <span className="text-muted-foreground font-mono text-xs">{row.industry_code}</span>
              <span>{row.industry_label}</span>
              {row.is_primary ? <Badge>primary</Badge> : null}
              {row.description_original && row.description_original !== row.industry_label ? (
                <span className="text-muted-foreground text-xs">{row.description_original}</span>
              ) : null}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
```

Route: render `<IndustriesSection industries={detail.industries} />` directly after `<CompanyRecordSection ... />`.

- [ ] **Step 4: Verify**

All tests green (`pnpm typecheck && pnpm test`). Dev server: `curl -s http://localhost:5183/no/companies/926056522 | grep -c 'Activities of advertising agencies'` ≥ 1; an EE company with multiple industries lists them primary-first. Kill server.

- [ ] **Step 5: Commit**

```bash
git add app/lib/countries.ts app/lib/countries.test.ts app/lib/queries.server.ts tests/queries.server.test.ts app/components/detail/industries-section.tsx app/routes/country-company-detail.tsx
git commit -m "feat(backoffice): shared industries section with canonical nace labels"
```

---

### Task 3: Currency with every amount + derived USD in the NO statements

**Files:**
- Modify: `app/components/detail/countries/no-financials.tsx`
- Create: `app/components/detail/countries/no-financials.test.ts`

**Interfaces:**
- Produces: exported pure helper `buildAmountFields(row: Record<string, unknown>, keys: string[]): [string, string | null][]` — for each present key: `*_amount_original` → `"1,777,595 NOK"` (row's `currency`); `*_amount_usd` with stored value → `"423.25 USD"`; `*_amount_usd` stored NULL but original + `fx_rate_to_usd` present → `"≈ 176,364.16 USD"` (`round(original * fx, 2)`); non-amount keys → `formatFieldValue` passthrough. NULL when nothing to show (FieldGrid renders the dash).

- [ ] **Step 1: Failing unit tests**

`app/components/detail/countries/no-financials.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { buildAmountFields } from "~/components/detail/countries/no-financials";

const row = {
  currency: "NOK",
  fx_rate_to_usd: 0.0992,
  total_assets_amount_original: 38582,
  total_assets_amount_usd: null,
  current_assets_amount_original: 4266,
  current_assets_amount_usd: 423.25,
  equity_amount_original: null,
  equity_amount_usd: null,
};

describe("buildAmountFields", () => {
  it("appends the currency to originals", () => {
    const fields = new Map(buildAmountFields(row, ["total_assets_amount_original"]));
    expect(fields.get("total_assets_amount_original")).toBe("38,582 NOK");
  });

  it("keeps stored usd values unmarked", () => {
    const fields = new Map(buildAmountFields(row, ["current_assets_amount_usd"]));
    expect(fields.get("current_assets_amount_usd")).toBe("423.25 USD");
  });

  it("derives missing usd from the fx rate with the ≈ marker", () => {
    const fields = new Map(buildAmountFields(row, ["total_assets_amount_usd"]));
    expect(fields.get("total_assets_amount_usd")).toBe("≈ 3,827.33 USD");
  });

  it("returns null when original and usd are both absent", () => {
    const fields = new Map(buildAmountFields(row, ["equity_amount_usd"]));
    expect(fields.get("equity_amount_usd")).toBeNull();
  });

  it("does not derive when the fx rate is missing", () => {
    const noFx = { ...row, fx_rate_to_usd: null };
    const fields = new Map(buildAmountFields(noFx, ["total_assets_amount_usd"]));
    expect(fields.get("total_assets_amount_usd")).toBeNull();
  });
});
```

(38,582 × 0.0992 = 3,827.3344 → rounded 3,827.33.) Run `pnpm test no-financials` — FAIL (not exported).

- [ ] **Step 2: Implement**

In `app/components/detail/countries/no-financials.tsx` add (and use `Intl.NumberFormat("en-US", { maximumFractionDigits: 2 })` locally as `anf`):

```ts
const anf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

export function buildAmountFields(
  row: Record<string, unknown>,
  keys: string[],
): [string, string | null][] {
  return keys
    .filter((k) => k in row)
    .map((k) => {
      if (k.endsWith("_amount_original")) {
        const v = row[k];
        if (typeof v !== "number") return [k, null];
        return [k, `${anf.format(v)} ${String(row.currency ?? "")}`.trim()];
      }
      if (k.endsWith("_amount_usd")) {
        const stored = row[k];
        if (typeof stored === "number") return [k, `${anf.format(stored)} USD`];
        const original = row[k.replace("_amount_usd", "_amount_original")];
        const fx = row.fx_rate_to_usd;
        if (typeof original === "number" && typeof fx === "number") {
          return [k, `≈ ${anf.format(Math.round(original * fx * 100) / 100)} USD`];
        }
        return [k, null];
      }
      return [k, formatFieldValue(k, row[k])];
    });
}
```

Then replace the INCOME and BALANCE grid usages: `<FieldGrid fields={pick(row, INCOME_KEYS)} />` → `<FieldGrid fields={buildAmountFields(row, INCOME_KEYS)} />` (same for BALANCE_KEYS; META/lineage/rest grids stay as they are — `fx_rate_to_usd` etc. remain visible in Filing details). Import `formatFieldValue` if not already imported.

- [ ] **Step 3: Verify**

`pnpm test no-financials` → PASS; full `pnpm typecheck && pnpm test` → green. Dev server on `/no/companies/926056522`: amounts render as `1,777,595 NOK`; USD cells show `≈ ...` values instead of dashes wherever the fx rate exists. Kill server.

- [ ] **Step 4: Commit**

```bash
git add app/components/detail/countries/no-financials.tsx app/components/detail/countries/no-financials.test.ts
git commit -m "feat(backoffice): currency-labeled amounts and derived usd in norway statements"
```

---

### Task 4: Gate + README + gap log

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README**

Append to the fidelity paragraph under `### Company detail`:

```markdown
NO/LV record cards join their `*_companies_translated` tables (LEFT JOIN —
never a table switch; the translated tables are missing base columns).
Industries render as a shared section (all rows, canonical NACE English).
Norway statement amounts carry their currency code; USD values shown with a
leading `≈` are derived in the UI as `original × fx_rate_to_usd` where the
pipeline left the stored USD NULL.
```

- [ ] **Step 2: Full gate**

`pnpm typecheck && pnpm test && pnpm build`; `pnpm start`, then:

```bash
curl -s 'http://localhost:3000/no/companies/926056522' | grep -c 'Consulting and advisory'   # >= 1
curl -s 'http://localhost:3000/no/companies/926056522' | grep -c 'NOK'                        # >= 1
curl -s 'http://localhost:3000/no/companies/926056522' | grep -c 'Activities of advertising'  # >= 1
```

Kill; port free.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(backoffice): document translated records, industries and derived usd"
```

---

## Out of scope (logged)

- **NO contacts pipeline gap** (canonical contacts ≈10% of companies; BRREG source is richer) — dagster work, added to the cumulative gap list.
- NO statements USD backfill upstream (the `≈` derivation is a UI bridge, not the fix).
- Translated tables for other countries (only NO/LV exist today).
- FI/EE/… country-specific financials depth passes; detail loader diet (dual SELECTs, NO's unused canonical financialsQuery).
