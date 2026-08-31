# Company ESEF Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every admin SE company a dedicated **ESEF tab** showing everything extracted from its ESEF filings — filings, company information at full fidelity, people, business items, contact candidates, group relationships — plus a per-filing **Report notes** reader for the narrative disclosure blocks. The Financial tab stays as-is (it already shows only the financial subset).

**Architecture:** New tab `esef` in the admin SE company area, backed by a dedicated server lib that queries the `esef_document_*` ClickHouse tables directly by `company_id` (full fidelity — independent of what the curated serving tables carry). Narrative notes are heavy (~10 MB/filing), so they live on a separate route `financials/esef/:documentId/notes` under the existing public company layout, linked from both the ESEF tab and the facts reader. Owner direction (2026-08-31): "we should have esef page where we show all esef informations… and on financial page we can show subset related to financial data."

**Tech Stack:** TypeScript, React Router v7 (file routes), shadcn/ui, @clickhouse/client, vitest 4 (SSR string tests via `renderToStaticMarkup`).

**Spec:** This header + the 2026-08-31 investigation of company 5020077862 (Svenska Handelsbanken): 4 filings, 541 facts (FY2023), 144 narrative blocks, 6 people observations, 44 business items, 6 contact candidates, 2 company-information rows.

## Global Constraints

- Run tests: `cd corpscout/services/backoffice && npx vitest run <file>`; `npm run typecheck` before every commit.
- Data tables and shapes (verified live 2026-08-31): `esef_document_people(company_id, country_code, fiscal_year, name, role, role_category, organization, status, confidence, candidate_uid, …)`, `esef_document_business_items(company_id, country_code, fiscal_year, item_kind, name, geography_type, confidence, candidate_uid, …)`, `esef_document_group_relationships(company_id, country_code, fiscal_year, related_company_name, relationship_type, ownership_percentage, jurisdiction, …)`, `esef_document_contact_candidates(company_id, country_iso2, fiscal_year, candidate_kind, normalized_value, registrable_domain, …)` (**note: `country_iso2`, not `country_code`**), `esef_document_company_information(company_id, country_iso2, fiscal_year, extraction_status, company_description, description_language, description_confidence, products_and_services_json, customer_markets_json, operating_geographies_json, business_segments_json, material_group_relationships_json, …)`, `esef_filings(lei, fxo_id, entity_name, period_end, error_count, warning_count, viewer_url, source_url, package_url, …)`, `esef_disclosures(source_document_id, source_fact_id, disclosure_kind, concept_qname, concept_local_name, language, blocks_json, plain_text, printed_page_number, anchor_visual_order, block_count, table_count, original_character_count, …)`.
- Filings map to a company through `corpscout.company_identifier` on `issuer_scheme = 'lei' AND issuer_id = upperUTF8(trimBoth(f.lei)) AND country_code = {country} AND company_id = {id}` (pattern from `app/lib/esef-financial-reports.server.ts:76-86`).
- Test conventions: exported SQL consts pinned by string assertions; ClickHouse mocked via `vi.mock("~/lib/clickhouse.server", …)` (see `tests/se-company-info.server.test.ts:1-21`).
- Conventional Commits; stage only files this plan touches.

## File Structure

- Modify: `app/lib/se-company-tabs.ts:10-20` (add tab)
- Modify: `app/routes.ts:109-144` (two new routes)
- Create: `app/lib/se-company-esef.server.ts` (SQL + loader for the tab)
- Create: `app/routes/admin-se-company-esef.tsx` (the tab page)
- Create: `app/lib/esef-report-notes.server.ts` (SQL + loader for notes)
- Create: `app/routes/company-esef-report-notes.tsx` (notes reader)
- Modify: `app/routes/company-esef-financial-report.tsx` (link to notes)
- Create: `tests/se-company-esef.server.test.ts`, `tests/admin-se-company-esef.test.tsx`, `tests/esef-report-notes.server.test.ts`

---

### Task 1: Server lib for the ESEF tab

**Files:**
- Create: `app/lib/se-company-esef.server.ts`
- Create: `tests/se-company-esef.server.test.ts`

**Interfaces:**
- Produces: `loadSeCompanyEsef(companyId: string): Promise<SeCompanyEsefDetail | null>` and the exported SQL consts below. `SeCompanyEsefDetail` is `{ filings: EsefTabFiling[]; information: EsefTabInformation[]; people: EsefTabPerson[]; businessItems: EsefTabBusinessItem[]; contacts: EsefTabContact[]; relationships: EsefTabRelationship[] }`; returns `null` when `filings` AND `information` are both empty (company has no ESEF footprint → route 404s).

- [ ] **Step 1: Write the failing test** (`tests/se-company-esef.server.test.ts`):

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  ESEF_TAB_FILINGS_SQL,
  ESEF_TAB_INFORMATION_SQL,
  ESEF_TAB_PEOPLE_SQL,
  ESEF_TAB_BUSINESS_ITEMS_SQL,
  ESEF_TAB_CONTACTS_SQL,
  ESEF_TAB_RELATIONSHIPS_SQL,
  loadSeCompanyEsef,
} from "~/lib/se-company-esef.server";

beforeEach(() => clickhouse.query.mockReset());

describe("SQL contracts", () => {
  it("keys every document query by SE company id", () => {
    for (const sql of [
      ESEF_TAB_INFORMATION_SQL,
      ESEF_TAB_PEOPLE_SQL,
      ESEF_TAB_BUSINESS_ITEMS_SQL,
      ESEF_TAB_RELATIONSHIPS_SQL,
    ]) {
      expect(sql).toContain("company_id = {companyId:String}");
    }
    // contact candidates use country_iso2, people/items use country_code
    expect(ESEF_TAB_CONTACTS_SQL).toContain("country_iso2 = 'SE'");
    expect(ESEF_TAB_PEOPLE_SQL).toContain("country_code = 'SE'");
    expect(ESEF_TAB_FILINGS_SQL).toContain("issuer_scheme = 'lei'");
    expect(ESEF_TAB_FILINGS_SQL).toContain(
      "uniqExactIf(facts.fxo_id, facts.fxo_id != '') AS fact_filing_marker",
    );
  });
});

describe("loadSeCompanyEsef", () => {
  it("returns null when the company has no ESEF footprint", async () => {
    clickhouse.query.mockResolvedValue([]);
    expect(await loadSeCompanyEsef("5555555555")).toBeNull();
  });

  it("assembles all six sections", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          fxo_id: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
          entity_name: "Svenska Handelsbanken AB",
          period_end: "2023-12-31",
          fiscal_year: 2023,
          fact_count: 541,
          note_count: 144,
          error_count: 0,
          warning_count: 0,
          viewer_url: "https://example.test/viewer",
          source_url: "",
          package_url: "",
        },
      ])
      .mockResolvedValueOnce([
        {
          fiscal_year: 2023,
          extraction_status: "enriched",
          company_description: "Handelsbanken is a Swedish credit institution.",
          description_language: "en",
          description_confidence: 0.9,
          products_and_services_json: "[]",
          customer_markets_json: "[]",
          operating_geographies_json: "[]",
          business_segments_json: "[]",
          material_group_relationships_json: "[]",
        },
      ])
      .mockResolvedValueOnce([
        {
          fiscal_year: 2023,
          name: "Carina Åkerström",
          role: "Chief Executive Officer (verkställande direktör)",
          role_category: "chief_executive",
          organization: "",
          status: "current",
          confidence: 0.95,
        },
      ])
      .mockResolvedValueOnce([
        {
          fiscal_year: 2023,
          item_kind: "business_segment",
          name: "Capital Markets",
          geography_type: "",
          confidence: 0.95,
        },
      ])
      .mockResolvedValueOnce([
        {
          fiscal_year: 2023,
          candidate_kind: "email",
          normalized_value: "sustainability@handelsbanken.se",
          registrable_domain: "handelsbanken.se",
        },
      ])
      .mockResolvedValueOnce([]);

    const detail = await loadSeCompanyEsef("5020077862");
    expect(detail?.filings[0].noteCount).toBe(144);
    expect(detail?.people[0].name).toBe("Carina Åkerström");
    expect(detail?.businessItems[0].itemKind).toBe("business_segment");
    expect(detail?.contacts[0].normalizedValue).toBe(
      "sustainability@handelsbanken.se",
    );
    expect(detail?.relationships).toEqual([]);
  });
});
```

- [ ] **Step 2: Run it — FAIL** (`npx vitest run tests/se-company-esef.server.test.ts` — module not found).

- [ ] **Step 3: Implement `app/lib/se-company-esef.server.ts`:**

```ts
import { chQuery } from "~/lib/clickhouse.server";

// Filings for the company via the LEI identifier bridge, annotated with how
// many facts and narrative notes have been parsed for each filing. A filing
// with fact_count 0 is cataloged but not yet parsed (backfill in flight).
export const ESEF_TAB_FILINGS_SQL = `
SELECT
  f.fxo_id AS fxo_id,
  f.entity_name AS entity_name,
  toString(f.period_end) AS period_end,
  toYear(f.period_end) AS fiscal_year,
  count(facts.fact_id) AS fact_count,
  uniqExactIf(facts.fxo_id, facts.fxo_id != '') AS fact_filing_marker,
  countIf(facts.value_kind = 'text') AS note_count,
  f.error_count AS error_count,
  f.warning_count AS warning_count,
  f.viewer_url AS viewer_url,
  f.source_url AS source_url,
  f.package_url AS package_url
FROM corpscout.esef_filings AS f FINAL
INNER JOIN corpscout.company_identifier AS identifier
  ON identifier.issuer_scheme = 'lei'
 AND identifier.issuer_id = upperUTF8(trimBoth(f.lei))
LEFT JOIN corpscout.esef_facts AS facts
  ON facts.fxo_id = f.fxo_id
WHERE identifier.country_code = 'SE'
  AND identifier.company_id = {companyId:String}
GROUP BY f.fxo_id, f.entity_name, f.period_end, f.error_count,
  f.warning_count, f.viewer_url, f.source_url, f.package_url
ORDER BY f.period_end DESC`;

export const ESEF_TAB_INFORMATION_SQL = `
SELECT
  fiscal_year,
  extraction_status,
  company_description,
  toString(description_language) AS description_language,
  toFloat64(description_confidence) AS description_confidence,
  products_and_services_json,
  customer_markets_json,
  operating_geographies_json,
  business_segments_json,
  material_group_relationships_json
FROM corpscout.esef_document_company_information FINAL
WHERE country_iso2 = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, extracted_at DESC`;

export const ESEF_TAB_PEOPLE_SQL = `
SELECT
  fiscal_year, name, role, role_category, organization, status,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_people FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, name, role`;

export const ESEF_TAB_BUSINESS_ITEMS_SQL = `
SELECT
  fiscal_year, item_kind, name, geography_type,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_business_items FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, item_kind, name`;

export const ESEF_TAB_CONTACTS_SQL = `
SELECT
  fiscal_year, candidate_kind, normalized_value, registrable_domain
FROM corpscout.esef_document_contact_candidates
WHERE country_iso2 = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, candidate_kind, normalized_value`;

export const ESEF_TAB_RELATIONSHIPS_SQL = `
SELECT
  fiscal_year, related_company_name, relationship_type,
  toString(ownership_percentage) AS ownership_percentage, jurisdiction,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_group_relationships FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, related_company_name`;

export interface EsefTabFiling {
  fxoId: string;
  entityName: string;
  periodEnd: string;
  fiscalYear: number;
  factCount: number;
  noteCount: number;
  errorCount: number;
  warningCount: number;
  viewerUrl: string;
  sourceUrl: string;
  packageUrl: string;
}
export interface EsefTabInformation {
  fiscalYear: number;
  extractionStatus: string;
  companyDescription: string;
  descriptionLanguage: string;
  descriptionConfidence: number;
  productsAndServicesJson: string;
  customerMarketsJson: string;
  operatingGeographiesJson: string;
  businessSegmentsJson: string;
  materialGroupRelationshipsJson: string;
}
export interface EsefTabPerson {
  fiscalYear: number;
  name: string;
  role: string;
  roleCategory: string;
  organization: string;
  status: string;
  confidence: number;
}
export interface EsefTabBusinessItem {
  fiscalYear: number;
  itemKind: string;
  name: string;
  geographyType: string;
  confidence: number;
}
export interface EsefTabContact {
  fiscalYear: number;
  candidateKind: string;
  normalizedValue: string;
  registrableDomain: string;
}
export interface EsefTabRelationship {
  fiscalYear: number;
  relatedCompanyName: string;
  relationshipType: string;
  ownershipPercentage: string;
  jurisdiction: string;
  confidence: number;
}
export interface SeCompanyEsefDetail {
  filings: EsefTabFiling[];
  information: EsefTabInformation[];
  people: EsefTabPerson[];
  businessItems: EsefTabBusinessItem[];
  contacts: EsefTabContact[];
  relationships: EsefTabRelationship[];
}

export async function loadSeCompanyEsef(
  companyId: string,
): Promise<SeCompanyEsefDetail | null> {
  const params = { companyId };
  const [filings, information, people, items, contacts, relationships] =
    await Promise.all([
      chQuery<Record<string, never>>(ESEF_TAB_FILINGS_SQL, params),
      chQuery<Record<string, never>>(ESEF_TAB_INFORMATION_SQL, params),
      chQuery<Record<string, never>>(ESEF_TAB_PEOPLE_SQL, params),
      chQuery<Record<string, never>>(ESEF_TAB_BUSINESS_ITEMS_SQL, params),
      chQuery<Record<string, never>>(ESEF_TAB_CONTACTS_SQL, params),
      chQuery<Record<string, never>>(ESEF_TAB_RELATIONSHIPS_SQL, params),
    ]);
  if (filings.length === 0 && information.length === 0) return null;
  return {
    filings: filings.map((r: any) => ({
      fxoId: r.fxo_id,
      entityName: r.entity_name,
      periodEnd: r.period_end,
      fiscalYear: Number(r.fiscal_year),
      factCount: Number(r.fact_count),
      noteCount: Number(r.note_count),
      errorCount: Number(r.error_count),
      warningCount: Number(r.warning_count),
      viewerUrl: r.viewer_url,
      sourceUrl: r.source_url,
      packageUrl: r.package_url,
    })),
    information: information.map((r: any) => ({
      fiscalYear: Number(r.fiscal_year),
      extractionStatus: r.extraction_status,
      companyDescription: r.company_description,
      descriptionLanguage: r.description_language,
      descriptionConfidence: Number(r.description_confidence),
      productsAndServicesJson: r.products_and_services_json,
      customerMarketsJson: r.customer_markets_json,
      operatingGeographiesJson: r.operating_geographies_json,
      businessSegmentsJson: r.business_segments_json,
      materialGroupRelationshipsJson: r.material_group_relationships_json,
    })),
    people: people.map((r: any) => ({
      fiscalYear: Number(r.fiscal_year),
      name: r.name,
      role: r.role,
      roleCategory: r.role_category,
      organization: r.organization,
      status: r.status,
      confidence: Number(r.confidence),
    })),
    businessItems: items.map((r: any) => ({
      fiscalYear: Number(r.fiscal_year),
      itemKind: r.item_kind,
      name: r.name,
      geographyType: r.geography_type,
      confidence: Number(r.confidence),
    })),
    contacts: contacts.map((r: any) => ({
      fiscalYear: Number(r.fiscal_year),
      candidateKind: r.candidate_kind,
      normalizedValue: r.normalized_value,
      registrableDomain: r.registrable_domain,
    })),
    relationships: relationships.map((r: any) => ({
      fiscalYear: Number(r.fiscal_year),
      relatedCompanyName: r.related_company_name,
      relationshipType: r.relationship_type,
      ownershipPercentage: r.ownership_percentage,
      jurisdiction: r.jurisdiction,
      confidence: Number(r.confidence),
    })),
  };
}
```

Replace the `(r: any)` casts with typed row interfaces if `tsc` is configured to reject `any` (mirror `SeCompanyInfoArtifactQueryRow` style from `se-company-info.server.ts:78-84`).

- [ ] **Step 4: Run tests — PASS**, then `npm run typecheck`.

- [ ] **Step 5: Commit** — `git add app/lib/se-company-esef.server.ts tests/se-company-esef.server.test.ts && git commit -m "feat(esef): server loader for the admin SE company ESEF tab"`

### Task 2: Register the tab and route

**Files:**
- Modify: `app/lib/se-company-tabs.ts:10-20`
- Modify: `app/routes.ts:109-144`

**Interfaces:**
- Produces: tab value `"esef"`, route `/admin/se/company/:companyId/esef` → `routes/admin-se-company-esef.tsx`.

- [ ] **Step 1:** Add `{ value: "esef", label: "ESEF" },` to `SE_COMPANY_TABS` between `financial` and `people` (the ESEF page is the source-level sibling of Financial).

- [ ] **Step 2:** In `app/routes.ts`, inside the `route("se/company/:companyId", "routes/admin-se-company-layout.tsx", [...])` block (line 109), add after the `financial` route (line 113):

```ts
      route("esef", "routes/admin-se-company-esef.tsx"),
```

- [ ] **Step 3:** Create a minimal `app/routes/admin-se-company-esef.tsx` that compiles (full UI in Task 3):

```tsx
import type { Route } from "./+types/admin-se-company-esef";
import { loadSeCompanyEsef } from "~/lib/se-company-esef.server";

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadSeCompanyEsef(params.companyId);
  if (!detail) throw new Response("No ESEF data for company", { status: 404 });
  return detail;
}

export default function AdminSwedenCompanyEsef({
  loaderData,
}: Route.ComponentProps) {
  return <pre>{JSON.stringify(loaderData.filings, null, 2)}</pre>;
}
```

- [ ] **Step 4:** `npm run typecheck` (typegen creates `+types/admin-se-company-esef`). Check whether an existing test pins the tab list (`rg -ln "SE_COMPANY_TABS" tests/ app/`) and update it to include the new entry.

- [ ] **Step 5: Commit** — `git commit -m "feat(esef): register ESEF tab in the admin SE company area"` (staged: the three files).

### Task 3: The ESEF tab page

**Files:**
- Modify: `app/routes/admin-se-company-esef.tsx`
- Create: `tests/admin-se-company-esef.test.tsx`

**Interfaces:**
- Consumes: `SeCompanyEsefDetail` from Task 1; `parseJsonList` from `~/lib/se-company-info-payload` (`app/lib/se-company-info-payload.ts:194`); shadcn `Card`/`Badge`/`Table` components as used across `components/admin/*`.

- [ ] **Step 1: Write the failing SSR test** (convention: `tests/source-information-sections.test.tsx:112-125`):

```tsx
import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { SeCompanyEsefView } from "~/routes/admin-se-company-esef";

const DETAIL = {
  filings: [
    {
      fxoId: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
      entityName: "Svenska Handelsbanken AB",
      periodEnd: "2023-12-31",
      fiscalYear: 2023,
      factCount: 541,
      noteCount: 144,
      errorCount: 0,
      warningCount: 0,
      viewerUrl: "https://example.test/viewer",
      sourceUrl: "",
      packageUrl: "",
    },
    {
      fxoId: "NHBDILHZTYCNBV5UYZ31-2024-12-31-ESEF-SE-0",
      entityName: "Svenska Handelsbanken AB",
      periodEnd: "2024-12-31",
      fiscalYear: 2024,
      factCount: 0,
      noteCount: 0,
      errorCount: 0,
      warningCount: 0,
      viewerUrl: "",
      sourceUrl: "",
      packageUrl: "",
    },
  ],
  information: [
    {
      fiscalYear: 2023,
      extractionStatus: "enriched",
      companyDescription: "Handelsbanken is a Swedish credit institution.",
      descriptionLanguage: "en",
      descriptionConfidence: 0.9,
      productsAndServicesJson:
        '[{"name":"Financing (finansiering)","confidence":0.9}]',
      customerMarketsJson: '[{"name":"Corporate customers","confidence":0.9}]',
      operatingGeographiesJson: '[{"name":"Sweden","confidence":0.9}]',
      businessSegmentsJson: '[{"name":"Capital Markets","confidence":0.95}]',
      materialGroupRelationshipsJson: "[]",
    },
  ],
  people: [
    {
      fiscalYear: 2023,
      name: "Carina Åkerström",
      role: "Chief Executive Officer (verkställande direktör)",
      roleCategory: "chief_executive",
      organization: "",
      status: "current",
      confidence: 0.95,
    },
  ],
  businessItems: [
    {
      fiscalYear: 2023,
      itemKind: "customer_market",
      name: "Corporate customers",
      geographyType: "",
      confidence: 0.9,
    },
  ],
  contacts: [
    {
      fiscalYear: 2023,
      candidateKind: "email",
      normalizedValue: "sustainability@handelsbanken.se",
      registrableDomain: "handelsbanken.se",
    },
  ],
  relationships: [],
};

describe("SeCompanyEsefView", () => {
  it("renders every section with parsed-vs-pending filings", () => {
    const html = renderToStaticMarkup(
      <SeCompanyEsefView companyId="5020077862" detail={DETAIL} />,
    );
    expect(html).toContain("541");
    expect(html).toContain("Notes (144)");
    expect(html).toContain("Not parsed yet");
    expect(html).toContain("Handelsbanken is a Swedish credit institution.");
    expect(html).toContain("Customer markets");
    expect(html).toContain("Carina Åkerström");
    expect(html).toContain("sustainability@handelsbanken.se");
    expect(html).toContain(
      "/company/se/5020077862/financials/esef/NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );
  });
});
```

- [ ] **Step 2: Run — FAIL** (no `SeCompanyEsefView` export).

- [ ] **Step 3: Implement the page.** Export a pure view component (so the SSR test needs no router) and keep the route default thin:

```tsx
import { Link } from "react-router";
import type { Route } from "./+types/admin-se-company-esef";
import {
  loadSeCompanyEsef,
  type SeCompanyEsefDetail,
} from "~/lib/se-company-esef.server";
import { parseJsonList } from "~/lib/se-company-info-payload";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadSeCompanyEsef(params.companyId);
  if (!detail) throw new Response("No ESEF data for company", { status: 404 });
  return detail;
}

const JSON_LIST_SECTIONS: ReadonlyArray<
  [keyof SeCompanyEsefDetail["information"][number] & string, string]
> = [
  ["productsAndServicesJson", "Products & services"],
  ["businessSegmentsJson", "Business segments"],
  ["customerMarketsJson", "Customer markets"],
  ["operatingGeographiesJson", "Operating geographies"],
  ["materialGroupRelationshipsJson", "Group relationships"],
];

export function SeCompanyEsefView({
  companyId,
  detail,
}: {
  companyId: string;
  detail: SeCompanyEsefDetail;
}) {
  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Filings</CardTitle>
          <CardDescription>
            Every ESEF annual report we know for this company. A filing without
            parsed facts is cataloged but still waiting for the parse backfill.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {detail.filings.map((filing) => (
            <div
              key={filing.fxoId}
              className="flex flex-wrap items-center gap-2 border-b pb-2 last:border-b-0"
            >
              <Badge variant="outline">{filing.fiscalYear}</Badge>
              <span className="font-mono text-xs">{filing.fxoId}</span>
              {filing.factCount > 0 ? (
                <>
                  <span>{filing.factCount} facts</span>
                  <Link
                    className="underline"
                    to={`/company/se/${companyId}/financials/esef/${filing.fxoId}`}
                  >
                    Open facts
                  </Link>
                  <Link
                    className="underline"
                    to={`/company/se/${companyId}/financials/esef/${filing.fxoId}/notes`}
                  >
                    Notes ({filing.noteCount})
                  </Link>
                </>
              ) : (
                <Badge variant="secondary">Not parsed yet</Badge>
              )}
              {filing.viewerUrl ? (
                <a
                  className="underline"
                  href={filing.viewerUrl}
                  target="_blank"
                  rel="noreferrer"
                >
                  Viewer ↗
                </a>
              ) : null}
            </div>
          ))}
        </CardContent>
      </Card>

      {detail.information.map((info) => (
        <Card key={`${info.fiscalYear}-${info.extractionStatus}`}>
          <CardHeader>
            <CardTitle>
              Company information · {info.fiscalYear}{" "}
              <Badge variant="outline">{info.extractionStatus}</Badge>
            </CardTitle>
            <CardDescription>
              LLM-extracted from the annual report narrative (
              {info.descriptionLanguage}, confidence{" "}
              {info.descriptionConfidence}).
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <p className="max-w-[90ch]">{info.companyDescription}</p>
            <div className="grid gap-4 sm:grid-cols-2">
              {JSON_LIST_SECTIONS.map(([key, label]) => {
                const items = parseJsonList(String(info[key] ?? ""));
                if (!items || items.length === 0) return null;
                return (
                  <section key={key}>
                    <h3 className="font-medium">{label}</h3>
                    <ul className="list-disc pl-4">
                      {items.map((item) => (
                        <li key={item.text}>
                          {item.text}
                          {item.detail ? (
                            <span className="text-muted-foreground">
                              {" "}
                              · {item.detail}
                            </span>
                          ) : null}
                        </li>
                      ))}
                    </ul>
                  </section>
                );
              })}
            </div>
          </CardContent>
        </Card>
      ))}

      {detail.people.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>People</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.people.map((person) => (
                <li
                  key={`${person.fiscalYear}-${person.name}-${person.role}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span className="font-medium">{person.name}</span>
                  <span>{person.role}</span>
                  <Badge variant="outline">{person.roleCategory}</Badge>
                  <Badge variant="outline">fiscal {person.fiscalYear}</Badge>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}

      {detail.businessItems.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Business items</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-2">
            {["product_or_service", "business_segment", "customer_market", "operating_geography"]
              .filter((kind) =>
                detail.businessItems.some((item) => item.itemKind === kind),
              )
              .map((kind) => (
                <section key={kind}>
                  <h3 className="font-medium">
                    {
                      {
                        product_or_service: "Products and services",
                        business_segment: "Business segments",
                        customer_market: "Customer markets",
                        operating_geography: "Operating geographies",
                      }[kind]
                    }
                  </h3>
                  <ul className="list-disc pl-4">
                    {detail.businessItems
                      .filter((item) => item.itemKind === kind)
                      .map((item) => (
                        <li key={`${item.fiscalYear}-${item.name}`}>
                          {item.name}{" "}
                          <Badge variant="outline">
                            fiscal {item.fiscalYear}
                          </Badge>
                        </li>
                      ))}
                  </ul>
                </section>
              ))}
          </CardContent>
        </Card>
      ) : null}

      {detail.contacts.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Contact candidates</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.contacts.map((contact) => (
                <li
                  key={`${contact.fiscalYear}-${contact.candidateKind}-${contact.normalizedValue}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <Badge variant="outline">{contact.candidateKind}</Badge>
                  <span>{contact.normalizedValue}</span>
                  <Badge variant="outline">fiscal {contact.fiscalYear}</Badge>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}

      {detail.relationships.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Group relationships</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.relationships.map((rel) => (
                <li
                  key={`${rel.fiscalYear}-${rel.relatedCompanyName}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span className="font-medium">{rel.relatedCompanyName}</span>
                  <Badge variant="outline">{rel.relationshipType}</Badge>
                  {rel.ownershipPercentage ? (
                    <span>{rel.ownershipPercentage}%</span>
                  ) : null}
                  {rel.jurisdiction ? <span>{rel.jurisdiction}</span> : null}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

export default function AdminSwedenCompanyEsef({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyEsefView companyId={params.companyId} detail={loaderData} />
  );
}
```

Note: `parseJsonList` lives in a client-safe lib (`se-company-info-payload.ts` has no server imports) — verify with `rg -n "clickhouse|\.server" app/lib/se-company-info-payload.ts` (must be empty) before importing it into a component.

- [ ] **Step 4: Run** `npx vitest run tests/admin-se-company-esef.test.tsx` → PASS; `npm run typecheck` → clean.

- [ ] **Step 5: Commit** — `git commit -m "feat(esef): admin SE company ESEF tab showing filings, info, people, items, contacts"`.

### Task 4: Report notes route

**Files:**
- Create: `app/lib/esef-report-notes.server.ts`
- Create: `app/routes/company-esef-report-notes.tsx`
- Create: `tests/esef-report-notes.server.test.ts`
- Modify: `app/routes.ts:62-65` (nested notes route)
- Modify: `app/routes/company-esef-financial-report.tsx` (header link)

**Interfaces:**
- Produces: `getEsefReportNotes(country: string, companyId: string, documentId: string): Promise<EsefReportNotes | null>` where `EsefReportNotes = { summary: EsefFinancialReportSummary; notes: EsefReportNote[] }` and `EsefReportNote = { disclosureId: string; conceptQname: string; conceptLocalName: string; language: string; printedPageNumber: string; blockCount: number; tableCount: number; characterCount: number; disclosure: EsefDisclosureDocument }`.
- Consumes: `parsePersistedEsefDisclosure` (`app/lib/esef-disclosures.ts:351`), `EsefDisclosureReader` (`app/components/detail/esef-disclosure-reader.tsx:35`), and the summary loader shape from `app/lib/esef-financial-reports.server.ts` (reuse its exported `getEsefFinancialReport`? No — that pulls all facts. Extract/reuse only the summary query: import is not possible since `REPORT_SUMMARY_QUERY` is private; copy the summary query into the new lib and pin both with the same test string so drift is caught, or export it from `esef-financial-reports.server.ts` and import — **prefer exporting `REPORT_SUMMARY_QUERY` and reusing it**).

- [ ] **Step 1: Failing test** (`tests/esef-report-notes.server.test.ts`):

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  ESEF_REPORT_NOTES_SQL,
  getEsefReportNotes,
} from "~/lib/esef-report-notes.server";

beforeEach(() => clickhouse.query.mockReset());

describe("ESEF_REPORT_NOTES_SQL", () => {
  it("reads tagged_fact disclosures for one document in visual order", () => {
    expect(ESEF_REPORT_NOTES_SQL).toContain("FROM corpscout.esef_disclosures");
    expect(ESEF_REPORT_NOTES_SQL).toContain(
      "disclosure_kind = 'tagged_fact'",
    );
    expect(ESEF_REPORT_NOTES_SQL).toContain(
      "source_document_id = {documentId:String}",
    );
    expect(ESEF_REPORT_NOTES_SQL).toContain("ORDER BY anchor_visual_order");
  });
});

describe("getEsefReportNotes", () => {
  it("parses persisted blocks and falls back to plain text", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          lei: "NHBDILHZTYCNBV5UYZ31",
          fxo_id: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
          entity_name: "Svenska Handelsbanken AB",
          fiscal_year: 2023,
          period_end: "2023-12-31",
          currency: "SEK",
          mapped_fact_count: 9,
          source_fact_count: 292,
          filing_version: 0,
          viewer_url: "",
          source_url: "",
          package_url: "",
          error_count: 0,
          warning_count: 0,
          date_added: "2024-04-24",
        },
      ])
      .mockResolvedValueOnce([
        {
          disclosure_id: "c64d2fa8",
          concept_qname: "ifrs-full:DisclosureOfFinanceCostExplanatory",
          concept_local_name: "DisclosureOfFinanceCostExplanatory",
          language: "sv",
          printed_page_number: "112",
          block_count: 5,
          table_count: 2,
          original_character_count: 900,
          blocks_json: JSON.stringify([{ type: "heading", text: "Not 5" }]),
          plain_text: "Not 5",
        },
        {
          disclosure_id: "broken",
          concept_qname: "ifrs-full:DisclosureOfDebtSecuritiesExplanatory",
          concept_local_name: "DisclosureOfDebtSecuritiesExplanatory",
          language: "sv",
          printed_page_number: "",
          block_count: 0,
          table_count: 0,
          original_character_count: 10,
          blocks_json: "not json",
          plain_text: "fallback text",
        },
      ]);

    const notes = await getEsefReportNotes(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );
    expect(notes?.notes[0].disclosure.blocks).toEqual([
      { type: "heading", text: "Not 5" },
    ]);
    // invalid persisted JSON degrades to a single paragraph of plain text
    expect(notes?.notes[1].disclosure).toEqual({
      blocks: [{ type: "paragraph", text: "fallback text" }],
      plainText: "fallback text",
    });
  });
});
```

- [ ] **Step 2: Run — FAIL** (module not found).

- [ ] **Step 3: Implement `app/lib/esef-report-notes.server.ts`:**

```ts
import { chQuery } from "~/lib/clickhouse.server";
import {
  parsePersistedEsefDisclosure,
  type EsefDisclosureDocument,
} from "~/lib/esef-disclosures";
import {
  REPORT_SUMMARY_QUERY,
  type EsefSummaryRow,
} from "~/lib/esef-financial-reports.server";
import type { EsefFinancialReportSummary } from "~/lib/esef-financial-reports";

export const ESEF_REPORT_NOTES_SQL = `
SELECT
  toString(disclosure_id) AS disclosure_id,
  concept_qname,
  concept_local_name,
  toString(language) AS language,
  toString(printed_page_number) AS printed_page_number,
  toUInt32(block_count) AS block_count,
  toUInt32(table_count) AS table_count,
  toUInt32(original_character_count) AS original_character_count,
  blocks_json,
  plain_text
FROM corpscout.esef_disclosures
WHERE disclosure_kind = 'tagged_fact'
  AND source_document_id = {documentId:String}
ORDER BY anchor_visual_order, concept_qname`;

export interface EsefReportNote {
  disclosureId: string;
  conceptQname: string;
  conceptLocalName: string;
  language: string;
  printedPageNumber: string;
  blockCount: number;
  tableCount: number;
  characterCount: number;
  disclosure: EsefDisclosureDocument;
}

export interface EsefReportNotes {
  summary: EsefFinancialReportSummary;
  notes: EsefReportNote[];
}

export async function getEsefReportNotes(
  country: string,
  companyId: string,
  documentId: string,
): Promise<EsefReportNotes | null> {
  const summaries = await chQuery<EsefSummaryRow>(REPORT_SUMMARY_QUERY, {
    country: country.toUpperCase(),
    id: companyId,
    documentId,
  });
  if (summaries.length === 0) return null;
  const rows = await chQuery<{
    disclosure_id: string;
    concept_qname: string;
    concept_local_name: string;
    language: string;
    printed_page_number: string;
    block_count: number;
    table_count: number;
    original_character_count: number;
    blocks_json: string;
    plain_text: string;
  }>(ESEF_REPORT_NOTES_SQL, { documentId });
  return {
    summary: summaries[0] as unknown as EsefFinancialReportSummary,
    notes: rows.map((row) => ({
      disclosureId: row.disclosure_id,
      conceptQname: row.concept_qname,
      conceptLocalName: row.concept_local_name,
      language: row.language,
      printedPageNumber: row.printed_page_number,
      blockCount: Number(row.block_count),
      tableCount: Number(row.table_count),
      characterCount: Number(row.original_character_count),
      disclosure:
        parsePersistedEsefDisclosure(row.blocks_json, row.plain_text) ?? {
          blocks: [{ type: "paragraph", text: row.plain_text }],
          plainText: row.plain_text,
        },
    })),
  };
}
```

Prerequisites inside `esef-financial-reports.server.ts`: `export` the currently-private `REPORT_SUMMARY_QUERY` and its row type as `EsefSummaryRow`, and reuse its existing summary→`EsefFinancialReportSummary` mapper (`reportSummary` at `:322`) instead of the `as unknown as` cast — export `reportSummary` and call it.

- [ ] **Step 4: Route + link.** In `app/routes.ts` after line 65 add:

```ts
      route(
        "financials/esef/:documentId/notes",
        "routes/company-esef-report-notes.tsx",
      ),
```

Create `app/routes/company-esef-report-notes.tsx`: loader calls `getEsefReportNotes`, 404 on null; page renders a back link to the facts reader, an `<h2>Report notes · {summary.fiscalYear}</h2>`, and one bordered `<section>` per note with `<h3>{note.conceptLocalName}</h3>`, badges for language / `p. {printedPageNumber}` / `{tableCount} tables`, and `<EsefDisclosureReader disclosure={note.disclosure} />`. In `app/routes/company-esef-financial-report.tsx` header button row (`:91-151`) add a `<Link to="notes">Report notes</Link>` button styled like the existing Source/Open report buttons.

- [ ] **Step 5: Run** `npx vitest run tests/esef-report-notes.server.test.ts app/lib/esef-financial-reports.sql.test.ts && npm run typecheck` → PASS/clean.

- [ ] **Step 6: Commit** — `git commit -m "feat(esef): per-filing report-notes reader for narrative disclosures"`.

### Task 5: Live verification

- [ ] **Step 1:** `http://localhost:5183/admin/se/company/5020077862/esef` — expect: 4 filings (2023 with "541 facts / Notes (144)", 2021/2022/2024 "Not parsed yet"), company information card with description + 5 JSON sections, 3 people, business items in 4 groups, 6 contacts.
- [ ] **Step 2:** Follow "Notes (144)" — expect 144 sections, tables rendered, Swedish text.
- [ ] **Step 3:** Check the tab renders in the layout nav and breadcrumbs name it (both read `SE_COMPANY_TABS`).
- [ ] **Step 4:** Report findings; no commit.

## Self-Review

- Owner direction covered: dedicated ESEF page with ALL ESEF info (T1-T3), Financial tab untouched, notes get a real surface (T4). ✔
- `country_iso2` vs `country_code` split verified against live schemas and pinned in the T1 test. ✔
- Notes weight isolated on its own route; the tab page carries only counts. ✔
- Names consistent across tasks: `loadSeCompanyEsef` / `SeCompanyEsefDetail` / `SeCompanyEsefView` / `getEsefReportNotes` / `ESEF_REPORT_NOTES_SQL`. ✔
- Dependency: the notes route reads `esef_disclosures` — independent of the disclosures-join fix plan, but exporting `REPORT_SUMMARY_QUERY` touches the same file; execute after that plan to avoid conflicts. ✔
