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
  ESEF_TAB_DOMAINS_SQL,
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
    // The register-verified se_esef_* views already scope every row to
    // Sweden and expose company_id first, so the queries no longer carry
    // their own country predicate or FINAL.
    expect(ESEF_TAB_CONTACTS_SQL).toContain(
      "FROM corpscout.se_esef_document_contact_candidates",
    );
    expect(ESEF_TAB_PEOPLE_SQL).toContain(
      "FROM corpscout.se_esef_document_people",
    );
    expect(ESEF_TAB_PEOPLE_SQL).not.toContain("FINAL");
    expect(ESEF_TAB_PEOPLE_SQL).not.toContain("country_code");
    expect(ESEF_TAB_FILINGS_SQL).toContain("issuer_scheme = 'lei'");
    // esef_facts is joined via a per-company IN-subquery scoped to this
    // company's LEI(s) (kills duplicate-identifier fan-out and keeps the
    // facts scan bounded); uniqExact/uniqExactIf dedup cross-week
    // reprocessed facts, and coalesce fills in 0 for filings with no facts
    // now that the outer GROUP BY / countIf guard is gone.
    expect(ESEF_TAB_FILINGS_SQL).toContain("uniqExact(fact_id) AS fact_count");
    expect(ESEF_TAB_FILINGS_SQL).toContain(
      "uniqExactIf(fact_id, value_kind = 'text') AS note_count",
    );
    expect(ESEF_TAB_FILINGS_SQL).toContain("coalesce(fc.fact_count, 0)");
    // esef_document_company_information is plain MergeTree, not ReplacingMergeTree,
    // so it cannot use FINAL; ClickHouse rejects with ILLEGAL_FINAL (code 181)
    expect(ESEF_TAB_INFORMATION_SQL).not.toContain("FINAL");
    // Re-extraction appends rows per (source_document_id, model, prompt);
    // LIMIT BY keeps only the newest extraction per filing.
    expect(ESEF_TAB_INFORMATION_SQL).toContain(
      "LIMIT 1 BY source_document_id",
    );
    expect(ESEF_TAB_DOMAINS_SQL).toContain("FROM corpscout.se_esef_domains");
    expect(ESEF_TAB_DOMAINS_SQL).toContain("company_id = {companyId:String}");
    expect(ESEF_TAB_DOMAINS_SQL).toContain("extraction_status = 'ok'");
    expect(ESEF_TAB_DOMAINS_SQL).not.toContain("FINAL");
    // Two filings in one fiscal year collapse to one row per domain.
    expect(ESEF_TAB_DOMAINS_SQL).toContain(
      "GROUP BY fiscal_year, registrable_domain",
    );
    expect(ESEF_TAB_DOMAINS_SQL).toContain(
      "toJSONString(arrayDistinct(arrayFlatten(groupArray(JSONExtract(roles_json, 'Array(String)'))))) AS roles_json",
    );
    expect(ESEF_TAB_DOMAINS_SQL).toContain("max(evidence_count) AS evidence_count");
    expect(ESEF_TAB_DOMAINS_SQL).toContain("max(corroborated) AS corroborated");
    // Websites are domains now (se_esef_domains); the contacts card shows the rest.
    expect(ESEF_TAB_CONTACTS_SQL).toContain("candidate_kind != 'website'");
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
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        {
          fiscal_year: 2024,
          registrable_domain: "handelsbanken.com",
          roles_json: '["company_website","report_disclosure"]',
          evidence_count: 19,
          corroborated: 1,
        },
      ]);

    const detail = await loadSeCompanyEsef("5020077862");
    expect(detail?.filings[0].noteCount).toBe(144);
    expect(detail?.people[0].name).toBe("Carina Åkerström");
    expect(detail?.businessItems[0].itemKind).toBe("business_segment");
    expect(detail?.contacts[0].normalizedValue).toBe(
      "sustainability@handelsbanken.se",
    );
    expect(detail?.relationships).toEqual([]);
    expect(detail?.domains).toEqual([
      {
        fiscalYear: 2024,
        registrableDomain: "handelsbanken.com",
        roles: ["company_website", "report_disclosure"],
        evidenceCount: 19,
        corroborated: true,
      },
    ]);
  });
});
