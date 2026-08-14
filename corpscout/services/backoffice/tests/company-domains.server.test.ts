import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  query: vi.fn(),
  insert: vi.fn(),
}));

vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertCompanyDomains: clickhouse.insert,
}));

import {
  getUnifiedCompanyDomains,
  recordCompanyDomainReview,
} from "~/lib/company-domains.server";

const row = {
  country_code: "SE",
  company_id: "5560593575",
  root_domain: "assaabloy.com",
  website_url: "https://www.assaabloy.com/",
  website_host: "www.assaabloy.com",
  source_names: ["wikidata", "esef_filing"],
  source_confidences: [1, 0.9],
  source_record_ids: ["Q123", "filing-2025"],
  source_urls: ["https://wikidata.org/wiki/Q123", "https://filing.example"],
  confidence_bases: ["official_website_claim", "repeated_filing_website"],
  suggested_confidence: 1,
  suggested_primary: 1,
  evidence_fingerprint: "a".repeat(64),
  review_status: "unreviewed",
  review_note: "",
  reviewed_by: "",
  reviewed_at: "",
  reviewed_evidence_fingerprint: "",
  is_active: 1,
  first_seen_at: "2026-08-01 00:00:00.000",
  last_seen_at: "2026-08-11 00:00:00.000",
  resolved_at: "2026-08-11 00:00:00.000",
};

function mockDomainQueries(
  rows: Array<typeof row>,
  wikidataEvidence: Array<Record<string, unknown>> = [],
) {
  clickhouse.query
    .mockResolvedValueOnce(rows)
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce(wikidataEvidence);
}

describe("unified company domains", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.insert.mockReset();
  });

  it("keeps source-specific confidence on one company-domain association", async () => {
    mockDomainQueries([row]);

    const [domain] = await getUnifiedCompanyDomains("se", "5560593575");

    expect(domain.rootDomain).toBe("assaabloy.com");
    expect(domain.sources).toEqual([
      expect.objectContaining({ name: "wikidata", confidence: 1 }),
      expect.objectContaining({ name: "esef_filing", confidence: 0.9 }),
    ]);
    expect(domain.sources.every((source) => source.evidence.length === 0)).toBe(
      true,
    );
    expect(domain.reviewStatus).toBe("unreviewed");
  });

  it("attaches compared values, page provenance, and WARC coordinates", async () => {
    const commonCrawlRow = {
      ...row,
      root_domain: "apoteket-receptfritt.se",
      website_url: "https://apoteket-receptfritt.se",
      website_host: "apoteket-receptfritt.se",
      source_names: ["common_crawl_identity"],
      source_confidences: [0.7],
      source_record_ids: [
        "run-vat:5560593575:apoteket-receptfritt.se",
      ],
      source_urls: ["https://apoteket-receptfritt.se/kontakta"],
      confidence_bases: ["se-domain-suggestions-dbt-v5:vat"],
      suggested_confidence: 0.7,
    };
    clickhouse.query
      .mockResolvedValueOnce([commonCrawlRow])
      .mockResolvedValueOnce([
        {
          root_domain: "apoteket-receptfritt.se",
          signal_type: "identifier",
          source_field: "vat",
          company_value: "SE556059357501",
          domain_value: "SE556059357501",
          score_contribution: 70,
          source_url: "https://apoteket-receptfritt.se/kontakta",
          crawl_id: "CC-MAIN-2026-25",
          discovery_run_id: "run-vat",
          suggested_at: "2026-08-10 10:49:11.577",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        {
          crawl_id: "CC-MAIN-2026-25",
          root_domain: "apoteket-receptfritt.se",
          source_field: "vat",
          domain_value: "SE556059357501",
          source_url: "https://apoteket-receptfritt.se/kontakta",
          extraction_method: "text",
          source_observed_at: "2026-07-20 17:48:38.778",
        },
      ])
      .mockResolvedValueOnce([
        {
          crawl_id: "CC-MAIN-2026-25",
          root_domain: "apoteket-receptfritt.se",
          source_url: "https://apoteket-receptfritt.se/kontakta",
          warc_filename:
            "crawl-data/CC-MAIN-2026-25/segments/example/warc/CC-MAIN-20260611030515-20260611060515-00688.warc.gz",
          warc_record_offset: 56333578,
          warc_record_length: 11919,
        },
      ]);

    const [domain] = await getUnifiedCompanyDomains("SE", "5560593575");

    expect(domain.sources[0].evidence).toEqual([
      expect.objectContaining({
        type: "common_crawl_match",
        sourceField: "vat",
        companyValue: "SE556059357501",
        domainValue: "SE556059357501",
        scoreContribution: 70,
        extractionMethod: "text",
        warcRecordOffset: 56333578,
      }),
    ]);
  });

  it("explains the exact identifier that links a Wikidata item", async () => {
    mockDomainQueries([row], [
      {
        wikidata_id: "Q123",
        match_method: "wikidata_registry_identifier",
        match_confidence: 1,
        identifier_type: "se_orgnr",
        wikidata_property_id: "P6460",
        company_value: "5560593575",
        wikidata_value: "556059-3575",
        source_record_id: "Q123:P6460:556059-3575",
        wikidata_url: "https://www.wikidata.org/entity/Q123",
        retrieved_at: "2026-07-22 23:49:43.536",
      },
    ]);

    const [domain] = await getUnifiedCompanyDomains("SE", "5560593575");

    expect(domain.sources[0].evidence).toEqual([
      expect.objectContaining({
        type: "wikidata_match",
        wikidataId: "Q123",
        propertyId: "P6460",
        companyValue: "5560593575",
        wikidataValue: "556059-3575",
      }),
    ]);
  });

  it("appends a full replacement row when a reviewer confirms a domain", async () => {
    mockDomainQueries([row]);
    const domains = await getUnifiedCompanyDomains("SE", "5560593575");

    await recordCompanyDomainReview({
      domains,
      rootDomain: "assaabloy.com",
      reviewStatus: "confirmed_primary",
      reviewedBy: "test-reviewer",
      reviewedAt: "2026-08-11T12:00:00.000Z",
    });

    expect(clickhouse.insert).toHaveBeenCalledWith([
      expect.objectContaining({
        country_code: "SE",
        company_id: "5560593575",
        root_domain: "assaabloy.com",
        source_names: ["wikidata", "esef_filing"],
        review_status: "confirmed_primary",
        reviewed_evidence_fingerprint: "a".repeat(64),
        reviewed_at: "2026-08-11 12:00:00.000",
        resolved_at: "2026-08-11 12:00:00.000",
      }),
    ]);
  });

  it("marks a review stale when the current source evidence changed", async () => {
    mockDomainQueries([
      {
        ...row,
        review_status: "confirmed_primary",
        reviewed_evidence_fingerprint: "b".repeat(64),
      },
    ]);

    const [domain] = await getUnifiedCompanyDomains("SE", "5560593575");

    expect(domain.evidenceChanged).toBe(true);
  });

  it("demotes the previous confirmed primary in the same insert", async () => {
    mockDomainQueries([
      {
        ...row,
        review_status: "confirmed_primary",
        reviewed_by: "earlier-reviewer",
        reviewed_at: "2026-08-10 10:00:00.000",
        reviewed_evidence_fingerprint: row.evidence_fingerprint,
      },
      {
        ...row,
        root_domain: "assaabloy.se",
        website_url: "https://assaabloy.se/",
        website_host: "assaabloy.se",
        evidence_fingerprint: "b".repeat(64),
        suggested_primary: 0,
      },
    ]);
    const domains = await getUnifiedCompanyDomains("SE", "5560593575");

    await recordCompanyDomainReview({
      domains,
      rootDomain: "assaabloy.se",
      reviewStatus: "confirmed_primary",
      reviewedBy: "test-reviewer",
      reviewedAt: "2026-08-11T12:00:00.000Z",
    });

    expect(clickhouse.insert).toHaveBeenCalledWith([
      expect.objectContaining({
        root_domain: "assaabloy.se",
        review_status: "confirmed_primary",
      }),
      expect.objectContaining({
        root_domain: "assaabloy.com",
        review_status: "confirmed_related",
      }),
    ]);
  });
});
