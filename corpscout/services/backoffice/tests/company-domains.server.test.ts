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

describe("unified company domains", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.insert.mockReset();
  });

  it("keeps source-specific confidence on one company-domain association", async () => {
    clickhouse.query.mockResolvedValue([row]);

    const [domain] = await getUnifiedCompanyDomains("se", "5560593575");

    expect(domain.rootDomain).toBe("assaabloy.com");
    expect(domain.sources).toEqual([
      expect.objectContaining({ name: "wikidata", confidence: 1 }),
      expect.objectContaining({ name: "esef_filing", confidence: 0.9 }),
    ]);
    expect(domain.reviewStatus).toBe("unreviewed");
  });

  it("appends a full replacement row when a reviewer confirms a domain", async () => {
    clickhouse.query.mockResolvedValue([row]);
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
    clickhouse.query.mockResolvedValue([
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
    clickhouse.query.mockResolvedValue([
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
