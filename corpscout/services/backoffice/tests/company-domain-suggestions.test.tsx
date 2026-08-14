import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { CompanyDomainSuggestionReview } from "~/components/domain-suggestions/company-domain-suggestion-review";
import { CompanyDomainSuggestionsSection } from "~/components/detail/company-domain-suggestions-section";
import type { CompanyDomain } from "~/lib/company-domains.server";

const domain: CompanyDomain = {
  countryCode: "SE",
  companyId: "5590000000",
  rootDomain: "acme-security.se",
  websiteUrl: "https://acme-security.se/",
  websiteHost: "acme-security.se",
  sources: [
    {
      name: "common_crawl_identity",
      confidence: 0.7,
      sourceRecordId: "run-2026-08-09:5590000000:acme-security.se",
      sourceUrl: "https://acme-security.se/about",
      confidenceBasis: "se-domain-suggestions-dbt-v5:vat",
      evidence: [
        {
          type: "common_crawl_match",
          signalType: "identifier",
          sourceField: "vat",
          companyValue: "SE559000000001",
          domainValue: "SE559000000001",
          scoreContribution: 70,
          sourceUrl: "https://acme-security.se/about",
          crawlId: "CC-MAIN-2026-25",
          extractionMethod: "text",
          sourceObservedAt: "2026-07-20 17:48:38.778",
          warcFilename:
            "crawl-data/CC-MAIN-2026-25/segments/1780687572613.18/warc/CC-MAIN-20260611030515-20260611060515-00688.warc.gz",
          warcRecordOffset: 56333578,
          warcRecordLength: 11919,
          discoveryRunId: "run-2026-08-09",
          suggestedAt: "2026-08-09 12:00:00.000",
        },
      ],
    },
  ],
  suggestedConfidence: 0.7,
  suggestedPrimary: true,
  evidenceFingerprint: "a".repeat(64),
  reviewStatus: "unreviewed",
  reviewNote: "",
  reviewedBy: "",
  reviewedAt: null,
  reviewedEvidenceFingerprint: "",
  evidenceChanged: false,
  active: true,
  firstSeenAt: "2026-08-09 12:00:00.000",
  lastSeenAt: "2026-08-09 12:00:00.000",
  resolvedAt: "2026-08-09 12:00:00.000",
};

function renderDomains(domains: CompanyDomain[]): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <CompanyDomainSuggestionsSection
            domains={domains}
            reviewAction="/company/se/5590000000/suggestions"
            technologyPath="/company/se/5590000000/technology"
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: ["/company/se/5590000000/suggestions"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("CompanyDomainSuggestionsSection", () => {
  it("renders one association with source-specific confidence", () => {
    const html = renderDomains([domain]);

    expect(html).toContain("Company domains");
    expect(html).toContain("acme-security.se");
    expect(html).toContain("70% suggested confidence");
    expect(html).toContain("Common Crawl identity");
    expect(html).toContain('href="https://acme-security.se/about"');
    expect(html).toContain("Exact VAT match from archived website evidence");
    expect(html).toContain("SE559000000001");
    expect(html).toContain("+70 points");
    expect(html).toContain("11 Jun 2026, 03:05–06:05 UTC");
    expect(html).toContain("CC-MAIN-2026-25");
    expect(html).not.toContain("se-domain-suggestions-dbt-v5:vat");
    expect(html).toContain("Inspect technology");
  });

  it("explains how a Wikidata website was linked to the Swedish company", () => {
    const html = renderDomains([
      {
        ...domain,
        rootDomain: "acme.se",
        websiteUrl: "https://acme.se",
        suggestedConfidence: 1,
        sources: [
          {
            name: "wikidata",
            confidence: 1,
            sourceRecordId: "Q123:website:https://acme.se",
            sourceUrl: "https://www.wikidata.org/entity/Q123",
            confidenceBasis: "official_website_claim",
            evidence: [
              {
                type: "wikidata_match",
                wikidataId: "Q123",
                matchMethod: "wikidata_registry_identifier",
                matchConfidence: 1,
                identifierType: "se_orgnr",
                propertyId: "P6460",
                companyValue: "5590000000",
                wikidataValue: "559000-0000",
                sourceRecordId: "Q123:P6460:559000-0000",
                wikidataUrl: "https://www.wikidata.org/entity/Q123",
                retrievedAt: "2026-07-22 23:49:43.536",
              },
            ],
          },
        ],
      },
    ]);

    expect(html).toContain("exact Swedish organisation number");
    expect(html).toContain("Exact Swedish organisation number match");
    expect(html).toContain("5590000000");
    expect(html).toContain("559000-0000");
    expect(html).toContain("P6460");
    expect(html).toContain("Official website claim: P856");
  });

  it("renders an explicit empty state", () => {
    const html = renderDomains([]);

    expect(html).toContain("No associated domains");
    expect(html).toContain("No source currently proposes a domain");
  });

  it("renders ClickHouse-backed review status and decision controls", () => {
    const html = renderDomains([
      {
        ...domain,
        reviewStatus: "confirmed_primary",
        reviewedBy: "test-reviewer",
        reviewedAt: "2026-08-10T12:00:00.000Z",
        reviewedEvidenceFingerprint: domain.evidenceFingerprint,
      },
    ]);

    expect(html).toContain("Confirmed primary");
    expect(html).toContain("Confirm related");
    expect(html).toContain("Reject");
    expect(html).toContain("Clear review");
    expect(html).toContain('name="root_domain" value="acme-security.se"');
  });
});

describe("CompanyDomainSuggestionReview", () => {
  it("renders the unified review queue and links each domain to its company", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <CompanyDomainSuggestionReview
              countryCode="se"
              query=""
              source="all"
              result={{
                rows: [{ ...domain, companyName: "Acme Security AB" }],
                total: 1,
                page: 1,
                pageSize: 50,
              }}
            />
          ),
        },
      ],
      { initialEntries: ["/countries/se/domain-suggestions"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Domain review");
    expect(html).toContain("Acme Security AB");
    expect(html).toContain("acme-security.se");
    expect(html).toContain("common crawl identity");
    expect(html).toContain("70%");
    expect(html).toContain('href="/company/se/5590000000/suggestions"');
  });
});
