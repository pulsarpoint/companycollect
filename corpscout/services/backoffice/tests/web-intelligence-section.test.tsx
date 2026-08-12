import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { WebIntelligenceSection } from "~/components/detail/web-intelligence-section";
import type { CompanyWebIntelligence } from "~/lib/web-intelligence";

const intelligence: CompanyWebIntelligence = {
  domain: "example.se",
  crawlCoverage: [
    {
      crawlId: "CC-MAIN-2026-30",
      observedPages: 18,
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  organizationClaims: [
    {
      crawlId: "CC-MAIN-2026-30",
      pageUrl: "https://example.se/about",
      entityTypes: ["Organization"],
      name: "Example AB",
      legalName: "Example Aktiebolag",
      description: "A website-provided description.",
      entityUrl: "https://example.se/",
      logo: "",
      email: "info@example.se",
      telephone: "+46 8 123 45 67",
      sameAs: ["https://www.linkedin.com/company/example"],
      country: "SE",
      foundingYear: 2012,
      employeeCount: 42,
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  contacts: [
    {
      type: "email",
      value: "info@example.se",
      source: "page_text",
      sourceUrl: "https://example.se/contact",
      lastObservedCrawl: "CC-MAIN-2026-30",
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  identifiers: [
    {
      type: "gtm",
      value: "GTM-EXAMPLE",
      sources: ["script"],
      firstObservedCrawl: "CC-MAIN-2026-25",
      lastObservedCrawl: "CC-MAIN-2026-30",
      observedCrawls: 2,
      observedPages: 8,
      sampleUrls: ["https://example.se/"],
    },
  ],
  industrySnapshots: [
    {
      crawlId: "CC-MAIN-2026-30",
      pageType: "corporate",
      pageTypeScore: 0.94,
      classificationConfident: true,
      classificationMargin: 0.3,
      sourceUrl: "https://example.se/",
      observedAt: "2026-07-25 12:00:00.000",
      industries: [
        {
          naceCode: "62.01",
          naceLabel: "Computer programming activities",
          rank: 1,
          isPrimary: true,
          score: 0.87,
          method: "content_classifier",
          sourceUrl: "https://example.se/",
        },
      ],
    },
  ],
  pageMetadataSnapshots: [
    {
      crawlId: "CC-MAIN-2026-30",
      sourceUrl: "https://example.se/",
      title: "Example AB",
      meta: { description: "Example website" },
      canonical: "https://example.se/",
      hreflang: ["sv-SE"],
      jsonLdTypes: ["Organization"],
      charset: "utf-8",
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  securitySnapshots: [
    {
      crawlId: "CC-MAIN-2026-30",
      sourceUrl: "https://example.se/",
      headers: { "strict-transport-security": "max-age=31536000" },
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  authoritySnapshots: [
    {
      crawlId: "CC-MAIN-2026-30",
      harmonicCentrality: 0.0012,
      harmonicRank: 12345,
      pageRank: 0.000002,
      pageRankRank: 23456,
      observedHosts: 82,
      observedAt: "2026-07-25 12:00:00.000",
    },
  ],
  truncated: {
    organizationClaims: false,
    contacts: false,
    identifiers: false,
  },
};

describe("WebIntelligenceSection", () => {
  it("shows website evidence with history, provenance, and verification caveats", () => {
    const html = renderToStaticMarkup(
      <WebIntelligenceSection intelligence={intelligence} />,
    );

    expect(html).toContain("Website-derived intelligence");
    expect(html).toContain("Website evidence, not registry-verified facts");
    expect(html).toContain("Example Aktiebolag");
    expect(html).toContain("https://example.se/about");
    expect(html).toContain("Last observed");
    expect(html).toContain("GTM-EXAMPLE");
    expect(html).toContain("These are separate from registry industries");
    expect(html).toContain("Computer programming activities");
    expect(html).toContain("strict-transport-security");
    expect(html).toContain("Web graph authority");
    expect(html).toContain("CC-MAIN-2026-30");
  });
});
