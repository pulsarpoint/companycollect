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
      confidence: 1,
      sourceRecordId: "run-2026-08-09:5590000000:acme-security.se",
      sourceUrl: "https://acme-security.se/about",
      confidenceBasis: "se-domain-suggestions-dbt-v5:vat+lei",
    },
  ],
  suggestedConfidence: 1,
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
    expect(html).toContain("100% suggested confidence");
    expect(html).toContain("Common Crawl identity");
    expect(html).toContain('href="https://acme-security.se/about"');
    expect(html).toContain("Inspect technology");
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
    expect(html).toContain("100%");
    expect(html).toContain('href="/company/se/5590000000/suggestions"');
  });
});
