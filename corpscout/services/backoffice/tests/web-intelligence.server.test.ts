import { describe, expect, it } from "vitest";
import { getDomainWebIntelligence } from "~/lib/web-intelligence.server";

describe("getDomainWebIntelligence", () => {
  it("loads bounded historical evidence for a linked Swedish company domain", async () => {
    const intelligence = await getDomainWebIntelligence("100.se");

    expect(intelligence.domain).toBe("100.se");
    expect(intelligence.crawlCoverage.length).toBeGreaterThan(0);
    expect(intelligence.contacts.length).toBeGreaterThan(0);
    expect(
      intelligence.identifiers.some(
        (identifier) =>
          identifier.type === "gtm" && identifier.value === "GTM-57C5VPRJ",
      ),
    ).toBe(true);
    expect(intelligence.industrySnapshots.length).toBeGreaterThan(0);
    expect(
      intelligence.securitySnapshots.some((snapshot) =>
        Object.keys(snapshot.headers).some(
          (header) => header.toLowerCase() === "strict-transport-security",
        ),
      ),
    ).toBe(true);
    expect(intelligence.authoritySnapshots.length).toBeGreaterThan(0);
  }, 30_000);

  it("keeps organization claims attached to the page and crawl that supplied them", async () => {
    const intelligence = await getDomainWebIntelligence("adtraction.com");
    const claim = intelligence.organizationClaims.find(
      (candidate) =>
        candidate.name.toLowerCase().includes("adtraction") ||
        candidate.legalName.toLowerCase().includes("adtraction"),
    );

    expect(claim).toBeDefined();
    expect(claim?.crawlId).toMatch(/^CC-MAIN-/);
    expect(claim?.pageUrl).toMatch(/^https?:\/\//);
    expect(claim?.observedAt).toBeTruthy();
  }, 30_000);
});
