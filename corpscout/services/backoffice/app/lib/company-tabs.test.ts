import { describe, expect, it } from "vitest";
import {
  companyTabFromPath,
  domainSuggestionsTabAvailable,
  domainSuggestionsTabSupported,
  technologySectionFromPath,
  technologyTabAvailable,
} from "~/lib/company-tabs";

describe("company detail tabs", () => {
  it("recognizes the technology route", () => {
    expect(companyTabFromPath("/company/se/5594643297/technology")).toBe(
      "technology",
    );
    expect(companyTabFromPath("/company/se/5594643297/financials")).toBe(
      "financials",
    );
    expect(companyTabFromPath("/company/se/5594643297/suggestions")).toBe(
      "suggestions",
    );
    expect(companyTabFromPath("/company/se/5594643297")).toBe("overview");
  });

  it("enables domain suggestions only for Swedish companies with candidates", () => {
    expect(domainSuggestionsTabSupported("se")).toBe(true);
    expect(domainSuggestionsTabAvailable("se", true)).toBe(true);
    expect(domainSuggestionsTabAvailable("se", false)).toBe(false);
    expect(domainSuggestionsTabAvailable("no", true)).toBe(false);
  });

  it("enables technology only for Swedish companies with a domain", () => {
    expect(technologyTabAvailable("se", true)).toBe(true);
    expect(technologyTabAvailable("se", false)).toBe(false);
    expect(technologyTabAvailable("no", true)).toBe(false);
  });

  it("recognizes nested technology sections", () => {
    expect(technologySectionFromPath("/company/se/5594643297/technology")).toBe(
      "overview",
    );
    expect(
      technologySectionFromPath(
        "/company/se/5594643297/technology/web-intelligence",
      ),
    ).toBe("web-intelligence");
    expect(
      technologySectionFromPath(
        "/company/se/5594643297/technology/infrastructure",
      ),
    ).toBe("infrastructure");
    expect(
      technologySectionFromPath(
        "/company/se/5594643297/technology/ip-addresses",
      ),
    ).toBe("ip-addresses");
    expect(
      technologySectionFromPath(
        "/company/se/5594643297/technology/ip-addresses/2a02%3A28f0%3A%3A1",
      ),
    ).toBe("ip-addresses");
    expect(
      technologySectionFromPath(
        "/admin/se/company/5594643297/technology/mail-security",
      ),
    ).toBe("mail-security");
  });
});
