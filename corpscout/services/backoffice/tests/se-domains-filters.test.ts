import { describe, expect, it } from "vitest";
import {
  EMPTY_SE_DOMAINS_FILTERS,
  parseSeDomainsFilters,
  seDomainHref,
  seDomainsHref,
} from "~/lib/se-domains-filters";

describe("se domains filters", () => {
  it("reads the seven filters off the URL, trimmed and capped", () => {
    const params = new URLSearchParams(
      "domain= Example.SE &company=5560125220&association=connected&status=active&minConfidence=0.7&maxConfidence=0.9&shared=1",
    );
    expect(parseSeDomainsFilters(params)).toEqual({
      domain: "example.se",
      company: "5560125220",
      association: "connected",
      status: "active",
      minConfidence: "0.7",
      maxConfidence: "0.9",
      shared: "1",
    });
  });

  it("drops values the catalogue does not know and confidences outside 0..1", () => {
    const params = new URLSearchParams(
      "association=maybe&status=gone&minConfidence=1.5&maxConfidence=abc&shared=yes&company=abc",
    );
    expect(parseSeDomainsFilters(params)).toEqual(EMPTY_SE_DOMAINS_FILTERS);
    expect(parseSeDomainsFilters(new URLSearchParams(""))).toEqual(EMPTY_SE_DOMAINS_FILTERS);
  });

  it("builds the list href with only the non-empty filters, page and non-default size", () => {
    expect(seDomainsHref(EMPTY_SE_DOMAINS_FILTERS, 1, 50)).toBe("/admin/se/companies/domains");
    expect(
      seDomainsHref(
        { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.9", shared: "1" },
        3,
        100,
      ),
    ).toBe("/admin/se/companies/domains?minConfidence=0.9&shared=1&page=3&pageSize=100");
  });

  it("links a domain to its detail page, encoded", () => {
    expect(seDomainHref("example.se")).toBe("/admin/se/companies/domains/example.se");
    expect(seDomainHref("xn--sthlm-oua.se")).toBe(
      "/admin/se/companies/domains/xn--sthlm-oua.se",
    );
  });

  it("carries a non-default connections direction and page size on the detail href", () => {
    expect(seDomainHref("example.se", { direction: "mutual", pageSize: 100 })).toBe(
      "/admin/se/companies/domains/example.se?direction=mutual&pageSize=100",
    );
    // The defaults are the bare path: "all" and the shared default size add nothing.
    expect(seDomainHref("example.se", { direction: "all", pageSize: 50 })).toBe(
      "/admin/se/companies/domains/example.se",
    );
  });
});
