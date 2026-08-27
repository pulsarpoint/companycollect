import { describe, expect, it } from "vitest";
import {
  DEFAULT_SE_PEOPLE_SOURCE_TAB,
  parseSePeopleSourceFilters,
  parseSePeopleSourceTab,
  parseSePeopleSourceView,
  SE_PEOPLE_SOURCE_TABS,
  sePeopleSourcesSearch,
} from "~/lib/se-people-sources";

function url(search: string): URL {
  return new URL(`https://backoffice.test/admin/se/people${search}`);
}

describe("SE_PEOPLE_SOURCE_TABS", () => {
  it("is the three source views, then the resolved table, in reading order", () => {
    expect(SE_PEOPLE_SOURCE_TABS.map((tab) => tab.value)).toEqual([
      "bolagsverket",
      "esef",
      "wikidata",
      "final",
    ]);
    expect(DEFAULT_SE_PEOPLE_SOURCE_TAB).toBe("bolagsverket");
  });
});

describe("parseSePeopleSourceTab", () => {
  it("accepts every catalog value", () => {
    for (const tab of SE_PEOPLE_SOURCE_TABS) {
      expect(parseSePeopleSourceTab(url(`?tab=${tab.value}`))).toBe(tab.value);
    }
  });

  it("falls back to the default for absent or an unrecognised value -- never throws", () => {
    for (const search of ["", "?tab=bogus", "?tab=Final", "?tab=final; DROP TABLE"]) {
      expect(parseSePeopleSourceTab(url(search))).toBe(DEFAULT_SE_PEOPLE_SOURCE_TAB);
    }
  });
});

describe("parseSePeopleSourceFilters", () => {
  it("trims and defaults to empty strings, never undefined", () => {
    expect(parseSePeopleSourceFilters(url("?companyId=  5560125220  &name=  Ada  "))).toEqual(
      { companyId: "5560125220", name: "Ada" },
    );
    expect(parseSePeopleSourceFilters(url(""))).toEqual({ companyId: "", name: "" });
  });
});

describe("parseSePeopleSourceView", () => {
  it("clamps page and pageSize, defaulting to page 1 / the shared default page size", () => {
    expect(parseSePeopleSourceView(url(""))).toEqual({ page: 1, pageSize: 50 });
    expect(parseSePeopleSourceView(url("?page=3&pageSize=25"))).toEqual({
      page: 3,
      pageSize: 25,
    });
    // pageSize is clamped to [10, 200] -- 5.5M-row bolagsverket view, never an
    // unbounded page.
    expect(parseSePeopleSourceView(url("?pageSize=5000"))).toEqual({
      page: 1,
      pageSize: 200,
    });
  });
});

describe("sePeopleSourcesSearch", () => {
  it("sets a non-default tab and drops page", () => {
    const current = new URLSearchParams("page=3");
    expect(sePeopleSourcesSearch(current, { tab: "esef" })).toBe("?tab=esef");
  });

  it("drops the tab param entirely for the default tab, so the default URL stays bare", () => {
    const current = new URLSearchParams("tab=esef&page=2");
    expect(sePeopleSourcesSearch(current, { tab: "bolagsverket" })).toBe("?");
  });

  it("sets companyId/name filters and resets page, preserving pageSize", () => {
    const current = new URLSearchParams("tab=final&pageSize=100&page=4");
    const search = sePeopleSourcesSearch(current, { companyId: "5560125220", name: "Ada" });
    const next = new URLSearchParams(search);
    expect(next.get("tab")).toBe("final");
    expect(next.get("pageSize")).toBe("100");
    expect(next.get("companyId")).toBe("5560125220");
    expect(next.get("name")).toBe("Ada");
    expect(next.has("page")).toBe(false);
  });

  it("clears a filter by passing the empty string, without touching the other filter", () => {
    const current = new URLSearchParams("companyId=5560125220&name=Ada");
    const search = sePeopleSourcesSearch(current, { companyId: "" });
    const next = new URLSearchParams(search);
    expect(next.has("companyId")).toBe(false);
    expect(next.get("name")).toBe("Ada");
  });

  it("a page link changes only page, never resetting the active tab or filters", () => {
    const current = new URLSearchParams("tab=esef&companyId=5560125220&name=Ada");
    const search = sePeopleSourcesSearch(current, { page: 3 });
    const next = new URLSearchParams(search);
    expect(next.get("tab")).toBe("esef");
    expect(next.get("companyId")).toBe("5560125220");
    expect(next.get("name")).toBe("Ada");
    expect(next.get("page")).toBe("3");
  });
});
