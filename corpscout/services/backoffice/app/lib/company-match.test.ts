import { describe, expect, it } from "vitest";
import { pickCompanyMatch, toIso2, type CompanyMatch } from "./company-match";

describe("toIso2", () => {
  it("passes through a lowercase iso2 unchanged", () => {
    expect(toIso2("se")).toBe("se");
  });

  it("lowercases an uppercase iso2", () => {
    expect(toIso2("CZ")).toBe("cz");
  });

  it("maps a known iso3 to its iso2", () => {
    expect(toIso2("CZE")).toBe("cz");
    expect(toIso2("SWE")).toBe("se");
  });

  it("returns null for an empty value", () => {
    expect(toIso2("")).toBeNull();
    expect(toIso2(null)).toBeNull();
    expect(toIso2(undefined)).toBeNull();
  });

  it("returns null for an unknown iso3", () => {
    expect(toIso2("XYZ")).toBeNull();
  });
});

describe("pickCompanyMatch", () => {
  const cz: CompanyMatch = { country_code: "cz", company_id: "29289688" };
  const br: CompanyMatch = { country_code: "br", company_id: "29289688" };

  it("resolves a collision to the candidate named by the row's country", () => {
    // The real bug: a TED winner_national_id of 29289688 matches both a
    // Czech and a Brazilian register row; winner_country='CZE' must select cz.
    expect(pickCompanyMatch([br, cz], "CZE")).toEqual(cz);
  });

  it("returns null for a collision with no row country to disambiguate", () => {
    expect(pickCompanyMatch([br, cz], null)).toBeNull();
    expect(pickCompanyMatch([br, cz], "")).toBeNull();
  });

  it("links a single candidate when the row carries no country signal", () => {
    expect(pickCompanyMatch([cz], undefined)).toEqual(cz);
  });

  it("returns null when the row's country matches no candidate", () => {
    expect(pickCompanyMatch([br, cz], "SWE")).toBeNull();
  });

  it("returns null for no candidates at all", () => {
    expect(pickCompanyMatch([], "CZE")).toBeNull();
    expect(pickCompanyMatch(undefined, "CZE")).toBeNull();
  });
});
