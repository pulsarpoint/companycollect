import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { filterableFacetKeys, parseFilters } from "~/lib/filters";

const ee = getCountry("ee")!;
const se = getCountry("se")!;
const cz = getCountry("cz")!;

describe("filterableFacetKeys", () => {
  it("lists only filterable column keys", () => {
    const keys = filterableFacetKeys(ee);
    expect(keys).toContain("status");
    expect(keys).toContain("legal_form");
    expect(keys).not.toContain("id");
    expect(keys).not.toContain("name");
  });
});

describe("parseFilters", () => {
  it("reads repeated params for whitelisted keys only", () => {
    const sp = new URLSearchParams(
      "f_status=Registered&f_status=Deleted&f_name=hack&f_bogus=x&q=grupp",
    );
    expect(parseFilters(sp, ee)).toEqual({
      status: ["Registered", "Deleted"],
    });
  });

  it("trims, drops empties, dedupes", () => {
    const sp = new URLSearchParams("f_status=+A+&f_status=A&f_status=");
    expect(parseFilters(sp, ee)).toEqual({ status: ["A"] });
  });

  it("returns empty object when nothing matches", () => {
    expect(parseFilters(new URLSearchParams("q=x&page=2"), ee)).toEqual({});
  });

  it("accepts only yes or no where a financial-data source exists", () => {
    const sp = new URLSearchParams(
      "f_flag_financials=yes&" +
        "f_flag_financials=no&" +
        "f_flag_financials=made_up",
    );
    expect(parseFilters(sp, se)).toEqual({
      flag_financials: ["yes", "no"],
    });
    expect(parseFilters(sp, ee)).toEqual({
      flag_financials: ["yes", "no"],
    });
    expect(parseFilters(sp, cz)).toEqual({});
  });
});
