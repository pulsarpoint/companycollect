import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { filterableFacetKeys, parseFilters } from "~/lib/filters";

const ee = getCountry("ee")!;

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
});

import { parseUnifiedFilters, UNIFIED_FACET_KEYS, UNIFIED_FACET_LABELS } from "~/lib/filters";

describe("unified filters", () => {
  it("exposes country first and industry last among the keys", () => {
    expect(UNIFIED_FACET_KEYS[0]).toBe("country");
    expect(UNIFIED_FACET_KEYS).toContain("status");
    expect(UNIFIED_FACET_KEYS).toContain("industry");
    expect(Object.keys(UNIFIED_FACET_LABELS)).toEqual(expect.arrayContaining(UNIFIED_FACET_KEYS));
  });

  it("whitelists country values against the registry", () => {
    const sp = new URLSearchParams("f_country=ee&f_country=xx&f_country=no&f_status=A");
    expect(parseUnifiedFilters(sp)).toEqual({ country: ["ee", "no"], status: ["A"] });
  });

  it("ignores unknown filter keys", () => {
    const sp = new URLSearchParams("f_bogus=1&f_industry=6201");
    expect(parseUnifiedFilters(sp)).toEqual({ industry: ["6201"] });
  });
});
