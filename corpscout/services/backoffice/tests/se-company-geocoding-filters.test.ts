import { describe, expect, it } from "vitest";
import {
  DEFAULT_GEOCODE_LIST_FILTER,
  GEOCODE_LIST_FILTERS,
  GEOCODE_STATUS_CLASSES,
  GEOCODE_STATUS_PARAM,
  geocodeListSearch,
  parseGeocodeListFilter,
} from "~/lib/se-company-geocoding-filters";

describe("GEOCODE_LIST_FILTERS", () => {
  it("is needs_attention, all, and every status class -- in that order", () => {
    expect(GEOCODE_LIST_FILTERS).toEqual([
      "needs_attention",
      "all",
      "geocoded",
      "ambiguous",
      "unmatched",
      "no_outcome",
    ]);
    expect(GEOCODE_STATUS_CLASSES).toEqual([
      "geocoded",
      "ambiguous",
      "unmatched",
      "no_outcome",
    ]);
    expect(DEFAULT_GEOCODE_LIST_FILTER).toBe("needs_attention");
  });
});

describe("parseGeocodeListFilter", () => {
  it("accepts every catalog value", () => {
    for (const value of GEOCODE_LIST_FILTERS) {
      expect(parseGeocodeListFilter(value)).toBe(value);
    }
  });

  it("falls back to the default for null, absent, or an unrecognised value -- never throws", () => {
    for (const value of [null, "", "bogus", "geocoded; DROP TABLE", "Geocoded"]) {
      expect(parseGeocodeListFilter(value)).toBe(DEFAULT_GEOCODE_LIST_FILTER);
    }
  });
});

describe("geocodeListSearch", () => {
  it("sets the status param for a non-default filter and preserves other params", () => {
    const current = new URLSearchParams("pageSize=100&page=3");
    expect(geocodeListSearch(current, "ambiguous")).toBe(
      "?pageSize=100&status=ambiguous",
    );
  });

  it("drops the status param entirely for the default filter, so the default URL stays bare", () => {
    const current = new URLSearchParams("status=ambiguous&page=2");
    expect(geocodeListSearch(current, "needs_attention")).toBe("?");
  });

  it("always drops page (a class change invalidates the old page number)", () => {
    const current = new URLSearchParams("page=5");
    const search = geocodeListSearch(current, "all");
    expect(new URLSearchParams(search).has("page")).toBe(false);
  });

  it("uses the same param name the route loader reads", () => {
    expect(GEOCODE_STATUS_PARAM).toBe("status");
  });
});
