import { describe, expect, it } from "vitest";
import {
  parseAddressQualityFilter,
  searchAddressQualityQueue,
} from "~/lib/address-quality.server";

describe("Sweden address quality queue", () => {
  it("defaults unknown filters to the actionable ambiguous queue", () => {
    expect(parseAddressQualityFilter(null)).toBe("ambiguous");
    expect(parseAddressQualityFilter("unknown")).toBe("ambiguous");
    expect(parseAddressQualityFilter("city_fallback")).toBe("city_fallback");
    expect(parseAddressQualityFilter("street_fallback")).toBe(
      "street_fallback",
    );
  });

  it("composes address-owned data with geocodes and company links", async () => {
    const result = await searchAddressQualityQueue({
      filter: "ambiguous",
      query: "",
      page: 1,
      pageSize: 25,
    });

    expect(result.stats.ambiguous).toBeGreaterThan(0);
    expect(result.total).toBe(result.stats.ambiguous);
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.length).toBeLessThanOrEqual(25);
    expect(result.rows.every((row) => row.matchStatus === "ambiguous")).toBe(
      true,
    );
    expect(result.rows.every((row) => row.addressId.length === 64)).toBe(true);
    expect(result.rows.some((row) => row.candidateRecordUrls.length > 1)).toBe(
      true,
    );
  });
});
