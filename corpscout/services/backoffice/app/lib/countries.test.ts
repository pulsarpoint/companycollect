import { describe, expect, it } from "vitest";
import { COUNTRIES, getCountry } from "~/lib/countries";

describe("country registry", () => {
  it("contains all ten countries with unique lowercase ISO2 codes", () => {
    const codes = COUNTRIES.map((c) => c.code);
    expect(codes).toEqual([...new Set(codes)]);
    expect(codes.every((c) => /^[a-z]{2}$/.test(c))).toBe(true);
    expect(codes.sort()).toEqual(
      ["br", "cz", "ee", "fi", "fr", "gb", "lv", "no", "se", "sk"].sort(),
    );
  });

  it("resolves countries case-insensitively", () => {
    expect(getCountry("no")?.name).toBe("Norway");
    expect(getCountry("NO")?.name).toBe("Norway");
    expect(getCountry("xx")).toBeUndefined();
  });

  it("maps Sweden to its status-based active expression", () => {
    const se = getCountry("se");
    expect(se?.companiesTable).toBe("se_companies");
    expect(se?.nameColumn).toBe("legal_name");
    expect(se?.activeExpr).toBe("status = 'active'");
  });
});
