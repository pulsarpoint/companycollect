import { describe, expect, it } from "vitest";
import { EU_EEA_COUNTRIES } from "./eu-countries";

describe("EU_EEA_COUNTRIES", () => {
  it("contains the EU-27 plus EEA (30 entries) with unique iso2 codes", () => {
    expect(EU_EEA_COUNTRIES).toHaveLength(30);
    expect(new Set(EU_EEA_COUNTRIES.map((c) => c.iso2)).size).toBe(30);
  });

  it("includes the loaded TED countries and sorts by name", () => {
    const codes = EU_EEA_COUNTRIES.map((c) => c.iso2);
    for (const code of ["SE", "FI", "NO"]) expect(codes).toContain(code);
    const names = EU_EEA_COUNTRIES.map((c) => c.name);
    expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b)));
  });
});
