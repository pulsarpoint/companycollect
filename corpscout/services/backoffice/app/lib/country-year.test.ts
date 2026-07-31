import { describe, expect, it } from "vitest";

import { parseYear, resolveYear, serializeYear } from "./country-year";

describe("parseYear", () => {
  it("reads a year", () => {
    expect(parseYear("2024")).toBe(2024);
  });

  it("ignores anything that is not a plausible year", () => {
    // Never throw on a hand-edited URL: an unusable value simply means "default".
    for (const raw of [null, "", "abc", "1200", "3000", "2024.5", "-2024"]) {
      expect(parseYear(raw), raw ?? "null").toBeNull();
    }
  });
});

describe("resolveYear", () => {
  const years = [2020, 2021, 2022, 2023, 2024];

  it("defaults to the latest year", () => {
    expect(resolveYear(null, years)).toBe(2024);
  });

  it("honours a year the data has", () => {
    expect(resolveYear(2021, years)).toBe(2021);
  });

  it("falls back to the latest when the year has no data", () => {
    // A page of empty cards is worse than showing the most recent real year.
    expect(resolveYear(1975, years)).toBe(2024);
    expect(resolveYear(2099, years)).toBe(2024);
  });

  it("returns null when there is nothing to show", () => {
    expect(resolveYear(2024, [])).toBeNull();
  });

  it("does not care about the order it was given", () => {
    expect(resolveYear(null, [2022, 2024, 2020])).toBe(2024);
  });
});

describe("serializeYear", () => {
  const years = [2022, 2023, 2024];

  it("writes nothing for the default year, so a clean view has a clean URL", () => {
    expect(serializeYear(2024, years)).toBe("");
  });

  it("writes the year when it is not the latest", () => {
    expect(serializeYear(2022, years)).toBe("year=2022");
  });

  it("round-trips", () => {
    const qs = serializeYear(2023, years);
    expect(resolveYear(parseYear(new URLSearchParams(qs).get("year")), years)).toBe(2023);
  });
});

describe("resolveYear with a caller fallback", () => {
  const years = [2022, 2023, 2024, 2025, 2026];

  it("prefers the caller's default over the latest", () => {
    // 2026 is real but barely filed; 2025 is the honest landing view.
    expect(resolveYear(null, years, 2025)).toBe(2025);
  });

  it("still honours an explicit request for the thin year", () => {
    expect(resolveYear(2026, years, 2025)).toBe(2026);
  });

  it("ignores a fallback the data does not have", () => {
    expect(resolveYear(null, years, 1999)).toBe(2026);
  });
});
