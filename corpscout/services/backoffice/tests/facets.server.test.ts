import { beforeEach, describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import {
  clearFacetCache,
  getFacetOptions,
  normalizeFacetText,
  rankFacetOptions,
  searchFacetOptions,
  type FacetOption,
} from "~/lib/facets.server";

const ee = getCountry("ee")!;

beforeEach(() => clearFacetCache());

describe("normalizeFacetText", () => {
  it("lowercases and strips diacritics", () => {
    expect(normalizeFacetText("Osaühing")).toBe("osauhing");
    expect(normalizeFacetText("São PAULO")).toBe("sao paulo");
    expect(normalizeFacetText("Řím")).toBe("rim");
  });
});

describe("rankFacetOptions", () => {
  const options: FacetOption[] = [
    { value: "Osaühing", label: "Osaühing", count: 100 },
    { value: "Aktsiaselts", label: "Aktsiaselts", count: 50 },
    { value: "Usaldusühing", label: "Usaldusühing", count: 10 },
  ];

  it("matches diacritic-insensitively anywhere in the string", () => {
    // Both contain "ühing" mid-string (no prefix match) → ordered by count desc.
    const hits = rankFacetOptions(options, "uhing", 50);
    expect(hits.map((o) => o.value)).toEqual(["Osaühing", "Usaldusühing"]);
  });

  it("ranks prefix matches before substring matches, ties by count desc", () => {
    const hits = rankFacetOptions(options, "usaldus", 50);
    expect(hits[0].value).toBe("Usaldusühing");
    const sub = rankFacetOptions(options, "ühing", 50);
    // both Osaühing (substring) and Usaldusühing (substring): count desc
    expect(sub.map((o) => o.value)).toEqual(["Osaühing", "Usaldusühing"]);
  });

  it("matches against the label too", () => {
    const labeled: FacetOption[] = [
      { value: "6201", label: "Computer programming activities", count: 5 },
    ];
    expect(rankFacetOptions(labeled, "programming", 50)).toHaveLength(1);
    expect(rankFacetOptions(labeled, "6201", 50)).toHaveLength(1);
  });

  it("respects the limit", () => {
    const many: FacetOption[] = Array.from({ length: 100 }, (_, i) => ({
      value: `v${i}`, label: `v${i}`, count: 100 - i,
    }));
    expect(rankFacetOptions(many, "v", 50)).toHaveLength(50);
  });
});

describe("facet cache against live ClickHouse (Estonia)", () => {
  it("loads status options with counts, sorted desc, no empties", async () => {
    const options = await getFacetOptions(ee, "status");
    expect(options.length).toBeGreaterThan(0);
    for (const o of options) {
      expect(o.value).not.toBe("");
      expect(o.count).toBeGreaterThan(0);
    }
    const counts = options.map((o) => o.count);
    expect([...counts].sort((a, b) => b - a)).toEqual(counts);
  });

  it("serves the second call from cache (same array reference)", async () => {
    const first = await getFacetOptions(ee, "status");
    const second = await getFacetOptions(ee, "status");
    expect(second).toBe(first);
  });

  it("rejects unknown or non-filterable facet keys", async () => {
    await expect(getFacetOptions(ee, "name")).rejects.toThrow(/unknown facet/i);
    await expect(getFacetOptions(ee, "id; DROP")).rejects.toThrow(/unknown facet/i);
  });

  it("searchFacetOptions: empty q caps at 200, typed q ranks matches", async () => {
    const top = await searchFacetOptions(ee, "legal_form", "");
    expect(top.length).toBeGreaterThan(0);
    expect(top.length).toBeLessThanOrEqual(200);
    const typed = await searchFacetOptions(ee, "legal_form", top[0].value.slice(0, 4));
    expect(typed.length).toBeGreaterThan(0);
    expect(typed.length).toBeLessThanOrEqual(50);
  });
});

describe("industry facet (Estonia)", () => {
  it("serves industry options with canonical english labels", async () => {
    const options = await getFacetOptions(ee, "industry");
    expect(options.length).toBeGreaterThan(0);
    const labeled = options.filter((o) => o.label !== o.value);
    // Most 4-digit EE codes resolve in nace_categories → english label
    expect(labeled.length).toBeGreaterThan(options.length / 2);
  });
});
