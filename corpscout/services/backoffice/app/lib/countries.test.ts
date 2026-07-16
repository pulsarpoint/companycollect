import { describe, expect, it } from "vitest";
import { COUNTRIES, getCountry, getSortColumn } from "~/lib/countries";

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

describe("company columns", () => {
  it("every country declares id and name columns with unique keys", () => {
    for (const c of COUNTRIES) {
      const keys = c.columns.map((col) => col.key);
      expect(keys, c.code).toEqual([...new Set(keys)]);
      expect(keys, c.code).toContain("id");
      expect(keys, c.code).toContain("name");
      expect(keys, c.code).not.toContain("industry"); // industry is virtual, merged post-query
      expect(keys, c.code).not.toContain("active"); // reserved, always selected
    }
  });

  it("every country has a sortable status column and a sortable name", () => {
    for (const c of COUNTRIES) {
      const status = c.columns.find((col) => col.kind === "status");
      expect(status, c.code).toBeDefined();
      expect(status?.sortable, c.code).toBe(true);
      expect(c.columns.find((col) => col.key === "name")?.sortable, c.code).toBe(true);
    }
  });

  it("every industry query is parameterized and returns the merge contract", () => {
    for (const c of COUNTRIES) {
      expect(c.industryQuery, c.code).toBeDefined();
      expect(c.industryQuery, c.code).toContain("{ids:Array(String)}");
      expect(c.industryQuery, c.code).toContain("AS company_id");
      expect(c.industryQuery, c.code).toContain("AS industry_code");
      expect(c.industryQuery, c.code).toContain("AS industry_label");
    }
  });

  it("getSortColumn whitelists: unknown or unsortable keys fall back to name", () => {
    const ee = getCountry("ee")!;
    expect(getSortColumn(ee, "status").key).toBe("status");
    expect(getSortColumn(ee, "id; DROP TABLE x").key).toBe("name");
    expect(getSortColumn(ee, null).key).toBe("name");
  });

  it("every country has a filterable status; only text/status kinds are filterable", () => {
    for (const c of COUNTRIES) {
      const filterable = c.columns.filter((col) => col.filterable);
      expect(filterable.length, c.code).toBeGreaterThan(0);
      expect(
        c.columns.find((col) => col.kind === "status")?.filterable,
        c.code,
      ).toBe(true);
      for (const col of filterable) {
        expect(["text", "status"], `${c.code}:${col.key}`).toContain(col.kind);
        expect(col.key, `${c.code}:${col.key}`).toMatch(/^[a-z_]+$/);
      }
    }
  });

  it("id, name, registered are never filterable", () => {
    for (const c of COUNTRIES) {
      for (const key of ["id", "name", "registered"]) {
        expect(
          c.columns.find((col) => col.key === key)?.filterable,
          `${c.code}:${key}`,
        ).toBeFalsy();
      }
    }
  });
});
