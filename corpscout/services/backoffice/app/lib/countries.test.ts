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

describe("industry facet and filter", () => {
  it("every country except lv has industryFacetQuery and industryFilterExpr", () => {
    for (const c of COUNTRIES) {
      if (c.code === "lv") {
        expect(c.industryFacetQuery, c.code).toBeUndefined();
        expect(c.industryFilterExpr, c.code).toBeUndefined();
        continue;
      }
      expect(c.industryFacetQuery, c.code).toContain(" AS value");
      expect(c.industryFacetQuery, c.code).toContain(" AS label");
      expect(c.industryFacetQuery, c.code).toContain(" AS cnt");
      expect(c.industryFilterExpr, c.code).toContain("{f_industry:Array(String)}");
    }
  });

  it("display industryQuery prefers canonical nace english labels", () => {
    // Every non-lv, non-br industryQuery joins nace_categories for the label.
    for (const c of COUNTRIES) {
      if (c.code === "lv" || c.code === "br") continue;
      expect(c.industryQuery, c.code).toContain("nace_categories");
    }
  });
});

describe("detail config", () => {
  const FIN = ["no", "fi", "ee", "lv", "gb", "br"];
  const CONTACTS = ["no", "fi", "ee", "lv", "cz", "br"];
  const DOMAINS = ["no", "fi", "ee", "lv", "cz", "br"];
  const NONE = ["se", "sk", "fr"];

  it("declares detail sections exactly per data availability", () => {
    for (const c of COUNTRIES) {
      if (NONE.includes(c.code)) {
        expect(c.detail, c.code).toBeUndefined();
        continue;
      }
      expect(!!c.detail?.financialsQuery, c.code).toBe(FIN.includes(c.code));
      expect(!!c.detail?.contactsQuery, c.code).toBe(CONTACTS.includes(c.code));
      expect(!!c.detail?.domainsQuery, c.code).toBe(DOMAINS.includes(c.code));
    }
  });

  it("every detail query is parameterized and canonical", () => {
    for (const c of COUNTRIES) {
      for (const q of [c.detail?.financialsQuery, c.detail?.contactsQuery, c.detail?.domainsQuery]) {
        if (!q) continue;
        expect(q, c.code).toContain("{id:String}");
      }
      if (c.detail?.financialsQuery) {
        for (const col of [
          "AS fiscal_year", "AS currency",
          "AS revenue_amount_original", "AS revenue_amount_usd",
          "AS net_result_amount_original", "AS net_result_amount_usd",
          "AS total_assets_amount_usd", "AS equity_amount_usd", "AS employees",
        ]) {
          expect(c.detail.financialsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
      if (c.detail?.contactsQuery) {
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_type");
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_value");
      }
      if (c.detail?.domainsQuery) {
        for (const col of ["AS domain", "AS website_url", "AS domain_source", "AS confidence", "AS is_primary"]) {
          expect(c.detail.domainsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
    }
  });

  it("norway declares a full statements query; others do not yet", () => {
    for (const c of COUNTRIES) {
      if (c.code === "no") {
        expect(c.detail?.statementsQuery).toContain("{id:String}");
        expect(c.detail?.statementsQuery).toContain("no_financial_statements");
      } else {
        expect(c.detail?.statementsQuery, c.code).toBeUndefined();
      }
    }
  });
});
