import { describe, expect, it } from "vitest";

import {
  BR_CONTRACT_SECTIONS,
  brAmendment,
  brContractType,
  brJsonName,
  brPersonType,
  brPower,
  brProcessCategory,
  brRevenueOrExpenditure,
  brSphere,
  brSupplierCountry,
  brTriStateFlag,
} from "./br-contract";

describe("PNCP JSON blobs", () => {
  it("reads the name out of the stored blob", () => {
    expect(brJsonName('{"id":1,"nome":"Contrato (termo inicial)"}')).toBe(
      "Contrato (termo inicial)",
    );
  });

  it("returns the raw string when the blob is not parseable", () => {
    // Never invent: an unexpected shape shows what is actually stored.
    expect(brJsonName("Contrato")).toBe("Contrato");
    expect(brJsonName('{"id":1}')).toBe('{"id":1}');
  });

  it("treats empty and missing as absent", () => {
    expect(brJsonName("")).toBeNull();
    expect(brJsonName(null)).toBeNull();
  });

  it("renders contract type in English with the source term kept", () => {
    expect(brContractType('{"id":7,"nome":"Empenho"}')).toBe("Commitment note (Empenho)");
    expect(brContractType('{"id":1,"nome":"Contrato (termo inicial)"}')).toBe(
      "Contract, initial term (Contrato (termo inicial))",
    );
  });

  it("falls back to the Portuguese name for an unmapped type", () => {
    // A new PNCP domain value must show the source's own word, not a guess.
    expect(brContractType('{"id":99,"nome":"Novo Tipo"}')).toBe("Novo Tipo");
  });

  it("renders process category in English with the source term kept", () => {
    expect(brProcessCategory('{"id":2,"nome":"Compras"}')).toBe("Goods purchase (Compras)");
    expect(brProcessCategory('{"id":3,"nome":"Informática (TIC)"}')).toBe(
      "IT and communications (Informática (TIC))",
    );
  });
});

describe("government codes", () => {
  it("gives sphere and power different meanings for the same letter E", () => {
    // The bug this whole page exists to fix: one contract carries
    // sphere=E and power=E, which the old page rendered as nothing at all.
    expect(brSphere("E")).toBe("State level (E)");
    expect(brPower("E")).toBe("Executive branch (E)");
  });

  it("decodes every sphere value observed in the corpus", () => {
    expect(brSphere("M")).toBe("Municipal level (M)");
    expect(brSphere("F")).toBe("Federal level (F)");
    expect(brSphere("D")).toBe("Federal District (D)");
    // 1,674 rows: inter-municipal consortia, which sit at no single level.
    expect(brSphere("N")).toBe("Not applicable (N)");
  });

  it("decodes every power value observed in the corpus", () => {
    expect(brPower("L")).toBe("Legislative branch (L)");
    expect(brPower("J")).toBe("Judiciary (J)");
    // 56,046 rows (48%), overwhelmingly municipalities that never declared one.
    expect(brPower("N")).toBe("Not stated (N)");
  });

  it("shows an unknown code raw rather than inventing a branch", () => {
    expect(brSphere("Z")).toBe("Z");
    expect(brPower("Z")).toBe("Z");
  });
});

describe("supplier identity", () => {
  it("decodes the entity type", () => {
    expect(brPersonType("PJ")).toBe("Company (PJ)");
    expect(brPersonType("PF")).toBe("Individual (PF)");
    expect(brPersonType("PE")).toBe("Foreign entity (PE)");
  });

  it("names the supplier country from its ISO alpha-3 code", () => {
    expect(brSupplierCountry("BRA")).toBe("Brazil (BRA)");
    expect(brSupplierCountry("DEU")).toBe("Germany (DEU)");
  });

  it("never infers Brazil from a blank country", () => {
    // 73,294 rows (63%) are blank while 42,684 say BRA explicitly, so blank
    // means the publisher did not state it. Filling in Brazil would fabricate
    // the one field a reader uses to spot a foreign supplier.
    expect(brSupplierCountry("")).toBeNull();
    expect(brSupplierCountry(null)).toBeNull();
  });

  it("shows an unmapped country code raw", () => {
    expect(brSupplierCountry("ZZZ")).toBe("ZZZ");
  });
});

describe("numbers that are not quantities", () => {
  it("reads amendment zero as the original contract", () => {
    // 109,204 rows are 0. "0" tells a reader nothing.
    expect(brAmendment(0)).toBe("Original — no amendment");
    expect(brAmendment(2)).toBe("Amendment 2");
  });

  it("names the direction of money", () => {
    expect(brRevenueOrExpenditure(0)).toBe("Expenditure");
    expect(brRevenueOrExpenditure(1)).toBe("Revenue");
  });
});

describe("three-state flags", () => {
  it("hides a flag the source never stated", () => {
    // parliamentary_amendment is NULL on 116,121 of 116,226 rows. Rendering
    // NULL as "No" would assert something PNCP never published.
    expect(brTriStateFlag(null)).toBeNull();
    expect(brTriStateFlag(undefined)).toBeNull();
  });

  it("distinguishes an asserted false from an unstated one", () => {
    expect(brTriStateFlag(0)).toBe("No");
    expect(brTriStateFlag(1)).toBe("Yes");
  });
});

describe("section layout", () => {
  it("labels every field in both English and the source's own field name", () => {
    const fields = BR_CONTRACT_SECTIONS.flatMap((s) => s.fields);
    expect(fields.length).toBeGreaterThan(30);
    for (const field of fields) {
      expect(field.en, `en label for ${field.key}`).toMatch(/\S/);
      expect(field.source, `source label for ${field.key}`).toMatch(/\S/);
      // The original is PNCP's real API field name, which is camelCase or
      // dotted — never a snake_case copy of our own column name. A few
      // genuinely coincide (PNCP's field really is `processo`), so the rule is
      // "no snake_case", not "differs from our column name".
      expect(field.source, `${field.key} must cite PNCP's field name`).not.toMatch(/_/);
    }
  });

  it("never lists the same column twice", () => {
    const keys = BR_CONTRACT_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("keeps our own pipeline verdicts out of the contract body", () => {
    // company_match_status and match_eligibility are our matcher's opinions,
    // not the register's data. They belong in the provenance footer.
    const keys = BR_CONTRACT_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
    expect(keys).not.toContain("company_match_status");
    expect(keys).not.toContain("match_eligibility");
  });

  it("omits the near-empty budget flags from the standing layout", () => {
    // 1 true, 0 true and 2 true out of 116,226. They are rendered only when a
    // contract actually asserts them, never as "not stated" on every page.
    const keys = BR_CONTRACT_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
    expect(keys).not.toContain("parliamentary_amendment");
    expect(keys).not.toContain("from_adhesion");
    expect(keys).not.toContain("has_reallocation");
  });
});
