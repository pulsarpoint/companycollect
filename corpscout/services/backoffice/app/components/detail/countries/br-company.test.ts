import { describe, expect, it } from "vitest";

import { brLegalNature, decorateBrRecord } from "./br-company";

describe("Brazilian legal nature", () => {
  it("translates the code, keeping the Portuguese the register published", () => {
    expect(brLegalNature("2062", "Sociedade Empresária Limitada")).toBe(
      "Private limited company (Sociedade Empresária Limitada)",
    );
    // 44.4M of 68.6M companies -- the single most common legal nature.
    expect(brLegalNature("2135", "Empresário (Individual)")).toBe(
      "Sole trader (Empresário (Individual))",
    );
  });

  it("names the two that are not businesses at all", () => {
    // RFB issues CNPJs to election candidates (2,937,479) and individual rural
    // producers (636,055), so a reader must be able to tell them apart from firms.
    expect(brLegalNature("4090", "Candidato a Cargo Político Eletivo")).toContain(
      "Candidate for elected political office",
    );
    expect(brLegalNature("4120", "Produtor Rural (Pessoa Física)")).toContain(
      "Individual rural producer",
    );
  });

  it("falls back to the Portuguese for an unmapped code", () => {
    // A new CONCLA value must show what RFB said, not vanish or be guessed at.
    expect(brLegalNature("9999", "Alguma Natureza Nova")).toBe("Alguma Natureza Nova");
  });

  it("falls back to the code when there is no description either", () => {
    expect(brLegalNature("9999", "")).toBe("9999");
  });

  it("is absent when the register stated nothing", () => {
    expect(brLegalNature("", "")).toBeNull();
  });
});

describe("decorating a Brazilian company record", () => {
  const RECORD = {
    legal_name: "CONSULTSIDE COMERCIO SERVICOS E SOLUCOES INTEGRADAS LTDA",
    legal_nature_code: "2062",
    legal_nature_description_pt: "Sociedade Empresária Limitada",
    company_size_code: "01",
    company_size_en: "Micro",
    status_en: "Active",
    status_code: "02",
    is_active: "yes",
    municipality_code: "3849",
    municipality_name: "SALVADOR",
  };

  it("pairs every unified value with the original, in one row", () => {
    // A derived value alone is unfalsifiable, and for size and status the code is
    // the ONLY original RFB publishes -- there is no Portuguese text behind them.
    const decorated = decorateBrRecord(RECORD);

    expect(decorated.company_size).toBe("Micro (01)");
    expect(decorated.status_en).toBe("Active (02)");
    expect(decorated.municipality_name).toBe("SALVADOR (3849)");
  });

  it("removes the codes as separate rows, having folded them in", () => {
    const decorated = decorateBrRecord(RECORD);

    for (const key of [
      "legal_nature_code",
      "company_size_code",
      "status_code",
      "municipality_code",
    ]) {
      expect(decorated, key).not.toHaveProperty(key);
    }
  });

  it("drops is_active, which is a second derivation rather than an original", () => {
    // status, status_code and is_active were three renderings of one fact. The
    // code is the original; is_active is just status computed again.
    expect(decorateBrRecord(RECORD)).not.toHaveProperty("is_active");
  });

  it("shows the value alone when the register published no code", () => {
    const decorated = decorateBrRecord({ ...RECORD, company_size_code: "" });

    expect(decorated.company_size).toBe("Micro");
  });

  it("shows the code alone when there is no decoded value", () => {
    // 4,063 rows carry no company_size_code at all, and a handful the reverse.
    const decorated = decorateBrRecord({ ...RECORD, company_size_en: "" });

    expect(decorated.company_size).toBe("01");
  });

  it("replaces the Portuguese-only field with a translated one", () => {
    const decorated = decorateBrRecord(RECORD);

    expect(decorated).not.toHaveProperty("legal_nature_description_pt");
    expect(decorated.legal_nature).toBe(
      "Private limited company (Sociedade Empresária Limitada)",
    );
  });

  it("leaves every other field untouched", () => {
    const decorated = decorateBrRecord(RECORD);

    expect(decorated.legal_name).toBe(RECORD.legal_name);
  });

  it("does not mutate the record it was given", () => {
    const record = { ...RECORD };
    decorateBrRecord(record);

    expect(record.legal_nature_code).toBe("2062");
  });
});


describe("pairing never invents a row", () => {
  it("leaves the original alone when the record has no field to fold it into", () => {
    // The record is SELECT * from br_companies, so a key that is not a column
    // is simply absent. Creating it would show a bare code under a label the
    // register never published -- which is how this first read "Status: 02".
    const decorated = decorateBrRecord({ status_code: "02", legal_name: "X" });

    expect(decorated).not.toHaveProperty("status_en");
    expect(decorated.status_code).toBe("02");
  });
});
