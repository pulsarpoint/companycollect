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
    status: "Active",
    status_code: "02",
    is_active: "yes",
    municipality_code: "3849",
    municipality_name: "SALVADOR",
  };

  it("drops the codes the page already decodes", () => {
    const decorated = decorateBrRecord(RECORD);

    expect(decorated).not.toHaveProperty("legal_nature_code");
    expect(decorated).not.toHaveProperty("company_size_code");
    expect(decorated).not.toHaveProperty("status_code");
    // status, is_active and status_code were three renderings of one fact.
    expect(decorated).not.toHaveProperty("is_active");
    // RFB's own municipality key, not the IBGE code, so it joins to nothing.
    expect(decorated).not.toHaveProperty("municipality_code");
  });

  it("keeps the labels those codes decoded to", () => {
    const decorated = decorateBrRecord(RECORD);

    expect(decorated.company_size_en).toBe("Micro");
    expect(decorated.status).toBe("Active");
    expect(decorated.municipality_name).toBe("SALVADOR");
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
