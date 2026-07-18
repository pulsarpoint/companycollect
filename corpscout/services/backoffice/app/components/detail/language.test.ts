import { describe, expect, it } from "vitest";
import { keyFacts, resolveRecordFields } from "~/components/detail/language";

// Realistic per-country fixtures, matching the live-audited paired-field
// landscape: NO (articles_purpose/activity_text/legal_form_description
// pairs), EE (legal_form/legal_form_subtype/status pairs), LV (activity_text
// pair + unpaired legal_form_description_en), FR/CZ (unpaired *_en singles),
// BR (unpaired *_en singles + an *_original-only currency field), GB (no
// pairs at all).

describe("resolveRecordFields — NO record (three pairs + passthrough)", () => {
  const shortNorwegian = "Aksjeselskap";
  const shortEnglish = "Private limited company";
  const activityNorwegian =
    "Utvikling av programvare for maritim navigasjon og logistikk.";
  const purposeNorwegian =
    "Selskapets formål er å drive utvikling, salg og drift av programvare.";
  const purposeEnglish =
    "The company's purpose is to develop, sell and operate software.";

  const noRecord: Record<string, unknown> = {
    business_id: "912345678",
    name: "Fjord Solutions AS",
    legal_form_description_original: shortNorwegian,
    legal_form_description_en: shortEnglish,
    activity_text_original: activityNorwegian,
    activity_text_en: "", // empty -> triggers fallback in en mode
    articles_purpose_original: purposeNorwegian,
    articles_purpose_en: purposeEnglish,
    last_submitted_accounts_year: 2024,
    source_run_id: "run-1", // lineage, must be excluded
  };

  it("en mode: shows _en values under base labels, falls back when _en is empty", () => {
    const { fields, longTexts, pairCount } = resolveRecordFields(noRecord, "en");

    expect(pairCount).toBe(3);

    expect(fields).toEqual([
      { key: "business_id", label: "Business ID", value: "912345678", fromOtherLang: false, isLongText: false },
      { key: "name", label: "Name", value: "Fjord Solutions AS", fromOtherLang: false, isLongText: false },
      { key: "legal_form_description", label: "Legal form description", value: shortEnglish, fromOtherLang: false, isLongText: false },
      { key: "last_submitted_accounts_year", label: "Last submitted accounts year", value: 2024, fromOtherLang: false, isLongText: false },
    ]);

    expect(longTexts).toEqual([
      { key: "activity_text", label: "Activity text", value: activityNorwegian, fromOtherLang: true, isLongText: true },
      { key: "articles_purpose", label: "Articles purpose", value: purposeEnglish, fromOtherLang: false, isLongText: true },
    ]);
  });

  it("original mode: shows _original values under the same base labels", () => {
    const { fields, longTexts, pairCount } = resolveRecordFields(noRecord, "original");

    expect(pairCount).toBe(3);

    expect(fields.find((f) => f.key === "legal_form_description")).toEqual({
      key: "legal_form_description",
      label: "Legal form description",
      value: shortNorwegian,
      fromOtherLang: false,
      isLongText: false,
    });

    expect(longTexts).toEqual([
      { key: "activity_text", label: "Activity text", value: activityNorwegian, fromOtherLang: false, isLongText: true },
      { key: "articles_purpose", label: "Articles purpose", value: purposeNorwegian, fromOtherLang: false, isLongText: true },
    ]);
  });

  it("never surfaces the raw *_original/_en keys themselves for collapsed pairs", () => {
    const { fields, longTexts } = resolveRecordFields(noRecord, "en");
    const allKeys = [...fields, ...longTexts].map((f) => f.key);
    expect(allKeys).not.toContain("legal_form_description_en");
    expect(allKeys).not.toContain("legal_form_description_original");
    expect(allKeys).not.toContain("activity_text_en");
    expect(allKeys).not.toContain("activity_text_original");
  });

  it("excludes lineage from fields entirely", () => {
    const { fields, longTexts } = resolveRecordFields(noRecord, "en");
    const allKeys = [...fields, ...longTexts].map((f) => f.key);
    expect(allKeys).not.toContain("source_run_id");
  });
});

describe("resolveRecordFields — fallback semantics", () => {
  it("falls back to original when the selected (en) variant is empty, marking fromOtherLang", () => {
    const record = { activity_text_original: "Ehtne tekst", activity_text_en: "" };
    const { longTexts } = resolveRecordFields(record, "en");
    expect(longTexts).toEqual([
      { key: "activity_text", label: "Activity text", value: "Ehtne tekst", fromOtherLang: true, isLongText: true },
    ]);
  });

  it("falls back to en when the selected (original) variant is empty, marking fromOtherLang", () => {
    const record = { legal_form_description_original: "", legal_form_description_en: "Private limited company" };
    const { fields } = resolveRecordFields(record, "original");
    expect(fields).toEqual([
      { key: "legal_form_description", label: "Legal form description", value: "Private limited company", fromOtherLang: true, isLongText: false },
    ]);
  });

  it("keeps a pair field present with an empty value when both variants are empty (no fallback possible)", () => {
    const record = { legal_form_original: "", legal_form_en: "" };
    const { fields } = resolveRecordFields(record, "en");
    expect(fields).toEqual([
      { key: "legal_form", label: "Legal form", value: "", fromOtherLang: false, isLongText: false },
    ]);
  });
});

describe("resolveRecordFields — EE record (three pairs)", () => {
  const eeRecord: Record<string, unknown> = {
    registry_code: "12345678",
    name: "Näidis OÜ",
    legal_form_original: "Osaühing",
    legal_form_en: "Private limited company",
    legal_form_subtype_original: "Tavaline",
    legal_form_subtype_en: "Regular",
    status_original: "Registrisse kantud",
    status_en: "Registered",
  };

  it("collapses legal_form, legal_form_subtype, and status pairs (pairCount 3)", () => {
    const { fields, pairCount } = resolveRecordFields(eeRecord, "en");
    expect(pairCount).toBe(3);
    expect(fields).toEqual([
      { key: "registry_code", label: "Registry code", value: "12345678", fromOtherLang: false, isLongText: false },
      { key: "name", label: "Name", value: "Näidis OÜ", fromOtherLang: false, isLongText: false },
      { key: "legal_form", label: "Legal form", value: "Private limited company", fromOtherLang: false, isLongText: false },
      { key: "legal_form_subtype", label: "Legal form subtype", value: "Regular", fromOtherLang: false, isLongText: false },
      { key: "status", label: "Status", value: "Registered", fromOtherLang: false, isLongText: false },
    ]);
  });

  it("resolves the original-language variant on request", () => {
    const { fields } = resolveRecordFields(eeRecord, "original");
    expect(fields.find((f) => f.key === "status")).toEqual({
      key: "status",
      label: "Status",
      value: "Registrisse kantud",
      fromOtherLang: false,
      isLongText: false,
    });
  });
});

describe("resolveRecordFields — BR record (unpaired singles, currency field must not collapse)", () => {
  const brRecord: Record<string, unknown> = {
    cnpj: "12.345.678/0001-90",
    name: "Empresa Exemplo LTDA",
    status_en: "Active", // no status_original -> unpaired
    company_size_en: "Small", // no company_size_original -> unpaired
    share_capital_amount_original: "R$ 100.000,00", // no _en -> unpaired, currency convention
  };

  it("does not collapse any of the unpaired singles, and pairCount is 0", () => {
    const { fields, pairCount } = resolveRecordFields(brRecord, "en");
    expect(pairCount).toBe(0);
    expect(fields).toEqual([
      { key: "cnpj", label: "CNPJ", value: "12.345.678/0001-90", fromOtherLang: false, isLongText: false },
      { key: "name", label: "Name", value: "Empresa Exemplo LTDA", fromOtherLang: false, isLongText: false },
      { key: "status_en", label: "Status en", value: "Active", fromOtherLang: false, isLongText: false },
      { key: "company_size_en", label: "Company size en", value: "Small", fromOtherLang: false, isLongText: false },
      { key: "share_capital_amount_original", label: "Share capital amount original", value: "R$ 100.000,00", fromOtherLang: false, isLongText: false },
    ]);
  });

  it("pins the share_capital_amount_original currency field specifically: it must survive un-collapsed", () => {
    const { fields } = resolveRecordFields(brRecord, "original");
    const shareCapital = fields.find((f) => f.key === "share_capital_amount_original");
    expect(shareCapital).toBeDefined();
    expect(shareCapital?.value).toBe("R$ 100.000,00");
    // Must NOT have been collapsed into a base "share_capital_amount" key.
    expect(fields.some((f) => f.key === "share_capital_amount")).toBe(false);
  });
});

describe("resolveRecordFields — LV record (one pair + an unpaired _en-only single)", () => {
  const lvRecord: Record<string, unknown> = {
    registration_number: "40000000000",
    name: "SIA Piemērs",
    activity_text_original: "Programmatūras izstrāde.",
    activity_text_en: "Software development.",
    legal_form_description_en: "Limited liability company", // no _original -> unpaired
  };

  it("collapses only activity_text (pairCount 1); legal_form_description_en passes through as-is", () => {
    const { fields, longTexts, pairCount } = resolveRecordFields(lvRecord, "en");
    expect(pairCount).toBe(1);
    expect(longTexts).toEqual([
      { key: "activity_text", label: "Activity text", value: "Software development.", fromOtherLang: false, isLongText: true },
    ]);
    expect(fields).toEqual([
      { key: "registration_number", label: "Registration number", value: "40000000000", fromOtherLang: false, isLongText: false },
      { key: "name", label: "Name", value: "SIA Piemērs", fromOtherLang: false, isLongText: false },
      { key: "legal_form_description_en", label: "Legal form description en", value: "Limited liability company", fromOtherLang: false, isLongText: false },
    ]);
  });
});

describe("resolveRecordFields — FR/CZ records (unpaired singles only, pairCount 0)", () => {
  it("FR: denomination_original, legal_form_en, status_en all pass through unpaired", () => {
    const frRecord: Record<string, unknown> = {
      siren: "123456789",
      denomination_original: "Société Exemple SARL",
      legal_form_en: "Limited liability company",
      status_en: "Active",
    };
    const { fields, pairCount } = resolveRecordFields(frRecord, "en");
    expect(pairCount).toBe(0);
    expect(fields.map((f) => f.key)).toEqual(["siren", "denomination_original", "legal_form_en", "status_en"]);
  });

  it("CZ: legal_form_en passes through unpaired", () => {
    const czRecord: Record<string, unknown> = {
      ico: "12345678",
      legal_form_en: "Limited liability company",
    };
    const { fields, pairCount } = resolveRecordFields(czRecord, "en");
    expect(pairCount).toBe(0);
    expect(fields.map((f) => f.key)).toEqual(["ico", "legal_form_en"]);
  });
});

describe("resolveRecordFields — GB record (zero pairs)", () => {
  it("pairCount is 0 for a record with no _en/_original fields at all", () => {
    const gbRecord: Record<string, unknown> = {
      company_number: "01234567",
      company_status: "active",
      incorporation_date: "2010-05-04",
      website: "https://example.co.uk",
    };
    const { pairCount } = resolveRecordFields(gbRecord, "en");
    expect(pairCount).toBe(0);
  });
});

describe("resolveRecordFields — lineage suffix exclusion (_language, _translated_at, _translation_provider, _translation_model)", () => {
  it("excludes all four translation-lineage suffix keys from fields and longTexts", () => {
    const record: Record<string, unknown> = {
      name: "X",
      activity_text_language: "no",
      activity_text_translated_at: "2026-01-01T00:00:00Z",
      activity_text_translation_provider: "deepl",
      activity_text_translation_model: "deepl-v2",
      source_run_id: "r1",
    };
    const { fields, longTexts } = resolveRecordFields(record, "en");
    expect(fields).toEqual([
      { key: "name", label: "Name", value: "X", fromOtherLang: false, isLongText: false },
    ]);
    expect(longTexts).toEqual([]);
  });
});

describe("resolveRecordFields — long text classification", () => {
  it("classifies articles_purpose/activity_text as long text via key-set, regardless of length", () => {
    const record = { articles_purpose_original: "short", articles_purpose_en: "short" };
    const { longTexts, fields } = resolveRecordFields(record, "en");
    expect(longTexts.map((f) => f.key)).toEqual(["articles_purpose"]);
    expect(fields).toEqual([]);
  });

  it("classifies any other field as long text once its resolved value exceeds 240 characters", () => {
    const longValue = "A".repeat(250);
    const record = { name: "X", business_description: longValue };
    const { fields, longTexts } = resolveRecordFields(record, "en");
    expect(fields.map((f) => f.key)).toEqual(["name"]);
    expect(longTexts).toEqual([
      { key: "business_description", label: "Business description", value: longValue, fromOtherLang: false, isLongText: true },
    ]);
  });

  it("keeps a field short (240 chars exactly) out of longTexts", () => {
    const exactly240 = "B".repeat(240);
    const record = { business_description: exactly240 };
    const { fields, longTexts } = resolveRecordFields(record, "en");
    expect(longTexts).toEqual([]);
    expect(fields.map((f) => f.key)).toEqual(["business_description"]);
  });
});

describe("keyFacts", () => {
  it("prefers legal_form_description over legal_form, resolved per lang", () => {
    const record: Record<string, unknown> = {
      legal_form_description_original: "Aksjeselskap",
      legal_form_description_en: "Private limited company",
      legal_form_original: "AS",
      legal_form_en: "PLC",
      status_original: "Aktivt",
      status_en: "Active",
      registration_date: "2015-03-02",
      primary_website_url: "https://example.no",
    };

    expect(keyFacts(record, "en")).toEqual([
      { label: "Legal form", value: "Private limited company" },
      { label: "Status", value: "Active" },
      { label: "Registered", value: "2015-03-02" },
      { label: "Website", value: "https://example.no", href: "https://example.no" },
    ]);

    expect(keyFacts(record, "original").find((f) => f.label === "Legal form")).toEqual({
      label: "Legal form",
      value: "Aksjeselskap",
    });
  });

  it("falls back through status candidates: lifecycle_status when status is absent", () => {
    const record = { lifecycle_status: "ACTIVE" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Status", value: "ACTIVE" }]);
  });

  it("falls back through status candidates: company_status (GB) when status/lifecycle_status are absent", () => {
    const record = { company_status: "active" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Status", value: "active" }]);
  });

  it("surfaces an unpaired status_en (BR/FR) when no other status candidate is present", () => {
    const record = { status_en: "Active" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Status", value: "Active" }]);
  });

  it("surfaces an unpaired legal_form_en (FR/CZ) when no paired legal form candidate is present", () => {
    const record = { legal_form_en: "Limited liability company" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Legal form", value: "Limited liability company" }]);
  });

  it("surfaces an unpaired legal_form_description_en (LV) when no paired legal form candidate is present", () => {
    const record = { legal_form_description_en: "Limited liability company" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Legal form", value: "Limited liability company" }]);
  });

  it("picks up a registered date from any candidate key (registered_date, LV-style)", () => {
    const record = { registered_date: "2018-07-11" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Registered", value: "2018-07-11" }]);
  });

  it("picks up a registered date from incorporation_date (GB/SE-style) when registration_date is absent", () => {
    const record = { incorporation_date: "2010-05-04" };
    expect(keyFacts(record, "en")).toEqual([{ label: "Registered", value: "2010-05-04" }]);
  });

  it("prefers primary_website_url over website when both present", () => {
    const record = { primary_website_url: "https://acme.example", website: "https://ignored.example" };
    expect(keyFacts(record, "en")).toEqual([
      { label: "Website", value: "https://acme.example", href: "https://acme.example" },
    ]);
  });

  it("falls back to website when primary_website_url is absent", () => {
    const record = { website: "https://acme.example" };
    expect(keyFacts(record, "en")).toEqual([
      { label: "Website", value: "https://acme.example", href: "https://acme.example" },
    ]);
  });

  it("skips absent facts entirely rather than emitting empty placeholders", () => {
    expect(keyFacts({ name: "X" }, "en")).toEqual([]);
  });

  it("skips a fact whose only candidate value is an empty string", () => {
    expect(keyFacts({ status: "", website: "" }, "en")).toEqual([]);
  });
});
