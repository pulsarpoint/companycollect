import { describe, expect, it } from "vitest";
import {
  formatFieldValue,
  humanizeFieldKey,
  isLineageKey,
  splitFields,
} from "~/components/detail/fields";

describe("humanizeFieldKey", () => {
  it("titles snake_case and upcases acronyms", () => {
    expect(humanizeFieldKey("operating_revenue_amount_usd")).toBe(
      "Operating revenue amount USD",
    );
    expect(humanizeFieldKey("business_id")).toBe("Business ID");
    expect(humanizeFieldKey("source_url")).toBe("Source URL");
    expect(humanizeFieldKey("name")).toBe("Name");
  });
});

describe("formatFieldValue", () => {
  it("maps empty to null (caller renders dash)", () => {
    expect(formatFieldValue("name", null)).toBeNull();
    expect(formatFieldValue("name", "")).toBeNull();
  });
  it("renders flag keys as yes/no", () => {
    expect(formatFieldValue("is_parent_company", 0)).toBe("no");
    expect(formatFieldValue("is_not_audited", 1)).toBe("yes");
    expect(formatFieldValue("opted_out_audit", "1")).toBe("yes");
  });
  it("groups numbers, passes strings through", () => {
    expect(formatFieldValue("total_assets_amount_original", 663788)).toBe("663,788");
    expect(formatFieldValue("accounts_type", "SELSKAP")).toBe("SELSKAP");
  });
});

describe("isLineageKey / splitFields", () => {
  it("classifies lineage vs visible, keeps source_url visible", () => {
    expect(isLineageKey("source_run_id")).toBe(true);
    expect(isLineageKey("source_url")).toBe(false);
    expect(isLineageKey("resolved_at")).toBe(true);
    expect(isLineageKey("name_normalized")).toBe(true);
    expect(isLineageKey("name")).toBe(false);
  });
  it("classifies translation-lineage suffixes as lineage", () => {
    expect(isLineageKey("activity_text_language")).toBe(true);
    expect(isLineageKey("activity_text_translated_at")).toBe(true);
    expect(isLineageKey("activity_text_translation_provider")).toBe(true);
    expect(isLineageKey("activity_text_translation_model")).toBe(true);
    expect(isLineageKey("legal_form")).toBe(false);
  });
  it("splits a record preserving order", () => {
    const { visible, lineage } = splitFields({
      name: "X", source_run_id: "r1", source_url: "https://a", resolved_at: "t",
    });
    expect(visible.map(([k]) => k)).toEqual(["name", "source_url"]);
    expect(lineage.map(([k]) => k)).toEqual(["source_run_id", "resolved_at"]);
  });
});
