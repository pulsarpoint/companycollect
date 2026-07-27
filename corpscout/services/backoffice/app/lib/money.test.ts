import { describe, expect, it } from "vitest";
import { formatMoneyField } from "./money";

describe("formatMoneyField", () => {
  it("formats the monetary columns each register publishes", () => {
    expect(formatMoneyField("awarded_amount_original", "1234567.89")).toBe(
      "1,234,567.89",
    );
    expect(formatMoneyField("estimated_value_amount_usd", 250000)).toBe("250,000");
    expect(formatMoneyField("valor_global", "98765.5")).toBe("98,765.5");
    expect(formatMoneyField("framework_maximum_amount_usd", "1000000.00")).toBe(
      "1,000,000",
    );
  });

  it("leaves companion columns that share money stems untouched", () => {
    expect(formatMoneyField("estimated_value_currency", "EUR")).toBeNull();
    expect(formatMoneyField("fx_rate_to_usd", "0.0951234567")).toBeNull();
    expect(formatMoneyField("notice_value_source_field", "award.value")).toBeNull();
  });

  it("requires the value itself to be numeric", () => {
    expect(formatMoneyField("notice_value_amount_original", "")).toBeNull();
    expect(formatMoneyField("notice_value_amount_original", "n/a")).toBeNull();
    expect(formatMoneyField("procurement_value_amount_usd", null)).toBeNull();
  });

  it("ignores non-monetary columns entirely", () => {
    expect(formatMoneyField("cpv_code", "45000000")).toBeNull();
    expect(formatMoneyField("notice_id", "20260123")).toBeNull();
    expect(formatMoneyField("award_year", "2026")).toBeNull();
    expect(formatMoneyField("lot_count", "12")).toBeNull();
  });

  it("declines values it cannot format without precision loss", () => {
    expect(
      formatMoneyField("total_value_amount_original", "123456789012345678901234.56"),
    ).toBeNull();
  });
});
