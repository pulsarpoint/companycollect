import { describe, expect, it } from "vitest";
import { qualityFlagLabel, buildAmountFields } from "~/components/detail/countries/no-financials";

const row = {
  currency: "NOK",
  fx_rate_to_usd: 0.0992,
  total_assets_amount_original: 38582,
  total_assets_amount_usd: null,
  current_assets_amount_original: 4266,
  current_assets_amount_usd: 423.25,
  equity_amount_original: null,
  equity_amount_usd: null,
};

describe("buildAmountFields", () => {
  it("appends the currency to originals", () => {
    const fields = new Map(buildAmountFields(row, ["total_assets_amount_original"]));
    expect(fields.get("total_assets_amount_original")).toBe("38,582 NOK");
  });

  it("keeps stored usd values unmarked", () => {
    const fields = new Map(buildAmountFields(row, ["current_assets_amount_usd"]));
    expect(fields.get("current_assets_amount_usd")).toBe("423.25 USD");
  });

  it("derives missing usd from the fx rate with the ≈ marker", () => {
    const fields = new Map(buildAmountFields(row, ["total_assets_amount_usd"]));
    expect(fields.get("total_assets_amount_usd")).toBe("≈ 3,827.33 USD");
  });

  it("returns null when original and usd are both absent", () => {
    const fields = new Map(buildAmountFields(row, ["equity_amount_usd"]));
    expect(fields.get("equity_amount_usd")).toBeNull();
  });

  it("does not derive when the fx rate is missing", () => {
    const noFx = { ...row, fx_rate_to_usd: null };
    const fields = new Map(buildAmountFields(noFx, ["total_assets_amount_usd"]));
    expect(fields.get("total_assets_amount_usd")).toBeNull();
  });
});

describe("qualityFlagLabel", () => {
  it("returns null for clean statements", () => {
    expect(qualityFlagLabel({ quality_flag: "" })).toBeNull();
    expect(qualityFlagLabel({})).toBeNull();
  });

  it("humanizes the implausible-magnitude flag", () => {
    expect(qualityFlagLabel({ quality_flag: "implausible_magnitude" })).toBe(
      "implausible values — likely source filing error",
    );
  });

  it("passes unknown flags through raw", () => {
    expect(qualityFlagLabel({ quality_flag: "some_future_flag" })).toBe("some_future_flag");
  });
});
