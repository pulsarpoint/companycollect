import { describe, expect, it } from "vitest";

import { eformsMoney, noticeText } from "./notice-money";

describe("eformsMoney", () => {
  it("formats the published figure with its currency", () => {
    expect(
      eformsMoney({ value_amount_original: "2000000000", value_currency: "NOK" }, "value"),
    ).toEqual({ original: "2,000,000,000 NOK", usd: null });
  });

  it("carries the USD twin when the FX step filled it", () => {
    expect(
      eformsMoney(
        {
          value_amount_original: "2000000000",
          value_currency: "NOK",
          value_amount_usd: "196512307.05",
        },
        "value",
      ),
    ).toEqual({ original: "2,000,000,000 NOK", usd: "$196,512,307.05" });
  });

  it("returns null when the register publishes no figure for that term", () => {
    // Not zero, not a dash: the term is absent, and the caller drops the row.
    expect(eformsMoney({ value_amount_original: null }, "value")).toBeNull();
    expect(eformsMoney({}, "value")).toBeNull();
    expect(eformsMoney({ value_amount_original: "" }, "value")).toBeNull();
  });

  it("shows an unparseable amount verbatim rather than NaN", () => {
    expect(eformsMoney({ value_amount_original: "n/a" }, "value")).toEqual({
      original: "n/a",
      usd: null,
    });
  });

  it("omits the currency when the register did not state one", () => {
    expect(eformsMoney({ value_amount_original: "1500" }, "value")).toEqual({
      original: "1,500",
      usd: null,
    });
  });
});

describe("noticeText", () => {
  it("treats blank and missing alike", () => {
    expect(noticeText(null)).toBeNull();
    expect(noticeText(undefined)).toBeNull();
    expect(noticeText("   ")).toBeNull();
    expect(noticeText("")).toBeNull();
  });

  it("trims what it keeps", () => {
    expect(noticeText("  Bærum kommune ")).toBe("Bærum kommune");
    expect(noticeText(0)).toBe("0");
  });
});
