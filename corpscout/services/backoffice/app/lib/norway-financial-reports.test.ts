import { describe, expect, it } from "vitest";
import {
  formatFileSize,
  parseWarnings,
  statementTypeLabel,
} from "~/lib/norway-financial-reports";

describe("Norway financial report presentation", () => {
  it("turns pipeline statement keys into readable labels", () => {
    expect(statementTypeLabel("income_statement")).toBe("Income statement");
    expect(statementTypeLabel("balance_sheet")).toBe("Balance sheet");
    expect(statementTypeLabel("other")).toBe("Other");
  });

  it("formats known PDF sizes and leaves missing sizes empty", () => {
    expect(formatFileSize(1_387_288)).toBe("1.3 MB");
    expect(formatFileSize(0)).toBeNull();
  });

  it("parses warning arrays without exposing malformed JSON failures", () => {
    expect(parseWarnings('["ocr_fallback","missing_period"]')).toEqual([
      "ocr_fallback",
      "missing_period",
    ]);
    expect(parseWarnings("not-json")).toEqual(["not-json"]);
    expect(parseWarnings("[]")).toEqual([]);
  });
});
