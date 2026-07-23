import { describe, expect, it } from "vitest";
import { buildUnifiedColumns, formatRevenueUsd } from "~/components/data-table/unified-columns";

describe("formatRevenueUsd", () => {
  it("formats large values compactly with the fiscal year", () => {
    expect(formatRevenueUsd(1_234_567, 2024)).toBe("$1.2M (2024)");
  });

  it("renders an em dash when revenue is null", () => {
    expect(formatRevenueUsd(null, null)).toBe("—");
  });

  it("renders an em dash when revenue is undefined", () => {
    expect(formatRevenueUsd(undefined, undefined)).toBe("—");
  });

  it("formats small values without compacting, still shows the year", () => {
    expect(formatRevenueUsd(950, 2023)).toBe("$950 (2023)");
  });

  it("omits the year suffix when fiscal year is null but revenue is present", () => {
    expect(formatRevenueUsd(5_000_000, null)).toBe("$5M");
  });
});

describe("buildUnifiedColumns", () => {
  it("includes the country column by default", () => {
    expect(buildUnifiedColumns("country", "asc").map((column) => column.id)).toContain("country");
  });

  it("omits the country column for a locked-country route", () => {
    expect(
      buildUnifiedColumns("name", "asc", { showCountry: false }).map((column) => column.id),
    ).toEqual(["name", "industry", "revenue"]);
  });
});
