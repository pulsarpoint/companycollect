import { describe, expect, it, vi } from "vitest";
import { getCountryMacroIndicators } from "~/lib/world-bank.server";

describe("getCountryMacroIndicators", () => {
  it("reads each indicator's own latest year from ClickHouse", async () => {
    const queryImpl = vi.fn(
      async (_sql: string, _params?: Record<string, unknown>) => [
        { indicator_code: "NY.GDP.MKTP.CD", value: 500, year: 2023 },
        { indicator_code: "NE.EXP.GNFS.CD", value: 300, year: 2022 },
        { indicator_code: "NE.IMP.GNFS.CD", value: 200, year: 2024 },
      ],
    );

    await expect(
      getCountryMacroIndicators("NO", { queryImpl }),
    ).resolves.toEqual({
      gdp: { value: 500, year: 2023 },
      exports: { value: 300, year: 2022 },
      imports: { value: 200, year: 2024 },
    });
    expect(queryImpl).toHaveBeenCalledOnce();
    const [sql, params] = queryImpl.mock.calls[0];
    expect(sql).toContain("corpscout.world_bank_macro_observations");
    expect(sql).toContain("argMax(value, year)");
    expect(params).toEqual({
      country: "no",
      indicators: ["NY.GDP.MKTP.CD", "NE.EXP.GNFS.CD", "NE.IMP.GNFS.CD"],
    });
  });

  it("returns null for indicators that have no observation", async () => {
    const queryImpl = vi.fn(
      async (_sql: string, _params?: Record<string, unknown>) => [
        { indicator_code: "NY.GDP.MKTP.CD", value: 42, year: 2023 },
      ],
    );

    await expect(
      getCountryMacroIndicators("ee", { queryImpl }),
    ).resolves.toEqual({
      gdp: { value: 42, year: 2023 },
      exports: null,
      imports: null,
    });
  });
});
