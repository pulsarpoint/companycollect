import { beforeEach, describe, expect, it, vi } from "vitest";

const chQuery = vi.fn();

vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: (...args: unknown[]) => chQuery(...args),
}));

const { loadEsefCountryCodes } = await import("~/lib/esef-countries.server");

beforeEach(() => {
  chQuery.mockReset();
});

describe("loadEsefCountryCodes", () => {
  it("returns sorted unique ISO-2 codes found in ESEF filing documents", async () => {
    chQuery.mockResolvedValue([
      { country_iso2: " se " },
      { country_iso2: "FI" },
      { country_iso2: "SE" },
      { country_iso2: "invalid" },
    ]);

    await expect(loadEsefCountryCodes()).resolves.toEqual(["FI", "SE"]);
    expect(String(chQuery.mock.calls[0][0])).toContain("FROM esef_filings");
  });
});
