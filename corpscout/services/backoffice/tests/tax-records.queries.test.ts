// Live ClickHouse integration tests for the taxRecordsQuery detail slice.
// Kept in its own file (not queries.server.test.ts) so it can land while the
// shared test file carries in-flight edits from other source work.
import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";

const withTaxRecords = COUNTRIES.filter((c) => c.detail?.taxRecordsQuery);

describe("taxRecordsQuery", () => {
  it("finland declares a tax records query", () => {
    expect(getCountry("fi")?.detail?.taxRecordsQuery).toBeTruthy();
  });

  // Schema smoke test: every declared tax-records SQL must execute against
  // the live ClickHouse schema (id "0" matches nothing; shape is what counts).
  it.each(withTaxRecords.map((c) => [c.code, c] as const))(
    "%s tax records query executes against the live schema",
    async (_code, country) => {
      const rows = await chQuery(country.detail!.taxRecordsQuery!, { id: "0" });
      expect(rows).toEqual([]);
    },
  );

  it("finland returns tax record rows for a company that has them", async () => {
    const fi = getCountry("fi")!;
    const seed = await chQuery<{ business_id: string }>(
      `SELECT business_id FROM fi_tax_records
       WHERE taxable_income_amount_original > 0
         AND business_id IN (SELECT business_id FROM fi_companies)
       LIMIT 1`,
    );
    expect(seed.length).toBe(1);

    const detail = await getCompanyDetail(fi, seed[0].business_id);
    expect(detail).not.toBeNull();
    expect(detail!.taxRecords.length).toBeGreaterThan(0);

    const row = detail!.taxRecords[0];
    expect(row.tax_year).toMatch(/^20\d{2}$/);
    expect(row.currency).toBe("EUR");
    // Newest first, one row per tax year.
    const years = detail!.taxRecords.map((r) => r.tax_year);
    expect([...years].sort().reverse()).toEqual(years);
    expect(new Set(years).size).toBe(years.length);
    // USD companion filled whenever the original is (FX coverage is complete
    // for EUR at year-end dates).
    for (const r of detail!.taxRecords) {
      if (r.taxable_income_amount_original != null) {
        expect(r.taxable_income_amount_usd).not.toBeNull();
      }
    }
  });

  it("countries without a tax records query return an empty slice", async () => {
    const ee = getCountry("ee")!;
    expect(ee.detail?.taxRecordsQuery).toBeUndefined();
    const seed = await chQuery<{ id: string }>(
      `SELECT ${ee.idColumn} AS id FROM ${ee.companiesTable} LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, seed[0].id);
    expect(detail!.taxRecords).toEqual([]);
  });
});
