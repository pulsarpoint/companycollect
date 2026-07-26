// Live ClickHouse integration tests for the canonical publicContractsQuery
// slice. Own file (not queries.server.test.ts) to stay clear of in-flight
// edits from other source work.
import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";

const withContracts = COUNTRIES.filter((c) => c.detail?.publicContractsQuery);

describe("publicContractsQuery", () => {
  it("finland declares a public contracts query", () => {
    expect(getCountry("fi")?.detail?.publicContractsQuery).toBeTruthy();
  });

  it("each country reads its own contracts view", () => {
    // The country is in the view name, so the query needs no country filter --
    // and must not silently read another country's rows.
    for (const country of withContracts) {
      expect(country.detail!.publicContractsQuery).toContain(
        `FROM ${country.code}_government_contracts`,
      );
    }
  });

  it.each(withContracts.map((c) => [c.code, c] as const))(
    "%s public contracts query executes against the live schema",
    async (_code, country) => {
      const rows = await chQuery(country.detail!.publicContractsQuery!, { id: "0" });
      expect(rows).toEqual([]);
    },
  );

  it("returns canonical rows for a company with contract wins", async () => {
    const fi = getCountry("fi")!;
    const seed = await chQuery<{ business_id: string }>(
      `SELECT winner_business_id AS business_id
       FROM fi_hilma_notice_winners
       WHERE winner_business_id != '' AND is_award = 1
         AND winner_business_id IN (SELECT business_id FROM fi_companies)
       GROUP BY winner_business_id
       ORDER BY count() DESC
       LIMIT 1`,
    );
    expect(seed.length).toBe(1);

    const detail = await getCompanyDetail(fi, seed[0].business_id);
    expect(detail).not.toBeNull();
    const contracts = detail!.publicContracts;
    expect(contracts.length).toBeGreaterThan(0);

    for (const row of contracts) {
      // The canonical slugs the country view emits, not per-page labels.
      expect(["finland_hilma_procurement", "ted_procurement"]).toContain(row.source);
      expect(row.notice_ref).not.toBe("");
      expect(row.contract_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      // Every Finnish source publishes a per-notice address, so a row that
      // cannot be traced back to its document is a bug.
      expect(row.source_url).toMatch(/^https:\/\//);
      // Hilma has no per-winner amount. Presenting its notice total as this
      // company's share is exactly the error this separation prevents.
      if (row.source === "finland_hilma_procurement") {
        expect(row.amount_original).toBeNull();
      }
    }
    // Newest first.
    const dates = contracts.map((r) => r.contract_date);
    expect([...dates].sort().reverse()).toEqual(dates);
  });

  it("companies without wins get an empty slice", async () => {
    const fi = getCountry("fi")!;
    const seed = await chQuery<{ business_id: string }>(
      `SELECT business_id FROM fi_companies
       WHERE business_id NOT IN (
         SELECT winner_business_id FROM fi_hilma_notice_winners
       ) AND business_id NOT IN (
         SELECT winner_national_id FROM ted_notice_winners
       )
       LIMIT 1`,
    );
    const detail = await getCompanyDetail(fi, seed[0].business_id);
    expect(detail!.publicContracts).toEqual([]);
  });
});
