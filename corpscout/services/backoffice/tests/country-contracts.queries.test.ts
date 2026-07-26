import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server";
import { getContractDetail, getCountryContracts, hasContracts } from "~/lib/contracts.server";

describe("country contracts", () => {
  it("lists contracts, not winner rows", async () => {
    const fi = getCountry("fi")!;
    const contracts = await getCountryContracts(fi, { limit: 50 });
    expect(contracts.length).toBeGreaterThan(0);

    for (const row of contracts) {
      expect(row.contract_ref).not.toBe("");
      expect(row.sources.length).toBeGreaterThan(0);
      // A contract has at least one winner, or it would not be an award.
      expect(row.winner_count).toBeGreaterThan(0);
    }
    // Newest first.
    const dates = contracts.map((r) => r.contract_date);
    expect([...dates].sort().reverse()).toEqual(dates);
  });

  it("opens a contract with every source it was published in", async () => {
    const fi = getCountry("fi")!;
    // Finland's registers cross-reference each other through Hilma's
    // ted_number, so a contract in both must show both -- that link matching
    // nothing is exactly the bug this guards.
    const [seed] = await chQuery<{ contract_key: string }>(
      `SELECT contract_key FROM fi_government_contracts
       WHERE contract_key != ''
       GROUP BY contract_key
       HAVING uniqExact(source_slug) > 1
       LIMIT 1`,
    );
    expect(seed).toBeDefined();

    const detail = await getContractDetail(fi, seed.contract_key);
    expect(detail).not.toBeNull();

    const sources = new Set(detail!.rows.map((r) => r.source));
    expect(sources.size).toBeGreaterThan(1);
    expect(sources).toContain("finland_hilma_procurement");
    expect(sources).toContain("ted_procurement");

    // Each source carries its own document, and its own raw record.
    for (const row of detail!.rows) {
      expect(row.source_url).toMatch(/^https:\/\//);
    }
    expect(detail!.sourceRecords.length).toBeGreaterThan(1);
    for (const record of detail!.sourceRecords) {
      expect(Object.keys(record.fields).length).toBeGreaterThan(5);
    }
  });

  it("names winners even when they have no company id to link to", async () => {
    const fi = getCountry("fi")!;
    const [seed] = await chQuery<{ contract_ref: string }>(
      `SELECT if(contract_key != '', contract_key, contract_id) AS contract_ref
       FROM fi_government_contracts
       GROUP BY contract_ref
       HAVING uniqExact(company_id) > 2
       LIMIT 1`,
    );
    const detail = await getContractDetail(fi, seed.contract_ref);

    expect(detail!.rows.length).toBeGreaterThan(2);
    for (const row of detail!.rows) {
      expect(row.winner_name === "" && row.company_id === "").toBe(false);
    }
  });

  it("countries with no contracts view are not offered the tab", () => {
    expect(hasContracts(getCountry("fi")!)).toBe(true);
    expect(hasContracts(getCountry("ee")!)).toBe(false);
  });
});
