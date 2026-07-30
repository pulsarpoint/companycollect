import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server";
import {
  getContractDetail,
  getCountryContractsPage,
  hasContracts,
} from "~/lib/contracts.server";

describe("country contracts", () => {
  it("lists contracts, not winner rows", async () => {
    const fi = getCountry("fi")!;
    const page = await getCountryContractsPage(fi, { pageSize: 50 });
    expect(page.total).toBeGreaterThan(0);
    expect(page.rows.length).toBeGreaterThan(0);

    for (const row of page.rows) {
      expect(row.contract_ref).not.toBe("");
      // A contract has at least one winner, or it would not be an award.
      expect(row.winner_name).not.toBe("");
      // At least the supplier shown, so the "1/N" position is well-formed.
      expect(row.supplier_count).toBeGreaterThanOrEqual(1);
    }
    // Newest first (the default sort).
    const dates = page.rows.map((r) => r.contract_date);
    expect([...dates].sort().reverse()).toEqual(dates);
  });

  it(
    "paginates and sorts server-side",
    async () => {
      const fi = getCountry("fi")!;
      const firstPage = await getCountryContractsPage(fi, { pageSize: 25, page: 1 });
      const secondPage = await getCountryContractsPage(fi, { pageSize: 25, page: 2 });
      expect(firstPage.total).toBe(secondPage.total);
      expect(firstPage.rows.length).toBe(25);
      expect(secondPage.rows.length).toBe(25);
      // No overlap between pages.
      const firstRefs = new Set(firstPage.rows.map((r) => r.contract_ref));
      for (const row of secondPage.rows) expect(firstRefs.has(row.contract_ref)).toBe(false);

      const byWinner = await getCountryContractsPage(fi, {
        pageSize: 25,
        sort: "winner",
        dir: "asc",
      });
      expect(byWinner.sort).toBe("winner");
      // Checked as monotonic (byte order), not re-sorted with localeCompare
      // and compared: ClickHouse's ORDER BY collates by byte value, which
      // disagrees with JS's locale-aware sort on case and punctuation (e.g.
      // "3M..." vs "3feR..."), and that mismatch isn't a query bug.
      const names = byWinner.rows.map((r) => r.winner_name);
      for (let i = 1; i < names.length; i++) {
        expect(names[i] >= names[i - 1]).toBe(true);
      }

      // An unrecognized sort key falls back to the default rather than erroring.
      const fallback = await getCountryContractsPage(fi, { sort: "not-a-real-key" });
      expect(fallback.sort).toBe("date");
    },
    20_000,
  );

  it("pairs the shown winner and amount from the same source", async () => {
    // Brazil is single-source, single-supplier throughout: every contract has
    // exactly one, so no "1/N" position is ever shown, and amount_original and
    // currency (when present) must never be split from each other.
    const br = getCountry("br")!;
    const page = await getCountryContractsPage(br, { pageSize: 100 });
    for (const row of page.rows) {
      expect(row.supplier_count).toBe(1);
      if (row.amount_original != null) expect(row.currency).not.toBe("");
    }
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

  it("the tab follows the views that exist, not a list in the code", async () => {
    // The point is that this is answered by the database: a country gains the
    // tab by gaining a view, with no change here. Brazil demonstrates it --
    // this test asserted `false` for it until br_government_contracts landed,
    // and the only edit needed was to this expectation.
    expect(await hasContracts(getCountry("fi")!)).toBe(true);
    expect(await hasContracts(getCountry("br")!)).toBe(true);
    // Estonia gained ee_government_contracts since this test was written --
    // exactly the "only edit needed was to this expectation" case above.
    expect(await hasContracts(getCountry("ee")!)).toBe(true);
  });
});
