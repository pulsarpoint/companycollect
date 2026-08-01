// Live ClickHouse integration tests for France's company-detail queries.
// Own file, so unrelated in-flight source work does not collide here.
import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";

/** FNAC DARTY. Chosen because one company exercises all three sections:
 * 8 filed fiscal years, 7 contract wins, and a Wikidata match (Q47088340). */
const FNAC = "055800296";

describe("France public contracts", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.publicContractsQuery).toBeTruthy();
  });

  it("reads France's own contracts view", () => {
    expect(getCountry("fr")!.detail!.publicContractsQuery).toContain(
      "FROM fr_government_contracts",
    );
  });

  it("returns nothing for an id that cannot exist", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.publicContractsQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("returns canonical rows for a company with wins", async () => {
    const rows = await chQuery<{
      source: string;
      amount_original: number | null;
      notice_amount_original: number | null;
    }>(getCountry("fr")!.detail!.publicContractsQuery!, { id: FNAC });
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].source).toBe("france_decp_procurement");
    // DECP publishes the notice total, never a per-winner split. The section
    // labels the notice figure rather than passing it off as this company's
    // share -- if per-winner values ever appear, this assertion is the signal
    // to revisit that labelling, not to delete the check.
    expect(rows.every((r) => r.amount_original == null)).toBe(true);
    expect(rows.some((r) => r.notice_amount_original != null)).toBe(true);
  });
});

describe("France Wikidata", () => {
  it("declares both queries", () => {
    expect(getCountry("fr")?.detail?.wikidataQuery).toBeTruthy();
    expect(getCountry("fr")?.detail?.wikidataPeopleQuery).toBeTruthy();
  });

  it("matches FNAC DARTY on its siren", async () => {
    const rows = await chQuery<{ wikidata_id: string; official_name: string }>(
      getCountry("fr")!.detail!.wikidataQuery!,
      { id: FNAC },
    );
    expect(rows.length).toBe(1);
    expect(rows[0].wikidata_id).toBe("Q47088340");
  });

  it("returns nothing for an unmatched company", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.wikidataQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("people query executes against the live schema", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.wikidataPeopleQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });
});

describe("France financial metrics", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.financialMetricsQuery).toBeTruthy();
  });

  it("returns nothing for an id that cannot exist", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.financialMetricsQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("returns one row per fiscal year, newest first", async () => {
    // 531615169 files under more than one balance type in the same year --
    // 41,055 (siren, fiscal_year) pairs do. Two rows for one year would put
    // the year in the table twice, and it is invisible until someone opens
    // exactly such a company.
    const rows = await chQuery<{ fiscal_year: string; balance_type: string }>(
      getCountry("fr")!.detail!.financialMetricsQuery!,
      { id: "531615169" },
    );
    expect(rows.length).toBeGreaterThan(1);
    expect(new Set(rows.map((r) => r.fiscal_year)).size).toBe(rows.length);
    const years = rows.map((r) => Number(r.fiscal_year));
    expect(years).toEqual([...years].sort((a, b) => b - a));
  });

  it("carries the ratio suite and the confidentiality status", async () => {
    const rows = await chQuery<{
      currency: string;
      confidentiality: string;
      revenue_original: number | null;
      ebitda_margin_percent: number | null;
      customer_payment_days: number | null;
    }>(getCountry("fr")!.detail!.financialMetricsQuery!, { id: "055800296" });
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].currency).toBe("EUR");
    expect(rows[0].confidentiality).not.toBe("");
    expect(rows.some((r) => r.ebitda_margin_percent != null)).toBe(true);
    expect(rows.some((r) => r.customer_payment_days != null)).toBe(true);
  });
});

describe("France contract summary", () => {
  it("declares the query", () => {
    expect(getCountry("fr")?.detail?.contractSummaryQuery).toBeTruthy();
  });

  it("returns nothing for a company with no awards", async () => {
    const rows = await chQuery(getCountry("fr")!.detail!.contractSummaryQuery!, {
      id: "0",
    });
    expect(rows).toEqual([]);
  });

  it("summarises a company with awards", async () => {
    const rows = await chQuery<{
      award_count: number | string;
      total_value_usd: number | null;
      last_award_date: string;
      sources: string;
    }>(getCountry("fr")!.detail!.contractSummaryQuery!, { id: "055800296" });
    expect(rows.length).toBe(1);
    expect(Number(rows[0].award_count)).toBeGreaterThan(0);
    expect(rows[0].sources).toBe("france_decp_procurement");
    expect(rows[0].last_award_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    // France publishes no per-winner value, so the summary's USD total is NULL
    // for all 99,287 companies. The header must not print "0" for it.
    expect(rows[0].total_value_usd).toBeNull();
  });
});
