import { beforeEach, describe, expect, test, vi } from "vitest";

const chQuery = vi.fn();
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: (...args: unknown[]) => chQuery(...args),
}));

const { getCountry } = await import("~/lib/countries");
const {
  getCompanyFacts,
  getCompanyFinancialDetail,
  getCompanyFinancials,
  getFactsDocument,
} = await import("~/lib/queries.server");

const sweden = getCountry("se");
if (!sweden) throw new Error("Sweden country configuration is missing");

beforeEach(() => chQuery.mockReset());

describe("getCompanyFinancials", () => {
  test("uses reported metrics while provenance columns are not migrated", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 0 }]).mockResolvedValueOnce([]);

    await getCompanyFinancials(sweden, "5569658767");

    expect(chQuery).toHaveBeenCalledTimes(2);
    expect(String(chQuery.mock.calls[0][0])).toContain("FROM system.columns");
    const financialsSql = String(chQuery.mock.calls[1][0]);
    expect(financialsSql).toContain("FROM se_bolagsverket_financial_metrics");
    expect(financialsSql).not.toContain("observation_kind");
    expect(financialsSql).toContain(
      "toString(fiscal_year) AS source_fiscal_year",
    );
  });

  test("uses unified metrics after provenance columns are migrated", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 1 }]).mockResolvedValueOnce([]);

    await getCompanyFinancials(sweden, "5569658767");

    const financialsSql = String(chQuery.mock.calls[1][0]);
    expect(financialsSql).toContain("observation_kind = 'reported'");
    expect(financialsSql).toContain("source_fiscal_year DESC");
  });
});

describe("getCompanyFinancialDetail", () => {
  test("loads the two Sweden source views independently", async () => {
    chQuery.mockImplementation((sqlValue: unknown) => {
      const sql = String(sqlValue);
      if (sql.includes("se_financials_bolagsverket_current")) {
        return Promise.resolve([
          {
            source_id: "bolagsverket-annual-accounts",
            accounting_scope: "standalone",
            source_document_id: "registry-document",
            source_record_uids: [],
            source_url: "",
            viewer_url: "",
            fiscal_year: "2024",
            currency: "SEK",
            revenue_amount_original: 1,
            revenue_amount_usd: 0.1,
            net_result_amount_original: 1,
            net_result_amount_usd: 0.1,
            total_assets_amount_original: 1,
            total_assets_amount_usd: 0.1,
            equity_amount_original: 1,
            equity_amount_usd: 0.1,
            employees: 1,
          },
        ]);
      }
      if (sql.includes("se_financials_esef_current")) {
        return Promise.resolve([
          {
            source_id: "esef",
            accounting_scope: "consolidated_ifrs",
            source_document_id: "esef-document",
            source_record_uids: [],
            source_url: "https://example.test/source",
            viewer_url: "https://example.test/viewer",
            fiscal_year: "2024",
            currency: "SEK",
            revenue_amount_original: 2,
            revenue_amount_usd: 0.2,
            net_result_amount_original: 2,
            net_result_amount_usd: 0.2,
            total_assets_amount_original: 2,
            total_assets_amount_usd: 0.2,
            equity_amount_original: 2,
            equity_amount_usd: 0.2,
            employees: 2,
          },
        ]);
      }
      if (sql.includes("AS ready")) return Promise.resolve([{ ready: 0 }]);
      return Promise.resolve([]);
    });

    const detail = await getCompanyFinancialDetail(sweden, "5569658767");
    const queries = chQuery.mock.calls.map((call) => String(call[0]));

    expect(detail.financialSources.map((source) => source.id)).toEqual([
      "bolagsverket-annual-accounts",
      "esef",
    ]);
    expect(
      detail.financialSources.map((source) => source.financials[0].source_id),
    ).toEqual(["bolagsverket-annual-accounts", "esef"]);
    expect(
      queries.some((sql) => sql.includes("se_financials_bolagsverket_current")),
    ).toBe(true);
    expect(
      queries.some((sql) => sql.includes("se_financials_esef_current")),
    ).toBe(true);
    expect(queries.some((sql) => sql.includes("WITH versions AS"))).toBe(false);
  });
});

describe("Sweden financial facts queries", () => {
  test("uses reported metrics ordering before provenance columns are migrated", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 0 }]).mockResolvedValueOnce([]);

    await getCompanyFacts(sweden, "5569658767", 2025);

    const factsSql = String(chQuery.mock.calls[1][0]);
    expect(factsSql).toContain("FROM se_financial_facts");
    expect(factsSql).not.toContain("observation_kind");
    expect(factsSql).not.toContain("source_fiscal_year");
    expect(factsSql).toContain("source_record_id DESC");
  });

  test("keeps provenance ordering after the metrics migration", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 1 }]).mockResolvedValueOnce([]);

    await getCompanyFacts(sweden, "5569658767", 2025);

    const factsSql = String(chQuery.mock.calls[1][0]);
    expect(factsSql).toContain("observation_kind = 'reported'");
    expect(factsSql).toContain("source_fiscal_year DESC");
  });

  test("uses reported document ordering before provenance columns are migrated", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 0 }]).mockResolvedValueOnce([]);

    await getFactsDocument(sweden, "5569658767", 2025);

    const documentSql = String(chQuery.mock.calls[1][0]);
    expect(documentSql).toContain("xhtml_object_key AS object_key");
    expect(documentSql).not.toContain("observation_kind");
    expect(documentSql).not.toContain("source_fiscal_year");
    expect(documentSql).toContain("source_record_id DESC");
  });

  test("keeps document provenance ordering after the metrics migration", async () => {
    chQuery.mockResolvedValueOnce([{ ready: 1 }]).mockResolvedValueOnce([]);

    await getFactsDocument(sweden, "5569658767", 2025);

    const documentSql = String(chQuery.mock.calls[1][0]);
    expect(documentSql).toContain("observation_kind = 'reported'");
    expect(documentSql).toContain("source_fiscal_year DESC");
  });
});
