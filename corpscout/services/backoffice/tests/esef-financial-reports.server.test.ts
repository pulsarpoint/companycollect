import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";
import { getEsefFinancialReport } from "~/lib/esef-financial-reports.server";
import { getCompanyFinancialDetail } from "~/lib/queries.server";

const BOTH_SOURCES_COMPANY = "5567081699";
const SAGAX = "5565200028";

describe("Sweden source-grouped financial reports", () => {
  it("keeps Bolagsverket annual accounts and ESEF as separate source datasets", async () => {
    const sweden = getCountry("se")!;
    const detail = await getCompanyFinancialDetail(sweden, BOTH_SOURCES_COMPANY);
    const registry = detail.financialSources.find(
      (source) => source.kind === "registry",
    );
    const esef = detail.financialSources.find(
      (source) => source.kind === "esef",
    );

    expect(sweden.detail?.financialSources?.map((source) => source.id)).toEqual(
      ["bolagsverket-annual-accounts", "esef"],
    );
    expect(registry?.kind).toBe("registry");
    expect(esef?.kind).toBe("esef");
    if (registry?.kind !== "registry" || esef?.kind !== "esef") {
      throw new Error("expected separate registry and ESEF sources");
    }
    expect(registry.financials.length).toBeGreaterThan(0);
    expect(esef.financials.length).toBeGreaterThan(0);
    expect(esef.financials[0].source_document_id).not.toBe("");
    expect(esef.financials[0].source_fact_count).toBeGreaterThan(0);
  }, 20_000);

  it("exposes Sagax rental-income facts even when standardized revenue is absent", async () => {
    const sweden = getCountry("se")!;
    const detail = await getCompanyFinancialDetail(sweden, SAGAX);
    const esef = detail.financialSources.find(
      (source) => source.kind === "esef",
    );
    const filing =
      esef?.kind === "esef"
        ? esef.financials.find((row) => row.fiscal_year === "2024")
        : undefined;

    expect(filing).toBeTruthy();
    const report = await getEsefFinancialReport(
      "se",
      SAGAX,
      filing!.source_document_id,
    );

    expect(report).not.toBeNull();
    expect(
      report!.facts.some(
        (fact) =>
          fact.conceptQname === "ifrs-full:RentalIncomeFromInvestmentProperty",
      ),
    ).toBe(true);
    const monetaryFact = report!.facts.find(
      (fact) =>
        fact.valueKind === "monetary" &&
        fact.currency === "SEK" &&
        fact.amountOriginal !== null,
    );
    const shareFact = report!.facts.find(
      (fact) => fact.unit === "xbrli:shares",
    );

    expect(monetaryFact?.amountUsd).not.toBeNull();
    expect(monetaryFact?.fxRateDate).not.toBe("");
    expect(shareFact?.amountUsd).toBeNull();
  }, 20_000);
});
