import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { SeCompanyEsefView } from "~/routes/admin-se-company-esef";

const DETAIL = {
  filings: [
    {
      fxoId: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
      entityName: "Svenska Handelsbanken AB",
      periodEnd: "2023-12-31",
      fiscalYear: 2023,
      factCount: 541,
      noteCount: 144,
      errorCount: 0,
      warningCount: 0,
      viewerUrl: "https://example.test/viewer",
      sourceUrl: "",
      packageUrl: "",
    },
    {
      fxoId: "NHBDILHZTYCNBV5UYZ31-2024-12-31-ESEF-SE-0",
      entityName: "Svenska Handelsbanken AB",
      periodEnd: "2024-12-31",
      fiscalYear: 2024,
      factCount: 0,
      noteCount: 0,
      errorCount: 0,
      warningCount: 0,
      viewerUrl: "",
      sourceUrl: "",
      packageUrl: "",
    },
  ],
  information: [
    {
      fiscalYear: 2023,
      extractionStatus: "enriched",
      companyDescription: "Handelsbanken is a Swedish credit institution.",
      descriptionLanguage: "en",
      descriptionConfidence: 0.9,
      productsAndServicesJson:
        '[{"name":"Financing (finansiering)","confidence":0.9}]',
      customerMarketsJson: '[{"name":"Corporate customers","confidence":0.9}]',
      operatingGeographiesJson: '[{"name":"Sweden","confidence":0.9}]',
      businessSegmentsJson: '[{"name":"Capital Markets","confidence":0.95}]',
      materialGroupRelationshipsJson: "[]",
    },
  ],
  people: [
    {
      fiscalYear: 2023,
      name: "Carina Åkerström",
      role: "Chief Executive Officer (verkställande direktör)",
      roleCategory: "chief_executive",
      organization: "",
      status: "current",
      confidence: 0.95,
    },
  ],
  businessItems: [
    {
      fiscalYear: 2023,
      itemKind: "customer_market",
      name: "Corporate customers",
      geographyType: "",
      confidence: 0.9,
    },
  ],
  contacts: [
    {
      fiscalYear: 2023,
      candidateKind: "email",
      normalizedValue: "sustainability@handelsbanken.se",
      registrableDomain: "handelsbanken.se",
    },
  ],
  relationships: [],
};

describe("SeCompanyEsefView", () => {
  it("renders every section with parsed-vs-pending filings", () => {
    const html = renderToStaticMarkup(
      <SeCompanyEsefView companyId="5020077862" detail={DETAIL} />,
    );
    expect(html).toContain("541");
    expect(html).toContain("Notes (144)");
    expect(html).toContain("Not parsed yet");
    expect(html).toContain("Handelsbanken is a Swedish credit institution.");
    expect(html).toContain("Customer markets");
    expect(html).toContain("Carina Åkerström");
    expect(html).toContain("sustainability@handelsbanken.se");
    expect(html).toContain(
      "/company/se/5020077862/financials/esef/NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );
  });
});
