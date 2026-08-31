import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { EsefSection } from "~/components/detail/esef-section";
import {
  EsefFactDetails,
  EsefFactsAccordion,
} from "~/components/detail/esef-facts-accordion";
import {
  esefConceptLabel,
  esefDecimalsLabel,
  esefDimensionSummary,
  esefFactConceptLabels,
  esefFactPeriod,
  esefTextValue,
  type EsefFinancialFact,
} from "~/lib/esef-financial-reports";
import { getCountry } from "~/lib/countries";
import type { EsefFilingRow } from "~/lib/queries.server";

const filing: EsefFilingRow = {
  primary_fxo_id: "example-2024-0",
  fiscal_year: 2024,
  period_end: "2024-12-31",
  currency: "SEK",
  revenue_amount_original: 4_994_000_000,
  revenue_amount_usd: 452_000_000,
  operating_profit_amount_original: 4_191_000_000,
  operating_profit_amount_usd: 379_000_000,
  profit_loss_amount_original: 5_274_000_000,
  profit_loss_amount_usd: 478_000_000,
  total_assets_amount_original: 84_000_000_000,
  total_assets_amount_usd: 7_619_000_000,
  equity_amount_original: 41_800_000_000,
  equity_amount_usd: 3_790_000_000,
  liabilities_amount_original: 42_200_000_000,
  liabilities_amount_usd: 3_829_000_000,
  cash_amount_original: 2_300_000_000,
  cash_amount_usd: 209_000_000,
  employees: 112,
  mapped_fact_count: 8,
  source_fact_count: 387,
  viewer_url: "https://example.test/viewer",
  source_url: "https://example.test/source",
  package_url: "https://example.test/package.zip",
  filing_versions: 1,
  composed_from_amendment: 0,
  source_record_uids: [],
  evidence: [],
};

const monetaryFact: EsefFinancialFact = {
  factId: "fact-1",
  conceptQname: "ifrs-full:Revenue",
  conceptLocalName: "Revenue",
  valueKind: "monetary",
  rawValue: "4994000000",
  amountOriginal: 4_994_000_000,
  amountUsd: 452_000_000,
  fxRateDate: "2024-12-31",
  fxSource: "ecb",
  decimals: -6,
  periodStart: "2024-01-01",
  periodInstant: "",
  periodDurationEnd: "2024-12-31",
  unit: "iso4217:SEK",
  currency: "SEK",
  dimensions: JSON.stringify({
    "ifrs-full:SegmentsAxis": "sagax:SwedenMember",
  }),
  language: "",
};

describe("ESEF source financials", () => {
  it("resolves Swedish facts against the filing's exact taxonomy entrypoint", () => {
    const factsQuery = getCountry("se")?.detail?.factsQuery ?? "";

    expect(factsQuery).toContain("se_financial_taxonomy_concept_labels");
    expect(factsQuery).toContain(
      "labels.taxonomy_entrypoint = ifNull(report.taxonomy_entrypoint, '')",
    );
    expect(factsQuery).toContain("concept_label_en_source");
    expect(factsQuery).toContain("concept_description_en_source");
  });

  it("shows source-currency values and the complete standardized metric set", () => {
    const html = renderToStaticMarkup(<EsefSection filings={[filing]} />);

    expect(html).toContain("4.99B SEK");
    expect(html).toContain("$452M");
    expect(html).toContain("Liabilities");
    expect(html).toContain("Cash");
    expect(html).toContain("Employees");
    expect(html).toContain("387 current facts");
    expect(html).toContain("8 standardized");
  });

  it("keeps exact ESEF period and dimension semantics readable", () => {
    expect(esefFactPeriod(monetaryFact)).toBe("2024-01-01 – 2024-12-31");
    expect(esefDimensionSummary(monetaryFact.dimensions)).toBe(
      "SegmentsAxis: SwedenMember",
    );
    expect(
      esefTextValue(
        '<div>Rental income <span class="value">SEK&nbsp;4.99bn</span></div>',
      ),
    ).toBe("Rental income SEK 4.99bn");
  });

  it("turns XBRL concept local names into readable labels", () => {
    expect(esefConceptLabel("AddressOfRegisteredOfficeOfEntity")).toBe(
      "Address Of Registered Office Of Entity",
    );
    expect(esefConceptLabel("IFRSAccountingStandardTaxonomy2024")).toBe(
      "IFRS Accounting Standard Taxonomy 2024",
    );
    expect(
      esefConceptLabel("", "ifrs-full:RentalIncomeFromInvestmentProperty"),
    ).toBe("Rental Income From Investment Property");
  });

  it("shows English first and keeps the submitted label underneath", () => {
    const fact: EsefFinancialFact = {
      ...monetaryFact,
      conceptQname: "sample:Revenue",
      conceptLocalName: "Revenue",
      conceptLabels: [
        {
          language: "en",
          label: "Revenue",
          isReportLanguage: true,
        },
        {
          language: "sv",
          label: "Intäkter",
          isReportLanguage: true,
        },
      ],
    };

    expect(esefFactConceptLabels(fact)).toEqual({
      submitted: "Intäkter",
      submittedLanguage: "sv",
      english: "Revenue",
    });
    const html = renderToStaticMarkup(<EsefFactsAccordion facts={[fact]} />);
    expect(html).toContain("Intäkter");
    expect(html).toContain("Revenue");
    expect(html).toContain("Original (sv): Intäkter");
  });

  it("shows taxonomy and translation provenance in the expanded fact detail", () => {
    const fact: EsefFinancialFact = {
      ...monetaryFact,
      conceptLabels: [
        {
          language: "sv",
          label: "Nettoomsättning",
          isReportLanguage: true,
          source: "taxonomy",
        },
        {
          language: "en",
          label: "Net sales",
          isReportLanguage: false,
          source: "translation",
          translationProvider: "deepseek",
          translationModel: "deepseek-chat",
          translationVersion: 42,
        },
      ],
      conceptDocumentation: [
        {
          language: "sv",
          label: "Företagets nettoomsättning.",
          isReportLanguage: true,
          source: "taxonomy",
        },
        {
          language: "en",
          label: "The company's net sales.",
          isReportLanguage: false,
          source: "translation",
          translationProvider: "deepseek",
          translationModel: "deepseek-chat",
          translationVersion: 42,
        },
      ],
      conceptTaxonomy: {
        entrypoint: "https://taxonomier.se/entrypoint-2024.xsd",
        sourceUrl: "https://taxonomier.se/concepts/net-sales",
      },
    };

    const html = renderToStaticMarkup(<EsefFactDetails fact={fact} />);

    expect(html).toContain("Machine translation");
    expect(html).toContain("deepseek · deepseek-chat");
    expect(html).toContain("Version 42");
    expect(html).toContain("Taxonomy entrypoint");
    expect(html).toContain("entrypoint-2024.xsd");
    expect(html).toContain("Taxonomy concept source");
    expect(html).toContain("concepts/net-sales");
  });

  it("explains XBRL decimals as reported precision", () => {
    expect(esefDecimalsLabel(-5)).toBe(
      "Reported precision: nearest 100,000 (XBRL decimals -5)",
    );
    expect(esefDecimalsLabel(2)).toBe(
      "Reported precision: nearest 0.01 (XBRL decimals 2)",
    );
  });

  it("renders compact accordion headers without mounting long disclosure text", () => {
    const narrativeFact: EsefFinancialFact = {
      ...monetaryFact,
      factId: "fact-text",
      conceptQname: "ifrs-full:DisclosureOfFairValueMeasurementExplanatory",
      conceptLocalName: "DisclosureOfFairValueMeasurementExplanatory",
      valueKind: "text",
      rawValue: `A short useful preview ${"supporting context ".repeat(80)}full-detail-sentinel`,
      amountOriginal: null,
      amountUsd: null,
      fxRateDate: "",
      fxSource: "",
      decimals: null,
      unit: "",
      currency: "",
      dimensions: "{}",
      language: "sv",
    };

    const html = renderToStaticMarkup(
      <EsefFactsAccordion facts={[narrativeFact]} />,
    );

    expect(html).toContain("Disclosure Of Fair Value Measurement Explanatory");
    expect(html).toContain("Narrative");
    expect(html).toContain("A short useful preview");
    expect(html).not.toContain("Disclosure text");
    expect(html).not.toContain("full-detail-sentinel");
  });

  it("renders type-aware value, conversion, precision, and dimension details", () => {
    const html = renderToStaticMarkup(<EsefFactDetails fact={monetaryFact} />);

    expect(html).toContain("Source amount");
    expect(html).toContain("4,994,000,000");
    expect(html).toContain("$452,000,000.00");
    expect(html).toContain("Nearest 1,000,000");
    expect(html).toContain("XBRL decimals -6");
    expect(html).toContain("Segments Axis");
    expect(html).toContain("Sweden Member");
    expect(html).toContain("ifrs-full:Revenue");
  });

  it("uses persisted disclosure blocks and exposes their evidence", () => {
    const fact: EsefFinancialFact = {
      ...monetaryFact,
      factId: "fact-text-persisted",
      valueKind: "text",
      rawValue: "raw fallback should not render",
      amountOriginal: null,
      amountUsd: null,
      currency: "",
      unit: "",
      structuredDisclosure: {
        blocks: [{ type: "heading", text: "Persisted business detail" }],
        plainText: "Persisted business detail",
      },
      disclosureEvidence: {
        sourceRecordUid: "a".repeat(64),
        textSha256: "b".repeat(64),
        parserName: "lxml_html_disclosure",
        parserVersion: "1",
      },
    };

    const html = renderToStaticMarkup(<EsefFactDetails fact={fact} />);

    expect(html).toContain("Persisted business detail");
    expect(html).toContain("Structured extraction v1");
    expect(html).toContain("lxml_html_disclosure");
    expect(html).toContain("a".repeat(64));
    expect(html).toContain("b".repeat(64));
    expect(html).not.toContain("raw fallback should not render");
  });
});
