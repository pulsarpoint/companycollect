export const FINANCIAL_FILING_FILTER_KEY = "financial_filing_status";

export const FINANCIAL_FILING_STATUSES = [
  "data_available",
  "filed_unstructured",
  "not_submitted",
  "unknown",
] as const;

export type FinancialFilingStatus = (typeof FINANCIAL_FILING_STATUSES)[number];

export type FinancialFilingStatusDefinition = {
  value: FinancialFilingStatus;
  shortLabel: string;
  label: string;
  meaning: string;
};

export type CompanyFinancialFilingStatus = {
  status: FinancialFilingStatus;
  reportPeriodEnd: string | null;
  filingRegisteredOn: string | null;
  sourceFileFormat: string | null;
  bolagsverketDocumentId: string | null;
  sourceSlug: string | null;
  observedAt: string | null;
};

export const FINANCIAL_FILING_STATUS_DEFINITIONS: readonly FinancialFilingStatusDefinition[] =
  [
    {
      value: "data_available",
      shortLabel: "Available",
      label: "Financial data available",
      meaning:
        "A structured annual report has been parsed into financial data.",
    },
    {
      value: "filed_unstructured",
      shortLabel: "Other format",
      label: "Filed in another format",
      meaning:
        "An official filing exists, but it is not available as structured financial data.",
    },
    {
      value: "not_submitted",
      shortLabel: "Not submitted",
      label: "Annual report not submitted",
      meaning:
        "An official source explicitly reports that the expected annual report is missing.",
    },
    {
      value: "unknown",
      shortLabel: "Unknown",
      label: "Filing status unknown",
      meaning:
        "No structured report or explicit official filing observation is available yet.",
    },
  ];

export function isFinancialFilingStatus(
  value: string,
): value is FinancialFilingStatus {
  return (FINANCIAL_FILING_STATUSES as readonly string[]).includes(value);
}

export function financialFilingStatusDefinition(
  value: FinancialFilingStatus,
): FinancialFilingStatusDefinition {
  return FINANCIAL_FILING_STATUS_DEFINITIONS.find(
    (definition) => definition.value === value,
  )!;
}

export function financialFilingSourceLabel(sourceSlug: string): string {
  if (sourceSlug === "sweden_financial") {
    return "Bolagsverket digital annual reports";
  }
  if (sourceSlug === "bolagsverket_xml_d3") {
    return "Bolagsverket scanned-document register";
  }
  return sourceSlug;
}
