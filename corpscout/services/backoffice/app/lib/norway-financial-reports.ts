export interface FinancialReportDocumentSummary {
  documentId: string;
  filingYear: number;
  sourceFileName: string;
  sourceUrl: string;
  factCount: number;
  pageCount: number;
  nativeTextPageCount: number;
  ocrPageCount: number;
  pdfSizeBytes: number;
  parseStatus: string;
  parseWarnings: string;
  retrievedAt: string;
  resolvedAt: string;
  hasReportMetadata: boolean;
}

export type NorwayFinancialReportSummary = FinancialReportDocumentSummary;

export interface NorwayFinancialFact {
  factOrdinal: number;
  pageNumber: number;
  lineNumber: number;
  statementType: string;
  tableTitle: string;
  rawLabel: string;
  normalizedLabel: string;
  canonicalConcept: string | null;
  columnLabel: string;
  fiscalYear: number | null;
  isComparative: boolean;
  valueKind: string;
  rawValue: string;
  currency: string;
  extractionMethod: string;
  mappingMethod: string;
  mappingConfidence: number | null;
  qualityFlags: string;
}

export interface NorwayFinancialReport {
  summary: FinancialReportDocumentSummary;
  facts: NorwayFinancialFact[];
}

export function statementTypeLabel(statementType: string): string {
  const labels: Record<string, string> = {
    income_statement: "Income statement",
    balance_sheet: "Balance sheet",
    cash_flow_statement: "Cash flow statement",
    equity_statement: "Equity statement",
    notes: "Notes",
    other: "Other",
  };
  return (
    labels[statementType] ??
    statementType
      .split("_")
      .filter(Boolean)
      .map((part, index) =>
        index === 0 ? `${part.charAt(0).toUpperCase()}${part.slice(1)}` : part,
      )
      .join(" ")
  );
}

export function formatFileSize(bytes: number): string | null {
  if (!Number.isFinite(bytes) || bytes <= 0) return null;
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = units[0];
  for (let index = 1; index < units.length && value >= 1024; index += 1) {
    value /= 1024;
    unit = units[index];
  }
  return `${value.toFixed(1)} ${unit}`;
}

export function parseWarnings(value: string): string[] {
  if (!value || value === "[]") return [];
  try {
    const parsed: unknown = JSON.parse(value);
    if (Array.isArray(parsed)) {
      return parsed.filter(
        (warning): warning is string => typeof warning === "string",
      );
    }
  } catch {
    // Older pipeline rows may carry one plain-text warning instead of JSON.
  }
  return [value];
}
