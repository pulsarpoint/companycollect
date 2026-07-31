import { chQuery } from "~/lib/clickhouse.server";
import type {
  NorwayFinancialFact,
  NorwayFinancialReport,
  NorwayFinancialReportSummary,
} from "~/lib/norway-financial-reports";

interface ReportSummaryRow {
  document_id: string;
  source_filing_year: number;
  source_file_name: string;
  source_url: string;
  fact_count: number;
  page_count: number;
  native_text_page_count: number;
  ocr_page_count: number;
  source_pdf_size_bytes: number;
  parse_status: string;
  parse_warnings: string;
  retrieved_at: string;
  resolved_at: string;
  has_report_metadata: number;
}

interface FinancialFactRow {
  fact_ordinal: number;
  page_number: number;
  line_number: number;
  statement_type: string;
  table_title: string;
  raw_label: string;
  normalized_label: string;
  canonical_concept: string | null;
  column_label: string;
  fiscal_year: number | null;
  is_comparative: number;
  value_kind: string;
  raw_value: string;
  currency: string;
  extraction_method: string;
  mapping_method: string;
  mapping_confidence: number | null;
  quality_flags: string;
}

const REPORTS_QUERY = `
WITH fact_documents AS (
  SELECT
    document_id,
    argMax(source_filing_year, resolved_at) AS source_filing_year,
    argMax(source_file_name, resolved_at) AS source_file_name,
    argMax(source_url, resolved_at) AS source_url,
    toUInt64(count()) AS fact_count,
    toUInt32(max(page_number)) AS facts_last_page,
    max(resolved_at) AS facts_resolved_at
  FROM no_financial_facts
  WHERE org_number = {id:String}
  GROUP BY document_id
),
report_documents AS (
  SELECT
    document_id,
    argMax(source_file_name, resolved_at) AS source_file_name,
    argMax(source_pdf_url, resolved_at) AS source_pdf_url,
    argMax(source_pdf_size_bytes, resolved_at) AS source_pdf_size_bytes,
    argMax(pdf_page_count, resolved_at) AS pdf_page_count,
    argMax(native_text_page_count, resolved_at) AS native_text_page_count,
    argMax(ocr_page_count, resolved_at) AS ocr_page_count,
    argMax(parse_status, resolved_at) AS parse_status,
    argMax(parse_warnings, resolved_at) AS parse_warnings,
    argMax(retrieved_at, resolved_at) AS retrieved_at,
    max(resolved_at) AS report_resolved_at
  FROM no_financial_reports
  WHERE org_number = {id:String}
  GROUP BY document_id
)
SELECT
  f.document_id AS document_id,
  f.source_filing_year AS source_filing_year,
  if(
    r.document_id = '',
    f.source_file_name,
    coalesce(nullIf(r.source_file_name, ''), f.source_file_name)
  ) AS source_file_name,
  if(
    r.document_id = '',
    f.source_url,
    coalesce(nullIf(r.source_pdf_url, ''), f.source_url)
  ) AS source_url,
  f.fact_count AS fact_count,
  if(r.document_id = '', f.facts_last_page, greatest(r.pdf_page_count, f.facts_last_page)) AS page_count,
  if(r.document_id = '', toUInt32(0), r.native_text_page_count) AS native_text_page_count,
  if(r.document_id = '', toUInt32(0), r.ocr_page_count) AS ocr_page_count,
  if(r.document_id = '', toUInt64(0), r.source_pdf_size_bytes) AS source_pdf_size_bytes,
  if(
    r.document_id = '',
    'facts_loaded',
    coalesce(nullIf(r.parse_status, ''), 'loaded')
  ) AS parse_status,
  if(r.document_id = '', '', r.parse_warnings) AS parse_warnings,
  if(r.document_id = '', '', coalesce(toString(r.retrieved_at), '')) AS retrieved_at,
  toString(greatest(f.facts_resolved_at, r.report_resolved_at)) AS resolved_at,
  toUInt8(r.document_id != '') AS has_report_metadata
FROM fact_documents AS f
LEFT JOIN report_documents AS r ON r.document_id = f.document_id
ORDER BY f.source_filing_year DESC, f.document_id DESC`;

export async function getNorwayFinancialReports(
  orgNumber: string,
): Promise<NorwayFinancialReportSummary[]> {
  const rows = await chQuery<ReportSummaryRow>(REPORTS_QUERY, { id: orgNumber });
  return rows.map((row) => ({
    documentId: row.document_id,
    filingYear: Number(row.source_filing_year),
    sourceFileName: row.source_file_name,
    sourceUrl: row.source_url,
    factCount: Number(row.fact_count),
    pageCount: Number(row.page_count),
    nativeTextPageCount: Number(row.native_text_page_count),
    ocrPageCount: Number(row.ocr_page_count),
    pdfSizeBytes: Number(row.source_pdf_size_bytes),
    parseStatus: row.parse_status,
    parseWarnings: row.parse_warnings,
    retrievedAt: row.retrieved_at,
    resolvedAt: row.resolved_at,
    hasReportMetadata: Boolean(row.has_report_metadata),
  }));
}

async function getNorwayFinancialFacts(
  orgNumber: string,
  documentId: string,
): Promise<NorwayFinancialFact[]> {
  const rows = await chQuery<FinancialFactRow>(
    `SELECT
       fact_ordinal,
       page_number,
       line_number,
       statement_type,
       table_title,
       raw_label,
       normalized_label,
       canonical_concept,
       column_label,
       fiscal_year,
       is_comparative,
       value_kind,
       raw_value,
       currency,
       extraction_method,
       mapping_method,
       mapping_confidence,
       quality_flags
     FROM no_financial_facts
     WHERE org_number = {id:String}
       AND document_id = {documentId:String}
     ORDER BY fact_ordinal`,
    { id: orgNumber, documentId },
  );
  return rows.map((row) => ({
    factOrdinal: Number(row.fact_ordinal),
    pageNumber: Number(row.page_number),
    lineNumber: Number(row.line_number),
    statementType: row.statement_type,
    tableTitle: row.table_title,
    rawLabel: row.raw_label,
    normalizedLabel: row.normalized_label,
    canonicalConcept: row.canonical_concept,
    columnLabel: row.column_label,
    fiscalYear: row.fiscal_year === null ? null : Number(row.fiscal_year),
    isComparative: Boolean(row.is_comparative),
    valueKind: row.value_kind,
    rawValue: row.raw_value,
    currency: row.currency,
    extractionMethod: row.extraction_method,
    mappingMethod: row.mapping_method,
    mappingConfidence:
      row.mapping_confidence === null ? null : Number(row.mapping_confidence),
    qualityFlags: row.quality_flags,
  }));
}

export async function getNorwayFinancialReport(
  orgNumber: string,
  documentId: string,
): Promise<NorwayFinancialReport | null> {
  const reports = await getNorwayFinancialReports(orgNumber);
  const summary = reports.find((report) => report.documentId === documentId);
  if (!summary) return null;
  const facts = await getNorwayFinancialFacts(orgNumber, documentId);
  return { summary, facts };
}
