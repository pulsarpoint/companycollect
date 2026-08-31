import { chQuery } from "~/lib/clickhouse.server";
import {
  parsePersistedEsefDisclosure,
  type EsefDisclosureDocument,
} from "~/lib/esef-disclosures";
import {
  reportSummary,
  REPORT_SUMMARY_QUERY,
  type EsefSummaryRow,
} from "~/lib/esef-financial-reports.server";
import type { EsefFinancialReportSummary } from "~/lib/esef-financial-reports";

export const ESEF_REPORT_NOTES_SQL = `
SELECT
  toString(disclosure_id) AS disclosure_id,
  concept_qname,
  concept_local_name,
  toString(language) AS language,
  toString(printed_page_number) AS printed_page_number,
  toUInt32(block_count) AS block_count,
  toUInt32(table_count) AS table_count,
  toUInt32(original_character_count) AS original_character_count,
  blocks_json,
  plain_text
FROM corpscout.esef_disclosures
WHERE disclosure_kind = 'tagged_fact'
  AND source_document_id = {documentId:String}
ORDER BY anchor_visual_order, concept_qname
LIMIT 1 BY disclosure_id`;

interface EsefReportNoteRow {
  disclosure_id: string;
  concept_qname: string;
  concept_local_name: string;
  language: string;
  printed_page_number: string;
  block_count: number;
  table_count: number;
  original_character_count: number;
  blocks_json: string;
  plain_text: string;
}

export interface EsefReportNote {
  disclosureId: string;
  conceptQname: string;
  conceptLocalName: string;
  language: string;
  printedPageNumber: string;
  blockCount: number;
  tableCount: number;
  characterCount: number;
  disclosure: EsefDisclosureDocument;
}

export interface EsefReportNotes {
  summary: EsefFinancialReportSummary;
  notes: EsefReportNote[];
}

export async function getEsefReportNotes(
  country: string,
  companyId: string,
  documentId: string,
): Promise<EsefReportNotes | null> {
  const summaries = await chQuery<EsefSummaryRow>(REPORT_SUMMARY_QUERY, {
    country: country.toUpperCase(),
    id: companyId,
    documentId,
  });
  if (summaries.length === 0) return null;
  const rows = await chQuery<EsefReportNoteRow>(ESEF_REPORT_NOTES_SQL, {
    documentId,
  });
  return {
    summary: reportSummary(summaries[0]),
    notes: rows.map((row) => ({
      disclosureId: row.disclosure_id,
      conceptQname: row.concept_qname,
      conceptLocalName: row.concept_local_name,
      language: row.language,
      printedPageNumber: row.printed_page_number,
      blockCount: Number(row.block_count),
      tableCount: Number(row.table_count),
      characterCount: Number(row.original_character_count),
      disclosure:
        parsePersistedEsefDisclosure(row.blocks_json, row.plain_text) ?? {
          blocks: [{ type: "paragraph", text: row.plain_text }],
          plainText: row.plain_text,
        },
    })),
  };
}
