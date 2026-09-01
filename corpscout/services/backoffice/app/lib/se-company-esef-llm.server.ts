import { chQuery } from "~/lib/clickhouse.server";
import { fetchObject } from "~/lib/object-store.server";

// The LLM company-information extraction persists its full exchange: the
// request body (OpenAI-compatible JSON) lands on S3 under
// esef_filings/llm_company_enrichment_requests and its object key plus the
// raw response text are stored per source document in
// esef_document_company_information. This page reads that record verbatim --
// nothing is recomputed.
const ESEF_DOCUMENT_BUCKET = "source-esef-filings";

export const ESEF_LLM_EXTRACTION_SQL = `
SELECT
  source_document_id,
  toInt32(fiscal_year) AS fiscal_year,
  extraction_status,
  model_provider,
  model_name,
  prompt_version,
  toInt64(coalesce(prompt_tokens, 0)) AS prompt_tokens,
  toInt64(coalesce(completion_tokens, 0)) AS completion_tokens,
  toInt64(coalesce(input_character_count, 0)) AS input_character_count,
  llm_request_object_key,
  llm_response_text,
  company_description,
  description_evidence_ids_json,
  people_json,
  products_and_services_json,
  customer_markets_json,
  operating_geographies_json,
  business_segments_json,
  material_group_relationships_json,
  toString(extracted_at) AS extracted_at
FROM corpscout.esef_document_company_information
WHERE source_document_id = {documentId:String}
ORDER BY extracted_at DESC
LIMIT 1`;

// What the extraction would draw on: the deterministic narrative evidence for
// the document. Shown as "what should be sent" when no LLM run exists yet,
// and as the evidence index next to a real run.
export const ESEF_LLM_EVIDENCE_SQL = `
SELECT
  disclosure_id,
  disclosure_kind,
  concept_local_name,
  toString(language) AS language,
  section_type,
  toInt32OrZero(coalesce(toString(printed_page_number), '')) AS printed_page_number,
  toInt64(original_character_count) AS original_character_count,
  toInt32(table_count) AS table_count,
  substring(plain_text, 1, 600) AS text_preview
FROM corpscout.esef_disclosures
WHERE source_document_id = {documentId:String}
ORDER BY disclosure_kind, anchor_visual_order, disclosure_id`;

interface EsefLlmExtractionRow {
  source_document_id: string;
  fiscal_year: number;
  extraction_status: string;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  prompt_tokens: number;
  completion_tokens: number;
  input_character_count: number;
  llm_request_object_key: string;
  llm_response_text: string;
  company_description: string;
  description_evidence_ids_json: string;
  people_json: string;
  products_and_services_json: string;
  customer_markets_json: string;
  operating_geographies_json: string;
  business_segments_json: string;
  material_group_relationships_json: string;
  extracted_at: string;
}

interface EsefLlmEvidenceRow {
  disclosure_id: string;
  disclosure_kind: string;
  concept_local_name: string;
  language: string;
  section_type: string;
  printed_page_number: number;
  original_character_count: number;
  table_count: number;
  text_preview: string;
}

export interface EsefLlmRequestMessage {
  role: string;
  content: string;
}

export interface EsefLlmExtraction {
  sourceDocumentId: string;
  fiscalYear: number;
  extractionStatus: string;
  modelProvider: string;
  modelName: string;
  promptVersion: string;
  promptTokens: number;
  completionTokens: number;
  inputCharacterCount: number;
  llmRequestObjectKey: string;
  llmResponseText: string;
  companyDescription: string;
  descriptionEvidenceIdsJson: string;
  peopleJson: string;
  productsAndServicesJson: string;
  customerMarketsJson: string;
  operatingGeographiesJson: string;
  businessSegmentsJson: string;
  materialGroupRelationshipsJson: string;
  extractedAt: string;
}

export interface EsefLlmEvidenceItem {
  disclosureId: string;
  disclosureKind: string;
  conceptLocalName: string;
  language: string;
  sectionType: string;
  printedPageNumber: number;
  originalCharacterCount: number;
  tableCount: number;
  textPreview: string;
}

export interface SeCompanyEsefLlmDetail {
  extraction: EsefLlmExtraction | null;
  // The stored request body, parsed: model + chat messages actually sent.
  requestModel: string;
  requestMessages: EsefLlmRequestMessage[];
  requestFetchError: string;
  evidence: EsefLlmEvidenceItem[];
}

function messageContentToText(content: unknown): string {
  if (typeof content === "string") return content;
  // OpenAI content-part arrays: [{type: "text", text: "..."}, ...]
  if (Array.isArray(content)) {
    return content
      .map((part) =>
        part && typeof part === "object" && "text" in part
          ? String((part as { text: unknown }).text ?? "")
          : "",
      )
      .join("");
  }
  return content == null ? "" : JSON.stringify(content);
}

async function loadRequestBody(
  objectKey: string,
): Promise<Pick<SeCompanyEsefLlmDetail, "requestModel" | "requestMessages" | "requestFetchError">> {
  try {
    const response = await fetchObject(ESEF_DOCUMENT_BUCKET, objectKey);
    if (!response.ok) {
      return {
        requestModel: "",
        requestMessages: [],
        requestFetchError: `S3 returned ${response.status} for ${objectKey}`,
      };
    }
    const body = (await response.json()) as {
      model?: unknown;
      messages?: unknown;
    };
    const messages = Array.isArray(body.messages)
      ? body.messages.map((message) => ({
          role: String(
            (message as { role?: unknown }).role ?? "unknown",
          ),
          content: messageContentToText(
            (message as { content?: unknown }).content,
          ),
        }))
      : [];
    return {
      requestModel: typeof body.model === "string" ? body.model : "",
      requestMessages: messages,
      requestFetchError: "",
    };
  } catch (error) {
    return {
      requestModel: "",
      requestMessages: [],
      requestFetchError: `Could not read stored request: ${String(error)}`,
    };
  }
}

export async function loadSeCompanyEsefLlm(
  documentId: string,
): Promise<SeCompanyEsefLlmDetail> {
  const [extractionRows, evidenceRows] = await Promise.all([
    chQuery<EsefLlmExtractionRow>(ESEF_LLM_EXTRACTION_SQL, { documentId }),
    chQuery<EsefLlmEvidenceRow>(ESEF_LLM_EVIDENCE_SQL, { documentId }),
  ]);

  const row = extractionRows[0];
  const extraction: EsefLlmExtraction | null = row
    ? {
        sourceDocumentId: row.source_document_id,
        fiscalYear: Number(row.fiscal_year),
        extractionStatus: row.extraction_status,
        modelProvider: row.model_provider,
        modelName: row.model_name,
        promptVersion: row.prompt_version,
        promptTokens: Number(row.prompt_tokens),
        completionTokens: Number(row.completion_tokens),
        inputCharacterCount: Number(row.input_character_count),
        llmRequestObjectKey: row.llm_request_object_key,
        llmResponseText: row.llm_response_text,
        companyDescription: row.company_description,
        descriptionEvidenceIdsJson: row.description_evidence_ids_json,
        peopleJson: row.people_json,
        productsAndServicesJson: row.products_and_services_json,
        customerMarketsJson: row.customer_markets_json,
        operatingGeographiesJson: row.operating_geographies_json,
        businessSegmentsJson: row.business_segments_json,
        materialGroupRelationshipsJson: row.material_group_relationships_json,
        extractedAt: row.extracted_at,
      }
    : null;

  const request = extraction?.llmRequestObjectKey
    ? await loadRequestBody(extraction.llmRequestObjectKey)
    : { requestModel: "", requestMessages: [], requestFetchError: "" };

  return {
    extraction,
    ...request,
    evidence: evidenceRows.map((r) => ({
      disclosureId: r.disclosure_id,
      disclosureKind: r.disclosure_kind,
      conceptLocalName: r.concept_local_name,
      language: r.language,
      sectionType: r.section_type,
      printedPageNumber: Number(r.printed_page_number),
      originalCharacterCount: Number(r.original_character_count),
      tableCount: Number(r.table_count),
      textPreview: r.text_preview,
    })),
  };
}
