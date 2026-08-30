import { Buffer } from "node:buffer";
import TurndownService from "turndown";
import { gfm } from "turndown-plugin-gfm";
import { chQuery } from "~/lib/clickhouse.server";
import { fetchObject } from "~/lib/object-store.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  loadSeCompanyShell,
  type SeCompanyShell,
} from "~/lib/se-company-shell.server";

export interface SeRatsitRequestRow {
  company_id: string;
  legal_name: string;
  batch_id: string;
  outcome: string;
  selected_at: string;
  attempted_at: string;
  completed_at: string;
  http_status: number | null;
  source_url: string;
  source_bucket: string;
  source_object_key: string;
  content_size_bytes: number;
  duration_ms: number;
  attempt_count: number;
  error_type: string;
  error_message: string;
  temporal_workflow_id: string;
  temporal_run_id: string;
  recorded_at: string;
}

interface SeRatsitQueryRow extends Omit<SeRatsitRequestRow, "legal_name"> {}

interface SeRatsitCompanyNameRow {
  company_id: string;
  legal_name: string;
}

interface SeRatsitCountRow {
  total: number | string;
}

export interface SeRatsitRequestPage {
  rows: SeRatsitRequestRow[];
  total: number;
  page: number;
  pageSize: number;
}

export interface SeRatsitResponsePayload {
  browserId: string;
  finalUrl: string;
  contentType: string;
  markdown: string;
}

export interface SeRatsitRequestDetail {
  request: SeRatsitRequestRow;
  company: SeCompanyShell | null;
  payload: SeRatsitResponsePayload | null;
  payloadError: string | null;
}

export interface SeRatsitRequestSelection {
  companyId: string;
  batchId: string;
}

export const RATSIT_REQUESTS_SQL = `SELECT
  r.company_id AS company_id,
  toString(r.batch_id) AS batch_id,
  toString(r.outcome) AS outcome,
  toString(r.selected_at) AS selected_at,
  toString(r.attempted_at) AS attempted_at,
  toString(r.completed_at) AS completed_at,
  r.http_status AS http_status,
  r.source_url AS source_url,
  toString(r.source_bucket) AS source_bucket,
  r.source_object_key AS source_object_key,
  r.content_size_bytes AS content_size_bytes,
  r.duration_ms AS duration_ms,
  r.attempt_count AS attempt_count,
  toString(r.error_type) AS error_type,
  r.error_message AS error_message,
  r.temporal_workflow_id AS temporal_workflow_id,
  r.temporal_run_id AS temporal_run_id,
  toString(r.recorded_at) AS recorded_at
FROM corpscout.se_company_ratsit_crawl_results AS r FINAL
ORDER BY r.completed_at DESC, r.recorded_at DESC, r.company_id, r.batch_id
LIMIT {limit:UInt32} OFFSET {offset:UInt32}`;

export const RATSIT_REQUEST_COUNT_SQL = `SELECT count() AS total
FROM corpscout.se_company_ratsit_crawl_results FINAL`;

export const RATSIT_REQUEST_DETAIL_SQL = `SELECT
  r.company_id AS company_id,
  toString(r.batch_id) AS batch_id,
  toString(r.outcome) AS outcome,
  toString(r.selected_at) AS selected_at,
  toString(r.attempted_at) AS attempted_at,
  toString(r.completed_at) AS completed_at,
  r.http_status AS http_status,
  r.source_url AS source_url,
  toString(r.source_bucket) AS source_bucket,
  r.source_object_key AS source_object_key,
  r.content_size_bytes AS content_size_bytes,
  r.duration_ms AS duration_ms,
  r.attempt_count AS attempt_count,
  toString(r.error_type) AS error_type,
  r.error_message AS error_message,
  r.temporal_workflow_id AS temporal_workflow_id,
  r.temporal_run_id AS temporal_run_id,
  toString(r.recorded_at) AS recorded_at
FROM corpscout.se_company_ratsit_crawl_results AS r FINAL
WHERE r.company_id = {companyId:String}
  AND r.batch_id = {batchId:UUID}
LIMIT 1`;

const RATSIT_COMPANY_NAMES_SQL = `SELECT
  company_id,
  legal_name
FROM corpscout.se_companies FINAL
WHERE company_id IN {companyIds:Array(String)}`;

function toRequestRow(
  row: SeRatsitQueryRow,
  legalName = "",
): SeRatsitRequestRow {
  return {
    ...row,
    legal_name: legalName,
    http_status: row.http_status === null ? null : Number(row.http_status),
    content_size_bytes: Number(row.content_size_bytes),
    duration_ms: Number(row.duration_ms),
    attempt_count: Number(row.attempt_count),
  };
}

export async function listSeRatsitRequests(options: {
  page: number;
  pageSize: number;
}): Promise<SeRatsitRequestPage> {
  const page = clampPage(options.page);
  const pageSize = clampPageSize(options.pageSize);
  const [requestRows, countRows] = await Promise.all([
    chQuery<SeRatsitQueryRow>(RATSIT_REQUESTS_SQL, {
      limit: pageSize,
      offset: (page - 1) * pageSize,
    }),
    chQuery<SeRatsitCountRow>(RATSIT_REQUEST_COUNT_SQL),
  ]);

  const companyIds = [...new Set(requestRows.map((row) => row.company_id))];
  const companyNames =
    companyIds.length === 0
      ? []
      : await chQuery<SeRatsitCompanyNameRow>(RATSIT_COMPANY_NAMES_SQL, {
          companyIds,
        });
  const nameByCompanyId = new Map(
    companyNames.map((row) => [row.company_id, row.legal_name]),
  );

  return {
    rows: requestRows.map((row) =>
      toRequestRow(row, nameByCompanyId.get(row.company_id) ?? ""),
    ),
    total: Number(countRows[0]?.total ?? 0),
    page,
    pageSize,
  };
}

interface RatsitResponseEnvelope {
  browserId: string;
  finalUrl: string;
  contentType: string;
  content: string;
}

export function parseRatsitResponseEnvelope(
  value: unknown,
  request: Pick<
    SeRatsitRequestRow,
    "company_id" | "batch_id" | "content_size_bytes"
  >,
): RatsitResponseEnvelope {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("The S3 object is not a Ratsit response envelope.");
  }
  const envelope = value as Record<string, unknown>;
  if (envelope.schema_version !== 1 || envelope.source !== "ratsit") {
    throw new Error("The S3 object has an unsupported Ratsit response schema.");
  }
  if (!envelope.result || typeof envelope.result !== "object") {
    throw new Error("The Ratsit response envelope has no result metadata.");
  }
  const result = envelope.result as Record<string, unknown>;
  if (
    result.company_id !== request.company_id ||
    result.batch_id !== request.batch_id
  ) {
    throw new Error("The S3 response does not match this ClickHouse request.");
  }
  if (typeof envelope.content !== "string") {
    throw new Error("The Ratsit response envelope has no HTML content.");
  }
  if (Buffer.byteLength(envelope.content, "utf8") !== request.content_size_bytes) {
    throw new Error("The S3 response size does not match ClickHouse metadata.");
  }
  return {
    browserId:
      typeof envelope.browser_id === "string" ? envelope.browser_id : "",
    finalUrl: typeof envelope.final_url === "string" ? envelope.final_url : "",
    contentType:
      typeof envelope.content_type === "string"
        ? envelope.content_type
        : "text/html",
    content: envelope.content,
  };
}

function safeAbsoluteUrl(href: string, baseUrl: string): string | null {
  try {
    const url = new URL(href, baseUrl);
    return ["http:", "https:", "mailto:", "tel:"].includes(url.protocol)
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}

function markdownTable(node: HTMLElement): string {
  const rows = Array.from((node as HTMLTableElement).rows ?? []);
  if (rows.length === 0) return "";
  const cells = rows.map((row) =>
    Array.from(row.cells).map((cell) =>
      (cell.textContent ?? "")
        .replace(/\s+/g, " ")
        .trim()
        .replaceAll("|", "\\|"),
    ),
  );
  const columnCount = Math.max(...cells.map((row) => row.length));
  const firstRowIsHeader = Array.from(rows[0]?.cells ?? []).some(
    (cell) => cell.nodeName === "TH",
  );
  const header = firstRowIsHeader
    ? cells.shift() ?? []
    : Array.from({ length: columnCount }, (_value, index) =>
        columnCount === 1 ? "Value" : `Column ${index + 1}`,
      );
  const padded = (row: string[]) => [
    ...row,
    ...Array.from({ length: columnCount - row.length }, () => ""),
  ];
  const line = (row: string[]) => `| ${padded(row).join(" | ")} |`;
  return [
    "",
    line(header),
    line(Array.from({ length: columnCount }, () => "---")),
    ...cells.map(line),
    "",
  ].join("\n");
}

export function htmlToRatsitMarkdown(html: string, baseUrl: string): string {
  const service = new TurndownService({
    headingStyle: "atx",
    bulletListMarker: "-",
    codeBlockStyle: "fenced",
    strongDelimiter: "**",
  });
  service.use(gfm);
  const removedElements = new Set([
    "SCRIPT",
    "STYLE",
    "NOSCRIPT",
    "SVG",
    "FORM",
    "BUTTON",
    "IMG",
  ]);
  service.addRule("discardNonDocumentContent", {
    filter: (node) => removedElements.has(node.nodeName),
    replacement: () => "",
  });
  service.addRule("absoluteRatsitLinks", {
    filter: "a",
    replacement(content, node) {
      const label = content.trim();
      const href = node.getAttribute("href")?.trim() ?? "";
      const absoluteUrl = safeAbsoluteUrl(href, baseUrl);
      if (!absoluteUrl) return label;
      return `[${label || absoluteUrl}](${absoluteUrl})`;
    },
  });
  // The GFM plugin deliberately keeps tables without a TH row as raw HTML.
  // Ratsit uses many such layout/data tables, while react-markdown correctly
  // refuses to execute raw HTML. Convert every table to a plain GFM matrix so
  // the viewer never displays markup as evidence text.
  service.addRule("allRatsitTables", {
    filter: "table",
    replacement: (_content, node) => markdownTable(node),
  });
  return service.turndown(html).trim();
}

async function loadResponsePayload(
  request: SeRatsitRequestRow,
): Promise<{
  payload: SeRatsitResponsePayload | null;
  payloadError: string | null;
}> {
  if (request.source_bucket === "" || request.source_object_key === "") {
    return { payload: null, payloadError: null };
  }
  try {
    const response = await fetchObject(
      request.source_bucket,
      request.source_object_key,
    );
    if (!response.ok) {
      throw new Error(`The object store returned HTTP ${response.status}.`);
    }
    const envelope = parseRatsitResponseEnvelope(await response.json(), request);
    return {
      payload: {
        browserId: envelope.browserId,
        finalUrl: envelope.finalUrl,
        contentType: envelope.contentType,
        markdown: htmlToRatsitMarkdown(
          envelope.content,
          envelope.finalUrl || request.source_url,
        ),
      },
      payloadError: null,
    };
  } catch (error) {
    return {
      payload: null,
      payloadError:
        error instanceof Error
          ? error.message
          : "The stored Ratsit response could not be loaded.",
    };
  }
}

export async function loadSeRatsitRequestDetail(
  selection: SeRatsitRequestSelection,
): Promise<SeRatsitRequestDetail | null> {
  const [queryRow] = await chQuery<SeRatsitQueryRow>(RATSIT_REQUEST_DETAIL_SQL, {
    companyId: selection.companyId,
    batchId: selection.batchId,
  });
  if (!queryRow) return null;

  const [company, response] = await Promise.all([
    loadSeCompanyShell(queryRow.company_id),
    loadResponsePayload(toRequestRow(queryRow)),
  ]);
  return {
    request: toRequestRow(queryRow, company?.legal_name ?? ""),
    company,
    ...response,
  };
}
