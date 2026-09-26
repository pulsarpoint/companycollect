import { chQuery } from "~/lib/clickhouse.server";
import { dagsterRunUrl } from "~/lib/dagster.server";
import { BRAVE_RESULTS_PAGE_SIZE } from "~/lib/brave-results";
import { QUEUE_UUID } from "~/lib/queues";
import { llmControl } from "~/lib/llm-control.server";

export interface BraveCaptchaRequest {
  external_request_id: string;
  presented: boolean | null;
  detected_at: string | null;
  detection_source: "page" | "agent_start" | null;
  confirmed_at: string | null;
  cleared: boolean | null;
  attempts: number;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  models: string[];
  proxy_route: string | null;
  request_status: string;
}

export interface BraveResultSummary {
  result_id: string;
  task_id: string;
  country_code: string;
  company_id: string;
  company_name: string;
  query_type: string;
  search_id: string;
  search_name: string;
  search_revision: number;
  query: string;
  status: "success" | "error";
  completed_at: string;
  error_type: string;
  error_stage: string;
  answer_preview: string;
  captcha_requests?: BraveCaptchaRequest[];
}

export interface BraveResultDetail extends BraveResultSummary {
  answer_text: string;
  source_url: string;
  route: string;
  elapsed_ms: number;
  attempt: number;
  source_run_id: string;
  execution_id: string;
  runUrl: string | null;
}

type BraveResultScope = { taskId: string } | { countryCode: string; companyId: string };

const SUMMARY_COLUMNS = `toString(result_id) AS result_id, toString(task_id) AS task_id,
  country_code, company_id, company_name, query_type, search_id, search_name, search_revision, query, status,
  toString(completed_at) AS completed_at, error_type, error_stage,
  leftUTF8(answer_text, 240) AS answer_preview`;

/** Read all completed attempts, including errors, independently of queue retention. */
export async function loadBraveResults(scope: BraveResultScope, search: URLSearchParams) {
  const resultId = (search.get("result") ?? "").trim().toLowerCase();
  if (resultId && !QUEUE_UUID.test(resultId)) throw new Response("Invalid Brave result ID.", { status: 400 });
  let where: string;
  let params: Record<string, unknown>;
  if ("taskId" in scope) {
    if (!QUEUE_UUID.test(scope.taskId)) throw new Response("Invalid Brave task ID.", { status: 400 });
    where = "r.task_id = {taskId:UUID}";
    params = { taskId: scope.taskId };
  } else {
    where = "r.country_code = {countryCode:String} AND r.company_id = {companyId:String}";
    params = { countryCode: scope.countryCode, companyId: scope.companyId };
  }
  const from = `FROM corpscout.company_brave_search_results AS r FINAL WHERE ${where}`;
  const [counts] = await chQuery<{total: string; succeeded: string; failed: string}>(
    `SELECT toString(count()) AS total, toString(countIf(status = 'success')) AS succeeded,
      toString(countIf(status = 'error')) AS failed ${from}`, params,
  );
  const total = Number(counts?.total ?? 0);
  const totalPages = Math.max(1, Math.ceil(total / BRAVE_RESULTS_PAGE_SIZE));
  const requestedPage = Number(search.get("page") ?? 1);
  const page = Number.isSafeInteger(requestedPage) && requestedPage > 0 ? Math.min(requestedPage, totalPages) : 1;
  const rows = await chQuery<BraveResultSummary>(
    `SELECT ${SUMMARY_COLUMNS} ${from}
      ORDER BY r.completed_at DESC, r.result_id DESC LIMIT ${BRAVE_RESULTS_PAGE_SIZE} OFFSET {offset:UInt64}`,
    { ...params, offset: (page - 1) * BRAVE_RESULTS_PAGE_SIZE },
  );
  const selectedId = resultId || rows[0]?.result_id;
  let selected: BraveResultDetail | null = null;
  if (selectedId) {
    const [row] = await chQuery<Omit<BraveResultDetail, "runUrl">>(
      `SELECT ${SUMMARY_COLUMNS}, answer_text, source_url, route, elapsed_ms, attempt,
        source_run_id, toString(execution_id) AS execution_id
        ${from} AND r.result_id = {resultId:UUID} LIMIT 1`,
      { ...params, resultId: selectedId },
    );
    if (!row) throw new Response("This Brave result was not found for this company or task.", { status: 404 });
    selected = { ...row, runUrl: row.source_run_id ? dagsterRunUrl(row.source_run_id) : null };
  }
  const ids = [...new Set([...rows.map(row => row.result_id), ...(selected ? [selected.result_id] : [])])];
  if (ids.length) {
    // A resumed execution may own the same browser request. Count its usage once,
    // while preserving distinct canceled/retried browser requests for the result.
    const { rows: requests } = await llmControl().query<{
      brave_result_id: string; external_request_id: string; captcha_stats: Omit<BraveCaptchaRequest, "external_request_id">;
    }>(`SELECT DISTINCT ON (external_request_id) brave_result_id,external_request_id,captcha_stats
      FROM processing.llm_external_requests WHERE service='brave'
        AND brave_result_id=ANY($1::uuid[]) AND captcha_stats IS NOT NULL
      ORDER BY external_request_id,captcha_stats_updated_at DESC`, [ids]);
    const byResult = new Map<string, BraveCaptchaRequest[]>();
    for (const request of requests) {
      const stats = byResult.get(request.brave_result_id) ?? [];
      stats.push({...request.captcha_stats, external_request_id: request.external_request_id});
      byResult.set(request.brave_result_id, stats);
    }
    for (const row of rows) row.captcha_requests = byResult.get(row.result_id) ?? [];
    if (selected) selected.captcha_requests = byResult.get(selected.result_id) ?? [];
  }
  return { rows, selected, total, succeeded: Number(counts?.succeeded ?? 0), failed: Number(counts?.failed ?? 0), page, totalPages };
}

export type BraveResultsPage = Awaited<ReturnType<typeof loadBraveResults>>;
