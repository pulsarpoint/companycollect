import { chQuery } from "~/lib/clickhouse.server";
import { dagsterRunUrl } from "~/lib/dagster.server";
import { BRAVE_RESULTS_PAGE_SIZE } from "~/lib/brave-results";
import { QUEUE_UUID } from "~/lib/queues";

export interface BraveResultSummary {
  result_id: string;
  task_id: string;
  country_code: string;
  company_id: string;
  company_name: string;
  query_type: string;
  query: string;
  status: "success" | "error";
  completed_at: string;
  error_type: string;
  error_stage: string;
  answer_preview: string;
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
  country_code, company_id, company_name, query_type, query, status,
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
  return { rows, selected, total, succeeded: Number(counts?.succeeded ?? 0), failed: Number(counts?.failed ?? 0), page, totalPages };
}

export type BraveResultsPage = Awaited<ReturnType<typeof loadBraveResults>>;
