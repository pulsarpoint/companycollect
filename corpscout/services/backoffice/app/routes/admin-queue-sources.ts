import { data } from "react-router";
import { parseQueueFilters } from "~/lib/queues";
import { listRuns } from "~/lib/dagster.server";
import { loadQueueSourcePage } from "~/lib/queue-history.server";

export async function loader({request, params}: {request: Request; params: {type?: string}}) {
  const search = new URL(request.url).searchParams;
  let filters;
  try { filters = parseQueueFilters(params.type, search); }
  catch { throw new Response("Invalid queue or task", {status: 400}); }
  if (!["webtech", "crawler"].includes(filters.type) || !filters.task) throw new Response("Select a crawler or Webtech task", {status: 400});
  const type = filters.type as "webtech" | "crawler";
  const job = type === "webtech" ? "webtech_scan_results_job" : filters.crawlType === "site_info" ? "website_site_info_results_job" : `website_${filters.crawlType}_crawl_results_job`;
  try {
    const runs = await listRuns({job, limit: 100, tags: {"processing/task_id": filters.task}});
    const rows = await loadQueueSourcePage(type, runs.map(run => ({taskId: filters.task,
      crawlType: type === "crawler" ? filters.crawlType : null, executionId: run.tags["crawler/execution_id"] || run.runId})), filters.page);
    return {rows, page: filters.page, error: null};
  } catch { return data({rows: [], page: filters.page, error: "Source websites are unavailable. Please retry."}, {status: 503}); }
}
