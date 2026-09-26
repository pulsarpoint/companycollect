import { createHash } from "node:crypto";
import { QUEUE_UUID } from "~/lib/queues";
import { dagsterRunUrl, launchRun, listRuns, runStatus, type DagsterOptions } from "~/lib/dagster.server";
import { NONE_FILTER_VALUE } from "~/lib/se-company-info-filters";
import { SE_COMPANIES_SERVING_TABLE } from "~/lib/se-company-info-lists.server";
import { parseSeCompanySelection } from "~/lib/se-company-selection.server";

export async function launchSeCompanyBraveAnalysis(
  value: unknown,
  requestedBy: string,
  submissionId: string,
  options: DagsterOptions = {},
) {
  if (!QUEUE_UUID.test(submissionId)) throw new Error("Invalid submission ID. Reload the page.");
  const selection = parseSeCompanySelection(value);
  if (selection.mode === "ids" && selection.companyIds.length === 0) {
    throw new Error("Select at least one company for Brave analysis.");
  }
  const input: Record<string, unknown> = {
    submission_id: submissionId,
    queue_scope: "workspace:SE",
    source_name: "backoffice:se-companies",
    source_relation: SE_COMPANIES_SERVING_TABLE,
    company_name_column: "legal_name",
    country_code: "SE",
  };
  if (selection.mode === "ids") {
    input.filters = { company_id: [...selection.companyIds].sort() };
  } else {
    // Mirror buildInfoListFilter: Dagster evaluates these predicates inside
    // ClickHouse and freezes the matches, without expanding IDs into run config.
    const query = selection.query;
    const filters: Record<string, string[]> = {};
    if (query.companyId) filters.company_id = [query.companyId];
    if (query.name) input.company_name_pattern = `%${query.name}%`;
    if (query.entity) input.company_id_length = query.entity === "legal" ? 10 : 12;
    if (query.status) filters.status = [query.status === NONE_FILTER_VALUE ? "" : query.status];
    if (query.legalForm) filters.legal_form_code = [query.legalForm === NONE_FILTER_VALUE ? "" : query.legalForm];
    if (query.description) filters.has_description = [query.description === "yes" ? "1" : "0"];
    if (query.source && query.source !== "scb") filters[`source_${query.source}`] = ["1"];
    for (const datatype of query.datatypes) filters[datatype] = ["1"];
    input.filters = filters;
    input.excluded_company_ids = [...selection.excludedCompanyIds].sort();
    input.select_all = true;
  }
  return submitBraveInput(input, submissionId, requestedBy, options);
}

export async function retryBraveFailures(taskId: string, submissionId: string, requestedBy: string, options: DagsterOptions = {}) {
  if (!QUEUE_UUID.test(taskId)) throw new Error("Invalid Brave task ID.");
  return submitBraveInput({submission_id: submissionId, queue_scope: "workspace:SE", country_code: "SE",
    source_name: "backoffice:brave-failed-companies", source_relation: "corpscout.company_brave_search_results",
    company_name_column: "company_name", source_final: true, filters: {task_id: [taskId], status: ["error"]}},
    submissionId, requestedBy, options);
}

async function submitBraveInput(input: Record<string, unknown>, submissionId: string, requestedBy: string, options: DagsterOptions) {
  if (!QUEUE_UUID.test(submissionId)) throw new Error("Invalid submission ID. Reload the page.");
  const fingerprint = createHash("sha256").update(JSON.stringify(input)).digest("hex");
  const existing = (await listRuns({job: "company_brave_queue_input_job", limit: 1,
    tags: {"processing/submission_id": submissionId}}, options))[0];
  if (existing && existing.tags["brave/selection_sha256"] !== fingerprint) throw new Error("This submission ID belongs to a different selection.");
  if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) {
    return {ok: true as const, runId: existing.runId, status: existing.status, runUrl: dagsterRunUrl(existing.runId, options.url)};
  }
  const run = await launchRun({
    job: "company_brave_queue_input_job", assetSelection: ["company_brave_queue_input"],
    runConfig: {ops: {company_brave_queue_input: {config: input}}},
    tags: {"processing/submission_id": submissionId, "brave/selection_sha256": fingerprint,
      "corpscout/requested_by": requestedBy, "backoffice/action": "add-brave-input"},
  }, options);
  return {ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId, options.url)};
}

export async function braveQueueSubmission(runId: string) {
  if (!QUEUE_UUID.test(runId)) throw new Error("Invalid import run ID.");
  const run = await runStatus(runId);
  if (run.jobName !== "company_brave_queue_input_job" || run.tags["backoffice/action"] !== "add-brave-input") throw new Error("Brave queue submission not found.");
  const task = run.tags["processing/task_id"];
  return {ok: true as const, runId, status: run.status, finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status),
    taskId: task && QUEUE_UUID.test(task) ? task : null};
}
