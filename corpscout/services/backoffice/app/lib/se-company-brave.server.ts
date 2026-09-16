import { randomUUID } from "node:crypto";
import { dagsterRunUrl, launchRun, type DagsterOptions } from "~/lib/dagster.server";
import { NONE_FILTER_VALUE } from "~/lib/se-company-info-filters";
import { SE_COMPANIES_SERVING_TABLE } from "~/lib/se-company-info-lists.server";
import { parseSeCompanySelection } from "~/lib/se-company-selection.server";

export async function launchSeCompanyBraveAnalysis(
  value: unknown,
  requestedBy: string,
  options: DagsterOptions = {},
) {
  const selection = parseSeCompanySelection(value);
  if (selection.mode === "ids" && selection.companyIds.length === 0) {
    throw new Error("Select at least one company for Brave analysis.");
  }
  const taskId = randomUUID();
  const input: Record<string, unknown> = {
    task_id: taskId,
    source_relation: SE_COMPANIES_SERVING_TABLE,
    company_name_column: "legal_name",
    country_code: "SE",
  };
  if (selection.mode === "ids") {
    input.filters = { company_id: selection.companyIds };
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
    input.excluded_company_ids = selection.excludedCompanyIds;
    input.select_all = true;
  }
  const run = await launchRun({
    job: "company_brave_search_workflow",
    runConfig: { ops: {
      company_brave_search_input: { config: input },
    } },
    tags: {
      "processing/task_id": taskId,
      "corpscout/trigger_source": "backoffice",
      "corpscout/request_id": taskId,
      "corpscout/requested_by": requestedBy,
      "corpscout/country_iso2": "SE",
      "corpscout/company_area": "brave",
      "corpscout/company_operation": "process",
    },
  }, options);
  return { ok: true as const, ...run, taskId, runUrl: dagsterRunUrl(run.runId, options.url) };
}
