import { describe, expect, it, vi } from "vitest";
import { launchSeCompanyBraveAnalysis } from "~/lib/se-company-brave.server";
import { EMPTY_INFO_FILTERS, PROFILE_DATATYPES } from "~/lib/se-company-info-filters";

const submissionId = "11111111-1111-4111-8111-111111111111";
function options() {
  return { url: "http://dagster.test/graphql", fetchImpl: vi.fn<typeof fetch>().mockResolvedValueOnce(new Response(JSON.stringify({data: {runsOrError: {__typename: "Runs", results: []}}}))).mockResolvedValue(new Response(JSON.stringify({
    data: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "brave-run", status: "QUEUED" } } },
  }))) };
}

describe("Swedish company Brave action", () => {
  it("prepares the exact distinct selection without bypassing model selection and processing", async () => {
    const opts = options();
    const result = await launchSeCompanyBraveAnalysis({ mode: "ids", companyIds: ["5560004615", "5560160680", "5560004615"] }, "operator", submissionId, opts);
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls[1][1]?.body)).variables.executionParams;
    expect(execution.selector.jobName).toBe("company_brave_queue_input_job");
    expect(execution.runConfigData.ops).toEqual({
      company_brave_queue_input: { config: {
        submission_id: submissionId, queue_scope: "workspace:SE", source_name: "backoffice:se-companies", source_relation: "corpscout.se_companies_serving",
        company_name_column: "legal_name", country_code: "SE", filters: { company_id: ["5560004615", "5560160680"] },
      } },
    });
    expect(execution.executionMetadata.tags).toContainEqual({ key: "processing/submission_id", value: submissionId });
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/requested_by", value: "operator" });
    expect(execution.runConfigData.ops.company_brave_queue_input.config).not.toHaveProperty("task_id");
    expect(execution.runConfigData.ops).not.toHaveProperty("company_brave_search_results");
    expect(result).toMatchObject({ ok: true, runId: "brave-run", runUrl: "http://dagster.test/runs/brave-run" });
  });

  it("passes all applied query filters and exclusions without reading or expanding matching companies", async () => {
    const opts = options();
    await launchSeCompanyBraveAnalysis({ mode: "query", query: {
      companyId: "5560004615", name: "Alpha' OR 1=1 --", entity: "sole", status: "none",
      legalForm: "49", description: "no", source: "esef", datatypes: PROFILE_DATATYPES.map((d) => d.key),
    }, excludedCompanyIds: ["198012345678", "198012345678"] }, "operator", submissionId, opts);
    const input = JSON.parse(String(opts.fetchImpl.mock.calls[1][1]?.body)).variables.executionParams.runConfigData.ops.company_brave_queue_input.config;
    expect(input).toMatchObject({
      company_name_pattern: "%Alpha' OR 1=1 --%", company_id_length: 12,
      filters: { company_id: ["5560004615"], status: [""], legal_form_code: ["49"], has_description: ["0"], source_esef: ["1"],
        ...Object.fromEntries(PROFILE_DATATYPES.map((d) => [d.key, ["1"]])),
      }, excluded_company_ids: ["198012345678"], select_all: true,
    });
    expect(input).not.toHaveProperty("max_companies");
    expect(input).not.toHaveProperty("source_final");
    expect(input).not.toHaveProperty("company_ids");
  });

  it.each(["", "scb", "bolagsverket", "wikidata"])("supports selecting all with source '%s' using compact filters", async (source) => {
    const opts = options();
    await launchSeCompanyBraveAnalysis({ mode: "query", query: { ...EMPTY_INFO_FILTERS, source }, excludedCompanyIds: [] }, "operator", submissionId, opts);
    const input = JSON.parse(String(opts.fetchImpl.mock.calls[1][1]?.body)).variables.executionParams.runConfigData.ops.company_brave_queue_input.config;
    expect(input.filters).toEqual(source && source !== "scb" ? { [`source_${source}`]: ["1"] } : {});
    expect(input.select_all).toBe(true);
    expect(input).not.toHaveProperty("company_ids");
  });

  it.each([null, { mode: "ids", companyIds: [] }, { mode: "ids", companyIds: ["NO:123"] },
    { mode: "query", query: { ...EMPTY_INFO_FILTERS, sql: "1=1" }, excludedCompanyIds: [] },
    { mode: "query", query: { ...EMPTY_INFO_FILTERS, datatypes: ["unknown"] }, excludedCompanyIds: [] },
  ])("rejects empty or invalid selections before launching: %j", async (selection) => {
    const opts = options();
    await expect(launchSeCompanyBraveAnalysis(selection, "operator", submissionId, opts)).rejects.toThrow();
    expect(opts.fetchImpl).not.toHaveBeenCalled();
  });

  it("surfaces a rejected Dagster configuration instead of claiming the task was submitted", async () => {
    const opts = options();
    opts.fetchImpl.mockResolvedValue(new Response(JSON.stringify({ data: { launchRun: {
      __typename: "RunConfigValidationInvalid", errors: [{ message: "Unknown filter", path: [], reason: "FIELD_NOT_DEFINED" }],
    } } })));
    await expect(launchSeCompanyBraveAnalysis({ mode: "ids", companyIds: ["5560004615"] }, "operator", submissionId, opts)).rejects.toThrow("Unknown filter");
  });
});

it("reuses an acknowledged submission and rejects reusing its receipt for different companies", async () => {
  const opts = options();
  const selection = {mode: "ids", companyIds: ["5560004615"]};
  await launchSeCompanyBraveAnalysis(selection, "operator", submissionId, opts);
  const tags = JSON.parse(String(opts.fetchImpl.mock.calls[1][1]?.body)).variables.executionParams.executionMetadata.tags;
  opts.fetchImpl.mockImplementation(async () => new Response(JSON.stringify({data: {runsOrError: {__typename: "Runs", results: [
    {runId: "original", status: "SUCCESS", tags},
  ]}}})));
  expect(await launchSeCompanyBraveAnalysis(selection, "operator", submissionId, opts)).toMatchObject({runId: "original", status: "SUCCESS"});
  await expect(launchSeCompanyBraveAnalysis({...selection, companyIds: ["5560160680"]}, "operator", submissionId, opts)).rejects.toThrow("different selection");
  expect(opts.fetchImpl).toHaveBeenCalledTimes(4);
});

it("retries only failed-company membership through the normal draft import receipt", async () => {
  const {retryBraveFailures} = await import("~/lib/se-company-brave.server");
  const opts = options();
  await retryBraveFailures(submissionId, "22222222-2222-4222-8222-222222222222", "operator", opts);
  const config = JSON.parse(String(opts.fetchImpl.mock.calls[1][1]?.body)).variables.executionParams.runConfigData.ops.company_brave_queue_input.config;
  expect(config).toMatchObject({source_relation: "corpscout.company_brave_search_results", source_final: true,
    filters: {task_id: [submissionId], status: ["error"]}, country_code: "SE", queue_scope: "workspace:SE"});
});
