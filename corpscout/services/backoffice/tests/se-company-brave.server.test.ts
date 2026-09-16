import { describe, expect, it, vi } from "vitest";
import { launchSeCompanyBraveAnalysis } from "~/lib/se-company-brave.server";
import { EMPTY_INFO_FILTERS, PROFILE_DATATYPES } from "~/lib/se-company-info-filters";

function options() {
  return { url: "http://dagster.test/graphql", fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
    data: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "brave-run", status: "QUEUED" } } },
  }))) };
}

describe("Swedish company Brave action", () => {
  it("launches initialization and processing for the exact distinct selection, including IDs from other pages", async () => {
    const opts = options();
    const result = await launchSeCompanyBraveAnalysis({ mode: "ids", companyIds: ["5560004615", "5560160680", "5560004615"] }, "operator", opts);
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body)).variables.executionParams;
    expect(execution.selector.jobName).toBe("company_brave_search_workflow");
    expect(execution.runConfigData.ops).toEqual({
      company_brave_search_input: { config: {
        task_id: result.taskId, source_relation: "corpscout.se_companies_serving",
        company_name_column: "legal_name", country_code: "SE", filters: { company_id: ["5560004615", "5560160680"] },
      } },
    });
    expect(execution.executionMetadata.tags).toContainEqual({ key: "processing/task_id", value: result.taskId });
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/requested_by", value: "operator" });
    expect(result).toMatchObject({ ok: true, runId: "brave-run", runUrl: "http://dagster.test/runs/brave-run" });
  });

  it("passes all applied query filters and exclusions without reading or expanding matching companies", async () => {
    const opts = options();
    await launchSeCompanyBraveAnalysis({ mode: "query", query: {
      companyId: "5560004615", name: "Alpha' OR 1=1 --", entity: "sole", status: "none",
      legalForm: "49", description: "no", source: "esef", datatypes: PROFILE_DATATYPES.map((d) => d.key),
    }, excludedCompanyIds: ["198012345678", "198012345678"] }, "operator", opts);
    const input = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body)).variables.executionParams.runConfigData.ops.company_brave_search_input.config;
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
    await launchSeCompanyBraveAnalysis({ mode: "query", query: { ...EMPTY_INFO_FILTERS, source }, excludedCompanyIds: [] }, "operator", opts);
    const input = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body)).variables.executionParams.runConfigData.ops.company_brave_search_input.config;
    expect(input.filters).toEqual(source && source !== "scb" ? { [`source_${source}`]: ["1"] } : {});
    expect(input.select_all).toBe(true);
    expect(input).not.toHaveProperty("company_ids");
  });

  it.each([null, { mode: "ids", companyIds: [] }, { mode: "ids", companyIds: ["NO:123"] },
    { mode: "query", query: { ...EMPTY_INFO_FILTERS, sql: "1=1" }, excludedCompanyIds: [] },
    { mode: "query", query: { ...EMPTY_INFO_FILTERS, datatypes: ["unknown"] }, excludedCompanyIds: [] },
  ])("rejects empty or invalid selections before launching: %j", async (selection) => {
    const opts = options();
    await expect(launchSeCompanyBraveAnalysis(selection, "operator", opts)).rejects.toThrow();
    expect(opts.fetchImpl).not.toHaveBeenCalled();
  });

  it("surfaces a rejected Dagster configuration instead of claiming the task was submitted", async () => {
    const opts = options();
    opts.fetchImpl.mockResolvedValue(new Response(JSON.stringify({ data: { launchRun: {
      __typename: "RunConfigValidationInvalid", errors: [{ message: "Unknown filter", path: [], reason: "FIELD_NOT_DEFINED" }],
    } } })));
    await expect(launchSeCompanyBraveAnalysis({ mode: "ids", companyIds: ["5560004615"] }, "operator", opts)).rejects.toThrow("Unknown filter");
  });
});
