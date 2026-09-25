import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { launchCompanyAction } from "~/lib/company-actions.server";
import { listPeoplePrompts } from "~/lib/people-prompts.server";
import { saveAndActivateLlmProfile } from "~/lib/llm-settings.server";
import { ACTIVE_COMPANY_RUN_STATUSES } from "~/lib/company-actions";

let directory: string;
let databasePath: string;
let input: Parameters<typeof launchCompanyAction>[0];
beforeEach(() => {
  vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "11".repeat(32));
  directory = mkdtempSync(join(tmpdir(), "company-actions-")); databasePath = join(directory, "settings.sqlite");
  const profileId = saveAndActivateLlmProfile({ name: "Chosen LLM", provider: "openrouter", model: "chosen/model", baseUrl: "https://openrouter.ai/api/v1", apiKey: "secret-must-not-travel" }, databasePath);
  const [prompt] = listPeoplePrompts(databasePath);
  input = { area: "info", operation: "process", profileId, promptId: prompt.promptId, promptRevision: prompt.revision, changedOnly: true, llmMaxCompanies: 1234, requestedBy: "operator" };
});
afterEach(() => { vi.unstubAllEnvs(); rmSync(directory, { recursive: true, force: true }); });

function options() {
  const fetchImpl = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
    const { variables } = JSON.parse(String(init?.body));
    return new Response(JSON.stringify({ data: variables.executionParams ? { launchRun: {
      __typename: "LaunchRunSuccess", run: { runId: "company-run", status: "QUEUED" },
    } } : { runsOrError: { __typename: "Runs", results: [] } } }));
  });
  return { url: "http://dagster:3000/graphql", databasePath, fetchImpl };
}

describe("company action dispatch", () => {
  it.each(["QUEUED", "STARTED", "CANCELING"])("blocks a sync while full processing is %s", async (status) => {
    const opts = options();
    opts.fetchImpl.mockImplementation(async (_url, init) => {
      const { variables } = JSON.parse(String(init?.body));
      expect(variables.filter.statuses).toEqual(ACTIVE_COMPANY_RUN_STATUSES);
      expect(variables.limit).toBe(1);
      return new Response(JSON.stringify({ data: { runsOrError: { __typename: "Runs", results:
        variables.filter.pipelineName.endsWith("refresh_job") ? [{ runId: "existing", jobName: variables.filter.pipelineName, status, tags: [] }] : [],
      } } }));
    });
    expect(await launchCompanyAction({ ...input, operation: "sync" }, opts)).toMatchObject({ ok: false, runId: "existing", status });
    expect(opts.fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("holds the workflow lock across concurrent requests but permits a different workflow", async () => {
    const opts = options();
    const first = launchCompanyAction({ ...input, operation: "sync" }, opts);
    const duplicate = launchCompanyAction(input, opts);
    const other = launchCompanyAction({ ...input, area: "finance", operation: "sync" }, opts);
    expect(await duplicate).toMatchObject({ ok: false, error: expect.stringContaining("already being submitted") });
    expect(await first).toMatchObject({ ok: true });
    expect(await other).toMatchObject({ ok: true });
    const launches = opts.fetchImpl.mock.calls.filter(([, init]) => JSON.parse(String(init?.body)).variables.executionParams);
    expect(launches).toHaveLength(2);
  });

  it("fails closed on a status error and releases the submission lock for a later retry", async () => {
    const opts = options();
    opts.fetchImpl.mockRejectedValueOnce(new Error("offline"));
    await expect(launchCompanyAction(input, opts)).rejects.toThrow("Could not verify");
    expect(opts.fetchImpl).toHaveBeenCalledTimes(2);
    expect(await launchCompanyAction(input, opts)).toMatchObject({ ok: true });
  });

  it("releases the lock after a rejected launch", async () => {
    const opts = options();
    const original = opts.fetchImpl.getMockImplementation()!;
    opts.fetchImpl.mockImplementation(async (url, init) => {
      if (JSON.parse(String(init?.body)).variables.executionParams) throw new Error("launch rejected");
      return original(url, init);
    });
    await expect(launchCompanyAction(input, opts)).rejects.toThrow("launch rejected");
    opts.fetchImpl.mockImplementation(original);
    expect(await launchCompanyAction(input, opts)).toMatchObject({ ok: true });
  });

  it.each([
    ["info", "sync", "se_company_basic_info_sync_job", 5],
    ["info", "process", "se_company_basic_info_refresh_job", 7],
    ["finance", "sync", "se_company_financial_sync_job", 5],
    ["finance", "process", "se_company_financial_refresh_job", 6],
    ["addresses", "sync", "se_company_address_sync_job", 5],
    ["addresses", "process", "se_company_address_refresh_job", 7],
    ["people", "sync", "se_company_person_sync_job", 6],
    ["people", "process", "se_company_person_refresh_job", 8],
    ["domains", "sync", "se_company_domain_sync_job", 4],
    ["domains", "process", "se_company_domain_refresh_job", 6],
  ] as const)("launches %s / %s as one complete global job", async (area, operation, job, stepCount) => {
    const opts = options();
    expect(await launchCompanyAction({ ...input, area, operation }, opts)).toMatchObject({ ok: true, runId: "company-run" });
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls.find(([, init]) => JSON.parse(String(init?.body)).variables.executionParams)?.[1]?.body)).variables.executionParams;
    expect(execution.selector.jobName).toBe(job);
    expect(execution.selector).not.toHaveProperty("assetSelection");
    expect(Object.keys(execution.runConfigData.ops)).toHaveLength(stepCount);
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/company_area", value: area });
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/company_operation", value: operation });
    const body = JSON.stringify(execution);
    expect(body).not.toContain("company_ids");
    expect(body).not.toContain("partition");
    expect(body).not.toContain("secret-must-not-travel");
    if (area === "info" || area === "finance") {
      const ops = execution.runConfigData.ops;
      const source = area === "info" ? "se_basic_info_suggestions_scb" : "se_company_financial_suggestions_ratsit";
      expect(ops[source].config).toEqual({ execute: true, page_size: area === "info" ? 20000 : 5000 });
      if (operation === "process") expect(ops[area === "info" ? "se_company_basic_info_publish" : "se_company_financial_publish"].config.changed_only).toBe(true);
      if (area === "finance") {
        expect(ops.se_ratsit_financial_periods_usd.config.execute).toBe(true);
        expect(body).not.toContain("system_prompt");
        expect(body).not.toContain("api_key_environment_variable");
      }
    }
  });

  it.each(["sync", "process"])("address %s uses the source, normalization and geocoding stages without model settings", async (operation) => {
    const opts = options();
    await launchCompanyAction({ ...input, area: "addresses", operation, profileId: "", promptId: "", promptRevision: 0 }, opts);
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls.find(([, init]) => JSON.parse(String(init?.body)).variables.executionParams)?.[1]?.body)).variables.executionParams;
    const ops = execution.runConfigData.ops;
    const expected = {
      se_company_address_suggestions_scb: { config: { execute: true, page_size: 10000 } },
      se_company_address_suggestions_bolagsverket: { config: { execute: true, page_size: 10000 } },
      se_company_address_suggestions_ratsit: { config: { execute: true, page_size: 10000 } },
      se_company_address_suggestions_esef: { config: { execute: true, page_size: 10000 } },
      se_company_address_normalize: { config: { changed_only: true, page_size: 20000 } },
      ...(operation === "process" ? {
        se_address_geocodes_warm: { config: { chunk_size: 150000, limit: 0 } },
        se_company_address_publish: { config: { changed_only: true, page_size: 20000 } },
      } : {}),
    };
    expect(ops).toEqual(expected);
    expect(JSON.stringify(execution)).not.toContain("llm");
    expect(JSON.stringify(execution)).not.toContain("system_prompt");
  });

  it("passes the chosen info model, encrypted credential and explicit LLM cap", async () => {
    const opts = options();
    await launchCompanyAction(input, opts);
    const config = JSON.parse(String(opts.fetchImpl.mock.calls.find(([, init]) => JSON.parse(String(init?.body)).variables.executionParams)?.[1]?.body)).variables.executionParams.runConfigData.ops.se_basic_info_suggestions_llm.config;
    expect(config).toEqual({ execute: true, max_companies: 1234, llm: {
      provider: "openrouter", model: "chosen/model", base_url: "https://openrouter.ai/api/v1",
      api_key_encrypted: expect.stringMatching(/^v1\./), temperature: 0, max_tokens: 6000, concurrency: 1,
    } });
  });

  it.each([["info", "sync"], ["finance", "sync"], ["finance", "process"], ["addresses", "sync"], ["addresses", "process"], ["people", "sync"]])
    ("needs no settings for %s / %s", async (area, operation) => {
      const opts = options(); opts.databasePath = join(directory, "missing-settings.sqlite");
      await expect(launchCompanyAction({ ...input, area, operation, profileId: "", promptId: "", llmMaxCompanies: 0 }, opts)).resolves.toMatchObject({ ok: true });
    });

  it("refuses invalid or retired operations, missing profiles and unsafe caps before launching", async () => {
    const opts = options();
    for (const changes of [{ area: "other" }, { operation: "publish" }, { operation: "extract" }, { requestedBy: " " }, { profileId: "missing" }, ...[0, -1, 1.5, 1000001, NaN].map((llmMaxCompanies) => ({ llmMaxCompanies }))]) {
      await expect(launchCompanyAction({ ...input, ...changes }, opts)).rejects.toThrow();
    }
    expect(opts.fetchImpl.mock.calls.some(([, init]) => JSON.parse(String(init?.body)).variables.executionParams)).toBe(false);
  });
});
