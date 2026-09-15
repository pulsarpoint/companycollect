import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { launchPeopleAction } from "~/lib/people-launch.server";
import { getPeoplePrompt, listPeoplePrompts, savePeoplePrompt } from "~/lib/people-prompts.server";
import { saveAndActivateLlmProfile } from "~/lib/llm-settings.server";

let directory: string;
let databasePath: string;
let input: Parameters<typeof launchPeopleAction>[0];
beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "people-launch-")); databasePath = join(directory, "settings.sqlite");
  const profileId = saveAndActivateLlmProfile({ name: "Chosen LLM", provider: "openrouter", model: "chosen/model", baseUrl: "https://openrouter.ai/api/v1", apiKeyEnvironmentVariable: "CUSTOM_LLM_KEY" }, databasePath);
  const [prompt] = listPeoplePrompts(databasePath);
  input = { operation: "process", profileId, promptId: prompt.promptId, promptRevision: prompt.revision, changedOnly: true, requestedBy: "operator" };
  process.env.CUSTOM_LLM_KEY = "secret-must-not-travel";
});
afterEach(() => { delete process.env.CUSTOM_LLM_KEY; rmSync(directory, { recursive: true, force: true }); });

function options() {
  const fetchImpl = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: { launchRun: {
    __typename: "LaunchRunSuccess", run: { runId: "people-run", status: "QUEUED" },
  } } }), { status: 200 }));
  return { url: "http://dagster:3000/graphql", databasePath, fetchImpl };
}

describe("all-company People launches", () => {
  it("sends the saved model and exact prompt to the match asset, with no company scope", async () => {
    const opts = options();
    const prompt = getPeoplePrompt(input.promptId, databasePath)!;
    const result = await launchPeopleAction(input, opts);
    expect(result).toMatchObject({ ok: true, runId: "people-run", status: "QUEUED" });
    const wire = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body));
    const execution = wire.variables.executionParams;
    expect(execution.selector.jobName).toBe("se_company_person_refresh_job");
    expect(execution.selector).not.toHaveProperty("assetSelection");
    const ops = execution.runConfigData.ops;
    expect(Object.keys(ops)).toHaveLength(8);
    expect(ops.se_company_person_match_input.config.changed_only).toBe(true);
    expect(ops.se_company_person_match.config).toMatchObject({
      model: "chosen/model", provider: "openrouter", api_key_environment_variable: "CUSTOM_LLM_KEY",
      system_prompt: prompt.systemPrompt, prompt_version: `people:${prompt.promptId}:r1`, changed_only: true,
    });
    expect(ops.se_company_person_suggestions_ratsit.config.execute).toBe(true);
    const body = JSON.stringify(wire);
    expect(body).not.toContain("company_ids");
    expect(body).not.toContain("selection");
    expect(body).not.toContain("secret-must-not-travel");
    savePeoplePrompt({ ...prompt, systemPrompt: "Later edit" }, databasePath);
    expect(ops.se_company_person_match.config.system_prompt).toBe(prompt.systemPrompt);
  });
  it("syncs all four sources and input hashes without a model, prompt or publishing", async () => {
    const opts = options();
    opts.databasePath = join(directory, "no-settings-needed.sqlite");
    await launchPeopleAction({ ...input, operation: "sync", profileId: "", promptId: "", promptRevision: 0 }, opts);
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body)).variables.executionParams;
    expect(execution.selector.jobName).toBe("se_company_person_sync_job");
    expect(execution.selector).not.toHaveProperty("assetSelection");
    expect(execution.runConfigData.ops).toEqual({
      se_company_person_suggestions_bolagsverket: { config: { execute: true, page_size: 10_000 } },
      se_company_person_suggestions_esef: { config: { execute: true, page_size: 10_000 } },
      se_company_person_suggestions_wikidata: { config: { execute: true, page_size: 10_000 } },
      se_company_person_suggestions_ratsit: { config: { execute: true, page_size: 10_000 } },
      se_company_person_normalize: { config: { changed_only: true } },
      se_company_person_match_input: { config: { changed_only: true } },
    });
    expect(JSON.stringify(execution)).not.toContain("system_prompt");
    expect(JSON.stringify(execution)).not.toContain("company_ids");
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/people_operation", value: "sync" });
  });
  it("can force LLM matching while keeping normalization and publication incremental", async () => {
    const opts = options();
    await launchPeopleAction({ ...input, changedOnly: false }, opts);
    const ops = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body)).variables.executionParams.runConfigData.ops;
    expect(ops.se_company_person_match.config.changed_only).toBe(false);
    expect(ops.se_company_person_normalize.config.changed_only).toBe(true);
    expect(ops.se_company_person_match_input.config.changed_only).toBe(true);
    expect(ops.se_company_person_publish.config.changed_only).toBe(true);
  });
  it("rejects missing models, prompts and stale prompt previews before sending a request", async () => {
    const opts = options();
    for (const changes of [{ profileId: "missing" }, { promptId: "missing" }, { promptRevision: 99 }, { operation: "delete" }, { operation: "publish" }, { operation: "refresh" }, { requestedBy: "" }]) {
      await expect(launchPeopleAction({ ...input, ...changes }, opts)).rejects.toThrow();
    }
    expect(opts.fetchImpl).not.toHaveBeenCalled();
  });
  it("reports Dagster launch errors without claiming a run was submitted", async () => {
    const opts = options();
    opts.fetchImpl.mockResolvedValue(new Response(JSON.stringify({ data: { launchRun: { __typename: "PythonError", message: "code location unavailable" } } })));
    await expect(launchPeopleAction(input, opts)).rejects.toThrow("code location unavailable");
  });
});
