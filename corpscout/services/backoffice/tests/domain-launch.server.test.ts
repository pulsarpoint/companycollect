import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { launchDomainAction } from "~/lib/domain-launch.server";
import { listDomainPrompts } from "~/lib/domain-prompts.server";
import { saveAndActivateLlmProfile } from "~/lib/llm-settings.server";

let directory: string;
let databasePath: string;
let input: Parameters<typeof launchDomainAction>[0];
beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "domain-launch-")); databasePath = join(directory, "settings.sqlite");
  const profileId = saveAndActivateLlmProfile({ name: "Verifier", provider: "openrouter", model: "chosen/model", baseUrl: "https://openrouter.ai/api/v1", apiKeyEnvironmentVariable: "DOMAIN_KEY" }, databasePath);
  const [prompt] = listDomainPrompts(databasePath);
  input = { operation: "process", profileId, promptId: prompt.promptId, promptRevision: prompt.revision, changedOnly: true, verifyDomains: true, requestedBy: "operator" };
});
afterEach(() => rmSync(directory, { recursive: true, force: true }));

function options() {
  return { url: "http://dagster:3000/graphql", databasePath, fetchImpl: vi.fn(async () => new Response(JSON.stringify({ data: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "domain-run", status: "QUEUED" } } } }))) };
}
function submitted(opts: ReturnType<typeof options>) {
  const call = opts.fetchImpl.mock.calls[0] as unknown as [unknown, RequestInit];
  return JSON.parse(String(call[1].body)).variables.executionParams;
}

describe("domain processing launch", () => {
  it("sends the saved model, prompt and reuse flag without a verification cutoff", async () => {
    const opts = options();
    await launchDomainAction(input, opts);
    const execution = submitted(opts);
    expect(execution.selector.jobName).toBe("se_company_domain_refresh_job");
    expect(execution.runConfigData.ops.se_company_domain_verification.config).toMatchObject({
      changed_only: true,
      verification: { provider: "openrouter", model: "chosen/model", api_key_environment_variable: "DOMAIN_KEY", system_prompt: listDomainPrompts(databasePath)[0].systemPrompt },
    });
    expect(execution.runConfigData.ops.se_company_domain_publish.config.verification)
      .toEqual(execution.runConfigData.ops.se_company_domain_verification.config.verification);
    expect(execution.runConfigData.ops.se_company_domain_verification.config.verification).not.toHaveProperty("max_tokens");
    expect(execution.runConfigData.ops.se_company_domain_verification.config).not.toHaveProperty("max_llm_calls");
    expect(execution.runConfigData.ops.se_company_domain_publish.config).not.toHaveProperty("max_llm_calls");
    expect(execution.executionMetadata.tags).toContainEqual({ key: "corpscout/verification_scope", value: "uncertain_or_conflicting" });
    expect(JSON.stringify(execution)).not.toContain("company_ids");
  });
  it.each(["sync", "process"])("%s can run without any LLM settings when verification is disabled", async (operation) => {
    const opts = options();
    await launchDomainAction({ ...input, operation, verifyDomains: false, profileId: "", promptId: "" }, opts);
    const execution = submitted(opts);
    expect(JSON.stringify(execution)).not.toContain("system_prompt");
    expect(JSON.stringify(execution)).not.toContain("api_key_environment_variable");
    if (operation === "sync") {
      expect(execution.runConfigData.ops).not.toHaveProperty("se_company_domain_publish");
      expect(execution.runConfigData.ops).not.toHaveProperty("se_company_domain_verification");
    } else {
      expect(execution.runConfigData.ops.se_company_domain_verification.config).not.toHaveProperty("verification");
    }
  });
  it("rejects stale prompts and missing model or prompt before submission", async () => {
    const opts = options();
    for (const change of [{ promptRevision: 999 }, { profileId: "missing" }, { promptId: "missing" }]) {
      await expect(launchDomainAction({ ...input, ...change }, opts)).rejects.toThrow();
    }
    expect(opts.fetchImpl).not.toHaveBeenCalled();
  });
});
