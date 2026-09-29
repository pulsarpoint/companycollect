import { describe, expect, it, vi } from "vitest";
import { launchDomainAction } from "~/lib/domain-launch.server";

function options() {
  return { url: "http://dagster:3000/graphql", fetchImpl: vi.fn(async () => new Response(JSON.stringify({ data: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "domain-run", status: "QUEUED" } } } }))) };
}
function submitted(opts: ReturnType<typeof options>) {
  const call = opts.fetchImpl.mock.calls[0] as unknown as [unknown, RequestInit];
  return JSON.parse(String(call[1].body)).variables.executionParams;
}

describe("domain processing launch", () => {
  it.each(["sync", "process"])("%s publishes source claims without an LLM verification step", async (operation) => {
    const opts = options();
    await launchDomainAction({ operation, changedOnly: true, requestedBy: "operator" }, opts);
    const execution = submitted(opts);
    expect(execution.selector.jobName).toBe(operation === "process" ? "se_company_domain_refresh_job" : "se_company_domain_sync_job");
    const ops = execution.runConfigData.ops;
    for (const source of ["brave", "wikidata", "esef_filing", "common_crawl_identity", "crawler_lookup"]) {
      expect(ops[`se_company_domain_suggestions_${source}`].config).toEqual({ execute: true, page_size: 5_000 });
    }
    expect(ops).not.toHaveProperty("se_company_domain_verification");
    if (operation === "process") {
      expect(ops.se_company_domain_publish.config).toEqual({changed_only: true, page_size: 1_000});
    } else {
      expect(ops).not.toHaveProperty("se_company_domain_publish");
    }
    expect(JSON.stringify(execution)).not.toMatch(/api_key|system_prompt|profile_id|verification_scope/);
    expect(execution.executionMetadata.tags).toContainEqual({key: "corpscout/requested_by", value: "operator"});
  });
  it.each([{operation: "unknown", requestedBy: "operator"}, {operation: "process", requestedBy: ""}])("rejects invalid operation or operator before submission", async (input) => {
    const opts = options();
    await expect(launchDomainAction({...input, changedOnly: true}, opts)).rejects.toThrow();
    expect(opts.fetchImpl).not.toHaveBeenCalled();
  });
});
