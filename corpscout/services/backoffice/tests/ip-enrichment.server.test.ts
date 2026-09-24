import { describe, expect, it, vi } from "vitest";
import { launchIpEnrichment } from "~/lib/ip-enrichment.server";

function options() {
  return {
    url: "http://dagster.test/graphql",
    fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            launchRun: {
              __typename: "LaunchRunSuccess",
              run: { runId: "enrichment-run", status: "QUEUED" },
            },
          },
        }),
      ),
    ),
  };
}

describe("IP enrichment submission", () => {
  it("launches both steps with the same task and selected addresses from multiple pages", async () => {
    const opts = options();
    const result = await launchIpEnrichment(
      { mode: "ips", ips: ["8.8.8.8", "2001:4860::8888", "8.8.8.8"] },
      "operator",
      opts,
    );
    const execution = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body))
      .variables.executionParams;
    expect(execution.selector.jobName).toBe("ip_enrichment_workflow");
    expect(execution.runConfigData.ops).toEqual({
      ip_enrichment_input: {
        config: {
          task_id: result.taskId,
          source_name: "backoffice:ip-addresses",
          source_relation: "corpscout.commoncrawl_ip_addresses",
          observed_at_column: "last_seen",
          filters: { ip: ["8.8.8.8", "2001:4860::8888"] },
        },
      },
      ip_enrichment_results: {
        config: { task_id: result.taskId, max_requests: null },
      },
    });
    expect(execution.executionMetadata.tags).toContainEqual({
      key: "corpscout/requested_by",
      value: "operator",
    });
    expect(result.runUrl).toBe("http://dagster.test/runs/enrichment-run");
  });

  it.each(["any", "4", "6"])(
    "submits all matching addresses compactly for version %s",
    async (version) => {
      const opts = options();
      await launchIpEnrichment(
        {
          mode: "all",
          filters: { search: "2001:db8:", version },
          excludedIps: ["2001:db8::1", "2001:db8::1"],
        },
        "operator",
        opts,
      );
      const input = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body))
        .variables.executionParams.runConfigData.ops.ip_enrichment_input.config;
      expect(input).toMatchObject({
        source_relation: "corpscout.commoncrawl_ip_addresses",
        observed_at_column: "last_seen",
        select_all: true,
        ip_search: "2001:db8:",
        filters: version === "any" ? {} : { ip_version: [version] },
        excluded_ips: ["2001:db8::1"],
      });
      expect(input).not.toHaveProperty("max_rows");
      expect(input).not.toHaveProperty("ips");
    },
  );

  it("supports the entire inventory without a pagination limit", async () => {
    const opts = options();
    await launchIpEnrichment(
      { mode: "all", filters: { search: "", version: "any" }, excludedIps: [] },
      "operator",
      opts,
    );
    const input = JSON.parse(String(opts.fetchImpl.mock.calls[0][1]?.body))
      .variables.executionParams.runConfigData.ops.ip_enrichment_input.config;
    expect(input).toMatchObject({
      select_all: true,
      ip_search: "",
      filters: {},
    });
    expect(input).not.toHaveProperty("max_rows");
  });

  it.each([
    null,
    { mode: "ips", ips: [] },
    { mode: "ips", ips: ["bad"] },
    { mode: "ips", ips: ["fe80::1%eth0"] },
    { mode: "ips", ips: ["8.8.8.8"], source_relation: "arbitrary" },
    {
      mode: "all",
      filters: { search: "' OR 1=1", version: "any" },
      excludedIps: [],
    },
    { mode: "all", filters: { search: "", version: 4 }, excludedIps: [] },
    {
      mode: "all",
      filters: { search: "", version: "any", sql: "1=1" },
      excludedIps: [],
    },
    {
      mode: "all",
      filters: { search: "", version: "any" },
      excludedIps: ["invalid"],
    },
  ])(
    "rejects invalid selections before contacting Dagster: %j",
    async (selection) => {
      const opts = options();
      await expect(
        launchIpEnrichment(selection, "operator", opts),
      ).rejects.toThrow();
      expect(opts.fetchImpl).not.toHaveBeenCalled();
    },
  );
});
