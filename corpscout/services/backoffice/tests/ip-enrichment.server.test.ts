import { beforeEach, describe, expect, it, vi } from "vitest";
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/dagster.server", () => dagster);
import { addIpsToEnrichmentQueue, ipEnrichmentQueueSubmission } from "~/lib/ip-enrichment.server";

const submission = "11111111-1111-4111-8111-111111111111";
beforeEach(() => { vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([]); dagster.launchRun.mockResolvedValue({runId: "new", status: "QUEUED"}); });

describe("IP enrichment queue submission", () => {
  it("appends selected addresses from multiple pages to the workspace draft via the input asset only", async () => {
    const result = await addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8", "2001:4860::8888", "8.8.8.8"] }, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: "ip_enrichment_input_job", assetSelection: ["ip_enrichment_input"],
      runConfig: {ops: {ip_enrichment_input: {config: {
        source_name: "backoffice:ip-addresses", source_relation: "corpscout.commoncrawl_ip_addresses", observed_at_column: "last_seen",
        filters: { ip: ["8.8.8.8", "2001:4860::8888"] }, queue_scope: "workspace", submission_id: submission,
      }}}},
      tags: expect.objectContaining({"processing/submission_id": submission, "backoffice/action": "add-ip-enrichment-input", "corpscout/requested_by": "operator"}),
    }));
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).not.toHaveProperty("task_id");
    expect(result).toEqual({ok: true, runId: "new", status: "QUEUED", runUrl: "http://dagster/runs/new"});
  });

  it.each(["any", "4", "6"])("submits all matching addresses compactly for version %s", async (version) => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "2001:db8:", version, asn: [], country: [], region: [], city: [] }, excludedIps: ["2001:db8::1", "2001:db8::1"] }, submission, "operator");
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).toMatchObject({ source_relation: "corpscout.ip_enrichment_search", observed_at_column: "last_seen", select_all: true, ip_search: "2001:db8:",
      filters: version === "any" ? {} : { ip_version: [version] }, excluded_ips: ["2001:db8::1"] });
    expect(config).not.toHaveProperty("max_rows");
    expect(config).not.toHaveProperty("ips");
  });

  it("maps the list filters to exact-match columns of the search table", async () => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "4", asn: ["3301", "15169"], country: ["SE"], region: ["AB"], city: ["Stockholm"] }, excludedIps: [] }, submission, "operator");
    const call = dagster.launchRun.mock.calls[0][0];
    expect(call.runConfig.ops.ip_enrichment_input.config).toEqual({
      source_name: "backoffice:ip-addresses", source_relation: "corpscout.ip_enrichment_search", observed_at_column: "last_seen",
      select_all: true, ip_search: "", excluded_ips: [], queue_scope: "workspace", submission_id: submission,
      filters: { asn: ["15169", "3301"], country_iso_code: ["SE"], subdivision_iso_code: ["AB"], city_name: ["Stockholm"], ip_version: ["4"] },
    });
    // The receipt fingerprint covers the filters: another filter set is another selection.
    const tags = call.tags;
    dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
    await expect(addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "4", asn: ["3301"], country: ["SE"], region: ["AB"], city: ["Stockholm"] }, excludedIps: [] }, submission, "operator")).rejects.toThrow("another selection");
    // The same filters in another order are the same selection.
    expect(await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "4", asn: ["15169", "3301"], country: ["SE"], region: ["AB"], city: ["Stockholm"] }, excludedIps: [] }, submission, "operator")).toMatchObject({runId: "new"});
  });

  it("accepts a selection saved before the location filters existed", async () => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "any" }, excludedIps: [] }, submission, "operator");
    expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config).toMatchObject({ select_all: true, filters: {} });
  });

  it("recovers an acknowledged submission and rejects a changed selection", async () => {
    const selection = { mode: "ips", ips: ["8.8.8.8"] };
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    const tags = dagster.launchRun.mock.calls[0][0].tags;
    dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
    expect(await addIpsToEnrichmentQueue(selection, submission, "operator")).toMatchObject({runId: "new"});
    expect(dagster.launchRun).toHaveBeenCalledTimes(1);
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["1.1.1.1"] }, submission, "operator")).rejects.toThrow("another selection");
    dagster.listRuns.mockResolvedValue([{runId: "failed", status: "FAILURE", tags}]);
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledTimes(2);  // a failed import is retried with the same receipt
  });

  it.each([
    null,
    { mode: "ips", ips: [] },
    { mode: "ips", ips: ["bad"] },
    { mode: "ips", ips: ["fe80::1%eth0"] },
    { mode: "ips", ips: ["8.8.8.8"], source_relation: "arbitrary" },
    { mode: "all", filters: { search: "' OR 1=1", version: "any" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: 4 }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", sql: "1=1" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any" }, excludedIps: ["invalid"] },
    { mode: "all", filters: { search: "", version: "any", asn: ["AS15169"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", asn: ["1 OR 1=1"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", country: ["se"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", country: "SE" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", region: ["AB"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", country: ["SE", "NO"], city: ["Oslo"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", country: ["SE"], city: ["x\u0000"] }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", asn: Array.from({ length: 51 }, (_, i) => String(i + 1)) }, excludedIps: [] },
  ])("rejects invalid selections before contacting Dagster: %j", async (selection) => {
    await expect(addIpsToEnrichmentQueue(selection, submission, "operator")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects a malformed submission id before contacting Dagster", async () => {
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8"] }, "not-a-uuid", "operator")).rejects.toThrow("submission ID");
    expect(dagster.listRuns).not.toHaveBeenCalled();
  });

  it("returns the resolved draft only for IP enrichment import runs", async () => {
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_input_job", status: "SUCCESS", tags: {"backoffice/action": "add-ip-enrichment-input", "processing/task_id": submission}});
    expect(await ipEnrichmentQueueSubmission(submission)).toMatchObject({finished: true, taskId: submission});
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_workflow", status: "SUCCESS", tags: {}});
    await expect(ipEnrichmentQueueSubmission(submission)).rejects.toThrow("not found");
  });
});
