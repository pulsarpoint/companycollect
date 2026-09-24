import { beforeEach, expect, it, vi } from "vitest";
import { addDomainToWebtechQueue, addSeDomainsToWebtechQueue, webtechQueueSubmission } from "~/lib/webtech-queue.server";
import { launchRun, listRuns, runStatus } from "~/lib/dagster.server";
vi.mock("~/lib/dagster.server", () => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: (id: string) => `http://dagster/runs/${id}`}));
const submission = "11111111-1111-4111-8111-111111111111";
const runId = "22222222-2222-4222-8222-222222222222";
const task = "33333333-3333-4333-8333-333333333333";
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(listRuns).mockResolvedValue([]);
  vi.mocked(launchRun).mockResolvedValue({runId, status: "QUEUED"});
});
it("adds the route domain to the shared draft using only the input asset", async () => {
  const receipt = await addDomainToWebtechQueue(" 100.SE ", submission, "operator");
  expect(launchRun).toHaveBeenCalledWith({
    job: "webtech_scan_input_job", assetSelection: ["webtech_scan_input"],
    runConfig: {ops: {webtech_scan_input: {config: {submission_id: submission, queue_scope: "workspace", targets: ["100.se"], source_name: "backoffice:se-company-domain"}}}},
    tags: {"processing/submission_id": submission, "webtech/queued_domain": "100.se", "corpscout/requested_by": "operator", "backoffice/action": "add-webtech-input"},
  });
  expect(receipt).toMatchObject({runId, status: "QUEUED"});
  const config = JSON.stringify(vi.mocked(launchRun).mock.calls[0][0]);
  expect(config).not.toContain("webtech_scan_results");
  expect(config).not.toContain("task_id");
  expect(config).not.toContain("force_rescan");
});
it.each(["", "https://100.se/path", "100.se/path", "127.0.0.1", "bad domain"])("rejects invalid domain %s", async domain => {
  await expect(addDomainToWebtechQueue(domain, submission, "operator")).rejects.toThrow();
  expect(launchRun).not.toHaveBeenCalled();
});
it("rejects invalid submission identity", async () => {
  await expect(addDomainToWebtechQueue("100.se", "invalid", "operator")).rejects.toThrow();
  expect(launchRun).not.toHaveBeenCalled();
});
it.each(["QUEUED", "STARTED", "SUCCESS"])("reuses an acknowledged %s import after a network retry", async status => {
  vi.mocked(listRuns).mockResolvedValue([{runId,status,tags:{"webtech/queued_domain":"100.se"}}] as never);
  expect(await addDomainToWebtechQueue("100.se", submission, "operator")).toMatchObject({runId,status});
  expect(launchRun).not.toHaveBeenCalled();
});
it.each(["FAILURE", "CANCELED"])("retries a %s import with its original submission receipt", async status => {
  vi.mocked(listRuns).mockResolvedValue([{runId,status,tags:{"webtech/queued_domain":"100.se"}}] as never);
  await addDomainToWebtechQueue("100.se", submission, "operator");
  expect(launchRun).toHaveBeenCalledWith(expect.objectContaining({runConfig:{ops:{webtech_scan_input:{config:expect.objectContaining({submission_id:submission})}}}}));
});
it("does not reuse a submission for another domain", async () => {
  vi.mocked(listRuns).mockResolvedValue([{runId,status:"SUCCESS",tags:{"webtech/queued_domain":"other.se"}}] as never);
  await expect(addDomainToWebtechQueue("100.se", submission, "operator")).rejects.toThrow("another domain");
  expect(launchRun).not.toHaveBeenCalled();
});
it("resolves the queue task only from the input run's saved tag", async () => {
  vi.mocked(runStatus).mockResolvedValue({runId,jobName:"webtech_scan_input_job",status:"SUCCESS",tags:{"processing/task_id":task,"backoffice/action":"add-webtech-input"}} as never);
  expect(await webtechQueueSubmission(runId)).toEqual({ok:true,runId,status:"SUCCESS",finished:true,taskId:task});
});
it("keeps an accepted run pending until import completion", async () => {
  vi.mocked(runStatus).mockResolvedValue({runId,jobName:"webtech_scan_input_job",status:"STARTED",tags:{"backoffice/action":"add-webtech-input"}} as never);
  expect(await webtechQueueSubmission(runId)).toMatchObject({finished:false,taskId:null});
});
it("refuses to treat results processing as an input submission", async () => {
  vi.mocked(runStatus).mockResolvedValue({runId,jobName:"webtech_scan_results_job",status:"SUCCESS",tags:{}} as never);
  await expect(webtechQueueSubmission(runId)).rejects.toThrow("not found");
});
it("fails closed when import history is unavailable", async () => {
  vi.mocked(listRuns).mockRejectedValue(new Error("unavailable"));
  await expect(addDomainToWebtechQueue("100.se", submission, "operator")).rejects.toThrow("unavailable");
  expect(launchRun).not.toHaveBeenCalled();
});

const filters = {domain: "", company: "", source: "", association: "", status: "", shared: "", minConfidence: "", maxConfidence: ""};
it("loads distinct selected roots from the current Swedish domain entity", async () => {
  await addSeDomainsToWebtechQueue({mode: "ids", domains: ["b.se", "a.se", "a.se"]}, submission, "operator");
  expect(launchRun).toHaveBeenCalledWith(expect.objectContaining({
    job: "webtech_scan_input_job", assetSelection: ["webtech_scan_input"],
    runConfig: {ops: {webtech_scan_input: {config: {
      submission_id: submission, queue_scope: "workspace", source_relation: "corpscout.se_company_domain",
      source_final: true, target_column: "root_domain", source_name: "backoffice:se-company-domains",
      filters: {root_domain: ["a.se", "b.se"]},
    }}}},
  }));
});
it("passes all matching filters and exclusions without a page limit or results processing", async () => {
  const query = {...filters, domain: "example", company: "1234567890", source: "brave", association: "connected", status: "active", shared: "1", minConfidence: "0.7", maxConfidence: "0.9"};
  await addSeDomainsToWebtechQueue({mode: "query", query, excludedDomains: ["example.se"]}, submission, "operator");
  const run = vi.mocked(launchRun).mock.calls[0][0];
  expect(run.runConfig).toEqual({ops: {webtech_scan_input: {config: {
    submission_id: submission, queue_scope: "workspace", source_relation: "corpscout.se_company_domain",
    source_final: true, target_column: "root_domain", source_name: "backoffice:se-company-domains",
    select_all: true, excluded_targets: ["example.se"], se_domain_filters: {
      domain: "example", company: "1234567890", source: "brave", association: "connected", status: "active", shared: true,
      min_confidence: 0.7, max_confidence: 0.9,
    },
  }}}});
  expect(run.assetSelection).toEqual(["webtech_scan_input"]);
});
it("recovers the same selection irrespective of selected root order, but rejects changed selection", async () => {
  await addSeDomainsToWebtechQueue({mode: "ids", domains: ["a.se", "b.se"]}, submission, "operator");
  const tags = vi.mocked(launchRun).mock.calls[0][0].tags;
  vi.mocked(listRuns).mockResolvedValue([{runId, status: "SUCCESS", tags}] as never);
  vi.mocked(launchRun).mockClear();
  await addSeDomainsToWebtechQueue({mode: "ids", domains: ["b.se", "a.se"]}, submission, "operator");
  expect(launchRun).not.toHaveBeenCalled();
  await expect(addSeDomainsToWebtechQueue({mode: "ids", domains: ["c.se"]}, submission, "operator")).rejects.toThrow("another domain or selection");
});
it.each([
  {mode: "ids", domains: []},
  {mode: "query", query: {...filters, status: "bogus"}, excludedDomains: []},
  {mode: "query", query: {...filters, page: "2"}, excludedDomains: []},
  {mode: "query", query: filters, excludedDomains: ["https://bad.se/"]},
])("rejects invalid bulk selections without launching anything: %j", async selection => {
  await expect(addSeDomainsToWebtechQueue(selection, submission, "operator")).rejects.toThrow();
  expect(launchRun).not.toHaveBeenCalled();
});
