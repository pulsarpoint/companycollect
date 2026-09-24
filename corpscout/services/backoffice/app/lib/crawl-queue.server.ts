import {createHash} from "node:crypto";
import {inputConfig, parseSeDomainSelection} from "~/lib/se-domain-crawl.server";
import {dagsterRunUrl, launchRun, listRuns, runStatus} from "~/lib/dagster.server";
import {QUEUE_UUID} from "~/lib/queues";

const JOB = "website_crawl_input_job";
const ASSET = "website_crawl_input";
const submissions = new Map<string, Promise<unknown>>();

export async function addSeDomainsToCrawlQueue(value: unknown, crawlType: string, submissionId: string, requestedBy: string) {
  if (!["full", "jobs", "site_info"].includes(crawlType)) throw new Error("Choose a crawl type.");
  if (!QUEUE_UUID.test(submissionId)) throw new Error("Invalid submission ID. Reload the page.");
  const selection = parseSeDomainSelection(value);
  if ((selection.mode === "ids" ? selection.domains.length : selection.excludedDomains.length) > 10000) throw new Error("Use Select all matching for large selections; at most 10,000 explicit domains or exclusions.");
  const config = {...inputConfig(selection), crawl_type: crawlType, queue_scope: "workspace", submission_id: submissionId};
  const fingerprint = createHash("sha256").update(JSON.stringify(config)).digest("hex");
  const previous = submissions.get(submissionId);
  const pending = previous ? previous.then(submit, submit) : submit();
  submissions.set(submissionId, pending);
  try { return await pending; }
  finally { if (submissions.get(submissionId) === pending) submissions.delete(submissionId); }
  async function submit() {
    const [existing] = await listRuns({job: JOB, limit: 1, tags: {"processing/submission_id": submissionId}});
    if (existing && existing.tags["crawler/selection_sha256"] !== fingerprint) throw new Error("This submission ID belongs to another selection.");
    if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) return {ok: true as const, runId: existing.runId, status: existing.status, runUrl: dagsterRunUrl(existing.runId), crawlType};
    const run = await launchRun({job: JOB, assetSelection: [ASSET], runConfig: {ops: {[ASSET]: {config}}}, tags: {
      "processing/submission_id": submissionId, "crawler/selection_sha256": fingerprint, "crawl/type": crawlType,
      "corpscout/requested_by": requestedBy, "backoffice/action": "add-crawl-input",
    }});
    return {ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId), crawlType};
  }
}

export async function crawlQueueSubmission(runId: string) {
  if (!QUEUE_UUID.test(runId)) throw new Error("Invalid import run ID.");
  const run = await runStatus(runId);
  if (run.jobName !== JOB || run.tags["backoffice/action"] !== "add-crawl-input") throw new Error("Crawl queue submission not found.");
  const task = run.tags["processing/task_id"];
  return {ok: true as const, runId, status: run.status, finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status),
    taskId: task && QUEUE_UUID.test(task) ? task : null};
}
