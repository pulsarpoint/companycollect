import { createHash } from "node:crypto";
import { parseSeDomainSelection } from "~/lib/se-domain-crawl.server";
import { graphDomainError } from "~/lib/domain-graph";
import { QUEUE_UUID } from "~/lib/queues";
import { dagsterRunUrl, launchRun, listRuns, runStatus } from "~/lib/dagster.server";
import { assertWebtechAvailable } from "~/lib/webtech-maintenance.server";

export class WebtechQueueRequestError extends Error {}
const JOB = "webtech_scan_input_job";
const ASSET = "webtech_scan_input";
const submissions = new Map<string, Promise<unknown>>();

/** Append to the workspace draft. Dagster chooses/creates task_id atomically. */
export async function addDomainToWebtechQueue(domainValue: string, submissionId: string, requestedBy: string) {
  const domain = domainValue.trim().toLowerCase();
  if (!domain || graphDomainError(domain)) throw new WebtechQueueRequestError("Choose a valid domain.");
  return submitWebtechInput({targets: [domain], source_name: "backoffice:se-company-domain"}, submissionId, requestedBy, {"webtech/queued_domain": domain});
}

export async function addSeDomainsToWebtechQueue(value: unknown, submissionId: string, requestedBy: string) {
  let selection;
  try { selection = parseSeDomainSelection(value); }
  catch (error) { throw new WebtechQueueRequestError(error instanceof Error ? error.message : "Invalid domain selection."); }
  const config: Record<string, unknown> = {
    source_relation: "corpscout.se_company_domain", source_final: true,
    target_column: "root_domain", source_name: "backoffice:se-company-domains",
  };
  if (selection.mode === "ids") {
    if (selection.domains.length > 10000) throw new WebtechQueueRequestError("Use Select all matching for more than 10,000 domains.");
    config.filters = {root_domain: [...selection.domains].sort()};
  } else {
    if (selection.excludedDomains.length > 10000) throw new WebtechQueueRequestError("Use at most 10,000 exclusions.");
    const q = selection.query;
    config.select_all = true;
    config.excluded_targets = [...selection.excludedDomains].sort();
    config.se_domain_filters = {
      domain: q.domain, company: q.company, source: q.source, association: q.association,
      status: q.status, shared: q.shared === "1",
      ...(q.minConfidence === "" ? {} : {min_confidence: Number(q.minConfidence)}),
      ...(q.maxConfidence === "" ? {} : {max_confidence: Number(q.maxConfidence)}),
    };
  }
  return submitWebtechInput(config, submissionId, requestedBy, {
    "webtech/selection_sha256": createHash("sha256").update(JSON.stringify(config)).digest("hex"),
  });
}

async function submitWebtechInput(config: Record<string, unknown>, submissionId: string, requestedBy: string, identityTags: Record<string, string>) {
  if (!QUEUE_UUID.test(submissionId)) throw new WebtechQueueRequestError("Invalid submission ID. Reload the page.");
  assertWebtechAvailable();
  const previous = submissions.get(submissionId);
  const pending = previous ? previous.then(submit, submit) : submit();
  submissions.set(submissionId, pending);
  try { return await pending; }
  finally { if (submissions.get(submissionId) === pending) submissions.delete(submissionId); }

  async function submit() {
    const runs = await listRuns({job: JOB, limit: 1, tags: {"processing/submission_id": submissionId}});
    const existing = runs[0];
    if (existing && Object.entries(identityTags).some(([key, value]) => existing.tags[key] !== value)) throw new WebtechQueueRequestError("This submission ID already belongs to another domain or selection.");
    // Retry failed imports using the same durable receipt, not a new selection.
    if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) {
      return {ok: true as const, runId: existing.runId, status: existing.status, runUrl: dagsterRunUrl(existing.runId)};
    }
    const run = await launchRun({
      job: JOB, assetSelection: [ASSET],
      runConfig: {ops: {[ASSET]: {config: {
        submission_id: submissionId, queue_scope: "workspace", ...config,
      }}}},
      tags: {"processing/submission_id": submissionId, ...identityTags,
        "corpscout/requested_by": requestedBy, "backoffice/action": "add-webtech-input"},
    });
    return {ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId)};
  }
}

export async function webtechQueueSubmission(runId: string) {
  if (!QUEUE_UUID.test(runId)) throw new WebtechQueueRequestError("Invalid import run ID.");
  const run = await runStatus(runId);
  if (run.jobName !== JOB || run.tags["backoffice/action"] !== "add-webtech-input") throw new WebtechQueueRequestError("Webtech queue submission not found.");
  const task = run.tags["processing/task_id"];
  return {ok: true as const, runId, status: run.status,
    finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status),
    taskId: task && QUEUE_UUID.test(task) ? task : null,
  };
}
