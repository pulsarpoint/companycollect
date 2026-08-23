import { WorkflowIdReusePolicy } from "@temporalio/common";
import {
  createSwedenPeopleProfileBulkJob,
  failSwedenPeopleProfileBulkJob,
  type SwedenPeopleProfileBulkJob,
} from "~/lib/sweden-person-profile-bulk.server";
import {
  type SwedenPeopleProfileBulkSelection,
  type SwedenPeopleProfileBulkWorkflowInput,
} from "~/temporal/sweden-people-profile-types";
import {
  swedenPeopleTemporalClient,
  swedenPeopleTemporalTaskQueue,
} from "~/lib/sweden-people-temporal-client.server";

function positiveIntegerEnvironmentValue({
  name,
  fallback,
  maximum,
}: {
  name: string;
  fallback: number;
  maximum: number;
}): number {
  const configured = Number(process.env[name]);
  if (!Number.isInteger(configured) || configured <= 0) return fallback;
  return Math.min(configured, maximum);
}

export async function startSwedenPeopleProfileBulkEnhancement(
  selection: SwedenPeopleProfileBulkSelection,
): Promise<SwedenPeopleProfileBulkJob> {
  const job = await createSwedenPeopleProfileBulkJob(selection);
  const input: SwedenPeopleProfileBulkWorkflowInput = {
    jobId: job.jobId,
    batchSize: positiveIntegerEnvironmentValue({
      name: "TEMPORAL_PEOPLE_PROFILE_BATCH_SIZE",
      fallback: 20,
      maximum: 250,
    }),
    concurrentRequests: positiveIntegerEnvironmentValue({
      name: "TEMPORAL_PEOPLE_PROFILE_CONCURRENCY",
      fallback: 2,
      maximum: 20,
    }),
    continueAsNewAfter: positiveIntegerEnvironmentValue({
      name: "TEMPORAL_PEOPLE_PROFILE_CONTINUE_AS_NEW_AFTER",
      fallback: 200,
      maximum: 2_000,
    }),
  };

  try {
    const client = await swedenPeopleTemporalClient();
    await client.workflow.start("swedenPeopleProfileBulkWorkflow", {
      workflowId: job.workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      taskQueue: swedenPeopleTemporalTaskQueue(),
      args: [input],
      memo: {
        country: "SE",
        operation: "bulk-person-profile-enhancement",
        candidateCount: job.totalCount,
      },
    });
    return job;
  } catch (error) {
    failSwedenPeopleProfileBulkJob(
      job.jobId,
      "Temporal could not start the bulk person-profile workflow.",
    );
    throw error;
  }
}
