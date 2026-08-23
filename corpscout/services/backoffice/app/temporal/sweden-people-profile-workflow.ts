import {
  ApplicationFailure,
  continueAsNew,
  proxyActivities,
} from "@temporalio/workflow";
import type * as activities from "./sweden-people-profile-activities";
import type { SwedenPeopleProfileBulkWorkflowInput } from "./sweden-people-profile-types";

const orchestrationActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "1 minute",
  retry: {
    maximumAttempts: 10,
    initialInterval: "1 second",
    maximumInterval: "30 seconds",
  },
});

const llmActivities = proxyActivities<
  Pick<typeof activities, "enhanceSwedenPeopleProfileCandidate">
>({
  startToCloseTimeout: "3 minutes",
  retry: {
    maximumAttempts: 4,
    initialInterval: "5 seconds",
    backoffCoefficient: 2,
    maximumInterval: "1 minute",
  },
});

function failureMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message.slice(0, 1_000);
  }
  return "The bulk person-profile workflow failed unexpectedly.";
}

async function failJob(jobId: string, error: unknown): Promise<never> {
  const message = failureMessage(error);
  await orchestrationActivities.failSwedenPeopleProfileJob({
    jobId,
    errorMessage: message,
  });
  throw ApplicationFailure.nonRetryable(
    message,
    "BULK_PERSON_PROFILE_JOB_FAILED",
  );
}

export async function swedenPeopleProfileBulkWorkflow(
  input: SwedenPeopleProfileBulkWorkflowInput,
): Promise<void> {
  try {
    await orchestrationActivities.validateSwedenPeopleProfileLlmConfiguration();
    await orchestrationActivities.markSwedenPeopleProfileJobRunning(input.jobId);
  } catch (error) {
    return await failJob(input.jobId, error);
  }

  let processedInThisRun = 0;
  while (true) {
    let draftTwoIds: string[];
    try {
      draftTwoIds =
        await orchestrationActivities.pendingSwedenPeopleProfileCandidateIds(
          input.jobId,
          input.batchSize,
        );
    } catch (error) {
      return await failJob(input.jobId, error);
    }

    if (draftTwoIds.length === 0) {
      try {
        await orchestrationActivities.completeSwedenPeopleProfileJob(
          input.jobId,
        );
        return;
      } catch (error) {
        return await failJob(input.jobId, error);
      }
    }

    for (
      let offset = 0;
      offset < draftTwoIds.length;
      offset += input.concurrentRequests
    ) {
      const concurrentIds = draftTwoIds.slice(
        offset,
        offset + input.concurrentRequests,
      );
      await Promise.all(
        concurrentIds.map(async (draftTwoId) => {
          try {
            await llmActivities.enhanceSwedenPeopleProfileCandidate({
              jobId: input.jobId,
              draftTwoId,
            });
          } catch (error) {
            await orchestrationActivities.recordSwedenPeopleProfileCandidateFailure(
              {
                jobId: input.jobId,
                draftTwoId,
                errorMessage: failureMessage(error),
              },
            );
          }
        }),
      );
      processedInThisRun += concurrentIds.length;
    }

    if (processedInThisRun >= input.continueAsNewAfter) {
      return await continueAsNew<typeof swedenPeopleProfileBulkWorkflow>(input);
    }
  }
}

