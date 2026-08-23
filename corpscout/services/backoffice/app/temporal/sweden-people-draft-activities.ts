import { ApplicationFailure, heartbeat } from "@temporalio/activity";
import { executeSwedenPeopleDraftInitialization } from "../lib/sweden-people-draft.server";
import { executeSwedenPeopleDraftTwoBuild } from "../lib/sweden-people-draft-two.server";
import type { SwedenPeopleDraftWorkflowInput } from "./sweden-people-draft-types";

export async function rebuildSwedenPeopleDraftOne(
  input: SwedenPeopleDraftWorkflowInput,
): Promise<void> {
  const job = await executeSwedenPeopleDraftInitialization(
    input.jobId,
    undefined,
    undefined,
    (progress) => heartbeat(progress),
  );
  if (!job) {
    throw ApplicationFailure.nonRetryable(
      "The Draft 1 processing job does not exist.",
      "DRAFT_ONE_JOB_NOT_FOUND",
    );
  }
  if (job.status === "failed") {
    throw ApplicationFailure.retryable(
      job.errorMessage,
      "DRAFT_ONE_REBUILD_FAILED",
    );
  }
}

export async function rebuildSwedenPeopleDraftTwo(
  input: SwedenPeopleDraftWorkflowInput,
): Promise<void> {
  const job = await executeSwedenPeopleDraftTwoBuild(
    input.jobId,
    undefined,
    undefined,
    (progress) => heartbeat(progress),
  );
  if (!job) {
    throw ApplicationFailure.nonRetryable(
      "The Draft 2 processing job does not exist.",
      "DRAFT_TWO_JOB_NOT_FOUND",
    );
  }
  if (job.status === "failed") {
    throw ApplicationFailure.retryable(
      job.errorMessage,
      "DRAFT_TWO_REBUILD_FAILED",
    );
  }
}
