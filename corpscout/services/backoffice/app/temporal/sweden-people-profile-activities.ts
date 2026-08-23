import { ApplicationFailure } from "@temporalio/activity";
import { getLlmRequestAvailability } from "../lib/llm-availability.server";
import {
  buildPersonProfileLlmInput,
  generatePersonProfileSuggestion,
  PersonProfileLlmError,
} from "../lib/person-profile-llm.server";
import {
  completeSwedenPeopleProfileBulkJob,
  failSwedenPeopleProfileBulkJob,
  listPendingSwedenPeopleProfileCandidateIds,
  markSwedenPeopleProfileBulkJobRunning,
  recordSwedenPeopleProfileBulkCandidateResult,
} from "../lib/sweden-person-profile-bulk.server";
import {
  getLatestPersonProfileResponse,
  savePersonProfileResponse,
} from "../lib/sweden-person-profile-responses.server";
import { getSwedenPeopleDraftTwoRow } from "../lib/sweden-people-draft-two.server";
import type { SwedenPeopleProfileBulkCandidateResult } from "./sweden-people-profile-types";

export async function validateSwedenPeopleProfileLlmConfiguration(): Promise<void> {
  const availability = getLlmRequestAvailability();
  if (!availability.ready) {
    throw ApplicationFailure.nonRetryable(
      availability.warning ?? "The LLM is not configured.",
      "LLM_CONFIGURATION",
    );
  }
}

export async function markSwedenPeopleProfileJobRunning(
  jobId: string,
): Promise<void> {
  markSwedenPeopleProfileBulkJobRunning(jobId);
}

export async function pendingSwedenPeopleProfileCandidateIds(
  jobId: string,
  limit: number,
): Promise<string[]> {
  return listPendingSwedenPeopleProfileCandidateIds(jobId, limit);
}

export async function enhanceSwedenPeopleProfileCandidate({
  jobId,
  draftTwoId,
}: {
  jobId: string;
  draftTwoId: string;
}): Promise<SwedenPeopleProfileBulkCandidateResult> {
  const candidate = await getSwedenPeopleDraftTwoRow(draftTwoId);
  if (!candidate) {
    throw ApplicationFailure.nonRetryable(
      "The Draft 2 person no longer exists.",
      "DRAFT_TWO_PERSON_NOT_FOUND",
    );
  }
  if (candidate.source_count < 2) {
    throw ApplicationFailure.nonRetryable(
      "The Draft 2 person no longer has evidence from multiple sources.",
      "MULTIPLE_SOURCES_REQUIRED",
    );
  }

  const input = buildPersonProfileLlmInput(candidate);
  const currentResponse = getLatestPersonProfileResponse({
    draftTwoId,
    input,
  });
  if (currentResponse) {
    recordSwedenPeopleProfileBulkCandidateResult({
      jobId,
      draftTwoId,
      status: "skipped_current",
    });
    return { draftTwoId, status: "skipped_current" };
  }

  try {
    const result = await generatePersonProfileSuggestion({ candidate });
    savePersonProfileResponse({
      draftTwoId,
      input,
      rawResponse: result.rawResponse,
      suggestion: result.suggestion,
      generation: result.generation,
    });
  } catch (error) {
    if (error instanceof PersonProfileLlmError) {
      if (error.kind === "configuration") {
        throw ApplicationFailure.nonRetryable(
          error.message,
          "LLM_CONFIGURATION",
        );
      }
      throw ApplicationFailure.retryable(
        error.message,
        `LLM_${error.kind.toUpperCase()}`,
      );
    }
    throw error;
  }

  recordSwedenPeopleProfileBulkCandidateResult({
    jobId,
    draftTwoId,
    status: "enhanced",
  });
  return { draftTwoId, status: "enhanced" };
}

export async function recordSwedenPeopleProfileCandidateFailure({
  jobId,
  draftTwoId,
  errorMessage,
}: {
  jobId: string;
  draftTwoId: string;
  errorMessage: string;
}): Promise<void> {
  recordSwedenPeopleProfileBulkCandidateResult({
    jobId,
    draftTwoId,
    status: "failed",
    errorMessage,
  });
}

export async function completeSwedenPeopleProfileJob(
  jobId: string,
): Promise<void> {
  completeSwedenPeopleProfileBulkJob(jobId);
}

export async function failSwedenPeopleProfileJob({
  jobId,
  errorMessage,
}: {
  jobId: string;
  errorMessage: string;
}): Promise<void> {
  failSwedenPeopleProfileBulkJob(jobId, errorMessage);
}
