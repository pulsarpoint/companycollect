export const SWEDEN_PEOPLE_TASK_QUEUE = "backoffice-sweden-people";

export type SwedenPeopleProfileBulkJobStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed";

export type SwedenPeopleProfileBulkCandidateStatus =
  | "pending"
  | "enhanced"
  | "skipped_current"
  | "failed";

export interface SwedenPeopleProfileBulkSelection {
  companyId: string;
  requiredSources: Array<"bolagsverket" | "esef" | "wikidata">;
  requireSavedSuggestion: boolean;
  draftTwoIds: string[] | null;
}

export interface SwedenPeopleProfileBulkWorkflowInput {
  jobId: string;
  batchSize: number;
  concurrentRequests: number;
  continueAsNewAfter: number;
}

export interface SwedenPeopleProfileBulkCandidateResult {
  draftTwoId: string;
  status: Exclude<SwedenPeopleProfileBulkCandidateStatus, "pending">;
}
