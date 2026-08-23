import { randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import {
  listSwedenPeopleDraftTwoLlmCandidateIds,
  type SwedenPeopleDraftSource,
} from "~/lib/sweden-people-draft-two.server";
import {
  connectCurationDatabase,
  listPersonProfileResponseDraftTwoIds,
  SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
} from "~/lib/sweden-person-profile-responses.server";
import type {
  SwedenPeopleProfileBulkCandidateStatus,
  SwedenPeopleProfileBulkJobStatus,
  SwedenPeopleProfileBulkSelection,
} from "~/temporal/sweden-people-profile-types";

export interface SwedenPeopleProfileBulkJob {
  jobId: string;
  workflowId: string;
  status: SwedenPeopleProfileBulkJobStatus;
  selection: SwedenPeopleProfileBulkSelection;
  requestedCount: number;
  totalCount: number;
  pendingCount: number;
  enhancedCount: number;
  skippedCurrentCount: number;
  failedCount: number;
  processedCount: number;
  progressPercent: number;
  errorMessage: string;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  updatedAt: string;
}

interface StoredBulkJobRow {
  job_id: string;
  workflow_id: string;
  status: SwedenPeopleProfileBulkJobStatus;
  selection_json: string;
  requested_count: number;
  total_count: number;
  pending_count: number;
  enhanced_count: number;
  skipped_current_count: number;
  failed_count: number;
  error_message: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

const JOB_SELECT_SQL = `
  SELECT
    job.job_id,
    job.workflow_id,
    job.status,
    job.selection_json,
    job.requested_count,
    job.total_count,
    coalesce(sum(candidate.status = 'pending'), 0) AS pending_count,
    coalesce(sum(candidate.status = 'enhanced'), 0) AS enhanced_count,
    coalesce(sum(candidate.status = 'skipped_current'), 0) AS skipped_current_count,
    coalesce(sum(candidate.status = 'failed'), 0) AS failed_count,
    job.error_message,
    job.created_at,
    job.started_at,
    job.completed_at,
    job.updated_at
  FROM person_profile_bulk_job AS job
  LEFT JOIN person_profile_bulk_job_candidate AS candidate
    ON candidate.job_id = job.job_id
`;

export class SwedenPeopleProfileBulkJobAlreadyRunningError extends Error {
  readonly job: SwedenPeopleProfileBulkJob;

  constructor(job: SwedenPeopleProfileBulkJob) {
    super("A bulk person-profile enhancement job is already running.");
    this.name = "SwedenPeopleProfileBulkJobAlreadyRunningError";
    this.job = job;
  }
}

export class SwedenPeopleProfileBulkSelectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SwedenPeopleProfileBulkSelectionError";
  }
}

function ensureBulkJobTables(database: DatabaseSync): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS person_profile_bulk_job (
      job_id TEXT PRIMARY KEY,
      workflow_id TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'completed', 'failed')
      ),
      selection_json TEXT NOT NULL CHECK (json_valid(selection_json)),
      requested_count INTEGER NOT NULL CHECK (requested_count >= 0),
      total_count INTEGER NOT NULL CHECK (total_count >= 0),
      error_message TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS person_profile_bulk_job_one_active
      ON person_profile_bulk_job((1))
      WHERE status IN ('queued', 'running');
    CREATE TABLE IF NOT EXISTS person_profile_bulk_job_candidate (
      job_id TEXT NOT NULL REFERENCES person_profile_bulk_job(job_id),
      draft_2_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'enhanced', 'skipped_current', 'failed')
      ),
      error_message TEXT NOT NULL DEFAULT '',
      completed_at TEXT,
      PRIMARY KEY (job_id, draft_2_id)
    );
    CREATE INDEX IF NOT EXISTS person_profile_bulk_candidate_pending
      ON person_profile_bulk_job_candidate(job_id, status, draft_2_id);
  `);
}

function mapBulkJob(row: StoredBulkJobRow): SwedenPeopleProfileBulkJob {
  const pendingCount = Number(row.pending_count);
  const enhancedCount = Number(row.enhanced_count);
  const skippedCurrentCount = Number(row.skipped_current_count);
  const failedCount = Number(row.failed_count);
  const processedCount = enhancedCount + skippedCurrentCount + failedCount;
  const totalCount = Number(row.total_count);
  return {
    jobId: row.job_id,
    workflowId: row.workflow_id,
    status: row.status,
    selection: JSON.parse(row.selection_json) as SwedenPeopleProfileBulkSelection,
    requestedCount: Number(row.requested_count),
    totalCount,
    pendingCount,
    enhancedCount,
    skippedCurrentCount,
    failedCount,
    processedCount,
    progressPercent:
      totalCount === 0 ? 0 : Math.round((processedCount / totalCount) * 100),
    errorMessage: row.error_message,
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    updatedAt: row.updated_at,
  };
}

function readBulkJob(
  database: DatabaseSync,
  whereClause: string,
  ...parameters: string[]
): SwedenPeopleProfileBulkJob | null {
  const row = database
    .prepare(
      `${JOB_SELECT_SQL}
       WHERE ${whereClause}
       GROUP BY job.job_id
       ORDER BY job.created_at DESC
       LIMIT 1`,
    )
    .get(...parameters) as unknown as StoredBulkJobRow | undefined;
  return row ? mapBulkJob(row) : null;
}

function normalizedSelection(
  selection: SwedenPeopleProfileBulkSelection,
): SwedenPeopleProfileBulkSelection {
  const requiredSources = [
    ...new Set(
      selection.requiredSources.filter((source): source is SwedenPeopleDraftSource =>
        ["bolagsverket", "esef", "wikidata"].includes(source),
      ),
    ),
  ].sort();
  const draftTwoIds =
    selection.draftTwoIds === null
      ? null
      : [
          ...new Set(
            selection.draftTwoIds
              .map((draftTwoId) => draftTwoId.trim())
              .filter((draftTwoId) => draftTwoId !== ""),
          ),
        ].sort();
  if (draftTwoIds !== null && draftTwoIds.length === 0) {
    throw new SwedenPeopleProfileBulkSelectionError(
      "Select at least one Draft 2 person to enhance.",
    );
  }
  return {
    companyId: selection.companyId.trim(),
    requiredSources,
    requireSavedSuggestion: selection.requireSavedSuggestion,
    draftTwoIds,
  };
}

export async function createSwedenPeopleProfileBulkJob(
  selectionInput: SwedenPeopleProfileBulkSelection,
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
  draftDatabasePath?: string,
): Promise<SwedenPeopleProfileBulkJob> {
  const selection = normalizedSelection(selectionInput);
  const savedResponseIds = selection.requireSavedSuggestion
    ? new Set(listPersonProfileResponseDraftTwoIds(databasePath))
    : null;
  const selectedIds =
    selection.draftTwoIds === null
      ? savedResponseIds === null
        ? undefined
        : [...savedResponseIds]
      : savedResponseIds === null
        ? selection.draftTwoIds
        : selection.draftTwoIds.filter((draftTwoId) =>
            savedResponseIds.has(draftTwoId),
          );
  const candidateIds = await listSwedenPeopleDraftTwoLlmCandidateIds({
    companyId: selection.companyId,
    selectedSources: selection.requiredSources,
    draftTwoIds: selectedIds,
    databasePath: draftDatabasePath,
  });
  if (candidateIds.length === 0) {
    throw new SwedenPeopleProfileBulkSelectionError(
      "The selection has no multi-source Draft 2 people eligible for LLM enhancement.",
    );
  }

  const database = connectCurationDatabase(databasePath);
  ensureBulkJobTables(database);
  database.exec("BEGIN IMMEDIATE");
  try {
    const activeJob = readBulkJob(
      database,
      "job.status IN ('queued', 'running')",
    );
    if (activeJob) {
      throw new SwedenPeopleProfileBulkJobAlreadyRunningError(activeJob);
    }

    const jobId = randomUUID();
    const workflowId = `backoffice-se-people-profile-${jobId}`;
    const now = new Date().toISOString();
    database
      .prepare(
        `INSERT INTO person_profile_bulk_job (
          job_id,
          workflow_id,
          status,
          selection_json,
          requested_count,
          total_count,
          created_at,
          updated_at
        ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?)`,
      )
      .run(
        jobId,
        workflowId,
        JSON.stringify(selection),
        selection.draftTwoIds?.length ?? candidateIds.length,
        candidateIds.length,
        now,
        now,
      );
    const insertCandidate = database.prepare(
      `INSERT INTO person_profile_bulk_job_candidate (job_id, draft_2_id)
       VALUES (?, ?)`,
    );
    for (const draftTwoId of candidateIds) {
      insertCandidate.run(jobId, draftTwoId);
    }
    database.exec("COMMIT");
    const job = readBulkJob(database, "job.job_id = ?", jobId);
    if (!job) throw new Error("The bulk LLM job could not be read back.");
    return job;
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  } finally {
    database.close();
  }
}

export function getLatestSwedenPeopleProfileBulkJob(
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): SwedenPeopleProfileBulkJob | null {
  const database = connectCurationDatabase(databasePath);
  try {
    ensureBulkJobTables(database);
    return readBulkJob(database, "1 = 1");
  } finally {
    database.close();
  }
}

export function listPendingSwedenPeopleProfileCandidateIds(
  jobId: string,
  limit: number,
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): string[] {
  const database = connectCurationDatabase(databasePath);
  try {
    ensureBulkJobTables(database);
    const rows = database
      .prepare(
        `SELECT draft_2_id
         FROM person_profile_bulk_job_candidate
         WHERE job_id = ? AND status = 'pending'
         ORDER BY draft_2_id
         LIMIT ?`,
      )
      .all(jobId, Math.max(1, Math.min(250, Math.floor(limit)))) as unknown as Array<{
      draft_2_id: string;
    }>;
    return rows.map((row) => row.draft_2_id);
  } finally {
    database.close();
  }
}

export function markSwedenPeopleProfileBulkJobRunning(
  jobId: string,
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): void {
  const database = connectCurationDatabase(databasePath);
  try {
    ensureBulkJobTables(database);
    const now = new Date().toISOString();
    database
      .prepare(
        `UPDATE person_profile_bulk_job
         SET status = 'running',
             started_at = coalesce(started_at, ?),
             updated_at = ?
         WHERE job_id = ? AND status IN ('queued', 'running')`,
      )
      .run(now, now, jobId);
  } finally {
    database.close();
  }
}

export function recordSwedenPeopleProfileBulkCandidateResult(
  {
    jobId,
    draftTwoId,
    status,
    errorMessage = "",
  }: {
    jobId: string;
    draftTwoId: string;
    status: Exclude<SwedenPeopleProfileBulkCandidateStatus, "pending">;
    errorMessage?: string;
  },
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): void {
  const database = connectCurationDatabase(databasePath);
  try {
    ensureBulkJobTables(database);
    database
      .prepare(
        `UPDATE person_profile_bulk_job_candidate
         SET status = ?, error_message = ?, completed_at = ?
         WHERE job_id = ? AND draft_2_id = ? AND status = 'pending'`,
      )
      .run(
        status,
        errorMessage.slice(0, 1_000),
        new Date().toISOString(),
        jobId,
        draftTwoId,
      );
  } finally {
    database.close();
  }
}

export function completeSwedenPeopleProfileBulkJob(
  jobId: string,
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): void {
  updateBulkJobTerminalStatus(jobId, "completed", "", databasePath);
}

export function failSwedenPeopleProfileBulkJob(
  jobId: string,
  errorMessage: string,
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): void {
  updateBulkJobTerminalStatus(
    jobId,
    "failed",
    errorMessage.slice(0, 1_000),
    databasePath,
  );
}

function updateBulkJobTerminalStatus(
  jobId: string,
  status: "completed" | "failed",
  errorMessage: string,
  databasePath: string,
): void {
  const database = connectCurationDatabase(databasePath);
  try {
    ensureBulkJobTables(database);
    const now = new Date().toISOString();
    database
      .prepare(
        `UPDATE person_profile_bulk_job
         SET status = ?, error_message = ?, completed_at = ?, updated_at = ?
         WHERE job_id = ? AND status IN ('queued', 'running')`,
      )
      .run(status, errorMessage, now, now, jobId);
  } finally {
    database.close();
  }
}
