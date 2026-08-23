import type { DatabaseSync } from "node:sqlite";

export type SwedenPeopleProcessingJobType =
  | "draft_1_rebuild"
  | "draft_2_rebuild";

export interface SwedenPeopleProcessingJobPayload {
  jobId: string;
  status: "queued" | "running" | "completed" | "failed";
  createdAt: string;
  updatedAt: string;
}

interface StoredJobRow {
  payload_json: string;
}

export function ensureSwedenPeopleProcessingJobTable(
  database: DatabaseSync,
): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS people_processing_job (
      job_id TEXT PRIMARY KEY,
      job_type TEXT NOT NULL CHECK (
        job_type IN ('draft_1_rebuild', 'draft_2_rebuild')
      ),
      status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'completed', 'failed')
      ),
      payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS people_processing_job_latest
      ON people_processing_job(job_type, created_at DESC);
    CREATE UNIQUE INDEX IF NOT EXISTS people_processing_job_one_active_per_type
      ON people_processing_job(job_type)
      WHERE status IN ('queued', 'running');
  `);
}

function parseJob<Job extends SwedenPeopleProcessingJobPayload>(
  row: StoredJobRow | undefined,
): Job | null {
  return row ? (JSON.parse(row.payload_json) as Job) : null;
}

export function readLatestSwedenPeopleProcessingJob<
  Job extends SwedenPeopleProcessingJobPayload,
>(
  database: DatabaseSync,
  jobType: SwedenPeopleProcessingJobType,
): Job | null {
  const row = database
    .prepare(
      `SELECT payload_json
       FROM people_processing_job
       WHERE job_type = ?
       ORDER BY created_at DESC
       LIMIT 1`,
    )
    .get(jobType) as unknown as StoredJobRow | undefined;
  return parseJob<Job>(row);
}

export function readActiveSwedenPeopleProcessingJob<
  Job extends SwedenPeopleProcessingJobPayload,
>(
  database: DatabaseSync,
  jobType: SwedenPeopleProcessingJobType,
): Job | null {
  const row = database
    .prepare(
      `SELECT payload_json
       FROM people_processing_job
       WHERE job_type = ? AND status IN ('queued', 'running')
       ORDER BY created_at DESC
       LIMIT 1`,
    )
    .get(jobType) as unknown as StoredJobRow | undefined;
  return parseJob<Job>(row);
}

export function readSwedenPeopleProcessingJob<
  Job extends SwedenPeopleProcessingJobPayload,
>(database: DatabaseSync, jobId: string): Job | null {
  const row = database
    .prepare(
      `SELECT payload_json
       FROM people_processing_job
       WHERE job_id = ?
       LIMIT 1`,
    )
    .get(jobId) as unknown as StoredJobRow | undefined;
  return parseJob<Job>(row);
}

export function insertSwedenPeopleProcessingJob(
  database: DatabaseSync,
  jobType: SwedenPeopleProcessingJobType,
  job: SwedenPeopleProcessingJobPayload,
): void {
  database
    .prepare(
      `INSERT INTO people_processing_job (
        job_id,
        job_type,
        status,
        payload_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?)`,
    )
    .run(
      job.jobId,
      jobType,
      job.status,
      JSON.stringify(job),
      job.createdAt,
      job.updatedAt,
    );
}

export function importLegacySwedenPeopleProcessingJob(
  database: DatabaseSync,
  jobType: SwedenPeopleProcessingJobType,
  job: SwedenPeopleProcessingJobPayload,
): void {
  database
    .prepare(
      `INSERT OR IGNORE INTO people_processing_job (
        job_id,
        job_type,
        status,
        payload_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?)`,
    )
    .run(
      job.jobId,
      jobType,
      job.status,
      JSON.stringify(job),
      job.createdAt,
      job.updatedAt,
    );
}

export function persistSwedenPeopleProcessingJob(
  database: DatabaseSync,
  job: SwedenPeopleProcessingJobPayload,
): void {
  database
    .prepare(
      `UPDATE people_processing_job
       SET status = ?, payload_json = ?, updated_at = ?
       WHERE job_id = ?`,
    )
    .run(job.status, JSON.stringify(job), job.updatedAt, job.jobId);
}
