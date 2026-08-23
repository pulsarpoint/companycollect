import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { DuckDBInstance } from "@duckdb/node-api";
import { afterEach, describe, expect, it } from "vitest";
import {
  getLatestSwedenPeopleDraftInitializationJob,
  initializeSwedenPeopleDraft,
} from "~/lib/sweden-people-draft.server";
import { getLatestSwedenPeopleDraftTwoJob } from "~/lib/sweden-people-draft-two.server";

const temporaryDirectories: string[] = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("Sweden people processing-job migration", () => {
  it("moves legacy jobs to SQLite and removes job and partial-build tables from DuckDB", async () => {
    const directory = mkdtempSync(join(tmpdir(), "sweden-job-migration-"));
    temporaryDirectories.push(directory);
    const databasePath = join(directory, "people-draft.duckdb");
    const curationDatabasePath = join(directory, "people-curation.sqlite");
    await initializeSwedenPeopleDraft({ databasePath });

    const instance = await DuckDBInstance.fromCache(databasePath);
    const connection = await instance.connect();
    try {
      await connection.run(`
        CREATE TABLE people_draft_step_1_build AS
          SELECT * FROM people_draft_step_1;
        CREATE TABLE people_draft_step_2_build (draft_2_id VARCHAR);
        CREATE TABLE people_draft_step_1_initialization_job (
          job_id VARCHAR,
          status VARCHAR,
          phase VARCHAR,
          current_source VARCHAR,
          processed_rows BIGINT,
          total_rows BIGINT,
          inserted_rows BIGINT,
          progress_percent INTEGER,
          message VARCHAR,
          error_message VARCHAR,
          created_at VARCHAR,
          started_at VARCHAR,
          completed_at VARCHAR,
          updated_at VARCHAR
        );
        INSERT INTO people_draft_step_1_initialization_job VALUES (
          'legacy-draft-1', 'running', 'importing', 'bolagsverket',
          100, 500, 100, 23, 'Importing', '',
          '2026-08-20T12:00:00.000Z', '2026-08-20T12:00:01.000Z', NULL,
          '2026-08-20T12:05:00.000Z'
        );
        CREATE TABLE people_draft_step_2_initialization_job (
          job_id VARCHAR,
          status VARCHAR,
          phase VARCHAR,
          processed_rows BIGINT,
          total_rows BIGINT,
          output_rows BIGINT,
          skipped_roleless_rows BIGINT,
          skipped_unmapped_rows BIGINT,
          unmapped_role_examples_json VARCHAR,
          progress_percent INTEGER,
          message VARCHAR,
          error_message VARCHAR,
          created_at VARCHAR,
          started_at VARCHAR,
          completed_at VARCHAR,
          updated_at VARCHAR
        );
        INSERT INTO people_draft_step_2_initialization_job VALUES (
          'legacy-draft-2', 'running', 'matching', 200, 800, 0, 10, 5,
          '["other:role"]', 34, 'Matching', '',
          '2026-08-20T13:00:00.000Z', '2026-08-20T13:00:01.000Z', NULL,
          '2026-08-20T13:05:00.000Z'
        );
      `);
    } finally {
      connection.closeSync();
    }

    await expect(
      getLatestSwedenPeopleDraftInitializationJob(
        databasePath,
        curationDatabasePath,
      ),
    ).resolves.toMatchObject({
      jobId: "legacy-draft-1",
      status: "failed",
      phase: "failed",
    });
    await expect(
      getLatestSwedenPeopleDraftTwoJob(
        databasePath,
        curationDatabasePath,
      ),
    ).resolves.toMatchObject({
      jobId: "legacy-draft-2",
      status: "failed",
      phase: "failed",
    });

    const inspectionConnection = await instance.connect();
    try {
      const tables = await inspectionConnection.runAndReadAll(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name IN (
          'people_draft_step_1_initialization_job',
          'people_draft_step_2_initialization_job',
          'people_draft_step_1_build',
          'people_draft_step_2_build'
        )
      `);
      expect(tables.getRowObjectsJson()).toEqual([]);
    } finally {
      inspectionConnection.closeSync();
    }

    const curationDatabase = new DatabaseSync(curationDatabasePath, {
      readOnly: true,
    });
    try {
      expect(
        curationDatabase
          .prepare(
            `SELECT job_type, status
             FROM people_processing_job
             ORDER BY job_type`,
          )
          .all(),
      ).toEqual([
        { job_type: "draft_1_rebuild", status: "failed" },
        { job_type: "draft_2_rebuild", status: "failed" },
      ]);
    } finally {
      curationDatabase.close();
    }
  });
});
