import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { afterEach, describe, expect, it } from "vitest";
import {
  completeSwedenPeopleProfileBulkJob,
  createSwedenPeopleProfileBulkJob,
  getLatestSwedenPeopleProfileBulkJob,
  listPendingSwedenPeopleProfileCandidateIds,
  markSwedenPeopleProfileBulkJobRunning,
  recordSwedenPeopleProfileBulkCandidateResult,
  SwedenPeopleProfileBulkJobAlreadyRunningError,
} from "~/lib/sweden-person-profile-bulk.server";

const temporaryDirectories: string[] = [];

function temporaryPaths() {
  const directory = mkdtempSync(join(tmpdir(), "people-profile-bulk-"));
  temporaryDirectories.push(directory);
  return {
    draftDatabasePath: join(directory, "people-draft.duckdb"),
    curationDatabasePath: join(directory, "people-curation.sqlite"),
  };
}

async function createDraftTwoFixture(databasePath: string): Promise<void> {
  const instance = await DuckDBInstance.fromCache(databasePath);
  const connection = await instance.connect();
  try {
    await connection.run(
      "CREATE TABLE people_draft_step_2 (" +
        "draft_2_id VARCHAR NOT NULL, " +
        "company_id VARCHAR NOT NULL, " +
        "source_count INTEGER NOT NULL, " +
        "bolagsverket_source_ids VARCHAR[] NOT NULL, " +
        "esef_source_ids VARCHAR[] NOT NULL, " +
        "wikidata_source_ids VARCHAR[] NOT NULL" +
        "); " +
        "INSERT INTO people_draft_step_2 VALUES " +
        "('multi-source', '5560000001', 2, ['b-1'], ['e-1'], []), " +
        "('single-source', '5560000001', 1, ['b-2'], [], []), " +
        "('other-company', '5560000002', 2, ['b-3'], [], ['w-3']);",
    );
  } finally {
    connection.closeSync();
  }
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("Sweden bulk person-profile job store", () => {
  it("queues only eligible multi-source people and records durable progress", async () => {
    const paths = temporaryPaths();
    await createDraftTwoFixture(paths.draftDatabasePath);

    const job = await createSwedenPeopleProfileBulkJob(
      {
        companyId: "5560000001",
        requiredSources: [],
        requireSavedSuggestion: false,
        draftTwoIds: ["multi-source", "single-source"],
      },
      paths.curationDatabasePath,
      paths.draftDatabasePath,
    );

    expect(job).toMatchObject({
      status: "queued",
      requestedCount: 2,
      totalCount: 1,
      pendingCount: 1,
      processedCount: 0,
    });
    await expect(
      createSwedenPeopleProfileBulkJob(
        {
          companyId: "",
          requiredSources: [],
          requireSavedSuggestion: false,
          draftTwoIds: ["other-company"],
        },
        paths.curationDatabasePath,
        paths.draftDatabasePath,
      ),
    ).rejects.toBeInstanceOf(SwedenPeopleProfileBulkJobAlreadyRunningError);

    markSwedenPeopleProfileBulkJobRunning(
      job.jobId,
      paths.curationDatabasePath,
    );
    expect(
      listPendingSwedenPeopleProfileCandidateIds(
        job.jobId,
        20,
        paths.curationDatabasePath,
      ),
    ).toEqual(["multi-source"]);

    recordSwedenPeopleProfileBulkCandidateResult(
      {
        jobId: job.jobId,
        draftTwoId: "multi-source",
        status: "enhanced",
      },
      paths.curationDatabasePath,
    );
    completeSwedenPeopleProfileBulkJob(
      job.jobId,
      paths.curationDatabasePath,
    );

    expect(
      getLatestSwedenPeopleProfileBulkJob(paths.curationDatabasePath),
    ).toMatchObject({
      jobId: job.jobId,
      status: "completed",
      totalCount: 1,
      pendingCount: 0,
      enhancedCount: 1,
      skippedCurrentCount: 0,
      failedCount: 0,
      processedCount: 1,
      progressPercent: 100,
    });
  });

  it("applies required-source filters when select-all is represented by a filter", async () => {
    const paths = temporaryPaths();
    await createDraftTwoFixture(paths.draftDatabasePath);

    const job = await createSwedenPeopleProfileBulkJob(
      {
        companyId: "",
        requiredSources: ["wikidata"],
        requireSavedSuggestion: false,
        draftTwoIds: null,
      },
      paths.curationDatabasePath,
      paths.draftDatabasePath,
    );

    expect(job.totalCount).toBe(1);
    expect(
      listPendingSwedenPeopleProfileCandidateIds(
        job.jobId,
        20,
        paths.curationDatabasePath,
      ),
    ).toEqual(["other-company"]);
  });
});
