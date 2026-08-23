import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { afterEach, describe, expect, it, vi } from "vitest";

type SourceName = "bolagsverket" | "esef" | "wikidata";

const sourceState = vi.hoisted(() => ({
  failSource: "" as "" | SourceName,
}));

vi.mock("~/lib/sweden-people-draft-sources.server", () => {
  const sources: SourceName[] = ["bolagsverket", "esef", "wikidata"];
  function observation(source: SourceName, index: number) {
    return {
      company_id: `556520000${index}`,
      name: `${source} Person`,
      source,
      source_entity_id: `${source}-entity`,
      source_record_uid: `${source}-record`,
      role_original: "Board member",
      fiscal_year: source === "wikidata" ? null : 2024,
      description: source === "wikidata" ? "Profile context" : null,
      source_profile_hash: `${source}-profile-hash`,
      source_role_hash: `${source}-role-hash`,
      source_payload_json: JSON.stringify({ source }),
      source_observed_at: "2026-08-20T12:00:00Z",
    };
  }
  const observations = {
    bolagsverket: observation("bolagsverket", 0),
    esef: observation("esef", 1),
    wikidata: observation("wikidata", 2),
  };

  return {
    SWEDEN_PEOPLE_DRAFT_SOURCES: sources,
    getSwedenPeopleDraftSourceCount: vi.fn(async () => 1),
    streamSwedenPeopleDraftSource(source: SourceName) {
      return (async function* () {
        if (sourceState.failSource === source) {
          throw new Error(`failed ${source}`);
        }
        yield observations[source];
      })();
    },
  };
});

vi.mock("~/lib/sweden-people-temporal-client.server", () => ({
  swedenPeopleTemporalClient: vi.fn(async () => ({
    workflow: { start: vi.fn(async () => undefined) },
  })),
  swedenPeopleTemporalTaskQueue: vi.fn(() => "test-people-task-queue"),
}));

import {
  executeSwedenPeopleDraftInitialization,
  getLatestSwedenPeopleDraftInitializationJob,
  getSwedenPeopleDraftStatus,
  initializeSwedenPeopleDraft,
  startSwedenPeopleDraftInitialization,
  SwedenPeopleDraftReplacementConfirmationError,
  type SwedenPeopleDraftInitializationJob,
} from "~/lib/sweden-people-draft.server";

const temporaryDirectories: string[] = [];

function temporaryDatabasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "sweden-draft-background-"));
  temporaryDirectories.push(directory);
  return join(directory, "people-draft.duckdb");
}

function curationDatabasePath(databasePath: string): string {
  return join(dirname(databasePath), "people-curation.sqlite");
}

async function waitForTerminalJob(
  databasePath: string,
): Promise<SwedenPeopleDraftInitializationJob> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const job =
      await getLatestSwedenPeopleDraftInitializationJob(
        databasePath,
        curationDatabasePath(databasePath),
      );
    if (job?.status === "completed" || job?.status === "failed") return job;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for the Draft 1 background job");
}

afterEach(() => {
  sourceState.failSource = "";
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("Sweden people Draft 1 background initialization", () => {
  it("imports every source and publishes the completed build", async () => {
    const databasePath = temporaryDatabasePath();

    const queued = await startSwedenPeopleDraftInitialization({
      databasePath,
      curationDatabasePath: curationDatabasePath(databasePath),
    });
    expect(queued.status).toBe("queued");
    await executeSwedenPeopleDraftInitialization(
      queued.jobId,
      databasePath,
      curationDatabasePath(databasePath),
    );

    const completed = await waitForTerminalJob(databasePath);
    expect(completed).toMatchObject({
      status: "completed",
      phase: "completed",
      processedRows: 3,
      totalRows: 3,
      insertedRows: 3,
      progressPercent: 100,
    });
    await expect(getSwedenPeopleDraftStatus(databasePath)).resolves.toEqual({
      tableExists: true,
      rowCount: 3,
    });

    const instance = await DuckDBInstance.fromCache(databasePath);
    const connection = await instance.connect();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT source, source_entity_id
         FROM people_draft_step_1
         ORDER BY source`,
      );
      const rows = reader.getRowObjectsJson();
      expect(rows).toEqual([
        {
          source: "bolagsverket",
          source_entity_id: "bolagsverket-entity",
        },
        { source: "esef", source_entity_id: "esef-entity" },
        { source: "wikidata", source_entity_id: "wikidata-entity" },
      ]);
    } finally {
      connection.closeSync();
    }
  });

  it("requires confirmation and preserves published rows when a rebuild fails", async () => {
    const databasePath = temporaryDatabasePath();
    await initializeSwedenPeopleDraft({ databasePath });

    const instance = await DuckDBInstance.fromCache(databasePath);
    const connection = await instance.connect();
    try {
      await connection.run(
        `INSERT INTO people_draft_step_1 (
          observation_id,
          company_id,
          name,
          source,
          source_entity_id,
          source_record_uid,
          source_profile_hash,
          source_role_hash,
          imported_at
        ) VALUES (
          'published-observation',
          '5565200028',
          'Published Person',
          'esef',
          'published-entity',
          'published-record',
          'published-profile-hash',
          'published-role-hash',
          '2026-08-20T12:00:00Z'
        )`,
      );
    } finally {
      connection.closeSync();
    }

    await expect(
      startSwedenPeopleDraftInitialization({
        databasePath,
        curationDatabasePath: curationDatabasePath(databasePath),
      }),
    ).rejects.toThrow(SwedenPeopleDraftReplacementConfirmationError);

    sourceState.failSource = "esef";
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      await startSwedenPeopleDraftInitialization({
        databasePath,
        curationDatabasePath: curationDatabasePath(databasePath),
        confirmReplacement: true,
      });
      const queued = await getLatestSwedenPeopleDraftInitializationJob(
        databasePath,
        curationDatabasePath(databasePath),
      );
      if (!queued) throw new Error("Expected a queued Draft 1 job");
      await executeSwedenPeopleDraftInitialization(
        queued.jobId,
        databasePath,
        curationDatabasePath(databasePath),
      );
      const failed = await waitForTerminalJob(databasePath);
      expect(failed.status).toBe("failed");
      expect(failed.errorMessage).toContain("previously published");
    } finally {
      consoleError.mockRestore();
    }

    await expect(getSwedenPeopleDraftStatus(databasePath)).resolves.toEqual({
      tableExists: true,
      rowCount: 1,
    });
    const preservedConnection = await instance.connect();
    try {
      const reader = await preservedConnection.runAndReadAll(
        "SELECT name FROM people_draft_step_1",
      );
      expect(reader.getRowObjectsJson()).toEqual([
        { name: "Published Person" },
      ]);
    } finally {
      preservedConnection.closeSync();
    }
  });
});
