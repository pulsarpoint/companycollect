import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { DuckDBAppender, DuckDBInstance } from "@duckdb/node-api";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("~/lib/sweden-people-temporal-client.server", () => ({
  swedenPeopleTemporalClient: vi.fn(async () => ({
    workflow: { start: vi.fn(async () => undefined) },
  })),
  swedenPeopleTemporalTaskQueue: vi.fn(() => "test-people-task-queue"),
}));
import {
  initializeSwedenPeopleDraft,
} from "~/lib/sweden-people-draft.server";
import {
  executeSwedenPeopleDraftTwoBuild,
  getLatestSwedenPeopleDraftTwoJob,
  getSwedenPeopleDraftOneRows,
  getSwedenPeopleDraftOneRowsPage,
  getSwedenPeopleDraftTwoRows,
  getSwedenPeopleDraftTwoRowsPage,
  getSwedenPeopleDraftTwoStatus,
  startSwedenPeopleDraftTwoBuild,
  type SwedenPeopleDraftTwoJob,
} from "~/lib/sweden-people-draft-two.server";

interface TestObservation {
  id: string;
  companyId: string;
  name: string;
  source: "bolagsverket" | "esef" | "wikidata";
  role: string | null;
  year: number | null;
  description: string | null;
  payload: Record<string, unknown>;
}

const temporaryDirectories: string[] = [];

function temporaryDatabasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "sweden-draft-two-"));
  temporaryDirectories.push(directory);
  return join(directory, "people-draft.duckdb");
}

function curationDatabasePath(databasePath: string): string {
  return join(dirname(databasePath), "people-curation.sqlite");
}

function appendNullableString(
  appender: DuckDBAppender,
  value: string | null,
): void {
  if (value === null) appender.appendNull();
  else appender.appendVarchar(value);
}

async function insertObservations(
  databasePath: string,
  observations: TestObservation[],
): Promise<void> {
  const instance = await DuckDBInstance.fromCache(databasePath);
  const connection = await instance.connect();
  const appender = await connection.createAppender("people_draft_step_1");
  try {
    for (const observation of observations) {
      appender.appendVarchar(observation.id);
      appender.appendVarchar(observation.companyId);
      appender.appendVarchar(observation.name);
      appender.appendVarchar(observation.source);
      appender.appendVarchar(`${observation.source}-entity-${observation.id}`);
      appender.appendVarchar(`${observation.source}-record-${observation.id}`);
      appendNullableString(appender, observation.role);
      if (observation.year === null) appender.appendNull();
      else appender.appendInteger(observation.year);
      appendNullableString(appender, observation.description);
      appender.appendVarchar(`profile-${observation.id}`);
      appender.appendVarchar(`role-${observation.id}`);
      appender.appendVarchar(JSON.stringify(observation.payload));
      appender.appendVarchar("2026-08-20T12:00:00Z");
      appender.appendVarchar("2026-08-20T12:00:00Z");
      appender.endRow();
    }
  } finally {
    appender.closeSync();
    connection.closeSync();
  }
}

async function waitForTerminalJob(
  databasePath: string,
): Promise<SwedenPeopleDraftTwoJob> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const job = await getLatestSwedenPeopleDraftTwoJob(
      databasePath,
      curationDatabasePath(databasePath),
    );
    if (job?.status === "completed" || job?.status === "failed") return job;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 10));
  }
  throw new Error("Timed out waiting for Draft 2");
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("Sweden people Draft 2", () => {
  it("merges yearly and cross-source evidence by company, person, and role", async () => {
    const databasePath = temporaryDatabasePath();
    await initializeSwedenPeopleDraft({ databasePath });
    await insertObservations(databasePath, [
      {
        id: "bolags-2022",
        companyId: "1111111111",
        name: "Anna Andersson",
        source: "bolagsverket",
        role: "Styrelseledamot",
        year: 2022,
        description: null,
        payload: { role_kind: "board_member" },
      },
      {
        id: "bolags-2023",
        companyId: "1111111111",
        name: "Anna Andersson",
        source: "bolagsverket",
        role: "Styrelseledamot",
        year: 2023,
        description: null,
        payload: { role_kind: "board_member" },
      },
      {
        id: "esef-ceo",
        companyId: "2222222222",
        name: "Micael Torsten Johansson",
        source: "esef",
        role: "Chief Executive Officer",
        year: 2024,
        description: null,
        payload: {
          role_category: "chief_executive",
          status: "current",
          effective_from: "2018-01-01",
          effective_to: null,
        },
      },
      {
        id: "wikidata-ceo",
        companyId: "2222222222",
        name: "Micael Johansson",
        source: "wikidata",
        role: "chief executive officer",
        year: null,
        description: "CEO of the company",
        payload: {
          role_property: "P169",
          is_current: 1,
          start_date: "2019-10-01",
          end_date: null,
        },
      },
      {
        id: "roleless",
        companyId: "3333333333",
        name: "Roleless Person",
        source: "bolagsverket",
        role: null,
        year: 2024,
        description: null,
        payload: { role_kind: "unknown" },
      },
    ]);

    const queued = await startSwedenPeopleDraftTwoBuild({
      databasePath,
      curationDatabasePath: curationDatabasePath(databasePath),
    });
    expect(queued.status).toBe("queued");
    await executeSwedenPeopleDraftTwoBuild(
      queued.jobId,
      databasePath,
      curationDatabasePath(databasePath),
    );
    const completed = await waitForTerminalJob(databasePath);
    expect(completed).toMatchObject({
      status: "completed",
      processedRows: 4,
      totalRows: 4,
      outputRows: 2,
      skippedRolelessRows: 1,
      skippedUnmappedRows: 0,
      unmappedRoleExamples: [],
      progressPercent: 100,
    });
    await expect(getSwedenPeopleDraftTwoStatus(databasePath)).resolves.toEqual({
      tableExists: true,
      rowCount: 2,
    });
    const multipleSourceRows = await getSwedenPeopleDraftTwoRows({
      databasePath,
      onlyMultipleSources: true,
    });
    expect(multipleSourceRows).toHaveLength(1);
    expect(multipleSourceRows[0]).toMatchObject({
      company_id: "2222222222",
      name: "Micael Torsten Johansson",
      source_count: 2,
      esef_source_ids: ["esef-ceo"],
      wikidata_source_ids: ["wikidata-ceo"],
      source_observations: [
        expect.objectContaining({
          observation_id: "esef-ceo",
          source: "esef",
          role_original: "Chief Executive Officer",
        }),
        expect.objectContaining({
          observation_id: "wikidata-ceo",
          source: "wikidata",
          description: "CEO of the company",
        }),
      ],
    });
    await expect(
      getSwedenPeopleDraftTwoRowsPage({
        databasePath,
        selectedSources: ["esef", "wikidata"],
      }),
    ).resolves.toMatchObject({
      totalRows: 1,
      rows: [
        expect.objectContaining({
          company_id: "2222222222",
          name: "Micael Torsten Johansson",
        }),
      ],
    });
    await expect(
      getSwedenPeopleDraftTwoRowsPage({
        databasePath,
        selectedSources: ["bolagsverket", "esef"],
      }),
    ).resolves.toMatchObject({ totalRows: 0, rows: [] });
    await expect(
      getSwedenPeopleDraftTwoRowsPage({
        databasePath,
        draftTwoIds: [multipleSourceRows[0].draft_2_id],
      }),
    ).resolves.toMatchObject({
      totalRows: 1,
      rows: [
        expect.objectContaining({ draft_2_id: multipleSourceRows[0].draft_2_id }),
      ],
    });
    await expect(
      getSwedenPeopleDraftTwoRowsPage({
        databasePath,
        draftTwoIds: [],
      }),
    ).resolves.toMatchObject({ totalRows: 0, rows: [] });

    await expect(
      getSwedenPeopleDraftOneRowsPage({
        databasePath,
        page: 2,
        pageSize: 2,
      }),
    ).resolves.toMatchObject({
      page: 2,
      pageSize: 2,
      totalRows: 5,
      totalPages: 3,
      rows: [
        expect.objectContaining({ observation_id: "wikidata-ceo" }),
        expect.objectContaining({ observation_id: "esef-ceo" }),
      ],
    });
    await expect(
      getSwedenPeopleDraftTwoRowsPage({
        databasePath,
        page: 2,
        pageSize: 1,
      }),
    ).resolves.toMatchObject({
      page: 2,
      pageSize: 1,
      totalRows: 2,
      totalPages: 2,
      rows: [
        expect.objectContaining({
          company_id: "2222222222",
          name: "Micael Torsten Johansson",
        }),
      ],
    });

    const instance = await DuckDBInstance.fromCache(databasePath);
    const connection = await instance.connect();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT * EXCLUDE (draft_2_id, created_at)
         FROM people_draft_step_2
         ORDER BY company_id`,
      );
      expect(reader.getRowObjectsJson()).toEqual([
        {
          company_id: "1111111111",
          name: "Anna Andersson",
          position: "board_member",
          start_year: 2022,
          end_year: 2023,
          source_count: 1,
          observation_count: 2,
          bolagsverket_source_ids: ["bolags-2022", "bolags-2023"],
          bolagsverket_descriptions: [],
          esef_source_ids: [],
          esef_descriptions: [],
          wikidata_source_ids: [],
          wikidata_descriptions: [],
        },
        {
          company_id: "2222222222",
          name: "Micael Torsten Johansson",
          position: "chief_executive_officer",
          start_year: 2018,
          end_year: null,
          source_count: 2,
          observation_count: 2,
          bolagsverket_source_ids: [],
          bolagsverket_descriptions: [],
          esef_source_ids: ["esef-ceo"],
          esef_descriptions: [],
          wikidata_source_ids: ["wikidata-ceo"],
          wikidata_descriptions: ["CEO of the company"],
        },
      ]);
    } finally {
      connection.closeSync();
    }
  });

  it("preserves unmapped observations in Draft 1 and skips them in Draft 2", async () => {
    const databasePath = temporaryDatabasePath();
    await initializeSwedenPeopleDraft({ databasePath });
    await insertObservations(databasePath, [
      {
        id: "mapped",
        companyId: "4444444444",
        name: "Mapped Person",
        source: "bolagsverket",
        role: "Styrelseledamot",
        year: 2024,
        description: null,
        payload: { role_kind: "board_member" },
      },
      {
        id: "employee-representative",
        companyId: "4444444444",
        name: "Employee Representative",
        source: "bolagsverket",
        role: "Arbetstagarrepresentant",
        year: 2024,
        description: null,
        payload: { role_kind: "other" },
      },
      {
        id: "unmapped",
        companyId: "4444444444",
        name: "Unmapped Person",
        source: "bolagsverket",
        role: "New role",
        year: 2024,
        description: null,
        payload: { role_kind: "other" },
      },
    ]);

    const queued = await startSwedenPeopleDraftTwoBuild({
      databasePath,
      curationDatabasePath: curationDatabasePath(databasePath),
    });
    await executeSwedenPeopleDraftTwoBuild(
      queued.jobId,
      databasePath,
      curationDatabasePath(databasePath),
    );
    const completed = await waitForTerminalJob(databasePath);
    expect(completed).toMatchObject({
      status: "completed",
      totalRows: 2,
      outputRows: 2,
      skippedUnmappedRows: 1,
      unmappedRoleExamples: ["bolagsverket:other:New role"],
    });
    await expect(getSwedenPeopleDraftTwoStatus(databasePath)).resolves.toEqual({
      tableExists: true,
      rowCount: 2,
    });
    await expect(
      getSwedenPeopleDraftOneRows({
        databasePath,
        onlyUnmapped: true,
      }),
    ).resolves.toEqual([
      {
        observation_id: "unmapped",
        company_id: "4444444444",
        name: "Unmapped Person",
        source: "bolagsverket",
        role_original: "New role",
        fiscal_year: 2024,
        description: null,
        source_entity_id: "bolagsverket-entity-unmapped",
        source_record_uid: "bolagsverket-record-unmapped",
        source_profile_hash: "profile-unmapped",
        source_role_hash: "role-unmapped",
        source_payload_json: '{"role_kind":"other"}',
        source_observed_at: "2026-08-20T12:00:00Z",
      },
    ]);

    const instance = await DuckDBInstance.fromCache(databasePath);
    const connection = await instance.connect();
    try {
      const roleReader = await connection.runAndReadAll(
        "SELECT name, position FROM people_draft_step_2 ORDER BY name",
      );
      expect(roleReader.getRowObjectsJson()).toEqual([
        {
          name: "Employee Representative",
          position: "employee_board_representative",
        },
        { name: "Mapped Person", position: "board_member" },
      ]);
      const reader = await connection.runAndReadAll(
        "SELECT observation_id FROM people_draft_step_1 ORDER BY observation_id",
      );
      expect(reader.getRowObjectsJson()).toEqual([
        { observation_id: "employee-representative" },
        { observation_id: "mapped" },
        { observation_id: "unmapped" },
      ]);
    } finally {
      connection.closeSync();
    }
  });
});
