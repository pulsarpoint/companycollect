import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";
import {
  getSwedenPeopleDraftStatus,
  initializeSwedenPeopleDraft,
  SwedenPeopleDraftTableExistsError,
} from "~/lib/sweden-people-draft.server";

const EXPECTED_COLUMNS = [
  "observation_id",
  "company_id",
  "name",
  "source",
  "source_entity_id",
  "source_record_uid",
  "role_original",
  "fiscal_year",
  "description",
  "source_profile_hash",
  "source_role_hash",
  "source_payload_json",
  "source_observed_at",
  "imported_at",
];

async function withTemporaryDatabase(
  test: (databasePath: string) => Promise<void>,
): Promise<void> {
  const temporaryDirectory = mkdtempSync(
    join(tmpdir(), "sweden-people-draft-"),
  );
  try {
    await test(join(temporaryDirectory, "nested", "people-draft.duckdb"));
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

describe("Sweden people draft database", () => {
  it("creates the local immutable DuckDB observation table without row-store indexes", async () => {
    await withTemporaryDatabase(async (databasePath) => {
      await expect(getSwedenPeopleDraftStatus(databasePath)).resolves.toEqual({
        tableExists: false,
        rowCount: 0,
      });

      await expect(
        initializeSwedenPeopleDraft({ databasePath }),
      ).resolves.toEqual({ tableExists: true, rowCount: 0 });

      const instance = await DuckDBInstance.fromCache(databasePath);
      const connection = await instance.connect();
      try {
        const columnsReader = await connection.runAndReadAll(
          `SELECT column_name
           FROM information_schema.columns
           WHERE table_schema = 'main'
             AND table_name = 'people_draft_step_1'
           ORDER BY ordinal_position`,
        );
        const columns = columnsReader.getRowObjectsJson() as Array<{
          column_name: string;
        }>;
        expect(columns.map((column) => column.column_name)).toEqual(
          EXPECTED_COLUMNS,
        );

        const indexesReader = await connection.runAndReadAll(
          `SELECT index_name
           FROM duckdb_indexes()
           WHERE table_name = 'people_draft_step_1'`,
        );
        expect(indexesReader.getRowObjectsJson()).toEqual([]);
      } finally {
        connection.closeSync();
      }
    });
  });

  it("preserves an existing table unless reinitialization is explicit", async () => {
    await withTemporaryDatabase(async (databasePath) => {
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
            'observation-1',
            '5565200028',
            'David Mindus',
            'esef',
            'candidate-1',
            'source-record-1',
            'profile-hash-1',
            'role-hash-1',
            '2026-08-20T12:00:00Z'
          )`,
        );
      } finally {
        connection.closeSync();
      }

      await expect(
        initializeSwedenPeopleDraft({ databasePath }),
      ).rejects.toThrow(SwedenPeopleDraftTableExistsError);
      await expect(getSwedenPeopleDraftStatus(databasePath)).resolves.toEqual({
        tableExists: true,
        rowCount: 1,
      });

      await expect(
        initializeSwedenPeopleDraft({
          databasePath,
          reinitialize: true,
        }),
      ).resolves.toEqual({ tableExists: true, rowCount: 0 });
    });
  });
});
