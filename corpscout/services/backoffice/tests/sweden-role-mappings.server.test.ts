import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  DISTINCT_SWEDEN_SOURCE_ROLES_QUERY,
  getSwedenSourceRoleRows,
  readSwedenRoleMappings,
  saveSwedenRoleMapping,
  SWEDEN_ROLE_MAPPINGS_PATH,
} from "~/lib/sweden-role-mappings.server";

describe("Sweden source role mappings", () => {
  it("reads the Dagster-owned mappings from the checked-in SQLite content", () => {
    const mappings = readSwedenRoleMappings();
    const mappingKeys = mappings.map(
      (mapping) =>
        `${mapping.source}:${mapping.source_role_code}:${mapping.source_role_name}`,
    );

    expect(mappings).toHaveLength(21);
    expect(new Set(mappingKeys).size).toBe(mappingKeys.length);
    expect(mappings).toContainEqual({
      source: "bolagsverket",
      source_role_code: "ceo",
      source_role_name: "",
      canonical_role_code: "chief_executive_officer",
      mapping_status: "mapped",
    });
    expect(mappings).toContainEqual({
      source: "bolagsverket",
      source_role_code: "unknown",
      source_role_name: "",
      canonical_role_code: null,
      mapping_status: "roleless",
    });
    expect(mappings).toContainEqual({
      source: "bolagsverket",
      source_role_code: "other",
      source_role_name: "Arbetstagarrepresentant",
      canonical_role_code: "employee_board_representative",
      mapping_status: "mapped",
    });
    expect(mappings).toContainEqual({
      source: "bolagsverket",
      source_role_code: "other",
      source_role_name: "Vice VD",
      canonical_role_code: "deputy_chief_executive_officer",
      mapping_status: "mapped",
    });
  });

  it("stores an exact admin mapping without mapping every native other role", () => {
    const temporaryDirectory = mkdtempSync(
      join(tmpdir(), "sweden-role-mappings-"),
    );
    const databasePath = join(temporaryDirectory, "role_mappings.sqlite");
    copyFileSync(SWEDEN_ROLE_MAPPINGS_PATH, databasePath);

    try {
      saveSwedenRoleMapping(
        {
          source: "bolagsverket",
          source_role_code: "other",
          source_role_name: "Vice koncernchef",
          canonical_role_code: "deputy_chief_executive_officer",
        },
        databasePath,
      );

      const mappings = readSwedenRoleMappings(databasePath);
      expect(mappings).toContainEqual({
        source: "bolagsverket",
        source_role_code: "other",
        source_role_name: "Vice koncernchef",
        canonical_role_code: "deputy_chief_executive_officer",
        mapping_status: "mapped",
      });
      expect(
        mappings.some(
          (mapping) =>
            mapping.source === "bolagsverket" &&
            mapping.source_role_code === "other" &&
            mapping.source_role_name === "",
        ),
      ).toBe(false);
    } finally {
      rmSync(temporaryDirectory, { recursive: true, force: true });
    }
  });

  it("queries original role values without roleless unknown fallbacks", () => {
    expect(DISTINCT_SWEDEN_SOURCE_ROLES_QUERY).toContain(
      "WHERE trim(role_original) != ''",
    );
    expect(DISTINCT_SWEDEN_SOURCE_ROLES_QUERY).toContain(
      "AND role_kind != 'unknown'",
    );
    expect(DISTINCT_SWEDEN_SOURCE_ROLES_QUERY).not.toContain(
      "if(role_original = '', toString(role_kind), role_original)",
    );
  });

  it("joins distinct ClickHouse roles to mapped, roleless, and unmapped states", async () => {
    const rows = await getSwedenSourceRoleRows();

    expect(rows.length).toBeGreaterThan(0);
    expect(
      rows.some(
        (row) =>
          row.source === "bolagsverket" &&
          row.source_role_code === "ceo" &&
          row.mapping_status === "mapped" &&
          row.canonical_role_code === "chief_executive_officer",
      ),
    ).toBe(true);
    expect(
      rows.some(
        (row) =>
          row.source === "bolagsverket" &&
          row.source_role_code === "other" &&
          row.source_role_name === "Vice VD" &&
          row.mapping_status === "mapped" &&
          row.canonical_role_code === "deputy_chief_executive_officer",
      ),
    ).toBe(true);
    expect(
      rows.some(
        (row) =>
          row.source === "bolagsverket" &&
          row.source_role_code === "unknown",
      ),
    ).toBe(false);
    expect(
      rows.some(
        (row) =>
          row.source === "bolagsverket" &&
          row.source_role_code === "other" &&
          row.mapping_status === "unmapped",
      ),
    ).toBe(true);
    expect(rows.some((row) => row.source === "esef")).toBe(true);
    expect(rows.some((row) => row.source === "wikidata")).toBe(true);
  });
});
