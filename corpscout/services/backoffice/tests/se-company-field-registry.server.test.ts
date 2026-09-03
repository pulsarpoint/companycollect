import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  FIELD_REGISTRY_SQL,
  FIELD_REGISTRY_VERSION_SQL,
  loadFieldRegistry,
  resetFieldRegistryCache,
} from "~/lib/se-company-field-registry.server";

const SCOPE = { datatype: "info", country: "SE" };

/** One row as FIELD_REGISTRY_SQL returns it: LowCardinality columns wrapped to
 * plain strings, the two Bools cast to 0/1, sources as a JSON array. */
function row(field: string, over: Record<string, unknown> = {}) {
  return {
    field,
    value_type: "text",
    display_group: "activity",
    structured: 0,
    python_only: 0,
    sources: ["llm", "scb"],
    policy_name: "source_precedence",
    policy_version: "source_precedence-v1",
    resolve_sql: `INSERT INTO corpscout.se_company_field /* ${field} */ SELECT {field:String}`,
    registry_version: "se-info-v1",
    ...over,
  };
}

/** The `field = '*'` row: the wide projection statement (spec 4.3). */
const PROJECTION_ROW = row("*", {
  value_type: "projection",
  display_group: "",
  sources: [],
  policy_name: "",
  policy_version: "",
  resolve_sql:
    "INSERT INTO corpscout.se_company_info SELECT {company_ids:Array(String)}",
});

function stubRegistry(version: string, rows: ReturnType<typeof row>[]) {
  clickhouse.query.mockImplementation(async (sql: string) => {
    if (sql === FIELD_REGISTRY_VERSION_SQL) return [{ registry_version: version }];
    if (sql === FIELD_REGISTRY_SQL) return rows;
    throw new Error(`unexpected query: ${sql}`);
  });
}

describe("loadFieldRegistry", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    resetFieldRegistryCache();
  });

  it("pins the argMax-per-field read, scoped to the probed registry version", () => {
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "argMax(registry_version, version) AS registry_version",
    );
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "FROM corpscout.se_company_field_registry",
    );
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "WHERE datatype = {datatype:String} AND country = {country:String}",
    );
    // Consumers read the ReplacingMergeTree with argMax(..., version) per
    // (datatype, country, field), like se_code_labels (spec 4.3).
    for (const column of [
      "value_type",
      "display_group",
      "sources",
      "policy_name",
      "policy_version",
      "resolve_sql",
      "registry_version",
    ]) {
      expect(FIELD_REGISTRY_SQL).toContain(`argMax(${column}, version)`);
    }
    expect(FIELD_REGISTRY_SQL).toContain(
      "toUInt8(argMax(structured, version)) AS structured",
    );
    expect(FIELD_REGISTRY_SQL).toContain(
      "toUInt8(argMax(python_only, version)) AS python_only",
    );
    expect(FIELD_REGISTRY_SQL).toContain("GROUP BY field");
    // A field a later registry dropped keeps its old rows for ever; only the
    // rows stamped with the probed version are the registry.
    expect(FIELD_REGISTRY_SQL).toContain(
      "WHERE registry_version = {registryVersion:String}",
    );
    expect(FIELD_REGISTRY_SQL).toContain("ORDER BY field");
  });

  it("splits the projection row out and maps the field rows", async () => {
    stubRegistry("se-info-v1", [
      PROJECTION_ROW,
      row("description", { sources: ["llm", "esef", "wikidata", "scb"] }),
      row("website", {
        display_group: "scale",
        value_type: "url",
        python_only: 1,
        sources: ["domains", "wikidata"],
      }),
    ]);

    const registry = await loadFieldRegistry();

    expect(registry.version).toBe("se-info-v1");
    expect(registry.projectionSql).toBe(PROJECTION_ROW.resolve_sql);
    expect(registry.fields.map((entry) => entry.field)).toEqual([
      "description",
      "website",
    ]);
    expect(registry.fields[1]).toEqual({
      field: "website",
      valueType: "url",
      displayGroup: "scale",
      structured: false,
      pythonOnly: true,
      sources: ["domains", "wikidata"],
      policyName: "source_precedence",
      policyVersion: "source_precedence-v1",
      resolveSql:
        "INSERT INTO corpscout.se_company_field /* website */ SELECT {field:String}",
      registryVersion: "se-info-v1",
    });
    expect(clickhouse.query).toHaveBeenNthCalledWith(
      1,
      FIELD_REGISTRY_VERSION_SQL,
      SCOPE,
    );
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, FIELD_REGISTRY_SQL, {
      ...SCOPE,
      registryVersion: "se-info-v1",
    });
  });

  it("serves the cached registry while the probe answers the same version", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);

    const first = await loadFieldRegistry();
    const second = await loadFieldRegistry();

    expect(second).toBe(first);
    expect(clickhouse.query.mock.calls.map(([sql]) => sql)).toEqual([
      FIELD_REGISTRY_VERSION_SQL,
      FIELD_REGISTRY_SQL,
      FIELD_REGISTRY_VERSION_SQL,
    ]);
  });

  it("re-reads when the probe reports a different version", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);
    await loadFieldRegistry();

    stubRegistry("se-info-v2", [
      { ...PROJECTION_ROW, registry_version: "se-info-v2" },
      row("description", { registry_version: "se-info-v2" }),
      row("legal_name", {
        display_group: "identity",
        sources: ["bolagsverket", "scb", "wikidata"],
        registry_version: "se-info-v2",
      }),
    ]);
    const refreshed = await loadFieldRegistry();

    expect(refreshed.version).toBe("se-info-v2");
    expect(refreshed.fields.map((entry) => entry.field)).toEqual([
      "description",
      "legal_name",
    ]);
    expect(clickhouse.query).toHaveBeenLastCalledWith(FIELD_REGISTRY_SQL, {
      ...SCOPE,
      registryVersion: "se-info-v2",
    });
  });

  it("resetFieldRegistryCache makes the next call read the rows again", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);
    const first = await loadFieldRegistry();
    resetFieldRegistryCache();

    const second = await loadFieldRegistry();

    expect(second).not.toBe(first);
    expect(second).toEqual(first);
    expect(clickhouse.query).toHaveBeenCalledTimes(4);
  });

  it("refuses an empty registry table instead of resolving with nothing", async () => {
    // argMax over no rows is '' for a String column.
    stubRegistry("", []);
    await expect(loadFieldRegistry()).rejects.toThrow(
      "materialize se_company_field_registry_clickhouse",
    );
  });

  it("refuses a registry version without its projection row", async () => {
    stubRegistry("se-info-v1", [row("description")]);
    await expect(loadFieldRegistry()).rejects.toThrow(
      "no projection row (field = '*')",
    );
  });
});
