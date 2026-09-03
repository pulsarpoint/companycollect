import { chQuery } from "~/lib/clickhouse.server";

/**
 * The SE company field registry as Dagster exports it into
 * corpscout.se_company_field_registry (spec 2026-09-02, section 4.3): one row
 * per field carrying the generated resolve statement, plus one row with
 * `field = '*'` whose resolve_sql is the wide-projection statement. The
 * backoffice READS this; the registry is owned by dagster_v3 code.
 */
export interface FieldRegistryEntry {
  field: string;
  valueType: string;
  displayGroup: string;
  structured: boolean;
  pythonOnly: boolean;
  /** Precedence order; position is the rank. */
  sources: string[];
  policyName: string;
  policyVersion: string;
  /** INSERT INTO corpscout.se_company_field ... SELECT, binding {field:String},
   * {company_ids:Array(String)}, {source_run_id:String}, {resolved_at:DateTime64(3, 'UTC')}. */
  resolveSql: string;
  registryVersion: string;
}

export interface FieldRegistry {
  version: string;
  fields: FieldRegistryEntry[];
  /** INSERT INTO corpscout.se_company_info ... SELECT, binding {company_ids:Array(String)}. */
  projectionSql: string;
}

const DATATYPE = "info";
const COUNTRY = "SE";
const PROJECTION_FIELD = "*";

/** The cheap probe every load runs: which registry version is current. The
 * table is a ReplacingMergeTree(version), so the newest `version` stamp
 * carries the current registry_version string. '' when the table is empty. */
export const FIELD_REGISTRY_VERSION_SQL = `SELECT argMax(registry_version, version) AS registry_version
FROM corpscout.se_company_field_registry
WHERE datatype = {datatype:String} AND country = {country:String}`;

/**
 * Every row of one registry version, argMax(..., version) per (datatype,
 * country, field) like se_code_labels. LowCardinality columns are wrapped in
 * toString() and the two Bools cast to UInt8 for one predictable JSON shape
 * (the INFO_SQL convention). Filtered to the probed registry_version: a field
 * a later registry dropped keeps its old rows in the table for ever.
 */
export const FIELD_REGISTRY_SQL = `WITH latest AS (
  SELECT
    field,
    toString(argMax(value_type, version)) AS value_type,
    toString(argMax(display_group, version)) AS display_group,
    toUInt8(argMax(structured, version)) AS structured,
    toUInt8(argMax(python_only, version)) AS python_only,
    argMax(sources, version) AS sources,
    toString(argMax(policy_name, version)) AS policy_name,
    argMax(policy_version, version) AS policy_version,
    argMax(resolve_sql, version) AS resolve_sql,
    argMax(registry_version, version) AS registry_version
  FROM corpscout.se_company_field_registry
  WHERE datatype = {datatype:String} AND country = {country:String}
  GROUP BY field
)
SELECT *
FROM latest
WHERE registry_version = {registryVersion:String}
ORDER BY field`;

interface FieldRegistryQueryRow {
  field: string;
  value_type: string;
  display_group: string;
  structured: number;
  python_only: number;
  sources: string[];
  policy_name: string;
  policy_version: string;
  resolve_sql: string;
  registry_version: string;
}

function toEntry(row: FieldRegistryQueryRow): FieldRegistryEntry {
  return {
    field: row.field,
    valueType: row.value_type,
    displayGroup: row.display_group,
    structured: row.structured === 1,
    pythonOnly: row.python_only === 1,
    sources: row.sources,
    policyName: row.policy_name,
    policyVersion: row.policy_version,
    resolveSql: row.resolve_sql,
    registryVersion: row.registry_version,
  };
}

let cached: FieldRegistry | undefined;

/** Tests only: forget the cached registry. */
export function resetFieldRegistryCache(): void {
  cached = undefined;
}

/**
 * The current registry. Every call probes the version (one aggregate over a
 * dozen rows); the full read runs only when the probed version differs from
 * the cached one, so a re-export under a new version is picked up on the next
 * decision without a restart, and the common case costs one small query.
 */
export async function loadFieldRegistry(): Promise<FieldRegistry> {
  const [probe] = await chQuery<{ registry_version: string }>(
    FIELD_REGISTRY_VERSION_SQL,
    { datatype: DATATYPE, country: COUNTRY },
  );
  const version = probe?.registry_version ?? "";
  if (version === "") {
    throw new Error(
      "corpscout.se_company_field_registry holds no info/SE rows; materialize se_company_field_registry_clickhouse first.",
    );
  }
  if (cached && cached.version === version) return cached;

  const rows = await chQuery<FieldRegistryQueryRow>(FIELD_REGISTRY_SQL, {
    datatype: DATATYPE,
    country: COUNTRY,
    registryVersion: version,
  });
  const projection = rows.find((row) => row.field === PROJECTION_FIELD);
  if (!projection) {
    throw new Error(
      `Registry ${version} has no projection row (field = '*'); the export is incomplete.`,
    );
  }
  cached = {
    version,
    fields: rows.filter((row) => row.field !== PROJECTION_FIELD).map(toEntry),
    projectionSql: projection.resolve_sql,
  };
  return cached;
}
