import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { chQuery } from "~/lib/clickhouse.server";
import type { PeopleSourceName } from "~/lib/people-sources";

export type SourceRoleMappingStatus = "mapped" | "roleless" | "unmapped";

export interface StoredSourceRoleMapping {
  source: PeopleSourceName;
  source_role_code: string;
  source_role_name: string;
  canonical_role_code: string | null;
  mapping_status: Exclude<SourceRoleMappingStatus, "unmapped">;
}

interface DistinctSourceRoleRow {
  source: PeopleSourceName;
  source_role_code: string;
  source_role_name: string;
  observation_count: number;
  company_count: number;
}

export interface SwedenSourceRoleRow extends DistinctSourceRoleRow {
  canonical_role_code: string | null;
  mapping_status: SourceRoleMappingStatus;
}

export interface SwedenSourceRoleMappingInput {
  source: PeopleSourceName;
  source_role_code: string;
  source_role_name: string;
  canonical_role_code: string;
}

export const SWEDEN_ROLE_MAPPINGS_PATH = join(
  process.cwd(),
  "content",
  "sweden",
  "people",
  "role_mappings.sqlite",
);

export const DISTINCT_SWEDEN_SOURCE_ROLES_QUERY = `WITH sweden_wikidata_companies AS (
  SELECT company_id, wikidata_id
  FROM (
    SELECT
      replaceRegexpAll(identifier_value, '[^0-9]', '') AS company_id,
      wikidata_id
    FROM corpscout.wikidata_company_identifiers FINAL
    WHERE identifier_type = 'se_orgnr'
      AND match(company_id, '^[0-9]{10}$')

    UNION ALL

    SELECT
      company_identifiers.company_id,
      wikidata_identifiers.wikidata_id
    FROM corpscout.company_identifier AS company_identifiers
    INNER JOIN corpscout.wikidata_company_identifiers AS wikidata_identifiers FINAL
      ON upperUTF8(wikidata_identifiers.identifier_value) =
         upperUTF8(company_identifiers.issuer_id)
    WHERE company_identifiers.country_code = 'SE'
      AND company_identifiers.issuer_scheme = 'lei'
      AND company_identifiers.is_current = 1
      AND wikidata_identifiers.identifier_type = 'lei'
  )
  GROUP BY company_id, wikidata_id
), source_roles AS (
  SELECT
    'bolagsverket' AS source,
    toString(role_kind) AS source_role_code,
    trim(role_original) AS source_role_name,
    count() AS observation_count,
    uniqExact(company_id) AS company_count
  FROM corpscout.se_financial_report_signatories
  WHERE trim(role_original) != ''
    AND role_kind != 'unknown'
  GROUP BY source_role_code, source_role_name

  UNION ALL

  SELECT
    'esef' AS source,
    toString(role_category) AS source_role_code,
    trim(role) AS source_role_name,
    count() AS observation_count,
    uniqExact(company_id) AS company_count
  FROM corpscout.esef_document_people FINAL
  WHERE country_code = 'SE'
    AND trim(role) != ''
  GROUP BY source_role_code, source_role_name

  UNION ALL

  SELECT
    'wikidata' AS source,
    toString(links.role_property) AS source_role_code,
    trim(toString(links.role_label)) AS source_role_name,
    count() AS observation_count,
    uniqExact(companies.company_id) AS company_count
  FROM sweden_wikidata_companies AS companies
  INNER JOIN corpscout.wikidata_company_people AS links FINAL
    ON links.company_wikidata_id = companies.wikidata_id
  WHERE trim(toString(links.role_label)) != ''
  GROUP BY source_role_code, source_role_name
)
SELECT
  source,
  source_role_code,
  source_role_name,
  observation_count,
  company_count
FROM source_roles
ORDER BY source, observation_count DESC, source_role_code, source_role_name`;

export function readSwedenRoleMappings(
  databasePath = SWEDEN_ROLE_MAPPINGS_PATH,
): StoredSourceRoleMapping[] {
  const database = new DatabaseSync(databasePath, { readOnly: true });
  try {
    return database
      .prepare(
        `SELECT
          source,
          source_role_code,
          source_role_name,
          canonical_role_code,
          mapping_status
        FROM role_mapping
        ORDER BY source, source_role_code, source_role_name`,
      )
      .all() as unknown as StoredSourceRoleMapping[];
  } finally {
    database.close();
  }
}

export function saveSwedenRoleMapping(
  mapping: SwedenSourceRoleMappingInput,
  databasePath = SWEDEN_ROLE_MAPPINGS_PATH,
): void {
  const database = new DatabaseSync(databasePath);
  try {
    database.exec("PRAGMA busy_timeout = 5000");
    database
      .prepare(
        `INSERT INTO role_mapping (
          source,
          source_role_code,
          source_role_name,
          canonical_role_code,
          mapping_status,
          dagster_module
        ) VALUES (?, ?, ?, ?, 'mapped', 'backoffice.admin')
        ON CONFLICT (source, source_role_code, source_role_name) DO UPDATE SET
          canonical_role_code = excluded.canonical_role_code,
          mapping_status = excluded.mapping_status,
          dagster_module = excluded.dagster_module`,
      )
      .run(
        mapping.source,
        mapping.source_role_code,
        mapping.source_role_name,
        mapping.canonical_role_code,
      );
  } finally {
    database.close();
  }
}

export async function getSwedenSourceRoleRows(): Promise<
  SwedenSourceRoleRow[]
> {
  const mappings = readSwedenRoleMappings();
  const mappingBySourceRole = new Map(
    mappings.map((mapping) => [
      `${mapping.source}\u0000${mapping.source_role_code}\u0000${mapping.source_role_name}`,
      mapping,
    ]),
  );
  const sourceRows = await chQuery<DistinctSourceRoleRow>(
    DISTINCT_SWEDEN_SOURCE_ROLES_QUERY,
  );

  return sourceRows.map((row) => {
    const mapping =
      mappingBySourceRole.get(
        `${row.source}\u0000${row.source_role_code}\u0000${row.source_role_name}`,
      ) ??
      mappingBySourceRole.get(
        `${row.source}\u0000${row.source_role_code}\u0000`,
      );
    return {
      ...row,
      canonical_role_code: mapping?.canonical_role_code ?? null,
      mapping_status: mapping?.mapping_status ?? "unmapped",
    };
  });
}
