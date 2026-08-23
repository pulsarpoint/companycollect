import { chQuery } from "~/lib/clickhouse.server";

/** One curated role of one person at this company. */
export interface SeCompanyPersonRoleRow {
  person_id: string;
  role_code: string;
  /** company_person_role_type's display name, falling back to the raw code so
   * a role the catalog has not learned yet still shows something. */
  role_label: string;
  role_group: string;
  fiscal_year: string;
  sources: string[];
  source_count: number;
  is_current: number;
  first_observed_at: string;
  last_observed_at: string;
}

/** One published person of this company, with the roles merged in. */
export interface SeCompanyPersonRow {
  person_id: string;
  name: string;
  description: string;
  draft_count: number;
  correction_count: number;
  merged_into_person_id: string;
  updated_at: string;
  roles: SeCompanyPersonRoleRow[];
}

/**
 * se_company_person is a ReplacingMergeTree, so FINAL. `draft_ids` and
 * `correction_ids` are counted in ClickHouse rather than shipped: the list
 * page shows "how much evidence" and "was it reviewed", and the person page
 * is where the ids themselves belong. A company with more than a few hundred
 * published people does not exist in this register, so the LIMIT is a
 * backstop, not paging.
 */
export const PEOPLE_SQL = `SELECT
  toString(p.person_id) AS person_id,
  p.name AS name,
  ifNull(p.description, '') AS description,
  toUInt32(length(p.draft_ids)) AS draft_count,
  toUInt32(length(p.correction_ids)) AS correction_count,
  ifNull(toString(p.merged_into_person_id), '') AS merged_into_person_id,
  toString(p.updated_at) AS updated_at
FROM corpscout.se_company_person AS p FINAL
WHERE p.company_id = {companyId:String}
ORDER BY p.name, p.person_id
LIMIT 300`;

/**
 * Roles for the whole company in one read, joined to the role catalog for a
 * label. `company_person_role_type` is a ReplacingMergeTree too, and its
 * String columns come back as '' on a LEFT JOIN miss rather than null -- hence
 * nullIf before the fallback, so an unknown role_code shows the code instead
 * of a blank cell.
 *
 * The company filter sits in a subquery rather than in the outer WHERE: with
 * `FINAL` on the left side of a LEFT JOIN, ClickHouse 26.5 pushes the
 * predicate into a block that no longer carries `company_id` and fails with
 * NOT_FOUND_COLUMN_IN_BLOCK. Filtering first also means FINAL dedups only
 * this company's parts.
 */
export const PEOPLE_ROLES_SQL = `SELECT
  toString(r.person_id) AS person_id,
  r.role_code AS role_code,
  ifNull(nullIf(t.display_name, ''), r.role_code) AS role_label,
  ifNull(toString(t.role_group), '') AS role_group,
  ifNull(toString(r.fiscal_year), '') AS fiscal_year,
  r.sources AS sources,
  toUInt32(r.source_count) AS source_count,
  toUInt8(r.is_current) AS is_current,
  toString(r.first_observed_at) AS first_observed_at,
  toString(r.last_observed_at) AS last_observed_at
FROM (
  SELECT person_id, role_code, fiscal_year, sources, source_count, is_current,
    first_observed_at, last_observed_at
  FROM corpscout.se_company_person_role FINAL
  WHERE company_id = {companyId:String}
) AS r
LEFT JOIN corpscout.company_person_role_type AS t FINAL
  ON t.role_code = r.role_code
ORDER BY r.is_current DESC, r.fiscal_year DESC NULLS LAST, r.role_code
LIMIT 900`;

/** Published people of one company, each carrying its own roles. */
export async function loadSeCompanyPeople(
  companyId: string,
): Promise<SeCompanyPersonRow[]> {
  const [people, roles] = await Promise.all([
    chQuery<Omit<SeCompanyPersonRow, "roles">>(PEOPLE_SQL, { companyId }),
    chQuery<SeCompanyPersonRoleRow>(PEOPLE_ROLES_SQL, { companyId }),
  ]);
  const rolesByPerson = new Map<string, SeCompanyPersonRoleRow[]>();
  for (const role of roles) {
    const group = rolesByPerson.get(role.person_id);
    if (group) group.push(role);
    else rolesByPerson.set(role.person_id, [role]);
  }
  return people.map((person) => ({
    ...person,
    roles: rolesByPerson.get(person.person_id) ?? [],
  }));
}
