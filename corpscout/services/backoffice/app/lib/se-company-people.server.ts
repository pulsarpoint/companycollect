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

/** One source observation of one person at this company, with the role AS THE
 * SOURCE WROTE IT (role_original / role / role_label) -- no canonical mapping.
 * `period` is whatever time context the source gives: a fiscal year
 * (bolagsverket), an effective range (esef) or a start–end range (wikidata),
 * already formatted because the shapes are source-specific. */
export interface SeCompanyPersonEvidenceRow {
  source: string;
  full_name: string;
  role: string;
  period: string;
}

/** All source observations of one person name, across sources. */
export interface SeCompanyPersonEvidenceGroup {
  full_name: string;
  sources: string[];
  entries: SeCompanyPersonEvidenceRow[];
}

/**
 * The raw evidence read: every person the three SOURCE views know at this
 * company, with original role text -- deliberately NOT the published/merged
 * tables (owner decision 2026-08-28: until the Ratsit spine lands, the company
 * People tab shows what each source says, verbatim). The role fallbacks mirror
 * each view's own shape: bolagsverket's free-text role_original falls back to
 * its role_kind enum, esef's role text to its role_category, wikidata's
 * role_label to the raw P-code.
 */
export const PEOPLE_EVIDENCE_SQL = `SELECT source, full_name, role, period
FROM (
  SELECT
    'bolagsverket' AS source,
    full_name AS full_name,
    if(trim(role_original) != '', role_original, role_kind) AS role,
    if(fiscal_year > 0, toString(fiscal_year), '') AS period
  FROM corpscout.se_company_person_bolagsverket
  WHERE company_id = {companyId:String} AND trim(full_name) != ''

  UNION ALL

  SELECT
    'esef' AS source,
    full_name AS full_name,
    if(trim(role) != '', role, role_category) AS role,
    trim(concat(ifNull(toString(effective_from), ''), ' – ', ifNull(toString(effective_to), ''))) AS period
  FROM corpscout.se_company_person_esef
  WHERE company_id = {companyId:String} AND trim(full_name) != ''

  UNION ALL

  SELECT
    'wikidata' AS source,
    full_name AS full_name,
    if(trim(role_label) != '', role_label, role_property) AS role,
    trim(concat(ifNull(toString(start_date), ''), ' – ', ifNull(toString(end_date), ''))) AS period
  FROM corpscout.se_company_person_wikidata
  WHERE company_id = {companyId:String} AND trim(full_name) != ''
)
ORDER BY full_name, source, period, role
LIMIT 900`;

/** Every source observation of this company's people, grouped by the verbatim
 * person name (no identity resolution -- two spellings are two groups, which
 * is exactly the honesty an evidence panel owes the reader). */
export async function loadSeCompanyPeopleEvidence(
  companyId: string,
): Promise<SeCompanyPersonEvidenceGroup[]> {
  const rows = await chQuery<SeCompanyPersonEvidenceRow>(PEOPLE_EVIDENCE_SQL, {
    companyId,
  });
  const groups = new Map<string, SeCompanyPersonEvidenceGroup>();
  for (const row of rows) {
    // The SQL always concatenates "start – end"; a missing side leaves a
    // dangling dash ("2018-01-01 –", "– 2020-06-30", or just "–").
    const period =
      row.period === "–"
        ? ""
        : row.period.endsWith(" –")
          ? `from ${row.period.slice(0, -2).trim()}`
          : row.period.startsWith("– ")
            ? `until ${row.period.slice(2).trim()}`
            : row.period;
    const cleaned = { ...row, period };
    const group = groups.get(row.full_name);
    if (group) {
      group.entries.push(cleaned);
      if (!group.sources.includes(row.source)) group.sources.push(row.source);
    } else {
      groups.set(row.full_name, {
        full_name: row.full_name,
        sources: [row.source],
        entries: [cleaned],
      });
    }
  }
  return [...groups.values()];
}
