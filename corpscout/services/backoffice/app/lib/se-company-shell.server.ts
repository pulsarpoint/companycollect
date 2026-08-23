import { chQuery } from "~/lib/clickhouse.server";

/**
 * The company identity every tab of the admin company area shares: the header
 * renders it once, above the `<Outlet />`, so a tab never has to re-read who
 * the company is.
 */
export interface SeCompanyShell {
  company_id: string;
  legal_name: string;
  legal_form_code: string;
  status: string;
  incorporation_date: string;
  /**
   * True when the shell came from `se_company_info` -- Dagster has published
   * this company. False means the company exists in the register but its
   * enrichment run has not landed yet, which is a normal pipeline state, not
   * a broken link.
   */
  published: boolean;
  /** From company_entity_types; "" when the legal form has no classification. */
  entity_type_label: string;
  is_public_sector: boolean;
}

/** What both shell queries project, before `published` is decided. */
interface SeCompanyShellQueryRow {
  company_id: string;
  legal_name: string;
  legal_form_code: string;
  status: string;
  incorporation_date: string;
}

interface SeEntityTypeQueryRow {
  entity_type_label: string;
  is_public_sector: number;
}

/**
 * Both shell queries project the same five columns under the same names, so
 * one row type covers either source. `FINAL` on both: se_company_info and
 * se_companies are ReplacingMergeTrees, and the newest version is the only
 * one a reviewer may be shown. Nullable columns are collapsed with ifNull so
 * the header never has to distinguish "" from null.
 */
export const SHELL_INFO_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  ifNull(i.legal_form_code, '') AS legal_form_code,
  toString(i.status) AS status,
  ifNull(toString(i.incorporation_date), '') AS incorporation_date
FROM corpscout.se_company_info AS i FINAL
WHERE i.company_id = {companyId:String}
LIMIT 1`;

export const SHELL_REGISTER_SQL = `SELECT
  c.company_id AS company_id,
  c.legal_name AS legal_name,
  ifNull(c.legal_form_code, '') AS legal_form_code,
  toString(c.status) AS status,
  ifNull(toString(c.incorporation_date), '') AS incorporation_date
FROM corpscout.se_companies AS c FINAL
WHERE c.company_id = {companyId:String}
LIMIT 1`;

/** The register's legal-form code classified once, centrally (see
 * entity-type.server.ts for why this is not derived per surface). */
export const SHELL_ENTITY_TYPE_SQL = `SELECT
  t.entity_type_label AS entity_type_label,
  toUInt8(t.is_public_sector) AS is_public_sector
FROM corpscout.company_entity_types AS t FINAL
WHERE t.country_code = 'SE' AND t.legal_form_code = {legalFormCode:String}
LIMIT 1`;

/**
 * Loads the company header for `/admin/se/company/:companyId/*`.
 *
 * Published rows win: se_company_info is what every surface actually serves.
 * A company missing from it is looked up in the register instead, and the
 * caller shows a "not published yet" note beside an otherwise complete
 * header. Null means neither table knows this id -- a 404.
 */
export async function loadSeCompanyShell(
  companyId: string,
): Promise<SeCompanyShell | null> {
  const [infoRows, registerRows] = await Promise.all([
    chQuery<SeCompanyShellQueryRow>(SHELL_INFO_SQL, { companyId }),
    chQuery<SeCompanyShellQueryRow>(SHELL_REGISTER_SQL, { companyId }),
  ]);
  const published = infoRows[0];
  const row = published ?? registerRows[0];
  if (!row) return null;
  const entityRows =
    row.legal_form_code === ""
      ? []
      : await chQuery<SeEntityTypeQueryRow>(SHELL_ENTITY_TYPE_SQL, {
          legalFormCode: row.legal_form_code,
        });
  const entity = entityRows[0];
  return {
    ...row,
    published: Boolean(published),
    entity_type_label: entity?.entity_type_label ?? "",
    is_public_sector: Number(entity?.is_public_sector ?? 0) === 1,
  };
}
