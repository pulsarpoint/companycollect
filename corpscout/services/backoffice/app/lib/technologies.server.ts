/**
 * `/admin/technologies` and `/admin/technologies/:slug`: reads over the
 * technology catalog (`corpscout.technology_catalog`, 7.9k rows,
 * ReplacingMergeTree -- always FINAL, same as technology-catalog.server.ts),
 * the weekly `technology_adoption` rollup, and one LIVE key-pruned query over
 * `commoncrawl_page_technologies` (10.6B rows, sort key head = root_domain).
 *
 * The rollup MAY BE EMPTY (its first run can still be in flight): every read
 * of it tolerates zero rows -- the list LEFT JOINs it and shows nothing, the
 * detail GROUP BYs it so an empty table yields no row, never a fake zero.
 *
 * The Swedish-companies query is the only touch of the 10.6B-row table and
 * is ONLY ever filtered by an explicit root_domain IN (SELECT ... FROM
 * company_domains WHERE country_code = 'SE') set (~17k domains), which
 * ClickHouse materializes first and prunes the sort key with. Grouping the
 * whole table is forbidden in a page load -- that is what the weekly rollup
 * is for.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import {
  SE_COMPANIES_USING_TECHNOLOGY_LIMIT,
  type TechnologyListFilters,
} from "~/lib/technologies";

export const TECHNOLOGY_CATALOG_TABLE = "corpscout.technology_catalog";
export const TECHNOLOGY_ADOPTION_TABLE = "corpscout.technology_adoption";

/* -------------------------------------------------------------------- */
/* Index: the catalog as a server-paged list                             */
/* -------------------------------------------------------------------- */

export interface TechnologyListRow {
  technology: string;
  slug: string;
  description: string;
  website: string;
  categories: string[];
  has_icon: 0 | 1;
  saas: 0 | 1;
  oss: 0 | 1;
  /** '' when the adoption rollup has no row for this technology (including
   * the rollup being entirely empty); otherwise a stringified count. */
  domain_count: string;
}

/**
 * The adoption LEFT JOIN reads the newest rollup row per technology
 * (argMax over computed_at: the rollup is weekly and append-shaped, so a
 * technology may carry several weeks of rows). A non-match yields the join
 * default '' technology, which is how `domain_count` distinguishes "not
 * computed" ('') from a real 0.
 */
export const TECHNOLOGY_ADOPTION_JOIN_SQL = `LEFT JOIN (
  SELECT
    technology,
    argMax(domain_count, computed_at) AS domain_count
  FROM ${TECHNOLOGY_ADOPTION_TABLE}
  GROUP BY technology
) AS adoption ON adoption.technology = catalog.technology`;

export const TECHNOLOGY_LIST_SELECT_SQL = `SELECT
  catalog.technology AS technology,
  catalog.slug AS slug,
  catalog.description AS description,
  catalog.website AS website,
  catalog.categories AS categories,
  toUInt8(catalog.icon_object_key != '') AS has_icon,
  toUInt8(catalog.saas) AS saas,
  toUInt8(catalog.oss) AS oss,
  if(adoption.technology != '', toString(adoption.domain_count), '') AS domain_count
FROM ${TECHNOLOGY_CATALOG_TABLE} AS catalog FINAL
${TECHNOLOGY_ADOPTION_JOIN_SQL}`;

/** Most-adopted first; with the rollup still empty every count ties at 0 and
 * the order degrades to alphabetical, which is the right cold-start list. */
const TECHNOLOGY_LIST_ORDER_SQL =
  "ORDER BY adoption.domain_count DESC, catalog.technology ASC";

/** Mirrors se-people-sources.server.ts's buildSourceWhere: a filter is
 * appended only when present, never as a SQL no-op, values always as named
 * params. */
function buildListWhere(filters: TechnologyListFilters): {
  where: string[];
  params: Record<string, unknown>;
} {
  const where: string[] = [];
  const params: Record<string, unknown> = {};
  const q = filters.q.trim();
  if (q !== "") {
    where.push("catalog.technology ILIKE {q:String}");
    params.q = `%${q}%`;
  }
  const category = filters.category.trim();
  if (category !== "") {
    where.push("has(catalog.categories, {category:String})");
    params.category = category;
  }
  return { where, params };
}

function whereClause(where: string[]): string {
  return where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
}

export async function listTechnologiesPage(
  filters: TechnologyListFilters,
  page: number,
  pageSize: number,
): Promise<TechnologyListRow[]> {
  const { where, params } = buildListWhere(filters);
  const limit = clampPageSize(pageSize);
  const offset = (clampPage(page) - 1) * limit;
  return chQuery<TechnologyListRow>(
    `${TECHNOLOGY_LIST_SELECT_SQL}
${whereClause(where)}
${TECHNOLOGY_LIST_ORDER_SQL}
${PAGE_LIMIT_OFFSET_SQL}`,
    { ...params, limit, offset },
  );
}

/** Same WHERE as the page, no join -- both filters only touch the catalog. */
export async function countTechnologies(
  filters: TechnologyListFilters,
): Promise<number> {
  const { where, params } = buildListWhere(filters);
  const [row] = await chQuery<{ total: string }>(
    `SELECT toString(count()) AS total
FROM ${TECHNOLOGY_CATALOG_TABLE} AS catalog FINAL
${whereClause(where)}`,
    params,
  );
  return Number(row?.total ?? 0);
}

export const TECHNOLOGY_CATEGORY_OPTIONS_SQL = `SELECT DISTINCT
  arrayJoin(categories) AS category
FROM ${TECHNOLOGY_CATALOG_TABLE} FINAL
ORDER BY category
LIMIT 500`;

/** The category filter's options, from the catalog's own values -- a 7.9k-row
 * FINAL scan, cheap. LIMIT 500 is a hard safety cap, not paging. */
export async function loadTechnologyCategoryOptions(): Promise<string[]> {
  const rows = await chQuery<{ category: string }>(
    TECHNOLOGY_CATEGORY_OPTIONS_SQL,
  );
  return rows.map((row) => row.category).filter((value) => value !== "");
}

/* -------------------------------------------------------------------- */
/* Detail: one catalog row by slug                                       */
/* -------------------------------------------------------------------- */

export interface TechnologyDetail {
  technology: string;
  slug: string;
  description: string;
  website: string;
  categories: string[];
  icon: boolean;
  saas: boolean;
  oss: boolean;
  pricing: string[];
  source: string;
  source_version: string;
  updated_at: string;
}

interface TechnologyDetailRow {
  technology: string;
  slug: string;
  description: string;
  website: string;
  categories: string[];
  has_icon: 0 | 1;
  saas: 0 | 1;
  oss: 0 | 1;
  pricing: string[];
  source: string;
  source_version: string;
  updated_at: string;
}

export const TECHNOLOGY_DETAIL_SQL = `SELECT
  technology,
  slug,
  description,
  website,
  categories,
  toUInt8(icon_object_key != '') AS has_icon,
  toUInt8(saas) AS saas,
  toUInt8(oss) AS oss,
  pricing,
  source,
  source_version,
  toString(updated_at) AS updated_at
FROM ${TECHNOLOGY_CATALOG_TABLE} FINAL
WHERE slug = {slug:String}
LIMIT 1`;

/** Null when the slug is unknown -- the route turns that into its 404. */
export async function loadTechnologyDetail(
  slug: string,
): Promise<TechnologyDetail | null> {
  const rows = await chQuery<TechnologyDetailRow>(TECHNOLOGY_DETAIL_SQL, {
    slug,
  });
  const row = rows[0];
  if (!row) return null;
  return {
    technology: row.technology,
    slug: row.slug,
    description: row.description,
    website: row.website,
    categories: row.categories,
    icon: Boolean(row.has_icon),
    saas: Boolean(row.saas),
    oss: Boolean(row.oss),
    pricing: row.pricing,
    source: row.source,
    source_version: row.source_version,
    updated_at: row.updated_at,
  };
}

/* -------------------------------------------------------------------- */
/* Detail: global adoption from the weekly rollup                        */
/* -------------------------------------------------------------------- */

export interface TechnologyAdoption {
  domainCount: number;
  computedAt: string;
}

/** GROUP BY so an empty rollup (or an unknown technology) yields ZERO rows,
 * not one aggregate row of defaults masquerading as a count of 0. The output
 * aliases must NOT reuse the source column names: ClickHouse substitutes
 * aliases into sibling expressions, and `max(computed_at) AS computed_at`
 * would turn argMax's second argument into an aggregate-inside-aggregate. */
export const TECHNOLOGY_ADOPTION_SQL = `SELECT
  technology,
  toString(argMax(domain_count, computed_at)) AS latest_domain_count,
  toString(max(computed_at)) AS latest_computed_at
FROM ${TECHNOLOGY_ADOPTION_TABLE}
WHERE technology = {name:String}
GROUP BY technology`;

/** Null means "not computed yet" -- the rollup's first run may be in flight. */
export async function loadTechnologyAdoption(
  name: string,
): Promise<TechnologyAdoption | null> {
  const rows = await chQuery<{
    latest_domain_count: string;
    latest_computed_at: string;
  }>(TECHNOLOGY_ADOPTION_SQL, { name });
  const row = rows[0];
  if (!row) return null;
  return {
    domainCount: Number(row.latest_domain_count),
    computedAt: row.latest_computed_at,
  };
}

/* -------------------------------------------------------------------- */
/* Detail: Swedish companies using the technology (live, key-pruned)     */
/* -------------------------------------------------------------------- */

export interface SeCompanyUsingTechnology {
  company_id: string;
  root_domain: string;
  /** '' when se_companies has no row for the id (the link still works). */
  legal_name: string;
}

/**
 * The inner IN-subquery hands ClickHouse the explicit SE root_domain set
 * (~17k rows, no FINAL needed -- IN is set semantics, a stale replaced row
 * only widens the probe set) so the 10.6B-row scan prunes on its sort key
 * head. The catalog's exact detector name arrives as a named param, resolved
 * from the slug by the caller -- never interpolated. The join back to
 * company_domains recovers (company_id, root_domain) and reads FINAL like
 * every other company_domains page read; se_companies supplies display
 * names.
 */
export const SE_COMPANIES_USING_TECHNOLOGY_SQL = `SELECT DISTINCT
  domains.company_id AS company_id,
  domains.root_domain AS root_domain,
  companies.legal_name AS legal_name
FROM corpscout.company_domains AS domains FINAL
LEFT JOIN corpscout.se_companies AS companies FINAL
  ON companies.company_id = domains.company_id
WHERE domains.country_code = 'SE'
  AND domains.root_domain IN (
    SELECT DISTINCT root_domain
    FROM corpscout.commoncrawl_page_technologies
    WHERE root_domain IN (
      SELECT root_domain
      FROM corpscout.company_domains
      WHERE country_code = 'SE'
    )
      AND technology = {name:String}
  )
ORDER BY legal_name ASC, company_id ASC, root_domain ASC
LIMIT ${SE_COMPANIES_USING_TECHNOLOGY_LIMIT}`;

/**
 * `name` is the catalog's exact `technology` value (detection rows store the
 * detector name, not the slug). Capped rows -- the section is a sample, not
 * an export.
 *
 * Guarded read (mirrors se-people-tasks.server.ts): this is the page's one
 * genuinely heavy query, and while something big (the adoption rollup's own
 * weekly materialization, say) saturates the server it can outlive the
 * client timeout. That must degrade to a section-level notice, never 500 the
 * whole detail page -- the catalog record and rollup count above it are
 * still perfectly renderable.
 */
export async function loadSeCompaniesUsingTechnology(
  name: string,
): Promise<{ rows: SeCompanyUsingTechnology[]; error: string }> {
  try {
    const rows = await chQuery<SeCompanyUsingTechnology>(
      SE_COMPANIES_USING_TECHNOLOGY_SQL,
      { name },
    );
    return { rows, error: "" };
  } catch (error) {
    return {
      rows: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
