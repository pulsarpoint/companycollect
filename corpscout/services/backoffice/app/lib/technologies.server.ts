/**
 * `/admin/technologies` and `/admin/technologies/:slug`: reads over the
 * technology catalog (`corpscout.technology_catalog`, 7.9k rows,
 * ReplacingMergeTree -- always FINAL, same as technology-catalog.server.ts)
 * and the three weekly rollups: `technology_adoption` (per-technology domain
 * count), `technology_top_domains` (top ~500 crawled domains per technology
 * by CommonCrawl harmonic centrality) and `technology_companies` (the
 * companies whose registered domains carry a detection, per country).
 *
 * EVERY rollup MAY BE EMPTY (a first population can still be in flight):
 * every read tolerates zero rows -- the list LEFT JOINs adoption and shows
 * nothing, the detail reads GROUP BY so an empty table yields no row (a null
 * "not computed yet", never a fake zero), and the two tab reads simply page
 * over nothing. The rollups are ReplacingMergeTree(computed_at), so every
 * read dedupes by its key with argMax/GROUP BY, keeping the latest
 * computed_at -- never a bare scan that could double-count a week.
 *
 * The old LIVE key-pruned query over `commoncrawl_page_technologies` (10.6B
 * rows, 15-18s a page load) is gone: the Companies tab reads the
 * `technology_companies` rollup instead. Touching the 10.6B-row table from a
 * page load is forbidden -- that is what the weekly rollups are for.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import type { TechnologyListFilters } from "~/lib/technologies";

export const TECHNOLOGY_CATALOG_TABLE = "corpscout.technology_catalog";
export const TECHNOLOGY_ADOPTION_TABLE = "corpscout.technology_adoption";
export const TECHNOLOGY_TOP_DOMAINS_TABLE = "corpscout.technology_top_domains";
export const TECHNOLOGY_COMPANIES_TABLE = "corpscout.technology_companies";

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
/* Detail: the weekly rollups behind the two adoption tabs               */
/* -------------------------------------------------------------------- */

/**
 * The tab header's "computed weekly" stamp: the newest computed_at the rollup
 * holds for this technology. GROUP BY so an empty rollup (mid first
 * population) yields ZERO rows -- null, "not computed yet" -- never a zero
 * date pretending to be a run. One shape for both rollup tables; the table
 * name comes from the two exported constants above, never from user input.
 */
function rollupComputedAtSql(table: string): string {
  return `SELECT
  toString(max(computed_at)) AS latest_computed_at
FROM ${table}
WHERE technology = {name:String}
GROUP BY technology`;
}

export const TECHNOLOGY_TOP_DOMAINS_COMPUTED_AT_SQL = rollupComputedAtSql(
  TECHNOLOGY_TOP_DOMAINS_TABLE,
);
export const TECHNOLOGY_COMPANIES_COMPUTED_AT_SQL = rollupComputedAtSql(
  TECHNOLOGY_COMPANIES_TABLE,
);

async function loadRollupComputedAt(
  sql: string,
  name: string,
): Promise<string | null> {
  const rows = await chQuery<{ latest_computed_at: string }>(sql, { name });
  return rows[0]?.latest_computed_at ?? null;
}

/* ---------------------- Domains tab (top domains) -------------------- */

export interface TechnologyDomainRow {
  root_domain: string;
  /** UInt64 arrives quoted from ClickHouse JSON; kept as a string. */
  harmonic_rank: string;
  harmonic_centrality: number;
}

/**
 * ReplacingMergeTree(computed_at) keyed by (technology, root_domain): GROUP
 * BY the domain and argMax over computed_at so an unmerged week never shows a
 * domain twice or with a stale rank. The output aliases must NOT reuse the
 * source column names (see TECHNOLOGY_ADOPTION_SQL's alias-substitution
 * note). Ordered by harmonic centrality, best-connected domain first; the
 * rollup itself caps the set at ~500 domains per technology, so paging over
 * it is cheap.
 */
export const TECHNOLOGY_TOP_DOMAINS_SQL = `SELECT
  root_domain,
  toString(argMax(harmonic_rank, computed_at)) AS latest_harmonic_rank,
  argMax(harmonic_centrality, computed_at) AS latest_harmonic_centrality
FROM ${TECHNOLOGY_TOP_DOMAINS_TABLE}
WHERE technology = {name:String}
GROUP BY root_domain
ORDER BY latest_harmonic_centrality DESC, root_domain ASC
${PAGE_LIMIT_OFFSET_SQL}`;

export async function loadTechnologyDomainsPage(
  name: string,
  page: number,
  pageSize: number,
): Promise<TechnologyDomainRow[]> {
  const limit = clampPageSize(pageSize);
  const offset = (clampPage(page) - 1) * limit;
  const rows = await chQuery<{
    root_domain: string;
    latest_harmonic_rank: string;
    latest_harmonic_centrality: number;
  }>(TECHNOLOGY_TOP_DOMAINS_SQL, { name, limit, offset });
  return rows.map((row) => ({
    root_domain: row.root_domain,
    harmonic_rank: row.latest_harmonic_rank,
    harmonic_centrality: row.latest_harmonic_centrality,
  }));
}

/** Distinct domains, matching the page query's GROUP BY dedupe. */
export const TECHNOLOGY_TOP_DOMAINS_COUNT_SQL = `SELECT
  toString(uniqExact(root_domain)) AS total
FROM ${TECHNOLOGY_TOP_DOMAINS_TABLE}
WHERE technology = {name:String}`;

export async function countTechnologyDomains(name: string): Promise<number> {
  const [row] = await chQuery<{ total: string }>(
    TECHNOLOGY_TOP_DOMAINS_COUNT_SQL,
    { name },
  );
  return Number(row?.total ?? 0);
}

export async function loadTechnologyDomainsComputedAt(
  name: string,
): Promise<string | null> {
  return loadRollupComputedAt(TECHNOLOGY_TOP_DOMAINS_COMPUTED_AT_SQL, name);
}

/* ---------------------- Companies tab (per country) ------------------ */

export interface TechnologyCompanyIndustry {
  code: string;
  label: string;
  is_primary: 0 | 1;
}

export interface TechnologyCompanyRow {
  country_code: string;
  company_id: string;
  root_domain: string;
  /** '' when no per-country name source exists for the row (non-SE, or an SE
   * id se_companies has no row for) -- the link still works. */
  legal_name: string;
  /** Empty for non-SE rows -- no per-country industry source yet. */
  industries: TechnologyCompanyIndustry[];
}

/**
 * ReplacingMergeTree(computed_at) keyed by (technology, country_code,
 * company_id, root_domain): GROUP BY the key IS the dedupe (computed_at is
 * the only non-key column, and the page does not display it). The optional
 * country filter is appended only when applied, value always a named param.
 */
export const TECHNOLOGY_COMPANIES_SELECT_SQL = `SELECT
  country_code,
  company_id,
  root_domain
FROM ${TECHNOLOGY_COMPANIES_TABLE}
WHERE technology = {name:String}`;

const TECHNOLOGY_COMPANIES_TAIL_SQL = `GROUP BY country_code, company_id, root_domain
ORDER BY country_code ASC, company_id ASC, root_domain ASC
${PAGE_LIMIT_OFFSET_SQL}`;

const COMPANIES_COUNTRY_FILTER_SQL = "AND country_code = {country:String}";

function companiesFilter(country: string): {
  filterSql: string;
  params: Record<string, unknown>;
} {
  if (country === "") return { filterSql: "", params: {} };
  return { filterSql: COMPANIES_COUNTRY_FILTER_SQL, params: { country } };
}

/**
 * Display names for the page's SE rows, keyed by the page's own company ids
 * (a per-page point lookup, same as the old live query's join). Sweden is
 * the only country with a register table here so far -- when another
 * country's rollup rows appear, add its name lookup beside this one and
 * merge it in `loadTechnologyCompaniesPage` (the extension point below).
 */
export const TECHNOLOGY_SE_COMPANY_NAMES_SQL = `SELECT
  company_id,
  legal_name
FROM corpscout.se_companies FINAL
WHERE company_id IN {ids:Array(String)}`;

/**
 * NACE industries for the page's SE rows, primary classification first.
 * `se_company_industry_display_current` is already a deduplicated _current
 * view -- no FINAL/argMax needed, same as company-sections.server.ts's
 * industries read. Non-SE countries have no industry source yet: same
 * extension point as the name lookup above.
 */
export const TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL = `SELECT
  company_id,
  classification_code AS code,
  label_en AS label,
  toUInt8(is_primary) AS is_primary
FROM corpscout.se_company_industry_display_current
WHERE company_id IN {ids:Array(String)}
ORDER BY company_id, is_primary DESC, classification_system, classification_code`;

export async function loadTechnologyCompaniesPage(
  name: string,
  country: string,
  page: number,
  pageSize: number,
): Promise<TechnologyCompanyRow[]> {
  const limit = clampPageSize(pageSize);
  const offset = (clampPage(page) - 1) * limit;
  const { filterSql, params } = companiesFilter(country);
  const base = await chQuery<{
    country_code: string;
    company_id: string;
    root_domain: string;
  }>(
    `${TECHNOLOGY_COMPANIES_SELECT_SQL}
${filterSql ? `  ${filterSql}\n` : ""}${TECHNOLOGY_COMPANIES_TAIL_SQL}`,
    { ...params, name, limit, offset },
  );

  // Per-country enrichment, keyed by THIS page's ids only. SE is the only
  // country with a name/industry source today; other countries fall back to
  // the bare company_id and an empty industries cell. EXTENSION POINT: a new
  // country's register lands here as another pair of keyed lookups.
  const seIds = [
    ...new Set(
      base
        .filter((row) => row.country_code === "SE")
        .map((row) => row.company_id),
    ),
  ];
  const names = new Map<string, string>();
  const industries = new Map<string, TechnologyCompanyIndustry[]>();
  if (seIds.length > 0) {
    const [nameRows, industryRows] = await Promise.all([
      chQuery<{ company_id: string; legal_name: string }>(
        TECHNOLOGY_SE_COMPANY_NAMES_SQL,
        { ids: seIds },
      ),
      chQuery<TechnologyCompanyIndustry & { company_id: string }>(
        TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL,
        { ids: seIds },
      ),
    ]);
    for (const row of nameRows) names.set(row.company_id, row.legal_name);
    for (const row of industryRows) {
      const list = industries.get(row.company_id) ?? [];
      list.push({ code: row.code, label: row.label, is_primary: row.is_primary });
      industries.set(row.company_id, list);
    }
  }

  return base.map((row) => ({
    country_code: row.country_code,
    company_id: row.company_id,
    root_domain: row.root_domain,
    legal_name:
      row.country_code === "SE" ? (names.get(row.company_id) ?? "") : "",
    industries:
      row.country_code === "SE"
        ? (industries.get(row.company_id) ?? [])
        : [],
  }));
}

/** Distinct rollup keys under the same filter as the page. */
export const TECHNOLOGY_COMPANIES_COUNT_SQL = `SELECT
  toString(uniqExact(country_code, company_id, root_domain)) AS total
FROM ${TECHNOLOGY_COMPANIES_TABLE}
WHERE technology = {name:String}`;

export async function countTechnologyCompanies(
  name: string,
  country: string,
): Promise<number> {
  const { filterSql, params } = companiesFilter(country);
  const [row] = await chQuery<{ total: string }>(
    `${TECHNOLOGY_COMPANIES_COUNT_SQL}${filterSql ? `\n  ${filterSql}` : ""}`,
    { ...params, name },
  );
  return Number(row?.total ?? 0);
}

/** The country filter's options: whatever countries the rollup actually
 * holds for this technology (today only SE; new countries appear here on
 * their own as the rollup grows). Unfiltered by the applied country, so the
 * Select can always switch back. */
export const TECHNOLOGY_COMPANY_COUNTRIES_SQL = `SELECT DISTINCT
  country_code
FROM ${TECHNOLOGY_COMPANIES_TABLE}
WHERE technology = {name:String}
ORDER BY country_code ASC`;

export async function loadTechnologyCompanyCountries(
  name: string,
): Promise<string[]> {
  const rows = await chQuery<{ country_code: string }>(
    TECHNOLOGY_COMPANY_COUNTRIES_SQL,
    { name },
  );
  return rows
    .map((row) => row.country_code)
    .filter((value) => value !== "");
}

export async function loadTechnologyCompaniesComputedAt(
  name: string,
): Promise<string | null> {
  return loadRollupComputedAt(TECHNOLOGY_COMPANIES_COMPUTED_AT_SQL, name);
}
