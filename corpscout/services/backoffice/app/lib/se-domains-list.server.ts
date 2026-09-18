import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import type { SeDomainsFilters } from "~/lib/se-domains-filters";
import {
  PAGE_LIMIT_OFFSET_SQL,
  SE_COMPANIES_SERVING_TABLE,
} from "~/lib/se-company-info-lists.server";

/**
 * The `/admin/se/companies/domains` list and the `/domains/:domain` detail page,
 * read straight off the `se_company_domain` entity (not the
 * `company_domains_resolved` view the per-company tab reads: this is the admin
 * view of what the fold holds, before any reviewer overlay).
 *
 * `se_company_domain` is a ReplacingMergeTree keyed on (company_id,
 * root_domain), so every read goes through FINAL. A domain appears once PER
 * COMPANY that claims it; `company_count` is how many companies claim that
 * domain in the whole entity (never just within the current filter), which is
 * what the `shared` filter and the detail page's "N companies" both mean.
 */

const SE_COMPANY_DOMAIN_TABLE = "corpscout.se_company_domain";

/** How many companies claim each domain, over the whole entity. */
const COMPANY_COUNT_BY_DOMAIN_SQL = `SELECT root_domain, uniqExact(company_id) AS company_count
  FROM ${SE_COMPANY_DOMAIN_TABLE} FINAL
  GROUP BY root_domain`;

const DOMAIN_ROW_COLUMNS_SQL = `d.company_id AS company_id,
  d.root_domain AS root_domain,
  d.website_url AS website_url,
  d.website_host AS website_host,
  toString(d.association) AS association,
  toUInt8(d.is_primary) AS is_primary,
  toFloat64(d.confidence) AS confidence,
  d.sources AS sources,
  d.supporting_sources AS supporting_sources,
  toString(d.verification_status) AS verification_status,
  toString(d.review_status) AS review_status,
  toUInt8(d.active) AS active,
  toString(d.inactive_reason) AS inactive_reason,
  toUInt32(s.company_count) AS company_count`;

const DOMAIN_ROWS_FROM_SQL = `FROM ${SE_COMPANY_DOMAIN_TABLE} AS d FINAL
INNER JOIN (${COMPANY_COUNT_BY_DOMAIN_SQL}) AS s ON s.root_domain = d.root_domain`;

export const DOMAINS_LIST_SELECT_SQL = `SELECT
  ${DOMAIN_ROW_COLUMNS_SQL}
${DOMAIN_ROWS_FROM_SQL}`;

export const DOMAINS_COUNTS_SQL = `SELECT
  toString(count()) AS rows,
  toString(uniqExact(d.root_domain)) AS domains,
  toString(uniqExact(d.company_id)) AS companies,
  toString(uniqExactIf(d.root_domain, s.company_count > 1)) AS shared
${DOMAIN_ROWS_FROM_SQL}`;

export const DOMAINS_COMPANY_NAMES_SQL = `SELECT company_id, legal_name
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE company_id IN {companyIds:Array(String)}`;

/** Every company claiming one domain, most confident first. */
export const DOMAIN_COMPANIES_SQL = `SELECT
  ${DOMAIN_ROW_COLUMNS_SQL}
${DOMAIN_ROWS_FROM_SQL}
WHERE d.root_domain = {domain:String}
ORDER BY d.confidence DESC, d.is_primary DESC, d.company_id`;

/** Which of a set of domains are SE company domains, and claimed by how many. */
export const DOMAIN_COMPANY_COUNTS_SQL = `SELECT root_domain, toString(uniqExact(company_id)) AS company_count
FROM ${SE_COMPANY_DOMAIN_TABLE} FINAL
WHERE root_domain IN {domains:Array(String)}
GROUP BY root_domain`;

/** One (company, domain) row of the entity, as the list and the detail page show it. */
export interface SeDomainRawRow {
  company_id: string;
  root_domain: string;
  website_url: string;
  website_host: string;
  association: string;
  is_primary: number;
  confidence: number;
  sources: string[];
  supporting_sources: string[];
  verification_status: string;
  review_status: string;
  active: number;
  inactive_reason: string;
  /** Companies claiming this domain across the whole entity. */
  company_count: number;
}

export interface SeDomainRow extends SeDomainRawRow {
  /** From the serving view; "" when the view has no row for the company. */
  legal_name: string;
}

export interface SeDomainsCounts {
  rows: number;
  domains: number;
  companies: number;
  shared: number;
}

/* -------------------------------------------------------------------- */
/* The WHERE clause both readers share                                   */
/* -------------------------------------------------------------------- */

export interface SeDomainsFilterSql {
  where: string[];
  params: Record<string, unknown>;
}

/**
 * One predicate per non-empty filter, every request value a bound parameter.
 * `status` and `shared` name fixed predicates rather than binding anything.
 */
export function buildSeDomainsFilter(filters: SeDomainsFilters): SeDomainsFilterSql {
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  if (filters.domain !== "") {
    where.push("d.root_domain LIKE {domain:String}");
    params.domain = `%${filters.domain}%`;
  }
  if (filters.company !== "") {
    where.push("d.company_id = {company:String}");
    params.company = filters.company;
  }
  if (filters.association !== "") {
    where.push("d.association = {association:String}");
    params.association = filters.association;
  }
  if (filters.status === "active") {
    where.push("d.active = 1");
  } else if (filters.status === "inactive") {
    where.push("d.active = 0");
  }
  if (filters.minConfidence !== "") {
    where.push("d.confidence >= {minConfidence:Float64}");
    params.minConfidence = Number(filters.minConfidence);
  }
  if (filters.maxConfidence !== "") {
    where.push("d.confidence <= {maxConfidence:Float64}");
    params.maxConfidence = Number(filters.maxConfidence);
  }
  if (filters.shared === "1") {
    where.push("s.company_count > 1");
  }
  return { where, params };
}

function whereSql(where: readonly string[]): string {
  return where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
}

/* -------------------------------------------------------------------- */
/* Company names                                                         */
/* -------------------------------------------------------------------- */

/**
 * Names the rows' distinct company ids in ONE `DOMAINS_COMPANY_NAMES_SQL` read
 * and attaches `legal_name` (`""` when the view has no row) -- never a join,
 * and never one lookup per row.
 */
async function withLegalNames(rows: SeDomainRawRow[]): Promise<SeDomainRow[]> {
  const companyIds = [...new Set(rows.map((row) => row.company_id))];
  const names =
    companyIds.length > 0
      ? await chQuery<{ company_id: string; legal_name: string }>(DOMAINS_COMPANY_NAMES_SQL, {
          companyIds,
        })
      : [];
  const nameByCompanyId = new Map(names.map((row) => [row.company_id, row.legal_name]));
  return rows.map((row) => ({
    ...row,
    legal_name: nameByCompanyId.get(row.company_id) ?? "",
  }));
}

/* -------------------------------------------------------------------- */
/* The page                                                              */
/* -------------------------------------------------------------------- */

export interface SeDomainsListQuery extends SeDomainsFilters {
  page: number;
  pageSize: number;
}

export interface SeDomainsListPage {
  rows: SeDomainRow[];
}

/** The page: most confident first, then domain, then company id for a stable order. */
export async function listSeDomainsPage(query: SeDomainsListQuery): Promise<SeDomainsListPage> {
  const { where, params } = buildSeDomainsFilter(query);
  const limit = clampPageSize(query.pageSize);
  const page = clampPage(query.page);
  const offset = (page - 1) * limit;
  const sql = `${DOMAINS_LIST_SELECT_SQL}
${whereSql(where)}
ORDER BY d.confidence DESC, d.root_domain, d.company_id
${PAGE_LIMIT_OFFSET_SQL}`;
  const rawRows = await chQuery<SeDomainRawRow>(sql, { ...params, limit, offset });
  return { rows: await withLegalNames(rawRows) };
}

/* -------------------------------------------------------------------- */
/* The counts strip                                                      */
/* -------------------------------------------------------------------- */

/**
 * `DOMAINS_COUNTS_SQL` under the SAME `where`/params `listSeDomainsPage` built
 * -- `rows` is also the pager's total.
 */
export async function loadSeDomainsCounts(filters: SeDomainsFilters): Promise<SeDomainsCounts> {
  const { where, params } = buildSeDomainsFilter(filters);
  const sql = `${DOMAINS_COUNTS_SQL}
${whereSql(where)}`;
  const [row] = await chQuery<{ rows: string; domains: string; companies: string; shared: string }>(
    sql,
    params,
  );
  return {
    rows: Number(row?.rows ?? 0),
    domains: Number(row?.domains ?? 0),
    companies: Number(row?.companies ?? 0),
    shared: Number(row?.shared ?? 0),
  };
}

/* -------------------------------------------------------------------- */
/* The domain detail page                                                */
/* -------------------------------------------------------------------- */

/** Every company claiming `domain`, named, most confident first. Empty when
 * the entity has no row for it. */
export async function loadSeDomainCompanies(domain: string): Promise<SeDomainRow[]> {
  const rows = await chQuery<SeDomainRawRow>(DOMAIN_COMPANIES_SQL, { domain });
  return withLegalNames(rows);
}

/**
 * `domain -> companies claiming it`, for the domains in `domains` that the
 * entity knows at all (absent = not an SE company domain). One read for a
 * whole page of graph neighbours; no read at all for an empty page.
 */
export async function loadSeDomainCompanyCounts(domains: string[]): Promise<Map<string, number>> {
  if (domains.length === 0) return new Map();
  const rows = await chQuery<{ root_domain: string; company_count: string }>(
    DOMAIN_COMPANY_COUNTS_SQL,
    { domains },
  );
  return new Map(rows.map((row) => [row.root_domain, Number(row.company_count)]));
}
