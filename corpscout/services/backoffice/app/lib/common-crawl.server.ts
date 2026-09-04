import { chQuery } from "~/lib/clickhouse.server";
import type { CommonCrawlFilters } from "~/lib/common-crawl";
import { clampPage, clampPageSize } from "~/lib/paging";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";

const WEB_FEATURES_TABLE = "stg_web_domain_match_features";

export interface CommonCrawlSearchRow {
  rootDomain: string;
  organizationName: string;
  address: string;
  industryCode: string;
  industryLabel: string;
  latestCrawlId: string;
  latestPageCount: number;
  crawlCount: number;
  observedAt: string;
}

interface TaxonomyRow {
  normalized_code: string;
}

interface RootDomainRow {
  root_domain: string;
}

interface CountRow {
  total: string;
}

interface CoverageRow {
  root_domain: string;
  latest_crawl_id: string;
  latest_page_count: string;
  crawl_count: string;
  observed_at: string;
}

interface NameRow {
  root_domain: string;
  organization_name: string;
}

interface AddressRow {
  root_domain: string;
  address: string;
}

interface IndustryRow {
  root_domain: string;
  industry_code: string;
  industry_label: string;
}

function normalizeIndustryCode(value: string): string {
  return value.replace(/[^0-9]/g, "");
}

/**
 * Resolves a user-facing NACE code or label against the small reference
 * taxonomy before touching the 100M-row industry feature slice. Both NACE
 * revisions remain eligible because older Common Crawl classifications use
 * revision 2 codes while newer taxonomy rows use revision 2.1.
 */
export async function resolveCommonCrawlIndustryCodes(
  industry: string,
): Promise<string[]> {
  const value = industry.trim();
  if (value === "") return [];
  const normalizedCode = normalizeIndustryCode(value);
  const searchingByCode = normalizedCode.length >= 2 && /^[0-9.\s]+$/.test(value);
  const rows = await chQuery<TaxonomyRow>(
    `SELECT normalized_code
FROM nace_categories
WHERE match(normalized_code, '^[0-9]{2,4}$')
  AND ${
    searchingByCode
      ? "startsWith(normalized_code, {industryCode:String})"
      : "positionCaseInsensitiveUTF8(description_en, {industry:String}) > 0"
  }
GROUP BY normalized_code
ORDER BY length(normalized_code), normalized_code
LIMIT 200`,
    searchingByCode ? { industryCode: normalizedCode } : { industry: value },
  );
  return rows.map((row) => row.normalized_code);
}

function candidateQuery(
  filters: CommonCrawlFilters,
  industryCodes: string[],
): { sql: string; params: Record<string, unknown> } | null {
  const ctes: string[] = [];
  const names: string[] = [];
  const params: Record<string, unknown> = {};

  if (filters.domain !== "") {
    names.push("domain_matches");
    ctes.push(`domain_matches AS (
  SELECT root_domain
  FROM commoncrawl_domains
  PREWHERE startsWith(root_domain, {domain:String})
  GROUP BY root_domain
)`);
    params.domain = filters.domain.toLowerCase();
  }

  if (filters.address !== "") {
    names.push("address_matches");
    ctes.push(`address_matches AS (
  SELECT root_domain
  FROM ${WEB_FEATURES_TABLE}
  PREWHERE feature_type = 'address' AND feature_subtype = 'postal'
  WHERE positionCaseInsensitiveUTF8(raw_value, {address:String}) > 0
  GROUP BY root_domain
)`);
    params.address = filters.address;
  }

  if (filters.industry !== "") {
    if (industryCodes.length === 0) return null;
    names.push("industry_matches");
    ctes.push(`industry_matches AS (
  SELECT root_domain
  FROM ${WEB_FEATURES_TABLE}
  PREWHERE feature_type = 'industry'
    AND feature_subtype = 'nace'
    AND normalized_value IN {industryCodes:Array(String)}
  GROUP BY root_domain
)`);
    params.industryCodes = industryCodes;
  }

  if (names.length === 0) return null;
  const [first, ...rest] = names;
  return {
    sql: `WITH ${ctes.join(",\n")}
SELECT ${first}.root_domain AS root_domain
FROM ${first}
${rest.map((name) => `INNER JOIN ${name} USING (root_domain)`).join("\n")}`,
    params,
  };
}

async function loadCoverage(domains: string[]): Promise<CoverageRow[]> {
  return chQuery<CoverageRow>(
    `WITH coverage_by_crawl AS (
  SELECT
    root_domain,
    crawl_id,
    toString(uniqExact(url)) AS page_count,
    max(resolved_at) AS crawl_observed_at
  FROM commoncrawl_domains
  PREWHERE root_domain IN {domains:Array(String)}
  GROUP BY root_domain, crawl_id
)
SELECT
  root_domain,
  argMax(crawl_id, tuple(crawl_id, crawl_observed_at)) AS latest_crawl_id,
  argMax(page_count, tuple(crawl_id, crawl_observed_at)) AS latest_page_count,
  toString(uniqExact(crawl_id)) AS crawl_count,
  toString(max(crawl_observed_at)) AS observed_at
FROM coverage_by_crawl
GROUP BY root_domain`,
    { domains },
  );
}

async function loadNames(domains: string[]): Promise<NameRow[]> {
  return chQuery<NameRow>(
    `SELECT
  root_domain,
  argMax(
    raw_value,
    tuple(crawl_id, toUInt8(feature_subtype = 'organization_legal_name'), indexed_at)
  ) AS organization_name
FROM ${WEB_FEATURES_TABLE}
PREWHERE feature_type = 'name'
  AND feature_subtype IN ('organization_name', 'organization_legal_name')
WHERE root_domain IN {domains:Array(String)}
GROUP BY root_domain`,
    { domains },
  );
}

async function loadAddresses(
  domains: string[],
  addressFilter: string,
): Promise<AddressRow[]> {
  return chQuery<AddressRow>(
    `SELECT
  root_domain,
  argMax(raw_value, tuple(crawl_id, indexed_at)) AS address
FROM ${WEB_FEATURES_TABLE}
PREWHERE feature_type = 'address' AND feature_subtype = 'postal'
WHERE root_domain IN {domains:Array(String)}
${
  addressFilter === ""
    ? ""
    : "  AND positionCaseInsensitiveUTF8(raw_value, {address:String}) > 0"
}
GROUP BY root_domain`,
    addressFilter === "" ? { domains } : { domains, address: addressFilter },
  );
}

async function loadIndustries(
  domains: string[],
  industryCodes: string[],
): Promise<IndustryRow[]> {
  return chQuery<IndustryRow>(
    `SELECT
  root_domain,
  argMax(nace_code, tuple(crawl_id, is_primary, score, resolved_at)) AS industry_code,
  argMax(nace_label, tuple(crawl_id, is_primary, score, resolved_at)) AS industry_label
FROM commoncrawl_industries
PREWHERE root_domain IN {domains:Array(String)}
WHERE 1 = 1
${
  industryCodes.length === 0
    ? ""
    : "  AND replaceRegexpAll(nace_code, '[^0-9]', '') IN {industryCodes:Array(String)}"
}
GROUP BY root_domain`,
    industryCodes.length === 0 ? { domains } : { domains, industryCodes },
  );
}

async function hydrateRows(
  domains: string[],
  filters: CommonCrawlFilters,
  industryCodes: string[],
): Promise<CommonCrawlSearchRow[]> {
  if (domains.length === 0) return [];
  const [coverageRows, nameRows, addressRows, industryRows] = await Promise.all([
    loadCoverage(domains),
    loadNames(domains),
    loadAddresses(domains, filters.address),
    loadIndustries(domains, filters.industry === "" ? [] : industryCodes),
  ]);
  const coverageByDomain = new Map(
    coverageRows.map((row) => [row.root_domain, row]),
  );
  const nameByDomain = new Map(
    nameRows.map((row) => [row.root_domain, row.organization_name]),
  );
  const addressByDomain = new Map(
    addressRows.map((row) => [row.root_domain, row.address]),
  );
  const industryByDomain = new Map(
    industryRows.map((row) => [row.root_domain, row]),
  );

  return domains.map((rootDomain) => {
    const coverage = coverageByDomain.get(rootDomain);
    const industry = industryByDomain.get(rootDomain);
    return {
      rootDomain,
      organizationName: nameByDomain.get(rootDomain) ?? "",
      address: addressByDomain.get(rootDomain) ?? "",
      industryCode: industry?.industry_code ?? "",
      industryLabel: industry?.industry_label ?? "",
      latestCrawlId: coverage?.latest_crawl_id ?? "",
      latestPageCount: Number(coverage?.latest_page_count ?? 0),
      crawlCount: Number(coverage?.crawl_count ?? 0),
      observedAt: coverage?.observed_at ?? "",
    };
  });
}

export async function searchCommonCrawlDomains(
  filters: CommonCrawlFilters,
  page: number,
  pageSize: number,
): Promise<{ rows: CommonCrawlSearchRow[]; total: number }> {
  const industryCodes =
    filters.industry === ""
      ? []
      : await resolveCommonCrawlIndustryCodes(filters.industry);
  const candidates = candidateQuery(filters, industryCodes);
  if (!candidates) return { rows: [], total: 0 };

  const limit = clampPageSize(pageSize);
  const offset = (clampPage(page) - 1) * limit;
  const [domainRows, countRows] = await Promise.all([
    chQuery<RootDomainRow>(
      `${candidates.sql}
ORDER BY root_domain
${PAGE_LIMIT_OFFSET_SQL}`,
      { ...candidates.params, limit, offset },
    ),
    chQuery<CountRow>(
      `SELECT toString(count()) AS total
FROM (
${candidates.sql}
)`,
      candidates.params,
    ),
  ]);
  const domains = domainRows.map((row) => row.root_domain);
  return {
    rows: await hydrateRows(domains, filters, industryCodes),
    total: Number(countRows[0]?.total ?? 0),
  };
}
