import { chQuery } from "~/lib/clickhouse.server";
import type {
  CompanyWebIntelligence,
  WebAuthoritySnapshot,
  WebContactObservation,
  WebIdentifierObservation,
  WebIndustrySnapshot,
  WebOrganizationClaim,
  WebPageMetadataSnapshot,
  WebSecuritySnapshot,
} from "~/lib/web-intelligence";

const MAX_PROFILE_ROWS = 250;
const MAX_CONTACT_ROWS = 250;
const MAX_IDENTIFIER_ROWS = 500;

interface CrawlCoverageRow {
  crawl_id: string;
  observed_pages: string;
  observed_at: string;
}

interface OrganizationClaimRow {
  crawl_id: string;
  page_url: string;
  script_index: number;
  entity_path: string;
  entity_types: string[];
  name: string;
  legal_name: string;
  description: string;
  entity_url: string;
  logo: string;
  email: string;
  telephone: string;
  same_as: string[];
  country: string;
  founding_year: number;
  employee_count: number;
  resolved_at: string;
}

interface ContactRow {
  crawl_id: string;
  contact_type: string;
  value: string;
  source: string;
  source_url: string;
  resolved_at: string;
}

interface IdentifierRow {
  crawl_id: string;
  id_type: string;
  id_value: string;
  source: string;
  source_url: string;
  url: string;
  resolved_at: string;
}

interface IndustryRow {
  crawl_id: string;
  nace_code: string;
  nace_label: string;
  rank: number;
  is_primary: number;
  score: number;
  nace_method: string;
  source_url: string;
  resolved_at: string;
}

interface PageSignalRow {
  crawl_id: string;
  page_type: string;
  page_type_score: number;
  nace_confident: number;
  nace_margin: number;
  source_url: string;
  resolved_at: string;
}

interface PageMetadataRow {
  crawl_id: string;
  source_url: string;
  title: string;
  meta: Record<string, string>;
  canonical: string;
  hreflang: string[];
  jsonld_types: string[];
  charset: string;
  resolved_at: string;
}

interface SecurityRow {
  crawl_id: string;
  source_url: string;
  headers: Record<string, string>;
  resolved_at: string;
}

interface AuthorityRow {
  crawl_id: string;
  cc_harmonic_centrality: number;
  cc_harmonic_rank: number;
  cc_pagerank: number;
  cc_pagerank_rank: number;
  n_hosts: number;
  resolved_at: string;
}

const crawlCoverageSql = `SELECT
  crawl_id,
  toString(uniqExact(url)) AS observed_pages,
  toString(max(resolved_at)) AS observed_at
FROM commoncrawl_domains
PREWHERE root_domain = {domain:String}
GROUP BY crawl_id
ORDER BY crawl_id DESC
LIMIT 12`;

const organizationClaimsSql = `SELECT
  crawl_id,
  page_url,
  script_index,
  entity_path,
  entity_types,
  name,
  legal_name,
  description,
  entity_url,
  logo,
  email,
  telephone,
  same_as,
  country,
  founding_year,
  employee_count,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_page_jsonld
PREWHERE crawl_id IN {crawlIds:Array(String)}
  AND root_domain = {domain:String}
WHERE is_organization = 1
  AND (
    name != '' OR legal_name != '' OR description != '' OR email != ''
    OR telephone != '' OR length(same_as) > 0
  )
ORDER BY crawl_id DESC, length(page_url), page_url, resolved_at DESC
LIMIT ${MAX_PROFILE_ROWS}`;

const contactsSql = `SELECT
  crawl_id,
  contact_type,
  value,
  source,
  source_url,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_domain_contact_info
PREWHERE root_domain = {domain:String}
ORDER BY resolved_at DESC
LIMIT ${MAX_CONTACT_ROWS}`;

const identifiersSql = `SELECT
  crawl_id,
  id_type,
  id_value,
  source,
  source_url,
  url,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_domain_identifiers
PREWHERE root_domain = {domain:String}
WHERE valid = 1
ORDER BY crawl_id DESC, id_type, id_value, resolved_at DESC
LIMIT ${MAX_IDENTIFIER_ROWS}`;

const industriesSql = `SELECT
  crawl_id,
  nace_code,
  nace_label,
  rank,
  is_primary,
  score,
  nace_method,
  source_url,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_industries
PREWHERE root_domain = {domain:String}
ORDER BY crawl_id DESC, rank, resolved_at DESC
LIMIT 100`;

const pageSignalsSql = `SELECT
  crawl_id,
  page_type,
  page_type_score,
  nace_confident,
  nace_margin,
  source_url,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_page_signals
PREWHERE root_domain = {domain:String}
ORDER BY crawl_id DESC, resolved_at DESC
LIMIT 24`;

const pageMetadataSql = `SELECT
  crawl_id,
  source_url,
  title,
  meta,
  canonical,
  hreflang,
  jsonld_types,
  charset,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_domain_page_meta
PREWHERE root_domain = {domain:String}
ORDER BY crawl_id DESC, resolved_at DESC
LIMIT 24`;

const securitySql = `SELECT
  crawl_id,
  source_url,
  headers,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_domain_security
PREWHERE root_domain = {domain:String}
ORDER BY crawl_id DESC, resolved_at DESC
LIMIT 24`;

const authoritySql = `SELECT
  crawl_id,
  cc_harmonic_centrality,
  cc_harmonic_rank,
  cc_pagerank,
  cc_pagerank_rank,
  n_hosts,
  toString(resolved_at) AS resolved_at
FROM commoncrawl_domain_graph_signals
PREWHERE root_domain = {domain:String}
ORDER BY crawl_id DESC, resolved_at DESC
LIMIT 24`;

/**
 * Mirrors ReplacingMergeTree latest-row semantics after a root-domain-pruned
 * read. This avoids FINAL over multi-billion-row evidence tables.
 */
function latestRowsByKey<T extends { resolved_at: string }>(
  rows: T[],
  keyOf: (row: T) => string,
): T[] {
  const latest = new Map<string, T>();
  for (const row of rows) {
    const key = keyOf(row);
    const existing = latest.get(key);
    if (!existing || row.resolved_at > existing.resolved_at) {
      latest.set(key, row);
    }
  }
  return [...latest.values()];
}

function organizationClaims(
  rows: OrganizationClaimRow[],
): WebOrganizationClaim[] {
  return latestRowsByKey(
    rows,
    (row) =>
      `${row.crawl_id}\u0000${row.page_url}\u0000${row.script_index}\u0000${row.entity_path}`,
  )
    .sort(
      (left, right) =>
        right.crawl_id.localeCompare(left.crawl_id) ||
        left.page_url.length - right.page_url.length ||
        left.page_url.localeCompare(right.page_url),
    )
    .map((row) => ({
      crawlId: row.crawl_id,
      pageUrl: row.page_url,
      entityTypes: row.entity_types,
      name: row.name,
      legalName: row.legal_name,
      description: row.description,
      entityUrl: row.entity_url,
      logo: row.logo,
      email: row.email,
      telephone: row.telephone,
      sameAs: row.same_as,
      country: row.country,
      foundingYear: row.founding_year || null,
      employeeCount: row.employee_count || null,
      observedAt: row.resolved_at,
    }));
}

function contactObservations(rows: ContactRow[]): WebContactObservation[] {
  return latestRowsByKey(rows, (row) => {
    const value =
      row.contact_type === "email" ? row.value.toLowerCase() : row.value;
    return `${row.contact_type}\u0000${value}`;
  })
    .sort(
      (left, right) =>
        left.contact_type.localeCompare(right.contact_type) ||
        left.value.localeCompare(right.value),
    )
    .map((row) => ({
      type: row.contact_type,
      value: row.contact_type === "email" ? row.value.toLowerCase() : row.value,
      source: row.source,
      sourceUrl: row.source_url,
      lastObservedCrawl: row.crawl_id,
      observedAt: row.resolved_at,
    }));
}

function identifierObservations(
  rows: IdentifierRow[],
): WebIdentifierObservation[] {
  const deduplicated = latestRowsByKey(
    rows,
    (row) =>
      `${row.crawl_id}\u0000${row.id_type}\u0000${row.id_value}\u0000${row.url}`,
  );
  const grouped = new Map<
    string,
    {
      type: string;
      value: string;
      sources: Set<string>;
      crawls: Set<string>;
      pages: Set<string>;
      sampleUrls: Set<string>;
    }
  >();
  for (const row of deduplicated) {
    const key = `${row.id_type}\u0000${row.id_value}`;
    const observation = grouped.get(key) ?? {
      type: row.id_type,
      value: row.id_value,
      sources: new Set<string>(),
      crawls: new Set<string>(),
      pages: new Set<string>(),
      sampleUrls: new Set<string>(),
    };
    observation.sources.add(row.source);
    observation.crawls.add(row.crawl_id);
    observation.pages.add(row.url);
    if (observation.sampleUrls.size < 3 && row.source_url) {
      observation.sampleUrls.add(row.source_url);
    }
    grouped.set(key, observation);
  }

  return [...grouped.values()]
    .map((observation) => {
      const crawls = [...observation.crawls].sort();
      return {
        type: observation.type,
        value: observation.value,
        sources: [...observation.sources].sort(),
        firstObservedCrawl: crawls[0] ?? "",
        lastObservedCrawl: crawls.at(-1) ?? "",
        observedCrawls: crawls.length,
        observedPages: observation.pages.size,
        sampleUrls: [...observation.sampleUrls],
      };
    })
    .sort(
      (left, right) =>
        left.type.localeCompare(right.type) ||
        left.value.localeCompare(right.value),
    );
}

function industrySnapshots(
  industryRows: IndustryRow[],
  signalRows: PageSignalRow[],
): WebIndustrySnapshot[] {
  const industries = latestRowsByKey(
    industryRows,
    (row) => `${row.crawl_id}\u0000${row.nace_code}`,
  );
  const signals = latestRowsByKey(signalRows, (row) => row.crawl_id);
  const crawls = new Set([
    ...industries.map((row) => row.crawl_id),
    ...signals.map((row) => row.crawl_id),
  ]);

  return [...crawls]
    .sort((left, right) => right.localeCompare(left))
    .map((crawlId) => {
      const signal = signals.find((row) => row.crawl_id === crawlId);
      const observations = industries
        .filter((row) => row.crawl_id === crawlId)
        .sort((left, right) => left.rank - right.rank);
      return {
        crawlId,
        pageType: signal?.page_type ?? "",
        pageTypeScore: signal ? signal.page_type_score : null,
        classificationConfident: signal ? signal.nace_confident === 1 : null,
        classificationMargin: signal ? signal.nace_margin : null,
        sourceUrl: signal?.source_url ?? observations[0]?.source_url ?? "",
        observedAt: signal?.resolved_at ?? observations[0]?.resolved_at ?? "",
        industries: observations.map((row) => ({
          naceCode: row.nace_code,
          naceLabel: row.nace_label,
          rank: row.rank,
          isPrimary: row.is_primary === 1,
          score: row.score,
          method: row.nace_method,
          sourceUrl: row.source_url,
        })),
      };
    });
}

function pageMetadataSnapshots(
  rows: PageMetadataRow[],
): WebPageMetadataSnapshot[] {
  return latestRowsByKey(rows, (row) => row.crawl_id)
    .sort((left, right) => right.crawl_id.localeCompare(left.crawl_id))
    .map((row) => ({
      crawlId: row.crawl_id,
      sourceUrl: row.source_url,
      title: row.title,
      meta: row.meta,
      canonical: row.canonical,
      hreflang: row.hreflang,
      jsonLdTypes: row.jsonld_types,
      charset: row.charset,
      observedAt: row.resolved_at,
    }));
}

function securitySnapshots(rows: SecurityRow[]): WebSecuritySnapshot[] {
  return latestRowsByKey(rows, (row) => row.crawl_id)
    .sort((left, right) => right.crawl_id.localeCompare(left.crawl_id))
    .map((row) => ({
      crawlId: row.crawl_id,
      sourceUrl: row.source_url,
      headers: row.headers,
      observedAt: row.resolved_at,
    }));
}

function authoritySnapshots(rows: AuthorityRow[]): WebAuthoritySnapshot[] {
  return latestRowsByKey(rows, (row) => row.crawl_id)
    .sort((left, right) => right.crawl_id.localeCompare(left.crawl_id))
    .map((row) => ({
      crawlId: row.crawl_id,
      harmonicCentrality: row.cc_harmonic_centrality,
      harmonicRank: row.cc_harmonic_rank,
      pageRank: row.cc_pagerank,
      pageRankRank: row.cc_pagerank_rank,
      observedHosts: row.n_hosts,
      observedAt: row.resolved_at,
    }));
}

/**
 * Loads bounded, source-linked observations for one exact root domain.
 * Returned claims remain unverified website evidence; callers must not merge
 * them into registry facts without an explicit reconciliation step.
 */
export async function getDomainWebIntelligence(
  domain: string,
): Promise<CompanyWebIntelligence> {
  const coveragePromise = chQuery<CrawlCoverageRow>(crawlCoverageSql, {
    domain,
  });
  const otherEvidencePromise = Promise.all([
    chQuery<ContactRow>(contactsSql, { domain }),
    chQuery<IdentifierRow>(identifiersSql, { domain }),
    chQuery<IndustryRow>(industriesSql, { domain }),
    chQuery<PageSignalRow>(pageSignalsSql, { domain }),
    chQuery<PageMetadataRow>(pageMetadataSql, { domain }),
    chQuery<SecurityRow>(securitySql, { domain }),
    chQuery<AuthorityRow>(authoritySql, { domain }),
  ]);
  const coverageRows = await coveragePromise;
  const crawlIds = coverageRows.map((row) => row.crawl_id);
  const profilePromise = crawlIds.length
    ? chQuery<OrganizationClaimRow>(organizationClaimsSql, {
        domain,
        crawlIds,
      })
    : Promise.resolve([]);
  const [profileRows, evidenceRows] = await Promise.all([
    profilePromise,
    otherEvidencePromise,
  ]);
  const [
    contactRows,
    identifierRows,
    industryRows,
    signalRows,
    metadataRows,
    securityRows,
    authorityRows,
  ] = evidenceRows;

  return {
    domain,
    crawlCoverage: coverageRows.map((row) => ({
      crawlId: row.crawl_id,
      observedPages: Number(row.observed_pages),
      observedAt: row.observed_at,
    })),
    organizationClaims: organizationClaims(profileRows),
    contacts: contactObservations(contactRows),
    identifiers: identifierObservations(identifierRows),
    industrySnapshots: industrySnapshots(industryRows, signalRows),
    pageMetadataSnapshots: pageMetadataSnapshots(metadataRows),
    securitySnapshots: securitySnapshots(securityRows),
    authoritySnapshots: authoritySnapshots(authorityRows),
    truncated: {
      organizationClaims: profileRows.length === MAX_PROFILE_ROWS,
      contacts: contactRows.length === MAX_CONTACT_ROWS,
      identifiers: identifierRows.length === MAX_IDENTIFIER_ROWS,
    },
  };
}
