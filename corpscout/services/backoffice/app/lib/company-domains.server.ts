import { chInsertCompanyDomains, chQuery } from "~/lib/clickhouse.server";

export const COMPANY_DOMAIN_REVIEW_STATUSES = [
  "unreviewed",
  "confirmed_primary",
  "confirmed_related",
  "rejected",
] as const;

export type CompanyDomainReviewStatus =
  (typeof COMPANY_DOMAIN_REVIEW_STATUSES)[number];

export interface CommonCrawlDomainEvidence {
  type: "common_crawl_match";
  signalType: string;
  sourceField: string;
  companyValue: string;
  domainValue: string;
  scoreContribution: number;
  sourceUrl: string;
  crawlId: string;
  extractionMethod: string;
  sourceObservedAt: string;
  warcFilename: string;
  warcRecordOffset: number;
  warcRecordLength: number;
  discoveryRunId: string;
  suggestedAt: string;
}

export interface WikidataDomainEvidence {
  type: "wikidata_match";
  wikidataId: string;
  matchMethod: string;
  matchConfidence: number;
  identifierType: string;
  propertyId: string;
  companyValue: string;
  wikidataValue: string;
  sourceRecordId: string;
  wikidataUrl: string;
  retrievedAt: string;
}

export type CompanyDomainSourceEvidence =
  | CommonCrawlDomainEvidence
  | WikidataDomainEvidence;

export interface CompanyDomainSource {
  name: string;
  confidence: number;
  sourceRecordId: string;
  sourceUrl: string;
  confidenceBasis: string;
  evidence: CompanyDomainSourceEvidence[];
}

export interface CompanyDomain {
  countryCode: string;
  companyId: string;
  rootDomain: string;
  websiteUrl: string;
  websiteHost: string;
  sources: CompanyDomainSource[];
  suggestedConfidence: number;
  suggestedPrimary: boolean;
  evidenceFingerprint: string;
  reviewStatus: CompanyDomainReviewStatus;
  reviewNote: string;
  reviewedBy: string;
  reviewedAt: string | null;
  reviewedEvidenceFingerprint: string;
  evidenceChanged: boolean;
  active: boolean;
  firstSeenAt: string;
  lastSeenAt: string;
  resolvedAt: string;
}

export const COMPANY_DOMAIN_SOURCES = [
  "all",
  "wikidata",
  "esef_filing",
  "common_crawl_identity",
] as const;

export type CompanyDomainSourceFilter = (typeof COMPANY_DOMAIN_SOURCES)[number];

export type CompanyDomainReviewQueueRow = CompanyDomain & {
  companyName: string;
};

export interface CompanyDomainReviewQueueResult {
  rows: CompanyDomainReviewQueueRow[];
  total: number;
  page: number;
  pageSize: number;
}

interface CompanyDomainRow {
  country_code: string;
  company_id: string;
  root_domain: string;
  website_url: string;
  website_host: string;
  source_names: string[];
  source_confidences: Array<number | string>;
  source_record_ids: string[];
  source_urls: string[];
  confidence_bases: string[];
  suggested_confidence: number | string;
  suggested_primary: number | string;
  evidence_fingerprint: string;
  review_status: string;
  review_note: string;
  reviewed_by: string;
  reviewed_at: string;
  reviewed_evidence_fingerprint: string;
  is_active: number | string;
  first_seen_at: string;
  last_seen_at: string;
  resolved_at: string;
}

type CompanyDomainQueueRow = CompanyDomainRow & { company_name: string };

interface CommonCrawlEvidenceRow {
  root_domain: string;
  signal_type: string;
  source_field: string;
  company_value: string;
  domain_value: string;
  score_contribution: number | string;
  source_url: string;
  crawl_id: string;
  discovery_run_id: string;
  suggested_at: string;
}

interface CommonCrawlIdentifierProvenanceRow {
  crawl_id: string;
  root_domain: string;
  source_field: string;
  domain_value: string;
  source_url: string;
  extraction_method: string;
  source_observed_at: string;
}

interface CommonCrawlWarcProvenanceRow {
  crawl_id: string;
  root_domain: string;
  source_url: string;
  warc_filename: string;
  warc_record_offset: number | string;
  warc_record_length: number | string;
}

interface WikidataMatchEvidenceRow {
  wikidata_id: string;
  match_method: string;
  match_confidence: number | string;
  identifier_type: string;
  wikidata_property_id: string;
  company_value: string;
  wikidata_value: string;
  source_record_id: string;
  wikidata_url: string;
  retrieved_at: string;
}

interface RecordCompanyDomainReviewInput {
  domains: CompanyDomain[];
  rootDomain: string;
  reviewStatus: CompanyDomainReviewStatus;
  note?: string;
  reviewedBy?: string;
  reviewedAt?: string;
}

export class CompanyDomainReviewValidationError extends Error {}

export const COMPANY_DOMAINS_QUERY = `SELECT
  country_code,
  company_id,
  root_domain,
  website_url,
  website_host,
  source_names,
  source_confidences,
  source_record_ids,
  source_urls,
  confidence_bases,
  suggested_confidence,
  suggested_primary,
  evidence_fingerprint,
  review_status,
  review_note,
  reviewed_by,
  ifNull(toString(reviewed_at), '') AS reviewed_at,
  reviewed_evidence_fingerprint,
  is_active,
  toString(first_seen_at) AS first_seen_at,
  toString(last_seen_at) AS last_seen_at,
  toString(resolved_at) AS resolved_at
FROM company_domains FINAL
WHERE country_code = {country:String}
  AND company_id = {companyId:String}
ORDER BY
  is_active DESC,
  review_status = 'confirmed_primary' DESC,
  suggested_primary DESC,
  suggested_confidence DESC,
  root_domain`;

export const COMPANY_DOMAIN_COMMON_CRAWL_EVIDENCE_QUERY = `SELECT
  root_domain,
  signal_type,
  source_field,
  company_value,
  domain_value,
  score_contribution,
  source_url,
  crawl_id,
  discovery_run_id,
  toString(suggested_at) AS suggested_at
FROM company_domain_suggestion_evidence_active
WHERE country_iso2 = {country:String}
  AND company_id = {companyId:String}
ORDER BY root_domain, score_contribution DESC, signal_type, source_field`;

export const COMPANY_DOMAIN_WIKIDATA_EVIDENCE_QUERY = `SELECT
  current.identifier_value AS wikidata_id,
  current.match_method,
  current.match_confidence,
  ids.identifier_type,
  ids.wikidata_property_id,
  current.company_id AS company_value,
  ids.identifier_value AS wikidata_value,
  ids.source_record_id AS source_record_id,
  companies.wikidata_url,
  toString(ids.retrieved_at) AS retrieved_at
FROM company_external_identifier_current AS current
INNER JOIN wikidata_company_identifiers AS ids FINAL
  ON ids.wikidata_id = current.identifier_value
 AND (
   (current.match_method = 'wikidata_registry_identifier'
     AND ids.identifier_type = 'se_orgnr')
   OR (current.match_method = 'wikidata_verified_lei'
     AND ids.identifier_type = 'lei')
 )
INNER JOIN wikidata_companies AS companies FINAL
  ON companies.wikidata_id = current.identifier_value
WHERE current.country_code = {country:String}
  AND current.company_id = {companyId:String}
  AND current.identifier_scheme = 'wikidata'
ORDER BY wikidata_id, identifier_type, wikidata_value`;

export const COMPANY_DOMAIN_IDENTIFIER_PROVENANCE_QUERY = `SELECT
  crawl_id,
  root_domain,
  lowerUTF8(id_type) AS source_field,
  id_value AS domain_value,
  source_url,
  argMax(source, resolved_at) AS extraction_method,
  toString(max(resolved_at)) AS source_observed_at
FROM commoncrawl_domain_identifiers FINAL
WHERE crawl_id IN {crawlIds:Array(String)}
  AND root_domain IN {rootDomains:Array(String)}
  AND source_url IN {sourceUrls:Array(String)}
  AND lowerUTF8(id_type) IN {sourceFields:Array(String)}
GROUP BY crawl_id, root_domain, source_field, domain_value, source_url`;

export const COMPANY_DOMAIN_WARC_PROVENANCE_QUERY = `WITH page_provenance AS (
  SELECT
    crawl_id,
    root_domain,
    page_url AS source_url,
    argMax(warc_filename, resolved_at) AS warc_filename,
    argMax(warc_record_offset, resolved_at) AS warc_record_offset,
    argMax(warc_record_length, resolved_at) AS warc_record_length,
    max(resolved_at) AS source_resolved_at
  FROM commoncrawl_page_technologies FINAL
  WHERE crawl_id IN {crawlIds:Array(String)}
    AND root_domain IN {rootDomains:Array(String)}
    AND page_url IN {sourceUrls:Array(String)}
  GROUP BY crawl_id, root_domain, source_url

  UNION ALL

  SELECT
    crawl_id,
    root_domain,
    page_url AS source_url,
    argMax(warc_filename, resolved_at) AS warc_filename,
    argMax(warc_record_offset, resolved_at) AS warc_record_offset,
    argMax(warc_record_length, resolved_at) AS warc_record_length,
    max(resolved_at) AS source_resolved_at
  FROM commoncrawl_page_jsonld FINAL
  WHERE crawl_id IN {crawlIds:Array(String)}
    AND root_domain IN {rootDomains:Array(String)}
    AND page_url IN {sourceUrls:Array(String)}
  GROUP BY crawl_id, root_domain, source_url
)
SELECT
  crawl_id,
  root_domain,
  source_url,
  argMax(warc_filename, source_resolved_at) AS warc_filename,
  argMax(warc_record_offset, source_resolved_at) AS warc_record_offset,
  argMax(warc_record_length, source_resolved_at) AS warc_record_length
FROM page_provenance
GROUP BY crawl_id, root_domain, source_url`;

function normalizeRootDomain(value: string): string {
  const normalized = value.trim().toLowerCase().replace(/\.$/, "");
  if (
    normalized.length === 0 ||
    normalized.length > 253 ||
    !normalized.includes(".") ||
    !/^[a-z0-9.-]+$/.test(normalized)
  ) {
    throw new CompanyDomainReviewValidationError(
      "A valid root domain is required.",
    );
  }
  return normalized;
}

function wikidataIdFromSource(source: {
  sourceRecordId: string;
  sourceUrl: string;
}): string {
  return (
    source.sourceRecordId.match(/\bQ\d+\b/)?.[0] ??
    source.sourceUrl.match(/\bQ\d+\b/)?.[0] ??
    ""
  );
}

function commonCrawlEvidenceKey(input: {
  crawlId: string;
  rootDomain: string;
  sourceField: string;
  domainValue: string;
  sourceUrl: string;
}): string {
  return [
    input.crawlId,
    input.rootDomain,
    input.sourceField,
    input.domainValue,
    input.sourceUrl,
  ].join("\u0000");
}

function warcProvenanceKey(input: {
  crawlId: string;
  rootDomain: string;
  sourceUrl: string;
}): string {
  return [input.crawlId, input.rootDomain, input.sourceUrl].join("\u0000");
}

async function hydrateCommonCrawlEvidence(
  rows: CommonCrawlEvidenceRow[],
): Promise<Map<string, CommonCrawlDomainEvidence[]>> {
  if (rows.length === 0) return new Map();

  const crawlIds = [...new Set(rows.map((row) => row.crawl_id))];
  const rootDomains = [...new Set(rows.map((row) => row.root_domain))];
  const sourceUrls = [...new Set(rows.map((row) => row.source_url))];
  const identifierRows = rows.filter(
    (row) => row.signal_type === "identifier",
  );
  const [identifierProvenance, warcProvenance] = await Promise.all([
    identifierRows.length === 0
      ? Promise.resolve([] as CommonCrawlIdentifierProvenanceRow[])
      : chQuery<CommonCrawlIdentifierProvenanceRow>(
          COMPANY_DOMAIN_IDENTIFIER_PROVENANCE_QUERY,
          {
            crawlIds,
            rootDomains,
            sourceUrls,
            sourceFields: [
              ...new Set(identifierRows.map((row) => row.source_field)),
            ],
          },
        ),
    chQuery<CommonCrawlWarcProvenanceRow>(
      COMPANY_DOMAIN_WARC_PROVENANCE_QUERY,
      { crawlIds, rootDomains, sourceUrls },
    ),
  ]);
  const identifierByKey = new Map(
    identifierProvenance.map((row) => [
      commonCrawlEvidenceKey({
        crawlId: row.crawl_id,
        rootDomain: row.root_domain,
        sourceField: row.source_field,
        domainValue: row.domain_value,
        sourceUrl: row.source_url,
      }),
      row,
    ]),
  );
  const warcByKey = new Map(
    warcProvenance.map((row) => [
      warcProvenanceKey({
        crawlId: row.crawl_id,
        rootDomain: row.root_domain,
        sourceUrl: row.source_url,
      }),
      row,
    ]),
  );
  const evidenceByDomain = new Map<string, CommonCrawlDomainEvidence[]>();

  for (const row of rows) {
    const identifier = identifierByKey.get(
      commonCrawlEvidenceKey({
        crawlId: row.crawl_id,
        rootDomain: row.root_domain,
        sourceField: row.source_field,
        domainValue: row.domain_value,
        sourceUrl: row.source_url,
      }),
    );
    const warc = warcByKey.get(
      warcProvenanceKey({
        crawlId: row.crawl_id,
        rootDomain: row.root_domain,
        sourceUrl: row.source_url,
      }),
    );
    const evidence: CommonCrawlDomainEvidence = {
      type: "common_crawl_match",
      signalType: row.signal_type,
      sourceField: row.source_field,
      companyValue: row.company_value,
      domainValue: row.domain_value,
      scoreContribution: Number(row.score_contribution),
      sourceUrl: row.source_url,
      crawlId: row.crawl_id,
      extractionMethod: identifier?.extraction_method ?? "",
      sourceObservedAt: identifier?.source_observed_at ?? "",
      warcFilename: warc?.warc_filename ?? "",
      warcRecordOffset: Number(warc?.warc_record_offset ?? 0),
      warcRecordLength: Number(warc?.warc_record_length ?? 0),
      discoveryRunId: row.discovery_run_id,
      suggestedAt: row.suggested_at,
    };
    const domainEvidence = evidenceByDomain.get(row.root_domain) ?? [];
    domainEvidence.push(evidence);
    evidenceByDomain.set(row.root_domain, domainEvidence);
  }
  return evidenceByDomain;
}

function wikidataEvidenceById(
  rows: WikidataMatchEvidenceRow[],
): Map<string, WikidataDomainEvidence[]> {
  const evidenceById = new Map<string, WikidataDomainEvidence[]>();
  for (const row of rows) {
    const evidence: WikidataDomainEvidence = {
      type: "wikidata_match",
      wikidataId: row.wikidata_id,
      matchMethod: row.match_method,
      matchConfidence: Number(row.match_confidence),
      identifierType: row.identifier_type,
      propertyId: row.wikidata_property_id,
      companyValue: row.company_value,
      wikidataValue: row.wikidata_value,
      sourceRecordId: row.source_record_id,
      wikidataUrl: row.wikidata_url,
      retrievedAt: row.retrieved_at,
    };
    const itemEvidence = evidenceById.get(row.wikidata_id) ?? [];
    itemEvidence.push(evidence);
    evidenceById.set(row.wikidata_id, itemEvidence);
  }
  return evidenceById;
}

function domainFromRow(
  row: CompanyDomainRow,
  commonCrawlEvidence = new Map<string, CommonCrawlDomainEvidence[]>(),
  wikidataEvidence = new Map<string, WikidataDomainEvidence[]>(),
): CompanyDomain {
  if (
    !COMPANY_DOMAIN_REVIEW_STATUSES.includes(
      row.review_status as CompanyDomainReviewStatus,
    )
  ) {
    throw new Error(
      `Unknown company-domain review status: ${row.review_status}`,
    );
  }
  const sourceCount = row.source_names.length;
  if (
    row.source_confidences.length !== sourceCount ||
    row.source_record_ids.length !== sourceCount ||
    row.source_urls.length !== sourceCount ||
    row.confidence_bases.length !== sourceCount
  ) {
    throw new Error(
      `Company-domain source arrays are misaligned for ${row.root_domain}`,
    );
  }
  return {
    countryCode: row.country_code,
    companyId: row.company_id,
    rootDomain: row.root_domain,
    websiteUrl: row.website_url,
    websiteHost: row.website_host,
    sources: row.source_names.map((name, index) => {
      const source = {
        name,
        confidence: Number(row.source_confidences[index]),
        sourceRecordId: row.source_record_ids[index] ?? "",
        sourceUrl: row.source_urls[index] ?? "",
        confidenceBasis: row.confidence_bases[index] ?? "",
      };
      const evidence =
        name === "common_crawl_identity"
          ? (commonCrawlEvidence.get(row.root_domain) ?? [])
          : name === "wikidata"
            ? (wikidataEvidence.get(wikidataIdFromSource(source)) ?? [])
            : [];
      return { ...source, evidence };
    }),
    suggestedConfidence: Number(row.suggested_confidence),
    suggestedPrimary: Boolean(Number(row.suggested_primary)),
    evidenceFingerprint: row.evidence_fingerprint,
    reviewStatus: row.review_status as CompanyDomainReviewStatus,
    reviewNote: row.review_note,
    reviewedBy: row.reviewed_by,
    reviewedAt: row.reviewed_at || null,
    reviewedEvidenceFingerprint: row.reviewed_evidence_fingerprint,
    evidenceChanged:
      row.reviewed_evidence_fingerprint !== "" &&
      row.reviewed_evidence_fingerprint !== row.evidence_fingerprint,
    active: Boolean(Number(row.is_active)),
    firstSeenAt: row.first_seen_at,
    lastSeenAt: row.last_seen_at,
    resolvedAt: row.resolved_at,
  };
}

export async function getUnifiedCompanyDomains(
  countryCode: string,
  companyId: string,
): Promise<CompanyDomain[]> {
  if (countryCode.toUpperCase() !== "SE") return [];
  const params = {
    country: "SE",
    companyId: companyId.trim(),
  };
  const [rows, commonCrawlRows, wikidataRows] = await Promise.all([
    chQuery<CompanyDomainRow>(COMPANY_DOMAINS_QUERY, params),
    chQuery<CommonCrawlEvidenceRow>(
      COMPANY_DOMAIN_COMMON_CRAWL_EVIDENCE_QUERY,
      params,
    ),
    chQuery<WikidataMatchEvidenceRow>(
      COMPANY_DOMAIN_WIKIDATA_EVIDENCE_QUERY,
      params,
    ),
  ]);
  const commonCrawlEvidence =
    await hydrateCommonCrawlEvidence(commonCrawlRows);
  const wikidataEvidence = wikidataEvidenceById(wikidataRows);
  return rows.map((row) =>
    domainFromRow(row, commonCrawlEvidence, wikidataEvidence),
  );
}

const COMPANY_DOMAIN_QUEUE_WHERE = `WHERE domains.country_code = {country:String}
  AND domains.is_active = 1
  AND domains.review_status = 'unreviewed'
  AND (
    {query:String} = ''
    OR positionCaseInsensitiveUTF8(companies.legal_name, {query:String}) > 0
    OR positionCaseInsensitiveUTF8(domains.company_id, {query:String}) > 0
    OR positionCaseInsensitiveUTF8(domains.root_domain, {query:String}) > 0
  )
  AND ({source:String} = 'all' OR has(domains.source_names, {source:String}))`;

export const COMPANY_DOMAIN_REVIEW_QUEUE_COUNT_QUERY = `SELECT count() AS total
FROM company_domains AS domains FINAL
INNER JOIN se_companies AS companies FINAL
  ON companies.company_id = domains.company_id
${COMPANY_DOMAIN_QUEUE_WHERE}`;

export const COMPANY_DOMAIN_REVIEW_QUEUE_QUERY = `SELECT
  domains.country_code,
  domains.company_id,
  companies.legal_name AS company_name,
  domains.root_domain,
  domains.website_url,
  domains.website_host,
  domains.source_names,
  domains.source_confidences,
  domains.source_record_ids,
  domains.source_urls,
  domains.confidence_bases,
  domains.suggested_confidence,
  domains.suggested_primary,
  domains.evidence_fingerprint,
  domains.review_status,
  domains.review_note,
  domains.reviewed_by,
  ifNull(toString(domains.reviewed_at), '') AS reviewed_at,
  domains.reviewed_evidence_fingerprint,
  domains.is_active,
  toString(domains.first_seen_at) AS first_seen_at,
  toString(domains.last_seen_at) AS last_seen_at,
  toString(domains.resolved_at) AS resolved_at
FROM company_domains AS domains FINAL
INNER JOIN se_companies AS companies FINAL
  ON companies.company_id = domains.company_id
${COMPANY_DOMAIN_QUEUE_WHERE}
ORDER BY domains.suggested_confidence DESC, companies.legal_name,
  domains.company_id, domains.root_domain
LIMIT {limit:UInt16} OFFSET {offset:UInt64}`;

export async function searchCompanyDomainReviewQueue(
  countryCode: string,
  options: {
    query?: string;
    source?: CompanyDomainSourceFilter;
    page?: number;
    pageSize?: number;
  } = {},
): Promise<CompanyDomainReviewQueueResult> {
  if (countryCode.toUpperCase() !== "SE") {
    return { rows: [], total: 0, page: 1, pageSize: 50 };
  }
  const pageSize = [25, 50, 100].includes(options.pageSize ?? 50)
    ? (options.pageSize ?? 50)
    : 50;
  const requestedPage = Number.isFinite(options.page)
    ? Math.max(1, Math.trunc(options.page ?? 1))
    : 1;
  const source = COMPANY_DOMAIN_SOURCES.includes(
    options.source as CompanyDomainSourceFilter,
  )
    ? (options.source as CompanyDomainSourceFilter)
    : "all";
  const params = {
    country: "SE",
    query: options.query?.trim() ?? "",
    source,
    limit: pageSize,
    offset: (requestedPage - 1) * pageSize,
  };
  const [countRows, rows] = await Promise.all([
    chQuery<{ total: number | string }>(
      COMPANY_DOMAIN_REVIEW_QUEUE_COUNT_QUERY,
      params,
    ),
    chQuery<CompanyDomainQueueRow>(COMPANY_DOMAIN_REVIEW_QUEUE_QUERY, params),
  ]);
  const total = Number(countRows[0]?.total ?? 0);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(requestedPage, lastPage);
  const pageRows =
    page === requestedPage
      ? rows
      : await chQuery<CompanyDomainQueueRow>(
          COMPANY_DOMAIN_REVIEW_QUEUE_QUERY,
          {
            ...params,
            offset: (page - 1) * pageSize,
          },
        );
  return {
    rows: pageRows.map((row) => ({
      ...domainFromRow(row),
      companyName: row.company_name,
    })),
    total,
    page,
    pageSize,
  };
}

function reviewedRow(
  domain: CompanyDomain,
  reviewStatus: CompanyDomainReviewStatus,
  note: string,
  reviewedBy: string,
  reviewedAt: string,
): Record<string, unknown> {
  const reviewed = reviewStatus !== "unreviewed";
  return {
    country_code: domain.countryCode,
    company_id: domain.companyId,
    root_domain: domain.rootDomain,
    website_url: domain.websiteUrl,
    website_host: domain.websiteHost,
    source_names: domain.sources.map((source) => source.name),
    source_confidences: domain.sources.map((source) => source.confidence),
    source_record_ids: domain.sources.map((source) => source.sourceRecordId),
    source_urls: domain.sources.map((source) => source.sourceUrl),
    confidence_bases: domain.sources.map((source) => source.confidenceBasis),
    suggested_confidence: domain.suggestedConfidence,
    suggested_primary: domain.suggestedPrimary ? 1 : 0,
    evidence_fingerprint: domain.evidenceFingerprint,
    review_status: reviewStatus,
    review_note: reviewed ? note : "",
    reviewed_by: reviewed ? reviewedBy : "",
    reviewed_at: reviewed ? reviewedAt : null,
    reviewed_evidence_fingerprint: reviewed ? domain.evidenceFingerprint : "",
    is_active: domain.active ? 1 : 0,
    first_seen_at: domain.firstSeenAt,
    last_seen_at: domain.lastSeenAt,
    resolved_at: reviewedAt,
  };
}

function clickHouseTimestamp(value?: string): string {
  const date = value === undefined ? new Date() : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new CompanyDomainReviewValidationError(
      "The company-domain review timestamp is invalid.",
    );
  }
  return date.toISOString().replace("T", " ").replace("Z", "");
}

export async function recordCompanyDomainReview(
  input: RecordCompanyDomainReviewInput,
): Promise<void> {
  if (!COMPANY_DOMAIN_REVIEW_STATUSES.includes(input.reviewStatus)) {
    throw new CompanyDomainReviewValidationError(
      "Unknown company-domain review status.",
    );
  }
  const rootDomain = normalizeRootDomain(input.rootDomain);
  const domain = input.domains.find(
    (candidate) => candidate.rootDomain === rootDomain,
  );
  if (!domain) {
    throw new CompanyDomainReviewValidationError(
      "This domain is no longer associated with the company.",
    );
  }
  const note = input.note?.trim() ?? "";
  const reviewedBy = input.reviewedBy?.trim() ?? "";
  if (note.length > 2_000) {
    throw new CompanyDomainReviewValidationError(
      "The review note is too long.",
    );
  }
  if (reviewedBy.length > 255) {
    throw new CompanyDomainReviewValidationError(
      "The reviewer identifier is too long.",
    );
  }
  const reviewedAt = clickHouseTimestamp(input.reviewedAt);
  const rows = [
    reviewedRow(domain, input.reviewStatus, note, reviewedBy, reviewedAt),
  ];
  if (input.reviewStatus === "confirmed_primary") {
    for (const sibling of input.domains) {
      if (
        sibling.rootDomain !== rootDomain &&
        sibling.reviewStatus === "confirmed_primary"
      ) {
        rows.push(
          reviewedRow(
            sibling,
            "confirmed_related",
            sibling.reviewNote,
            reviewedBy,
            reviewedAt,
          ),
        );
      }
    }
  }
  await chInsertCompanyDomains(rows);
}
