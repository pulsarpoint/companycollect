import { chInsertCompanyDomains, chQuery } from "~/lib/clickhouse.server";

export const COMPANY_DOMAIN_REVIEW_STATUSES = [
  "unreviewed",
  "confirmed_primary",
  "confirmed_related",
  "rejected",
] as const;

export type CompanyDomainReviewStatus =
  (typeof COMPANY_DOMAIN_REVIEW_STATUSES)[number];

export interface CompanyDomainSource {
  name: string;
  confidence: number;
  sourceRecordId: string;
  sourceUrl: string;
  confidenceBasis: string;
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

function domainFromRow(row: CompanyDomainRow): CompanyDomain {
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
    sources: row.source_names.map((name, index) => ({
      name,
      confidence: Number(row.source_confidences[index]),
      sourceRecordId: row.source_record_ids[index] ?? "",
      sourceUrl: row.source_urls[index] ?? "",
      confidenceBasis: row.confidence_bases[index] ?? "",
    })),
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
  const rows = await chQuery<CompanyDomainRow>(COMPANY_DOMAINS_QUERY, {
    country: "SE",
    companyId: companyId.trim(),
  });
  return rows.map(domainFromRow);
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
