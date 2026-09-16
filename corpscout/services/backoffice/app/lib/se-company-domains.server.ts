import { chQuery } from "~/lib/clickhouse.server";

/**
 * One company/domain association as the `se_company_domain` entity
 * holds it. Each `source_names[i]` lines up with `source_confidences[i]`,
 * `source_urls[i]` and `confidence_bases[i]` -- the arrays are parallel, so
 * the page renders them zipped rather than as four separate lists.
 */
export interface SeCompanyDomainRow {
  root_domain: string;
  website_url: string;
  website_host: string;
  source_names: string[];
  supporting_sources: string[];
  source_confidences: number[];
  source_urls: string[];
  confidence_bases: string[];
  suggested_confidence: number;
  suggested_primary: number;
  review_status: string;
  review_note: string;
  reviewed_by: string;
  reviewed_at: string;
  is_active: number;
  first_seen_at: string;
  last_seen_at: string;
  resolved_at: string;
  verification_status?: string;
  verification_reason?: string;
  inactive_reason?: string;
}

/**
 * se_company_domain is keyed on (company_id, root_domain) in Sweden and is a
 * ReplacingMergeTree, so FINAL: a re-reviewed domain must show once, in its
 * newest state. 'SE' is a literal because this is the Sweden admin area, not
 * a value a request supplies -- the company id, which is, stays a named
 * parameter.
 *
 * Rejected and inactive rows are kept rather than filtered: this is the admin
 * view of what the pipeline holds, and "we rejected this domain" is exactly
 * what a reviewer opening the tab needs to see.
 */
export const COMPANY_DOMAINS_SQL = `SELECT
  d.root_domain AS root_domain,
  d.website_url AS website_url,
  d.website_host AS website_host,
  d.source_names AS source_names,
  d.supporting_sources AS supporting_sources,
  arrayMap(value -> toFloat64(value), d.source_confidences) AS source_confidences,
  d.source_urls AS source_urls,
  d.confidence_bases AS confidence_bases,
  toFloat64(d.suggested_confidence) AS suggested_confidence,
  toUInt8(d.suggested_primary) AS suggested_primary,
  toString(d.review_status) AS review_status,
  d.review_note AS review_note,
  d.reviewed_by AS reviewed_by,
  ifNull(toString(d.reviewed_at), '') AS reviewed_at,
  toUInt8(d.is_active) AS is_active,
  toString(d.first_seen_at) AS first_seen_at,
  toString(d.last_seen_at) AS last_seen_at,
  toString(d.resolved_at) AS resolved_at,
  toString(entity.verification_status) AS verification_status,
  entity.verification_reason AS verification_reason,
  entity.inactive_reason AS inactive_reason
FROM corpscout.company_domains_resolved AS d
LEFT JOIN corpscout.se_company_domain AS entity FINAL
  ON entity.company_id = d.company_id AND entity.root_domain = d.root_domain
WHERE d.country_code = 'SE' AND d.company_id = {companyId:String}
ORDER BY d.is_active DESC, d.suggested_primary DESC, length(d.supporting_sources) DESC, d.suggested_confidence DESC, d.root_domain
LIMIT 100`;

/** Every domain associated with one Swedish company, primary first. */
export async function loadSeCompanyDomains(
  companyId: string,
): Promise<SeCompanyDomainRow[]> {
  return chQuery<SeCompanyDomainRow>(COMPANY_DOMAINS_SQL, { companyId });
}
