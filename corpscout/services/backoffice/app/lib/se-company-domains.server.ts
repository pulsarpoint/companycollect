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
  association?: string;
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
  entity.inactive_reason AS inactive_reason,
  multiIf(d.is_active = 1, 'connected', d.review_status = 'rejected', 'not_connected', entity.association) AS association
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

export interface DomainRelationshipQuote {
  evidence_id: string;
  quote: string;
}

export interface DomainRelationshipStatement {
  related_entity_name: string;
  description: string;
  relationship_supported: boolean;
  evidence: DomainRelationshipQuote[];
}

export interface DomainRelationshipEvidence {
  id: string;
  url: string;
  report_member?: string;
  source_url?: string;
  page_title?: string;
  fetched_at?: string;
  locations: { xpath?: string; page_id: string; source_line?: number | null; link_id?: string; dom_path?: string[] }[];
  source_context: {
    text: string;
    local_text: string;
    headings: string[];
    table_headers: string[];
    attributes?: string[];
    truncated: boolean;
    reading_order: string;
  };
}

export interface SeDomainRelationship {
  source_kind: "esef" | "website";
  source_document_id: string;
  registrable_domain: string;
  period_end: string;
  captured_at: string;
  source_url: string;
  statements: DomainRelationshipStatement[];
  evidence: DomainRelationshipEvidence[];
}

interface StoredDomainRelationship extends Omit<SeDomainRelationship, "statements" | "evidence"> {
  statements_json: string;
  input_json: string;
}

export async function loadSeCompanyDomainRelationships(companyId: string): Promise<SeDomainRelationship[]> {
  const rows = await chQuery<StoredDomainRelationship>(`SELECT * FROM (SELECT
    'esef' AS source_kind, source_document_id, registrable_domain, toString(period_end) AS period_end,
    '' AS captured_at,
    source_url, statements_json, input_json
    FROM corpscout.se_company_domain_relationships
    WHERE company_id = {companyId:String}
    UNION ALL
    SELECT 'website' AS source_kind, concat(result_id, ':', source_host) AS source_document_id,
    registrable_domain, '' AS period_end, toString(captured_at) AS captured_at,
    source_url, statements_json, input_json
    FROM corpscout.website_domain_relationships_current
    WHERE country_code = 'SE' AND company_id = {companyId:String})
    ORDER BY source_kind DESC, captured_at DESC, period_end DESC, registrable_domain, source_document_id
    LIMIT 500`, { companyId });
  return rows.map(({ statements_json, input_json, ...row }) => ({
    ...row,
    statements: JSON.parse(statements_json) as DomainRelationshipStatement[],
    evidence: (JSON.parse(input_json) as { evidence: DomainRelationshipEvidence[] }).evidence,
  }));
}
