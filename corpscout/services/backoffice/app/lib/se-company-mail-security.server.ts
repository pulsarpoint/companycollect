import { chQuery } from "~/lib/clickhouse.server";
import {
  evaluateMailSecurity,
  type MailDnsRecord,
  type MailSecurityReport,
} from "~/lib/mail-security";

// Live mail-security scoring: ONE keyed query against the deduplicated
// current-state DNS table (~1.4B rows keyed by root_domain), then the pure
// evaluator runs in-process. No precompute (owner decision): the query is a
// primary-key point lookup and the evaluation is milliseconds.
//
// Row scoping: mail analysis needs MX/TXT/SPF rows anywhere under the
// domain (DKIM selectors live on subnames), but DNSSEC presence
// only needs apex RRSIG/DNSKEY/DS -- unrestricted RRSIG rows would flood the
// result (hundreds of signatures per signed zone) and crowd out the mail
// rows within the LIMIT. CNAME rows are fetched only for `._domainkey.`
// names: providers commonly delegate DKIM selectors via CNAME.
const mailDnsRecordsSql = `SELECT
  name,
  any(raw_record_type) AS record_type,
  any(raw_value) AS value,
  toString(min(raw_first_seen)) AS first_seen,
  toString(max(raw_last_seen)) AS last_seen
FROM (
  SELECT
    record_id,
    name,
    toString(record_type) AS raw_record_type,
    toString(value) AS raw_value,
    first_seen AS raw_first_seen,
    last_seen AS raw_last_seen
  FROM commoncrawl_domain_dns_records_current
  WHERE root_domain = {domain:String}
    AND (
      record_type IN ('MX', 'TXT', 'SPF')
      OR (record_type IN ('RRSIG', 'DNSKEY', 'DS') AND name = {domain:String})
      OR (record_type = 'CNAME' AND position(name, '._domainkey.') > 0)
    )
)
GROUP BY record_id, name
ORDER BY record_type, name, value
LIMIT 2000`;

type MailDnsRecordRow = MailDnsRecord & {
  first_seen: string;
  last_seen: string;
};

export type SeCompanyMailSecurity = {
  domain: string;
  recordCount: number;
  report: MailSecurityReport;
};

/**
 * Fetch the mail-relevant crawled DNS records for one root domain and score
 * them. Returns `recordCount: 0` (with an all-fail/unknown report) when the
 * crawl holds nothing for the domain -- the route renders an empty state
 * instead of a score in that case.
 */
export async function getDomainMailSecurity(
  domain: string,
): Promise<SeCompanyMailSecurity> {
  const normalized = domain.trim().toLowerCase().replace(/\.$/, "");
  const rows = await chQuery<MailDnsRecordRow>(mailDnsRecordsSql, {
    domain: normalized,
  });
  return {
    domain: normalized,
    recordCount: rows.length,
    report: evaluateMailSecurity(rows, normalized),
  };
}
