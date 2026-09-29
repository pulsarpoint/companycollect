import { chQuery } from "~/lib/clickhouse.server";
import {
  PROVIDER_DOMAINS_PAGE_SIZE,
  type DomainServices,
  type ProviderDomainsFilter,
  type ProviderDomainsPage,
  type ProviderSummary,
  type TypeCounts,
  type UnmappedKey,
} from "~/lib/domain-services";

type HistoryRow = {
  service_type: string;
  provider_key: string;
  provider_slug: string;
  service_keys: string[];
  first_seen: string;
  last_seen: string;
  evidence: string | number;
  record_types: string[];
  confidence: number;
};

type EvidenceRow = {
  service_type: string;
  provider_key: string;
  record_name: string;
  record_type: string;
  subject: string;
  rule_id: string;
  valid_from: string;
  valid_to: string;
};

/** A domain's service periods (history), which of them are current, and the records behind them. */
export async function getDomainServices(domain: string): Promise<DomainServices> {
  const [{ resolutions = 0 } = {}] = await chQuery<{ resolutions: string | number }>(
    "SELECT count() AS resolutions FROM corpscout.dns_record_resolutions WHERE root_domain = {domain:String}",
    { domain },
  );
  if (Number(resolutions) === 0) return { domain, resolved: false, intervals: [], evidence: [] };

  const [history, now, evidence] = await Promise.all([
    chQuery<HistoryRow>(
      `SELECT service_type, provider_key, provider_slug, service_keys, toString(first_seen) AS first_seen,
              toString(last_seen) AS last_seen, evidence, record_types, confidence
       FROM corpscout.domain_services_history(domain = {domain:String})
       ORDER BY service_type, first_seen`,
      { domain },
    ),
    chQuery<{ service_type: string; provider_key: string; first_seen: string }>(
      `SELECT service_type, provider_key, toString(first_seen) AS first_seen
       FROM corpscout.domain_services_now(domain = {domain:String})`,
      { domain },
    ),
    // The domain filter sits inside the latest-resolution subquery:
    // dns_record_services_current has none of its own.
    chQuery<EvidenceRow>(
      `SELECT s.service_type AS service_type, s.provider_key AS provider_key, s.record_name AS record_name,
              s.record_type AS record_type, s.subject AS subject, s.rule_id AS rule_id,
              toString(s.valid_from) AS valid_from, toString(s.valid_to) AS valid_to
       FROM corpscout.dns_record_services AS s
       INNER JOIN (SELECT root_domain, record_id, max(resolved_at) AS resolved_at FROM corpscout.dns_record_resolutions
                   WHERE root_domain = {domain:String} GROUP BY root_domain, record_id) AS l
         USING (root_domain, record_id, resolved_at)
       WHERE s.root_domain = {domain:String}
       ORDER BY s.service_type, s.provider_key, s.valid_from
       LIMIT 2000`,
      { domain },
    ),
  ]);
  const current = new Set(now.map((r) => `${r.service_type}\u0000${r.provider_key}\u0000${r.first_seen}`));
  return {
    domain,
    resolved: true,
    intervals: history.map((r) => ({
      serviceType: r.service_type,
      providerKey: r.provider_key,
      providerSlug: r.provider_slug,
      serviceKeys: r.service_keys,
      firstSeen: r.first_seen,
      lastSeen: r.last_seen,
      isCurrent: current.has(`${r.service_type}\u0000${r.provider_key}\u0000${r.first_seen}`),
      evidence: Number(r.evidence),
      recordTypes: r.record_types,
      confidence: Number(r.confidence),
    })),
    evidence: evidence.map((r) => ({
      serviceType: r.service_type,
      providerKey: r.provider_key,
      recordName: r.record_name,
      recordType: r.record_type,
      subject: r.subject,
      ruleId: r.rule_id,
      validFrom: r.valid_from,
      validTo: r.valid_to,
    })),
  };
}

/** Every mapped provider with distinct domains now/ever, in total and per service type. */
export async function getProviderSummaries(): Promise<ProviderSummary[]> {
  const rows = await chQuery<{
    provider_slug: string;
    name: string | null;
    category: string | null;
    service_type: string;
    domains_now: string | number;
    domains_ever: string | number;
  }>(
    `SELECT c.provider_slug AS provider_slug, any(p.provider_name) AS name, any(p.provider_category) AS category,
            c.service_type AS service_type, sum(c.domains_now) AS domains_now, sum(c.domains_ever) AS domains_ever
     FROM corpscout.provider_service_counts AS c
     LEFT JOIN (SELECT provider_slug, any(provider_name) AS provider_name, any(provider_category) AS provider_category
                FROM corpscout.provider_services FINAL GROUP BY provider_slug) AS p USING (provider_slug)
     WHERE c.provider_slug != ''
     GROUP BY c.provider_slug, c.service_type`,
  );
  const bySlug = new Map<string, ProviderSummary>();
  for (const r of rows) {
    const p = bySlug.get(r.provider_slug) ?? {
      slug: r.provider_slug,
      name: r.name || r.provider_slug,
      category: r.category ?? "",
      domainsNow: 0,
      domainsEver: 0,
      byType: {},
    };
    if (r.service_type === "") {
      p.domainsNow = Number(r.domains_now);
      p.domainsEver = Number(r.domains_ever);
    } else {
      p.byType[r.service_type] = { now: Number(r.domains_now), ever: Number(r.domains_ever) };
    }
    bySlug.set(r.provider_slug, p);
  }
  return [...bySlug.values()];
}

/** Provider domains the resolver saw but no provider definition names yet. */
export async function getUnmappedKeys(limit = 100): Promise<UnmappedKey[]> {
  const rows = await chQuery<{ provider_key: string; domains_now: string | number; domains_ever: string | number }>(
    `SELECT provider_key, sum(domains_now) AS domains_now, sum(domains_ever) AS domains_ever
     FROM corpscout.provider_service_counts
     WHERE provider_slug = '' AND service_type = ''
     GROUP BY provider_key
     ORDER BY domains_now DESC, provider_key
     LIMIT {limit:UInt32}`,
    { limit },
  );
  return rows.map((r) => ({ providerKey: r.provider_key, domainsNow: Number(r.domains_now), domainsEver: Number(r.domains_ever) }));
}

const PROVIDER_FILTER = `provider_slug = {slug:String}
  AND ({type:String} = '' OR service_type = {type:String})
  AND ({service:String} = '' OR has(service_keys, {service:String}))
  AND ({now:UInt8} = 0 OR is_current = 1)`;

/** One page of a provider's domains, with its per-type and per-service counts. */
export async function getProviderDomains(slug: string, filter: ProviderDomainsFilter): Promise<ProviderDomainsPage> {
  const params = { slug, type: filter.serviceType, service: filter.service, now: filter.now ? 1 : 0 };
  const [{ total = 0 } = {}] = await chQuery<{ total: string | number }>(
    `SELECT uniqExact(root_domain) AS total FROM corpscout.domain_service_intervals WHERE ${PROVIDER_FILTER}`,
    params,
  );
  const pageSize = PROVIDER_DOMAINS_PAGE_SIZE;
  const lastPage = Math.max(1, Math.ceil(Number(total) / pageSize));
  const page = Math.min(Math.max(1, filter.page), lastPage);
  const [rows, types, services] = await Promise.all([
    chQuery<{
      root_domain: string;
      service_types: string[];
      service_keys: string[];
      first_seen: string;
      last_seen: string;
      is_current: number;
    }>(
      `SELECT root_domain, arraySort(groupUniqArray(service_type)) AS service_types,
              arraySort(arrayDistinct(arrayFlatten(groupArray(service_keys)))) AS service_keys,
              toString(min(first_seen)) AS first_seen, toString(max(last_seen)) AS last_seen, max(is_current) AS is_current
       FROM corpscout.domain_service_intervals
       WHERE ${PROVIDER_FILTER}
       GROUP BY root_domain
       ORDER BY root_domain
       LIMIT {limit:UInt32} OFFSET {offset:UInt32}`,
      { ...params, limit: pageSize, offset: (page - 1) * pageSize },
    ),
    chQuery<{ service_type: string; now: string | number; ever: string | number }>(
      `SELECT service_type, uniqExactIf(root_domain, is_current = 1) AS now, uniqExact(root_domain) AS ever
       FROM corpscout.domain_service_intervals
       WHERE provider_slug = {slug:String}
       GROUP BY service_type`,
      { slug },
    ),
    chQuery<{ service: string; now: string | number; ever: string | number }>(
      `SELECT service, uniqExactIf(root_domain, is_current = 1) AS now, uniqExact(root_domain) AS ever
       FROM corpscout.domain_service_intervals
       ARRAY JOIN service_keys AS service
       WHERE provider_slug = {slug:String}
       GROUP BY service
       ORDER BY ever DESC`,
      { slug },
    ),
  ]);
  const byType: TypeCounts = {};
  for (const t of types) byType[t.service_type] = { now: Number(t.now), ever: Number(t.ever) };
  return {
    rows: rows.map((r) => ({
      domain: r.root_domain,
      serviceTypes: r.service_types,
      serviceKeys: r.service_keys,
      firstSeen: r.first_seen,
      lastSeen: r.last_seen,
      isCurrent: Number(r.is_current) === 1,
    })),
    total: Number(total),
    page,
    pageSize,
    byType,
    byService: services.map((s) => ({ service: s.service, now: Number(s.now), ever: Number(s.ever) })),
  };
}
