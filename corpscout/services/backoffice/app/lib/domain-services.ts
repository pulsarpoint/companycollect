// Domain services: providers a domain uses over time, as resolved from its
// DNS records by dns-detect (dns_record_services and the tables derived from
// it, migrations 000468/000470). Shared by the server queries and the pages.

export type ServiceInterval = {
  serviceType: string;
  providerKey: string;
  providerSlug: string;
  serviceKeys: string[];
  firstSeen: string;
  lastSeen: string;
  isCurrent: boolean;
  evidence: number;
  recordTypes: string[];
  confidence: number;
};

export type ServiceEvidence = {
  serviceType: string;
  providerKey: string;
  recordName: string;
  recordType: string;
  subject: string;
  ruleId: string;
  validFrom: string;
  validTo: string;
};

export type DomainServices = {
  domain: string;
  resolved: boolean;
  intervals: ServiceInterval[];
  evidence: ServiceEvidence[];
};

export type TypeCounts = Record<string, { now: number; ever: number }>;

export type ProviderSummary = {
  slug: string;
  name: string;
  category: string;
  domainsNow: number;
  domainsEver: number;
  byType: TypeCounts;
};

export type UnmappedKey = { providerKey: string; domainsNow: number; domainsEver: number };

export type ProviderDomainRow = {
  domain: string;
  serviceTypes: string[];
  serviceKeys: string[];
  firstSeen: string;
  lastSeen: string;
  isCurrent: boolean;
};

export type ProviderDomainsFilter = { serviceType: string; service: string; now: boolean; page: number };

export const PROVIDER_DOMAINS_PAGE_SIZE = 50;

export type ProviderDomainsPage = {
  rows: ProviderDomainRow[];
  total: number;
  page: number;
  pageSize: typeof PROVIDER_DOMAINS_PAGE_SIZE;
  byType: TypeCounts;
  byService: { service: string; now: number; ever: number }[];
};

/** Display labels, in the order the Services tab lists service types. */
export const SERVICE_TYPE_LABELS: Record<string, string> = {
  dns: "DNS",
  email: "Mail",
  email_security: "Mail filtering",
  email_sending: "Outbound mail",
  hosting: "Hosting",
  cdn: "CDN",
  paas: "PaaS",
  iaas: "IaaS",
  waf: "WAF",
  ddos_protection: "DDoS protection",
  dmarc_reporting: "DMARC reporting",
  saas_verification: "Verification",
};

export function serviceTypeLabel(serviceType: string): string {
  return SERVICE_TYPE_LABELS[serviceType] ?? serviceType;
}

function typeOrder(serviceType: string): number {
  const i = Object.keys(SERVICE_TYPE_LABELS).indexOf(serviceType);
  return i === -1 ? Number.MAX_SAFE_INTEGER : i;
}

/** Current intervals grouped by service type, in label order. */
export function groupCurrent(intervals: ServiceInterval[]) {
  const groups = new Map<string, ServiceInterval[]>();
  for (const interval of intervals) {
    if (!interval.isCurrent) continue;
    groups.set(interval.serviceType, [...(groups.get(interval.serviceType) ?? []), interval]);
  }
  return [...groups.entries()]
    .sort(([a], [b]) => typeOrder(a) - typeOrder(b) || a.localeCompare(b))
    .map(([serviceType, list]) => ({ serviceType, label: serviceTypeLabel(serviceType), intervals: list }));
}

const SERVICE_RE = /^[a-z0-9.-]{1,120}$/;

/** Provider Domains tab filters from the URL; anything unexpected falls back to a default. */
export function parseProviderDomainsFilter(search: URLSearchParams): ProviderDomainsFilter {
  const type = search.get("type") ?? "";
  const service = search.get("service") ?? "";
  const page = Number.parseInt(search.get("page") ?? "1", 10);
  return {
    serviceType: type in SERVICE_TYPE_LABELS ? type : "",
    service: SERVICE_RE.test(service) ? service : "",
    now: search.get("now") !== "0",
    page: Number.isFinite(page) && page >= 1 ? page : 1,
  };
}
