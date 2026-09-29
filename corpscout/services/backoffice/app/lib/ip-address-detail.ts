// Client-safe helpers for the IP address detail page. Server reads live in
// ip-address-detail.server.ts; components import only types from there.

export const IP_DETAIL_PAGE_SIZE = 50;
/**
 * Hostnames the DNS history link passes at most; a root domain with more is read by
 * `root_domain =` alone.
 */
export const DNS_HISTORY_MAX_HOSTNAMES = 200;

export type IpDomainScope = "exact" | "segment";

export const IP_DETAIL_TABS = [
  { value: "overview", label: "Overview" },
  { value: "registration", label: "Registration" },
  { value: "dns", label: "DNS records" },
  { value: "domains", label: "Domains" },
] as const;

export type IpDetailTab = (typeof IP_DETAIL_TABS)[number]["value"];

export function ipAddressDetailHref(ip: string, tab?: IpDetailTab): string {
  return `/admin/ip-addresses/${encodeURIComponent(ip)}${tab ? `/${tab}` : ""}`;
}

export function ipDetailTabFromPath(pathname: string): IpDetailTab {
  const suffix = pathname.split("/")[4] ?? "";
  return IP_DETAIL_TABS.some((tab) => tab.value === suffix)
    ? (suffix as IpDetailTab)
    : "overview";
}

export function ipEnrichmentTaskHref(taskId: string): string {
  return `/admin/queues/ip-enrichment?task=${encodeURIComponent(taskId)}`;
}

/** The `corpscout.source` marker the enrichers write into raw_response. */
export function rdapSourceLabel(source: string): string {
  if (source === "ripe-rest") return "RIPE REST";
  if (source === "apnic-whois") return "APNIC whois";
  return "RDAP";
}

/** Keyset cursor from `?after=`; bounded so a pasted URL cannot grow the query. */
export function parseAfter(value: string | null): string {
  return (value ?? "").slice(0, 600);
}

export function parseDomainScope(value: string | null): IpDomainScope {
  return value === "segment" ? "segment" : "exact";
}

/** "12" when the page holds everything, "50+" when a next page exists. */
export function formatPageTotal(
  total: number | null,
  shown: number,
): string {
  return total === null ? `${shown.toLocaleString("en-US")}+` : total.toLocaleString("en-US");
}

export function ipDnsHistoryHref(ip: string, rootDomain: string, hostnames: string[]): string {
  const params = new URLSearchParams();
  for (const hostname of hostnames) params.append("h", hostname);
  const query = params.toString();
  return `/admin/ip-addresses/${encodeURIComponent(ip)}/dns/${encodeURIComponent(rootDomain)}${query ? `?${query}` : ""}`;
}
