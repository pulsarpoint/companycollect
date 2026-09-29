// Client-safe helpers for the IP address detail page. Server reads live in
// ip-address-detail.server.ts; components import only types from there.

export const IP_DETAIL_PAGE_SIZE = 50;
/**
 * Root domains per DNS-records page. Each one is a separate commoncrawl_domain_dns_records
 * read (1.4-9 s each on production, 2026-09-29), so the page stays small.
 */
export const IP_DNS_PAGE_SIZE = 10;
/** The DNS and domain counts stop here and render as "10,000+". */
export const IP_DETAIL_COUNT_CAP = 10_000;

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

export function parsePage(value: string | null): number {
  const page = Number(value ?? "1");
  return Number.isSafeInteger(page) && page > 0 ? page : 1;
}

export function parseDomainScope(value: string | null): IpDomainScope {
  return value === "segment" ? "segment" : "exact";
}

export function formatCappedCount(count: number, capped: boolean): string {
  return `${count.toLocaleString("en-US")}${capped ? "+" : ""}`;
}
