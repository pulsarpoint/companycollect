import type { SeDomainsFilters } from "~/lib/se-domains-filters";

export type SeDomainSelection =
  | { mode: "ids"; domains: string[] }
  | { mode: "query"; query: SeDomainsFilters; excludedDomains: string[] };

export const NO_DOMAINS_SELECTED: SeDomainSelection = { mode: "ids", domains: [] };

export function selectionForSeDomainFilters(selection: SeDomainSelection, filters: SeDomainsFilters): SeDomainSelection {
  return selection.mode === "query" && JSON.stringify(selection.query) !== JSON.stringify(filters)
    ? NO_DOMAINS_SELECTED : selection;
}

export function isSeDomainSelected(selection: SeDomainSelection, domain: string): boolean {
  return selection.mode === "ids" ? selection.domains.includes(domain) : !selection.excludedDomains.includes(domain);
}

/** A shared domain is one selection even when several company rows show it. */
export function selectSeDomains(selection: SeDomainSelection, domains: readonly string[], checked: boolean): SeDomainSelection {
  const values = new Set(selection.mode === "ids" ? selection.domains : selection.excludedDomains);
  for (const domain of domains) {
    if (checked === (selection.mode === "ids")) values.add(domain);
    else values.delete(domain);
  }
  return selection.mode === "ids"
    ? { mode: "ids", domains: [...values] }
    : { ...selection, excludedDomains: [...values] };
}

export const DOMAIN_CRAWL_TYPES = [
  { value: "full", label: "Full crawl" },
  { value: "jobs", label: "Jobs" },
  { value: "site_info", label: "Basic info" },
] as const;
export type DomainCrawlType = typeof DOMAIN_CRAWL_TYPES[number]["value"];
