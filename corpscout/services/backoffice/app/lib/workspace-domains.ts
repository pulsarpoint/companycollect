import { normalizeDomainSuffix } from "~/lib/domain-suffix";
import type { DomainSelection } from "~/lib/domain-selection";

export const DOMAIN_SOURCES = ["commoncrawl", "commoncrawl_graph", "se_company_domain"] as const;
export const DOMAIN_SOURCE_LABELS: Record<string, string> = {
  commoncrawl: "Common Crawl pages", commoncrawl_graph: "Common Crawl graph", se_company_domain: "Swedish companies",
};
type Presence = "any" | "with" | "without";
export interface WorkspaceDomainFilters {
  prefix: string;
  suffix: string;
  sources: string[];
  sourceMatch: "any" | "all";
  dns: Presence;
  websites: Presence | "observed";
  companies: Presence;
  companyMatching: Presence;
}
export type WorkspaceDomainSelection = DomainSelection<WorkspaceDomainFilters>;

export function parseWorkspaceDomainFilters(params: URLSearchParams): WorkspaceDomainFilters {
  const presence = (value: string | null): Presence => value === "with" || value === "without" ? value : "any";
  return {
    prefix: (params.get("prefix") ?? "").trim().toLowerCase().slice(0, 253),
    suffix: normalizeDomainSuffix(params.get("suffix")),
    sources: [...new Set(params.getAll("source"))].filter((value) => DOMAIN_SOURCES.some((source) => source === value)).sort(),
    sourceMatch: params.get("sourceMatch") === "all" ? "all" : "any",
    dns: presence(params.get("dns")),
    websites: params.get("websites") === "observed" ? "observed" : presence(params.get("websites")),
    companies: presence(params.get("companies")),
    companyMatching: presence(params.get("companyMatching")),
  };
}

export function workspaceDomainsHref(filters: WorkspaceDomainFilters, after = "") {
  const params = new URLSearchParams();
  if (filters.prefix) params.set("prefix", filters.prefix);
  if (filters.suffix) params.set("suffix", filters.suffix);
  for (const source of filters.sources) params.append("source", source);
  if (filters.sources.length && filters.sourceMatch === "all") params.set("sourceMatch", "all");
  for (const field of ["dns", "websites", "companies", "companyMatching"] as const) {
    if (filters[field] !== "any") params.set(field, filters[field]);
  }
  if (after) params.set("after", after);
  return `/admin/domains${params.size ? `?${params}` : ""}`;
}
export function workspaceDomainHref(domain: string) {
  return `/admin/domains/${encodeURIComponent(domain)}`;
}
export function workspaceWebtechHref(domain: string, site?: string) {
  return `${workspaceDomainHref(domain)}/web-technologies${site ? `?site=${encodeURIComponent(site)}` : ""}`;
}
