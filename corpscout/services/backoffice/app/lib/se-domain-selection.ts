import type { SeDomainsFilters } from "~/lib/se-domains-filters";

import type { DomainSelection } from "~/lib/domain-selection";

export type SeDomainSelection = DomainSelection<SeDomainsFilters>;

export const DOMAIN_CRAWL_TYPES = [
  { value: "full", label: "Full crawl" },
  { value: "jobs", label: "Jobs" },
  { value: "site_info", label: "Basic info" },
] as const;
export type DomainCrawlType = typeof DOMAIN_CRAWL_TYPES[number]["value"];
