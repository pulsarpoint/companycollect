/**
 * The URL-facing filter state of the `/admin/se/companies/domains` list: the
 * filters the filter sheet edits and the two href builders the list rows
 * and the domain detail page link through. Client-safe (no ClickHouse import),
 * exactly like `se-people-filters.ts`, so the route, the table component and
 * the loader all share one definition.
 *
 * Every helper is pure and derives its href from the state it is handed --
 * never from the live location -- so a link is identical on the server render
 * and in a test.
 */
import type { GraphDirection } from "~/lib/domain-graph";
import { DEFAULT_PAGE_SIZE } from "~/lib/paging";

const MAX_FIELD_LENGTH = 253;
const ALL_DIGITS = /^[0-9]+$/;
const CONFIDENCE = /^(0(\.[0-9]+)?|1(\.0+)?)$/;

/** The three values `se_company_domain.association` may hold (table CONSTRAINT). */
export const DOMAIN_ASSOCIATIONS = ["connected", "uncertain", "not_connected"] as const;
export type DomainAssociation = (typeof DOMAIN_ASSOCIATIONS)[number];

/** The `status` filter's two values: `active = 1` and `active = 0`. */
export const DOMAIN_STATUSES = ["active", "inactive"] as const;
export type DomainStatus = (typeof DOMAIN_STATUSES)[number];

export const DOMAIN_SOURCE_VALUES = ["brave", "wikidata", "esef_filing", "common_crawl_identity"] as const;

export interface SeDomainsFilters {
  /** A root-domain fragment, lower-cased (domains are stored lower-case). */
  domain: string;
  /** A company id (digits only). */
  company: string;
  source: string;
  association: string;
  status: string;
  /** Inclusive bounds on `confidence`, as the URL spells them ("0.7"). */
  minConfidence: string;
  maxConfidence: string;
  /** "1" restricts the list to domains claimed by more than one company. */
  shared: string;
}

export const EMPTY_SE_DOMAINS_FILTERS: SeDomainsFilters = {
  domain: "",
  company: "",
  source: "",
  association: "",
  status: "",
  minConfidence: "",
  maxConfidence: "",
  shared: "",
};

export function isDomainAssociation(value: string): value is DomainAssociation {
  return (DOMAIN_ASSOCIATIONS as readonly string[]).includes(value);
}

export function isDomainStatus(value: string): value is DomainStatus {
  return (DOMAIN_STATUSES as readonly string[]).includes(value);
}

function confidence(value: string | null): string {
  const trimmed = (value ?? "").trim();
  return CONFIDENCE.test(trimmed) ? trimmed : "";
}

/**
 * Reads the filters off the URL, dropping anything the catalogue does
 * not know: `association` and `status` only on their fixed values, `company`
 * only when all digits, `minConfidence`/`maxConfidence` only as a decimal in
 * 0..1, `shared` only as "1". `domain` is free text, trimmed, lower-cased and
 * capped at the DNS name limit.
 */
export function parseSeDomainsFilters(params: URLSearchParams): SeDomainsFilters {
  const association = (params.get("association") ?? "").trim();
  const status = (params.get("status") ?? "").trim();
  const company = (params.get("company") ?? "").trim();
  const source = (params.get("source") ?? "").trim();
  return {
    domain: (params.get("domain") ?? "").trim().toLowerCase().slice(0, MAX_FIELD_LENGTH),
    company: ALL_DIGITS.test(company) ? company : "",
    source: (DOMAIN_SOURCE_VALUES as readonly string[]).includes(source) ? source : "",
    association: isDomainAssociation(association) ? association : "",
    status: isDomainStatus(status) ? status : "",
    minConfidence: confidence(params.get("minConfidence")),
    maxConfidence: confidence(params.get("maxConfidence")),
    shared: params.get("shared") === "1" ? "1" : "",
  };
}

const LIST_PATH = "/admin/se/companies/domains";

/**
 * The list's own URL for a given filter state and page: only the non-empty
 * filters, `page` when it is not 1, and `pageSize` when it is not the shared
 * default -- so the unfiltered first page of the default size is the bare list
 * path.
 */
export function seDomainsHref(
  filters: SeDomainsFilters,
  page: number,
  pageSize: number,
): string {
  const params = new URLSearchParams();
  for (const key of Object.keys(EMPTY_SE_DOMAINS_FILTERS) as (keyof SeDomainsFilters)[]) {
    if (filters[key] !== "") params.set(key, filters[key]);
  }
  if (page !== 1) params.set("page", String(page));
  if (pageSize !== DEFAULT_PAGE_SIZE) params.set("pageSize", String(pageSize));
  const qs = params.toString();
  return qs === "" ? LIST_PATH : `${LIST_PATH}?${qs}`;
}

/**
 * A domain's row links to its detail page: the companies behind it, then its
 * connections in the Common Crawl graph. The connections' direction tab and
 * page size ride on the URL when they are not the defaults ("all", the shared
 * default size); the page number never does -- a direction change starts at 1.
 */
export function seDomainHref(
  domain: string,
  connections: { direction?: GraphDirection; pageSize?: number } = {},
): string {
  const params = new URLSearchParams();
  if (connections.direction !== undefined && connections.direction !== "all") {
    params.set("direction", connections.direction);
  }
  if (connections.pageSize !== undefined && connections.pageSize !== DEFAULT_PAGE_SIZE) {
    params.set("pageSize", String(connections.pageSize));
  }
  const qs = params.toString();
  const path = `${LIST_PATH}/${encodeURIComponent(domain)}`;
  return qs === "" ? path : `${path}?${qs}`;
}
