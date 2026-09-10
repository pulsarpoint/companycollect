/**
 * The URL-facing filter state of the `/admin/se/people` list (person spec section 7):
 * the six filters the filter bar edits and the two href builders the page and the
 * People tab's own rows link through. Client-safe (no ClickHouse import) so the route
 * component, the table component and the loader can all share one definition.
 *
 * Every helper is pure and derives its href from the state it is handed -- never from
 * the live location -- so a link is identical on the server render and in a test.
 */
import { DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { isPersonSource, isPersonStatus } from "~/lib/se-person-fields";

const MAX_FIELD_LENGTH = 100;
const YEAR_PATTERN = /^[0-9]{4}$/;

export interface SePeopleFilters {
  /** A company id (digits) or a legal-name prefix -- resolved to ids once, by the
   * loader, through `resolveSePeopleCompanyIds`. */
  company: string;
  name: string;
  source: string;
  role: string;
  year: string;
  status: string;
}

export const EMPTY_SE_PEOPLE_FILTERS: SePeopleFilters = {
  company: "", name: "", source: "", role: "", year: "", status: "",
};

function capped(value: string): string {
  return value.trim().slice(0, MAX_FIELD_LENGTH);
}

/**
 * Reads the six filters off the URL, dropping anything the catalogue does not know:
 * `source` only when `isPersonSource`, `status` only when `isPersonStatus`, `year`
 * only on a four-digit pattern. `company`, `name` and `role` are free text, trimmed
 * and capped at 100 characters -- a company id or a name/role fragment either one.
 */
export function parseSePeopleFilters(params: URLSearchParams): SePeopleFilters {
  const source = (params.get("source") ?? "").trim();
  const status = (params.get("status") ?? "").trim();
  const year = (params.get("year") ?? "").trim();
  return {
    company: capped(params.get("company") ?? ""),
    name: capped(params.get("name") ?? ""),
    source: isPersonSource(source) ? source : "",
    role: capped(params.get("role") ?? ""),
    year: YEAR_PATTERN.test(year) ? year : "",
    status: isPersonStatus(status) ? status : "",
  };
}

/**
 * The list's own URL for a given filter state and page: only the non-empty filters,
 * `page` when it is not 1, and `pageSize` when it is not the shared default -- so the
 * unfiltered first page of the default size is the bare `/admin/se/people`.
 */
export function sePeopleHref(
  filters: SePeopleFilters,
  page: number,
  pageSize: number,
): string {
  const params = new URLSearchParams();
  if (filters.company !== "") params.set("company", filters.company);
  if (filters.name !== "") params.set("name", filters.name);
  if (filters.source !== "") params.set("source", filters.source);
  if (filters.role !== "") params.set("role", filters.role);
  if (filters.year !== "") params.set("year", filters.year);
  if (filters.status !== "") params.set("status", filters.status);
  if (page !== 1) params.set("page", String(page));
  if (pageSize !== DEFAULT_PAGE_SIZE) params.set("pageSize", String(pageSize));
  const qs = params.toString();
  return qs === "" ? "/admin/se/people" : `/admin/se/people?${qs}`;
}

/** A published person's row links into their company's People tab, pre-selected. */
export function sePersonHref(companyId: string, personKey: string): string {
  return `/admin/se/company/${encodeURIComponent(companyId)}/people?person=${encodeURIComponent(personKey)}`;
}
