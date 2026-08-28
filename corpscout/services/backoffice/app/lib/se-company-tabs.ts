/**
 * The sub-menu of the Sweden company admin area
 * (`/admin/se/company/:companyId/<tab>`).
 *
 * Client-safe on purpose: both the company layout (which renders the tabs)
 * and the admin breadcrumbs (which name the active one) need it, and the
 * breadcrumbs live outside the company route tree where no loader data
 * reaches them.
 */
export const SE_COMPANY_TABS = [
  { value: "info", label: "Info" },
  { value: "address", label: "Address" },
  { value: "financial", label: "Financial" },
  { value: "people", label: "People" },
  { value: "domains", label: "Domains" },
  { value: "technology", label: "Technology" },
  { value: "contracts", label: "Contracts" },
  { value: "jobs", label: "Jobs" },
  { value: "listed", label: "Publicly traded" },
] as const;

export type SeCompanyTab = (typeof SE_COMPANY_TABS)[number]["value"];

/** The company id in `/admin/se/company/:companyId/...`, or "". */
export function seCompanyIdFromPath(pathname: string): string {
  const segments = pathname.split("/");
  // ["", "admin", "se", "company", <id>, <tab>]
  if (segments[1] !== "admin" || segments[2] !== "se") return "";
  if (segments[3] !== "company") return "";
  return segments[4] ?? "";
}

/**
 * Which tab a path is on. Info is the fallback because the area's index
 * redirects there: a bare `/admin/se/company/:id` is Info in every way that
 * matters to a reader, including mid-redirect.
 */
export function seCompanyTabFromPath(pathname: string): SeCompanyTab {
  const segment = pathname.split("/")[5] ?? "";
  const tab = SE_COMPANY_TABS.find((entry) => entry.value === segment);
  return tab ? tab.value : "info";
}

export function seCompanyTabLabel(tab: SeCompanyTab): string {
  return SE_COMPANY_TABS.find((entry) => entry.value === tab)?.label ?? "Info";
}

export function seCompanyTabPath(companyId: string, tab: SeCompanyTab): string {
  return `/admin/se/company/${encodeURIComponent(companyId)}/${tab}`;
}
