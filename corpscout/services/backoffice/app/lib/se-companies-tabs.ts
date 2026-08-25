/**
 * The top tabs of the Sweden all-companies list area
 * (`/admin/se/companies/<tab>`).
 *
 * Client-safe on purpose, exactly like `se-company-tabs.ts` (the single-company
 * detail area's sibling): both the companies-list layout (which renders the
 * tabs) and the admin breadcrumbs (which name the active one) need it, and the
 * breadcrumbs live outside the companies route tree where no loader data
 * reaches them.
 *
 * Info is the section index, so its path is the bare root; the other tabs are
 * child segments under it.
 */
export const SE_COMPANIES_TABS = [
  { value: "info", label: "Info" },
  { value: "geocoding", label: "Geocoding" },
  { value: "financial", label: "Financial" },
  { value: "people", label: "People" },
] as const;

export type SeCompaniesTab = (typeof SE_COMPANIES_TABS)[number]["value"];

/**
 * Which tab a path is on. Info is the fallback because it is the section index:
 * a bare `/admin/se/companies` is Info in every way that matters to a reader.
 */
export function seCompaniesTabFromPath(pathname: string): SeCompaniesTab {
  // ["", "admin", "se", "companies", <tab?>]
  const segment = pathname.split("/")[4] ?? "";
  const tab = SE_COMPANIES_TABS.find((entry) => entry.value === segment);
  return tab ? tab.value : "info";
}

export function seCompaniesTabLabel(tab: SeCompaniesTab): string {
  return SE_COMPANIES_TABS.find((entry) => entry.value === tab)?.label ?? "Info";
}

/** Info is the index route, so it has no segment of its own. */
export function seCompaniesTabPath(tab: SeCompaniesTab): string {
  return tab === "info" ? "/admin/se/companies" : `/admin/se/companies/${tab}`;
}
