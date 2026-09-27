export type DomainSelection<Filters> =
  | { mode: "ids"; domains: string[] }
  | { mode: "query"; query: Filters; excludedDomains: string[] };

export const NO_DOMAINS_SELECTED: { mode: "ids"; domains: string[] } = { mode: "ids", domains: [] };

export function selectionForDomainFilters<F>(selection: DomainSelection<F>, filters: F): DomainSelection<F> {
  return selection.mode === "query" && JSON.stringify(selection.query) !== JSON.stringify(filters)
    ? NO_DOMAINS_SELECTED : selection;
}

export function isDomainSelected(selection: DomainSelection<unknown>, domain: string): boolean {
  return selection.mode === "ids" ? selection.domains.includes(domain) : !selection.excludedDomains.includes(domain);
}

/** Keep explicit picks or all-matching exclusions across pages. */
export function selectDomains<F>(selection: DomainSelection<F>, domains: readonly string[], checked: boolean): DomainSelection<F> {
  const values = new Set(selection.mode === "ids" ? selection.domains : selection.excludedDomains);
  for (const domain of domains) {
    if (checked === (selection.mode === "ids")) values.add(domain);
    else values.delete(domain);
  }
  return selection.mode === "ids"
    ? { mode: "ids", domains: [...values] }
    : { ...selection, excludedDomains: [...values] };
}
