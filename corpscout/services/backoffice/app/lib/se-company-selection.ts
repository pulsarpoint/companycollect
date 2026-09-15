import { functionalUpdate, type Updater } from "@tanstack/react-table";
import { selectedRowIds, type RowSelection } from "~/lib/row-selection";
import type { SeCompanyInfoTableFilters } from "~/lib/se-company-info-filters";

/** JSON payload for an SE company action's `selection` field. Query selections
 * are resolved on the server when the action runs, never expanded in the browser.
 * Page, page size and sort are deliberately absent from the query. */
export type SeCompanySelection =
  | { mode: "ids"; companyIds: string[] }
  | {
      mode: "query";
      query: SeCompanyInfoTableFilters;
      excludedCompanyIds: string[];
    };

export const NO_COMPANIES_SELECTED: SeCompanySelection = { mode: "ids", companyIds: [] };

/** Changing the filter must not silently retarget a query selection. Explicit
 * picks still survive filter changes, as they did before query selection. */
export function selectionForSeCompanyFilters(
  selection: SeCompanySelection,
  filters: SeCompanyInfoTableFilters,
): SeCompanySelection {
  return selection.mode === "query" && JSON.stringify(selection.query) !== JSON.stringify(filters)
    ? NO_COMPANIES_SELECTED
    : selection;
}

/** TanStack needs ticks only for the rendered page in query mode. */
export function seCompanyRowSelection(
  selection: SeCompanySelection,
  pageIds: readonly string[],
): RowSelection {
  if (selection.mode === "ids") {
    return Object.fromEntries(selection.companyIds.map((id) => [id, true]));
  }
  const excluded = new Set(selection.excludedCompanyIds);
  return Object.fromEntries(pageIds.filter((id) => !excluded.has(id)).map((id) => [id, true]));
}

/** Apply row/header checkbox changes without losing picks or exclusions from
 * other pages. In query mode only unchecked ids need to cross the wire. */
export function updateSeCompanyRowSelection(
  selection: SeCompanySelection,
  pageIds: readonly string[],
  updater: Updater<RowSelection>,
): SeCompanySelection {
  const rows = functionalUpdate(updater, seCompanyRowSelection(selection, pageIds));
  if (selection.mode === "ids") {
    return { mode: "ids", companyIds: selectedRowIds(rows) };
  }
  const excluded = new Set(selection.excludedCompanyIds);
  for (const id of pageIds) {
    if (rows[id]) excluded.delete(id);
    else excluded.add(id);
  }
  return { ...selection, excludedCompanyIds: [...excluded] };
}
