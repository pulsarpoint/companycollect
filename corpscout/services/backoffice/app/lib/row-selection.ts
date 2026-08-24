import type { RowSelectionState } from "@tanstack/react-table";

/**
 * The shape a multi-select list keeps its ticks in: TanStack Table's own
 * `RowSelectionState`, an id-keyed map, where the id is whatever the table's
 * `getRowId` returns -- for the SE company lists, the `company_id`.
 *
 * The state deliberately lives OUTSIDE the table (in the route component), so
 * filtering, sorting or paging -- all of which are search-param navigations
 * that re-run the loader with a new page of rows -- keep the ticks. That makes
 * a selection a set of ids that mostly names rows which are not on screen, and
 * every helper here is written for that: nothing is derived from the visible
 * rows.
 */
export type RowSelection = RowSelectionState;

/** Nothing selected -- also what "Clear" puts back. */
export const NO_ROWS_SELECTED: RowSelection = {};

/**
 * The selected ids, across every page the reviewer visited.
 *
 * TanStack DELETES a row's key when it is unticked, so the map is normally
 * all-true; a `false` that arrived some other way (a restored state, a hand-
 * written one in a test) must still read as "not selected", which is why this
 * filters on the value rather than returning `Object.keys` as they are.
 */
export function selectedRowIds(selection: RowSelection): string[] {
  return Object.keys(selection).filter((id) => selection[id]);
}

/** How many rows are selected in total -- NOT how many of them are on the
 * page being shown. This is the number the toolbar indicator says. */
export function selectedRowCount(selection: RowSelection): number {
  return selectedRowIds(selection).length;
}
