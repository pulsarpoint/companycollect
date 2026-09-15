import { useState } from "react";
import type { Route } from "./+types/admin-se-companies-info";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import {
  NO_COMPANIES_SELECTED,
  selectionForSeCompanyFilters,
  type SeCompanySelection,
} from "~/lib/se-company-selection";
import { parseInfoFilters, parseListView } from "~/lib/se-company-info-filters";
import {
  listSeCompanyInfoPage,
  loadSeCompanyInfoCounts,
  loadSeCompanyInfoFilterOptions,
  resolveInfoSort,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md). Parsing lives in the client-safe
// `se-company-info-filters` module, shared with the ledger route and directly
// testable: it returns the filters as APPLIED (unknown and "Any" values
// dropped), so the chips and the Filters count can never claim a filter the
// query does not have.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseInfoFilters(url);
  const view = parseListView(url);
  // Whitelisted here as well as inside the query builder, so the component
  // renders the sort that was actually applied -- an unknown ?sort= shows the
  // default column as active rather than an indicator on nothing.
  const sort = resolveInfoSort(view.sort, view.dir);

  const [listPage, counts, options] = await Promise.all([
    listSeCompanyInfoPage({
      ...filters,
      page: view.page,
      pageSize: view.pageSize,
      ...sort,
    }),
    loadSeCompanyInfoCounts(filters),
    // Cached for ten minutes server-side (see FILTER_OPTIONS_TTL_MS): the
    // filter sheet's discrete option lists must not cost a FINAL scan per load.
    loadSeCompanyInfoFilterOptions(),
  ]);
  // listSeCompanyInfoPage runs no count() of its own -- the table's pagination
  // total is the counts strip's `total`, which shares this exact WHERE and is
  // already loaded above for the strip.
  return { listPage, counts, options, total: counts.total, filters, view, sort };
}

export function meta() {
  return [{ title: "Companies · Info | CompanyCollect" }];
}

export default function AdminSeCompanyInfoTable({ loaderData }: Route.ComponentProps) {
  const { listPage, counts, options, total, filters, view, sort } = loaderData;
  // Route-owned state survives pagination and sorting. Future bulk actions
  // submit this selection alongside the action name; query selections must
  // reach resolveSeCompanySelection on the server without client expansion.
  const [selection, setSelection] = useState<SeCompanySelection>(NO_COMPANIES_SELECTED);
  const currentSelection = selectionForSeCompanyFilters(selection, filters);
  if (currentSelection !== selection) setSelection(currentSelection);
  // The layout owns the page header now (title + tab bar), so this tab renders
  // only its own body.
  return (
    <SeCompanyInfoTable
      rows={listPage.rows}
      total={total}
      page={view.page}
      pageSize={view.pageSize}
      sort={sort.sort}
      dir={sort.dir}
      counts={counts}
      options={options}
      filters={filters}
      selection={currentSelection}
      onSelectionChange={setSelection}
    />
  );
}
