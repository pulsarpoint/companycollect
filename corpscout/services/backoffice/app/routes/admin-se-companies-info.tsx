import { useState } from "react";
import { Link } from "react-router";
import type { Route } from "./+types/admin-se-companies-info";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import { NO_ROWS_SELECTED, type RowSelection } from "~/lib/row-selection";
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
  // The picked companies live HERE, in the route component, because that is
  // where the Pipeline sheet's consumer can reach them: the sheet renders
  // beside the table's Filters button and needs the same `selection` the
  // checkboxes write to. Filtering, sorting and paging are all search-param
  // navigations of THIS route, which re-run the loader without unmounting this
  // component, so the ticks survive them -- a side effect of the placement, not
  // a store: nothing here persists a selection past a reload.
  //
  //   `selection`     TanStack's RowSelectionState, keyed by company_id (the
  //                   table passes `getRowId: row => row.company_id`), and so
  //                   full of ids whose rows are not on screen.
  //   `setSelection`  the OnChangeFn the table's checkboxes call; also what
  //                   the toolbar's Clear resets.
  //   `selectedRowIds(selection)` (~/lib/row-selection) turns it into the
  //                   id list the pipeline launches post as `company_ids`.
  const [selection, setSelection] = useState<RowSelection>(NO_ROWS_SELECTED);
  // The layout owns the page header now (title + tab bar), so this tab renders
  // only its own body. The corrections ledger is not a tab -- it is a secondary
  // link from here, its old sidebar entry having been folded into "Companies".
  return (
    <>
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <Link
          className="underline underline-offset-2"
          to="/admin/se/company-info/corrections"
        >
          Info corrections
        </Link>
      </div>
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
        selection={selection}
        onSelectionChange={setSelection}
      />
    </>
  );
}
