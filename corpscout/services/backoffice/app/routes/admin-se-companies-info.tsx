import { useState } from "react";
import type { Route } from "./+types/admin-se-company-info-table";
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
  return [{ title: "Company info | CompanyCollect" }];
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
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        {/* Named for the sidebar entry that opens it, not for what the list
            now shows: the page IS the companies list of se_company_info. */}
        <h1 className="text-2xl font-semibold tracking-tight">Company info</h1>
        <p className="text-sm text-muted-foreground">
          Every Swedish company published in se_company_info, read FINAL.
          Filter and sort to find one; each row opens that company's info page,
          where its description, sources, suggestions and corrections live.
        </p>
      </header>
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
    </div>
  );
}
