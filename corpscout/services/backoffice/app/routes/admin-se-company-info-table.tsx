import type { Route } from "./+types/admin-se-company-info-table";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
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
  // listSeCompanyInfoPage no longer runs its own count() -- the table's
  // pagination total is the sum of the counts strip's by-source breakdown,
  // which shares this exact WHERE and is already loaded above for the strip.
  const total = counts.bySource.reduce((sum, entry) => sum + entry.count, 0);

  return { listPage, counts, options, total, filters, view, sort };
}

export function meta() {
  return [{ title: "Company info | CompanyCollect" }];
}

export default function AdminSeCompanyInfoTable({ loaderData }: Route.ComponentProps) {
  const { listPage, counts, options, total, filters, view, sort } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Company info</h1>
        <p className="text-sm text-muted-foreground">
          The published se_company_info row for every Swedish company, read
          FINAL. Filter and sort to find what to review next; each row opens
          that company's info page.
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
      />
    </div>
  );
}
