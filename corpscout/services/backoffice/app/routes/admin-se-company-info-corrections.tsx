import type { Route } from "./+types/admin-se-company-info-corrections";
import { SeCompanyInfoCorrectionsTable } from "~/components/admin/se-company-info-corrections-table";
import {
  parseCorrectionFilters,
  parseListView,
} from "~/lib/se-company-info-filters";
import {
  listSeCompanyInfoCorrectionsPage,
  loadSeCompanyInfoCorrectionFilterOptions,
  resolveCorrectionsSort,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md). Filter/view parsing is the
// company-info table route's own, from the client-safe filters module.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseCorrectionFilters(url);
  const view = parseListView(url);
  const sort = resolveCorrectionsSort(view.sort, view.dir);

  const [listPage, options] = await Promise.all([
    listSeCompanyInfoCorrectionsPage({
      ...filters,
      page: view.page,
      pageSize: view.pageSize,
      ...sort,
    }),
    loadSeCompanyInfoCorrectionFilterOptions(),
  ]);

  return { listPage, options, filters, view, sort };
}

export function meta() {
  return [{ title: "Info corrections | CompanyCollect" }];
}

export default function AdminSeCompanyInfoCorrections({ loaderData }: Route.ComponentProps) {
  const { listPage, options, filters, view, sort } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Info corrections</h1>
        <p className="text-sm text-muted-foreground">
          Every row of the se_company_info_correction ledger, newest first
          unless sorted. Status is computed against the published row: undone
          (superseded by a later undo), applied, stale (evidence moved since
          the decision), or pending.
        </p>
      </header>
      <SeCompanyInfoCorrectionsTable
        rows={listPage.rows}
        total={listPage.total}
        page={view.page}
        pageSize={view.pageSize}
        sort={sort.sort}
        dir={sort.dir}
        options={options}
        filters={filters}
      />
    </div>
  );
}
