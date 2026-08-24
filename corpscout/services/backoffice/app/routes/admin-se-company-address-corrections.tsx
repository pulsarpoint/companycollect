import type { Route } from "./+types/admin-se-company-address-corrections";
import { SeCompanyAddressCorrectionsTable } from "~/components/admin/se-company-address-corrections-table";
import {
  SE_ADDRESS_CORRECTION_KINDS,
  SE_ADDRESS_CORRECTION_STATUSES,
} from "~/lib/se-address-corrections";
import {
  parseCorrectionFilters,
  parseListView,
} from "~/lib/se-company-info-filters";
import {
  listSeCompanyAddressCorrectionsPage,
  loadSeCompanyAddressCorrectionFilterOptions,
  resolveCorrectionsSort,
} from "~/lib/se-company-address-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md). Filter/view parsing is the
// client-safe module both correction ledgers share, told which kinds this one
// allows.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseCorrectionFilters(url, {
    kinds: SE_ADDRESS_CORRECTION_KINDS,
    statuses: SE_ADDRESS_CORRECTION_STATUSES,
  });
  const view = parseListView(url);
  const sort = resolveCorrectionsSort(view.sort, view.dir);

  const [listPage, options] = await Promise.all([
    listSeCompanyAddressCorrectionsPage({
      ...filters,
      page: view.page,
      pageSize: view.pageSize,
      ...sort,
    }),
    loadSeCompanyAddressCorrectionFilterOptions(),
  ]);

  return { listPage, options, filters, view, sort };
}

export function meta() {
  return [{ title: "Address corrections | CompanyCollect" }];
}

export default function AdminSeCompanyAddressCorrections({
  loaderData,
}: Route.ComponentProps) {
  const { listPage, options, filters, view, sort } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Address corrections</h1>
        <p className="text-sm text-muted-foreground">
          Every row of the se_company_address_correction ledger, newest first
          unless sorted. Each decision names the address it is about. Status is
          computed against what is published now: undone (superseded by a later
          undo), applied, stale (the evidence moved, or the address it named is
          gone), or pending.
        </p>
      </header>
      <SeCompanyAddressCorrectionsTable
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
