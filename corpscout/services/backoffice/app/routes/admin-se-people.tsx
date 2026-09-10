import type { Route } from "./+types/admin-se-people";
import { SePeopleTable } from "~/components/admin/se-people-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { parseSePeopleFilters } from "~/lib/se-people-filters";
import {
  listSePeoplePage,
  loadSePeopleCounts,
  resolveSePeopleCompanyIds,
} from "~/lib/se-people-list.server";

// Only `loader`, `meta` and the component live here (see CLAUDE.md, "Route modules
// and .server files"): any other export touching `~/lib/se-people-list.server` would
// keep that module in the client bundle and break the production build.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseSePeopleFilters(url.searchParams);
  const page = clampPage(Number.parseInt(url.searchParams.get("page") ?? "1", 10));
  const pageSize = clampPageSize(
    Number.parseInt(url.searchParams.get("pageSize") ?? String(DEFAULT_PAGE_SIZE), 10),
  );

  // The company-name filter is resolved ONCE here: the same ids then feed both
  // readers below, so the counts strip and the pager total honour it too
  // (pre-flight review 3.3).
  const { companyIds, truncated } = await resolveSePeopleCompanyIds(filters.company);

  const [{ rows }, counts] = await Promise.all([
    listSePeoplePage({ ...filters, companyIds, page, pageSize }),
    loadSePeopleCounts({ ...filters, companyIds }),
  ]);

  return { rows, counts, truncatedCompanies: truncated, page, pageSize, filters };
}

export function meta() {
  return [{ title: "People | CompanyCollect admin" }];
}

export default function AdminSePeople({ loaderData }: Route.ComponentProps) {
  const { rows, counts, truncatedCompanies, page, pageSize, filters } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">People</h1>
        <p className="text-muted-foreground text-sm">
          Every published person of the SE person entity, one row each; a row opens
          that company's People tab with the person selected.
        </p>
      </header>
      <SePeopleTable
        rows={rows}
        counts={counts}
        truncatedCompanies={truncatedCompanies}
        page={page}
        pageSize={pageSize}
        filters={filters}
      />
    </div>
  );
}
