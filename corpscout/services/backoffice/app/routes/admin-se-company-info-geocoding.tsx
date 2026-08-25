import type { Route } from "./+types/admin-se-company-info-geocoding";
import { SeCompanyGeocodingTable } from "~/components/admin/se-company-geocoding-table";
import { parseListView } from "~/lib/se-company-info-filters";
import {
  GEOCODE_STATUS_PARAM,
  parseGeocodeListFilter,
} from "~/lib/se-company-geocoding-filters";
import {
  countForFilter,
  listSeCompanyGeocodingPage,
  loadSeCompanyGeocodingCounts,
} from "~/lib/se-company-geocoding-list.server";

// Only `loader`, `meta` and the component live here, same discipline as
// admin-se-company-info-table.tsx: any other export touching `~/lib/*.server`
// would keep that module in the client bundle and break the production build
// (see CLAUDE.md). `parseListView` is reused rather than re-spelled -- it
// already clamps page/pageSize the same way every other admin list does.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filter = parseGeocodeListFilter(url.searchParams.get(GEOCODE_STATUS_PARAM));
  const view = parseListView(url);

  const [listPage, counts] = await Promise.all([
    listSeCompanyGeocodingPage({ filter, page: view.page, pageSize: view.pageSize }),
    // One scan of `published` covers the strip's six numbers AND this page's
    // own pagination total (countForFilter) -- no separate count() query.
    loadSeCompanyGeocodingCounts(),
  ]);

  return {
    listPage,
    counts,
    total: countForFilter(counts, filter),
    page: view.page,
    pageSize: view.pageSize,
    filter,
  };
}

export function meta() {
  return [{ title: "Geocoding | CompanyCollect" }];
}

export default function AdminSeCompanyInfoGeocoding({
  loaderData,
}: Route.ComponentProps) {
  const { listPage, counts, total, page, pageSize, filter } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Geocoding</h1>
        <p className="text-sm text-muted-foreground">
          Every Swedish company with a published address (se_company_address),
          and that address's own geocode outcome. Defaults to companies that
          need attention: an address with no successful geocode mapping.
        </p>
      </header>
      <SeCompanyGeocodingTable
        rows={listPage.rows}
        total={total}
        page={page}
        pageSize={pageSize}
        filter={filter}
        counts={counts}
      />
    </div>
  );
}
