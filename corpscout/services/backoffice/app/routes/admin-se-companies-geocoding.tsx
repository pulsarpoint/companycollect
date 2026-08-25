import type { Route } from "./+types/admin-se-companies-geocoding";
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

// Only server-only exports (`loader`), `meta` and the component live here, same
// discipline as admin-se-companies-info.tsx: any OTHER export touching
// `~/lib/*.server` would keep that module in the client bundle and break the
// production build (see CLAUDE.md). `parseListView` is reused rather than
// re-spelled -- it already clamps page/pageSize the same way every other admin
// list does.

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
  return [{ title: "Companies · Geocoding | CompanyCollect" }];
}

export default function AdminSeCompanyInfoGeocoding({
  loaderData,
}: Route.ComponentProps) {
  const { listPage, counts, total, page, pageSize, filter } = loaderData;
  // The layout owns the page header (title + tab bar); this tab renders only its
  // own body -- the geocoding list.
  return (
    <SeCompanyGeocodingTable
      rows={listPage.rows}
      total={total}
      page={page}
      pageSize={pageSize}
      filter={filter}
      counts={counts}
    />
  );
}
