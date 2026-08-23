import type { Route } from "./+types/admin-se-company-info-corrections";
import { SeCompanyInfoCorrectionsTable } from "~/components/admin/se-company-info-corrections-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { ANY_FILTER_VALUE } from "~/lib/se-company-info-filters";
import {
  listSeCompanyInfoCorrectionsPage,
  loadSeCompanyInfoCorrectionFilterOptions,
  resolveCorrectionsSort,
  type SeCompanyInfoCorrectionListFilters,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md).

/** As applied, not as typed -- see the company-info table route's own
 * parseFilters for why the select's "Any" sentinel is normalised here. */
function parseFilters(url: URL): SeCompanyInfoCorrectionListFilters {
  const q = (name: string) => {
    const value = url.searchParams.get(name) ?? "";
    return value === ANY_FILTER_VALUE ? "" : value;
  };
  return {
    companyId: q("companyId"),
    kind: q("kind"),
    status: q("status"),
    decidedBy: q("decidedBy"),
  };
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseFilters(url);
  const page = clampPage(Number.parseInt(url.searchParams.get("page") || "1", 10));
  const pageSize = clampPageSize(
    Number.parseInt(url.searchParams.get("pageSize") || String(DEFAULT_PAGE_SIZE), 10),
  );
  const sort = resolveCorrectionsSort(
    url.searchParams.get("sort") ?? undefined,
    url.searchParams.get("dir") ?? undefined,
  );

  const [listPage, options] = await Promise.all([
    listSeCompanyInfoCorrectionsPage({ ...filters, page, pageSize, ...sort }),
    loadSeCompanyInfoCorrectionFilterOptions(),
  ]);

  return { listPage, options, filters, page, pageSize, sort };
}

export function meta() {
  return [{ title: "Info corrections | CompanyCollect" }];
}

export default function AdminSeCompanyInfoCorrections({ loaderData }: Route.ComponentProps) {
  const { listPage, options, filters, page, pageSize, sort } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Info corrections</h1>
        <p className="text-sm text-muted-foreground">
          Every row of the se_company_info_correction ledger, newest first.
          Status is computed against the published row: undone (superseded by
          a later undo), applied, stale (evidence moved since the decision),
          or pending.
        </p>
      </header>
      <SeCompanyInfoCorrectionsTable
        rows={listPage.rows}
        total={listPage.total}
        page={page}
        pageSize={pageSize}
        sort={sort.sort}
        dir={sort.dir}
        options={options}
        filters={{
          companyId: filters.companyId ?? "",
          kind: filters.kind ?? "",
          status: filters.status ?? "",
          decidedBy: filters.decidedBy ?? "",
        }}
      />
    </div>
  );
}
