import type { Route } from "./+types/admin-se-company-info-table";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import {
  listSeCompanyInfoPage,
  loadSeCompanyInfoCounts,
  type SeCompanyInfoListFilters,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md).

function parseFilters(url: URL): SeCompanyInfoListFilters {
  const q = (name: string) => url.searchParams.get(name) ?? "";
  const entity = q("entity");
  return {
    companyId: q("companyId"),
    name: q("name"),
    source: q("source"),
    multi: url.searchParams.get("multi") === "1",
    entity: entity === "legal" || entity === "sole" ? entity : undefined,
    corrected: url.searchParams.get("corrected") === "1",
  };
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseFilters(url);
  const page = clampPage(Number.parseInt(url.searchParams.get("page") || "1", 10));
  const pageSize = clampPageSize(
    Number.parseInt(url.searchParams.get("pageSize") || String(DEFAULT_PAGE_SIZE), 10),
  );

  const [listPage, counts] = await Promise.all([
    listSeCompanyInfoPage({ ...filters, page, pageSize }),
    loadSeCompanyInfoCounts(filters),
  ]);
  // listSeCompanyInfoPage no longer runs its own count() -- the table's
  // pagination total is the sum of the counts strip's by-source breakdown,
  // which shares this exact WHERE and is already loaded above for the strip.
  const total = counts.bySource.reduce((sum, entry) => sum + entry.count, 0);

  return { listPage, counts, total, filters, page, pageSize };
}

export function meta() {
  return [{ title: "Company info | CompanyCollect" }];
}

export default function AdminSeCompanyInfoTable({ loaderData }: Route.ComponentProps) {
  const { listPage, counts, total, filters, page, pageSize } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Company info</h1>
        <p className="text-sm text-muted-foreground">
          The published se_company_info row for every Swedish company, read
          FINAL. Filter to find what to review next; each row links to the
          per-company review page.
        </p>
      </header>
      <SeCompanyInfoTable
        rows={listPage.rows}
        total={total}
        page={page}
        pageSize={pageSize}
        counts={counts}
        filters={{
          companyId: filters.companyId ?? "",
          name: filters.name ?? "",
          source: filters.source ?? "",
          multi: filters.multi ?? false,
          entity: filters.entity ?? "",
          corrected: filters.corrected ?? false,
        }}
      />
    </div>
  );
}
