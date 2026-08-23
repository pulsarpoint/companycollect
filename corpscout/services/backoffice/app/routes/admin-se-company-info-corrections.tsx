import type { Route } from "./+types/admin-se-company-info-corrections";
import { SeCompanyInfoCorrectionsTable } from "~/components/admin/se-company-info-corrections-table";
import {
  listSeCompanyInfoCorrectionsPage,
  type SeCompanyInfoCorrectionListFilters,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md).

function clampPageSize(value: number): number {
  return Math.min(200, Math.max(10, value));
}

function parseFilters(url: URL): SeCompanyInfoCorrectionListFilters {
  const q = (name: string) => url.searchParams.get(name) ?? "";
  return {
    companyId: q("companyId"),
    kind: q("kind"),
    status: q("status"),
  };
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseFilters(url);
  const page = Math.max(1, Number.parseInt(url.searchParams.get("page") || "1", 10) || 1);
  const pageSize = clampPageSize(
    Number.parseInt(url.searchParams.get("pageSize") || "50", 10) || 50,
  );

  const listPage = await listSeCompanyInfoCorrectionsPage({ ...filters, page, pageSize });

  return { listPage, filters, page, pageSize };
}

export function meta() {
  return [{ title: "Info corrections | CompanyCollect" }];
}

export default function AdminSeCompanyInfoCorrections({ loaderData }: Route.ComponentProps) {
  const { listPage, filters, page, pageSize } = loaderData;
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
        filters={{
          companyId: filters.companyId ?? "",
          kind: filters.kind ?? "",
          status: filters.status ?? "",
        }}
      />
    </div>
  );
}
