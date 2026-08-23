import type { Route } from "./+types/admin-se-company-info-table";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { ANY_FILTER_VALUE } from "~/lib/se-company-info-filters";
import {
  listSeCompanyInfoPage,
  loadSeCompanyInfoCounts,
  loadSeCompanyInfoFilterOptions,
  resolveInfoSort,
  type SeCompanyInfoListFilters,
} from "~/lib/se-company-info-lists.server";

// Only `loader`, `meta` and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md).

/**
 * The filters as APPLIED, not as typed: the sheet's selects always submit a
 * value, so "Any" arrives as the sentinel and must be normalised away here --
 * otherwise the page would show a chip saying "Status any" for a filter the
 * query builder (rightly) ignores. `suggestion` is normalised the same way:
 * only yes/no mean anything.
 */
function parseFilters(url: URL): SeCompanyInfoListFilters {
  const q = (name: string) => {
    const value = url.searchParams.get(name) ?? "";
    return value === ANY_FILTER_VALUE ? "" : value;
  };
  const entity = q("entity");
  const suggestion = q("suggestion");
  return {
    companyId: q("companyId"),
    name: q("name"),
    source: q("source"),
    status: q("status"),
    legalForm: q("legalForm"),
    language: q("language"),
    suggestion: suggestion === "yes" || suggestion === "no" ? suggestion : "",
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
  // Whitelisted here as well as inside the query builder, so the component
  // renders the sort that was actually applied -- an unknown ?sort= shows the
  // default column as active rather than an indicator on nothing.
  const sort = resolveInfoSort(
    url.searchParams.get("sort") ?? undefined,
    url.searchParams.get("dir") ?? undefined,
  );

  const [listPage, counts, options] = await Promise.all([
    listSeCompanyInfoPage({ ...filters, page, pageSize, ...sort }),
    loadSeCompanyInfoCounts(filters),
    // Cached for ten minutes server-side (see FILTER_OPTIONS_TTL_MS): the
    // filter sheet's discrete option lists must not cost a FINAL scan per load.
    loadSeCompanyInfoFilterOptions(),
  ]);
  // listSeCompanyInfoPage no longer runs its own count() -- the table's
  // pagination total is the sum of the counts strip's by-source breakdown,
  // which shares this exact WHERE and is already loaded above for the strip.
  const total = counts.bySource.reduce((sum, entry) => sum + entry.count, 0);

  return { listPage, counts, options, total, filters, page, pageSize, sort };
}

export function meta() {
  return [{ title: "Company info | CompanyCollect" }];
}

export default function AdminSeCompanyInfoTable({ loaderData }: Route.ComponentProps) {
  const { listPage, counts, options, total, filters, page, pageSize, sort } = loaderData;
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
        page={page}
        pageSize={pageSize}
        sort={sort.sort}
        dir={sort.dir}
        counts={counts}
        options={options}
        filters={{
          companyId: filters.companyId ?? "",
          name: filters.name ?? "",
          source: filters.source ?? "",
          status: filters.status ?? "",
          legalForm: filters.legalForm ?? "",
          language: filters.language ?? "",
          suggestion: filters.suggestion ?? "",
          entity: filters.entity ?? "",
          multi: filters.multi ?? false,
          corrected: filters.corrected ?? false,
        }}
      />
    </div>
  );
}
