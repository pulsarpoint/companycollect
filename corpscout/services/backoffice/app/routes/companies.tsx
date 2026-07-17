import { Form, Link } from "react-router";
import { X } from "lucide-react";
import type { Route } from "./+types/companies";
import { getCountry } from "~/lib/countries";
import { parseUnifiedFilters } from "~/lib/filters";
import { searchUnifiedCompanies } from "~/lib/unified.server";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildUnifiedColumns } from "~/components/data-table/unified-columns";
import { FilterSidebar, facetLabel } from "~/components/data-table/filter-sidebar";
import { clearAllFilters, removeFilterValue } from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

export function meta(_: Route.MetaArgs) {
  return [{ title: "Companies – CompanyCollect Backoffice" }];
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q") ?? "";
  const filters = parseUnifiedFilters(url.searchParams);
  const result = await searchUnifiedCompanies({
    q,
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
    filters,
  });
  return { q, result, filters };
}

const nf = new Intl.NumberFormat("en-US");

export default function Companies({ loaderData }: Route.ComponentProps) {
  const { q, result, filters } = loaderData;
  const columns = buildUnifiedColumns(result.sort, result.dir);
  const searchParams = useEffectiveSearchParams();

  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Companies</h2>
        <div className="flex gap-2">
          <Form method="get" className="flex gap-2">
            <Input type="search" name="q" defaultValue={q} placeholder="Search by name…" className="w-64" />
            <Button type="submit" variant="secondary">
              Search
            </Button>
          </Form>
          <FilterSidebar filters={filters} />
        </div>
      </div>

      {Object.keys(filters).length > 0 ? (
        <div className="flex flex-wrap items-center gap-2">
          {Object.entries(filters).flatMap(([key, values]) =>
            values.map((value) => (
              <Badge key={`${key}:${value}`} variant="secondary" className="gap-1">
                <span className="text-muted-foreground">{facetLabel(key)}:</span>
                {key === "country" ? (getCountry(value)?.name ?? value) : value}
                <Link
                  to={removeFilterValue(searchParams, key, value)}
                  preventScrollReset
                  aria-label={`Remove ${value}`}
                >
                  <X className="size-3" />
                </Link>
              </Badge>
            )),
          )}
          <Link
            to={clearAllFilters(searchParams)}
            preventScrollReset
            className="text-muted-foreground text-xs underline"
          >
            Clear all
          </Link>
        </div>
      ) : null}

      <p className="text-muted-foreground text-sm">
        {nf.format(result.total)} companies{q ? ` matching “${q}”` : ""}
      </p>
      <DataTable columns={columns} data={result.rows} />
      <DataTablePagination total={result.total} page={result.page} pageSize={result.pageSize} />
    </>
  );
}
