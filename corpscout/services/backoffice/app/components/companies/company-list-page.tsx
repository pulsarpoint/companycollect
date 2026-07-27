import { Form, Link } from "react-router";
import { ArrowLeft, X } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import { getCountry } from "~/lib/countries";
import type { CompanyListData } from "~/lib/company-list.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildUnifiedColumns } from "~/components/data-table/unified-columns";
import { FilterSidebar, facetLabel } from "~/components/data-table/filter-sidebar";
import { clearAllFilters, removeFilterValue } from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

const nf = new Intl.NumberFormat("en-US");

export function CompanyListPage({
  data,
  lockedCountry,
}: {
  data: CompanyListData;
  lockedCountry?: CountryConfig;
}) {
  const { q, result, filters } = data;
  const columns = buildUnifiedColumns(result.sort, result.dir, {
    showCountry: lockedCountry == null,
  });
  const searchParams = useEffectiveSearchParams();
  const title = lockedCountry ? `Companies in ${lockedCountry.name}` : "Companies";

  return (
    <div className="flex flex-col gap-4">
      {lockedCountry ? (
        <div>
          <Button
            variant="ghost"
            size="sm"
            className="-ml-2"
            nativeButton={false}
            render={<Link to={`/countries/${lockedCountry.code}`} />}
          >
            <ArrowLeft data-icon="inline-start" />
            {lockedCountry.name}
          </Button>
        </div>
      ) : null}

      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="flex items-center gap-2 text-xl font-semibold">
          {lockedCountry ? <span aria-hidden>{lockedCountry.flag}</span> : null}
          <span>{title}</span>
        </h2>
        <div className="flex flex-wrap gap-2">
          <Form method="get" className="flex gap-2">
            <Input type="search" name="q" defaultValue={q} placeholder="Search by name…" className="w-64" />
            <Button type="submit" variant="secondary">
              Search
            </Button>
          </Form>
          <FilterSidebar filters={filters} lockedCountry={lockedCountry?.code} />
        </div>
      </div>

      {Object.keys(filters).length > 0 ? (
        <div className="flex flex-wrap items-center gap-2">
          {Object.entries(filters).flatMap(([key, values]) =>
            values.map((value) => (
              <Badge key={`${key}:${value}`} variant="secondary" className="gap-1">
                <span className="text-muted-foreground">{facetLabel(key)}:</span>
                {key === "country"
                  ? (getCountry(value)?.name ?? value)
                  : key === "has_financials"
                    ? (value === "true" ? "yes" : value)
                    : value}
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
      <DataTable columns={columns} data={result.rows} emptyText="No companies found." />
      <DataTablePagination total={result.total} page={result.page} pageSize={result.pageSize} />
    </div>
  );
}
