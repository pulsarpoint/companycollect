import { Form, Link, redirect } from "react-router";
import { X } from "lucide-react";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { parseFilters } from "~/lib/filters";
import { searchCompanies } from "~/lib/queries.server";
import { getLegalFormLabels } from "~/lib/legal-forms.server";
import {
  availableCompanyColumns,
  parseCompanyColumns,
} from "~/lib/company-columns";
import { CompanyColumnPicker } from "~/components/data-table/company-column-picker";
import { availableCompanyFlags, flagFilterKey } from "~/lib/company-flags";
import {
  FINANCIAL_FILING_FILTER_KEY,
  isFinancialFilingStatus,
} from "~/lib/financial-filing-status";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildCompanyColumns } from "~/components/data-table/company-columns";
import {
  FilterSidebar,
  facetLabel,
  facetValueLabel,
} from "~/components/data-table/filter-sidebar";
import {
  clearAllFilters,
  removeFilterValue,
} from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const url = new URL(request.url);
  const legacyFilterKey = `f_${FINANCIAL_FILING_FILTER_KEY}`;
  if (country.code === "se" && url.searchParams.has(legacyFilterKey)) {
    const legacyStatuses = url.searchParams
      .getAll(legacyFilterKey)
      .filter(isFinancialFilingStatus);
    const financialDataFilterKey = `f_${flagFilterKey("financials")}`;
    url.searchParams.delete(legacyFilterKey);
    if (!url.searchParams.has(financialDataFilterKey)) {
      const hasData = legacyStatuses.includes("data_available");
      const hasNoData = legacyStatuses.some(
        (status) => status !== "data_available",
      );
      if (hasData !== hasNoData) {
        url.searchParams.set(financialDataFilterKey, hasData ? "yes" : "no");
      }
    }
    throw redirect(`${url.pathname}${url.search}`);
  }
  const filters = parseFilters(url.searchParams, country);
  const [result, legalFormLabels] = await Promise.all([
    searchCompanies(country, {
      q: url.searchParams.get("q") ?? "",
      page: Number(url.searchParams.get("page") ?? "1") || 1,
      pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
      sort: url.searchParams.get("sort"),
      dir: url.searchParams.get("dir"),
      filters,
    }),
    // A few dozen rows, cached per process — se_companies stores only the code.
    getLegalFormLabels(country),
  ]);
  const available = availableCompanyColumns(country);
  return {
    q: url.searchParams.get("q") ?? "",
    result,
    filters,
    legalForms: Object.fromEntries(legalFormLabels),
    available,
    visibleColumns: parseCompanyColumns(url.searchParams, available),
  };
}

export function meta({ params }: Route.MetaArgs) {
  const country = getCountry(params.country);
  return [
    {
      title: `${country?.name ?? "Country"} companies – CompanyCollect Backoffice`,
    },
  ];
}

export default function CountryCompanies({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { q, result, filters, legalForms, available, visibleColumns } =
    loaderData;
  const country = getCountry(params.country)!;
  const columns = buildCompanyColumns(
    country,
    result.sort,
    result.dir,
    legalForms,
    visibleColumns,
  );
  const searchParams = useEffectiveSearchParams();
  const flagLegend = availableCompanyFlags(country.code);

  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">
          {country.flag} {country.name} companies
        </h2>
        <div className="flex gap-2">
          {country.code === "se" ? (
            <div className="flex gap-2">
              <Button
                variant="outline"
                render={<Link to="/countries/se/address-quality" />}
                nativeButton={false}
              >
                Review address quality
              </Button>
              <Button
                variant="outline"
                render={<Link to="/countries/se/domain-suggestions" />}
                nativeButton={false}
              >
                Review domain suggestions
              </Button>
            </div>
          ) : null}
          <Form method="get" className="flex gap-2">
            <Input
              type="search"
              name="q"
              defaultValue={q}
              placeholder="Search by name…"
              className="w-64"
            />
            <Button type="submit" variant="secondary">
              Search
            </Button>
          </Form>
          <CompanyColumnPicker
            countryCode={country.code}
            visible={visibleColumns}
            available={available}
          />
          <FilterSidebar country={country} filters={filters} />
        </div>
      </div>

      {Object.keys(filters).length > 0 ? (
        <div className="flex flex-wrap items-center gap-2">
          {Object.entries(filters).flatMap(([key, values]) =>
            values.map((value) => (
              <Badge
                key={`${key}:${value}`}
                variant="secondary"
                className="gap-1"
              >
                <span className="text-muted-foreground">
                  {facetLabel(country, key)}:
                </span>
                {facetValueLabel(key, value)}
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

      {flagLegend.length > 0 && visibleColumns.includes("data") ? (
        <div className="text-muted-foreground flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
          <span className="uppercase tracking-wide">Data held</span>
          {flagLegend.map((flag) => (
            <span key={flag.id} className="flex items-center gap-1.5">
              <span className="inline-flex size-5 shrink-0 items-center justify-center rounded-full border border-emerald-600/30 bg-emerald-500/15 text-[10px] leading-none font-semibold text-emerald-700 dark:border-emerald-400/30 dark:bg-emerald-400/15 dark:text-emerald-300">
                {flag.char}
              </span>
              <span>{flag.label}</span>
            </span>
          ))}
          {flagLegend.length > 0 ? (
            <span className="opacity-70">green = held, red = not held</span>
          ) : null}
        </div>
      ) : null}

      <DataTable columns={columns} data={result.rows} />

      <DataTablePagination
        total={result.total}
        page={result.page}
        pageSize={result.pageSize}
      />
    </>
  );
}
