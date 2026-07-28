import { useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { Search } from "lucide-react";
import type { Route } from "./+types/countries";
import { getCountryDirectory } from "~/lib/countries-overview.server";
import { CountryWorldMap } from "~/components/countries/country-world-map";
import { formatRevenueUsd } from "~/lib/money";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldLabel } from "~/components/ui/field";
import { InputGroup, InputGroupAddon, InputGroupInput } from "~/components/ui/input-group";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export function meta(_: Route.MetaArgs) {
  return [{ title: "Countries – CompanyCollect Backoffice" }];
}

export async function loader() {
  return { countries: await getCountryDirectory() };
}

export function HydrateFallback() {
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Skeleton className="h-[32rem]" />
      <Skeleton className="h-[32rem]" />
    </div>
  );
}

const nf = new Intl.NumberFormat("en-US");

function normalizeCountrySearch(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

export default function Countries({ loaderData }: Route.ComponentProps) {
  const { countries } = loaderData;
  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const normalizedQuery = normalizeCountrySearch(query.trim());
  const visibleCountries = useMemo(
    () =>
      countries.filter((country) => {
        if (!normalizedQuery) return true;
        return (
          normalizeCountrySearch(country.country_name).includes(normalizedQuery) ||
          country.country_code.includes(normalizedQuery)
        );
      }),
    [countries, normalizedQuery],
  );

  function openCountry(code: string) {
    navigate(`/countries/${code}`);
  }

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Countries</h1>
        <p className="text-muted-foreground mt-1 text-sm">
          Explore company coverage and financial reporting across national registries.
        </p>
      </div>

      <div className="grid min-w-0 gap-4 lg:grid-cols-2">
        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>Registry coverage</CardTitle>
            <CardDescription>Highlighted countries have company data available.</CardDescription>
          </CardHeader>
          <CardContent className="flex min-h-[26rem] items-center justify-center">
            <CountryWorldMap countries={countries} />
          </CardContent>
        </Card>

        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>Country directory</CardTitle>
            <CardDescription>{nf.format(countries.length)} connected registries</CardDescription>
            <Field className="mt-2">
              <FieldLabel htmlFor="country-search" className="sr-only">
                Filter countries by name or ISO code
              </FieldLabel>
              <InputGroup>
                <InputGroupAddon>
                  <Search aria-hidden />
                </InputGroupAddon>
                <InputGroupInput
                  id="country-search"
                  type="search"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Filter by country name or code…"
                />
              </InputGroup>
            </Field>
          </CardHeader>
          <CardContent className="min-w-0 px-0">
            <div className="max-h-[34rem] overflow-auto border-y">
              <Table className="table-fixed text-xs">
                <TableHeader className="bg-card sticky top-0 z-10">
                  <TableRow>
                    <TableHead className="w-[25%] pl-4">Country</TableHead>
                    <TableHead className="w-[19%] whitespace-normal text-right leading-tight">Companies</TableHead>
                    <TableHead className="w-[25%] whitespace-normal text-right leading-tight">
                      Financial coverage
                    </TableHead>
                    <TableHead className="w-[20%] whitespace-normal text-right leading-tight">
                      Reported revenue
                    </TableHead>
                    <TableHead className="w-[11%] whitespace-normal pr-4 text-right leading-tight">
                      Latest FY
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visibleCountries.map((country) => {
                    const coverage =
                      country.total_companies > 0
                        ? (country.companies_with_financials / country.total_companies) * 100
                        : 0;
                    return (
                      <TableRow
                        key={country.country_code}
                        role="link"
                        tabIndex={0}
                        aria-label={`Open ${country.country_name}, ${nf.format(country.total_companies)} companies`}
                        className="cursor-pointer transition-colors focus-visible:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset"
                        onClick={() => openCountry(country.country_code)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            openCountry(country.country_code);
                          }
                        }}
                      >
                        <TableCell className="pl-4 font-medium">
                          <span className="flex min-w-0 items-center gap-1.5">
                            <span aria-hidden>{country.flag}</span>
                            <span className="min-w-0 truncate" title={country.country_name}>
                              {country.country_name}
                            </span>
                            <span className="text-muted-foreground hidden font-mono text-[0.65rem] uppercase xl:inline">
                              {country.country_code}
                            </span>
                          </span>
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {nf.format(country.total_companies)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          <span>{nf.format(country.companies_with_financials)}</span>
                          <span className="text-muted-foreground ml-1">({coverage.toFixed(1)}%)</span>
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatRevenueUsd(country.revenue_usd, null)}
                        </TableCell>
                        <TableCell className="pr-4 text-right tabular-nums">
                          {country.latest_fiscal_year ?? "—"}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                  {visibleCountries.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-muted-foreground h-24 text-center">
                        No countries match “{query}”.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
