import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/financials-industry";
import { getIndustryFinancials } from "~/lib/financial-aggregates.server";
import { getCountry } from "~/lib/countries";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { RevenueBarChart } from "~/components/financials/revenue-bar-chart";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export async function loader({ params, request }: Route.LoaderArgs) {
  const countryCode = new URL(request.url).searchParams.get("country")?.toLowerCase();
  if (countryCode && !getCountry(countryCode)) {
    throw new Response("Country not found", { status: 404 });
  }

  const data = await getIndustryFinancials(params.division, countryCode);
  if (!data) throw new Response("Not found", { status: 404 });
  return data;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const label = loaderData?.label;
  const country = loaderData?.countryCode
    ? getCountry(loaderData.countryCode)?.name
    : null;
  const title = label ? `${params.division} ${label}` : `Industry ${params.division}`;
  return [{ title: `${title}${country ? ` · ${country}` : ""} – CompanyCollect Backoffice` }];
}

const nf = new Intl.NumberFormat("en-US");

export default function FinancialsIndustry({ loaderData }: Route.ComponentProps) {
  const { division, label, countryCode, countries, topCompanies } = loaderData;
  const selectedCountry = countryCode ? getCountry(countryCode) : null;
  const backHref = selectedCountry
    ? `/countries/${selectedCountry.code}?tab=business`
    : "/financials";

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={backHref} />}
        >
          <ArrowLeft data-icon="inline-start" />
          {selectedCountry ? selectedCountry.name : "Financials"}
        </Button>
      </div>

      <div>
        <h2 className="flex items-baseline gap-2 text-2xl font-semibold">
          <span className="text-muted-foreground font-mono text-base">{division}</span>
          <span>{label}</span>
        </h2>
        {selectedCountry ? (
          <p className="text-muted-foreground mt-1 text-sm">
            {selectedCountry.flag} {selectedCountry.name} only
          </p>
        ) : null}
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {selectedCountry ? `Revenue in ${selectedCountry.name}` : "Revenue by country"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <RevenueBarChart
            items={countries.map((c) => ({
              key: c.country_code,
              label: getCountry(c.country_code)?.name ?? c.country_code,
              revenue_usd: c.revenue_usd,
              href: `/countries/${c.country_code}`,
            }))}
          />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Country</TableHead>
                <TableHead className="text-right">Companies</TableHead>
                <TableHead className="text-right">Revenue (USD)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {countries.map((c) => {
                const country = getCountry(c.country_code);
                return (
                  <TableRow key={c.country_code}>
                    <TableCell>
                      <Link
                        to={`/countries/${c.country_code}`}
                        className="flex w-fit items-center gap-1.5 font-medium underline-offset-2 hover:underline"
                      >
                        <span>{country?.flag}</span>
                        <span>{country?.name ?? c.country_code}</span>
                      </Link>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{nf.format(c.companies)}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatRevenueUsd(c.revenue_usd, null)}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Top companies</CardTitle>
        </CardHeader>
        <CardContent>
          <TopCompaniesTable companies={topCompanies} showCountry={!selectedCountry} />
        </CardContent>
      </Card>

      <MethodologyNote />
    </div>
  );
}
