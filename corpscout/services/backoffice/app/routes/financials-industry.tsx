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

export async function loader({ params }: Route.LoaderArgs) {
  const data = await getIndustryFinancials(params.division);
  if (!data) throw new Response("Not found", { status: 404 });
  return data;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const label = loaderData?.label;
  const title = label ? `${params.division} ${label}` : `Industry ${params.division}`;
  return [{ title: `${title} – CompanyCollect Backoffice` }];
}

const nf = new Intl.NumberFormat("en-US");

export default function FinancialsIndustry({ loaderData }: Route.ComponentProps) {
  const { division, label, countries, topCompanies } = loaderData;

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button variant="ghost" size="sm" className="-ml-2" render={<Link to="/financials" />}>
          <ArrowLeft className="size-4" />
          Financials
        </Button>
      </div>

      <h2 className="flex items-baseline gap-2 text-2xl font-semibold">
        <span className="text-muted-foreground font-mono text-base">{division}</span>
        <span>{label}</span>
      </h2>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Revenue by country</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <RevenueBarChart
            items={countries.map((c) => ({
              key: c.country_code,
              label: getCountry(c.country_code)?.name ?? c.country_code,
              revenue_usd: c.revenue_usd,
              href: `/financials/country/${c.country_code}`,
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
                        to={`/financials/country/${c.country_code}`}
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
          <TopCompaniesTable companies={topCompanies} showCountry />
        </CardContent>
      </Card>

      <MethodologyNote />
    </div>
  );
}
