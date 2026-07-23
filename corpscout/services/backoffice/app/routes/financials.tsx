import { Link } from "react-router";
import type { Route } from "./+types/financials";
import { getGlobalFinancialOverview } from "~/lib/financial-aggregates.server";
import { getCountry } from "~/lib/countries";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { RevenueBarChart } from "~/components/financials/revenue-bar-chart";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export function meta(_: Route.MetaArgs) {
  return [{ title: "Financials – CompanyCollect Backoffice" }];
}

export async function loader() {
  return await getGlobalFinancialOverview();
}

const nf = new Intl.NumberFormat("en-US");

export default function Financials({ loaderData }: Route.ComponentProps) {
  const { countries, topDivisions, topCompanies } = loaderData;

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <h2 className="text-xl font-semibold">Financials</h2>

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
              href: `/countries/${c.country_code}`,
            }))}
          />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Country</TableHead>
                <TableHead className="text-right">Companies</TableHead>
                <TableHead className="text-right">Revenue (USD)</TableHead>
                <TableHead className="text-right">Latest FY</TableHead>
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
                    <TableCell className="text-right tabular-nums">
                      {c.latest_fiscal_year ?? "—"}
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
          <CardTitle className="text-base">Top industries</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <RevenueBarChart
            items={topDivisions.map((d) => ({
              key: d.division,
              label: d.label,
              revenue_usd: d.revenue_usd,
              href: `/financials/industry/${d.division}`,
            }))}
          />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Industry</TableHead>
                <TableHead className="text-right">Companies</TableHead>
                <TableHead className="text-right">Revenue (USD)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {topDivisions.map((d) => (
                <TableRow key={d.division}>
                  <TableCell>
                    <Link
                      to={`/financials/industry/${d.division}`}
                      className="font-medium underline-offset-2 hover:underline"
                    >
                      {d.label}
                    </Link>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{nf.format(d.companies)}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatRevenueUsd(d.revenue_usd, null)}
                  </TableCell>
                </TableRow>
              ))}
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
