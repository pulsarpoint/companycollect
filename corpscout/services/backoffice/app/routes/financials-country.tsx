import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/financials-country";
import { getCountryFinancials } from "~/lib/financial-aggregates.server";
import { getCountry } from "~/lib/countries";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export async function loader({ params }: Route.LoaderArgs) {
  const data = await getCountryFinancials(params.country);
  if (!data) throw new Response("Not found", { status: 404 });
  return data;
}

export function meta({ params }: Route.MetaArgs) {
  const country = getCountry(params.country);
  const name = country?.name ?? params.country;
  return [{ title: `${name} financials – CompanyCollect Backoffice` }];
}

const nf = new Intl.NumberFormat("en-US");

export default function FinancialsCountry({ loaderData, params }: Route.ComponentProps) {
  const { totals, divisions, unmapped, topCompanies } = loaderData;
  const country = getCountry(params.country)!;

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button variant="ghost" size="sm" className="-ml-2" render={<Link to="/financials" />}>
          <ArrowLeft className="size-4" />
          Financials
        </Button>
      </div>

      <h2 className="flex items-center gap-2 text-2xl font-semibold">
        <span>{country.flag}</span>
        <span>{country.name}</span>
      </h2>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <Card size="sm">
          <CardHeader>
            <CardDescription>Companies</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold tabular-nums">{nf.format(totals.companies)}</p>
          </CardContent>
        </Card>
        <Card size="sm">
          <CardHeader>
            <CardDescription>Revenue (USD)</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold tabular-nums">
              {formatRevenueUsd(totals.revenue_usd, null)}
            </p>
          </CardContent>
        </Card>
        <Card size="sm">
          <CardHeader>
            <CardDescription>Latest FY</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold tabular-nums">{totals.latest_fiscal_year ?? "—"}</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Industries</CardTitle>
        </CardHeader>
        <CardContent>
          {divisions ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Industry</TableHead>
                  <TableHead className="text-right">Companies</TableHead>
                  <TableHead className="text-right">Revenue (USD)</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {divisions.map((d) => (
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
                {unmapped ? (
                  <TableRow className="text-muted-foreground">
                    <TableCell>{unmapped.label}</TableCell>
                    <TableCell className="text-right tabular-nums">{nf.format(unmapped.companies)}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatRevenueUsd(unmapped.revenue_usd, null)}
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          ) : (
            <p className="text-muted-foreground text-sm">
              Industry breakdown unavailable — no NACE mapping for this source yet.
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Top companies</CardTitle>
        </CardHeader>
        <CardContent>
          <TopCompaniesTable companies={topCompanies} showCountry={false} />
        </CardContent>
      </Card>

      <MethodologyNote />
    </div>
  );
}
