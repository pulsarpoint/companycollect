import { Link } from "react-router";
import { getCountry } from "~/lib/countries";
import type { TopCompany } from "~/lib/financial-aggregates.server";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { Badge } from "~/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

/**
 * Shared top-companies list used on the financials landing, country, and
 * industry pages. `showCountry` adds a flag+code column for cross-country
 * views (landing, industry); country pages pass `false` since every row is
 * already scoped to one country.
 */
export function TopCompaniesTable({
  companies,
  showCountry,
}: {
  companies: TopCompany[];
  showCountry: boolean;
}) {
  if (companies.length === 0) {
    return <p className="text-muted-foreground text-sm">No companies with financial data.</p>;
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Company</TableHead>
          {showCountry ? <TableHead>Country</TableHead> : null}
          <TableHead className="text-right">Revenue (USD)</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {companies.map((c) => {
          const country = getCountry(c.country_code);
          return (
            <TableRow key={`${c.country_code}-${c.company_id}`}>
              <TableCell>
                <div className="flex flex-wrap items-center gap-2">
                  <Link
                    to={`/company/${c.country_code}/${encodeURIComponent(c.company_id)}`}
                    className="font-medium underline-offset-2 hover:underline"
                  >
                    {c.name}
                  </Link>
                  {c.excluded_from_sums ? (
                    <Badge variant="outline">foreign branch — parent entity accounts</Badge>
                  ) : null}
                </div>
              </TableCell>
              {showCountry ? (
                <TableCell>
                  <span className="flex items-center gap-1.5 whitespace-nowrap">
                    <span>{country?.flag}</span>
                    <span>{country?.code.toUpperCase() ?? c.country_code.toUpperCase()}</span>
                  </span>
                </TableCell>
              ) : null}
              <TableCell className="text-right tabular-nums">
                {formatRevenueUsd(c.revenue_usd, c.fiscal_year)}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
