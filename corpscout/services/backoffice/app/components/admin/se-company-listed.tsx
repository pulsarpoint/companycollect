import { LandmarkIcon } from "lucide-react";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { SeCompanyListed } from "~/lib/se-company-listed.server";

/**
 * The company's public-market identity: a traded / not-traded verdict, its
 * current LEI(s), and every ESEF annual financial report filed under them.
 *
 * The verdict is "at least one ESEF filing", not "has an LEI": LEIs are
 * issued for derivatives reporting and much else, while an ESEF annual
 * financial report is filed only by issuers on an EU regulated market.
 */
export function SeCompanyListedTab({
  companyId,
  listed,
}: {
  companyId: string;
  listed: SeCompanyListed;
}) {
  const { leis, filings } = listed;
  if (leis.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <LandmarkIcon />
          </EmptyMedia>
          <EmptyTitle>No LEI recorded</EmptyTitle>
          <EmptyDescription>
            No current LEI links to this company in company_identifier, so
            there is no public-market identity to show. Nearly all Swedish
            companies are in this state -- an LEI is only ever obtained for
            financial-market activity.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  const traded = filings.length > 0;
  const financialsHref = `/company/se/${encodeURIComponent(companyId)}/financials`;
  return (
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center gap-2">
            <CardTitle className="text-base">
              {traded ? "Publicly traded" : "Not publicly traded"}
            </CardTitle>
            <Badge variant={traded ? "default" : "outline"}>
              {traded
                ? `${filings.length} ESEF filing${filings.length === 1 ? "" : "s"}`
                : "no ESEF filings"}
            </Badge>
          </div>
          <CardDescription>
            {traded
              ? "At least one ESEF annual financial report is filed under this company's LEI, which only issuers on an EU regulated market file."
              : "This company holds an LEI but no ESEF annual financial report is filed under it. An LEI alone does not mean a listing."}
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-1">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            LEI{leis.length === 1 ? "" : "s"}
          </span>
          <ul className="flex flex-col gap-1 text-sm">
            {leis.map((row) => (
              <li key={row.lei} className="flex flex-wrap items-center gap-2">
                <span className="font-mono">{row.lei}</span>
                {row.entity_status === "" ? null : (
                  <Badge variant="outline">{row.entity_status}</Badge>
                )}
                {row.registration_status === "" ? null : (
                  <Badge variant="outline">{row.registration_status}</Badge>
                )}
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>

      {traded ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">ESEF filings</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="overflow-x-auto">
              <Table className="min-w-[36rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead>Period end</TableHead>
                    <TableHead>Entity</TableHead>
                    <TableHead>Country</TableHead>
                    <TableHead>Added</TableHead>
                    <TableHead>Filing</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filings.map((filing) => (
                    <TableRow key={filing.fxo_id}>
                      <TableCell className="tabular-nums align-top whitespace-nowrap">
                        <Link
                          className="underline underline-offset-2"
                          to={financialsHref}
                        >
                          {filing.period_end}
                        </Link>
                      </TableCell>
                      <TableCell className="align-top">
                        {filing.entity_name}
                      </TableCell>
                      <TableCell className="align-top">
                        {filing.country}
                      </TableCell>
                      <TableCell className="tabular-nums align-top whitespace-nowrap">
                        {filing.date_added}
                      </TableCell>
                      <TableCell className="align-top">
                        <span className="text-muted-foreground font-mono text-xs">
                          {filing.fxo_id}
                        </span>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            <p className="text-muted-foreground text-xs">
              Each period links to the{" "}
              <Link className="underline underline-offset-2" to={financialsHref}>
                public financials page
              </Link>
              , where the ESEF source carries the extracted figures and the
              document reader.
            </p>
          </CardContent>
        </Card>
      ) : null}
    </section>
  );
}
