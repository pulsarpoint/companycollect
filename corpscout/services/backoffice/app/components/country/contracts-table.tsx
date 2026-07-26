import { Link } from "react-router";
import type { CountryContractRow } from "~/lib/contracts.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Empty, EmptyDescription, EmptyTitle } from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const compactUsd = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
  style: "currency",
  currency: "USD",
});

export function CountryContractsTable({
  countryCode,
  contracts,
}: {
  countryCode: string;
  contracts: CountryContractRow[];
}) {
  if (contracts.length === 0) {
    return (
      <Empty>
        <EmptyTitle>No contracts yet</EmptyTitle>
        <EmptyDescription>
          This country has no ingested award notices. For countries with no
          national register loaded, that means TED has not been backfilled.
        </EmptyDescription>
      </Empty>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Government contracts</CardTitle>
        <CardDescription>
          Award notices, one row per contract rather than per winner. A contract
          published in both a national register and TED is shown once, carrying
          both sources. Open a contract for its winners and source documents.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table className="min-w-[52rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Buyer</TableHead>
                <TableHead>Contract</TableHead>
                <TableHead className="text-right">Winners</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead>Sources</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {contracts.map((c) => (
                <TableRow key={c.contract_ref}>
                  <TableCell className="align-top tabular-nums whitespace-nowrap">
                    {c.contract_date || "—"}
                  </TableCell>
                  <TableCell className="align-top">{c.buyer_name}</TableCell>
                  <TableCell className="align-top">
                    <Link
                      to={`/countries/${countryCode}/contracts/${encodeURIComponent(c.contract_ref)}`}
                      className="underline underline-offset-2"
                    >
                      {c.title || "Untitled contract"}
                    </Link>
                  </TableCell>
                  <TableCell className="text-right align-top tabular-nums">
                    {c.winner_count}
                  </TableCell>
                  <TableCell className="text-right align-top tabular-nums">
                    {c.amount_usd != null ? (
                      compactUsd.format(c.amount_usd)
                    ) : c.notice_amount_usd != null ? (
                      // No per-winner amount from this register. The notice
                      // total covers every winner, so it is labelled rather
                      // than passed off as the contract's award value.
                      <div className="flex flex-col items-end gap-0.5">
                        <span>{compactUsd.format(c.notice_amount_usd)}</span>
                        <span className="text-muted-foreground text-xs">whole notice</span>
                      </div>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </TableCell>
                  <TableCell className="align-top">
                    <div className="flex flex-wrap gap-1">
                      {c.sources.map((s) => (
                        <Badge key={s} variant="secondary">
                          {s.replace(/_procurement$/, "")}
                        </Badge>
                      ))}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
