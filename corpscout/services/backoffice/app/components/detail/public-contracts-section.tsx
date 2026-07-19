import type { PublicContractRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function money(v: number | null) {
  return v == null ? <span className="text-muted-foreground">—</span> : nf.format(v);
}

/** Original value on top, USD equivalent muted beneath — both or whichever exists. */
function MoneyPair({
  original,
  usd,
  currency,
}: {
  original: number | null;
  usd: number | null;
  currency: string;
}) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      <span>
        {money(original)}
        {original != null && currency !== "" ? ` ${currency}` : ""}
      </span>
      <span className="text-muted-foreground text-xs">
        {usd == null ? "—" : `$${nf.format(usd)}`}
      </span>
    </div>
  );
}

/**
 * Public procurement contract wins, rendered identically for every country
 * from the canonical PublicContractRow shape. Which portals feed it (Hilma,
 * TED, …) is decided per country in its publicContractsQuery — this component
 * only labels the source.
 */
export function PublicContractsSection({
  contracts,
}: {
  contracts: PublicContractRow[];
}) {
  if (contracts.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Public contracts</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="overflow-x-auto">
          <Table className="min-w-[44rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Buyer</TableHead>
                <TableHead>Contract</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead>Source</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {contracts.map((c) => (
                <TableRow key={`${c.source}:${c.notice_ref}:${c.contract_date}:${c.title}`}>
                  <TableCell className="tabular-nums align-top whitespace-nowrap">
                    {c.contract_date}
                  </TableCell>
                  <TableCell className="align-top">{c.buyer_name}</TableCell>
                  <TableCell className="align-top">{c.title}</TableCell>
                  <TableCell className="text-right tabular-nums align-top">
                    <MoneyPair
                      original={c.amount_original}
                      usd={c.amount_usd}
                      currency={c.currency}
                    />
                  </TableCell>
                  <TableCell className="align-top">
                    <div className="flex flex-col items-start gap-0.5">
                      <Badge variant="secondary">{c.source}</Badge>
                      <span className="text-muted-foreground text-xs">{c.notice_ref}</span>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="text-muted-foreground text-xs">
          Contract award notices where this company is a named winner, from the
          national procurement portal and TED (EU). Framework agreements are
          often published without a value.
        </p>
      </CardContent>
    </Card>
  );
}
