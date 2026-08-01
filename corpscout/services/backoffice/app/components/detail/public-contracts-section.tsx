import type { PublicContractRow } from "~/lib/queries.server";
import type { ContractSummaryRow } from "~/lib/queries.server";
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
const summaryNf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

export function PublicContractsSection({
  contracts,
  summary = null,
}: {
  contracts: PublicContractRow[];
  /** Award history in one line. Optional: a country that declares no
   * contractSummaryQuery renders exactly what it rendered before. */
  summary?: ContractSummaryRow | null;
}) {
  if (contracts.length === 0) return null;

  return (
    <Card id="government-contracts">
      <CardHeader>
        <CardTitle className="text-base">Government contracts</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {summary ? (
          <div className="text-muted-foreground flex flex-wrap gap-x-6 gap-y-1 text-sm">
            <span>
              <span className="text-foreground tabular-nums">
                {summaryNf.format(Number(summary.award_count))}
              </span>{" "}
              awards
            </span>
            {summary.last_award_date === "" ? null : (
              <span>
                latest{" "}
                <span className="text-foreground tabular-nums">
                  {summary.last_award_date}
                </span>
              </span>
            )}
            {/* Only where the register publishes a per-winner figure. France
                publishes none, so printing a zero here would say this company
                won nothing. */}
            {summary.total_value_usd == null ? null : (
              <span>
                <span className="text-foreground tabular-nums">
                  ${summaryNf.format(summary.total_value_usd)}
                </span>{" "}
                across {summaryNf.format(Number(summary.valued_count))} valued
              </span>
            )}
            {summary.sources === "" ? null : <span>{summary.sources}</span>}
          </div>
        ) : null}
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
                    {c.amount_original != null || c.notice_amount_original == null ? (
                      <MoneyPair
                        original={c.amount_original}
                        usd={c.amount_usd}
                        currency={c.currency}
                      />
                    ) : (
                      // No per-winner figure from this source. The notice total
                      // is the whole procurement across every winner, so it is
                      // shown labelled rather than passed off as this
                      // company's share.
                      <div className="flex flex-col items-end gap-0.5">
                        <MoneyPair
                          original={c.notice_amount_original}
                          usd={c.notice_amount_usd}
                          currency={c.notice_currency}
                        />
                        <span className="text-muted-foreground text-xs">
                          whole notice
                        </span>
                      </div>
                    )}
                  </TableCell>
                  <TableCell className="align-top">
                    <div className="flex flex-col items-start gap-0.5">
                      <Badge variant="secondary">{c.source}</Badge>
                      {c.source_url === "" ? (
                        <span className="text-muted-foreground text-xs">{c.notice_ref}</span>
                      ) : (
                        <a
                          href={c.source_url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-muted-foreground hover:text-foreground text-xs underline underline-offset-2"
                        >
                          {c.notice_ref}
                        </a>
                      )}
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
          often published without a value, and some registers publish only a
          value for the whole notice rather than per winner.
        </p>
      </CardContent>
    </Card>
  );
}
