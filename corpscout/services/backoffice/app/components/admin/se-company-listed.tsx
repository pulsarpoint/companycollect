import { LandmarkIcon } from "lucide-react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
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

const compactUsd = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

/** EODHD exchange codes spelled out for the venues our companies actually
 * trade on; an unknown code falls back to itself rather than a blank. */
const EXCHANGE_NAMES: Record<string, string> = {
  ST: "Nasdaq Stockholm",
  LSE: "London Stock Exchange",
  F: "Frankfurt",
  STU: "Stuttgart",
  US: "US (NYSE/NASDAQ/OTC)",
  OL: "Oslo Børs",
  HE: "Nasdaq Helsinki",
  CO: "Nasdaq Copenhagen",
};

function exchangeLabel(code: string): string {
  return EXCHANGE_NAMES[code] ?? code;
}

const priceFormat = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
  minimumFractionDigits: 2,
});

/** The LEI line the verdict card and the not-traded state both show: holding
 * an LEI is identity context, not the trading verdict. */
function LeiList({ leis }: { leis: SeCompanyListed["leis"] }) {
  if (leis.length === 0) return null;
  return (
    <div className="flex flex-col gap-1">
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
    </div>
  );
}

/**
 * The company's public-market state, built on the EODHD market facts.
 *
 * The verdict is "an EODHD symbol resolved to this company": ESMA FIRDS gives
 * ISIN -> issuer LEI, GLEIF gives LEI -> registration number, the register
 * turns that into our company_id, and EODHD's ISIN closes the chain. ESEF
 * filings deliberately play NO part here — a filing is a reporting fact, not
 * trading information.
 */
export function SeCompanyListedTab({
  listed,
}: {
  companyId: string;
  listed: SeCompanyListed;
}) {
  const { leis, symbols, summary, leadSymbolKey, prices } = listed;

  if (symbols.length === 0) {
    return (
      <section className="flex flex-col gap-4">
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <LandmarkIcon />
            </EmptyMedia>
            <EmptyTitle>Not publicly traded</EmptyTitle>
            <EmptyDescription>
              No EODHD symbol resolves to this company — detection follows the
              deterministic ISIN → LEI → register chain into
              company_traded_symbols, and no listed line matched. Nearly all
              Swedish companies are in this state.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
        {leis.length === 0 ? null : (
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Identity</CardTitle>
              <CardDescription>
                The company holds an LEI, which alone does not mean a listing —
                LEIs are issued for derivatives reporting and much else.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <LeiList leis={leis} />
            </CardContent>
          </Card>
        )}
      </section>
    );
  }

  const lead =
    symbols.find((s) => s.eodhd_symbol_key === leadSymbolKey) ?? symbols[0];
  const instrumentCount = new Set(symbols.map((s) => s.isin)).size;
  const currency = summary?.lead_currency ?? "";
  const chartConfig = {
    close: {
      label: `Close${currency === "" ? "" : ` (${currency})`}`,
      color: "var(--chart-1)",
    },
  } satisfies ChartConfig;

  return (
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center gap-2">
            <CardTitle className="text-base">Publicly traded</CardTitle>
            <Badge variant="default">
              {symbols.length} listing{symbols.length === 1 ? "" : "s"}
            </Badge>
          </div>
          <CardDescription>
            {instrumentCount === 1
              ? "One instrument (ISIN)"
              : `${instrumentCount} instruments (distinct ISINs — share classes / depositary receipts)`}{" "}
            quoted as {symbols.length} listed line
            {symbols.length === 1 ? "" : "s"} across venues, resolved to this
            company through the ISIN → LEI → register identity chain.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm sm:grid-cols-4">
            <div className="flex flex-col">
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Lead listing
              </dt>
              <dd className="flex flex-wrap items-center gap-1 font-medium">
                <span>{lead.ticker}</span>
                <Badge variant="outline">{lead.exchange_code}</Badge>
              </dd>
            </div>
            <div className="flex flex-col">
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Last close
              </dt>
              <dd className="tabular-nums">
                {summary?.last_close == null ? (
                  <span className="text-muted-foreground">—</span>
                ) : (
                  <>
                    {priceFormat.format(summary.last_close)}{" "}
                    {summary.lead_currency}
                    {summary.last_day === "" ? null : (
                      <span className="text-muted-foreground text-xs">
                        {" "}
                        ({summary.last_day})
                      </span>
                    )}
                  </>
                )}
              </dd>
            </div>
            <div className="flex flex-col">
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Traded value
              </dt>
              <dd className="tabular-nums">
                {summary === null ? (
                  <span className="text-muted-foreground">—</span>
                ) : (
                  <>
                    ${compactUsd.format(summary.traded_usd)}
                    <span className="text-muted-foreground text-xs">
                      {" "}
                      ({summary.year} turnover)
                    </span>
                  </>
                )}
              </dd>
            </div>
            <div className="flex flex-col">
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Venues
              </dt>
              <dd className="tabular-nums">
                {summary === null ? (
                  <span className="text-muted-foreground">—</span>
                ) : (
                  summary.venues
                )}
              </dd>
            </div>
          </dl>
          {summary === null ? (
            <p className="text-muted-foreground text-xs">
              No market summary row yet — the summary asset runs separately
              from the symbol resolve, so quote and turnover can lag a new
              listing.
            </p>
          ) : null}
          <LeiList leis={leis} />
        </CardContent>
      </Card>

      {prices.length < 2 ? null : (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {lead.ticker} · {exchangeLabel(lead.exchange_code)} — 5 years
              {currency === "" ? "" : ` · ${currency}`}
            </CardTitle>
            <CardDescription>
              Daily close for the lead listing
              {currency === "" ? "" : ` in ${currency}`}, from EODHD end-of-day
              prices.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={chartConfig} className="h-56 w-full">
              <AreaChart data={prices}>
                <CartesianGrid vertical={false} />
                <XAxis
                  dataKey="price_date"
                  tickLine={false}
                  axisLine={false}
                  minTickGap={48}
                />
                <YAxis
                  dataKey="close"
                  tickLine={false}
                  axisLine={false}
                  width={56}
                  domain={["auto", "auto"]}
                />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Area
                  dataKey="close"
                  type="monotone"
                  stroke="var(--color-close)"
                  fill="var(--color-close)"
                  fillOpacity={0.15}
                  dot={false}
                />
              </AreaChart>
            </ChartContainer>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Listings</CardTitle>
          <CardDescription>
            One row per listed line. The SAME instrument (ISIN) quoted on
            several venues appears once per venue — cross-listings, not
            duplicates; a different ISIN is a different instrument (another
            share class or a depositary receipt).
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table className="min-w-[28rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Exchange</TableHead>
                  <TableHead>ISIN</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {symbols.map((symbol) => (
                  <TableRow key={symbol.eodhd_symbol_key}>
                    <TableCell className="align-top font-medium">
                      {symbol.ticker}
                      {symbol.eodhd_symbol_key === lead.eodhd_symbol_key ? (
                        <Badge variant="outline" className="ml-2">
                          lead
                        </Badge>
                      ) : null}
                    </TableCell>
                    <TableCell className="align-top">
                      {symbol.exchange_code}
                    </TableCell>
                    <TableCell className="align-top font-mono text-xs">
                      {symbol.isin}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </section>
  );
}
