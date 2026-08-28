import { LandmarkIcon } from "lucide-react";
import { Area, Bar, CartesianGrid, ComposedChart, XAxis, YAxis } from "recharts";
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

const compactCount = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 2,
});

const signedPercent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 1,
  signDisplay: "always",
});

/** A signed, coloured return figure — green up, red down, muted em dash when
 * the series does not reach the window. */
function ReturnStat({
  label,
  value,
}: {
  label: string;
  value: number | null;
}) {
  const tone =
    value === null || value === 0
      ? "text-muted-foreground"
      : value > 0
        ? "text-emerald-600 dark:text-emerald-400"
        : "text-red-600 dark:text-red-400";
  return (
    <div className="flex flex-col">
      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </dt>
      <dd className={`tabular-nums font-medium ${tone}`}>
        {value === null ? "—" : signedPercent.format(value)}
      </dd>
    </div>
  );
}

/** The key-stats strip above the price chart: 52-week range and average
 * volume over the trailing 365 days, plus the headline returns. Every price
 * figure carries the lead currency; returns are adjusted-close based. */
function MarketStatStrip({
  stats,
  currency,
}: {
  stats: NonNullable<SeCompanyListed["stats"]>;
  currency: string;
}) {
  return (
    <dl className="grid grid-cols-2 gap-x-6 gap-y-3 border-b pb-4 text-sm sm:grid-cols-3 lg:grid-cols-6">
      <div className="flex flex-col">
        <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          52-week range
        </dt>
        <dd className="tabular-nums">
          {stats.low52w === null || stats.high52w === null ? (
            <span className="text-muted-foreground">—</span>
          ) : (
            <>
              {priceFormat.format(stats.low52w)} –{" "}
              {priceFormat.format(stats.high52w)}
              {currency === "" ? "" : ` ${currency}`}
            </>
          )}
        </dd>
      </div>
      <div className="flex flex-col">
        <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Avg volume (1Y)
        </dt>
        <dd className="tabular-nums">
          {stats.avgVolume === null ? (
            <span className="text-muted-foreground">—</span>
          ) : (
            compactCount.format(stats.avgVolume)
          )}
        </dd>
      </div>
      {stats.returns.map((entry) => (
        <ReturnStat key={entry.label} label={entry.label} value={entry.value} />
      ))}
    </dl>
  );
}

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
function TradedByYearCard({
  summaries,
}: {
  summaries: SeCompanyListed["summaries"];
}) {
  if (summaries.length === 0) return null;
  const cumulative = summaries.reduce((sum, row) => sum + row.traded_usd, 0);
  const first = summaries[summaries.length - 1].year;
  const last = summaries[0].year;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Trading by year</CardTitle>
        <CardDescription>
          One row per calendar year the company traded: that year's turnover
          across all venues (USD), the year-end close on the lead venue, and
          the venue count. The bottom row is the cumulative turnover for every
          year we hold.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Table className="min-w-[28rem]">
          <TableHeader>
            <TableRow>
              <TableHead>Year</TableHead>
              <TableHead className="text-right">Traded value (USD)</TableHead>
              <TableHead className="text-right">Year-end close</TableHead>
              <TableHead className="text-right">Venues</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {summaries.map((row) => (
              <TableRow key={row.year}>
                <TableCell className="font-medium tabular-nums">{row.year}</TableCell>
                <TableCell className="text-right tabular-nums">
                  ${compactUsd.format(row.traded_usd)}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {row.last_close === null
                    ? "—"
                    : `${priceFormat.format(row.last_close)} ${row.lead_currency}`}
                </TableCell>
                <TableCell className="text-right tabular-nums">{row.venues}</TableCell>
              </TableRow>
            ))}
            <TableRow className="border-t-2 font-medium">
              <TableCell>
                All years
                <span className="text-muted-foreground text-xs font-normal">
                  {" "}
                  ({first}–{last})
                </span>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                ${compactUsd.format(cumulative)}
              </TableCell>
              <TableCell className="text-right text-muted-foreground">—</TableCell>
              <TableCell className="text-right text-muted-foreground">—</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export function SeCompanyListedTab({
  listed,
}: {
  companyId: string;
  listed: SeCompanyListed;
}) {
  const { leis, symbols, summary, summaries, leadSymbolKey, prices, stats } =
    listed;

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
  // The summary names the lead currency; before the summary asset has run,
  // the lead line's own EODHD quote currency stands in.
  const currency = summary?.lead_currency ?? lead.quote_currency;
  const chartConfig = {
    close: {
      label: `Close${currency === "" ? "" : ` (${currency})`}`,
      color: "var(--chart-1)",
    },
    volume: {
      label: "Volume",
      color: "var(--muted-foreground)",
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
              {lead.symbol_name === "" ? null : (
                <span className="text-muted-foreground text-xs">
                  {lead.symbol_name}
                </span>
              )}
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
                      ({summary.year} turnover — see Trading by year)
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

      <TradedByYearCard summaries={summaries} />

      {prices.length < 2 ? null : (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {lead.ticker} · {exchangeLabel(lead.exchange_code)} — 5 years
              {currency === "" ? "" : ` · ${currency}`}
            </CardTitle>
            <CardDescription>
              Daily close and volume for the lead listing
              {currency === "" ? "" : ` in ${currency}`}, from EODHD end-of-day
              prices. Range, volume and returns cover the trailing windows;
              returns use the adjusted close.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {stats === null ? null : (
              <MarketStatStrip stats={stats} currency={currency} />
            )}
            <ChartContainer config={chartConfig} className="h-64 w-full">
              <ComposedChart data={prices}>
                <CartesianGrid vertical={false} />
                <XAxis
                  dataKey="price_date"
                  tickLine={false}
                  axisLine={false}
                  minTickGap={48}
                />
                <YAxis
                  yAxisId="close"
                  dataKey="close"
                  tickLine={false}
                  axisLine={false}
                  width={56}
                  domain={["auto", "auto"]}
                />
                {/* Hidden volume axis scaled so the bars stay a muted band in
                    the bottom quarter, under the price line. */}
                <YAxis
                  yAxisId="volume"
                  hide
                  domain={[0, (dataMax: number) => dataMax * 4]}
                />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar
                  yAxisId="volume"
                  dataKey="volume"
                  fill="var(--color-volume)"
                  fillOpacity={0.25}
                />
                <Area
                  yAxisId="close"
                  dataKey="close"
                  type="monotone"
                  stroke="var(--color-close)"
                  fill="var(--color-close)"
                  fillOpacity={0.15}
                  dot={false}
                />
              </ComposedChart>
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
            <Table className="min-w-[36rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Exchange</TableHead>
                  <TableHead>Currency</TableHead>
                  <TableHead>ISIN</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {symbols.map((symbol) => (
                  <TableRow
                    key={symbol.eodhd_symbol_key}
                    className={
                      symbol.is_delisted === 1 ? "text-muted-foreground" : undefined
                    }
                  >
                    <TableCell className="align-top font-medium">
                      <div className="flex flex-wrap items-center gap-2">
                        {symbol.ticker}
                        {symbol.eodhd_symbol_key === lead.eodhd_symbol_key ? (
                          <Badge variant="outline">lead</Badge>
                        ) : null}
                        {symbol.instrument_type === "" ||
                        symbol.instrument_type === "Common Stock" ? null : (
                          <Badge variant="secondary">
                            {symbol.instrument_type}
                          </Badge>
                        )}
                        {symbol.is_delisted === 1 ? (
                          <Badge variant="destructive">delisted</Badge>
                        ) : null}
                      </div>
                      {symbol.symbol_name === "" ? null : (
                        <div className="text-muted-foreground text-xs font-normal">
                          {symbol.symbol_name}
                        </div>
                      )}
                    </TableCell>
                    <TableCell className="align-top">
                      <Badge variant="outline">{symbol.exchange_code}</Badge>{" "}
                      <span className="text-muted-foreground text-xs">
                        {exchangeLabel(symbol.exchange_code)}
                      </span>
                    </TableCell>
                    <TableCell className="align-top tabular-nums">
                      {symbol.quote_currency === "" ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        symbol.quote_currency
                      )}
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
