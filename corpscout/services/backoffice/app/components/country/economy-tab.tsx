import { useState } from "react";
import type { CountryImfOutlook, CountryWorldBankSeries, CountryImfSeries } from "~/lib/country-statistics";
import { Badge } from "~/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import { EconomicPulseChart, ImfForecastChart } from "~/components/countries/country-statistics-charts";
import { EmptyData, formatImfValue, formatWorldBankValue } from "~/components/country/shared";

export function EconomyTab({
  worldBank,
  worldBankUpdatedDate,
  imf,
}: {
  worldBank: CountryWorldBankSeries[];
  worldBankUpdatedDate: string | null;
  imf: CountryImfOutlook;
}) {
  const [range, setRange] = useState<"5" | "10" | "max">("10");
  const latestYear = Math.max(
    ...worldBank.flatMap((series) => series.points.map((point) => point.year)),
  );
  const minYear = range === "max" ? undefined : latestYear - Number(range) + 1;

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Economic history</CardTitle>
          <CardDescription>
            Comparable World Bank annual measures; each series keeps its own observation year.
          </CardDescription>
          <CardAction>
            <ToggleGroup
              value={[range]}
              onValueChange={(values) => {
                const next = values.at(-1) as typeof range | undefined;
                if (next) setRange(next);
              }}
              variant="outline"
              size="sm"
              spacing={0}
              aria-label="Economic history range"
            >
              <ToggleGroupItem value="5">5Y</ToggleGroupItem>
              <ToggleGroupItem value="10">10Y</ToggleGroupItem>
              <ToggleGroupItem value="max">Max</ToggleGroupItem>
            </ToggleGroup>
          </CardAction>
        </CardHeader>
        <CardContent>
          <EconomicPulseChart series={worldBank} minYear={minYear} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Latest World Bank observations</CardTitle>
          <CardDescription>National macro indicators, shown without cross-source fallback.</CardDescription>
          {worldBankUpdatedDate ? (
            <CardAction>
              <Badge variant="outline">Updated {worldBankUpdatedDate}</Badge>
            </CardAction>
          ) : null}
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Indicator</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead className="text-right">Year</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {worldBank.map((series) => (
                <TableRow key={series.indicatorCode}>
                  <TableCell>
                    <p className="font-medium">{series.indicatorName}</p>
                    <p className="text-muted-foreground font-mono text-xs">
                      {series.indicatorCode}
                    </p>
                  </TableCell>
                  <TableCell className="text-right font-medium tabular-nums">
                    {formatWorldBankValue(series.indicatorCode, series.latest.value)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {series.latest.year}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>IMF outlook</CardTitle>
          <CardDescription>
            Latest World Economic Outlook vintage, with estimates kept distinct from actuals.
          </CardDescription>
          {imf.vintageDate ? (
            <CardAction>
              <Badge variant="outline">Vintage {imf.vintageDate}</Badge>
            </CardAction>
          ) : null}
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {imf.series.length > 0 ? (
            <>
              <ImfForecastChart series={imf.series} />
              <ImfOutlookTable series={imf.series} />
            </>
          ) : (
            <EmptyData
              title="IMF outlook is not loaded yet"
              description="The country page remains available while the IMF WEO asset is awaiting its first materialization."
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function ImfOutlookTable({ series }: { series: CountryImfSeries[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Indicator</TableHead>
          <TableHead className="text-right">Latest actual</TableHead>
          <TableHead className="text-right">Next estimate</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {series.map((item) => {
          const actual = item.points.filter((point) => !point.isEstimate).at(-1);
          const estimate = item.points.find((point) => point.isEstimate);
          return (
            <TableRow key={item.indicatorCode}>
              <TableCell>
                <p className="font-medium">{item.indicatorName}</p>
                <p className="text-muted-foreground font-mono text-xs">
                  {item.indicatorCode}
                </p>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {actual ? `${formatImfValue(item, actual.value)} · ${actual.year}` : "—"}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {estimate ? (
                  <span className="inline-flex items-center justify-end gap-2">
                    {formatImfValue(item, estimate.value)} · {estimate.year}
                    <Badge variant="secondary">Estimate</Badge>
                  </span>
                ) : (
                  "—"
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
