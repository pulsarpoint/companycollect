import { useState } from "react";
import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/company-facts";
import { getCountry } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server";
import { getCompanyFacts, type FactRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.factsQuery) throw new Response("Not found", { status: 404 });
  const year = Number(params.year);
  if (!Number.isInteger(year) || year < 1900 || year > 2200) {
    throw new Response("Not found", { status: 404 });
  }
  const [names, facts] = await Promise.all([
    chQuery<{ name: string }>(
      `SELECT ${country.nameColumn} AS name FROM ${country.companiesTable}
       WHERE ${country.idColumn} = {id:String} LIMIT 1`,
      { id: params.id },
    ),
    getCompanyFacts(country, params.id, year),
  ]);
  if (names.length === 0) throw new Response("Company not found", { status: 404 });
  return { name: names[0].name, facts, year };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData?.name ?? params.id;
  return [{ title: `${name} – ${params.year} facts – CompanyCollect Backoffice` }];
}

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function FactValue({ fact }: { fact: FactRow }) {
  if (fact.amount_original != null || fact.amount_usd != null) {
    return (
      <div className="flex flex-col items-end">
        <span className="tabular-nums">
          {fact.amount_original == null ? "—" : nf.format(fact.amount_original)}
          {fact.currency ? ` ${fact.currency}` : ""}
        </span>
        <span className="text-muted-foreground text-xs tabular-nums">
          {fact.amount_usd == null ? "—" : `$${nf.format(fact.amount_usd)}`}
        </span>
      </div>
    );
  }
  if (fact.value_kind === "date" && fact.date_value) {
    return <span className="tabular-nums">{fact.date_value}</span>;
  }
  const text = fact.text_value ?? fact.raw_value;
  return <span className="break-words whitespace-pre-wrap">{text}</span>;
}

/** Compact rendering of the XBRL dimensions JSON: member local names only. */
function dimensionSummary(dimensions: string): string {
  if (!dimensions || dimensions === "{}") return "";
  try {
    const parsed = JSON.parse(dimensions) as Record<string, string>;
    return Object.values(parsed)
      .map((v) => v.split(":").pop() ?? v)
      .join(", ");
  } catch {
    return dimensions;
  }
}

export default function CompanyFacts({ loaderData, params }: Route.ComponentProps) {
  const { name, facts, year } = loaderData;
  const [filter, setFilter] = useState("");
  const needle = filter.trim().toLowerCase();
  const visible = needle
    ? facts.filter(
        (f) =>
          f.concept.toLowerCase().includes(needle) ||
          f.raw_value.toLowerCase().includes(needle) ||
          (f.text_value ?? "").toLowerCase().includes(needle),
      )
    : facts;
  const moneyCount = facts.filter((f) => f.amount_original != null || f.amount_usd != null).length;

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={`/company/${params.country}/${params.id}`} />}
        >
          <ArrowLeft className="size-4" />
          {name}
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-2xl font-semibold">Filing facts · {year}</h2>
        <Badge variant="outline">{facts.length} facts</Badge>
        {facts.length > 0 ? <Badge variant="outline">{moneyCount} monetary</Badge> : null}
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-4">
          <CardTitle className="text-base">
            Every value tagged in the source annual report, in document order
          </CardTitle>
          <Input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter concepts and values…"
            className="max-w-56"
          />
        </CardHeader>
        <CardContent>
          {facts.length === 0 ? (
            <p className="text-muted-foreground text-sm">
              No facts are loaded for this filing year yet. Facts for older
              filing years are being restored from the source archive — they
              appear here as soon as that load completes.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table className="min-w-[48rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead>Concept</TableHead>
                    <TableHead>Kind</TableHead>
                    <TableHead className="text-right">Value</TableHead>
                    <TableHead>Dimensions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visible.map((f, i) => (
                    <TableRow key={i}>
                      <TableCell className="max-w-72 break-words font-mono text-xs align-top">
                        {f.concept}
                      </TableCell>
                      <TableCell className="text-muted-foreground align-top text-xs">
                        {f.value_kind}
                      </TableCell>
                      <TableCell className="max-w-96 text-right align-top">
                        <FactValue fact={f} />
                      </TableCell>
                      <TableCell className="text-muted-foreground max-w-56 break-words align-top text-xs">
                        {dimensionSummary(f.dimensions)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              {needle && visible.length !== facts.length ? (
                <p className="text-muted-foreground mt-2 text-xs">
                  Showing {visible.length} of {facts.length} facts.
                </p>
              ) : null}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
