import { Link } from "react-router";
import type { Route } from "./+types/country-contract-detail";
import { getCountry } from "~/lib/countries";
import {
  getContractDetail,
  type ContractWinnerRow,
  type SourceRecord,
} from "~/lib/contracts.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const detail = await getContractDetail(country, params.ref);
  if (!detail) throw new Response("Contract not found", { status: 404 });

  return { country, detail };
}

export function meta({ loaderData }: Route.MetaArgs) {
  const title = loaderData?.detail.rows[0]?.title ?? "Contract";
  return [{ title: `${title} – CompanyCollect Backoffice` }];
}

function money(v: number | null, currency: string) {
  if (v == null) return <span className="text-muted-foreground">—</span>;
  return (
    <span className="tabular-nums">
      {nf.format(v)}
      {currency ? ` ${currency}` : ""}
    </span>
  );
}

/** Source rows differ in shape by design, so they are rendered generically:
 * whatever the register publishes is what shows. Empty values are dropped
 * rather than printed as blank rows. */
function SourceFields({ fields }: { fields: SourceRecord }) {
  const entries = Object.entries(fields).filter(
    ([, v]) => v !== null && v !== "" && !(Array.isArray(v) && v.length === 0),
  );
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-2 sm:grid-cols-2 lg:grid-cols-3">
      {entries.map(([key, value]) => (
        <div key={key} className="flex flex-col gap-0.5 overflow-hidden">
          <dt className="text-muted-foreground text-xs">{key}</dt>
          <dd className="truncate text-sm" title={String(value)}>
            {typeof value === "object" ? JSON.stringify(value) : String(value)}
          </dd>
        </div>
      ))}
    </dl>
  );
}

export default function CountryContractDetail({ loaderData }: Route.ComponentProps) {
  const { country, detail } = loaderData;
  const first: ContractWinnerRow = detail.rows[0];

  // One entry per source the contract appears under, each with its own notice
  // id and its own document.
  const bySource = new Map<string, ContractWinnerRow[]>();
  for (const row of detail.rows) {
    const list = bySource.get(row.source) ?? [];
    list.push(row);
    bySource.set(row.source, list);
  }

  // Winners are per (source, winner), so the same company appearing in two
  // registers is one winner shown once, carrying both sources.
  const winners = new Map<
    string,
    { company_id: string; name: string; sources: Set<string>; amount: number | null; currency: string }
  >();
  for (const row of detail.rows) {
    const key = row.company_id !== "" ? `id:${row.company_id}` : `name:${row.winner_name}`;
    const existing = winners.get(key);
    if (existing) {
      existing.sources.add(row.source);
      if (existing.amount == null) {
        existing.amount = row.amount_original;
        existing.currency = row.currency;
      }
    } else {
      winners.set(key, {
        company_id: row.company_id,
        name: row.winner_name !== "" ? row.winner_name : row.company_id,
        sources: new Set([row.source]),
        amount: row.amount_original,
        currency: row.currency,
      });
    }
  }

  const noAmount = detail.rows.every(
    (r) => r.amount_original == null && r.notice_amount_original == null,
  );

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="self-start"
          nativeButton={false}
          render={<Link to={`/countries/${country.code}/contracts`} />}
        >
          ← {country.flag} {country.name} contracts
        </Button>
        <h1 className="text-2xl font-semibold tracking-tight">{first.title || "Untitled contract"}</h1>
        <div className="text-muted-foreground flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
          <span>{first.buyer_name}</span>
          {first.contract_date ? <span>· {first.contract_date}</span> : null}
          {[...bySource.keys()].map((s) => (
            <Badge key={s} variant="secondary">
              {s}
            </Badge>
          ))}
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Winners</CardTitle>
          <CardDescription>
            Companies named as winners on this contract. Non-winning tenderers
            are not published in the registers ingested here, so this is the
            full set of participants known — not the full set that bid.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table className="min-w-[36rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Company</TableHead>
                  <TableHead>Registered id</TableHead>
                  <TableHead>Seen in</TableHead>
                  <TableHead className="text-right">Awarded</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...winners.values()].map((w) => (
                  <TableRow key={w.company_id || w.name}>
                    <TableCell className="align-top">
                      {w.company_id !== "" ? (
                        <Link
                          to={`/company/${country.code}/${w.company_id}`}
                          className="underline underline-offset-2"
                        >
                          {w.name}
                        </Link>
                      ) : (
                        // No registry match, so there is no company page to
                        // link to. The name is still worth showing.
                        <span>{w.name}</span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground align-top tabular-nums">
                      {w.company_id || "—"}
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex flex-wrap gap-1">
                        {[...w.sources].map((s) => (
                          <Badge key={s} variant="outline">
                            {s}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell className="text-right align-top">
                      {money(w.amount, w.currency)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      {noAmount ? (
        <Alert>
          <AlertTitle>
            {first.directive_governed === "no"
              ? "No contract value is published for this contract"
              : first.directive_governed === "yes"
                ? "The award amount is published in TED, which is not loaded for this country"
                : "No contract value is available for this contract"}
          </AlertTitle>
          <AlertDescription>
            {first.directive_governed === "no"
              ? "It falls below the EU procurement thresholds, so it is published only in the national register — and that register publishes no monetary value in any of its 44 fields. No amount exists to load."
              : first.directive_governed === "yes"
                ? "EU procurement directives govern it, so the same contract is also published in TED with a per-winner awarded amount. TED has not been backfilled for this country, so the figure is missing here rather than missing at source."
                : "The register does not say whether EU procurement thresholds apply, so it is unknown whether an amount exists in TED."}
          </AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Sources</CardTitle>
          <CardDescription>
            Where this contract was published, and the document each record came
            from. A contract in more than one register appears once per register.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {[...bySource.entries()].map(([source, rows]) => (
            <div key={source} className="flex flex-col gap-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">{source}</Badge>
                <span className="text-muted-foreground text-sm tabular-nums">
                  {rows[0].source_notice_id}
                  {rows[0].source_lot_id ? ` · lot ${rows[0].source_lot_id}` : ""}
                </span>
                {rows[0].source_url !== "" ? (
                  <a
                    href={rows[0].source_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-sm underline underline-offset-2"
                  >
                    Open source document
                  </a>
                ) : (
                  <span className="text-muted-foreground text-sm">
                    Published as a bulk download, with no address per contract
                  </span>
                )}
              </div>
              <div className="text-muted-foreground flex flex-wrap gap-x-4 gap-y-1 text-xs">
                <span>
                  Awarded to this contract:{" "}
                  {money(
                    rows.reduce<number | null>(
                      (sum, r) => (r.amount_original == null ? sum : (sum ?? 0) + r.amount_original),
                      null,
                    ),
                    rows[0].currency,
                  )}
                  {/* Name the register field, so the figure can be checked
                      against the source rather than taken on trust. */}
                  {rows[0].value_source_field !== ""
                    ? ` (${rows[0].value_source_field})`
                    : ""}
                </span>
                <span>
                  Whole notice: {money(rows[0].notice_amount_original, rows[0].notice_currency)}
                  {rows[0].notice_value_source_field !== ""
                    ? ` (${rows[0].notice_value_source_field})`
                    : ""}
                </span>
                {rows[0].cpv_code ? <span>CPV {rows[0].cpv_code}</span> : null}
                {rows[0].agreement_type ? <span>{rows[0].agreement_type}</span> : null}
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      {detail.sourceRecords.map((record) => (
        <Card key={`${record.source}:${record.notice}`}>
          <CardHeader>
            <CardTitle className="text-base">
              {record.source} — record {record.notice}
            </CardTitle>
            <CardDescription>
              Everything this register publishes for the contract. Fields differ
              between registers by design.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Separator className="mb-4" />
            <SourceFields fields={record.fields} />
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
