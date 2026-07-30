import { Link } from "react-router";
import type { Route } from "./+types/country-contract-detail";
import { getCountry, type CountryConfig } from "~/lib/countries";
import { humanizeFieldKey, splitFields } from "~/components/detail/fields";
import { maskPersonalSupplierId, supplierStatusLabel } from "~/lib/supplier-label";
import { BrContractRecord } from "~/components/detail/countries/br-contract";
import { TedNoticeRecord } from "~/components/detail/countries/ted-notice";
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

/**
 * A register's own record, for sources without a bespoke view yet.
 *
 * It printed the RAW column name as the label — `source_slug`, `resolved_at`,
 * `country_iso2` — and put lineage plumbing in the body beside real data. Both
 * helpers to fix that already existed and simply were not used here:
 * `splitFields` separates lineage (source_run_id, resolved_at, partition_key)
 * from what the register actually published, and `humanizeFieldKey` turns a
 * column name into a label.
 *
 * Still generic, and still a fallback. A register that earns a bespoke view gets
 * proper labels AND the source's own field names beside them, the way
 * BrContractRecord pairs "What was procured" with `objetoContrato` — this only
 * stops the fallback being unreadable.
 */
function SourceFields({ fields }: { fields: SourceRecord }) {
  const { visible, lineage } = splitFields(fields);
  const shown = visible.filter(
    ([, v]) => v !== null && v !== "" && !(Array.isArray(v) && v.length === 0),
  );
  return (
    <div className="flex flex-col gap-4">
      <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
        {shown.map(([key, value]) => {
          const text =
            typeof value === "object" ? JSON.stringify(value) : String(value);
          return (
            <div key={key} className="flex flex-col gap-0.5 overflow-hidden">
              <dt className="text-xs leading-tight">
                <span className="text-foreground">{humanizeFieldKey(key)}</span>
                <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
                  {key}
                </span>
              </dt>
              <dd className="text-sm break-words" title={text}>
                {text}
              </dd>
            </div>
          );
        })}
      </dl>
      {lineage.length > 0 ? (
        // Kept, not dropped: which run produced a row is worth being able to
        // check. Just not interleaved with the register's own data.
        <details className="text-muted-foreground text-xs">
          <summary className="cursor-pointer">Lineage</summary>
          <dl className="mt-2 grid grid-cols-1 gap-x-6 gap-y-1 sm:grid-cols-2 lg:grid-cols-3">
            {lineage
              .filter(([, v]) => v !== null && v !== "")
              .map(([key, value]) => (
                <div key={key} className="flex gap-1.5 overflow-hidden">
                  <dt className="font-mono">{key}</dt>
                  <dd className="truncate">{String(value)}</dd>
                </div>
              ))}
          </dl>
        </details>
      ) : null}
    </div>
  );
}

/**
 * Per-register rendering, not one generic grid.
 *
 * The six ingested procurement registers share ZERO domain columns, and
 * br_pncp_contracts overlaps every other one by 0-2.9%. There is no common
 * contract shape, so a generic renderer can only print `column_name: value` —
 * which is how a reader came to be shown
 * `{"id":1,"nome":"Contrato (termo inicial)"}` and a bare `E` for two different
 * code sets. Registers without a dedicated view fall back to the generic grid
 * until they get one.
 */
function RegisterRecord({
  source,
  fields,
}: {
  source: string;
  fields: SourceRecord;
}) {
  if (source === "brazil_pncp_procurement") {
    return <BrContractRecord fields={fields} />;
  }
  // One view for TED, not four: it is a single register serving se, fi, no and
  // ee, and its notices have the same shape in all of them.
  if (source === "ted_procurement") {
    return <TedNoticeRecord fields={fields} />;
  }
  return <SourceFields fields={fields} />;
}


/**
 * The buyer, as an organisation with its own page.
 *
 * A buyer is a company record like any other -- a ministry, a municipality, a
 * hospital trust -- and until now the contract page named it as plain text with
 * no way through. Linked only when a register actually holds it, so the link is
 * never a dead end: all 4,898 Brazilian buyers resolve, via the CNPJ's first
 * eight digits.
 */
function BuyerCard({
  country,
  first,
  buyer,
}: {
  country: CountryConfig;
  first: ContractWinnerRow;
  buyer: { company_id: string; country_code: string } | null;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Buyer</CardTitle>
        <CardDescription>
          The public body that awarded this contract.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-2 text-sm">
        <div>
          {buyer ? (
            <Link
              to={`/company/${buyer.country_code.toLowerCase()}/${buyer.company_id}`}
              className="font-medium underline underline-offset-2"
            >
              {first.buyer_name || buyer.company_id}
            </Link>
          ) : (
            <span className="font-medium">{first.buyer_name || "—"}</span>
          )}
        </div>
        {first.buyer_id ? (
          <div className="text-muted-foreground text-xs">
            <span className="tabular-nums">{first.buyer_id}</span>
            {buyer == null ? (
              // Said plainly rather than left as an unexplained absence of a link.
              <span className="ml-2">not matched to a company record</span>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
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
    {
      company_id: string;
      name: string;
      registered_id: string;
      match_status: string;
      sources: Set<string>;
      amount: number | null;
      currency: string;
    }
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
        registered_id: row.winner_registered_id,
        match_status: row.winner_match_status,
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

      {/* Two columns: what the register PUBLISHED on the left (where it came
          from, and the record itself), who it INVOLVES on the right. The
          register record is the long one, so it gets the taller column and the
          parties stay visible beside it rather than below the fold. */}
      {/* Two columns at lg: what the register PUBLISHED on the left (where it
          came from, and the record itself), who it INVOLVES on the right.

          On one column the order inverts to parties first, then the contract
          record, then the source: a reader on a phone wants who won and who
          paid before the provenance of the document. The right column is
          therefore FIRST in the DOM and ordered back at lg, and the Sources
          card carries its own order so it falls last on small screens
          without reversing the register records among themselves. */}
      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-2">
        <div className="flex flex-col gap-4 lg:order-2">
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
                        {/* The company id when matched, otherwise the id the
                            register published plus why it did not resolve. CPFs
                            are masked for display; stored verbatim. */}
                        {w.company_id ||
                          maskPersonalSupplierId(w.registered_id, country.code) ||
                          "—"}
                        {supplierStatusLabel(w.match_status) ? (
                          <Badge
                            variant="outline"
                            className="ml-2 px-1 py-0 text-[10px] font-normal"
                          >
                            {supplierStatusLabel(w.match_status)}
                          </Badge>
                        ) : null}
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
          <BuyerCard country={country} first={first} buyer={detail.buyer} />
        </div>
        <div className="flex flex-col gap-4 lg:order-1">
        <Card className="order-2 lg:order-1">
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
                  {/* Brazil's tipo_contrato is stored as PNCP's raw
                      {"id":n,"nome":"..."} JSON, so this generic chip printed the
                      blob. Splitting it into id + name is an ingest change across
                      116,226 rows, tracked separately; until then a blob is
                      suppressed here and the per-register view below renders the
                      name properly. */}
                  {rows[0].agreement_type && !rows[0].agreement_type.startsWith("{") ? (
                    <span>{rows[0].agreement_type}</span>
                  ) : null}
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
        {detail.sourceRecords.map((record) => (
          <Card
            key={`${record.source}:${record.notice}`}
            className="order-1 lg:order-2"
          >
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
              <RegisterRecord source={record.source} fields={record.fields} />
            </CardContent>
          </Card>
        ))}
        </div>
      </div>
    </div>
  );
}
