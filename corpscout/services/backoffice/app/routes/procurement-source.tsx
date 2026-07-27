import { Form, Link } from "react-router";
import type { Route } from "./+types/procurement-source";
import {
  countRows,
  getCoverage,
  getRegisterByPath,
  listSourceRecords,
  sourceSlugToPath,
  type SourceRow,
} from "~/lib/procurements.server";
import { formatMoneyField } from "~/lib/money";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US");
const PAGE_SIZE = 50;

export async function loader({ params, request }: Route.LoaderArgs) {
  const register = await getRegisterByPath(params.source);
  if (!register) throw new Response("Source not found", { status: 404 });

  const url = new URL(request.url);
  const table = url.searchParams.get("table") ?? register.notice_table;
  const page = Math.max(Number(url.searchParams.get("page") ?? "1"), 1);

  const [records, coverage, counts] = await Promise.all([
    listSourceRecords(register, {
      table,
      country: url.searchParams.get("country") ?? undefined,
      from: url.searchParams.get("from") ?? undefined,
      to: url.searchParams.get("to") ?? undefined,
      limit: PAGE_SIZE,
      offset: (page - 1) * PAGE_SIZE,
    }),
    getCoverage(register),
    countRows(register.source_tables),
  ]);

  return { register, records, coverage, counts, table, page };
}

export function meta({ loaderData }: Route.MetaArgs) {
  const name = loaderData?.register.register_name ?? "Source";
  return [{ title: `${name} – CompanyCollect Backoffice` }];
}

function cell(column: string, value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.length > 0 ? value.join(", ") : "—";
  if (typeof value === "object") return JSON.stringify(value);
  const money = formatMoneyField(column, value);
  if (money !== null) return money;
  const text = String(value);
  return text === "" ? "—" : text;
}

export default function ProcurementSource({ loaderData }: Route.ComponentProps) {
  const { register, records, coverage, counts, table, page } = loaderData;
  const path = sourceSlugToPath(register.source_slug);
  const pages = Math.max(Math.ceil(records.total / PAGE_SIZE), 1);
  const keyColumn = register.notice_key_column;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="self-start"
          nativeButton={false}
          render={<Link to="/procurements" />}
        >
          ← Procurement sources
        </Button>
        <h1 className="text-2xl font-semibold tracking-tight">
          {register.register_name}
        </h1>
        <div className="text-muted-foreground flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
          <span>{register.operator}</span>
          {register.country_codes.map((code) => (
            <Badge key={code} variant="secondary">
              {code}
            </Badge>
          ))}
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>About this register</CardTitle>
          <CardDescription>{register.coverage_description}</CardDescription>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2">
            <div>
              <dt className="text-muted-foreground text-xs">Grain</dt>
              <dd className="text-sm">{register.grain_description}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground text-xs">Licence</dt>
              <dd className="text-sm">{register.licence}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-muted-foreground text-xs">
                How we obtain it
              </dt>
              <dd className="text-sm">{register.retrieval_method}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-muted-foreground text-xs">Read from</dt>
              <dd className="truncate text-sm">
                <a
                  href={register.api_or_download_url}
                  className="underline underline-offset-2"
                  rel="noreferrer noopener"
                  target="_blank"
                >
                  {register.api_or_download_url}
                </a>
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground text-xs">Homepage</dt>
              <dd className="truncate text-sm">
                <a
                  href={register.homepage_url}
                  className="underline underline-offset-2"
                  rel="noreferrer noopener"
                  target="_blank"
                >
                  {register.homepage_url}
                </a>
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground text-xs">Open tenders</dt>
              <dd className="truncate text-sm">
                {register.open_tenders_url ? (
                  <a
                    href={register.open_tenders_url}
                    className="underline underline-offset-2"
                    rel="noreferrer noopener"
                    target="_blank"
                  >
                    {register.open_tenders_url}
                  </a>
                ) : (
                  // Not a missing link. Some registers report awards without
                  // ever advertising a tender, and the notes say which.
                  <span className="text-muted-foreground">
                    No single portal — see notes
                  </span>
                )}
              </dd>
            </div>
            {register.notes ? (
              <div className="sm:col-span-2">
                <dt className="text-muted-foreground text-xs">Notes</dt>
                <dd className="text-sm">{register.notes}</dd>
              </div>
            ) : null}
          </dl>
        </CardContent>
      </Card>

      {coverage.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>What we hold</CardTitle>
            <CardDescription>
              Per country, because a register serving several is loaded to a
              different depth in each. The caveat is about our coverage, not the
              register's scope.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            {coverage.map((row) => (
              <div key={row.country_code} className="flex flex-col gap-1">
                <div className="flex items-center gap-2 text-sm">
                  <Badge variant="outline">{row.country_code}</Badge>
                  <span className="tabular-nums">
                    {row.coverage_from ?? "—"} → {row.coverage_to ?? "—"}
                  </span>
                </div>
                <p className="text-muted-foreground text-xs">{row.caveat}</p>
              </div>
            ))}
          </CardContent>
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Records</CardTitle>
          <CardDescription>
            {nf.format(records.total)} rows in <code>{table}</code>, at this
            register's own grain and in its own columns — not the canonical
            projection the country views expose.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {register.source_tables.length > 1 ? (
            <div className="flex flex-wrap gap-2">
              {register.source_tables.map((name) => (
                <Button
                  key={name}
                  size="sm"
                  variant={name === table ? "default" : "outline"}
                  nativeButton={false}
                  render={<Link to={`/procurements/${path}?table=${name}`} />}
                >
                  {name}
                  <span className="text-muted-foreground ml-1 tabular-nums">
                    {nf.format(counts[name] ?? 0)}
                  </span>
                </Button>
              ))}
            </div>
          ) : null}

          <Form method="get" className="flex flex-wrap items-end gap-2">
            <input type="hidden" name="table" value={table} />
            {records.countryColumn ? (
              <label className="flex flex-col gap-1 text-xs">
                <span className="text-muted-foreground">Country</span>
                <Input name="country" placeholder="SE" className="w-24" />
              </label>
            ) : null}
            {records.dateColumn ? (
              <>
                <label className="flex flex-col gap-1 text-xs">
                  <span className="text-muted-foreground">
                    From ({records.dateColumn})
                  </span>
                  <Input name="from" type="date" className="w-40" />
                </label>
                <label className="flex flex-col gap-1 text-xs">
                  <span className="text-muted-foreground">To</span>
                  <Input name="to" type="date" className="w-40" />
                </label>
              </>
            ) : null}
            <Button type="submit" size="sm">
              Filter
            </Button>
          </Form>

          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  {records.columns.map((column) => (
                    <TableHead key={column} className="whitespace-nowrap">
                      {column}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {records.rows.map((row: SourceRow, index) => {
                  const key = keyColumn ? String(row[keyColumn] ?? "") : "";
                  return (
                    <TableRow key={`${key}-${index}`}>
                      {records.columns.map((column) => (
                        <TableCell
                          key={column}
                          className="max-w-[22rem] truncate align-top text-xs"
                          title={cell(column, row[column])}
                        >
                          {column === keyColumn && key !== "" ? (
                            <Link
                              to={`/procurements/${path}/${encodeURIComponent(key)}`}
                              className="underline underline-offset-2"
                            >
                              {key}
                            </Link>
                          ) : (
                            cell(column, row[column])
                          )}
                        </TableCell>
                      ))}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">
              Page {page} of {nf.format(pages)}
            </span>
            <div className="flex gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={page <= 1}
                nativeButton={false}
                render={
                  <Link to={`/procurements/${path}?table=${table}&page=${page - 1}`} />
                }
              >
                Previous
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={page >= pages}
                nativeButton={false}
                render={
                  <Link to={`/procurements/${path}?table=${table}&page=${page + 1}`} />
                }
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
