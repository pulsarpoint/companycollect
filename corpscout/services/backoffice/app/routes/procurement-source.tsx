import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import type { Route } from "./+types/procurement-source";
import {
  countRows,
  getCoverage,
  getFilterOptions,
  getRegisterByPath,
  listSourceRecords,
  matchCompanies,
  type SourceQuery,
  type SourceRow,
} from "~/lib/procurements.server";
import { sourceSlugToPath } from "~/lib/procurement-paths";
import { pickCompanyMatch } from "~/lib/company-match";
import { formatMoneyField } from "~/lib/money";
import { visibleColumns } from "~/lib/procurement-columns";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { ProcurementFilterSheet } from "~/components/procurements/filter-sheet";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

const nf = new Intl.NumberFormat("en-US");

export async function loader({ params, request }: Route.LoaderArgs) {
  const register = await getRegisterByPath(params.source);
  if (!register) throw new Response("Source not found", { status: 404 });

  const url = new URL(request.url);
  const q = (name: string) => url.searchParams.get(name) ?? "";
  const num = (name: string) => {
    const parsed = Number.parseFloat(q(name));
    return Number.isFinite(parsed) ? parsed : undefined;
  };
  const table = url.searchParams.get("table") ?? undefined;
  const page = Math.max(1, Number.parseInt(q("page") || "1", 10) || 1);
  const pageSize = Math.min(200, Math.max(10, Number.parseInt(q("pageSize") || "50", 10) || 50));

  const query: SourceQuery = {
    table,
    country: q("country"),
    from: q("from"),
    to: q("to"),
    buyer: q("buyer"),
    winner: q("winner"),
    noticeType: q("noticeType"),
    awardResult: q("awardResult"),
    valueMin: num("valueMin"),
    valueMax: num("valueMax"),
    limit: pageSize,
    offset: (page - 1) * pageSize,
  };

  const [records, filterOptions, coverage, counts] = await Promise.all([
    listSourceRecords(register, query),
    getFilterOptions(register, table),
    getCoverage(register),
    countRows(register.source_tables),
  ]);

  // Batch-resolve buyer/winner/supplier ids on this page to company pages.
  const idColumns = ID_NAME_PAIRS.map(([idCol]) => idCol).filter((c) =>
    records.columns.includes(c),
  );
  const ids = records.rows.flatMap((row) =>
    idColumns.map((c) => String(row[c] ?? "")).filter((v) => v !== ""),
  );
  const companyLinks = await matchCompanies(ids);

  return {
    register,
    records,
    filterOptions,
    coverage,
    counts,
    companyLinks,
    idColumns,
    page,
    pageSize,
    table: records.columns.length > 0 ? (table ?? register.notice_table) : register.notice_table,
    query,
  };
}

export function meta({ loaderData }: Route.MetaArgs) {
  const name = loaderData?.register.register_name ?? "Source";
  return [{ title: `${name} – CompanyCollect Backoffice` }];
}

/** Which id column names to the name column its register actually renders it
 * as. Pairs rather than a Record because an id can recur across registers
 * under a different name column: Hilma's buyer_business_id has no buyer_name,
 * only buyer_name_fi. */
const ID_NAME_PAIRS: [idColumn: string, nameColumn: string][] = [
  ["buyer_national_id", "buyer_name"], // TED
  ["buyer_org_number", "buyer_name"], // Doffin
  ["buyer_business_id", "buyer_name_fi"], // Hilma
  ["buyer_cnpj", "buyer_name"], // PNCP
  ["buyer_id_normalized", "buyer_name"], // UHM
  ["winner_national_id", "winner_name"], // TED
  ["winner_org_number", "winner_name"], // Doffin
  ["winner_business_id", "winner_name"], // Hilma (fi_hilma_notice_winners)
  ["supplier_id_normalized", "supplier_name"], // UHM
  ["supplier_cnpj", "supplier_name"], // PNCP
];

function cellText(column: string, value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.length > 0 ? value.join(", ") : "—";
  if (typeof value === "object") return JSON.stringify(value);
  const money = formatMoneyField(column, value);
  if (money !== null) return money;
  const text = String(value);
  return text === "" ? "—" : text;
}

function buildColumns(args: {
  columns: string[];
  keyColumn: string;
  path: string;
  companyLinks: Record<string, { country_code: string; company_id: string }[]>;
}): ColumnDef<SourceRow, unknown>[] {
  const { columns, keyColumn, path, companyLinks } = args;
  return visibleColumns(columns).map((column) => ({
    id: column,
    accessorFn: (row: SourceRow) => row[column],
    header: column,
    cell: ({ row }) => {
      const value = row.original[column];
      const text = cellText(column, value);
      if (column === keyColumn && text !== "—") {
        return (
          <Link
            to={`/procurements/${path}/${encodeURIComponent(text)}`}
            className="underline underline-offset-2"
          >
            {text}
          </Link>
        );
      }
      // Buyer/winner/supplier names link to the matched company page, but
      // only when the row's country picks out exactly one candidate:
      // national org-number formats collide across registers, so an id alone
      // is not enough (see ~/lib/company-match.ts).
      for (const [idCol, nameCol] of ID_NAME_PAIRS) {
        if (nameCol !== column) continue;
        const candidates = companyLinks[String(row.original[idCol] ?? "")];
        const countryColumn = idCol.startsWith("buyer_")
          ? "buyer_country"
          : idCol.startsWith("supplier_")
            ? "supplier_country_code"
            : "winner_country";
        const rawCountry = row.original[countryColumn];
        const rowCountry = typeof rawCountry === "string" ? rawCountry : null;
        const match = pickCompanyMatch(candidates, rowCountry);
        if (match) {
          return (
            <Link
              to={`/company/${match.country_code.toLowerCase()}/${encodeURIComponent(match.company_id)}`}
              className="underline underline-offset-2"
            >
              {text}
            </Link>
          );
        }
      }
      const isMoney = formatMoneyField(column, value) !== null;
      return (
        <span
          className={`block max-w-[22rem] truncate ${isMoney ? "text-right tabular-nums" : ""}`}
          title={text}
        >
          {text}
        </span>
      );
    },
  }));
}

export default function ProcurementSource({ loaderData }: Route.ComponentProps) {
  const {
    register,
    records,
    filterOptions,
    coverage,
    counts,
    companyLinks,
    page,
    pageSize,
    table,
    query,
  } = loaderData;
  const path = sourceSlugToPath(register.source_slug);
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

          <div className="flex items-center justify-between gap-2">
            <ProcurementFilterSheet
              values={{
                country: query.country ?? "",
                from: query.from ?? "",
                to: query.to ?? "",
                buyer: query.buyer ?? "",
                winner: query.winner ?? "",
                noticeType: query.noticeType ?? "",
                awardResult: query.awardResult ?? "",
                valueMin: query.valueMin != null ? String(query.valueMin) : "",
                valueMax: query.valueMax != null ? String(query.valueMax) : "",
              }}
              available={{
                country: records.filters.country !== null,
                date: records.filters.date !== null,
                buyer: records.filters.buyerName !== null,
                winner: records.filters.winnerName !== null || records.filters.winnerId !== null,
                noticeType: records.filters.noticeType !== null,
                awardResult: records.filters.awardResult !== null,
                usdValue: records.filters.usdValue !== null,
              }}
              options={filterOptions}
              table={table}
            />
          </div>
          <DataTable
            columns={buildColumns({ columns: records.columns, keyColumn, path, companyLinks })}
            data={records.rows}
            emptyText="No records match these filters."
          />
          <DataTablePagination total={records.total} page={page} pageSize={pageSize} itemsLabel="records" />
        </CardContent>
      </Card>
    </div>
  );
}
