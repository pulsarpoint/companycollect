import { Link } from "react-router";
import type { ColumnDef } from "@tanstack/react-table";
import type { ContractSortKey, CountryContractListRow } from "~/lib/contracts.server";
import type { SortDir } from "~/lib/countries";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { Badge } from "~/components/ui/badge";
import { brContractType } from "~/components/detail/countries/br-contract";
import { CONTRACT_COLUMNS, type ContractColumnId } from "~/lib/contract-columns";
import { cpvSubjects } from "~/lib/cpv";
import {
  maskPersonalSupplierId,
  supplierPosition,
  supplierStatusLabel,
} from "~/lib/supplier-label";

const EMPTY = <span className="text-muted-foreground">—</span>;
const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function truncated(value: string, maxWidthClassName: string) {
  if (value === "") return EMPTY;
  return (
    <span className={`block truncate ${maxWidthClassName}`} title={value}>
      {value}
    </span>
  );
}

/**
 * The table's columns, restricted to the ones the reader chose.
 *
 * Every column is built and then selected from, rather than assembled
 * conditionally: the definitions stay readable, and `visible` — already in
 * canonical order from `parseContractColumns` — decides both membership and
 * order in one place.
 */
export function buildContractColumns(
  countryCode: string,
  sort: ContractSortKey,
  dir: SortDir,
  visible: ContractColumnId[] = CONTRACT_COLUMNS.map((c) => c.id),
): ColumnDef<CountryContractListRow, unknown>[] {
  function header(label: string, sortKey?: ContractSortKey, align?: "start" | "end") {
    return () => (
      <DataTableColumnHeader
        label={label}
        sortKey={sortKey}
        currentSort={sort}
        currentDir={dir}
        align={align}
      />
    );
  }

  const defs: ColumnDef<CountryContractListRow, unknown>[] = [
    {
      id: "date",
      header: header("Date", "date"),
      cell: ({ row }) => (
        <span className="tabular-nums whitespace-nowrap">{row.original.contract_date || "—"}</span>
      ),
    },
    {
      id: "buyer",
      header: header("Buyer", "buyer"),
      cell: ({ row }) => truncated(row.original.buyer_name, "max-w-[14rem]"),
    },
    {
      id: "winner",
      header: header("Winner", "winner"),
      cell: ({ row }) => {
        const { winner_name, winner_registered_id, winner_match_status, supplier_count } =
          row.original;
        if (winner_name === "") return EMPTY;
        const position = supplierPosition(supplier_count);
        const status = supplierStatusLabel(winner_match_status);
        // Masked for display only; stored verbatim. PNCP publishes 2,733
        // unmasked CPFs and RFB masks its own, so the same mask is applied.
        const id = maskPersonalSupplierId(winner_registered_id, countryCode);
        return (
          <div className="max-w-[16rem]">
            <span className="block truncate" title={winner_name}>
              {winner_name}
              {position ? (
                <span className="text-muted-foreground"> ({position})</span>
              ) : null}
            </span>
            {status ? (
              <span className="text-muted-foreground flex items-center gap-1.5 text-xs">
                <Badge variant="outline" className="px-1 py-0 text-[10px] font-normal">
                  {status}
                </Badge>
                {id ? <span className="tabular-nums">{id}</span> : null}
              </span>
            ) : null}
          </div>
        );
      },
    },
    {
      id: "title",
      header: header("Contract"),
      cell: ({ row }) => {
        const title = row.original.title || "Untitled contract";
        return (
          <Link
            to={`/countries/${countryCode}/contracts/${encodeURIComponent(row.original.contract_ref)}`}
            className="block max-w-[26rem] truncate underline-offset-2 hover:underline"
            title={title}
          >
            {title}
          </Link>
        );
      },
    },
    {
      id: "amount_original",
      header: header("Amount (original)", "amount_original", "end"),
      cell: ({ row }) => {
        const { amount_original, currency } = row.original;
        if (amount_original == null) return <div className="text-right">{EMPTY}</div>;
        return (
          <div className="text-right tabular-nums">
            {nf.format(amount_original)}
            {currency ? ` ${currency}` : ""}
          </div>
        );
      },
    },
    {
      id: "amount_usd",
      header: header("Amount (USD)", "amount_usd", "end"),
      cell: ({ row }) => {
        const { amount_usd } = row.original;
        if (amount_usd == null) return <div className="text-right">{EMPTY}</div>;
        return <div className="text-right tabular-nums">${nf.format(amount_usd)}</div>;
      },
    },
    {
      id: "agreement_type",
      header: header("Agreement type"),
      cell: ({ row }) =>
        // Brazil's agreement types are Portuguese (Empenho 50,191, Contrato
        // (termo inicial) 33,799, Outros 28,610, Termo de Adesão 153) on a page
        // labelled in English. Translated with the original kept beside it, the
        // same pairing used everywhere else. Other registers publish their own
        // wording and pass through -- ee and no publish none at all.
        truncated(
          countryCode === "br"
            ? (brContractType(row.original.agreement_type) ?? "")
            : row.original.agreement_type,
          "max-w-[14rem]",
        ),
    },
    {
      id: "cpv",
      header: header("CPV"),
      cell: ({ row }) => {
        // Decoded, never the bare code: 45213100 names nothing to a reader,
        // "Construction work" does. The exact code stays beside it because the
        // division label is a summary and the code is what the buyer stated.
        const subject = cpvSubjects(row.original.cpv_code)[0];
        if (!subject) return EMPTY;
        return (
          <div className="max-w-[16rem]" title={`${subject.label} (${subject.code})`}>
            <span className="block truncate">{subject.label}</span>
            <span className="text-muted-foreground font-mono text-[10px] tabular-nums">
              {subject.code}
            </span>
          </div>
        );
      },
    },
    {
      id: "source",
      header: header("Source"),
      cell: ({ row }) =>
        row.original.source_url === "" ? (
          EMPTY
        ) : (
          <a
            href={row.original.source_url}
            target="_blank"
            rel="noreferrer"
            className="text-muted-foreground hover:text-foreground text-xs underline underline-offset-2 whitespace-nowrap"
          >
            Open
          </a>
        ),
    },
  ];

  const byId = new Map(defs.map((d) => [d.id as string, d]));
  return visible
    .map((id) => byId.get(id))
    .filter((d): d is ColumnDef<CountryContractListRow, unknown> => d !== undefined);
}
