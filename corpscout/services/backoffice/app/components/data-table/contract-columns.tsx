import { Link } from "react-router";
import type { ColumnDef } from "@tanstack/react-table";
import type { ContractSortKey, CountryContractListRow } from "~/lib/contracts.server";
import type { SortDir } from "~/lib/countries";
import { DataTableColumnHeader } from "~/components/data-table/column-header";

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

export function buildContractColumns(
  countryCode: string,
  sort: ContractSortKey,
  dir: SortDir,
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

  return [
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
        const { winner_name, winner_extra_count } = row.original;
        if (winner_name === "") return EMPTY;
        return (
          <span className="block max-w-[14rem] truncate" title={winner_name}>
            {winner_name}
            {winner_extra_count > 0 ? (
              <span className="text-muted-foreground"> +{winner_extra_count}</span>
            ) : null}
          </span>
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
      cell: ({ row }) => truncated(row.original.agreement_type, "max-w-[10rem]"),
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
}
