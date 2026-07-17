import { Link } from "react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { getCountry, type SortDir } from "~/lib/countries";
import type { UnifiedRow } from "~/lib/unified.server";
import { DataTableColumnHeader } from "~/components/data-table/column-header";

const EMPTY = <span className="text-muted-foreground">—</span>;

export function buildUnifiedColumns(sort: string, dir: SortDir): ColumnDef<UnifiedRow, unknown>[] {
  return [
    {
      id: "name",
      header: () => (
        <DataTableColumnHeader label="Name" sortKey="name" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const s = row.original.name ?? "";
        if (s === "") return EMPTY;
        return (
          <Link
            to={`/company/${row.original.country_code}/${encodeURIComponent(row.original.id)}`}
            className="block max-w-[26rem] truncate font-medium underline-offset-2 hover:underline"
            title={s}
          >
            {s}
          </Link>
        );
      },
    },
    {
      id: "industry",
      header: () => <DataTableColumnHeader label="Industry" currentSort={sort} currentDir={dir} />,
      cell: ({ row }) => {
        const code = row.original.industry_code;
        const label = row.original.industry_label;
        if (!code && !label) return EMPTY;
        return (
          <span className="flex max-w-[22rem] items-baseline gap-1.5">
            {code ? <span className="text-muted-foreground font-mono text-xs">{code}</span> : null}
            {label ? (
              <span className="truncate" title={String(label)}>
                {label}
              </span>
            ) : null}
          </span>
        );
      },
    },
    {
      id: "country",
      header: () => (
        <DataTableColumnHeader label="Country" sortKey="country" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const country = getCountry(row.original.country_code);
        if (!country) return row.original.country_code;
        return (
          <span className="flex items-center gap-1.5 whitespace-nowrap">
            <span>{country.flag}</span>
            <span>{country.name}</span>
          </span>
        );
      },
    },
  ];
}
