import type { ColumnDef } from "@tanstack/react-table";
import type { CompanyColumn, CountryConfig, SortDir } from "~/lib/countries";
import type { CompanyListRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { DataTableColumnHeader } from "~/components/data-table/column-header";

const EMPTY = <span className="text-muted-foreground">—</span>;

function text(value: unknown) {
  const s = value == null ? "" : String(value);
  if (s === "") return EMPTY;
  return s;
}

function cellFor(col: CompanyColumn) {
  return ({ row }: { row: { original: CompanyListRow } }) => {
    const value = row.original[col.key];
    switch (col.kind) {
      case "id":
        return (
          <span className="text-muted-foreground font-mono text-xs whitespace-nowrap">
            {text(value)}
          </span>
        );
      case "date":
        return <span className="tabular-nums whitespace-nowrap">{text(value)}</span>;
      case "status":
        return (
          <Badge variant={row.original.active ? "default" : "outline"}>
            {text(value)}
          </Badge>
        );
      default:
        if (col.key === "name") {
          const s = value == null ? "" : String(value);
          return (
            <span className="block max-w-[22rem] truncate font-medium" title={s}>
              {s === "" ? EMPTY : s}
            </span>
          );
        }
        return <span className="block max-w-[14rem] truncate">{text(value)}</span>;
    }
  };
}

export function buildCompanyColumns(
  country: CountryConfig,
  sort: string,
  dir: SortDir,
): ColumnDef<CompanyListRow, unknown>[] {
  const defs: ColumnDef<CompanyListRow, unknown>[] = country.columns.map((col) => ({
    id: col.key,
    header: () => (
      <DataTableColumnHeader
        label={col.label}
        sortKey={col.sortable ? col.key : undefined}
        currentSort={sort}
        currentDir={dir}
      />
    ),
    cell: cellFor(col),
  }));

  if (country.industryQuery) {
    const industryDef: ColumnDef<CompanyListRow, unknown> = {
      id: "industry",
      header: () => (
        <DataTableColumnHeader label="Industry" currentSort={sort} currentDir={dir} />
      ),
      cell: ({ row }) => {
        const code = row.original.industry_code;
        const label = row.original.industry_label;
        if (!code && !label) return EMPTY;
        return (
          <span className="flex max-w-[20rem] items-baseline gap-1.5">
            {code ? (
              <span className="text-muted-foreground font-mono text-xs">{code}</span>
            ) : null}
            {label ? (
              <span className="truncate" title={String(label)}>
                {label}
              </span>
            ) : null}
          </span>
        );
      },
    };
    // Insert industry right after the name column.
    const nameIndex = defs.findIndex((d) => d.id === "name");
    defs.splice(nameIndex + 1, 0, industryDef);
  }

  return defs;
}
