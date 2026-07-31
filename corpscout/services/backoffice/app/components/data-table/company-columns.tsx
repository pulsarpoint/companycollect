import { Link } from "react-router";
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

function cellFor(
  col: CompanyColumn,
  country: CountryConfig,
  legalForms: Record<string, { en: string; original: string }> = {},
) {
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
        if (col.key === "legal_form") {
          // Sweden stores only the code, so the column read 51, 61, E-ORGFO.
          // company_entity_types carries the register's own wording and covers
          // 3,407,806 of its 3,407,809 companies; the code shows through for
          // whatever it does not.
          const code = value == null ? "" : String(value);
          if (code === "") return EMPTY;
          const entry = legalForms[code];
          // English leads where it exists; the register's own term is what a
          // reader checks it against, and stands alone until the translator
          // has been round.
          const shown = entry?.en || entry?.original || code;
          const original = entry?.original ?? "";
          return (
            <span
              className="block max-w-[14rem] truncate"
              title={[shown, original !== shown ? original : "", `(${code})`]
                .filter(Boolean)
                .join(" · ")}
            >
              {shown}
            </span>
          );
        }
        if (col.key === "name") {
          const s = value == null ? "" : String(value);
          if (s === "") return EMPTY;
          return (
            <Link
              to={`/company/${country.code}/${encodeURIComponent(String(row.original.id))}`}
              className="block max-w-[22rem] truncate font-medium underline-offset-2 hover:underline"
              title={s}
            >
              {s}
            </Link>
          );
        }
        return <span className="block max-w-[14rem] truncate">{text(value)}</span>;
    }
  };
}

/** "68.20 Rental and operating…" -> "Rental and operating…" */
function stripLeadingCode(label: string): string {
  return label.replace(/^\s*[0-9][0-9.\-/]*\s+/, "");
}

export function buildCompanyColumns(
  country: CountryConfig,
  sort: string,
  dir: SortDir,
  /** Legal-form code -> the register's own wording. Sweden stores only the
   * code, so without this its column reads 51, 61, E-ORGFO. */
  legalForms: Record<string, { en: string; original: string }> = {},
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
    cell: cellFor(col, country, legalForms),
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
        // A NACE description already begins with its own code — "68.20 Rental
        // and operating of own or leased real estate" — so printing the code
        // beside it showed the same number twice, once as 6820 and once as
        // 68.20. The column is for scanning what a company does; the code
        // stays on hover and on the company page.
        const text = label ? stripLeadingCode(String(label)) : String(code);
        return (
          <span
            className="block max-w-[20rem] truncate"
            title={[code, label].filter(Boolean).join(" · ")}
          >
            {text}
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
