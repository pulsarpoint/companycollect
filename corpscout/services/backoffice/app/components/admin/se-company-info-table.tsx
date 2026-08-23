import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { DataTablePagination } from "~/components/data-table/pagination";
import { LegalForm } from "~/components/admin/legal-form";
import { SeCompanyInfoFilterSheet } from "~/components/admin/se-company-info-filter-sheet";
import type { SortDir } from "~/lib/countries";
import type {
  SeCompanyInfoFilterOptions,
  SeCompanyInfoListCounts,
  SeCompanyInfoListRow,
  SeCompanyInfoSortKey,
} from "~/lib/se-company-info-lists.server";
import type { SeCompanyInfoTableFilters } from "~/lib/se-company-info-filters";

const nf = new Intl.NumberFormat("en-US");

export type { SeCompanyInfoTableFilters };

const ENTITY_LABELS: Record<string, string> = {
  legal: "Legal",
  sole: "Sole trader",
};

/** Every column sorts server-side, so the columns are built per render with
 * the sort the URL asked for. `sortKey` is typed against the query builder's
 * whitelist, so a header can never name a column the server would reject
 * (and silently fall back on). */
function buildColumns(
  sort: string,
  dir: SortDir,
): ColumnDef<SeCompanyInfoListRow, unknown>[] {
  const head = (label: string, sortKey: SeCompanyInfoSortKey) => () => (
    <DataTableColumnHeader
      label={label}
      sortKey={sortKey}
      currentSort={sort}
      currentDir={dir}
    />
  );
  return [
    {
      id: "company_id",
      header: head("Company", "company_id"),
      cell: ({ row }) => (
        // The id opens this company's info hub -- so does the row (rowHref
        // below); the hub is what links on to the public company page.
        <Link
          to={`/admin/se/company/${encodeURIComponent(row.original.company_id)}/info`}
          className="font-mono text-xs underline underline-offset-2"
        >
          {row.original.company_id}
        </Link>
      ),
    },
    {
      id: "legal_name",
      header: head("Legal name", "legal_name"),
      cell: ({ row }) => (
        <span className="block max-w-[16rem] truncate" title={row.original.legal_name}>
          {row.original.legal_name}
        </span>
      ),
    },
    {
      id: "status",
      header: head("Status", "status"),
      cell: ({ row }) => <Badge variant="outline">{row.original.status}</Badge>,
    },
    {
      id: "legal_form_code",
      header: head("Legal form", "legal_form_code"),
      // Sorted by the CODE (that is what INFO_SORT_COLUMNS orders on) but read
      // as its name: the Swedish one, with the English muted beside it and the
      // code itself on hover.
      cell: ({ row }) => (
        <LegalForm
          className="block max-w-[18rem] truncate text-xs"
          form={{
            code: row.original.legal_form_code,
            label_sv: row.original.legal_form_label_sv,
            label_en: row.original.legal_form_label_en,
          }}
        />
      ),
    },
    {
      id: "entity_type",
      header: head("Entity", "entity_type"),
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs">
          {ENTITY_LABELS[row.original.entity_type] ?? row.original.entity_type}
        </span>
      ),
    },
    {
      // Task 17: the only description fact this list carries. Whether the text
      // came from the model, a reviewer or one register is the detail page's
      // story, and it needs the sources beside it to be worth anything.
      id: "has_description",
      header: head("Description", "has_description"),
      cell: ({ row }) => (
        <Badge variant={row.original.has_description ? "default" : "outline"}>
          {row.original.has_description ? "yes" : "no"}
        </Badge>
      ),
    },
  ];
}

/** What the filtered list contains, in the list's own terms: how many
 * companies, and how many of them have a description. The model/review numbers
 * belong to the Pipeline page, which is where they can be acted on. */
function CountsStrip({ counts }: { counts: SeCompanyInfoListCounts }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {[
        ["Companies", counts.total],
        ["With description", counts.withDescription],
        ["Without description", counts.withoutDescription],
      ].map(([label, count]) => (
        <Badge key={label} variant="outline">
          {label}
          <span className="text-muted-foreground ml-1 tabular-nums">
            {nf.format(count as number)}
          </span>
        </Badge>
      ))}
    </div>
  );
}

export function SeCompanyInfoTable({
  rows,
  total,
  page,
  pageSize,
  sort,
  dir,
  counts,
  filters,
  options,
}: {
  rows: SeCompanyInfoListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
  counts: SeCompanyInfoListCounts;
  filters: SeCompanyInfoTableFilters;
  options: SeCompanyInfoFilterOptions;
}) {
  return (
    <div className="flex flex-col gap-4">
      <SeCompanyInfoFilterSheet
        filters={filters}
        view={{ sort, dir, pageSize }}
        options={options}
      />
      <CountsStrip counts={counts} />
      <DataTable
        columns={buildColumns(sort, dir)}
        data={rows}
        emptyText="No companies match these filters."
        minWidthClassName="min-w-[48rem]"
        rowHref={(row) => `/admin/se/company/${encodeURIComponent(row.company_id)}/info`}
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
