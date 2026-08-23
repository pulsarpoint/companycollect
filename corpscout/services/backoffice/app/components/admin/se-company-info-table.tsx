import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { DataTablePagination } from "~/components/data-table/pagination";
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

function sourceLabel(source: string): string {
  return source === "" ? "none" : source;
}

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
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs">
          {row.original.legal_form_code === "" ? "—" : row.original.legal_form_code}
        </span>
      ),
    },
    {
      id: "description_source",
      header: head("Source", "description_source"),
      cell: ({ row }) => (
        <Badge variant="secondary">{sourceLabel(row.original.description_source)}</Badge>
      ),
    },
    {
      id: "description_sources",
      header: head("Sources", "description_sources"),
      cell: ({ row }) => {
        const sources = row.original.description_sources;
        return (
          <span className="text-muted-foreground text-xs">
            {sources.length > 0 ? sources.join(", ") : "—"}
          </span>
        );
      },
    },
    {
      id: "description_language",
      header: head("Language", "description_language"),
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs">
          {row.original.description_language === "" ? "—" : row.original.description_language}
        </span>
      ),
    },
    {
      id: "description_snippet",
      header: head("Description", "description_snippet"),
      cell: ({ row }) => {
        const snippet = row.original.description_snippet;
        return (
          <span className="block max-w-[24rem] truncate" title={snippet}>
            {snippet === "" ? "—" : snippet}
          </span>
        );
      },
    },
    {
      id: "has_suggestion",
      header: head("Suggestion", "has_suggestion"),
      cell: ({ row }) => (
        <Badge variant={row.original.has_suggestion ? "default" : "outline"}>
          {row.original.has_suggestion ? "yes" : "no"}
        </Badge>
      ),
    },
    {
      id: "corrections_count",
      header: head("Corrections", "corrections_count"),
      cell: ({ row }) => <span className="tabular-nums">{row.original.corrections_count}</span>,
    },
    {
      id: "resolved_at",
      header: head("Resolved", "resolved_at"),
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs">{row.original.resolved_at}</span>
      ),
    },
  ];
}

function CountsStrip({ counts }: { counts: SeCompanyInfoListCounts }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {counts.bySource.map((entry) => (
        <Badge key={entry.source} variant="secondary">
          {sourceLabel(entry.source)}
          <span className="text-muted-foreground ml-1 tabular-nums">{nf.format(entry.count)}</span>
        </Badge>
      ))}
      <Badge variant="outline">
        Multi-source
        <span className="text-muted-foreground ml-1 tabular-nums">
          {nf.format(counts.multiSourceCount)}
        </span>
      </Badge>
      <Badge variant="outline">
        Pending model
        <span className="text-muted-foreground ml-1 tabular-nums">
          {nf.format(counts.pendingModelCount)}
        </span>
      </Badge>
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
        minWidthClassName="min-w-[72rem]"
        rowHref={(row) => `/admin/se/company/${encodeURIComponent(row.company_id)}/info`}
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
