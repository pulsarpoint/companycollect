import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { DataTablePagination } from "~/components/data-table/pagination";
import { SeCompanyInfoCorrectionsFilterSheet } from "~/components/admin/se-company-info-filter-sheet";
import type { SortDir } from "~/lib/countries";
import type {
  SeCompanyInfoCorrectionFilterOptions,
  SeCompanyInfoCorrectionListRow,
  SeCompanyInfoCorrectionSortKey,
  SeInfoCorrectionStatus,
} from "~/lib/se-company-info-lists.server";
import type { SeCompanyInfoCorrectionsTableFilters } from "~/lib/se-company-info-filters";

export type { SeCompanyInfoCorrectionsTableFilters };

const STATUS_VARIANT: Record<
  SeInfoCorrectionStatus,
  "default" | "secondary" | "destructive" | "outline"
> = {
  applied: "default",
  pending: "secondary",
  stale: "destructive",
  undone: "outline",
};

/** Mirrors the review page's own rendering: override shows its description
 * (or "clear description" for a null one); approve/reject and undo show the
 * 8-char id of the suggestion or correction they act on, the same prefix the
 * review page's own correction rows use. */
function payloadSummary(row: SeCompanyInfoCorrectionListRow): string {
  if (row.correction_kind === "undo") {
    return `undo ${(row.supersedes_correction_id ?? "").slice(0, 8)}`;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(row.payload);
  } catch {
    parsed = null;
  }
  const obj = parsed !== null && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  if (row.correction_kind === "override_field") {
    const description = obj.description;
    return typeof description === "string" && description !== "" ? description : "clear description";
  }
  if (row.correction_kind === "approve_suggestion" || row.correction_kind === "reject_suggestion") {
    const suggestionId = typeof obj.suggestion_id === "string" ? obj.suggestion_id : "";
    return `suggestion ${suggestionId.slice(0, 8)}`;
  }
  return row.payload;
}

/** Every column sorts server-side, so the columns are built per render with
 * the sort the URL asked for. `sortKey` is typed against the ledger query's
 * own whitelist, so a header can never name a column the server would reject. */
function buildColumns(
  sort: string,
  dir: SortDir,
): ColumnDef<SeCompanyInfoCorrectionListRow, unknown>[] {
  const head = (label: string, sortKey: SeCompanyInfoCorrectionSortKey) => () => (
    <DataTableColumnHeader
      label={label}
      sortKey={sortKey}
      currentSort={sort}
      currentDir={dir}
    />
  );
  return [
    {
      id: "created_at",
      header: head("Decided", "created_at"),
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs whitespace-nowrap">
          {row.original.created_at}
        </span>
      ),
    },
    {
      id: "company_id",
      header: head("Company", "company_id"),
      cell: ({ row }) => {
        const id = row.original.company_id;
        // The row itself opens the review page; the id links to the company page.
        return (
          <Link
            to={`/company/se/${encodeURIComponent(id)}`}
            className="font-mono text-xs underline underline-offset-2"
          >
            {id}
          </Link>
        );
      },
    },
    {
      id: "correction_id",
      header: head("Id", "correction_id"),
      cell: ({ row }) => (
        <span className="font-mono text-xs">{row.original.correction_id.slice(0, 8)}</span>
      ),
    },
    {
      id: "correction_kind",
      header: head("Kind", "correction_kind"),
      cell: ({ row }) => <Badge variant="outline">{row.original.correction_kind}</Badge>,
    },
    {
      id: "payload",
      header: head("Payload", "payload"),
      cell: ({ row }) => {
        const summary = payloadSummary(row.original);
        return (
          <span className="block max-w-[22rem] truncate" title={summary}>
            {summary}
          </span>
        );
      },
    },
    {
      id: "reason",
      header: head("Reason", "reason"),
      cell: ({ row }) => (
        <span className="block max-w-[16rem] truncate" title={row.original.reason}>
          {row.original.reason}
        </span>
      ),
    },
    {
      id: "decided_by",
      header: head("Decided by", "decided_by"),
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs">{row.original.decided_by}</span>
      ),
    },
    {
      id: "status",
      header: head("Status", "status"),
      cell: ({ row }) => (
        <Badge variant={STATUS_VARIANT[row.original.status]}>{row.original.status}</Badge>
      ),
    },
  ];
}

export function SeCompanyInfoCorrectionsTable({
  rows,
  total,
  page,
  pageSize,
  sort,
  dir,
  filters,
  options,
}: {
  rows: SeCompanyInfoCorrectionListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
  filters: SeCompanyInfoCorrectionsTableFilters;
  options: SeCompanyInfoCorrectionFilterOptions;
}) {
  return (
    <div className="flex flex-col gap-4">
      <SeCompanyInfoCorrectionsFilterSheet
        filters={filters}
        view={{ sort, dir, pageSize }}
        options={options}
      />
      <DataTable
        columns={buildColumns(sort, dir)}
        data={rows}
        emptyText="No corrections match these filters."
        minWidthClassName="min-w-[64rem]"
        rowHref={(row) => `/admin/se/company/${encodeURIComponent(row.company_id)}/info`}
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="corrections" />
    </div>
  );
}
