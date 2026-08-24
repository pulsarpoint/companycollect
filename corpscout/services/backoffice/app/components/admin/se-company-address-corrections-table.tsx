import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { EMPTY_VALUE } from "~/components/admin/definition-list";
import { DataTable } from "~/components/data-table/data-table";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { DataTablePagination } from "~/components/data-table/pagination";
// One sheet serves both correction ledgers -- the four filters are the ledger
// shape, and only the kinds differ, so the address ledger's enums are passed in.
import { SeCompanyInfoCorrectionsFilterSheet } from "~/components/admin/se-company-info-filter-sheet";
import type { SortDir } from "~/lib/countries";
import {
  SE_ADDRESS_CORRECTION_KINDS,
  SE_ADDRESS_CORRECTION_STATUSES,
  type SeAddressCorrectionStatus,
} from "~/lib/se-address-corrections";
import type {
  SeCompanyAddressCorrectionFilterOptions,
  SeCompanyAddressCorrectionListRow,
  SeCompanyAddressCorrectionSortKey,
} from "~/lib/se-company-address-lists.server";
import type { SeCompanyInfoCorrectionsTableFilters } from "~/lib/se-company-info-filters";

const STATUS_VARIANT: Record<
  SeAddressCorrectionStatus,
  "default" | "secondary" | "destructive" | "outline"
> = {
  applied: "default",
  pending: "secondary",
  stale: "destructive",
  undone: "outline",
};

/**
 * The decision in one line, as the Address tab itself would put it: an override
 * lists the fields it moved (a null reads as the clear it is), a reject says
 * what it claims, and an undo names the 8-char prefix of the correction it
 * cancels -- the same prefix every ledger row shows.
 */
function payloadSummary(row: SeCompanyAddressCorrectionListRow): string {
  if (row.correction_kind === "undo") {
    return `undo ${(row.supersedes_correction_id ?? "").slice(0, 8)}`;
  }
  if (row.correction_kind === "reject_address") {
    return "not an address of this company";
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(row.payload);
  } catch {
    parsed = null;
  }
  const obj =
    parsed !== null && typeof parsed === "object"
      ? (parsed as Record<string, unknown>)
      : {};
  if (row.correction_kind === "override_field") {
    const fields = Object.entries(obj)
      .filter(([name]) => name !== "address_key")
      .map(([name, value]) => (value === null ? `clear ${name}` : `${name} = ${String(value)}`));
    // A payload that names no field at all is malformed; show it raw rather
    // than an empty cell, since that is exactly what a reviewer needs to see.
    return fields.length === 0 ? row.payload : fields.join(", ");
  }
  return row.payload;
}

/** Every column sorts server-side, so the columns are built per render with the
 * sort the URL asked for. `sortKey` is typed against the ledger query's own
 * whitelist, so a header can never name a column the server would reject. */
function buildColumns(
  sort: string,
  dir: SortDir,
): ColumnDef<SeCompanyAddressCorrectionListRow, unknown>[] {
  const head = (label: string, sortKey: SeCompanyAddressCorrectionSortKey) => () => (
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
        // The row itself opens the Address tab; the id links to the company page.
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
      // The column the info ledger has no equivalent of: a company has several
      // addresses, so which one a correction decides is the first thing to know.
      id: "address_key",
      header: head("Address", "address_key"),
      cell: ({ row }) => {
        const key = row.original.address_key;
        // An undo names a correction, not an address, and a malformed row names
        // nothing at all -- neither has a card to link to.
        if (key === "") return EMPTY_VALUE;
        return (
          <Link
            to={`/admin/se/company/${encodeURIComponent(row.original.company_id)}/address`}
            className="font-mono text-xs underline underline-offset-2"
            title={key}
          >
            {key.slice(0, 8)}
          </Link>
        );
      },
    },
    {
      id: "payload",
      header: head("Decision", "payload"),
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

export function SeCompanyAddressCorrectionsTable({
  rows,
  total,
  page,
  pageSize,
  sort,
  dir,
  filters,
  options,
}: {
  rows: SeCompanyAddressCorrectionListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
  filters: SeCompanyInfoCorrectionsTableFilters;
  options: SeCompanyAddressCorrectionFilterOptions;
}) {
  return (
    <div className="flex flex-col gap-4">
      <SeCompanyInfoCorrectionsFilterSheet
        filters={filters}
        view={{ sort, dir, pageSize }}
        options={options}
        kinds={SE_ADDRESS_CORRECTION_KINDS}
        statuses={SE_ADDRESS_CORRECTION_STATUSES}
      />
      <DataTable
        columns={buildColumns(sort, dir)}
        data={rows}
        emptyText="No corrections match these filters."
        minWidthClassName="min-w-[68rem]"
        rowHref={(row) =>
          `/admin/se/company/${encodeURIComponent(row.company_id)}/address`
        }
      />
      <DataTablePagination
        total={total}
        page={page}
        pageSize={pageSize}
        itemsLabel="corrections"
      />
    </div>
  );
}
