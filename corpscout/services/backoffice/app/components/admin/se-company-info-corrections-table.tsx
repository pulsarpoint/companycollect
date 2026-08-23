import type { ColumnDef } from "@tanstack/react-table";
import { Form, Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import type {
  SeCompanyInfoCorrectionListRow,
  SeInfoCorrectionStatus,
} from "~/lib/se-company-info-lists.server";
import { SE_INFO_CORRECTION_KINDS } from "~/lib/se-info-corrections";

// Mirrors SE_INFO_CORRECTION_STATUSES in se-company-info-lists.server.ts.
// Kept as a local literal, not an import, because a route component may not
// pull a value -- only a type -- out of a `.server` module (see CLAUDE.md):
// react-router's build strips server-only modules from the client bundle
// entirely.
const SE_INFO_CORRECTION_STATUSES: readonly SeInfoCorrectionStatus[] = [
  "pending",
  "applied",
  "stale",
  "undone",
];

export interface SeCompanyInfoCorrectionsTableFilters {
  companyId: string;
  kind: string;
  status: string;
}

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

const COLUMNS: ColumnDef<SeCompanyInfoCorrectionListRow, unknown>[] = [
  {
    id: "created_at",
    header: "Decided",
    cell: ({ row }) => (
      <span className="text-muted-foreground text-xs whitespace-nowrap">
        {row.original.created_at}
      </span>
    ),
  },
  {
    id: "company_id",
    header: "Company",
    cell: ({ row }) => {
      const id = row.original.company_id;
      return (
        <div className="flex flex-col gap-0.5">
          <Link
            to={`/company/se/${encodeURIComponent(id)}`}
            className="font-mono text-xs underline underline-offset-2"
          >
            {id}
          </Link>
          <Link
            to={`/admin/se/company/${encodeURIComponent(id)}/info`}
            className="text-muted-foreground text-xs underline underline-offset-2"
          >
            Review
          </Link>
        </div>
      );
    },
  },
  {
    id: "correction_id",
    header: "Id",
    cell: ({ row }) => (
      <span className="font-mono text-xs">{row.original.correction_id.slice(0, 8)}</span>
    ),
  },
  {
    id: "correction_kind",
    header: "Kind",
    cell: ({ row }) => <Badge variant="outline">{row.original.correction_kind}</Badge>,
  },
  {
    id: "payload",
    header: "Payload",
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
    header: "Reason",
    cell: ({ row }) => (
      <span className="block max-w-[16rem] truncate" title={row.original.reason}>
        {row.original.reason}
      </span>
    ),
  },
  {
    id: "decided_by",
    header: "Decided by",
    cell: ({ row }) => <span className="text-muted-foreground text-xs">{row.original.decided_by}</span>,
  },
  {
    id: "status",
    header: "Status",
    cell: ({ row }) => (
      <Badge variant={STATUS_VARIANT[row.original.status]}>{row.original.status}</Badge>
    ),
  },
];

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-medium">{label}</Label>
      {children}
    </div>
  );
}

function FilterForm({ filters }: { filters: SeCompanyInfoCorrectionsTableFilters }) {
  return (
    <Form
      key={JSON.stringify(filters)}
      method="get"
      className="flex flex-wrap items-end gap-3"
    >
      <Field label="Company id">
        <Input
          name="companyId"
          defaultValue={filters.companyId}
          className="w-40"
          placeholder="Exact id"
        />
      </Field>
      <Field label="Kind">
        <Select name="kind" defaultValue={filters.kind === "" ? undefined : filters.kind}>
          <SelectTrigger className="w-44" size="sm">
            <SelectValue placeholder="Any" />
          </SelectTrigger>
          <SelectContent>
            {SE_INFO_CORRECTION_KINDS.map((kind) => (
              <SelectItem key={kind} value={kind}>
                {kind}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
      <Field label="Status">
        <Select name="status" defaultValue={filters.status === "" ? undefined : filters.status}>
          <SelectTrigger className="w-32" size="sm">
            <SelectValue placeholder="Any" />
          </SelectTrigger>
          <SelectContent>
            {SE_INFO_CORRECTION_STATUSES.map((status) => (
              <SelectItem key={status} value={status}>
                {status}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
      <Button type="submit" size="sm">
        Apply filters
      </Button>
      <Button
        variant="ghost"
        size="sm"
        nativeButton={false}
        render={<Link to="/admin/se/company-info/corrections" />}
      >
        Clear
      </Button>
    </Form>
  );
}

export function SeCompanyInfoCorrectionsTable({
  rows,
  total,
  page,
  pageSize,
  filters,
}: {
  rows: SeCompanyInfoCorrectionListRow[];
  total: number;
  page: number;
  pageSize: number;
  filters: SeCompanyInfoCorrectionsTableFilters;
}) {
  return (
    <div className="flex flex-col gap-4">
      <FilterForm filters={filters} />
      <DataTable
        columns={COLUMNS}
        data={rows}
        emptyText="No corrections match these filters."
        minWidthClassName="min-w-[64rem]"
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="corrections" />
    </div>
  );
}
