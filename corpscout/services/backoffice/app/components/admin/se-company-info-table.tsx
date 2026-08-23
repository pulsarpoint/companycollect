import type { ColumnDef } from "@tanstack/react-table";
import { Form, Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
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
  SeCompanyInfoListCounts,
  SeCompanyInfoListRow,
} from "~/lib/se-company-info-lists.server";

const nf = new Intl.NumberFormat("en-US");

// Mirrors INFO_LIST_SOURCES in se-company-info-lists.server.ts (the values
// description_source actually takes). Kept as a local literal, not an
// import, because a route component may not pull a value -- only a type --
// out of a `.server` module (see CLAUDE.md): react-router's build strips
// server-only modules from the client bundle entirely.
const INFO_LIST_SOURCES = ["scb", "wikidata", "esef", "llm", "reviewed", "none"] as const;

export interface SeCompanyInfoTableFilters {
  companyId: string;
  name: string;
  source: string;
  multi: boolean;
  entity: string;
  corrected: boolean;
}

function sourceLabel(source: string): string {
  return source === "" ? "none" : source;
}

const COLUMNS: ColumnDef<SeCompanyInfoListRow, unknown>[] = [
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
    id: "legal_name",
    header: "Legal name",
    accessorFn: (row) => row.legal_name,
    cell: ({ row }) => (
      <span className="block max-w-[16rem] truncate" title={row.original.legal_name}>
        {row.original.legal_name}
      </span>
    ),
  },
  {
    id: "status",
    header: "Status",
    cell: ({ row }) => <Badge variant="outline">{row.original.status}</Badge>,
  },
  {
    id: "description_source",
    header: "Source",
    cell: ({ row }) => <Badge variant="secondary">{sourceLabel(row.original.description_source)}</Badge>,
  },
  {
    id: "description_sources",
    header: "Sources",
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
    id: "description_snippet",
    header: "Description",
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
    header: "Suggestion",
    cell: ({ row }) => (
      <Badge variant={row.original.has_suggestion ? "default" : "outline"}>
        {row.original.has_suggestion ? "yes" : "no"}
      </Badge>
    ),
  },
  {
    id: "corrections_count",
    header: "Corrections",
    cell: ({ row }) => <span className="tabular-nums">{row.original.corrections_count}</span>,
  },
  {
    id: "resolved_at",
    header: "Resolved",
    cell: ({ row }) => (
      <span className="text-muted-foreground text-xs">{row.original.resolved_at}</span>
    ),
  },
];

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

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-medium">{label}</Label>
      {children}
    </div>
  );
}

function FilterForm({ filters }: { filters: SeCompanyInfoTableFilters }) {
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
      <Field label="Name">
        <Input
          name="name"
          defaultValue={filters.name}
          className="w-48"
          placeholder="Legal name contains"
        />
      </Field>
      <Field label="Source">
        <Select name="source" defaultValue={filters.source === "" ? undefined : filters.source}>
          <SelectTrigger className="w-32" size="sm">
            <SelectValue placeholder="Any" />
          </SelectTrigger>
          <SelectContent>
            {INFO_LIST_SOURCES.map((source) => (
              <SelectItem key={source} value={source}>
                {source}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
      <Field label="Entity">
        <Select name="entity" defaultValue={filters.entity === "" ? undefined : filters.entity}>
          <SelectTrigger className="w-32" size="sm">
            <SelectValue placeholder="Any" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="legal">Legal (10-digit)</SelectItem>
            <SelectItem value="sole">Sole trader (12-digit)</SelectItem>
          </SelectContent>
        </Select>
      </Field>
      <label className="flex items-center gap-2 pb-1.5 text-sm">
        <Checkbox name="multi" value="1" defaultChecked={filters.multi} />
        Multi-source
      </label>
      <label className="flex items-center gap-2 pb-1.5 text-sm">
        <Checkbox name="corrected" value="1" defaultChecked={filters.corrected} />
        Has corrections
      </label>
      <Button type="submit" size="sm">
        Apply filters
      </Button>
      <Button
        variant="ghost"
        size="sm"
        nativeButton={false}
        render={<Link to="/admin/se/company-info" />}
      >
        Clear
      </Button>
    </Form>
  );
}

export function SeCompanyInfoTable({
  rows,
  total,
  page,
  pageSize,
  counts,
  filters,
}: {
  rows: SeCompanyInfoListRow[];
  total: number;
  page: number;
  pageSize: number;
  counts: SeCompanyInfoListCounts;
  filters: SeCompanyInfoTableFilters;
}) {
  return (
    <div className="flex flex-col gap-4">
      <FilterForm filters={filters} />
      <CountsStrip counts={counts} />
      <DataTable
        columns={COLUMNS}
        data={rows}
        emptyText="No companies match these filters."
        minWidthClassName="min-w-[64rem]"
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
