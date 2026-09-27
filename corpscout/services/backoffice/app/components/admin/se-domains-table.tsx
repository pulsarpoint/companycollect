import type { ColumnDef } from "@tanstack/react-table";
import type { ReactNode } from "react";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { NO_DOMAINS_SELECTED, isDomainSelected, selectDomains } from "~/lib/domain-selection";
import { type SeDomainSelection } from "~/lib/se-domain-selection";
import { SeDomainsFilterSheet } from "~/components/admin/se-domains-filter-sheet";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import {
  seDomainHref,
  type SeDomainsFilters,
} from "~/lib/se-domains-filters";
// Type-only: erased at build, so the component never drags ClickHouse into the
// client bundle (see CLAUDE.md, "Route modules and .server files").
import type { SeDomainRow, SeDomainsCounts } from "~/lib/se-domains-list.server";

const nf = new Intl.NumberFormat("en-US");

export function confidencePercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

const ASSOCIATION_BADGE_VARIANT: Record<string, "default" | "secondary" | "outline" | "destructive"> = {
  connected: "default",
  uncertain: "outline",
  not_connected: "destructive",
};

/** "not_connected" -> "not connected": the URL keeps the stored value, the reader
 * sees words. */
function associationLabel(value: string): string {
  return value.replaceAll("_", " ");
}

function statusLabel(value: string): string {
  return value.length === 0 ? value : value[0].toUpperCase() + value.slice(1);
}

/**
 * `sources` holds one entry per source RECORD, so a source that evidenced the
 * domain several times (every ESEF filing, say) repeats. The badge shows it
 * once, with the repeat count -- the reader wants "which sources", not a
 * list of identical chips.
 */
export function sourceCounts(sources: readonly string[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const source of sources) counts.set(source, (counts.get(source) ?? 0) + 1);
  return [...counts];
}

/** A company's link target: its own Domains tab, where the row can be reviewed. */
export function seCompanyDomainsHref(companyId: string): string {
  return `/admin/se/company/${encodeURIComponent(companyId)}/domains`;
}

/** Rows, distinct domains, distinct companies and shared domains -- all under the
 * SAME filter the page itself reads. */
function CountsStrip({ counts }: { counts: SeDomainsCounts }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {(
        [
          ["Rows", counts.rows],
          ["Domains", counts.domains],
          ["Companies", counts.companies],
          ["Shared domains", counts.shared],
        ] as const
      ).map(([label, count]) => (
        <Badge key={label} variant="outline">
          {label}
          <span className="text-muted-foreground ml-1 tabular-nums">{nf.format(count)}</span>
        </Badge>
      ))}
    </div>
  );
}

/**
 * The columns describing one (company, domain) row from the company's side --
 * everything but the domain itself. The list prepends the domain column; the
 * domain page, where the domain is the heading, shows exactly these.
 */
export function seDomainCompanyColumns(): ColumnDef<SeDomainRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <div className="flex flex-col gap-0.5">
          <Link
            to={seCompanyDomainsHref(row.original.company_id)}
            className="font-medium underline-offset-4 hover:underline"
          >
            {row.original.legal_name || row.original.company_id}
          </Link>
          <span className="text-muted-foreground font-mono text-xs">SE {row.original.company_id}</span>
        </div>
      ),
    },
    {
      id: "association",
      header: "Association",
      cell: ({ row }) => (
        <div className="flex flex-wrap gap-1">
          <Badge variant={ASSOCIATION_BADGE_VARIANT[row.original.association] ?? "outline"}>
            {associationLabel(row.original.association)}
          </Badge>
          {row.original.is_primary === 1 ? <Badge variant="secondary">primary</Badge> : null}
        </div>
      ),
    },
    {
      id: "confidence",
      header: () => <span className="block text-right">Confidence</span>,
      cell: ({ row }) => (
        <span className="block text-right tabular-nums">{confidencePercent(row.original.confidence)}</span>
      ),
    },
    {
      id: "sources",
      header: "Sources",
      cell: ({ row }) => (
        <div className="flex flex-wrap gap-1">
          {sourceCounts(row.original.sources).map(([source, count]) => (
            <Badge key={source} variant="secondary">
              {count > 1 ? `${source} ×${nf.format(count)}` : source}
            </Badge>
          ))}
        </div>
      ),
    },
    {
      id: "verification",
      header: "Verification",
      cell: ({ row }) => <span className="text-sm">{row.original.verification_status}</span>,
    },
    {
      id: "review",
      header: "Review",
      cell: ({ row }) => (
        <Badge variant={row.original.review_status === "unreviewed" ? "outline" : "secondary"}>
          {row.original.review_status}
        </Badge>
      ),
    },
    {
      id: "status",
      header: "Status",
      cell: ({ row }) =>
        row.original.active === 1 ? (
          <Badge variant="secondary">active</Badge>
        ) : (
          <Badge variant="outline">{row.original.inactive_reason || "inactive"}</Badge>
        ),
    },
  ];
}

function columns(): ColumnDef<SeDomainRow, unknown>[] {
  return [
    {
      id: "domain",
      header: "Domain",
      cell: ({ row }) => (
        <div className="flex flex-col gap-0.5">
          <Link
            to={seDomainHref(row.original.root_domain)}
            className="font-mono font-medium underline-offset-4 hover:underline"
          >
            {row.original.root_domain}
          </Link>
          {row.original.company_count > 1 ? (
            <span className="text-muted-foreground text-xs">
              {nf.format(row.original.company_count)} companies
            </span>
          ) : null}
        </div>
      ),
    },
    ...seDomainCompanyColumns(),
  ];
}

/**
 * `/admin/se/companies/domains`'s body: the counts strip, the filter bar, the
 * server page as a `DataTable` (each row opening the domain's own page) and the
 * pagination footer.
 */
export function SeDomainsTable({
  rows,
  counts,
  page,
  pageSize,
  filters,
  selection,
}: {
  rows: SeDomainRow[];
  counts: SeDomainsCounts;
  page: number;
  pageSize: number;
  filters: SeDomainsFilters;
  selection?: {
    value: SeDomainSelection;
    onChange: (selection: SeDomainSelection) => void;
    actions: ReactNode;
  };
}) {
  const pageDomains = [...new Set(rows.map((row) => row.root_domain))];
  const selectedOnPage = selection ? pageDomains.filter((domain) => isDomainSelected(selection.value, domain)).length : 0;
  const selectedCount = selection?.value.mode === "ids" ? selection.value.domains.length
    : selection ? Math.max(0, counts.domains - (selection.value.mode === "query" ? selection.value.excludedDomains.length : 0)) : 0;
  const domainColumns = columns();
  if (selection) domainColumns.unshift({
    id: "select",
    header: () => <Checkbox aria-label="Select every domain on this page"
      checked={pageDomains.length > 0 && selectedOnPage === pageDomains.length}
      indeterminate={selectedOnPage > 0 && selectedOnPage < pageDomains.length}
      disabled={pageDomains.length === 0}
      onCheckedChange={(checked) => selection.onChange(selectDomains(selection.value, pageDomains, checked))} />,
    cell: ({ row }) => <span className="flex" onClick={(event) => event.stopPropagation()} onKeyDown={(event) => event.stopPropagation()}>
      <Checkbox aria-label={`Select ${row.original.root_domain} (${row.original.company_id})`}
        checked={isDomainSelected(selection.value, row.original.root_domain)}
        onCheckedChange={(checked) => selection.onChange(selectDomains(selection.value, [row.original.root_domain], checked))} />
    </span>,
  });
  return (
    <div className="flex flex-col gap-4">
      <SeDomainsFilterSheet filters={filters} pageSize={pageSize} />
      <CountsStrip counts={counts} />
      {selection && <div className="flex flex-wrap items-center gap-2">
        <Button variant="outline" size="sm" disabled={counts.domains === 0}
          onClick={() => selection.onChange({ mode: "query", query: filters, excludedDomains: [] })}>
          Select all {nf.format(counts.domains)} matching domains
        </Button>
        {selectedCount > 0 && <>
          <span role="status" className="text-sm tabular-nums">{nf.format(selectedCount)} {selectedCount === 1 ? "domain" : "domains"} selected{selection.value.mode === "query" ? " across all pages" : ""}</span>
          <Button variant="ghost" size="sm" onClick={() => selection.onChange(NO_DOMAINS_SELECTED)}>Clear selection</Button>
        </>}
        {selection.actions}
      </div>}
      <DataTable
        columns={domainColumns}
        data={rows}
        emptyText="No domains match these filters."
        minWidthClassName="min-w-[72rem]"
        rowHref={(row) => seDomainHref(row.root_domain)}
      />
      <DataTablePagination total={counts.rows} page={page} pageSize={pageSize} itemsLabel="rows" />
    </div>
  );
}
