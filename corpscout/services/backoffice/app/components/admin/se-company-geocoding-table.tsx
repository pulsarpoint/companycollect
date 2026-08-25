import type { ColumnDef } from "@tanstack/react-table";
import { Link, useNavigate } from "react-router";
import { Badge } from "~/components/ui/badge";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import type {
  SeCompanyGeocodingCounts,
  SeCompanyGeocodingListRow,
} from "~/lib/se-company-geocoding-list.server";
import {
  GEOCODE_LIST_FILTERS,
  GEOCODE_LIST_FILTER_LABELS,
  geocodeListSearch,
  type GeocodeListFilter,
  type GeocodeStatusClass,
} from "~/lib/se-company-geocoding-filters";

const nf = new Intl.NumberFormat("en-US");

/** One published address as a line a reader recognises: street, then "postal
 * code city". Mirrors se-company-address.tsx's own `displayAddress` (minus
 * care_of, which this list does not project) so the same address reads the
 * same way on both pages. */
function displayAddress(row: SeCompanyGeocodingListRow): string {
  const locality = [row.postal_code, row.city].filter((part) => part !== "").join(" ");
  return [row.street_address, locality].filter((part) => part !== "").join(", ") || "—";
}

const STATUS_BADGE_VARIANT: Record<
  GeocodeStatusClass,
  "secondary" | "outline" | "destructive"
> = {
  geocoded: "secondary",
  ambiguous: "outline",
  unmatched: "destructive",
  no_outcome: "outline",
};

/** `row.geocode_class` is computed SQL-side (GEOCODE_STATUS_CLASS_EXPR), not
 * re-derived here -- a status this tab has never seen cannot be classified
 * two different ways by two copies of the same multiIf. */
function GeocodeStatusBadge({ row }: { row: SeCompanyGeocodingListRow }) {
  return (
    <Badge
      variant={STATUS_BADGE_VARIANT[row.geocode_class]}
      title={row.geocode_status || "Never geocoded"}
    >
      {GEOCODE_LIST_FILTER_LABELS[row.geocode_class]}
    </Badge>
  );
}

function columns(): ColumnDef<SeCompanyGeocodingListRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <div className="flex flex-col gap-0.5">
          <Link
            to={`/company/SE/${encodeURIComponent(row.original.company_id)}`}
            className="font-medium underline underline-offset-2"
          >
            {row.original.legal_name}
          </Link>
          <span className="text-muted-foreground font-mono text-xs">
            SE {row.original.company_id}
          </span>
        </div>
      ),
    },
    {
      id: "address",
      header: "Address",
      cell: ({ row }) => (
        <span className="block max-w-[24rem] truncate" title={displayAddress(row.original)}>
          {displayAddress(row.original)}
        </span>
      ),
    },
    {
      id: "geocode_status",
      header: "Geocode status",
      cell: ({ row }) => <GeocodeStatusBadge row={row.original} />,
    },
  ];
}

/** Every class the tab shows, in the header strip -- the same numbers the
 * toggle below filters by, so the strip and the toggle can never disagree
 * about how many companies are in each bucket. */
function CountsStrip({ counts }: { counts: SeCompanyGeocodingCounts }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {(
        [
          ["With address", counts.total],
          ["Needs attention", counts.needsAttention],
          ["Geocoded", counts.geocoded],
          ["Ambiguous", counts.ambiguous],
          ["Unmatched", counts.unmatched],
          ["No outcome", counts.noOutcome],
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

/** The class toggle: one segmented control, URL-driven exactly the way the
 * Address quality tab's own Issue toggle is (ToggleGroup + useNavigate,
 * `preventScrollReset`) -- this tab's only filter dimension, so a full
 * Filters sheet (the pattern the company-info list uses for its many
 * independent filters) would be one lever in an otherwise empty drawer. */
function StatusToggle({ filter }: { filter: GeocodeListFilter }) {
  const navigate = useNavigate();
  const searchParams = useEffectiveSearchParams();
  return (
    <ToggleGroup
      value={[filter]}
      onValueChange={(values) => {
        const next = values.at(-1) as GeocodeListFilter | undefined;
        if (next) {
          navigate(geocodeListSearch(searchParams, next), { preventScrollReset: true });
        }
      }}
      variant="outline"
      size="sm"
      spacing={0}
      aria-label="Geocode status filter"
    >
      {GEOCODE_LIST_FILTERS.map((value) => (
        <ToggleGroupItem key={value} value={value}>
          {GEOCODE_LIST_FILTER_LABELS[value]}
        </ToggleGroupItem>
      ))}
    </ToggleGroup>
  );
}

export function SeCompanyGeocodingTable({
  rows,
  total,
  page,
  pageSize,
  filter,
  counts,
}: {
  rows: SeCompanyGeocodingListRow[];
  total: number;
  page: number;
  pageSize: number;
  filter: GeocodeListFilter;
  counts: SeCompanyGeocodingCounts;
}) {
  return (
    <div className="flex flex-col gap-4">
      <CountsStrip counts={counts} />
      <div className="max-w-full overflow-x-auto pb-1">
        <StatusToggle filter={filter} />
      </div>
      <DataTable
        columns={columns()}
        data={rows}
        emptyText="No companies match this filter."
        minWidthClassName="min-w-[48rem]"
        rowHref={(row) => `/company/SE/${encodeURIComponent(row.company_id)}`}
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
