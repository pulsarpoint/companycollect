import type { ColumnDef, OnChangeFn } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { EMPTY_VALUE } from "~/components/admin/definition-list";
import { DataTable } from "~/components/data-table/data-table";
import { DataTableColumnHeader } from "~/components/data-table/column-header";
import { DataTablePagination } from "~/components/data-table/pagination";
import { LegalForm } from "~/components/admin/legal-form";
import { SeCompanyInfoFilterSheet } from "~/components/admin/se-company-info-filter-sheet";
import type { SortDir } from "~/lib/countries";
import {
  NO_ROWS_SELECTED,
  selectedRowCount,
  type RowSelection,
} from "~/lib/row-selection";
import type {
  SeCompanyInfoFilterOptions,
  SeCompanyInfoListCounts,
  SeCompanyInfoListRow,
  SeCompanyInfoSortKey,
} from "~/lib/se-company-info-lists.server";
import {
  PROFILE_DATATYPES,
  PROFILE_SOURCES_LEGEND,
  profileSourceParts,
  type ProfileDatatypeKey,
  type SeCompanyInfoTableFilters,
} from "~/lib/se-company-info-filters";

const nf = new Intl.NumberFormat("en-US");

export type { SeCompanyInfoTableFilters };

const ENTITY_LABELS: Record<string, string> = {
  legal: "Legal",
  sole: "Sole trader",
};

/**
 * Which registers built this company's profile, one monospace letter each in
 * the catalog's canonical order (B, S, E, W). Monospace and one badge per
 * letter so the column scans vertically: an 'S' row and a 'BSEW' row line up,
 * and the legend above the table names every letter. Each badge carries its
 * source's full name as a tooltip, so the letters never have to be memorised.
 */
function ProfileSources({ profileSources }: { profileSources: string }) {
  const parts = profileSourceParts(profileSources);
  // Cannot happen while SCB is the register base, but a blank cell would read
  // as "unknown" rather than "none" -- say it with the shared em dash instead.
  if (parts.length === 0) return EMPTY_VALUE;
  return (
    <span className="flex gap-1">
      {parts.map((part) => (
        <Badge
          key={part.letter}
          variant="outline"
          className="px-1.5 font-mono text-xs"
          title={part.label}
        >
          {part.letter}
        </Badge>
      ))}
    </span>
  );
}

/**
 * One datatype's presence on this company: a check when the datatype's own
 * final table has a row for it, the shared em dash when it does not.
 *
 * The dash is `EMPTY_VALUE` itself rather than a second muted dash of its own,
 * so "nothing here" looks identical in this column and in every definition
 * list on the detail pages. Both states carry the column's name in the title
 * and in `data-presence`/`data-present`: four adjacent one-glyph cells are
 * unreadable to a screen reader (and indistinguishable to a test) otherwise.
 */
function PresenceCell({
  datatype,
  label,
  present,
}: {
  datatype: ProfileDatatypeKey;
  label: string;
  present: number;
}) {
  return (
    <span
      data-presence={datatype}
      data-present={present ? "yes" : "no"}
      title={`${label}: ${present ? "yes" : "no"}`}
    >
      {present ? "✓" : EMPTY_VALUE}
    </span>
  );
}

/**
 * The multi-select column, always the FIRST one: a checkbox per row, plus one
 * in the header that ticks -- or clears -- every row of the page being shown.
 *
 * Selection is TanStack's own row-selection model, keyed by `company_id`
 * (DataTable's `selection.getRowId`) rather than by row index, and its state
 * is owned by the route component. So the ids survive every search-param
 * navigation -- filter, sort, page -- and the header speaks only for the rows
 * it can see: `toggleAllPageRowsSelected` carries the rest of the selection
 * forward, so "select all" on page 2 can never drop what page 1 picked.
 */
export function selectionColumn(): ColumnDef<SeCompanyInfoListRow, unknown> {
  return {
    id: "select",
    header: ({ table }) => (
      <Checkbox
        aria-label="Select every company on this page"
        checked={table.getIsAllPageRowsSelected()}
        // TanStack's `getIsSomePageRowsSelected` is already false once the
        // whole page is selected, so the two states cannot both be claimed:
        // ticked when every row on the page is picked, mixed in between.
        indeterminate={table.getIsSomePageRowsSelected()}
        onCheckedChange={(checked) => table.toggleAllPageRowsSelected(checked)}
      />
    ),
    cell: ({ row }) => (
      // The whole row is a link (DataTable's `rowHref`), and the escape hatch
      // that lets inner controls keep their own behaviour looks for
      // `a, button, input, ...` -- which a Base UI checkbox, a
      // `<span role="checkbox">`, is none of. Without stopping the event
      // here, ticking a box would navigate away to that company's page.
      <span
        data-slot="row-select"
        className="flex"
        onClick={(event) => event.stopPropagation()}
        onKeyDown={(event) => event.stopPropagation()}
      >
        <Checkbox
          // Named after the company, not "Select row": a column of boxes that
          // all read alike says nothing about which company each one picks --
          // to a screen reader, or to a test.
          aria-label={`Select ${row.original.legal_name}`}
          checked={row.getIsSelected()}
          onCheckedChange={(checked) => row.toggleSelected(checked)}
        />
      </span>
    ),
  };
}

/**
 * How many companies are picked -- across every page visited, not just this
 * one -- and the single control that empties the lot.
 *
 * Rendered as nothing at all while the count is zero: a "0 selected" chip
 * would be permanent noise on a list nobody has ticked. The count comes from
 * the selection itself rather than from the rows on screen, which is what
 * makes it the cross-page total.
 */
export function SelectionIndicator({
  selection,
  onSelectionChange,
}: {
  selection: RowSelection;
  onSelectionChange: OnChangeFn<RowSelection>;
}) {
  const count = selectedRowCount(selection);
  if (count === 0) return null;
  return (
    <div data-slot="selection-indicator" className="flex items-center gap-1 text-sm">
      <span className="font-medium tabular-nums">{nf.format(count)} selected</span>
      <span aria-hidden="true" className="text-muted-foreground">
        ·
      </span>
      <Button
        variant="ghost"
        size="sm"
        onClick={() => onSelectionChange(NO_ROWS_SELECTED)}
      >
        Clear
      </Button>
    </div>
  );
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
    // First, before the company id: the checkbox is what the eye starts a row
    // on, and every list that grows a bulk action puts it there.
    selectionColumn(),
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
    // One column per datatype, built FROM the catalog so the headers, the
    // sort keys and the projected columns are one list: a datatype cannot be
    // shown without being sortable, or sortable without being shown.
    ...PROFILE_DATATYPES.map(
      (datatype): ColumnDef<SeCompanyInfoListRow, unknown> => ({
        id: datatype.key,
        header: head(datatype.label, datatype.key),
        cell: ({ row }) => (
          <PresenceCell
            datatype={datatype.key}
            label={datatype.label}
            present={row.original[datatype.key]}
          />
        ),
      }),
    ),
    {
      // Which REGISTERS built the profile, across every datatype -- not where
      // the published text came from. Sorted server-side by the same derived
      // string it shows, so the rarer profiles (~3.1k Wikidata, ~400 ESEF
      // today) can be brought to the top of 3.5M rows in one click.
      id: "profile_sources",
      header: head("Sources", "profile_sources"),
      cell: ({ row }) => <ProfileSources profileSources={row.original.profile_sources} />,
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
  selection,
  onSelectionChange,
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
  /** The picked companies and the setter that owns them, both from the route
   * component -- this table is controlled, which is the whole reason a
   * selection outlives the page of rows it was made on. */
  selection: RowSelection;
  onSelectionChange: OnChangeFn<RowSelection>;
}) {
  return (
    <div className="flex flex-col gap-4">
      <SeCompanyInfoFilterSheet
        filters={filters}
        view={{ sort, dir, pageSize }}
        options={options}
      />
      <div className="flex flex-wrap items-center justify-between gap-2">
        <CountsStrip counts={counts} />
        {/* Beside the counts rather than above the table: it is a fact about
            the list, and it appears only once something is picked. */}
        <SelectionIndicator selection={selection} onSelectionChange={onSelectionChange} />
      </div>
      {/* One line, above the table, so the Sources letters can be read without
          hovering each badge. Built from the same catalog the letters are. */}
      <p className="text-muted-foreground text-xs">{PROFILE_SOURCES_LEGEND}</p>
      <DataTable
        columns={buildColumns(sort, dir)}
        data={rows}
        emptyText="No companies match these filters."
        // Four presence columns wider than it was: below this the header row
        // wraps and the checks stop lining up down the column, which is the
        // only thing that makes them scannable. The wrapper scrolls, not the page.
        minWidthClassName="min-w-[72rem]"
        rowHref={(row) => `/admin/se/company/${encodeURIComponent(row.company_id)}/info`}
        // Keyed by company_id, never by row index: page 2's third row is a
        // different company from page 1's third row, and the selection is
        // read long after both pages are gone.
        selection={{
          state: selection,
          onChange: onSelectionChange,
          getRowId: (row) => row.company_id,
        }}
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
