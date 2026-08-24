import type { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { EMPTY_VALUE } from "~/components/admin/definition-list";
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
      />
      <DataTablePagination total={total} page={page} pageSize={pageSize} itemsLabel="companies" />
    </div>
  );
}
