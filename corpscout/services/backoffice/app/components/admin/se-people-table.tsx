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
import {
  PERSON_SOURCES,
  PERSON_STATUSES,
  personSourceLabel,
} from "~/lib/se-person-fields";
import {
  EMPTY_SE_PEOPLE_FILTERS,
  sePeopleHref,
  sePersonHref,
  type SePeopleFilters,
} from "~/lib/se-people-filters";
// Type-only: erased at build, so the component never drags ClickHouse into the
// client bundle (see CLAUDE.md, "Route modules and .server files").
import type {
  SePeopleCounts,
  SePeopleListRow,
} from "~/lib/se-people-list.server";

const nf = new Intl.NumberFormat("en-US");

/** Base UI's Select reserves "" for the unselected/placeholder state, so an explicit,
 * selectable "Any" needs a real sentinel value -- a value outside both catalogues, so
 * it can never collide with a real source or status. */
const ANY = "__any__";

function statusOptionLabel(status: string): string {
  return status.length === 0 ? status : status[0].toUpperCase() + status.slice(1);
}

/** The person's status, spelled the same way the `status` filter's three values are:
 * "active", "hidden" or "withdrawn". A published, non-hidden, non-withdrawn row with
 * an `inactive_reason` the catalogue does not (yet) name falls back to that reason
 * itself rather than a misleading fixed label. */
function personStatus(row: SePeopleListRow): string {
  if (row.active === 1) return "active";
  return row.inactive_reason === "" ? "inactive" : row.inactive_reason;
}

const STATUS_BADGE_VARIANT: Record<string, "secondary" | "outline" | "destructive"> = {
  active: "secondary",
  hidden: "outline",
  withdrawn: "destructive",
};

/** `first_year`-`last_year`, collapsed to one year when they match, blank when
 * neither is known. */
function yearsRange(row: SePeopleListRow): string {
  if (row.first_year === "" && row.last_year === "") return "";
  if (row.first_year === row.last_year) return row.first_year;
  return `${row.first_year || "?"}–${row.last_year || "?"}`;
}

/** A select whose options are a fixed catalogue, submitted through the surrounding
 * `<Form method="get">` by its `name` -- the same `name` + `defaultValue` pattern
 * `se-company-info-filter-sheet.tsx`'s `FilterSelect` uses, so Apply's one GET
 * navigation carries every field at once. */
function PeopleSelect({
  name,
  label,
  value,
  options,
  labelOf = (option: string) => option,
}: {
  name: string;
  label: string;
  value: string;
  options: readonly string[];
  labelOf?: (option: string) => string;
}) {
  return (
    <Select name={name} defaultValue={value === "" ? ANY : value}>
      <SelectTrigger className="w-full" size="sm" aria-label={label}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ANY}>Any</SelectItem>
        {options.map((option) => (
          <SelectItem key={option} value={option}>
            {labelOf(option)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function Field({ label, htmlFor, children }: { label: string; htmlFor?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label htmlFor={htmlFor} className="text-xs font-medium">
        {label}
      </Label>
      {children}
    </div>
  );
}

/**
 * The list's one filter bar: company, name, source, role, year and status, all
 * submitted together by Apply -- a plain GET `<Form>`, so the filters work with no
 * JavaScript state to keep in step and the result is bookmarkable. Applying a filter
 * deliberately drops `page` (a stale page number over a new result set is
 * meaningless); `pageSize` rides along as a hidden field so it survives.
 */
function PeopleFilterBar({ filters, pageSize }: { filters: SePeopleFilters; pageSize: number }) {
  const hasFilters = Object.values(filters).some((value) => value !== "");
  return (
    <Form method="get" className="flex flex-wrap items-end gap-2">
      <input type="hidden" name="pageSize" value={pageSize} />
      <Field label="Company" htmlFor="people-company">
        <Input
          id="people-company"
          name="company"
          defaultValue={filters.company}
          placeholder="Id or name"
          className="w-40"
        />
      </Field>
      <Field label="Name" htmlFor="people-name">
        <Input
          id="people-name"
          name="name"
          defaultValue={filters.name}
          placeholder="Display name"
          className="w-40"
        />
      </Field>
      <Field label="Source">
        <PeopleSelect
          name="source"
          label="Source"
          value={filters.source}
          options={PERSON_SOURCES}
          labelOf={personSourceLabel}
        />
      </Field>
      <Field label="Role" htmlFor="people-role">
        <Input
          id="people-role"
          name="role"
          defaultValue={filters.role}
          placeholder="Role code"
          className="w-36"
        />
      </Field>
      <Field label="Year" htmlFor="people-year">
        <Input id="people-year" name="year" defaultValue={filters.year} placeholder="YYYY" className="w-20" />
      </Field>
      <Field label="Status">
        <PeopleSelect
          name="status"
          label="Status"
          value={filters.status}
          options={PERSON_STATUSES}
          labelOf={statusOptionLabel}
        />
      </Field>
      <Button type="submit" variant="secondary">
        Apply
      </Button>
      {hasFilters ? (
        <Button
          variant="ghost"
          nativeButton={false}
          render={<Link to={sePeopleHref(EMPTY_SE_PEOPLE_FILTERS, 1, pageSize)} />}
        >
          Clear
        </Button>
      ) : null}
    </Form>
  );
}

/** The strip's three numbers -- Persons, Active, Companies -- under the SAME filter
 * (and, under a company-name filter, the same resolved ids) the page itself reads. */
function CountsStrip({ counts }: { counts: SePeopleCounts }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {(
        [
          ["Persons", counts.persons],
          ["Active", counts.active],
          ["Companies", counts.companies],
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

function columns(): ColumnDef<SePeopleListRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <div className="flex flex-col gap-0.5">
          <span className="font-medium">{row.original.legal_name || "—"}</span>
          <span className="text-muted-foreground font-mono text-xs">SE {row.original.company_id}</span>
        </div>
      ),
    },
    {
      id: "person",
      header: "Person",
      cell: ({ row }) => (
        <div className="flex flex-col gap-0.5">
          <span className="font-medium">{row.original.display_name}</span>
          <span className="flex flex-wrap gap-1">
            {row.original.birth_year === "" ? null : (
              <Badge variant="outline">b. {row.original.birth_year}</Badge>
            )}
            {row.original.wikidata_id === "" ? null : (
              <Badge variant="outline">{row.original.wikidata_id}</Badge>
            )}
          </span>
        </div>
      ),
    },
    {
      id: "roles",
      header: "Roles",
      cell: ({ row }) => {
        const years = yearsRange(row.original);
        return (
          <div className="flex flex-col gap-1">
            <span className="flex flex-wrap gap-1">
              {row.original.current_roles.map((code) => (
                <Badge key={code} variant="outline">
                  {code}
                </Badge>
              ))}
            </span>
            {years === "" ? null : <span className="text-muted-foreground text-xs">{years}</span>}
          </div>
        );
      },
    },
    {
      id: "sources",
      header: "Sources",
      cell: ({ row }) => (
        <div className="flex flex-wrap gap-1">
          {row.original.sources.map((source) => (
            <Badge key={source} variant="secondary">
              {personSourceLabel(source)}
            </Badge>
          ))}
        </div>
      ),
    },
    {
      id: "status",
      header: "Status",
      cell: ({ row }) => {
        const status = personStatus(row.original);
        return <Badge variant={STATUS_BADGE_VARIANT[status] ?? "outline"}>{status}</Badge>;
      },
    },
  ];
}

/**
 * `/admin/se/people`'s body: the counts strip, the filter bar, a note when a
 * company-name filter matched more companies than the page could carry, the server
 * page as a `DataTable` (each row linking into that company's People tab, the
 * selected person pre-picked), and the pagination footer.
 */
export function SePeopleTable({
  rows,
  counts,
  truncatedCompanies,
  page,
  pageSize,
  filters,
}: {
  rows: SePeopleListRow[];
  counts: SePeopleCounts;
  truncatedCompanies: boolean;
  page: number;
  pageSize: number;
  filters: SePeopleFilters;
}) {
  return (
    <div className="flex flex-col gap-4">
      <CountsStrip counts={counts} />
      <PeopleFilterBar filters={filters} pageSize={pageSize} />
      {truncatedCompanies ? (
        <p className="text-muted-foreground text-xs">
          Showing people from the first 200 matching companies.
        </p>
      ) : null}
      <DataTable
        columns={columns()}
        data={rows}
        emptyText="No people match these filters."
        minWidthClassName="min-w-[64rem]"
        rowHref={(row) => sePersonHref(row.company_id, row.person_key)}
      />
      <DataTablePagination total={counts.persons} page={page} pageSize={pageSize} itemsLabel="people" />
    </div>
  );
}
