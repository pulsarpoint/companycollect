import type { ColumnDef } from "@tanstack/react-table";
import { Form, Link } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { SePeopleTaskRow } from "~/lib/se-people-tasks.server";
import type {
  SePeopleBolagsverketRow,
  SePeopleEsefRow,
  SePeopleFinalRow,
  SePeopleSourcePage,
  SePeopleWikidataRow,
} from "~/lib/se-people-sources.server";
import {
  SE_PEOPLE_SOURCE_TABS,
  sePeopleSourcesSearch,
  type SePeopleSourceFilters,
  type SePeopleSourceView,
} from "~/lib/se-people-sources";

function cell(value: string | number | null | undefined): string {
  return value === null || value === undefined || value === "" ? "—" : String(value);
}

/** Every tab's company_id cell: a link to the company page. Unlike the
 * People tab (whole row links to the person review page), the three source
 * tabs offer only this cell-level affordance -- a source row has no detail
 * page of its own to link the row to. */
function CompanyCell({ companyId }: { companyId: string }) {
  return (
    <Link
      to={`/company/SE/${encodeURIComponent(companyId)}`}
      className="font-mono text-xs underline underline-offset-2"
    >
      {companyId}
    </Link>
  );
}

function bolagsverketColumns(): ColumnDef<SePeopleBolagsverketRow, unknown>[] {
  return [
    {
      id: "company_id",
      header: "Company",
      cell: ({ row }) => <CompanyCell companyId={row.original.company_id} />,
    },
    { id: "full_name", header: "Name", accessorFn: (row) => row.full_name },
    { id: "first_name", header: "First name", accessorFn: (row) => row.first_name },
    { id: "last_name", header: "Last name", accessorFn: (row) => row.last_name },
    { id: "role_original", header: "Role", accessorFn: (row) => row.role_original },
    { id: "role_kind", header: "Role kind", accessorFn: (row) => row.role_kind },
    {
      id: "signatory_kind",
      header: "Signatory kind",
      accessorFn: (row) => row.signatory_kind,
    },
    {
      id: "fiscal_year",
      header: "Fiscal year",
      cell: ({ row }) => cell(row.original.fiscal_year),
    },
  ];
}

function esefColumns(): ColumnDef<SePeopleEsefRow, unknown>[] {
  return [
    {
      id: "company_id",
      header: "Company",
      cell: ({ row }) => <CompanyCell companyId={row.original.company_id} />,
    },
    { id: "full_name", header: "Name", accessorFn: (row) => row.full_name },
    { id: "role", header: "Role", accessorFn: (row) => row.role },
    {
      id: "role_category",
      header: "Role category",
      accessorFn: (row) => row.role_category,
    },
    { id: "organization", header: "Organization", accessorFn: (row) => row.organization },
    { id: "status", header: "Status", accessorFn: (row) => row.status },
    {
      id: "effective_from",
      header: "Effective from",
      cell: ({ row }) => cell(row.original.effective_from),
    },
    {
      id: "effective_to",
      header: "Effective to",
      cell: ({ row }) => cell(row.original.effective_to),
    },
    {
      id: "confidence",
      header: "Confidence",
      cell: ({ row }) =>
        row.original.confidence === null ? "—" : row.original.confidence.toFixed(2),
    },
  ];
}

function wikidataColumns(): ColumnDef<SePeopleWikidataRow, unknown>[] {
  return [
    {
      id: "company_id",
      header: "Company",
      cell: ({ row }) => <CompanyCell companyId={row.original.company_id} />,
    },
    { id: "full_name", header: "Name", accessorFn: (row) => row.full_name },
    {
      id: "person_wikidata_id",
      header: "Wikidata ID",
      cell: ({ row }) => (
        <a
          href={`https://www.wikidata.org/wiki/${encodeURIComponent(row.original.person_wikidata_id)}`}
          target="_blank"
          rel="noreferrer"
          className="font-mono text-xs underline underline-offset-2"
        >
          {row.original.person_wikidata_id}
        </a>
      ),
    },
    {
      id: "role_property",
      header: "Role property",
      accessorFn: (row) => row.role_property,
    },
    {
      id: "start_date",
      header: "Start date",
      cell: ({ row }) => cell(row.original.start_date),
    },
    { id: "end_date", header: "End date", cell: ({ row }) => cell(row.original.end_date) },
    {
      id: "birth_year",
      header: "Birth year",
      cell: ({ row }) => cell(row.original.birth_year),
    },
    {
      id: "description",
      header: "Description",
      cell: ({ row }) => (
        <span
          className="block max-w-[20rem] truncate"
          title={row.original.description ?? undefined}
        >
          {cell(row.original.description)}
        </span>
      ),
    },
  ];
}

function finalColumns(): ColumnDef<SePeopleFinalRow, unknown>[] {
  return [
    {
      id: "company_id",
      header: "Company",
      cell: ({ row }) => <CompanyCell companyId={row.original.company_id} />,
    },
    {
      id: "name",
      header: "Person",
      cell: ({ row }) => (
        <Link
          to={`/admin/se/people/person/${encodeURIComponent(row.original.company_id)}/${encodeURIComponent(row.original.person_id)}`}
          className="font-medium underline underline-offset-2"
        >
          {row.original.name}
        </Link>
      ),
    },
    {
      id: "description",
      header: "Description",
      cell: ({ row }) => (
        <span
          className="block max-w-[24rem] truncate"
          title={row.original.description ?? undefined}
        >
          {cell(row.original.description)}
        </span>
      ),
    },
    {
      id: "model",
      header: "Model",
      cell: ({ row }) =>
        `${cell(row.original.model_provider)} / ${cell(row.original.model_name)}`,
    },
    { id: "updated_at", header: "Updated", accessorFn: (row) => row.updated_at },
  ];
}

const nf = new Intl.NumberFormat("en-US");

/** Dagster reports seconds since the epoch as a float. Mirrors
 * se-company-info-pipeline.tsx's `instant`. */
function instant(seconds: number | null): string {
  if (seconds === null) return "—";
  return new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 19);
}

/** Finished runs only -- a running run's duration would need the clock, same
 * reasoning as se-company-info-pipeline.tsx's `duration`. */
function duration(row: SePeopleTaskRow): string {
  if (row.startTime === null) return "—";
  if (row.endTime === null) return "running";
  const total = Math.max(0, Math.round(row.endTime - row.startTime));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

/** Colored-badge-per-status convention, mirrored from
 * se-company-info-pipeline.tsx's `statusVariant`. */
function statusVariant(
  status: string | null,
): "default" | "secondary" | "destructive" | "outline" {
  if (status === "SUCCESS") return "default";
  if (status === "FAILURE" || status === "CANCELED") return "destructive";
  if (status === "STARTED" || status === "STARTING" || status === "QUEUED") return "secondary";
  return "outline";
}

function metricsText(metrics: Record<string, number>): string {
  const entries = Object.entries(metrics);
  if (entries.length === 0) return "—";
  return entries.map(([key, value]) => `${key.replace(/_count$/, "")}: ${nf.format(value)}`).join(", ");
}

/** Tasks tab: every people asset/job's latest run, columns per spec --
 * asset/job, colored status badge, started/ended, duration, and whatever key
 * metadata counts `se-people-tasks.server.ts` picked out (see that module's
 * `TASK_SPECS` doc comment for what is and is not included and why). */
function SePeopleTasksTable({ rows, error }: { rows: SePeopleTaskRow[]; error: string }) {
  return (
    <div className="flex flex-col gap-4">
      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Dagster is unreachable</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
      <div className="overflow-x-auto">
        <Table className="min-w-[64rem]">
          <TableHeader>
            <TableRow>
              <TableHead>Asset / job</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Started</TableHead>
              <TableHead>Ended</TableHead>
              <TableHead>Duration</TableHead>
              <TableHead>Key metrics</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-muted-foreground text-sm">
                  {error ? "No run data -- Dagster could not be reached." : "No runs to show."}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row) => (
                <TableRow key={row.key}>
                  <TableCell className="text-sm">{row.label}</TableCell>
                  <TableCell>
                    {row.status ? (
                      <Badge variant={statusVariant(row.status)}>{row.status}</Badge>
                    ) : (
                      <span className="text-muted-foreground text-xs">No runs yet</span>
                    )}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs">
                    {instant(row.startTime)}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs">
                    {instant(row.endTime)}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs">
                    {duration(row)}
                  </TableCell>
                  <TableCell className="text-xs">{metricsText(row.metrics)}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

/** The company id / name filter form every tab shares. A plain GET `<Form>`,
 * mirroring se-company-info-filter-sheet.tsx's ViewFields pattern: `tab` and
 * `pageSize` ride along as hidden fields so a filter submit stays on the same
 * tab and page size, and always drops `page` (the browser simply omits it). */
function SourceFilterForm({
  tab,
  filters,
  pageSize,
}: {
  tab: SePeopleSourcePage["tab"];
  filters: SePeopleSourceFilters;
  pageSize: number;
}) {
  const searchParams = useEffectiveSearchParams();
  const hasFilters = filters.companyId !== "" || filters.name !== "";
  return (
    <Form method="get" className="flex flex-wrap items-end gap-2">
      <input type="hidden" name="tab" value={tab} />
      <input type="hidden" name="pageSize" value={pageSize} />
      <div className="flex flex-col gap-1">
        <Label htmlFor="se-people-company-id" className="text-xs font-medium">
          Company ID
        </Label>
        <Input
          id="se-people-company-id"
          name="companyId"
          defaultValue={filters.companyId}
          placeholder="10-digit org number"
          className="w-48"
        />
      </div>
      <div className="flex flex-col gap-1">
        <Label htmlFor="se-people-name" className="text-xs font-medium">
          Name
        </Label>
        <Input
          id="se-people-name"
          name="name"
          defaultValue={filters.name}
          placeholder="Search by name…"
          className="w-56"
        />
      </div>
      <Button type="submit" variant="secondary">
        Filter
      </Button>
      {hasFilters ? (
        <Button
          variant="ghost"
          render={
            <Link
              to={sePeopleSourcesSearch(searchParams, { tab, companyId: "", name: "" })}
            />
          }
          nativeButton={false}
        >
          Clear
        </Button>
      ) : null}
    </Form>
  );
}

function SourceTabsNav({ tab }: { tab: SePeopleSourcePage["tab"] }) {
  const searchParams = useEffectiveSearchParams();
  return (
    <Tabs value={tab}>
      <TabsList variant="line">
        {SE_PEOPLE_SOURCE_TABS.map((entry) => (
          <TabsTrigger
            key={entry.value}
            value={entry.value}
            render={
              <Link
                to={sePeopleSourcesSearch(searchParams, { tab: entry.value })}
                preventScrollReset
              />
            }
            nativeButton={false}
          >
            {entry.label}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  );
}

/**
 * `/admin/se/people`'s body: the tab bar (a route search-param, not React
 * state -- a cold load and a client navigation must always agree on which
 * tab is active), the shared filter form, and ONE `DataTable` for whichever
 * tab the loader fetched. Only the active tab's rows ever reach this
 * component -- the other three tables are never mounted, mirroring why the
 * loader never queries more than one source per request.
 */
export function SePeopleSourcesTable({
  page,
  filters,
  view,
}: {
  page: SePeopleSourcePage;
  filters: SePeopleSourceFilters;
  view: SePeopleSourceView;
}) {
  return (
    <div className="flex flex-col gap-4">
      <SourceTabsNav tab={page.tab} />
      {page.tab === "tasks" ? null : (
        <SourceFilterForm tab={page.tab} filters={filters} pageSize={view.pageSize} />
      )}
      {page.tab === "bolagsverket" ? (
        <DataTable
          columns={bolagsverketColumns()}
          data={page.rows}
          emptyText="No Bolagsverket signatory rows match these filters."
          minWidthClassName="min-w-[72rem]"
        />
      ) : page.tab === "esef" ? (
        <DataTable
          columns={esefColumns()}
          data={page.rows}
          emptyText="No ESEF person rows match these filters."
          minWidthClassName="min-w-[72rem]"
        />
      ) : page.tab === "wikidata" ? (
        <DataTable
          columns={wikidataColumns()}
          data={page.rows}
          emptyText="No Wikidata person rows match these filters."
          minWidthClassName="min-w-[64rem]"
        />
      ) : page.tab === "final" ? (
        <DataTable
          columns={finalColumns()}
          data={page.rows}
          emptyText="No resolved people yet -- run the pipeline's clean-copy step from se/people/pipeline."
          minWidthClassName="min-w-[56rem]"
          rowHref={(row) =>
            `/admin/se/people/person/${encodeURIComponent(row.company_id)}/${encodeURIComponent(row.person_id)}`
          }
        />
      ) : (
        <SePeopleTasksTable rows={page.rows} error={page.error} />
      )}
      {page.tab === "tasks" ? null : (
        <DataTablePagination
          total={page.total}
          page={view.page}
          pageSize={view.pageSize}
          itemsLabel={page.tab === "final" ? "people" : "source rows"}
        />
      )}
    </div>
  );
}
