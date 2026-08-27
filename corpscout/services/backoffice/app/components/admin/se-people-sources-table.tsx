import type { ColumnDef } from "@tanstack/react-table";
import { Form, Link } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
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
      <SourceFilterForm tab={page.tab} filters={filters} pageSize={view.pageSize} />
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
      ) : (
        <DataTable
          columns={finalColumns()}
          data={page.rows}
          emptyText="No resolved people yet -- run the pipeline's clean-copy step from se/people/pipeline."
          minWidthClassName="min-w-[56rem]"
          rowHref={(row) =>
            `/admin/se/people/person/${encodeURIComponent(row.company_id)}/${encodeURIComponent(row.person_id)}`
          }
        />
      )}
      <DataTablePagination
        total={page.total}
        page={view.page}
        pageSize={view.pageSize}
        itemsLabel={page.tab === "final" ? "people" : "source rows"}
      />
    </div>
  );
}
