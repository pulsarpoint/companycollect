import type { ColumnDef } from "@tanstack/react-table";
import { Form, Link, useNavigate } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
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
import { TechnologyIcon } from "~/components/detail/technology-label";
import {
  technologiesSearch,
  technologyDetailPath,
  type TechnologyListFilters,
  type TechnologyListView,
} from "~/lib/technologies";
import type { TechnologyListRow } from "~/lib/technologies.server";

const nf = new Intl.NumberFormat("en-US");

/** The catalog row rendered through the shared TechnologyIcon (proxy icon or
 * monogram); the name is the link into /admin/technologies/:slug. */
function technologyColumns(): ColumnDef<TechnologyListRow, unknown>[] {
  return [
    {
      id: "technology",
      header: "Technology",
      cell: ({ row }) => (
        <span className="flex items-center gap-1.5 font-medium">
          <TechnologyIcon
            name={row.original.technology}
            entry={{
              slug: row.original.slug,
              description: row.original.description,
              website: row.original.website,
              categories: row.original.categories,
              saas: Boolean(row.original.saas),
              oss: Boolean(row.original.oss),
              icon: Boolean(row.original.has_icon),
            }}
          />
          <Link
            to={technologyDetailPath(row.original.slug)}
            className="underline-offset-2 hover:underline"
          >
            {row.original.technology}
          </Link>
        </span>
      ),
    },
    {
      id: "categories",
      header: "Categories",
      cell: ({ row }) => (
        <div className="flex max-w-56 flex-wrap gap-1">
          {row.original.categories.map((category) => (
            <Badge key={category} variant="outline">
              {category}
            </Badge>
          ))}
        </div>
      ),
    },
    {
      id: "description",
      header: "Description",
      cell: ({ row }) => (
        <span
          className="text-muted-foreground line-clamp-2 block max-w-[28rem] text-xs whitespace-normal"
          title={row.original.description || undefined}
        >
          {row.original.description || "—"}
        </span>
      ),
    },
    {
      id: "model",
      header: "Model",
      cell: ({ row }) => (
        <div className="flex gap-1">
          {row.original.saas ? <Badge variant="secondary">SaaS</Badge> : null}
          {row.original.oss ? <Badge variant="outline">OSS</Badge> : null}
        </div>
      ),
    },
    {
      id: "domain_count",
      header: "Adopting domains",
      cell: ({ row }) =>
        row.original.domain_count === "" ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <span className="tabular-nums">
            {nf.format(Number(row.original.domain_count))}
          </span>
        ),
    },
  ];
}

/** Name search as a plain GET form (mirrors SourceFilterForm); the category
 * filter navigates on change like DataTablePagination's page-size select --
 * both are loader navigations, never component state. */
function TechnologiesFilterBar({
  filters,
  categories,
  pageSize,
}: {
  filters: TechnologyListFilters;
  categories: string[];
  pageSize: number;
}) {
  const searchParams = useEffectiveSearchParams();
  const navigate = useNavigate();
  const hasFilters = filters.q !== "" || filters.category !== "";
  const ANY_CATEGORY = "__any__";
  // Base UI renders the selected VALUE unless given labels; the sentinel
  // must read "All categories", not "__any__".
  const categoryItems: Record<string, string> = {
    [ANY_CATEGORY]: "All categories",
    ...Object.fromEntries(categories.map((category) => [category, category])),
  };
  return (
    <div className="flex flex-wrap items-end gap-2">
      <Form method="get" className="flex flex-wrap items-end gap-2">
        <input type="hidden" name="pageSize" value={pageSize} />
        {filters.category !== "" ? (
          <input type="hidden" name="category" value={filters.category} />
        ) : null}
        <div className="flex flex-col gap-1">
          <Label htmlFor="technologies-q" className="text-xs font-medium">
            Name
          </Label>
          <Input
            id="technologies-q"
            name="q"
            defaultValue={filters.q}
            placeholder="Search technologies…"
            className="w-56"
          />
        </div>
        <Button type="submit" variant="secondary">
          Search
        </Button>
      </Form>
      <div className="flex flex-col gap-1">
        <Label htmlFor="technologies-category" className="text-xs font-medium">
          Category
        </Label>
        <Select
          items={categoryItems}
          value={filters.category === "" ? ANY_CATEGORY : filters.category}
          onValueChange={(value: string | null) => {
            if (value === null) return;
            navigate(
              technologiesSearch(searchParams, {
                category: value === ANY_CATEGORY ? "" : value,
              }),
              { preventScrollReset: true },
            );
          }}
        >
          <SelectTrigger id="technologies-category" className="w-56">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY_CATEGORY}>All categories</SelectItem>
            {categories.map((category) => (
              <SelectItem key={category} value={category}>
                {category}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      {hasFilters ? (
        <Button
          variant="ghost"
          render={
            <Link to={technologiesSearch(searchParams, { q: "", category: "" })} />
          }
          nativeButton={false}
        >
          Clear
        </Button>
      ) : null}
    </div>
  );
}

/** `/admin/technologies`'s body: filter bar, ONE server-fetched page of the
 * catalog, and the shared pagination footer. */
export function TechnologiesTable({
  rows,
  total,
  filters,
  categories,
  view,
}: {
  rows: TechnologyListRow[];
  total: number;
  filters: TechnologyListFilters;
  categories: string[];
  view: TechnologyListView;
}) {
  return (
    <div className="flex flex-col gap-4">
      <TechnologiesFilterBar
        filters={filters}
        categories={categories}
        pageSize={view.pageSize}
      />
      <DataTable
        columns={technologyColumns()}
        data={rows}
        emptyText="No technologies match these filters."
        minWidthClassName="min-w-[64rem]"
        rowHref={(row) => technologyDetailPath(row.slug)}
      />
      <DataTablePagination
        total={total}
        page={view.page}
        pageSize={view.pageSize}
        itemsLabel="technologies"
      />
    </div>
  );
}
