import type { ColumnDef } from "@tanstack/react-table";
import { DatabaseSearchIcon, SearchIcon } from "lucide-react";
import { Form, Link } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  commonCrawlDomainPath,
  type CommonCrawlFilters,
  type CommonCrawlListView,
} from "~/lib/common-crawl";
import type { CommonCrawlSearchRow } from "~/lib/common-crawl.server";

const numberFormat = new Intl.NumberFormat("en-US");

function commonCrawlColumns(): ColumnDef<CommonCrawlSearchRow, unknown>[] {
  return [
    {
      id: "domain",
      header: "Domain",
      cell: ({ row }) => (
        <div className="flex min-w-48 flex-col gap-1">
          <Link
            to={commonCrawlDomainPath(row.original.rootDomain)}
            className="font-mono font-medium underline-offset-2 hover:underline"
          >
            {row.original.rootDomain}
          </Link>
          {row.original.organizationName ? (
            <span className="text-muted-foreground line-clamp-1 text-xs">
              {row.original.organizationName}
            </span>
          ) : null}
        </div>
      ),
    },
    {
      id: "address",
      header: "Extracted address",
      cell: ({ row }) => (
        <span className="text-muted-foreground line-clamp-2 block max-w-sm text-xs whitespace-normal">
          {row.original.address || "—"}
        </span>
      ),
    },
    {
      id: "industry",
      header: "Inferred industry",
      cell: ({ row }) =>
        row.original.industryCode || row.original.industryLabel ? (
          <div className="flex max-w-sm items-start gap-2">
            {row.original.industryCode ? (
              <Badge variant="outline">{row.original.industryCode}</Badge>
            ) : null}
            <span className="text-muted-foreground line-clamp-2 text-xs whitespace-normal">
              {row.original.industryLabel || "Label unavailable"}
            </span>
          </div>
        ) : (
          <span className="text-muted-foreground">—</span>
        ),
    },
    {
      id: "coverage",
      header: "Crawl coverage",
      cell: ({ row }) => (
        <div className="flex min-w-36 flex-col gap-1 text-xs tabular-nums">
          <span className="font-mono">
            {row.original.latestCrawlId || "Unknown crawl"}
          </span>
          <span className="text-muted-foreground">
            {numberFormat.format(row.original.latestPageCount)} pages ·{" "}
            {numberFormat.format(row.original.crawlCount)} snapshots
          </span>
        </div>
      ),
    },
  ];
}

function CommonCrawlSearchForm({
  filters,
  pageSize,
}: {
  filters: CommonCrawlFilters;
  pageSize: number;
}) {
  const hasFilters =
    filters.domain !== "" || filters.address !== "" || filters.industry !== "";
  return (
    <Card>
      <CardHeader>
        <CardTitle>Search extracted evidence</CardTitle>
        <CardDescription>
          Filters are combined. Industry accepts a NACE code or words from a
          NACE label; address matches website-provided postal addresses.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Form
          key={`${filters.domain}\u0000${filters.address}\u0000${filters.industry}`}
          method="get"
          className="flex flex-col gap-4"
        >
          <input type="hidden" name="pageSize" value={pageSize} />
          <FieldGroup className="grid gap-4 md:grid-cols-3">
            <Field>
              <FieldLabel htmlFor="common-crawl-domain">Domain</FieldLabel>
              <Input
                id="common-crawl-domain"
                name="domain"
                defaultValue={filters.domain}
                placeholder="example.com"
                autoComplete="off"
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="common-crawl-address">Address</FieldLabel>
              <Input
                id="common-crawl-address"
                name="address"
                defaultValue={filters.address}
                placeholder="Street, postal code, or city"
                autoComplete="off"
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="common-crawl-industry">Industry</FieldLabel>
              <Input
                id="common-crawl-industry"
                name="industry"
                defaultValue={filters.industry}
                placeholder="62.01 or computer programming"
                autoComplete="off"
              />
            </Field>
          </FieldGroup>
          <div className="flex flex-wrap gap-2">
            <Button type="submit">
              <SearchIcon data-icon="inline-start" />
              Search Common Crawl
            </Button>
            {hasFilters ? (
              <Button
                variant="ghost"
                render={<Link to="/admin/common-crawl" />}
                nativeButton={false}
              >
                Clear
              </Button>
            ) : null}
          </div>
        </Form>
      </CardContent>
    </Card>
  );
}

export function CommonCrawlTable({
  rows,
  total,
  filters,
  view,
  searched,
}: {
  rows: CommonCrawlSearchRow[];
  total: number;
  filters: CommonCrawlFilters;
  view: CommonCrawlListView;
  searched: boolean;
}) {
  return (
    <div className="flex flex-col gap-5">
      <CommonCrawlSearchForm filters={filters} pageSize={view.pageSize} />
      {searched ? (
        <div className="flex flex-col gap-4">
          <DataTable
            columns={commonCrawlColumns()}
            data={rows}
            emptyText="No Common Crawl domains match these filters."
            minWidthClassName="min-w-[62rem]"
            rowHref={(row) => commonCrawlDomainPath(row.rootDomain)}
          />
          <DataTablePagination
            total={total}
            page={view.page}
            pageSize={view.pageSize}
            itemsLabel="domains"
          />
        </div>
      ) : (
        <Empty className="min-h-64 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <DatabaseSearchIcon />
            </EmptyMedia>
            <EmptyTitle>Search the Common Crawl evidence index</EmptyTitle>
            <EmptyDescription>
              Enter a domain, extracted address, or inferred industry. Open a
              result to inspect its source-linked ClickHouse evidence by crawl.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
    </div>
  );
}
