import type { ColumnDef } from "@tanstack/react-table";
import { ArrowLeft, Search, SearchX } from "lucide-react";
import { Form, Link, useNavigate } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import type {
  CompanyDomainReviewQueueResult,
  CompanyDomainReviewQueueRow,
  CompanyDomainSourceFilter,
} from "~/lib/company-domains.server";

const percent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 0,
});

const sourceOptions: Array<{
  value: CompanyDomainSourceFilter;
  label: string;
}> = [
  { value: "all", label: "All" },
  { value: "wikidata", label: "Wikidata" },
  { value: "esef_filing", label: "ESEF" },
  { value: "common_crawl_identity", label: "Common Crawl" },
];

function columns(
  countryCode: string,
): ColumnDef<CompanyDomainReviewQueueRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <div className="flex max-w-96 flex-col gap-1">
          <Link
            to={`/company/${countryCode}/${encodeURIComponent(row.original.companyId)}/suggestions`}
            className="truncate font-medium underline-offset-2 hover:underline"
          >
            {row.original.companyName}
          </Link>
          <span className="text-muted-foreground font-mono text-xs">
            {row.original.companyId}
          </span>
        </div>
      ),
    },
    {
      id: "domain",
      header: "Domain",
      cell: ({ row }) => (
        <span className="font-medium">{row.original.rootDomain}</span>
      ),
    },
    {
      id: "sources",
      header: "Sources",
      cell: ({ row }) => (
        <div className="flex flex-wrap gap-1">
          {row.original.sources.map((source) => (
            <Badge key={source.name} variant="outline">
              {source.name.replaceAll("_", " ")}
            </Badge>
          ))}
        </div>
      ),
    },
    {
      id: "confidence",
      header: "Suggested confidence",
      cell: ({ row }) => (
        <span className="font-medium tabular-nums">
          {percent.format(row.original.suggestedConfidence)}
        </span>
      ),
    },
  ];
}

export function CompanyDomainSuggestionReview({
  countryCode,
  query,
  source,
  result,
}: {
  countryCode: string;
  query: string;
  source: CompanyDomainSourceFilter;
  result: CompanyDomainReviewQueueResult;
}) {
  const navigate = useNavigate();
  const searchParams = useEffectiveSearchParams();

  function selectSource(nextSource: CompanyDomainSourceFilter) {
    const next = new URLSearchParams(searchParams);
    next.delete("page");
    if (nextSource === "all") next.delete("source");
    else next.set("source", nextSource);
    navigate(`?${next.toString()}`, { preventScrollReset: true });
  }

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5">
      <header className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2 self-start"
          render={<Link to={`/countries/${countryCode}/companies`} />}
          nativeButton={false}
        >
          <ArrowLeft data-icon="inline-start" />
          Sweden companies
        </Button>
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Domain review
          </h1>
          <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
            Unreviewed company/domain associations from every discovery source.
            Open a company to inspect evidence, technology, and infrastructure
            before confirming or rejecting the association.
          </p>
        </div>
      </header>

      <div className="flex flex-wrap items-end justify-between gap-4">
        <Form method="get" className="w-full max-w-xl">
          <input
            type="hidden"
            name="source"
            value={source === "all" ? "" : source}
          />
          <FieldGroup>
            <Field orientation="horizontal">
              <FieldLabel htmlFor="company-domain-search" className="sr-only">
                Search domains
              </FieldLabel>
              <Input
                id="company-domain-search"
                type="search"
                name="q"
                defaultValue={query}
                placeholder="Company, organization number, or domain…"
              />
              <Button type="submit" variant="secondary">
                <Search data-icon="inline-start" />
                Search
              </Button>
            </Field>
          </FieldGroup>
        </Form>

        <Field orientation="horizontal" className="w-auto">
          <FieldLabel>Source</FieldLabel>
          <ToggleGroup
            value={[source]}
            onValueChange={(values) => {
              const next = values.at(-1) as
                CompanyDomainSourceFilter | undefined;
              if (next) selectSource(next);
            }}
            variant="outline"
            size="sm"
            spacing={0}
            aria-label="Domain source filter"
          >
            {sourceOptions.map((option) => (
              <ToggleGroupItem key={option.value} value={option.value}>
                {option.label}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </Field>
      </div>

      {result.rows.length ? (
        <>
          <DataTable
            columns={columns(countryCode)}
            data={result.rows}
            minWidthClassName="min-w-[58rem]"
          />
          <DataTablePagination
            total={result.total}
            page={result.page}
            pageSize={result.pageSize}
            itemsLabel="domains"
          />
        </>
      ) : (
        <Empty>
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <SearchX />
            </EmptyMedia>
            <EmptyTitle>No unreviewed domains found</EmptyTitle>
            <EmptyDescription>
              Change the search or source filter, or review another queue.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
    </div>
  );
}
