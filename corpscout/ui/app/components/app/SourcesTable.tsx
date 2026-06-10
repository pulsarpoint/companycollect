import { Link } from "react-router";
import { useMemo, useState } from "react";
import type { DataSource } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "~/components/ui/pagination";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

interface SourcesTableProps {
  sources: DataSource[];
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;

function sourceCategoryLabel(value: string): string {
  const labels: Record<string, string> = {
    ai_research: "AI Research",
    security_identifier: "Security Identifier",
  };
  if (labels[value]) return labels[value];

  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function countryLabel(value: string): string {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function visiblePageNumbers(currentPage: number, pageCount: number): number[] {
  const start = Math.max(1, currentPage - 2);
  const end = Math.min(pageCount, start + 4);
  const adjustedStart = Math.max(1, end - 4);

  return Array.from(
    { length: end - adjustedStart + 1 },
    (_, index) => adjustedStart + index,
  );
}

export function SourcesTable({ sources }: SourcesTableProps) {
  const [country, setCountry] = useState("all");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState<(typeof PAGE_SIZE_OPTIONS)[number]>(10);

  const countryOptions = useMemo(() => {
    return Array.from(new Set(sources.map((source) => source.country).filter(Boolean)))
      .sort((a, b) => countryLabel(a).localeCompare(countryLabel(b)));
  }, [sources]);

  const filteredSources = useMemo(() => {
    if (country === "all") return sources;
    return sources.filter((source) => source.country === country);
  }, [country, sources]);

  const pageCount = Math.max(1, Math.ceil(filteredSources.length / pageSize));
  const safePage = Math.min(currentPage, pageCount);
  const pageStart = (safePage - 1) * pageSize;
  const visibleSources = filteredSources.slice(pageStart, pageStart + pageSize);
  const resultStart = filteredSources.length === 0 ? 0 : pageStart + 1;
  const resultEnd = Math.min(pageStart + pageSize, filteredSources.length);
  const pageNumbers = visiblePageNumbers(safePage, pageCount);

  function updateCountry(value: string) {
    setCountry(value);
    setCurrentPage(1);
  }

  function updatePageSize(value: string) {
    setPageSize(Number(value) as (typeof PAGE_SIZE_OPTIONS)[number]);
    setCurrentPage(1);
  }

  function goToPage(page: number) {
    setCurrentPage(Math.min(Math.max(1, page), pageCount));
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Select value={country} onValueChange={updateCountry}>
            <SelectTrigger className="w-[12rem]" aria-label="Filter by country">
              <SelectValue placeholder="Country" />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value="all">All countries</SelectItem>
                {countryOptions.map((option) => (
                  <SelectItem key={option} value={option}>
                    {countryLabel(option)}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
          <Select value={String(pageSize)} onValueChange={updatePageSize}>
            <SelectTrigger className="w-[8rem]" aria-label="Rows per page">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {PAGE_SIZE_OPTIONS.map((option) => (
                  <SelectItem key={option} value={String(option)}>
                    {option} rows
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
        <div className="text-sm text-muted-foreground">
          {filteredSources.length === 0
            ? "No sources"
            : `${resultStart}-${resultEnd} of ${filteredSources.length.toLocaleString()} sources`}
        </div>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Source</TableHead>
              <TableHead>Country</TableHead>
              <TableHead>Category</TableHead>
              <TableHead>Storage</TableHead>
              <TableHead>Auth</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Capabilities</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {visibleSources.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-24 text-center text-muted-foreground">
                  No sources match this country.
                </TableCell>
              </TableRow>
            ) : (
              visibleSources.map((s) => {
                return (
                  <TableRow key={s.name}>
                    <TableCell className="min-w-[18rem] font-medium">
                      <Link
                        to={`/sources/${s.name}`}
                        className="text-foreground hover:underline"
                      >
                        {s.display_name || s.name}
                      </Link>
                      {s.description && (
                        <p className="mt-1 max-w-xl text-xs leading-5 text-muted-foreground line-clamp-2">
                          {s.description}
                        </p>
                      )}
                      <div className="mt-1 font-mono text-xs text-muted-foreground">
                        {s.registry_key}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge className="text-muted-foreground" variant="outline">
                        {countryLabel(s.country)}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge className="text-muted-foreground" variant="outline">
                        {sourceCategoryLabel(s.source_group)}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-col gap-1 text-sm">
                        <div className="font-mono">{s.clickhouse_database}</div>
                        <div className="font-mono text-xs text-muted-foreground">
                          {s.clickhouse_table_prefix}_*
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {s.auth_required ? "Required" : "Public"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge
                        className={
                          s.enabled
                            ? "border-green-200 bg-green-100 text-green-800"
                            : "border-amber-200 bg-amber-100 text-amber-800"
                        }
                        variant="outline"
                      >
                        {s.enabled ? "Enabled" : "Disabled"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex max-w-sm flex-wrap gap-1">
                        {s.capabilities.map((capability) => (
                          <Badge key={capability} className="text-muted-foreground" variant="outline">
                            {capability}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>

      <Pagination className="justify-end">
        <PaginationContent>
          <PaginationItem>
            <PaginationPrevious
              href="#"
              aria-disabled={safePage <= 1}
              className={safePage <= 1 ? "pointer-events-none opacity-50" : undefined}
              onClick={(event) => {
                event.preventDefault();
                goToPage(safePage - 1);
              }}
            />
          </PaginationItem>
          {pageNumbers.map((page) => (
            <PaginationItem key={page}>
              <PaginationLink
                href="#"
                isActive={page === safePage}
                onClick={(event) => {
                  event.preventDefault();
                  goToPage(page);
                }}
              >
                {page}
              </PaginationLink>
            </PaginationItem>
          ))}
          <PaginationItem>
            <PaginationNext
              href="#"
              aria-disabled={safePage >= pageCount}
              className={safePage >= pageCount ? "pointer-events-none opacity-50" : undefined}
              onClick={(event) => {
                event.preventDefault();
                goToPage(safePage + 1);
              }}
            />
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    </div>
  );
}
