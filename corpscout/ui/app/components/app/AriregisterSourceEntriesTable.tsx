import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "react-router";
import { ArrowDown, ArrowUp, ChevronsUpDown, Search, X } from "lucide-react";
import { api } from "~/lib/api";
import type { AriregisterSourceEntryListItem } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const PAGE_SIZE = 50;

type SortDirection = "asc" | "desc";

const FILTER_OPTIONS = {
  lifecycle_status: [
    ["active", "Active"],
    ["inactive", "Inactive"],
    ["deleted", "Deleted"],
  ],
  translation_status: [
    ["missing", "Missing translations"],
    ["complete", "Complete"],
  ],
} as const;

function pageFromParams(searchParams: URLSearchParams) {
  const page = Number(searchParams.get("source_page") ?? "1");
  return Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
}

function formatDate(value: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function compactStatus(status?: string) {
  return status ? status.replace(/_/g, " ") : "-";
}

function formatCount(value: number) {
  return value.toLocaleString();
}

function SortIcon({
  active,
  direction,
}: {
  active: boolean;
  direction: SortDirection;
}) {
  if (!active) return <ChevronsUpDown className="size-3.5 text-muted-foreground" />;
  if (direction === "asc") return <ArrowUp className="size-3.5" />;
  return <ArrowDown className="size-3.5" />;
}

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: readonly (readonly [string, string])[];
  onChange: (value: string) => void;
}) {
  return (
    <label className="flex items-center gap-2 text-xs text-muted-foreground">
      <span>{label}</span>
      <select
        className="h-8 rounded-md border border-input bg-background px-2 text-sm text-foreground"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      >
        <option value="">All</option>
        {options.map(([optionValue, optionLabel]) => (
          <option key={optionValue} value={optionValue}>
            {optionLabel}
          </option>
        ))}
      </select>
    </label>
  );
}

function TranslationBadge({ missingCount }: { missingCount: number }) {
  if (missingCount > 0) {
    return <Badge variant="outline">{formatCount(missingCount)} missing</Badge>;
  }
  return <Badge variant="secondary">Complete</Badge>;
}

function RelatedCounts({ item }: { item: AriregisterSourceEntryListItem }) {
  return (
    <div className="flex flex-wrap gap-1">
      <Badge variant="outline">{formatCount(item.website_count)} websites</Badge>
      <Badge variant="outline">{formatCount(item.domain_count)} domains</Badge>
      <Badge variant="outline">{formatCount(item.contact_count)} contacts</Badge>
    </div>
  );
}

function SourceEntryRow({ item }: { item: AriregisterSourceEntryListItem }) {
  return (
    <TableRow>
      <TableCell>
        <div className="flex min-w-56 flex-col gap-1">
          <span className="font-medium">{item.legal_name || "Unnamed company"}</span>
          <span className="font-mono text-xs text-muted-foreground">
            {item.registry_code}
          </span>
          <div className="flex flex-wrap gap-1">
            <Badge variant="outline">{compactStatus(item.lifecycle_status)}</Badge>
            {item.legal_form_label && (
              <Badge variant="secondary">{item.legal_form_label}</Badge>
            )}
          </div>
        </div>
      </TableCell>
      <TableCell>
        <div className="flex min-w-48 flex-col gap-1">
          <span>{item.primary_industry_label ?? "-"}</span>
          <span className="text-xs text-muted-foreground">
            {item.primary_industry_code ?? item.primary_nace_code ?? "-"}
          </span>
        </div>
      </TableCell>
      <TableCell>
        <div className="flex min-w-44 flex-col gap-1">
          <span>{item.city_or_area ?? "-"}</span>
          <span className="text-xs text-muted-foreground">
            {item.normalized_full_address ?? item.postal_code ?? ""}
          </span>
        </div>
      </TableCell>
      <TableCell>
        <div className="flex min-w-28 flex-col gap-1">
          <span>{item.employee_count?.toLocaleString() ?? "-"}</span>
          {item.latest_financial_year && (
            <span className="text-xs text-muted-foreground">
              FY {item.latest_financial_year}
            </span>
          )}
        </div>
      </TableCell>
      <TableCell>
        <RelatedCounts item={item} />
      </TableCell>
      <TableCell>
        <TranslationBadge missingCount={item.translation_missing_count} />
      </TableCell>
      <TableCell>
        <span className="whitespace-nowrap text-sm">{formatDate(item.updated_at)}</span>
      </TableCell>
    </TableRow>
  );
}

export function AriregisterSourceEntriesTable() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState<AriregisterSourceEntryListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [searchInput, setSearchInput] = useState(
    searchParams.get("source_q") ?? "",
  );

  const page = pageFromParams(searchParams);
  const query = searchParams.get("source_q") ?? "";
  const lifecycleStatus = searchParams.get("source_lifecycle_status") ?? "";
  const translationStatus = searchParams.get("source_translation_status") ?? "";
  const sort = searchParams.get("source_sort") ?? "";
  const sortDirection: SortDirection =
    searchParams.get("source_dir") === "asc" ? "asc" : "desc";
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getAriregisterSourceEntries({
        page,
        limit: PAGE_SIZE,
        q: query || undefined,
        lifecycle_status: lifecycleStatus || undefined,
        translation_status: translationStatus || undefined,
        sort: sort || undefined,
        dir: sort ? sortDirection : undefined,
      });
      setItems(Array.isArray(res.items) ? res.items : []);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [lifecycleStatus, page, query, sort, sortDirection, translationStatus]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    setSearchInput(query);
  }, [query]);

  const setParam = useCallback(
    (key: string, value: string) => {
      const next = new URLSearchParams(searchParams);
      if (value && !(key === "source_page" && value === "1")) next.set(key, value);
      else next.delete(key);
      if (key !== "source_page") next.delete("source_page");
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const applySearch = () => setParam("source_q", searchInput.trim());
  const clearSearch = () => {
    setSearchInput("");
    setParam("source_q", "");
  };

  const setSort = useCallback(
    (sortKey: string) => {
      const next = new URLSearchParams(searchParams);
      const nextDirection: SortDirection =
        sort === sortKey && sortDirection === "asc" ? "desc" : "asc";
      next.set("source_sort", sortKey);
      next.set("source_dir", nextDirection);
      next.delete("source_page");
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams, sort, sortDirection],
  );

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex items-center">
          <Search className="absolute left-2.5 size-4 text-muted-foreground" />
          <Input
            className="h-8 w-72 pl-8 pr-8 text-sm"
            placeholder="Search source entries"
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            onKeyDown={(event) => event.key === "Enter" && applySearch()}
          />
          {searchInput && (
            <button className="absolute right-2" onClick={clearSearch} type="button">
              <X className="size-3.5 text-muted-foreground" />
            </button>
          )}
        </div>
        {searchInput !== query && (
          <Button size="sm" variant="secondary" className="h-8" onClick={applySearch}>
            Search
          </Button>
        )}
        <FilterSelect
          label="Lifecycle"
          value={lifecycleStatus}
          options={FILTER_OPTIONS.lifecycle_status}
          onChange={(value) => setParam("source_lifecycle_status", value)}
        />
        <FilterSelect
          label="Translation"
          value={translationStatus}
          options={FILTER_OPTIONS.translation_status}
          onChange={(value) => setParam("source_translation_status", value)}
        />
        <span className="ml-auto text-sm text-muted-foreground">
          {loading ? "Loading..." : `${total.toLocaleString()} source entries`}
        </span>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>
                <button
                  type="button"
                  className="flex h-8 items-center gap-1 text-left font-medium"
                  onClick={() => setSort("organization")}
                >
                  Company
                  <SortIcon active={sort === "organization"} direction={sortDirection} />
                </button>
              </TableHead>
              <TableHead>Industry</TableHead>
              <TableHead>Location</TableHead>
              <TableHead>Employees</TableHead>
              <TableHead>Related Data</TableHead>
              <TableHead>Translation</TableHead>
              <TableHead>
                <button
                  type="button"
                  className="flex h-8 items-center gap-1 text-left font-medium"
                  onClick={() => setSort("updated_at")}
                >
                  Updated
                  <SortIcon active={sort === "updated_at"} direction={sortDirection} />
                </button>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: 8 }).map((_, rowIndex) => (
                <TableRow key={rowIndex}>
                  {Array.from({ length: 7 }).map((__, cellIndex) => (
                    <TableCell key={cellIndex}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : items.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={7}
                  className="py-12 text-center text-muted-foreground"
                >
                  No Ariregister source entries found.
                </TableCell>
              </TableRow>
            ) : (
              items.map((item) => (
                <SourceEntryRow key={item.company_id} item={item} />
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-sm text-muted-foreground">
            Page {page} of {totalPages}
          </span>
          <div className="flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={page <= 1 || loading}
              onClick={() => setParam("source_page", String(page - 1))}
            >
              Previous
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={page >= totalPages || loading}
              onClick={() => setParam("source_page", String(page + 1))}
            >
              Next
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
