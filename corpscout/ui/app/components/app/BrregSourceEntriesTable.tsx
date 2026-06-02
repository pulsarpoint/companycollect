import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router";
import {
  ArrowDown,
  ArrowUp,
  ChevronsUpDown,
  ListChecks,
  Search,
  X,
} from "lucide-react";
import { BrregSourceEntryActionSheet } from "~/components/app/BrregSourceEntryActionSheet";
import { api } from "~/lib/api";
import type { BrregSourceEntryListItem } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
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
    ["liquidating", "Liquidating"],
    ["bankrupt", "Bankrupt"],
    ["forced_dissolution", "Forced dissolution"],
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

function formatCurrencyCents(value?: number) {
  if (value == null) return "-";
  return new Intl.NumberFormat(undefined, {
    currency: "USD",
    maximumFractionDigits: 0,
    style: "currency",
  }).format(value / 100);
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
    return <Badge variant="outline">{missingCount.toLocaleString()} missing</Badge>;
  }
  return <Badge variant="secondary">Complete</Badge>;
}

function ActionTaskBadges({
  pendingCount,
  runningCount,
  succeededCount,
  emptyLabel,
}: {
  pendingCount: number;
  runningCount: number;
  succeededCount: number;
  emptyLabel?: string;
}) {
  if (pendingCount === 0 && runningCount === 0 && succeededCount === 0) {
    return emptyLabel ? (
      <Badge className="text-muted-foreground" variant="outline">
        {emptyLabel}
      </Badge>
    ) : null;
  }

  return (
    <div className="flex flex-wrap gap-1">
      {pendingCount > 0 && (
        <Badge variant="outline">{formatCount(pendingCount)} queued</Badge>
      )}
      {runningCount > 0 && (
        <Badge variant="default">{formatCount(runningCount)} running</Badge>
      )}
      {succeededCount > 0 && (
        <Badge variant="secondary">{formatCount(succeededCount)} done</Badge>
      )}
    </div>
  );
}

function TranslationState({ item }: { item: BrregSourceEntryListItem }) {
  return (
    <div className="flex min-w-40 flex-col gap-1">
      <TranslationBadge missingCount={item.translation_missing_count} />
      <ActionTaskBadges
        pendingCount={item.translation_pending_count}
        runningCount={item.translation_running_count}
        succeededCount={item.translation_succeeded_count}
      />
    </div>
  );
}

function DomainDiscoveryState({ item }: { item: BrregSourceEntryListItem }) {
  return (
    <div className="flex min-w-36 flex-col gap-1">
      <ActionTaskBadges
        pendingCount={item.domain_pending_count}
        runningCount={item.domain_running_count}
        succeededCount={item.domain_succeeded_count}
        emptyLabel="Disabled"
      />
    </div>
  );
}

function EntryCounts({ item }: { item: BrregSourceEntryListItem }) {
  return (
    <div className="flex flex-wrap gap-1">
      <Badge variant="outline">{formatCount(item.website_count)} websites</Badge>
      <Badge variant="outline">{formatCount(item.domain_count)} domains</Badge>
      <Badge variant="outline">{formatCount(item.contact_count)} contacts</Badge>
    </div>
  );
}

export function BrregSourceEntriesTable() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState<BrregSourceEntryListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [searchInput, setSearchInput] = useState(searchParams.get("source_q") ?? "");
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [allFilteredSelected, setAllFilteredSelected] = useState(false);
  const [actionSheetOpen, setActionSheetOpen] = useState(false);

  const page = pageFromParams(searchParams);
  const query = searchParams.get("source_q") ?? "";
  const lifecycleStatus = searchParams.get("source_lifecycle_status") ?? "";
  const translationStatus = searchParams.get("source_translation_status") ?? "";
  const sort = searchParams.get("source_sort") ?? "";
  const sortDirection: SortDirection =
    searchParams.get("source_dir") === "asc" ? "asc" : "desc";
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const actionFilters = useMemo(() => {
    const filters: Record<string, string> = {};
    if (query) filters.q = query;
    if (lifecycleStatus) filters.lifecycle_state = lifecycleStatus;
    return filters;
  }, [lifecycleStatus, query]);
  const filterKey = JSON.stringify(actionFilters);
  const selectedIdList = useMemo(() => Array.from(selectedIds), [selectedIds]);
  const currentPageSelected =
    items.length > 0 && (allFilteredSelected || items.every((item) => selectedIds.has(item.company_id)));
  const someCurrentPageSelected =
    !allFilteredSelected && items.some((item) => selectedIds.has(item.company_id)) && !currentPageSelected;

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getBrregSourceEntries({
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

  useEffect(() => {
    setSelectedIds(new Set());
    setAllFilteredSelected(false);
  }, [filterKey]);

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

  const toggleCurrentPageSelection = (checked: boolean) => {
    setAllFilteredSelected(false);
    setSelectedIds((current) => {
      const next = new Set(current);
      for (const item of items) {
        if (checked) next.add(item.company_id);
        else next.delete(item.company_id);
      }
      return next;
    });
  };

  const toggleRowSelection = (id: string, checked: boolean) => {
    setAllFilteredSelected(false);
    setSelectedIds((current) => {
      const next = new Set(current);
      if (checked) next.add(id);
      else next.delete(id);
      return next;
    });
  };

  const clearSelection = () => {
    setSelectedIds(new Set());
    setAllFilteredSelected(false);
  };

  const columns = useMemo(
    () => [
      { label: "Organization", sort: "organization" },
      { label: "Industry", sort: "industry" },
      { label: "Location", sort: "location" },
      { label: "Employees", sort: "employees" },
      { label: "Revenue", sort: "revenue" },
      { label: "Signals" },
      { label: "Translation", sort: "translation_missing" },
      { label: "Discovery" },
      { label: "Updated", sort: "updated_at" },
    ],
    [],
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
        <Button
          size="sm"
          variant="outline"
          className="h-8"
          disabled={loading || total === 0}
          onClick={() => setActionSheetOpen(true)}
        >
          <ListChecks className="size-4" />
          Action
        </Button>
        <span className="ml-auto text-sm text-muted-foreground">
          {loading ? "Loading..." : `${total.toLocaleString()} source entries`}
        </span>
      </div>

      {(selectedIdList.length > 0 || allFilteredSelected) && (
        <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          <span>
            {allFilteredSelected
              ? `${total.toLocaleString()} filtered source entries selected`
              : `${selectedIdList.length.toLocaleString()} source entries selected`}
          </span>
          {!allFilteredSelected && total > items.length && Object.keys(actionFilters).length > 0 && (
            <Button size="sm" variant="ghost" className="h-7 px-2" onClick={() => setAllFilteredSelected(true)}>
              Select all filtered
            </Button>
          )}
          <Button size="sm" variant="ghost" className="h-7 px-2" onClick={clearSelection}>
            Clear
          </Button>
        </div>
      )}

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10">
                <Checkbox
                  checked={currentPageSelected ? true : someCurrentPageSelected ? "indeterminate" : false}
                  disabled={loading || items.length === 0}
                  aria-label="Select current page"
                  onCheckedChange={(checked) => toggleCurrentPageSelection(checked === true)}
                />
              </TableHead>
              {columns.map((column) => (
                <TableHead key={column.label}>
                  {column.sort ? (
                    <button
                      type="button"
                      className="flex h-8 items-center gap-1 text-left font-medium"
                      onClick={() => setSort(column.sort)}
                    >
                      {column.label}
                      <SortIcon active={sort === column.sort} direction={sortDirection} />
                    </button>
                  ) : (
                    column.label
                  )}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: 8 }).map((_, rowIndex) => (
                <TableRow key={rowIndex}>
                  <TableCell>
                    <Skeleton className="size-4" />
                  </TableCell>
                  {columns.map((column) => (
                    <TableCell key={column.label}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : items.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={columns.length + 1}
                  className="py-12 text-center text-muted-foreground"
                >
                  No BRREG source entries found.
                </TableCell>
              </TableRow>
            ) : (
              items.map((item) => (
                <TableRow key={item.company_id}>
                  <TableCell>
                    <Checkbox
                      checked={allFilteredSelected || selectedIds.has(item.company_id)}
                      aria-label={`Select ${item.organization_name || item.organization_number}`}
                      onCheckedChange={(checked) => toggleRowSelection(item.company_id, checked === true)}
                    />
                  </TableCell>
                  <TableCell>
                    <div className="flex min-w-56 flex-col gap-1">
                      <Link
                        className="font-medium underline-offset-4 hover:underline"
                        to={`/sources/brreg/companies/${item.company_id}`}
                      >
                        {item.organization_name || "Unnamed organization"}
                      </Link>
                      <span className="font-mono text-xs text-muted-foreground">
                        {item.organization_number}
                      </span>
                      <div className="flex flex-wrap gap-1">
                        <Badge variant="outline">{compactStatus(item.lifecycle_status)}</Badge>
                        {item.organization_form_label && (
                          <Badge variant="secondary">{item.organization_form_label}</Badge>
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
                    <div className="flex min-w-40 flex-col gap-1">
                      <span>{item.city ?? item.municipality ?? "-"}</span>
                      <span className="text-xs text-muted-foreground">
                        {item.county ?? item.postal_code ?? ""}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span>{item.employee_count?.toLocaleString() ?? "-"}</span>
                      {item.employee_band && (
                        <span className="text-xs text-muted-foreground">{item.employee_band}</span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span>{formatCurrencyCents(item.latest_revenue_usd_cents)}</span>
                      {item.latest_financial_year && (
                        <span className="text-xs text-muted-foreground">
                          FY {item.latest_financial_year}
                        </span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <EntryCounts item={item} />
                  </TableCell>
                  <TableCell>
                    <TranslationState item={item} />
                  </TableCell>
                  <TableCell>
                    <DomainDiscoveryState item={item} />
                  </TableCell>
                  <TableCell>
                    <span className="whitespace-nowrap text-sm">{formatDate(item.updated_at)}</span>
                  </TableCell>
                </TableRow>
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
      <BrregSourceEntryActionSheet
        open={actionSheetOpen}
        onOpenChange={setActionSheetOpen}
        selectedIds={allFilteredSelected ? [] : selectedIdList}
        totalCount={total}
        filters={actionFilters}
        initialScope={allFilteredSelected ? "filtered" : undefined}
        onStarted={() => {
          clearSelection();
          load();
        }}
      />
    </div>
  );
}
