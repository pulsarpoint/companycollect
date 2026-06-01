import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowDown, ArrowUp, ChevronsUpDown, ListChecks, Search, X } from "lucide-react";
import { useSearchParams } from "react-router";
import { BrregRawRecordActionSheet } from "~/components/app/BrregRawRecordActionSheet";
import { BrregRawRecordDetailSheet } from "~/components/app/BrregRawRecordDetailSheet";
import { api } from "~/lib/api";
import type { BrregRawRecordListItem } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Skeleton } from "~/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

const PAGE_SIZE = 50;

type SortDirection = "asc" | "desc";

const FILTER_OPTIONS = {
  lifecycle_state: [
    ["input", "Input"],
    ["translated", "Translated"],
    ["ready_to_enhance", "Ready to enhance"],
    ["enhanced", "Enhanced"],
  ],
  translation_status: [
    ["not_started", "Not started"],
    ["succeeded", "Succeeded"],
    ["skipped", "Skipped"],
    ["failed", "Failed"],
  ],
  domain_status: [
    ["not_started", "Not started"],
    ["succeeded", "Succeeded"],
    ["partial", "Partial"],
    ["not_found", "Not found"],
    ["failed", "Failed"],
    ["skipped", "Skipped"],
  ],
  domain_search: [
    ["performed", "Search performed"],
    ["with_markdown", "Has markdown"],
    ["missing", "No search evidence"],
  ],
  financial_status: [
    ["not_started", "Not started"],
    ["succeeded", "Succeeded"],
    ["not_available", "Not available"],
    ["failed", "Failed"],
    ["skipped", "Skipped"],
  ],
  enhanced_status: [
    ["not_started", "Not started"],
    ["built", "Built"],
    ["published", "Published"],
    ["failed", "Failed"],
  ],
} as const;

function pageFromParams(searchParams: URLSearchParams) {
  const page = Number(searchParams.get("page") ?? "1");
  return Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
}

function statusClass(status: string) {
  if (status === "succeeded" || status === "built" || status === "published") {
    return "border-green-200 bg-green-100 text-green-800";
  }
  if (status.includes("failed")) return "border-red-200 bg-red-100 text-red-800";
  if (status === "running") return "border-blue-200 bg-blue-100 text-blue-800";
  if (status === "not_started") return "border-gray-200 bg-gray-50 text-gray-700";
  return "border-amber-200 bg-amber-50 text-amber-800";
}

function formatDate(value: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function compactStatus(status: string) {
  return status ? status.replace(/_/g, " ") : "not started";
}

function StatusBadge({ status }: { status: string }) {
  return (
    <Badge variant="outline" className={`whitespace-nowrap text-xs ${statusClass(status)}`}>
      {compactStatus(status)}
    </Badge>
  );
}

function SortIcon({ active, direction }: { active: boolean; direction: SortDirection }) {
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

export function BrregRawRecordsTable() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState<BrregRawRecordListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [searchInput, setSearchInput] = useState(searchParams.get("q") ?? "");
  const [selectedRecordID, setSelectedRecordID] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [allFilteredSelected, setAllFilteredSelected] = useState(false);
  const [actionSheetOpen, setActionSheetOpen] = useState(false);

  const page = pageFromParams(searchParams);
  const query = searchParams.get("q") ?? "";
  const lifecycleState = searchParams.get("state") ?? searchParams.get("lifecycle_state") ?? "";
  const translationStatus = searchParams.get("translation_status") ?? "";
  const domainStatus = searchParams.get("domain_status") ?? "";
  const domainSearch = searchParams.get("domain_search") ?? "";
  const financialStatus = searchParams.get("financial_status") ?? "";
  const enhancedStatus = searchParams.get("enhanced_status") ?? "";
  const sort = searchParams.get("sort") ?? "";
  const sortDirection: SortDirection = searchParams.get("dir") === "asc" ? "asc" : "desc";
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const actionFilters = useMemo(() => {
    const filters: Record<string, string> = {};
    if (query) filters.q = query;
    if (lifecycleState) filters.lifecycle_state = lifecycleState;
    if (translationStatus) filters.translation_status = translationStatus;
    if (domainStatus) filters.domain_status = domainStatus;
    if (financialStatus) filters.financial_status = financialStatus;
    if (enhancedStatus) filters.enhanced_status = enhancedStatus;
    return filters;
  }, [domainStatus, enhancedStatus, financialStatus, lifecycleState, query, translationStatus]);
  const filterKey = JSON.stringify(actionFilters);
  const selectedIdList = useMemo(() => Array.from(selectedIds), [selectedIds]);
  const currentPageSelected =
    items.length > 0 && (allFilteredSelected || items.every((item) => selectedIds.has(item.id)));
  const someCurrentPageSelected =
    !allFilteredSelected && items.some((item) => selectedIds.has(item.id)) && !currentPageSelected;

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getBrregRawRecords({
        page,
        limit: PAGE_SIZE,
        q: query || undefined,
        state: lifecycleState || undefined,
        translation_status: translationStatus || undefined,
        domain_status: domainStatus || undefined,
        domain_search: domainSearch || undefined,
        financial_status: financialStatus || undefined,
        enhanced_status: enhancedStatus || undefined,
        sort: sort || undefined,
        dir: sort ? sortDirection : undefined,
      });
      setItems(Array.isArray(res.items) ? res.items : []);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [
    domainStatus,
    domainSearch,
    enhancedStatus,
    financialStatus,
    lifecycleState,
    page,
    query,
    sort,
    sortDirection,
    translationStatus,
  ]);

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
      if (value && !(key === "page" && value === "1")) next.set(key, value);
      else next.delete(key);
      if (key !== "page") next.delete("page");
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const applySearch = () => setParam("q", searchInput.trim());
  const clearSearch = () => {
    setSearchInput("");
    setParam("q", "");
  };

  const setSort = useCallback(
    (sortKey: string) => {
      const next = new URLSearchParams(searchParams);
      const nextDirection: SortDirection = sort === sortKey && sortDirection === "asc" ? "desc" : "asc";
      next.set("sort", sortKey);
      next.set("dir", nextDirection);
      next.delete("page");
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams, sort, sortDirection],
  );

  const toggleCurrentPageSelection = (checked: boolean) => {
    setAllFilteredSelected(false);
    setSelectedIds((current) => {
      const next = new Set(current);
      for (const item of items) {
        if (checked) next.add(item.id);
        else next.delete(item.id);
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
      { label: "Website", sort: "website" },
      { label: "Lifecycle", sort: "state" },
      { label: "Translation", sort: "translation_status" },
      { label: "Domain", sort: "domain_status" },
      { label: "Financial", sort: "financial_status" },
      { label: "Enhanced", sort: "enhanced_status" },
      { label: "Last seen", sort: "last_seen_at" },
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
            placeholder="Search organization or number"
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
          label="State"
          value={lifecycleState}
          options={FILTER_OPTIONS.lifecycle_state}
          onChange={(value) => setParam("state", value)}
        />
        <FilterSelect
          label="Translation"
          value={translationStatus}
          options={FILTER_OPTIONS.translation_status}
          onChange={(value) => setParam("translation_status", value)}
        />
        <FilterSelect
          label="Domain"
          value={domainStatus}
          options={FILTER_OPTIONS.domain_status}
          onChange={(value) => setParam("domain_status", value)}
        />
        <FilterSelect
          label="Search evidence"
          value={domainSearch}
          options={FILTER_OPTIONS.domain_search}
          onChange={(value) => setParam("domain_search", value)}
        />
        <FilterSelect
          label="Financial"
          value={financialStatus}
          options={FILTER_OPTIONS.financial_status}
          onChange={(value) => setParam("financial_status", value)}
        />
        <FilterSelect
          label="Enhanced"
          value={enhancedStatus}
          options={FILTER_OPTIONS.enhanced_status}
          onChange={(value) => setParam("enhanced_status", value)}
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
          {loading ? "Loading..." : `${total.toLocaleString()} records`}
        </span>
      </div>

      {(selectedIdList.length > 0 || allFilteredSelected) && (
        <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          <span>
            {allFilteredSelected
              ? `${total.toLocaleString()} filtered records selected`
              : `${selectedIdList.length.toLocaleString()} records selected`}
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
                  <button
                    type="button"
                    className="flex h-8 items-center gap-1 text-left font-medium"
                    onClick={() => setSort(column.sort)}
                  >
                    {column.label}
                    <SortIcon active={sort === column.sort} direction={sortDirection} />
                  </button>
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
                <TableCell colSpan={columns.length + 1} className="py-12 text-center text-muted-foreground">
                  No BRREG raw records found.
                </TableCell>
              </TableRow>
            ) : (
              items.map((item) => (
                <TableRow
                  key={item.id}
                  className="cursor-pointer hover:bg-muted/40"
                  tabIndex={0}
                  onClick={() => setSelectedRecordID(item.id)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setSelectedRecordID(item.id);
                    }
                  }}
                >
                  <TableCell onClick={(event) => event.stopPropagation()}>
                    <Checkbox
                      checked={allFilteredSelected || selectedIds.has(item.id)}
                      aria-label={`Select ${item.organization_name || item.organization_number}`}
                      onCheckedChange={(checked) => toggleRowSelection(item.id, checked === true)}
                    />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span className="font-medium">{item.organization_name || "Unnamed organization"}</span>
                      <span className="font-mono text-xs text-muted-foreground">{item.organization_number}</span>
                      {item.registration_status && (
                        <span className="text-xs text-muted-foreground">{item.registration_status}</span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    {item.website ? (
                      <a
                        className="max-w-48 truncate text-sm text-primary underline-offset-4 hover:underline"
                        href={item.website}
                        onClick={(event) => event.stopPropagation()}
                        rel="noreferrer"
                        target="_blank"
                      >
                        {item.website}
                      </a>
                    ) : (
                      <span className="text-sm text-muted-foreground">-</span>
                    )}
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={item.lifecycle_state} />
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={item.translation_status} />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <StatusBadge status={item.domain_status} />
                      {item.best_domain && <span className="text-xs text-muted-foreground">{item.best_domain}</span>}
                    </div>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={item.financial_status} />
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={item.enhanced_status} />
                  </TableCell>
                  <TableCell>
                    <span className="whitespace-nowrap text-sm">{formatDate(item.last_seen_at)}</span>
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
              onClick={() => setParam("page", String(page - 1))}
            >
              Previous
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={page >= totalPages || loading}
              onClick={() => setParam("page", String(page + 1))}
            >
              Next
            </Button>
          </div>
        </div>
      )}
      <BrregRawRecordDetailSheet
        id={selectedRecordID}
        open={selectedRecordID !== null}
        onClose={() => setSelectedRecordID(null)}
      />
      <BrregRawRecordActionSheet
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
