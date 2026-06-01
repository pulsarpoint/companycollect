import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ArrowUpDown, Search, X } from "lucide-react";
import { api } from "~/lib/api";
import type { RawInput } from "~/types/api";
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
import { RawInputDetailSheet } from "~/components/app/RawInputDetailSheet";

const PAGE_SIZE = 50;

const SOURCE_LABELS: Record<string, string> = {
  ariregister: "Ariregister",
  companies_house: "Companies House",
  cvr: "CVR",
  gleif: "GLEIF",
};

const SOURCE_OPTIONS = ["companies_house", "gleif", "cvr", "ariregister"];
const STATUS_OPTIONS = ["pending", "processing", "processed", "failed", "ignored", "superseded"];
const TRANSLATION_OPTIONS = ["pending", "translating", "translated", "failed"];

function validOption(value: string | null, options: string[]) {
  return value && options.includes(value) ? value : "";
}

function validHasSuggestion(value: string | null) {
  return value === "true" || value === "false" ? value : "";
}

function pageFromParams(searchParams: URLSearchParams) {
  const page = Number(searchParams.get("page") ?? "1");
  return Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
}

function currentSearchParams() {
  return new URLSearchParams(window.location.search);
}

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
}

function statusBadgeVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "pending": return "default";
    case "processing": return "secondary";
    case "processed": return "outline";
    case "failed": return "destructive";
    default: return "outline";
  }
}

function translationBadgeClass(status?: string) {
  switch (status) {
    case "translated": return "border-green-200 bg-green-100 text-green-800";
    case "failed": return "border-red-200 bg-red-100 text-red-800";
    case "translating": return "border-blue-200 bg-blue-100 text-blue-800";
    case "pending": return "border-amber-200 bg-amber-100 text-amber-800";
    default: return "";
  }
}

function SortIcon({ column, currentSort, currentDir }: { column: string; currentSort: string; currentDir: "asc" | "desc" }) {
  if (currentSort !== column) return <ArrowUpDown className="ml-1 inline size-3 text-muted-foreground" />;
  return currentDir === "asc"
    ? <ArrowUp className="ml-1 inline size-3" />
    : <ArrowDown className="ml-1 inline size-3" />;
}

export interface RawInputsTableProps {
  sourceName?: string;
  requiresTranslation: boolean;
}

export function RawInputsTable({ sourceName, requiresTranslation }: RawInputsTableProps) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState<RawInput[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(() => pageFromParams(searchParams));
  const [sourceFilter, setSourceFilter] = useState(() => validOption(searchParams.get("source"), SOURCE_OPTIONS));
  const [statusFilter, setStatusFilter] = useState(() => validOption(searchParams.get("processing_status") ?? searchParams.get("status"), STATUS_OPTIONS));
  const [translationFilter, setTranslationFilter] = useState(() => validOption(searchParams.get("translation_status"), TRANSLATION_OPTIONS));
  const [hasSuggestionFilter, setHasSuggestionFilter] = useState(() => validHasSuggestion(searchParams.get("has_suggestion")));
  const [searchQ, setSearchQ] = useState(() => searchParams.get("q") ?? "");
  const [searchInput, setSearchInput] = useState(() => searchParams.get("q") ?? "");
  const [sortCol, setSortCol] = useState(() => searchParams.get("sort") ?? "created_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">(() => searchParams.get("dir") === "asc" ? "asc" : "desc");
  const [selectedRow, setSelectedRow] = useState<RawInput | null>(null);

  const load = useCallback(
    async (
      nextPage: number,
      source: string,
      status: string,
      translationStatus: string,
      hasSuggestion: string,
      query: string,
      sort: string,
      dir: "asc" | "desc",
    ) => {
      setLoading(true);
      try {
        const res = await api.getRawInputs({
          page: nextPage,
          limit: PAGE_SIZE,
          source: (sourceName ?? source) || undefined,
          processing_status: status || undefined,
          translation_status: translationStatus || undefined,
          has_suggestion: hasSuggestion || undefined,
          q: query || undefined,
          sort,
          dir,
        });
        setItems(res.items);
        setTotal(res.total);
        setPage(nextPage);
      } finally {
        setLoading(false);
      }
    },
    [sourceName],
  );

  useEffect(() => {
    load(page, sourceFilter, statusFilter, translationFilter, hasSuggestionFilter, searchQ, sortCol, sortDir);
  }, [load, page, sourceFilter, statusFilter, translationFilter, hasSuggestionFilter, searchQ, sortCol, sortDir]);

  useEffect(() => {
    setPage(pageFromParams(searchParams));
    setSourceFilter(validOption(searchParams.get("source"), SOURCE_OPTIONS));
    setStatusFilter(validOption(searchParams.get("processing_status") ?? searchParams.get("status"), STATUS_OPTIONS));
    setTranslationFilter(validOption(searchParams.get("translation_status"), TRANSLATION_OPTIONS));
    setHasSuggestionFilter(validHasSuggestion(searchParams.get("has_suggestion")));
    const q = searchParams.get("q") ?? "";
    setSearchQ(q);
    setSearchInput(q);
    setSortCol(searchParams.get("sort") ?? "created_at");
    setSortDir(searchParams.get("dir") === "asc" ? "asc" : "desc");
  }, [searchParams]);

  function handleSort(col: string) {
    const next = currentSearchParams();
    if (col === sortCol) {
      const nextDir = sortDir === "asc" ? "desc" : "asc";
      setSortDir(nextDir);
      next.set("dir", nextDir);
    } else {
      setSortCol(col);
      next.set("sort", col);
      next.set("dir", "desc");
    }
    next.delete("page");
    setPage(1);
    setSearchParams(next, { replace: true });
  }

  function setFilterParam(key: string, value: string) {
    const next = currentSearchParams();
    if (value && !(key === "page" && value === "1")) next.set(key, value);
    else next.delete(key);
    if (key === "processing_status") next.delete("status");
    if (key !== "page") {
      next.delete("page");
      setPage(1);
    } else {
      setPage(Number(value) || 1);
    }
    setSearchParams(next, { replace: true });
  }

  const applySearch = () => setFilterParam("q", searchInput);
  const clearSearch = () => {
    setSearchInput("");
    setFilterParam("q", "");
  };
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const columns = useMemo<ColumnDef<RawInput>[]>(() => {
    const cols: ColumnDef<RawInput>[] = [];

    if (!sourceName) {
      cols.push({
        accessorKey: "source",
        header: () => (
          <button className="flex items-center font-medium" onClick={() => handleSort("source")}>
            Source <SortIcon column="source" currentSort={sortCol} currentDir={sortDir} />
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="text-xs font-medium">
            {SOURCE_LABELS[getValue() as string] ?? (getValue() as string)}
          </span>
        ),
      });
    }

    cols.push(
      {
        accessorKey: "name",
        header: () => (
          <button className="flex items-center font-medium" onClick={() => handleSort("name")}>
            Name <SortIcon column="name" currentSort={sortCol} currentDir={sortDir} />
          </button>
        ),
        cell: ({ getValue }) => <span className="font-medium">{getValue() as string}</span>,
      },
      {
        accessorKey: "native_id",
        header: "ID",
        cell: ({ getValue }) => (
          <span className="font-mono text-xs text-muted-foreground">{getValue() as string}</span>
        ),
      },
      {
        accessorKey: "status",
        header: () => (
          <button className="flex items-center font-medium" onClick={() => handleSort("status")}>
            Status <SortIcon column="status" currentSort={sortCol} currentDir={sortDir} />
          </button>
        ),
        cell: ({ getValue }) => {
          const value = getValue() as string;
          return <Badge variant={statusBadgeVariant(value)} className="text-xs">{value}</Badge>;
        },
      },
    );

    if (requiresTranslation) {
      cols.push({
        accessorKey: "translation_status",
        header: "Translation",
        enableSorting: false,
        cell: ({ row }) => {
          const status = row.original.translation_status ?? "pending";
          return (
            <Badge variant="outline" className={`text-xs ${translationBadgeClass(status)}`}>
              {status}
            </Badge>
          );
        },
      });
    }

    cols.push({
      accessorKey: "created_at",
      header: () => (
        <button className="flex items-center font-medium" onClick={() => handleSort("created_at")}>
          Created <SortIcon column="created_at" currentSort={sortCol} currentDir={sortDir} />
        </button>
      ),
      cell: ({ getValue }) => {
        const value = getValue() as string;
        return (
          <div className="text-sm">
            <div>{new Date(value).toLocaleDateString()}</div>
            <div className="text-xs text-muted-foreground">{timeAgo(value)}</div>
          </div>
        );
      },
    });

    return cols;
  }, [sourceName, requiresTranslation, sortCol, sortDir]);

  const table = useReactTable({
    data: items,
    columns,
    getRowId: (row) => row.id,
    enableRowSelection: false,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    manualPagination: true,
    pageCount: totalPages,
  });

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex items-center">
          <Search className="absolute left-2.5 size-4 text-muted-foreground" />
          <Input
            className="h-8 w-56 pl-8 pr-8 text-sm"
            placeholder="Search by name..."
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            onKeyDown={(event) => event.key === "Enter" && applySearch()}
          />
          {searchInput && (
            <button className="absolute right-2" onClick={clearSearch}>
              <X className="size-3.5 text-muted-foreground" />
            </button>
          )}
        </div>
        {searchInput !== searchQ && (
          <Button size="sm" variant="secondary" className="h-8" onClick={applySearch}>
            Search
          </Button>
        )}

        {!sourceName && (
          <select
            className="h-8 rounded-md border bg-background px-2 text-sm"
            value={sourceFilter}
            onChange={(event) => setFilterParam("source", event.target.value)}
          >
            <option value="">All sources</option>
            {SOURCE_OPTIONS.map((source) => (
              <option key={source} value={source}>{SOURCE_LABELS[source] ?? source}</option>
            ))}
          </select>
        )}

        <select
          className="h-8 rounded-md border bg-background px-2 text-sm"
          value={statusFilter}
          onChange={(event) => setFilterParam("processing_status", event.target.value)}
        >
          <option value="">All statuses</option>
          {STATUS_OPTIONS.map((status) => (
            <option key={status} value={status}>{status}</option>
          ))}
        </select>

        {requiresTranslation && (
          <select
            className="h-8 rounded-md border bg-background px-2 text-sm"
            value={translationFilter}
            onChange={(event) => setFilterParam("translation_status", event.target.value)}
          >
            <option value="">All translations</option>
            {TRANSLATION_OPTIONS.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
        )}

        <select
          className="h-8 rounded-md border bg-background px-2 text-sm"
          value={hasSuggestionFilter}
          onChange={(event) => setFilterParam("has_suggestion", event.target.value)}
        >
          <option value="">Any suggestion state</option>
          <option value="true">Has suggestion</option>
          <option value="false">No suggestion</option>
        </select>

        <span className="ml-auto text-sm text-muted-foreground">
          {loading ? "Loading..." : `${total.toLocaleString()} entries`}
        </span>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: 8 }).map((_, rowIndex) => (
                <TableRow key={rowIndex}>
                  {columns.map((_, cellIndex) => (
                    <TableCell key={cellIndex}><Skeleton className="h-4 w-full" /></TableCell>
                  ))}
                </TableRow>
              ))
            ) : table.getRowModel().rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length} className="py-12 text-center text-muted-foreground">
                  No raw inputs found.
                </TableCell>
              </TableRow>
            ) : (
              table.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => setSelectedRow(row.original)}
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
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
              onClick={() => setFilterParam("page", String(page - 1))}
            >
              Previous
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={page >= totalPages || loading}
              onClick={() => setFilterParam("page", String(page + 1))}
            >
              Next
            </Button>
          </div>
        </div>
      )}

      {selectedRow && (
        <RawInputDetailSheet
          open={!!selectedRow}
          onClose={() => setSelectedRow(null)}
          source={selectedRow.source}
          id={selectedRow.id}
        />
      )}
    </div>
  );
}
