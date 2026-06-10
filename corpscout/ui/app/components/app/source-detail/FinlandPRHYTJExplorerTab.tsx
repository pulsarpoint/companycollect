import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import { Filter, Search } from "lucide-react";
import { type ColumnDef, type SortingState } from "@tanstack/react-table";

import { api, errorMessage } from "~/lib/api";
import { formatDate } from "~/lib/utils";
import type {
  DataSource,
  SourceExplorerCompany,
  SourceExplorerFormFilterOption,
} from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { DataTable } from "~/components/ui/data-table";
import { Input } from "~/components/ui/input";
import { SourceExplorerFiltersSheet } from "~/components/app/source-detail/SourceExplorerFiltersSheet";

const PAGE_SIZE = 50;

interface FinlandPRHYTJExplorerTabProps {
  source: DataSource;
}

function emptyText(value: string | null | undefined): string {
  return value ? value : "-";
}

function dateText(value: string | null | undefined): string {
  return value ? value : "-";
}

function countSummary(company: SourceExplorerCompany): string {
  return [
    `${company.name_history_count.toLocaleString()} names`,
    `${company.registered_entry_count.toLocaleString()} entries`,
    `${company.address_count.toLocaleString()} addresses`,
  ].join(" / ");
}

function paramList(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const raw of values) {
    for (const value of raw.split(",")) {
      const trimmed = value.trim();
      if (!trimmed || seen.has(trimmed)) continue;
      seen.add(trimmed);
      result.push(trimmed);
    }
  }
  return result;
}

function setRepeatedParam(params: URLSearchParams, key: string, values: string[]) {
  params.delete(key);
  for (const value of values) {
    if (value) params.append(key, value);
  }
}

export function FinlandPRHYTJExplorerTab({ source }: FinlandPRHYTJExplorerTabProps) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [companies, setCompanies] = useState<SourceExplorerCompany[]>([]);
  const [formOptions, setFormOptions] = useState<SourceExplorerFormFilterOption[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [loadingFilters, setLoadingFilters] = useState(true);
  const [error, setError] = useState<string>();
  const [filterError, setFilterError] = useState<string>();
  const [filtersOpen, setFiltersOpen] = useState(false);

  const page = Math.max(1, Number(searchParams.get("page") ?? 1));
  const q = searchParams.get("q") ?? "";
  const activeOnly = searchParams.get("active") === "true";
  const formCodes = useMemo(
    () => paramList(searchParams.getAll("form")),
    [searchParams],
  );
  const sortKey = searchParams.get("sort") ?? "name";
  const sortDir = (searchParams.get("dir") ?? "asc") as "asc" | "desc";
  const [searchInput, setSearchInput] = useState(q);
  const activeFilterCount = (activeOnly ? 1 : 0) + formCodes.length;

  const sorting: SortingState = useMemo(
    () => [{ id: sortKey, desc: sortDir === "desc" }],
    [sortDir, sortKey],
  );

  const columns = useMemo<ColumnDef<SourceExplorerCompany, unknown>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Company",
        enableSorting: true,
        cell: ({ row }) => (
          <div className="flex min-w-64 flex-col gap-1">
            <span className="font-medium">{emptyText(row.original.name)}</span>
            <span className="font-mono text-xs text-muted-foreground">
              {row.original.business_id}
            </span>
          </div>
        ),
      },
      {
        accessorKey: "lifecycle_status",
        header: "Status",
        enableSorting: true,
        cell: ({ row }) => (
          <div className="flex flex-col gap-1">
            <Badge variant={row.original.is_active ? "secondary" : "outline"}>
              {emptyText(row.original.lifecycle_status)}
            </Badge>
            <span className="text-xs text-muted-foreground">
              {emptyText(row.original.trade_register_status_description)}
            </span>
          </div>
        ),
      },
      {
        accessorKey: "registration_date",
        header: "Registered",
        enableSorting: true,
        cell: ({ row }) => (
          <div className="flex flex-col gap-1 text-sm">
            <span>{dateText(row.original.registration_date)}</span>
            {row.original.end_date ? (
              <span className="text-xs text-muted-foreground">
                Ended {row.original.end_date}
              </span>
            ) : null}
          </div>
        ),
      },
      {
        accessorKey: "main_business_line_description_en",
        header: "Business Line",
        enableSorting: true,
        cell: ({ row }) => (
          <div className="flex min-w-72 flex-col gap-1">
            <span className="text-sm">
              {emptyText(row.original.main_business_line_description_en)}
            </span>
            <span className="font-mono text-xs text-muted-foreground">
              {emptyText(row.original.main_business_line_code)}
            </span>
          </div>
        ),
      },
      {
        accessorKey: "company_form_description_en",
        header: "Form",
        enableSorting: true,
        cell: ({ row }) => (
          <span className="text-sm">
            {emptyText(row.original.company_form_description_en)}
          </span>
        ),
      },
      {
        accessorKey: "website",
        header: "Website",
        enableSorting: false,
        cell: ({ row }) =>
          row.original.website ? (
            <a
              href={row.original.website}
              target="_blank"
              rel="noreferrer"
              className="font-mono text-xs text-primary hover:underline"
            >
              {row.original.website}
            </a>
          ) : (
            <span className="text-muted-foreground">-</span>
          ),
      },
      {
        id: "counts",
        header: "Details",
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs text-muted-foreground">
            {countSummary(row.original)}
          </span>
        ),
      },
      {
        accessorKey: "latest_ingested_at",
        header: "Ingested",
        enableSorting: true,
        cell: ({ row }) => (
          <span className="text-xs text-muted-foreground">
            {formatDate(row.original.latest_ingested_at)}
          </span>
        ),
      },
    ],
    [],
  );

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const res = await api.getSourceExplorerCompanies(source.name, {
        offset: (page - 1) * PAGE_SIZE,
        limit: PAGE_SIZE,
        q: q || undefined,
        active: activeOnly ? "true" : undefined,
        form: formCodes,
        sort: sortKey,
        dir: sortDir,
      });
      setCompanies(Array.isArray(res.items) ? res.items : []);
      setTotal(res.total);
    } catch (err) {
      setError(errorMessage(err, "Failed to load source explorer."));
      setCompanies([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, [activeOnly, formCodes, page, q, sortDir, sortKey, source.name]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    let ignore = false;
    setLoadingFilters(true);
    setFilterError(undefined);
    api
      .getSourceExplorerFilterOptions(source.name)
      .then((res) => {
        if (!ignore) setFormOptions(Array.isArray(res.forms) ? res.forms : []);
      })
      .catch((err) => {
        if (!ignore) {
          setFormOptions([]);
          setFilterError(errorMessage(err, "Failed to load filter options."));
        }
      })
      .finally(() => {
        if (!ignore) setLoadingFilters(false);
      });
    return () => {
      ignore = true;
    };
  }, [source.name]);

  useEffect(() => {
    setSearchInput(q);
  }, [q]);

  function setParam(updates: Record<string, string>) {
    const next = new URLSearchParams(searchParams);
    for (const [key, value] of Object.entries(updates)) {
      if (value) next.set(key, value);
      else next.delete(key);
    }
    if (!("page" in updates)) next.delete("page");
    setSearchParams(next, { replace: true });
  }

  function applySearch() {
    setParam({ q: searchInput.trim() });
  }

  function clearFilters() {
    setSearchInput("");
    const next = new URLSearchParams(searchParams);
    next.delete("q");
    next.delete("active");
    next.delete("form");
    next.delete("page");
    setSearchParams(next, { replace: true });
  }

  function applyFilters(value: { activeOnly: boolean; formCodes: string[] }) {
    const next = new URLSearchParams(searchParams);
    if (value.activeOnly) next.set("active", "true");
    else next.delete("active");
    setRepeatedParam(next, "form", value.formCodes);
    next.delete("page");
    setSearchParams(next, { replace: true });
  }

  return (
    <div className="flex flex-col gap-4">
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center gap-2">
        <Input
          className="w-80"
          placeholder="Search name, business ID, line, form, website"
          value={searchInput}
          onChange={(event) => setSearchInput(event.target.value)}
          onKeyDown={(event) => event.key === "Enter" && applySearch()}
        />
        <Button size="sm" onClick={applySearch} disabled={loading}>
          <Search data-icon="inline-start" />
          Search
        </Button>
        <Button
          size="sm"
          variant={activeFilterCount > 0 ? "secondary" : "outline"}
          onClick={() => setFiltersOpen(true)}
        >
          <Filter data-icon="inline-start" />
          Filters
          {activeFilterCount > 0 ? (
            <Badge variant="outline">{activeFilterCount}</Badge>
          ) : null}
        </Button>
        {(q || activeOnly || formCodes.length > 0) ? (
          <Button size="sm" variant="ghost" onClick={clearFilters}>
            Clear
          </Button>
        ) : null}
        <span className="ml-auto text-sm text-muted-foreground">
          {loading ? "Loading..." : `${total.toLocaleString()} companies`}
        </span>
      </div>

      <DataTable
        columns={columns}
        data={companies}
        total={total}
        page={page}
        pageSize={PAGE_SIZE}
        sorting={sorting}
        onSortingChange={(updater) => {
          const next = typeof updater === "function" ? updater(sorting) : updater;
          if (next.length > 0) {
            setParam({
              sort: next[0].id,
              dir: next[0].desc ? "desc" : "asc",
            });
          }
        }}
        onPageChange={(nextPage) => setParam({ page: String(nextPage) })}
        loading={loading}
      />

      <SourceExplorerFiltersSheet
        open={filtersOpen}
        onOpenChange={setFiltersOpen}
        forms={formOptions}
        value={{ activeOnly, formCodes }}
        loading={loadingFilters}
        error={filterError}
        onApply={applyFilters}
        onClear={clearFilters}
      />
    </div>
  );
}
