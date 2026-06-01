import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { type ColumnDef, type SortingState } from "@tanstack/react-table";
import { toast } from "sonner";
import { api } from "~/lib/api";
import type { ReviewCandidate } from "~/types/api";
import { BulkReviewTable, type ActiveFilter, type FilterDef } from "~/components/app/BulkReviewTable";
import { ReviewSheet } from "~/components/app/ReviewSheet";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";

const FILTER_DEFS: FilterDef[] = [
  {
    key: "signal",
    label: "Signal",
    type: "select",
    options: ["certsh", "wikidata", "whois", "registry_website", "search"].map((v) => ({
      value: v,
      label: v,
    })),
  },
  { key: "min_confidence", label: "Min confidence", type: "min-number" },
];

const PAGE_SIZE = 50;

export function DomainCandidatesTab() {
  const [items, setItems] = useState<ReviewCandidate[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [activeFilters, setActiveFilters] = useState<ActiveFilter[]>([]);
  const [searchQ, setSearchQ] = useState("");
  const [selected, setSelected] = useState<ReviewCandidate | null>(null);

  const searchQRef = useRef(searchQ);
  useEffect(() => {
    searchQRef.current = searchQ;
  }, [searchQ]);

  const buildFilters = useCallback(
    (f: ActiveFilter[], q: string) => ({
      signal: f.find((x) => x.key === "signal")?.value,
      min_confidence: f.find((x) => x.key === "min_confidence")?.value
        ? Number(f.find((x) => x.key === "min_confidence")!.value)
        : undefined,
      q: q || undefined,
    }),
    [],
  );

  const fetchPage = useCallback(
    async (p: number, f: ActiveFilter[], q: string) => {
      setLoading(true);
      try {
        const res = await api.getReview(p, PAGE_SIZE, buildFilters(f, q));
        setItems(res.items);
        setTotal(res.total);
        setPage(p);
      } catch {
        toast.error("Failed to load review queue.");
      } finally {
        setLoading(false);
      }
    },
    [buildFilters],
  );

  // Initial load
  useEffect(() => {
    fetchPage(1, [], "");
  }, [fetchPage]);

  const handleFilterChange = useCallback((filters: ActiveFilter[]) => {
    setActiveFilters(filters);
    fetchPage(1, filters, searchQRef.current);
  }, [fetchPage]);

  const handleSearch = useCallback(
    (q: string) => {
      setSearchQ(q);
      fetchPage(1, activeFilters, q);
    },
    [activeFilters, fetchPage],
  );

  const handlePageChange = (p: number) => fetchPage(p, activeFilters, searchQ);

  const handleApprove = async (ids: string[]) => {
    await api.bulkReview(ids, "approved");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  };

  const handleReject = async (ids: string[]) => {
    await api.bulkReview(ids, "rejected");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  };

  const handleSelectAllFiltered = useCallback(async () => {
    const filters = buildFilters(activeFilters, searchQRef.current);
    const res = await api.getReviewIDs(filters);
    return res.ids;
  }, [activeFilters, buildFilters]);

  const handleSingleAction = useCallback(
    async (id: string, action: "approved" | "rejected" | "superseded") => {
      await api.createReview(id, action);
      setItems((prev) => prev.filter((i) => i.id !== id));
      setTotal((prev) => Math.max(0, prev - 1));
      setSelected((prev) => (prev?.id === id ? null : prev));
      toast.success(`Candidate ${action}.`);
    },
    [],
  );

  const columns = useMemo<ColumnDef<ReviewCandidate, unknown>[]>(
    () => [
      {
        accessorKey: "company_name",
        header: "Company",
        enableSorting: false,
        cell: ({ getValue }) => <span className="font-medium">{getValue() as string}</span>,
      },
      {
        accessorKey: "domain",
        header: "Domain",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-primary">{getValue() as string}</span>
        ),
      },
      {
        accessorKey: "signal",
        header: "Signal",
        enableSorting: false,
        cell: ({ getValue }) => {
          const v = getValue() as string;
          return <Badge variant="outline">{v}</Badge>;
        },
      },
      {
        accessorKey: "confidence",
        header: "Conf",
        enableSorting: false,
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return <span className="font-bold">{v}</span>;
        },
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
            <Button
              size="sm"
              variant="default"
              className="h-7 bg-green-600 hover:bg-green-700 text-xs"
              onClick={() => handleSingleAction(row.original.id, "approved")}
            >
              ✓
            </Button>
            <Button
              size="sm"
              variant="destructive"
              className="h-7 text-xs"
              onClick={() => handleSingleAction(row.original.id, "rejected")}
            >
              ✗
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="h-7 text-xs"
              onClick={() => setSelected(row.original)}
            >
              ···
            </Button>
          </div>
        ),
      },
    ],
    [handleSingleAction],
  );

  return (
    <>
      <BulkReviewTable
        columns={columns}
        data={items}
        total={total}
        loading={loading}
        page={page}
        pageSize={PAGE_SIZE}
        onPageChange={handlePageChange}
        sorting={sorting}
        onSortingChange={setSorting}
        onApprove={handleApprove}
        onReject={handleReject}
        filterDefs={FILTER_DEFS}
        onFilterChange={handleFilterChange}
        onSearch={handleSearch}
        searchPlaceholder="Search company or domain…"
        onRowClick={setSelected}
        onSelectAllFiltered={handleSelectAllFiltered}
      />
      <ReviewSheet
        candidate={selected}
        onClose={() => setSelected(null)}
        onAction={handleSingleAction}
      />
    </>
  );
}
