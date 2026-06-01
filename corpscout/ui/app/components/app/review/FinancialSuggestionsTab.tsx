import { useCallback, useEffect, useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { toast } from "sonner";
import { CheckCircle } from "lucide-react";
import { api } from "~/lib/api";
import type { CompanyFinancialPending } from "~/types/api";
import { BulkReviewTable } from "~/components/app/BulkReviewTable";
import { Button } from "~/components/ui/button";

const PAGE_SIZE = 50;

function formatUsd(cents: number | null): { text: string; negative: boolean } {
  if (cents == null) return { text: "—", negative: false };
  const negative = cents < 0;
  const absCents = Math.abs(cents);
  // cents → dollars (divide by 100), then format
  const dollars = absCents / 100;
  let text: string;
  if (dollars >= 1_000_000) {
    text = `$${(dollars / 1_000_000).toFixed(1)}M`;
  } else {
    // < 1M: show in k (dollars / 1000)
    text = `$${Math.round(dollars / 1_000)}k`;
  }
  return { text: negative ? `-${text}` : text, negative };
}

export function FinancialSuggestionsTab() {
  const [items, setItems] = useState<CompanyFinancialPending[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchPage = useCallback(async (p: number) => {
    setLoading(true);
    try {
      const res = await api.getFinancialSuggestions(p, PAGE_SIZE);
      setItems(res.items);
      setTotal(res.total);
      setPage(p);
    } catch {
      toast.error("Failed to load financial suggestions.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPage(1);
  }, [fetchPage]);

  const handleApprove = useCallback(async (ids: string[]) => {
    await api.bulkFinancialSuggestions(ids, "approve");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  }, []);

  const handleReject = useCallback(async (ids: string[]) => {
    await api.bulkFinancialSuggestions(ids, "reject");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  }, []);

  const handleSelectAllFiltered = useCallback(async () => {
    const res = await api.getFinancialSuggestionIDs();
    return res.ids;
  }, []);

  const columns = useMemo<ColumnDef<CompanyFinancialPending, unknown>[]>(
    () => [
      {
        accessorKey: "company_name",
        header: "Company",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="font-medium">{getValue() as string}</span>
        ),
      },
      {
        accessorKey: "year",
        header: "Year",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="text-muted-foreground">{getValue() as number}</span>
        ),
      },
      {
        accessorKey: "source_name",
        header: "Source",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="text-muted-foreground">{getValue() as string}</span>
        ),
      },
      {
        accessorKey: "employee_count",
        header: "Employees",
        enableSorting: false,
        cell: ({ getValue }) => {
          const v = getValue() as number | null;
          return (
            <span className="text-muted-foreground">
              {v != null ? v.toLocaleString() : "—"}
            </span>
          );
        },
      },
      {
        accessorKey: "revenue_usd",
        header: "Revenue (USD)",
        enableSorting: false,
        cell: ({ getValue }) => {
          const { text } = formatUsd(getValue() as number | null);
          return <span className="text-muted-foreground">{text}</span>;
        },
      },
      {
        accessorKey: "profit_usd",
        header: "Profit (USD)",
        enableSorting: false,
        cell: ({ getValue }) => {
          const { text, negative } = formatUsd(getValue() as number | null);
          return (
            <span className={negative ? "text-red-500" : "text-muted-foreground"}>
              {text}
            </span>
          );
        },
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div
            className="flex justify-end gap-1"
            onClick={(e) => e.stopPropagation()}
          >
            <Button
              size="sm"
              variant="default"
              className="h-7 bg-green-600 hover:bg-green-700 text-xs"
              disabled={actionLoading}
              onClick={async () => {
                setActionLoading(true);
                try {
                  await handleApprove([row.original.id]);
                  toast.success("Approved.");
                } catch {
                  toast.error("Failed to approve suggestion.");
                } finally {
                  setActionLoading(false);
                }
              }}
            >
              ✓
            </Button>
            <Button
              size="sm"
              variant="destructive"
              className="h-7 text-xs"
              disabled={actionLoading}
              onClick={async () => {
                setActionLoading(true);
                try {
                  await handleReject([row.original.id]);
                  toast.success("Rejected.");
                } catch {
                  toast.error("Failed to reject suggestion.");
                } finally {
                  setActionLoading(false);
                }
              }}
            >
              ✗
            </Button>
          </div>
        ),
      },
    ],
    [handleApprove, handleReject, actionLoading],
  );

  if (!loading && total === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-muted-foreground">
        <CheckCircle className="mb-4 size-12 text-green-500" />
        <p className="text-lg font-medium">No pending financial suggestions</p>
      </div>
    );
  }

  return (
    <BulkReviewTable
      columns={columns}
      data={items}
      total={total}
      loading={loading}
      page={page}
      pageSize={PAGE_SIZE}
      onPageChange={fetchPage}
      onApprove={handleApprove}
      onReject={handleReject}
      onSelectAllFiltered={handleSelectAllFiltered}
    />
  );
}
