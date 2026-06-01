import { useCallback, useEffect, useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { toast } from "sonner";
import { Check, CheckCircle, X } from "lucide-react";
import { api } from "~/lib/api";
import type { CompanySuggestion } from "~/types/api";
import { BulkReviewTable } from "~/components/app/BulkReviewTable";
import { Button } from "~/components/ui/button";

const PAGE_SIZE = 50;

export function CompanySuggestionsTab() {
  const [items, setItems] = useState<CompanySuggestion[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchPage = useCallback(async (p: number) => {
    setLoading(true);
    try {
      const res = await api.getCompanySuggestions(p, PAGE_SIZE);
      setItems(res.items);
      setTotal(res.total);
      setPage(p);
    } catch {
      toast.error("Failed to load company suggestions.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPage(1);
  }, [fetchPage]);

  const handleApprove = useCallback(async (ids: string[]) => {
    await api.bulkCompanySuggestions(ids, "approve");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  }, []);

  const handleReject = useCallback(async (ids: string[]) => {
    await api.bulkCompanySuggestions(ids, "reject");
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  }, []);

  const handleSelectAllFiltered = useCallback(async () => {
    const res = await api.getCompanySuggestionIDs();
    return res.ids;
  }, []);

  const columns = useMemo<ColumnDef<CompanySuggestion, unknown>[]>(
    () => [
      {
        id: "company",
        header: "Company",
        enableSorting: false,
        cell: ({ row }) => {
          const company = row.original.company_name || row.original.proposed_name || "Unnamed company";
          const nativeID = row.original.source_native_id || row.original.registration_number || row.original.lei;
          return (
            <div className="min-w-0">
              <div className="truncate font-medium">{company}</div>
              {nativeID ? (
                <div className="truncate text-xs text-muted-foreground">{nativeID}</div>
              ) : null}
            </div>
          );
        },
      },
      {
        accessorKey: "source_type",
        header: "Source",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="text-sm text-muted-foreground">{getValue() as string}</span>
        ),
      },
      {
        id: "sections",
        header: "Sections",
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-sm text-muted-foreground">
            {row.original.pending_count} pending / {row.original.total_count} total
          </span>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="text-sm text-muted-foreground">{getValue() as string}</span>
        ),
      },
      {
        accessorKey: "confidence",
        header: "Conf",
        enableSorting: false,
        cell: ({ getValue }) => {
          const v = getValue() as number | null;
          return (
            <span className="text-muted-foreground">
              {v != null ? Math.round(v * 100) : "—"}
            </span>
          );
        },
      },
      {
        accessorKey: "created_at",
        header: "Created",
        enableSorting: false,
        cell: ({ getValue }) => (
          <span className="text-sm text-muted-foreground">
            {new Date(getValue() as string).toLocaleDateString()}
          </span>
        ),
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
              className="h-7 w-7 bg-green-600 p-0 hover:bg-green-700"
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
              <Check className="size-4" aria-hidden="true" />
            </Button>
            <Button
              size="sm"
              variant="destructive"
              className="h-7 w-7 p-0"
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
              <X className="size-4" aria-hidden="true" />
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
        <p className="text-lg font-medium">No pending company suggestions</p>
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
