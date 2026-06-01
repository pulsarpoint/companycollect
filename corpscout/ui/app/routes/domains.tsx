import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { type ColumnDef, type SortingState } from "@tanstack/react-table";
import { pgrest } from "~/lib/pgrest";
import type { VDomain } from "~/types/api";
import { DataTable } from "~/components/ui/data-table";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { UploadDomainsDialog } from "~/components/app/UploadDomainsDialog";
import { signalColor, confidenceColor, formatDate } from "~/lib/utils";

const PAGE_SIZE = 50;

const SIGNALS = ["registry_website", "wikidata", "certsh", "whois", "search"] as const;

export default function DomainsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [domains, setDomains] = useState<VDomain[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [uploadOpen, setUploadOpen] = useState(false);

  const page = Math.max(1, Number(searchParams.get("page") ?? 1));
  const signal = searchParams.get("signal") ?? "";
  const minConf = searchParams.get("min_confidence") ?? "";
  const orphan = searchParams.get("orphan") === "1";
  const sortKey = searchParams.get("sort") ?? "first_seen_at";
  const sortDir = (searchParams.get("dir") ?? "desc") as "asc" | "desc";
  const q = searchParams.get("q") ?? "";

  // Local input state so typing doesn't immediately trigger a fetch on each keystroke.
  const [inputValue, setInputValue] = useState(q);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Keep local input in sync when the URL param changes externally (e.g. back/forward).
  useEffect(() => { setInputValue(q); }, [q]);

  const columns = useMemo<ColumnDef<VDomain, unknown>[]>(() => {
    return [
      {
        accessorKey: "domain",
        header: "Domain",
        enableSorting: true,
        cell: ({ row }) => (
          <Link
            to={`/domains/${row.original.id}`}
            className="font-mono font-medium hover:underline"
          >
            {row.original.domain}
          </Link>
        ),
      },
      {
        accessorKey: "primary_company_name",
        header: "Company",
        enableSorting: true,
        cell: ({ row }) =>
          row.original.primary_company_id ? (
            <Link
              to={`/companies/${row.original.primary_company_id}`}
              className="hover:underline text-sm"
            >
              {row.original.primary_company_name}
            </Link>
          ) : (
            <span className="text-muted-foreground text-sm">No company</span>
          ),
      },
      {
        accessorKey: "primary_signal",
        header: "Signal",
        enableSorting: true,
        cell: ({ row }) =>
          row.original.primary_signal ? (
            <Badge variant="outline" className={signalColor(row.original.primary_signal)}>
              {row.original.primary_signal}
            </Badge>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
      },
      {
        accessorKey: "max_confidence",
        header: "Confidence",
        enableSorting: true,
        cell: ({ row }) =>
          row.original.max_confidence != null ? (
            <span className={`font-bold ${confidenceColor(row.original.max_confidence)}`}>
              {row.original.max_confidence}
            </span>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
      },
      {
        accessorKey: "company_count",
        header: "Companies",
        enableSorting: true,
        cell: ({ row }) => (
          <span className={row.original.company_count === 0 ? "text-muted-foreground" : "font-medium"}>
            {row.original.company_count}
          </span>
        ),
      },
      {
        accessorKey: "first_seen_at",
        header: "First Seen",
        enableSorting: true,
        cell: ({ row }) => (
          <span className="text-sm text-muted-foreground">
            {row.original.first_seen_at ? formatDate(row.original.first_seen_at) : "—"}
          </span>
        ),
      },
    ];
  }, []);

  const sorting: SortingState = useMemo(
    () => [{ id: sortKey, desc: sortDir === "desc" }],
    [sortKey, sortDir]
  );

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string | number> = {
        limit: PAGE_SIZE,
        offset: (page - 1) * PAGE_SIZE,
        order: `${sortKey}.${sortDir}`,
      };
      if (signal) params["primary_signal"] = `eq.${signal}`;
      if (minConf) params["max_confidence"] = `gte.${minConf}`;
      if (orphan) params["company_count"] = "eq.0";
      if (q) params["domain"] = `ilike.*${q}*`;

      const res = await pgrest<VDomain>("v_domains", params);
      setDomains(res.data);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [page, signal, minConf, orphan, sortKey, sortDir, q]);

  useEffect(() => { fetchData(); }, [fetchData]);

  function setParam(updates: Record<string, string>) {
    const next = new URLSearchParams(searchParams);
    for (const [k, v] of Object.entries(updates)) {
      if (v) next.set(k, v); else next.delete(k);
    }
    next.set("page", updates.page ?? "1");
    setSearchParams(next);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Domains</h1>
        <div className="flex items-center gap-3">
          <span className="text-sm text-muted-foreground">{total.toLocaleString()} total</span>
          <Button variant="outline" size="sm" onClick={() => setUploadOpen(true)}>
            Upload CSV
          </Button>
        </div>
      </div>

      <div className="flex flex-wrap gap-2 items-center">
        <input
          type="search"
          placeholder="Search domain…"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring w-52"
          value={inputValue}
          onChange={(e) => {
            const val = e.target.value;
            setInputValue(val);
            if (debounceRef.current) clearTimeout(debounceRef.current);
            debounceRef.current = setTimeout(() => setParam({ q: val }), 400);
          }}
        />
        <select
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
          value={signal}
          onChange={(e) => setParam({ signal: e.target.value })}
        >
          <option value="">All signals</option>
          {SIGNALS.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
          value={minConf}
          onChange={(e) => setParam({ min_confidence: e.target.value })}
        >
          <option value="">Any confidence</option>
          <option value="50">≥ 50</option>
          <option value="65">≥ 65</option>
          <option value="75">≥ 75</option>
          <option value="90">≥ 90</option>
        </select>
        <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
          <input
            type="checkbox"
            className="rounded border-input"
            checked={orphan}
            onChange={(e) => setParam({ orphan: e.target.checked ? "1" : "" })}
          />
          Unlinked domains only
        </label>
      </div>

      <DataTable
        columns={columns}
        data={domains}
        total={total}
        page={page}
        pageSize={PAGE_SIZE}
        sorting={sorting}
        onSortingChange={(updater) => {
          const next = typeof updater === "function" ? updater(sorting) : updater;
          if (next.length > 0) {
            setParam({ sort: next[0].id, dir: next[0].desc ? "desc" : "asc", page: "1" });
          }
        }}
        onPageChange={(p) => setParam({ page: String(p) })}
        loading={loading}
      />

      <UploadDomainsDialog
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        onSuccess={() => fetchData()}
      />
    </div>
  );
}
