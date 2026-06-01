import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { type ColumnDef, type SortingState } from "@tanstack/react-table";
import { pgrest } from "~/lib/pgrest";
import type { VCompany } from "~/types/api";
import { DataTable } from "~/components/ui/data-table";
import { Badge } from "~/components/ui/badge";
import { formatDate } from "~/lib/utils";
import { api } from "~/lib/api";
import type { Country } from "~/types/api";

const PAGE_SIZE = 50;

function fmtRevenue(cents: number | null): string {
  if (cents == null) return "—";
  const usd = cents / 100;
  if (usd < 1_000) return "<$1K";
  if (usd < 1_000_000) return `$${Math.round(usd / 1_000)}K`;
  if (usd < 1_000_000_000) return `$${(usd / 1_000_000).toFixed(1)}M`;
  return `$${(usd / 1_000_000_000).toFixed(1)}B`;
}

const STATUS_COLORS: Record<string, string> = {
  active: "text-green-700 border-green-300 bg-green-50",
  inactive: "text-yellow-700 border-yellow-300 bg-yellow-50",
  dissolved: "text-red-700 border-red-300 bg-red-50",
};

const columns: ColumnDef<VCompany, unknown>[] = [
  {
    accessorKey: "name",
    header: "Company",
    enableSorting: true,
    cell: ({ row }) => (
      <Link to={`/companies/${row.original.id}`} className="font-medium hover:underline text-primary">
        {row.original.name}
      </Link>
    ),
  },
  {
    accessorKey: "country_name",
    header: "Country",
    enableSorting: true,
    cell: ({ row }) => (
      <span className="text-sm">{row.original.country_name} <span className="text-muted-foreground font-mono text-xs">{row.original.country_iso2}</span></span>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    enableSorting: true,
    cell: ({ row }) => (
      <Badge variant="outline" className={STATUS_COLORS[row.original.status] ?? ""}>
        {row.original.status}
      </Badge>
    ),
  },
  {
    accessorKey: "primary_source_display_name",
    header: "Source",
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-sm text-muted-foreground">
        {row.original.primary_source_display_name ?? row.original.primary_source ?? "—"}
      </span>
    ),
  },
  {
    accessorKey: "domain_count",
    header: "Domains",
    enableSorting: true,
    cell: ({ row }) => (
      <span className={row.original.domain_count > 0 ? "font-medium" : "text-muted-foreground"}>
        {row.original.domain_count}
      </span>
    ),
  },
  {
    accessorKey: "employee_count",
    header: "Employees",
    enableSorting: false,
    cell: ({ row }) => {
      const v = row.original.employee_count;
      return (
        <span className={v == null ? "text-muted-foreground" : ""}>
          {v == null ? "—" : v.toLocaleString()}
        </span>
      );
    },
  },
  {
    accessorKey: "revenue_usd",
    header: "Revenue",
    enableSorting: false,
    cell: ({ row }) => {
      const formatted = fmtRevenue(row.original.revenue_usd);
      return (
        <span className={formatted === "—" ? "text-muted-foreground" : ""}>
          {formatted}
        </span>
      );
    },
  },
  {
    accessorKey: "headquarters_location",
    header: "HQ",
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-sm text-muted-foreground">{row.original.headquarters_location ?? "—"}</span>
    ),
  },
  {
    accessorKey: "created_at",
    header: "Added",
    enableSorting: true,
    cell: ({ row }) => <span className="text-sm text-muted-foreground">{formatDate(row.original.created_at)}</span>,
  },
];

export default function CompaniesPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [companies, setCompanies] = useState<VCompany[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [countries, setCountries] = useState<Country[]>([]);

  const page = Math.max(1, Number(searchParams.get("page") ?? 1));
  const q = searchParams.get("q") ?? "";
  const status = searchParams.get("status") ?? "";
  const country = searchParams.get("country") ?? "";
  const sortKey = searchParams.get("sort") ?? "name";
  const sortDir = (searchParams.get("dir") ?? "asc") as "asc" | "desc";
  const minEmployees = searchParams.get("min_emp") ?? "";
  const maxEmployees = searchParams.get("max_emp") ?? "";
  const minRevUsd = searchParams.get("min_rev") ?? "";
  const maxRevUsd = searchParams.get("max_rev") ?? "";

  const sorting: SortingState = useMemo(
    () => [{ id: sortKey, desc: sortDir === "desc" }],
    [sortKey, sortDir]
  );

  useEffect(() => {
    api.getCountries().then(setCountries).catch(() => {});
  }, []);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string | number | string[]> = {
        limit: PAGE_SIZE,
        offset: (page - 1) * PAGE_SIZE,
        order: `${sortKey}.${sortDir}`,
      };
      if (q) params["name"] = `ilike.*${q}*`;
      if (status) params["status"] = `eq.${status}`;
      if (country) params["country_iso2"] = `eq.${country}`;

      // employee_count range — PostgREST needs duplicate keys, use array values
      const empFilter: string[] = [];
      if (minEmployees) empFilter.push(`gte.${minEmployees}`);
      if (maxEmployees) empFilter.push(`lte.${maxEmployees}`);
      if (empFilter.length === 1) params["employee_count"] = empFilter[0];
      else if (empFilter.length === 2) params["employee_count"] = empFilter;

      // revenue_usd range — inputs are in $M, stored as USD cents (multiply by 100_000_00)
      const revFilter: string[] = [];
      if (minRevUsd) revFilter.push(`gte.${Math.round(Number(minRevUsd) * 100_000_00)}`);
      if (maxRevUsd) revFilter.push(`lte.${Math.round(Number(maxRevUsd) * 100_000_00)}`);
      if (revFilter.length === 1) params["revenue_usd"] = revFilter[0];
      else if (revFilter.length === 2) params["revenue_usd"] = revFilter;

      const res = await pgrest<VCompany>("v_companies", params);
      setCompanies(res.data);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [page, q, status, country, sortKey, sortDir, minEmployees, maxEmployees, minRevUsd, maxRevUsd]);

  useEffect(() => { fetchData(); }, [fetchData]);

  function setParam(updates: Record<string, string>) {
    const next = new URLSearchParams(searchParams);
    for (const [k, v] of Object.entries(updates)) {
      if (v) next.set(k, v); else next.delete(k);
    }
    setSearchParams(next);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Companies</h1>
        <span className="text-sm text-muted-foreground">{total.toLocaleString()} total</span>
      </div>

      <div className="flex flex-wrap gap-2">
        <input
          className="h-9 rounded-md border border-input bg-background px-3 text-sm w-64 focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Search name…"
          defaultValue={q}
          onKeyDown={(e) => {
            if (e.key === "Enter") setParam({ q: e.currentTarget.value, page: "1" });
          }}
        />
        <select
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
          value={status}
          onChange={(e) => setParam({ status: e.target.value, page: "1" })}
        >
          <option value="">All statuses</option>
          <option value="active">Active</option>
          <option value="inactive">Inactive</option>
          <option value="dissolved">Dissolved</option>
        </select>
        <select
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
          value={country}
          onChange={(e) => setParam({ country: e.target.value, page: "1" })}
        >
          <option value="">All countries</option>
          {countries.map((c) => (
            <option key={c.id} value={c.iso_alpha2}>{c.name} ({c.iso_alpha2})</option>
          ))}
        </select>
        <input
          className="h-9 rounded-md border border-input bg-background px-3 text-sm w-32 focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Min employees"
          type="number"
          min={0}
          defaultValue={minEmployees}
          onKeyDown={(e) => {
            if (e.key === "Enter") setParam({ min_emp: e.currentTarget.value, page: "1" });
          }}
        />
        <input
          className="h-9 rounded-md border border-input bg-background px-3 text-sm w-32 focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Max employees"
          type="number"
          min={0}
          defaultValue={maxEmployees}
          onKeyDown={(e) => {
            if (e.key === "Enter") setParam({ max_emp: e.currentTarget.value, page: "1" });
          }}
        />
        <input
          className="h-9 rounded-md border border-input bg-background px-3 text-sm w-36 focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Min revenue $M"
          type="number"
          min={0}
          defaultValue={minRevUsd}
          onKeyDown={(e) => {
            if (e.key === "Enter") setParam({ min_rev: e.currentTarget.value, page: "1" });
          }}
        />
        <input
          className="h-9 rounded-md border border-input bg-background px-3 text-sm w-36 focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Max revenue $M"
          type="number"
          min={0}
          defaultValue={maxRevUsd}
          onKeyDown={(e) => {
            if (e.key === "Enter") setParam({ max_rev: e.currentTarget.value, page: "1" });
          }}
        />
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
            setParam({ sort: next[0].id, dir: next[0].desc ? "desc" : "asc", page: "1" });
          }
        }}
        onPageChange={(p) => setParam({ page: String(p) })}
        loading={loading}
      />
    </div>
  );
}
