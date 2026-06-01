# Review Page — Bulk Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the plain review page with a tabbed, TanStack Table–powered layout featuring column sorting, server-side filtering, multi-row selection, and bulk approve/reject — backed by a shared `BulkReviewTable<TData>` component that every future review tab reuses.

**Architecture:** The backend gains text search + signal/confidence filters on `GET /api/v1/review` and a new `POST /api/v1/review/bulk` endpoint. On the frontend, a generic `BulkReviewTable<TData>` component owns all selection/filter/bulk-action logic; each tab (`DomainCandidatesTab`, `CompanySuggestionsTab`) is a thin wrapper that provides columns, data, and two action callbacks.

**Tech Stack:** Go (Chi, sqlc, pgx), React Router v7, TanStack Table v8, shadcn/ui (Checkbox, Popover, Tabs), Sonner toasts.

---

## File Map

| File | Action |
|------|--------|
| `database/queries/domains.sql` | Modify — add `q` param to ListDomains + CountDomains, add JOINs to CountDomains |
| `scheduler/internal/db/gen/` | Regenerated — `make sqlc-generate` |
| `scheduler/internal/httpapi/review.go` | Modify — add signal/min_confidence/q params, add handleBulkReview |
| `scheduler/internal/httpapi/handlers.go` | Modify — register POST /review/bulk |
| `ui/app/components/ui/checkbox.tsx` | Add via shadcn CLI |
| `ui/app/components/ui/popover.tsx` | Add via shadcn CLI |
| `ui/app/lib/api.ts` | Modify — update getReview params, add bulkReview |
| `ui/app/types/api.ts` | Modify — add CompanySuggestion interface |
| `ui/app/components/app/BulkReviewTable.tsx` | Create — shared table engine |
| `ui/app/components/app/review/DomainCandidatesTab.tsx` | Create |
| `ui/app/components/app/review/CompanySuggestionsTab.tsx` | Create |
| `ui/app/routes/review.tsx` | Rewrite — thin tab orchestrator |
| `ui/app/components/app/ReviewTable.tsx` | Delete |

---

### Task 1: Backend — Add text search to ListDomains + CountDomains

**Files:**
- Modify: `database/queries/domains.sql`
- Regenerate: `scheduler/internal/db/gen/` (run `make sqlc-generate`)

- [ ] **Step 1: Update `database/queries/domains.sql`**

Replace both `ListDomains` and `CountDomains` queries. `CountDomains` needs JOINs added (it previously only queried `company_domains`; the `q` filter references `companies.name` and `domains.domain`).

```sql
-- name: ListDomains :many
SELECT d.domain, c.name AS company_name, cd.*
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
  AND (sqlc.narg('q')::text IS NULL
       OR c.name ILIKE '%' || sqlc.narg('q') || '%'
       OR d.domain ILIKE '%' || sqlc.narg('q') || '%')
ORDER BY cd.confidence DESC, d.domain
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountDomains :one
SELECT COUNT(*) FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
  AND (sqlc.narg('q')::text IS NULL
       OR c.name ILIKE '%' || sqlc.narg('q') || '%'
       OR d.domain ILIKE '%' || sqlc.narg('q') || '%');
```

- [ ] **Step 2: Regenerate sqlc**

```bash
cd scheduler
GOWORK=off make sqlc-generate
```

Expected: no errors, files updated in `scheduler/internal/db/gen/`.

- [ ] **Step 3: Verify generated struct has `Q` field**

```bash
grep "Q " scheduler/internal/db/gen/domains.sql.go
```

Expected output contains:
```
Q             *string `json:"q"`
```

- [ ] **Step 4: Verify build still compiles**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output (clean build).

- [ ] **Step 5: Commit**

```bash
git add database/queries/domains.sql scheduler/internal/db/gen/
git commit -m "feat(db): add text search param to ListDomains and CountDomains"
```

---

### Task 2: Backend — Review endpoint filters + bulk action

**Files:**
- Modify: `scheduler/internal/httpapi/review.go`
- Modify: `scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Write failing tests first**

Create `scheduler/internal/httpapi/review_test.go`:

```go
package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestHandleListReview_filters_by_signal(t *testing.T) {
	q := &stubQuerier{}
	signal := "certsh"
	status := "needs_review"
	q.On("ListDomains", mock.Anything, db.ListDomainsParams{
		Status: &status,
		Signal: &signal,
		Offset: 0,
		Limit:  50,
	}).Return([]db.ListDomainsRow{{Domain: "example.com", CompanyName: "Acme"}}, nil)
	q.On("CountDomains", mock.Anything, db.CountDomainsParams{
		Status: &status,
		Signal: &signal,
	}).Return(int64(1), nil)

	r := routerForHandlers(q)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/review?signal=certsh", nil)
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, float64(1), body["total"])
	q.AssertExpectations(t)
}

func TestHandleBulkReview_approves_multiple(t *testing.T) {
	q := &stubQuerier{}
	q.On("ReviewCompanyDomain", mock.Anything, mock.MatchedBy(func(p db.ReviewCompanyDomainParams) bool {
		return p.Status == "active"
	})).Return(nil).Times(2)

	r := routerForHandlers(q)
	body := map[string]any{
		"ids":    []string{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
		"action": "approved",
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/review/bulk", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["updated"])
	q.AssertExpectations(t)
}

func TestHandleBulkReview_rejects_invalid_action(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)
	body := map[string]any{"ids": []string{"00000000-0000-0000-0000-000000000001"}, "action": "bogus"}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/review/bulk", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi/... 2>&1 | grep -E "FAIL|undefined|no field"
```

Expected: build error — `db.ListDomainsParams` has no field `Q` yet (Task 1 must be done first), OR test functions compile but route doesn't exist. After Task 1 the struct has `Q`; tests fail because `handleBulkReview` doesn't exist.

- [ ] **Step 3: Rewrite `scheduler/internal/httpapi/review.go`**

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (h *Handlers) handleListReview(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := int32((page - 1) * limit)
	status := "needs_review"

	var minConf *int16
	if s := r.URL.Query().Get("min_confidence"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			v := int16(n)
			minConf = &v
		}
	}

	params := db.ListDomainsParams{
		Status:        &status,
		Signal:        queryString(r, "signal"),
		MinConfidence: minConf,
		Q:             queryString(r, "q"),
		Offset:        offset,
		Limit:         int32(limit),
	}

	items, err := h.db.ListDomains(r.Context(), params)
	if err != nil {
		slog.Error("list review queue", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, err := h.db.CountDomains(r.Context(), db.CountDomainsParams{
		Status:        &status,
		Signal:        params.Signal,
		MinConfidence: params.MinConfidence,
		Q:             params.Q,
	})
	if err != nil {
		slog.Error("count review queue", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "total": total, "page": page, "limit": limit,
	})
}

func (h *Handlers) handleCreateReview(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	var body struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	status, ok := reviewActionToStatus(body.Action)
	if !ok {
		writeError(w, http.StatusBadRequest, "action must be approved, rejected, or superseded")
		return
	}

	if err := h.db.ReviewCompanyDomain(r.Context(), db.ReviewCompanyDomainParams{
		ID:     id,
		Status: status,
	}); err != nil {
		slog.Error("review company domain", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) handleBulkReview(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs    []string `json:"ids"`
		Action string   `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(body.IDs) == 0 {
		writeError(w, http.StatusBadRequest, "ids must not be empty")
		return
	}
	status, ok := reviewActionToStatus(body.Action)
	if !ok {
		writeError(w, http.StatusBadRequest, "action must be approved, rejected, or superseded")
		return
	}

	updated := 0
	for _, idStr := range body.IDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		if err := h.db.ReviewCompanyDomain(r.Context(), db.ReviewCompanyDomainParams{
			ID:     id,
			Status: status,
		}); err != nil {
			slog.Error("bulk review company domain", "id", id, "error", err)
			continue
		}
		updated++
	}
	writeJSON(w, http.StatusOK, map[string]any{"updated": updated})
}

func reviewActionToStatus(action string) (string, bool) {
	switch action {
	case "approved":
		return "active", true
	case "rejected":
		return "rejected", true
	case "superseded":
		return "superseded", true
	default:
		return "", false
	}
}
```

- [ ] **Step 4: Register `POST /review/bulk` in `scheduler/internal/httpapi/handlers.go`**

Find the line `r.Post("/review/{id}/reviews", h.handleCreateReview)` and add the bulk route directly after it:

```go
r.Get("/review", h.handleListReview)
r.Post("/review/{id}/reviews", h.handleCreateReview)
r.Post("/review/bulk", h.handleBulkReview)
```

- [ ] **Step 5: Run tests — expect pass**

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi/... -v -run "TestHandleListReview\|TestHandleBulkReview" 2>&1
```

Expected:
```
--- PASS: TestHandleListReview_filters_by_signal
--- PASS: TestHandleBulkReview_approves_multiple
--- PASS: TestHandleBulkReview_rejects_invalid_action
```

- [ ] **Step 6: Run full test suite**

```bash
cd scheduler && GOWORK=off make test 2>&1 | tail -15
```

Expected: all packages `ok`, no `FAIL`.

- [ ] **Step 7: Rebuild scheduler and restart**

```bash
cd scheduler && GOWORK=off make build
docker compose build scheduler && docker compose up -d scheduler
```

- [ ] **Step 8: Smoke-test the new params**

```bash
curl -s "http://localhost:8094/api/v1/review?signal=certsh&min_confidence=80&limit=2" | python3 -m json.tool | head -20
```

Expected: JSON with `items`, all items having `"signal": "certsh"` and `confidence >= 80`.

```bash
curl -s -X POST "http://localhost:8094/api/v1/review/bulk" \
  -H "Content-Type: application/json" \
  -d '{"ids":[],"action":"approved"}' | python3 -m json.tool
```

Expected: `{"error": "ids must not be empty"}` with 400 status.

- [ ] **Step 9: Commit**

```bash
git add scheduler/internal/httpapi/review.go scheduler/internal/httpapi/review_test.go scheduler/internal/httpapi/handlers.go
git commit -m "feat(api): add signal/confidence/search filters and bulk review endpoint"
```

---

### Task 3: Frontend — shadcn components + api.ts update

**Files:**
- Add: `ui/app/components/ui/checkbox.tsx`
- Add: `ui/app/components/ui/popover.tsx`
- Modify: `ui/app/lib/api.ts`

- [ ] **Step 1: Add shadcn checkbox and popover**

```bash
cd ui && pnpm dlx shadcn@latest add checkbox popover
```

Expected: two new files created:
- `app/components/ui/checkbox.tsx`
- `app/components/ui/popover.tsx`

- [ ] **Step 2: Verify files exist**

```bash
ls ui/app/components/ui/checkbox.tsx ui/app/components/ui/popover.tsx
```

Expected: both files listed without error.

- [ ] **Step 3: Update `ui/app/lib/api.ts`**

Replace the `getReview` and `createReview` lines, and add `bulkReview`. Find:

```typescript
  getReview: (page = 1, limit = 50) =>
    get<ReviewListResponse>(`/review?page=${page}&limit=${limit}`),

  createReview: (id: string, action: "approved" | "rejected" | "superseded") =>
    post<unknown>(`/review/${id}/reviews`, { action, reviewed_by: "ops" }),
```

Replace with:

```typescript
  getReview: (
    page = 1,
    limit = 50,
    filters?: { signal?: string; min_confidence?: number; q?: string },
  ) => {
    const qs = new URLSearchParams({ page: String(page), limit: String(limit) });
    if (filters?.signal) qs.set("signal", filters.signal);
    if (filters?.min_confidence != null) qs.set("min_confidence", String(filters.min_confidence));
    if (filters?.q) qs.set("q", filters.q);
    return get<ReviewListResponse>(`/review?${qs.toString()}`);
  },

  createReview: (id: string, action: "approved" | "rejected" | "superseded") =>
    post<unknown>(`/review/${id}/reviews`, { action, reviewed_by: "ops" }),

  bulkReview: (ids: string[], action: "approved" | "rejected" | "superseded") =>
    post<{ updated: number }>("/review/bulk", { ids, action }),
```

- [ ] **Step 4: Run typecheck**

```bash
cd ui && pnpm typecheck 2>&1 | head -20
```

Expected: no errors related to api.ts.

- [ ] **Step 5: Commit**

```bash
git add ui/app/components/ui/checkbox.tsx ui/app/components/ui/popover.tsx ui/app/lib/api.ts
git commit -m "feat(ui): add checkbox/popover shadcn components, update review API methods"
```

---

### Task 4: Frontend — BulkReviewTable shared component

**Files:**
- Create: `ui/app/components/app/BulkReviewTable.tsx`

- [ ] **Step 1: Create `ui/app/components/app/BulkReviewTable.tsx`**

```tsx
import { useState, useEffect, useRef, useMemo } from "react";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type OnChangeFn,
  type RowSelectionState,
} from "@tanstack/react-table";
import { ChevronUp, ChevronDown, ChevronsUpDown, X, SlidersHorizontal } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Badge } from "~/components/ui/badge";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Popover, PopoverContent, PopoverTrigger } from "~/components/ui/popover";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export type FilterDef =
  | { key: string; label: string; type: "select"; options: { value: string; label: string }[] }
  | { key: string; label: string; type: "min-number" };

export interface ActiveFilter {
  key: string;
  label: string;
  value: string;
  display: string;
}

interface BulkReviewTableProps<TData extends { id: string }> {
  columns: ColumnDef<TData, unknown>[];
  data: TData[];
  total: number;
  loading: boolean;
  page: number;
  pageSize?: number;
  onPageChange: (p: number) => void;
  sorting?: SortingState;
  onSortingChange?: OnChangeFn<SortingState>;
  onApprove: (ids: string[]) => Promise<void>;
  onReject: (ids: string[]) => Promise<void>;
  filterDefs?: FilterDef[];
  onFilterChange?: (filters: ActiveFilter[]) => void;
  onSearch?: (q: string) => void;
  onRowClick?: (row: TData) => void;
}

export function BulkReviewTable<TData extends { id: string }>({
  columns,
  data,
  total,
  loading,
  page,
  pageSize = 50,
  onPageChange,
  sorting = [],
  onSortingChange,
  onApprove,
  onReject,
  filterDefs = [],
  onFilterChange,
  onSearch,
  onRowClick,
}: BulkReviewTableProps<TData>) {
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});
  const [bulkLoading, setBulkLoading] = useState(false);
  const [activeFilters, setActiveFilters] = useState<ActiveFilter[]>([]);
  const [searchValue, setSearchValue] = useState("");
  const [filterOpen, setFilterOpen] = useState(false);
  const [pendingFilter, setPendingFilter] = useState<Record<string, string>>({});
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => onSearch?.(searchValue), 300);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [searchValue, onSearch]);

  const allColumns = useMemo<ColumnDef<TData, unknown>[]>(
    () => [
      {
        id: "select",
        header: ({ table }) => (
          <Checkbox
            checked={table.getIsAllPageRowsSelected()}
            onCheckedChange={(v) => table.toggleAllPageRowsSelected(!!v)}
            aria-label="Select all"
          />
        ),
        cell: ({ row }) => (
          <Checkbox
            checked={row.getIsSelected()}
            onCheckedChange={(v) => row.toggleSelected(!!v)}
            aria-label="Select row"
            onClick={(e) => e.stopPropagation()}
          />
        ),
        enableSorting: false,
      },
      ...columns,
    ],
    [columns],
  );

  const table = useReactTable({
    data,
    columns: allColumns,
    state: { sorting, rowSelection },
    getRowId: (row) => row.id,
    onSortingChange,
    onRowSelectionChange: setRowSelection,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    manualPagination: true,
    pageCount: Math.ceil(total / pageSize),
    enableRowSelection: true,
  });

  const selectedIds = Object.keys(rowSelection);
  const pageCount = Math.ceil(total / pageSize);
  const start = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, total);

  const handleBulkAction = async (action: "approve" | "reject") => {
    if (selectedIds.length === 0) return;
    setBulkLoading(true);
    try {
      if (action === "approve") {
        await onApprove(selectedIds);
        toast.success(`Approved ${selectedIds.length} items.`);
      } else {
        await onReject(selectedIds);
        toast.success(`Rejected ${selectedIds.length} items.`);
      }
      setRowSelection({});
    } catch {
      toast.error("Bulk action failed. Some items may not have been updated.");
    } finally {
      setBulkLoading(false);
    }
  };

  const applyFilter = () => {
    const newFilters: ActiveFilter[] = Object.entries(pendingFilter)
      .filter(([, v]) => v !== "")
      .map(([key, value]) => {
        const def = filterDefs.find((d) => d.key === key)!;
        const display =
          def.type === "select"
            ? (def.options.find((o) => o.value === value)?.label ?? value)
            : `≥ ${value}`;
        return { key, label: def.label, value, display };
      });
    setActiveFilters(newFilters);
    onFilterChange?.(newFilters);
    setFilterOpen(false);
  };

  const removeFilter = (key: string) => {
    const updated = activeFilters.filter((f) => f.key !== key);
    setActiveFilters(updated);
    setPendingFilter((prev) => { const next = { ...prev }; delete next[key]; return next; });
    onFilterChange?.(updated);
  };

  const clearFilters = () => {
    setActiveFilters([]);
    setPendingFilter({});
    onFilterChange?.([]);
  };

  const openFilter = () => {
    const init: Record<string, string> = {};
    for (const f of activeFilters) init[f.key] = f.value;
    setPendingFilter(init);
    setFilterOpen(true);
  };

  return (
    <div className="space-y-3">
      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2">
        {onSearch && (
          <Input
            placeholder="Search company or domain…"
            value={searchValue}
            onChange={(e) => setSearchValue(e.target.value)}
            className="h-8 w-56"
          />
        )}
        {filterDefs.length > 0 && (
          <Popover open={filterOpen} onOpenChange={setFilterOpen}>
            <PopoverTrigger asChild>
              <Button variant="outline" size="sm" className="h-8" onClick={openFilter}>
                <SlidersHorizontal className="mr-1 size-3" />
                Filter
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-64 space-y-3 p-4" align="start">
              {filterDefs.map((def) => (
                <div key={def.key} className="space-y-1">
                  <label className="text-xs font-medium text-muted-foreground">{def.label}</label>
                  {def.type === "select" ? (
                    <select
                      className="w-full rounded-md border bg-background px-2 py-1 text-sm"
                      value={pendingFilter[def.key] ?? ""}
                      onChange={(e) => setPendingFilter((p) => ({ ...p, [def.key]: e.target.value }))}
                    >
                      <option value="">Any</option>
                      {def.options.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  ) : (
                    <Input
                      type="number"
                      placeholder="e.g. 70"
                      className="h-8"
                      value={pendingFilter[def.key] ?? ""}
                      onChange={(e) => setPendingFilter((p) => ({ ...p, [def.key]: e.target.value }))}
                    />
                  )}
                </div>
              ))}
              <div className="flex gap-2 pt-1">
                <Button size="sm" className="flex-1" onClick={applyFilter}>Apply</Button>
                <Button size="sm" variant="outline" onClick={() => setFilterOpen(false)}>Cancel</Button>
              </div>
            </PopoverContent>
          </Popover>
        )}
        {activeFilters.map((f) => (
          <Badge key={f.key} variant="secondary" className="gap-1 text-xs">
            {f.label}: {f.display}
            <button onClick={() => removeFilter(f.key)} className="ml-1 opacity-60 hover:opacity-100">
              <X className="size-3" />
            </button>
          </Badge>
        ))}
        {activeFilters.length > 0 && (
          <button onClick={clearFilters} className="text-xs text-muted-foreground hover:text-foreground">
            Clear all
          </button>
        )}
        <span className="ml-auto text-xs text-muted-foreground">
          {total.toLocaleString()} total
        </span>
      </div>

      {/* Bulk action bar */}
      {selectedIds.length > 0 && (
        <div className="flex items-center gap-3 rounded-md border border-blue-300 bg-blue-50 px-3 py-2 dark:border-blue-800 dark:bg-blue-950">
          <span className="text-sm font-medium text-blue-700 dark:text-blue-300">
            {selectedIds.length} selected
          </span>
          <Button
            size="sm"
            variant="default"
            className="h-7 bg-green-600 hover:bg-green-700"
            disabled={bulkLoading}
            onClick={() => handleBulkAction("approve")}
          >
            Approve {selectedIds.length}
          </Button>
          <Button
            size="sm"
            variant="destructive"
            className="h-7"
            disabled={bulkLoading}
            onClick={() => handleBulkAction("reject")}
          >
            Reject {selectedIds.length}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="ml-auto h-7"
            onClick={() => setRowSelection({})}
          >
            Deselect all
          </Button>
        </div>
      )}

      {/* Table */}
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id}>
                {hg.headers.map((header) => {
                  const canSort = header.column.getCanSort();
                  const sorted = header.column.getIsSorted();
                  return (
                    <TableHead
                      key={header.id}
                      className={canSort ? "cursor-pointer select-none" : ""}
                      onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                    >
                      <span className="flex items-center gap-1">
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {canSort && (
                          sorted === "asc" ? <ChevronUp className="size-3" /> :
                          sorted === "desc" ? <ChevronDown className="size-3" /> :
                          <ChevronsUpDown className="size-3 opacity-40" />
                        )}
                      </span>
                    </TableHead>
                  );
                })}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={allColumns.length} className="h-32 text-center text-muted-foreground">
                  Loading…
                </TableCell>
              </TableRow>
            ) : table.getRowModel().rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={allColumns.length} className="h-32 text-center">
                  <div className="text-muted-foreground">No results.</div>
                  {activeFilters.length > 0 && (
                    <button onClick={clearFilters} className="mt-1 text-xs text-primary hover:underline">
                      Clear filters
                    </button>
                  )}
                </TableCell>
              </TableRow>
            ) : (
              table.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  data-state={row.getIsSelected() ? "selected" : undefined}
                  className={onRowClick ? "cursor-pointer" : ""}
                  onClick={onRowClick ? () => onRowClick(row.original) : undefined}
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

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-muted-foreground">
        <span>{total === 0 ? "No results" : `${start}–${end} of ${total.toLocaleString()}`}</span>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
            Previous
          </Button>
          <span>Page {page} of {pageCount || 1}</span>
          <Button variant="outline" size="sm" disabled={page >= pageCount} onClick={() => onPageChange(page + 1)}>
            Next
          </Button>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Run typecheck**

```bash
cd ui && pnpm typecheck 2>&1 | grep -i "bulkreview\|error" | head -20
```

Expected: no errors related to `BulkReviewTable.tsx`.

- [ ] **Step 3: Commit**

```bash
git add ui/app/components/app/BulkReviewTable.tsx
git commit -m "feat(ui): add BulkReviewTable shared component with selection, filters, bulk actions"
```

---

### Task 5: Frontend — DomainCandidatesTab

**Files:**
- Create: `ui/app/components/app/review/DomainCandidatesTab.tsx`

- [ ] **Step 1: Create directory**

```bash
mkdir -p ui/app/components/app/review
```

- [ ] **Step 2: Create `ui/app/components/app/review/DomainCandidatesTab.tsx`**

```tsx
import { useCallback, useState } from "react";
import { type ColumnDef, type SortingState } from "@tanstack/react-table";
import { toast } from "sonner";
import { api } from "~/lib/api";
import { signalColor, confidenceColor } from "~/lib/utils";
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
  useState(() => { fetchPage(1, [], ""); });

  const handleFilterChange = (filters: ActiveFilter[]) => {
    setActiveFilters(filters);
    fetchPage(1, filters, searchQ);
  };

  const handleSearch = (q: string) => {
    setSearchQ(q);
    fetchPage(1, activeFilters, q);
  };

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

  const handleSingleAction = async (id: string, action: "approved" | "rejected" | "superseded") => {
    await api.createReview(id, action);
    setItems((prev) => prev.filter((i) => i.id !== id));
    setTotal((prev) => Math.max(0, prev - 1));
    if (selected?.id === id) setSelected(null);
    toast.success(`Candidate ${action}.`);
  };

  const columns: ColumnDef<ReviewCandidate, unknown>[] = [
    {
      accessorKey: "company_name",
      header: "Company",
      enableSorting: true,
      cell: ({ getValue }) => <span className="font-medium">{getValue() as string}</span>,
    },
    {
      accessorKey: "domain",
      header: "Domain",
      enableSorting: true,
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
        return (
          <Badge className={signalColor(v)} variant="outline">
            {v}
          </Badge>
        );
      },
    },
    {
      accessorKey: "confidence",
      header: "Conf",
      enableSorting: true,
      cell: ({ getValue }) => {
        const v = getValue() as number;
        return <span className={`font-bold ${confidenceColor(v)}`}>{v}</span>;
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
  ];

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
        onRowClick={setSelected}
      />
      <ReviewSheet
        candidate={selected}
        onClose={() => setSelected(null)}
        onAction={handleSingleAction}
      />
    </>
  );
}
```

- [ ] **Step 3: Run typecheck**

```bash
cd ui && pnpm typecheck 2>&1 | grep -i "DomainCandidates\|error" | head -20
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add ui/app/components/app/review/DomainCandidatesTab.tsx
git commit -m "feat(ui): add DomainCandidatesTab with sorting, filtering, and bulk actions"
```

---

### Task 6: Frontend — CompanySuggestionsTab + add CompanySuggestion type

**Files:**
- Modify: `ui/app/types/api.ts`
- Create: `ui/app/components/app/review/CompanySuggestionsTab.tsx`

- [ ] **Step 1: Add `CompanySuggestion` to `ui/app/types/api.ts`**

Append after the `ReviewListResponse` interface (after line ~35):

```typescript
export interface CompanySuggestion {
  id: string;
  proposed_display_name: string;
  proposed_legal_name: string | null;
  proposed_website: string | null;
  proposed_country_id: string | null;
  proposed_profile: Record<string, unknown>;
  confidence: number | null;
  status: string;
  reviewed_by: string | null;
  review_note: string | null;
  created_at: string;
  updated_at: string;
}

export interface CompanySuggestionListResponse {
  items: CompanySuggestion[];
  page: number;
  limit: number;
  total: number;
}
```

- [ ] **Step 2: Create `ui/app/components/app/review/CompanySuggestionsTab.tsx`**

```tsx
import { useCallback, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { toast } from "sonner";
import { CheckCircle } from "lucide-react";
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

  useState(() => { fetchPage(1); });

  const handleApprove = async (ids: string[]) => {
    await Promise.all(ids.map((id) => api.approveCompanySuggestion(id)));
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  };

  const handleReject = async (ids: string[]) => {
    await Promise.all(ids.map((id) => api.rejectCompanySuggestion(id)));
    setItems((prev) => prev.filter((i) => !ids.includes(i.id)));
    setTotal((prev) => Math.max(0, prev - ids.length));
  };

  const columns: ColumnDef<CompanySuggestion, unknown>[] = [
    {
      accessorKey: "proposed_display_name",
      header: "Proposed Name",
      enableSorting: true,
      cell: ({ getValue }) => <span className="font-medium">{getValue() as string}</span>,
    },
    {
      accessorKey: "confidence",
      header: "Conf",
      enableSorting: true,
      cell: ({ getValue }) => {
        const v = getValue() as number | null;
        return <span className="text-muted-foreground">{v != null ? Math.round(v * 100) : "—"}</span>;
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
        <div className="flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
          <Button
            size="sm"
            variant="default"
            className="h-7 bg-green-600 hover:bg-green-700 text-xs"
            onClick={() => handleApprove([row.original.id]).then(() => toast.success("Approved."))}
          >
            ✓
          </Button>
          <Button
            size="sm"
            variant="destructive"
            className="h-7 text-xs"
            onClick={() => handleReject([row.original.id]).then(() => toast.success("Rejected."))}
          >
            ✗
          </Button>
        </div>
      ),
    },
  ];

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
    />
  );
}
```

- [ ] **Step 3: Add `getCompanySuggestions`, `approveCompanySuggestion`, `rejectCompanySuggestion` to `ui/app/lib/api.ts`**

Append inside the `api` object (before the closing `}`):

```typescript
  getCompanySuggestions: (page = 1, limit = 50) =>
    get<CompanySuggestionListResponse>(`/suggestions/companies?page=${page}&limit=${limit}`),

  approveCompanySuggestion: (id: string) =>
    post<unknown>(`/suggestions/companies/${id}/approve`, { reviewed_by: "ops" }),

  rejectCompanySuggestion: (id: string) =>
    post<unknown>(`/suggestions/companies/${id}/reject`, { reviewed_by: "ops" }),
```

Also add `CompanySuggestionListResponse` to the imports at the top of `api.ts`:

```typescript
import type {
  StatsResponse,
  ReviewListResponse,
  CompanySuggestionListResponse,
  // ... rest unchanged
} from "~/types/api";
```

- [ ] **Step 4: Run typecheck**

```bash
cd ui && pnpm typecheck 2>&1 | head -20
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add ui/app/types/api.ts ui/app/components/app/review/CompanySuggestionsTab.tsx ui/app/lib/api.ts
git commit -m "feat(ui): add CompanySuggestionsTab and CompanySuggestion type"
```

---

### Task 7: Frontend — Rewrite review.tsx + delete ReviewTable.tsx

**Files:**
- Rewrite: `ui/app/routes/review.tsx`
- Delete: `ui/app/components/app/ReviewTable.tsx`

- [ ] **Step 1: Rewrite `ui/app/routes/review.tsx`**

```tsx
import { useEffect, useState } from "react";
import { api } from "~/lib/api";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { DomainCandidatesTab } from "~/components/app/review/DomainCandidatesTab";
import { CompanySuggestionsTab } from "~/components/app/review/CompanySuggestionsTab";

export default function ReviewPage() {
  const [pendingDomains, setPendingDomains] = useState<number | null>(null);

  useEffect(() => {
    api.getStats().then((s) => setPendingDomains(s.pending_review)).catch(() => {});
  }, []);

  return (
    <div>
      <h1 className="mb-4 text-xl font-semibold">Review Queue</h1>
      <Tabs defaultValue="domain_candidates">
        <TabsList className="mb-4">
          <TabsTrigger value="domain_candidates" className="gap-2">
            Domain Candidates
            {pendingDomains != null && (
              <span className="rounded-full bg-primary px-2 py-0.5 text-xs font-medium text-primary-foreground">
                {pendingDomains.toLocaleString()}
              </span>
            )}
          </TabsTrigger>
          <TabsTrigger value="company_suggestions" className="gap-2">
            New Companies
            <span className="rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">0</span>
          </TabsTrigger>
        </TabsList>
        <TabsContent value="domain_candidates">
          <DomainCandidatesTab />
        </TabsContent>
        <TabsContent value="company_suggestions">
          <CompanySuggestionsTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}
```

- [ ] **Step 2: Delete `ReviewTable.tsx`**

```bash
rm ui/app/components/app/ReviewTable.tsx
```

- [ ] **Step 3: Run typecheck — confirm no references to ReviewTable remain**

```bash
cd ui && pnpm typecheck 2>&1 | head -30
```

Expected: no errors. If `ReviewTable` is still imported somewhere, remove that import.

- [ ] **Step 4: Build the UI**

```bash
cd ui && pnpm build 2>&1 | tail -10
```

Expected: build succeeds with no errors.

- [ ] **Step 5: Test in browser**

Open `http://localhost:8094/review`. Verify:
- Two tabs: "Domain Candidates" with count badge, "New Companies" with 0
- Domain Candidates tab loads the table with data
- Clicking a column header sorts (UI indicator changes)
- Clicking Filter ▾ opens popover; select Signal = certsh, click Apply → filter chip appears, table refetches
- Checking rows shows bulk action bar with "Approve N / Reject N"
- Clicking ··· opens the detail sheet
- Clicking ✓ on a row removes it from the table
- New Companies tab shows the empty state (checkmark + "No pending company suggestions")

- [ ] **Step 6: Commit**

```bash
git add ui/app/routes/review.tsx
git rm ui/app/components/app/ReviewTable.tsx
git commit -m "feat(ui): rewrite review page with tabbed BulkReviewTable layout"
```
