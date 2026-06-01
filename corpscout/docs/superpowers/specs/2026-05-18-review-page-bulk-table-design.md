# Review Page — Bulk Table Design Spec

**Date:** 2026-05-18
**Status:** Approved

## Problem

The `/review` page renders a plain HTML table with no sorting, no filtering, and no multi-select. With 25,288 pending domain candidates a reviewer cannot efficiently triage — there is no way to bulk-approve high-confidence certsh hits or bulk-reject low-confidence ones.

Additionally the table has no "what you're approving" context column, and a future review queue for company suggestions (and eventually contact/location/status suggestions) will need the same interaction patterns. Building a one-off table now would produce N copies of the same selection + bulk-action logic.

## Goal

1. Replace the review page with a tabbed layout: **Domain Candidates** (live, 25k items) and **New Companies** (stub, 0 items now).
2. Extract all selection, filter, bulk-action, and sorting logic into a single shared `BulkReviewTable<TData>` component so future tabs are thin wrappers.
3. Add server-side text search (`q`), signal filter, and confidence filter to `GET /api/v1/review`.
4. Add `POST /api/v1/review/bulk` for efficient multi-row approve/reject.

## Non-Goals

- Tabs for contact / location / status / relationship suggestions — those tables are empty; add when populated.
- Inline editing of suggestion fields.
- Persisting filter state across page refreshes.

## Architecture

```
review.tsx
  └── <Tabs>
        ├── <DomainCandidatesTab>   columns + data hook + action handlers
        │     └── <BulkReviewTable<ReviewCandidate>>   ← shared engine
        │           └── <ReviewSheet>   detail panel (existing, unchanged)
        └── <CompanySuggestionsTab>  columns + data hook + action handlers
              └── <BulkReviewTable<CompanySuggestion>>
```

`BulkReviewTable` owns: row selection state, bulk action bar, filter chip bar, Filter popover, search input, column sort state. It renders nothing domain-specific — it knows about `id: string` on each row and nothing else.

Each tab owns: API call, column definitions, filter definitions, `onApprove(ids[])` / `onReject(ids[])` implementations.

## Backend Changes

### 1. `database/queries/domains.sql`

Add `q` (text search) parameter to `ListDomains` and `CountDomains`, and expose `signal` and `min_confidence` on the review endpoint:

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

Run `make sqlc-generate` after updating the SQL.

### 2. `scheduler/internal/httpapi/review.go`

**Update `handleListReview`** to accept `signal`, `min_confidence`, and `q` query params:

```go
func (h *Handlers) handleListReview(w http.ResponseWriter, r *http.Request) {
    page  := queryInt(r, "page", 1)
    limit := min(queryInt(r, "limit", 50), 200)
    offset := int32((page - 1) * limit)
    status := "needs_review"

    var minConf *int16
    if s := r.URL.Query().Get("min_confidence"); s != "" {
        if n, err := strconv.Atoi(s); err == nil { v := int16(n); minConf = &v }
    }

    params := db.ListDomainsParams{
        Status:        &status,
        Signal:        queryString(r, "signal"),
        MinConfidence: minConf,
        Q:             queryString(r, "q"),
        Offset:        offset,
        Limit:         int32(limit),
    }
    // ... rest unchanged
}
```

**Add `handleBulkReview`** — new handler for `POST /api/v1/review/bulk`:

```go
func (h *Handlers) handleBulkReview(w http.ResponseWriter, r *http.Request) {
    var body struct {
        IDs    []string `json:"ids"`
        Action string   `json:"action"`
    }
    // decode body, validate action in (approved, rejected, superseded)
    // map action → status string
    // loop: h.db.ReviewCompanyDomain(ctx, {ID: uuid, Status: status}) for each id
    // return {updated: N}
}
```

### 3. `scheduler/internal/httpapi/handlers.go`

Add route:
```go
r.Post("/review/bulk", h.handleBulkReview)
```

### 4. `scheduler/internal/httpapi/testhelpers_test.go`

No new querier methods needed — `ReviewCompanyDomain` already exists in the stub.

## Frontend Changes

### Prerequisite: Add Missing shadcn Components

The existing `ui/app/components/ui/` directory does not include `checkbox.tsx` or `popover.tsx`. Both are needed by `BulkReviewTable`. Add them with:

```bash
cd ui
pnpm dlx shadcn@latest add checkbox popover
```

### File Map

| File | Action |
|------|--------|
| `ui/app/components/ui/checkbox.tsx` | **Add** via shadcn CLI |
| `ui/app/components/ui/popover.tsx` | **Add** via shadcn CLI |
| `ui/app/components/app/BulkReviewTable.tsx` | **Create** — shared engine |
| `ui/app/components/app/review/DomainCandidatesTab.tsx` | **Create** |
| `ui/app/components/app/review/CompanySuggestionsTab.tsx` | **Create** |
| `ui/app/routes/review.tsx` | **Rewrite** — thin tab orchestrator |
| `ui/app/components/app/ReviewTable.tsx` | **Delete** — replaced by BulkReviewTable |
| `ui/app/components/app/ReviewSheet.tsx` | **Keep unchanged** |
| `ui/app/lib/api.ts` | **Update** — new params on `getReview`, add `bulkReview` |
| `ui/app/types/api.ts` | No changes needed |

### `BulkReviewTable<TData extends { id: string }>` — Props

```typescript
export type FilterDef =
  | { key: string; label: string; type: 'select'; options: { value: string; label: string }[] }
  | { key: string; label: string; type: 'min-number' }

export interface ActiveFilter {
  key: string; label: string; value: string; display: string;
}

interface BulkReviewTableProps<TData extends { id: string }> {
  columns: ColumnDef<TData, unknown>[]
  data: TData[]
  total: number
  loading: boolean
  page: number
  pageSize?: number          // default 50
  onPageChange: (p: number) => void
  sorting?: SortingState
  onSortingChange?: OnChangeFn<SortingState>
  onApprove: (ids: string[]) => Promise<void>
  onReject: (ids: string[]) => Promise<void>
  filterDefs?: FilterDef[]
  onFilterChange?: (filters: ActiveFilter[]) => void
  onSearch?: (q: string) => void
  onRowClick?: (row: TData) => void
}
```

### `BulkReviewTable` — Internal Behaviour

- **Row selection**: TanStack Table `getRowId` uses `row.id`. `rowSelection` state is `Record<string, boolean>`. Header checkbox selects/deselects all visible rows.
- **Bulk action bar**: renders between filter bar and table when `Object.keys(rowSelection).length > 0`. Shows "N selected · [Approve N] [Reject N] [Deselect all]". Approve/Reject buttons set `bulkLoading = true`, call `onApprove`/`onReject` with selected IDs, clear `rowSelection` on success, call `toast.success`.
- **Filter chips**: `activeFilters: ActiveFilter[]` state. Filter popover (shadcn `Popover`) shows `filterDefs`. Applying a filter adds a chip; clicking × removes it; "Clear all" empties the array. On any change calls `onFilterChange`.
- **Search**: debounced 300 ms via `useEffect`. Calls `onSearch(q)`.
- **Sorting**: passes through to `onSortingChange`. Uses existing `DataTable` sort header pattern (chevron icons).
- **Per-row actions**: the columns prop is responsible for the ✓ / ✗ / ··· cells. BulkReviewTable does not inject per-row action buttons — the tab's column definitions include them.

### Column Definitions — Domain Candidates

```typescript
const columns: ColumnDef<ReviewCandidate>[] = [
  { id: 'select', /* checkbox — injected by BulkReviewTable header, not in col defs */ },
  { accessorKey: 'company_name', header: 'Company', enableSorting: true },
  { accessorKey: 'domain',       header: 'Domain',  enableSorting: true,
    cell: ({ getValue }) => <span className="font-mono text-primary">{getValue()}</span> },
  { accessorKey: 'signal', header: 'Signal',
    cell: ({ getValue }) => <Badge className={signalColor(getValue())} variant="outline">{getValue()}</Badge> },
  { accessorKey: 'confidence', header: 'Conf', enableSorting: true,
    cell: ({ getValue }) => <span className={`font-bold ${confidenceColor(getValue())}`}>{getValue()}</span> },
  { id: 'actions', header: '', enableSorting: false,
    cell: ({ row }) => <RowActions item={row.original} onApprove={...} onReject={...} onView={...} /> },
]
```

The checkbox column is handled by BulkReviewTable itself: it prepends a `select` column with `cell: ({ row }) => <Checkbox checked={row.getIsSelected()} onCheckedChange={row.getToggleSelectedHandler()} />`.

### Filter Definitions — Domain Candidates

```typescript
const filterDefs: FilterDef[] = [
  { key: 'signal', label: 'Signal', type: 'select',
    options: ['certsh','wikidata','whois','registry_website','search'].map(v => ({ value: v, label: v })) },
  { key: 'min_confidence', label: 'Min confidence', type: 'min-number' },
]
```

### Column Definitions — Company Suggestions

```typescript
const columns: ColumnDef<CompanySuggestion>[] = [
  { accessorKey: 'proposed_display_name', header: 'Proposed Name', enableSorting: true },
  { id: 'country', header: 'Country', /* from proposed_country_id lookup — show raw UUID for now */ },
  { accessorKey: 'confidence', header: 'Conf', enableSorting: true },
  { id: 'actions', /* Approve / Reject / ··· */ },
]
```

### `api.ts` Changes

```typescript
getReview: (p = 1, limit = 50, filters?: { signal?: string; min_confidence?: number; q?: string }) => {
  const qs = new URLSearchParams({ page: String(p), limit: String(limit) })
  if (filters?.signal) qs.set('signal', filters.signal)
  if (filters?.min_confidence != null) qs.set('min_confidence', String(filters.min_confidence))
  if (filters?.q) qs.set('q', filters.q)
  return get<ReviewListResponse>(`/review?${qs}`)
},

bulkReview: (ids: string[], action: 'approved' | 'rejected' | 'superseded') =>
  post<{ updated: number }>('/review/bulk', { ids, action }),
```

### `review.tsx` — Rewrite

```typescript
export default function ReviewPage() {
  return (
    <div>
      <h1 className="mb-4 text-xl font-semibold">Review Queue</h1>
      <Tabs defaultValue="domain_candidates">
        <TabsList>
          <TabsTrigger value="domain_candidates">
            Domain Candidates <PendingBadge count={domainCount} />
          </TabsTrigger>
          <TabsTrigger value="company_suggestions">
            New Companies <PendingBadge count={0} />
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
  )
}
```

Tab counts are fetched once on mount via `GET /api/v1/stats` (already returns `pending_review`; company suggestions count can be added to stats or fetched separately — for now show 0 if not available).

## Error Handling

- Bulk action failure: `toast.error("Bulk action failed. N items may not have been updated.")` — do not clear selection so user can retry.
- Filter/search API error: show inline alert below the filter bar, keep existing data visible.
- Empty state: "No candidates match your filters." with a "Clear filters" link.

## Existing Components Untouched

- `ReviewSheet.tsx` — used as-is by `DomainCandidatesTab` for the ··· detail panel.
- `DataTable.tsx` — `BulkReviewTable` uses `useReactTable` directly (same pattern as DataTable) rather than wrapping it, to keep selection state internal.
