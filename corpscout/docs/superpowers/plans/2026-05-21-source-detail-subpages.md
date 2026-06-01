# Source Detail Subpages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the tabbed `/sources/:name` page into separate full URL pages at `/sources/:name/:subpage`, add a read-only raw-inputs table page per source, and introduce a `requires_translation` flag on data sources to replace hardcoded `brreg` checks.

**Architecture:** A DB migration adds `requires_translation` to `data_sources`, auto-surfaced through sqlc's `SELECT *`. The `sources_.$name.tsx` route becomes a layout (Outlet + nav) following the same pattern as `review.tsx`. A shared `RawInputsTable` component is extracted from `review.companies.tsx` and reused in the new `/sources/:name/raw_input` route.

**Tech Stack:** Go (sqlc, pgx, testify), React Router v7, TypeScript, TanStack Table, shadcn/ui.

---

### Task 1: DB migration — add `requires_translation`

**Files:**
- Create: `database/migrations/000039_source_requires_translation.up.sql`
- Create: `database/migrations/000039_source_requires_translation.down.sql`

- [ ] **Step 1: Write the up migration**

```sql
-- database/migrations/000039_source_requires_translation.up.sql
ALTER TABLE data_sources
  ADD COLUMN requires_translation BOOLEAN NOT NULL DEFAULT false;

UPDATE data_sources SET requires_translation = true WHERE name = 'brreg';
```

- [ ] **Step 2: Write the down migration**

```sql
-- database/migrations/000039_source_requires_translation.down.sql
ALTER TABLE data_sources DROP COLUMN requires_translation;
```

- [ ] **Step 3: Apply the migration**

Run from `corpscout/`:
```bash
GOWORK=off make migrate-up
```
Expected: `migrating... done`

- [ ] **Step 4: Verify**

```bash
docker run --rm postgres:16-alpine psql \
  "postgres://corpscout:password123@100.85.212.113:5432/corpscout?sslmode=disable" \
  -c "SELECT name, requires_translation FROM data_sources ORDER BY name;"
```
Expected: `brreg` row shows `t`, all others show `f`.

- [ ] **Step 5: Commit**

```bash
git add database/migrations/000039_source_requires_translation.up.sql \
        database/migrations/000039_source_requires_translation.down.sql
git commit -m "feat(db): add requires_translation column to data_sources"
```

---

### Task 2: Regenerate sqlc + backend test

**Files:**
- Modify (generated): `scheduler/internal/db/gen/models.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`

The queries use `SELECT *`, so after `make sqlc-generate` the `db.DataSource` struct automatically gains `RequiresTranslation bool \`json:"requires_translation"\``. The `sourceView` struct in `sources.go` embeds `db.DataSource`, so no handler code changes are needed.

- [ ] **Step 1: Regenerate sqlc**

Run from `corpscout/scheduler/`:
```bash
GOWORK=off make sqlc-generate
```
Expected: exits 0, no errors.

- [ ] **Step 2: Verify the new field**

```bash
grep "RequiresTranslation" scheduler/internal/db/gen/models.go
```
Expected: `RequiresTranslation bool \`json:"requires_translation"\``

- [ ] **Step 3: Run existing tests to confirm nothing broke**

```bash
cd scheduler && GOWORK=off go test ./...
```
Expected: all pass.

- [ ] **Step 4: Write the failing test**

Add to `scheduler/internal/httpapi/sources_test.go`, inside `package httpapi_test`:

```go
func TestGetSource_includes_requires_translation(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetSourceByName", mock.Anything, "brreg").Return(db.DataSource{
		ID:                  uuid.New(),
		Name:                "brreg",
		RequiresTranslation: true,
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, true, body["requires_translation"])
}
```

- [ ] **Step 5: Run the test to confirm it passes**

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi/ -run TestGetSource_includes_requires_translation -v
```
Expected: PASS (the field is included via the embedded struct).

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/db/gen/ scheduler/internal/httpapi/sources_test.go
git commit -m "feat(scheduler): surface requires_translation in source API response"
```

---

### Task 3: TypeScript type + replace hardcoded brreg checks

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/components/app/source-detail/RawInputsTab.tsx`

- [ ] **Step 1: Add `requires_translation` to the `DataSource` interface**

In `ui/app/types/api.ts`, find the `DataSource` interface and add the field after `capabilities`:

```ts
  capabilities: string[];
  requires_translation: boolean;
  created_at: string;
```

- [ ] **Step 2: Replace hardcoded check in `RawInputsTab.tsx`**

In `ui/app/components/app/source-detail/RawInputsTab.tsx`, change:

```ts
  const isBrreg = source.name === "brreg";
```

to:

```ts
  const isBrreg = source.requires_translation;
```

- [ ] **Step 3: Run typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0, no errors.

- [ ] **Step 4: Commit**

```bash
git add ui/app/types/api.ts ui/app/components/app/source-detail/RawInputsTab.tsx
git commit -m "feat(ui): add requires_translation to DataSource type, replace hardcoded brreg checks"
```

---

### Task 4: Extract `RawInputDetailSheet` to a shared file

**Files:**
- Create: `ui/app/components/app/RawInputDetailSheet.tsx`
- Modify: `ui/app/routes/review.companies.tsx`

The `RawInputDetailSheet` component currently lives inline in `review.companies.tsx`. It will be needed by the new source raw_input page too.

- [ ] **Step 1: Create `ui/app/components/app/RawInputDetailSheet.tsx`**

Cut the following from `review.companies.tsx` and paste into the new file. The file should contain everything needed to render the sheet (imports, helper functions it uses, and the component itself):

```tsx
import { useEffect, useState } from "react";
import { api } from "~/lib/api";
import type { RawInputDetail } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";

const SOURCE_LABELS: Record<string, string> = {
  companies_house: "Companies House",
  brreg: "Brreg",
};

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

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  if (!value && value !== 0) return null;
  return (
    <div className="grid grid-cols-[140px_1fr] gap-2 py-1.5">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className="text-sm break-all">{value}</span>
    </div>
  );
}

export function RawInputDetailSheet({
  open,
  onClose,
  source,
  id,
}: {
  open: boolean;
  onClose: () => void;
  source: string;
  id: string;
}) {
  const [detail, setDetail] = useState<RawInputDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [jsonExpanded, setJsonExpanded] = useState(false);

  useEffect(() => {
    if (!open || !id) return;
    setDetail(null);
    setJsonExpanded(false);
    setLoading(true);
    api.getRawInput(source, id).then(setDetail).finally(() => setLoading(false));
  }, [open, source, id]);

  const typeLabel = detail?.source === "companies_house"
    ? detail.company_type
    : detail?.registration_status;

  return (
    <Sheet open={open} onOpenChange={(v) => !v && onClose()}>
      <SheetContent className="w-[480px] sm:max-w-[480px] overflow-y-auto">
        {loading && (
          <div className="space-y-3 pt-6">
            <Skeleton className="h-6 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-full" />
          </div>
        )}
        {detail && (
          <>
            <SheetHeader className="pb-4">
              <SheetTitle className="text-lg leading-snug">{detail.name}</SheetTitle>
              <div className="flex items-center gap-2 mt-1">
                <Badge variant="outline" className="text-xs">
                  {SOURCE_LABELS[detail.source] ?? detail.source}
                </Badge>
                <Badge variant={statusBadgeVariant(detail.status)} className="text-xs">
                  {detail.status}
                </Badge>
                {detail.source === "brreg" && (
                  <Badge variant="outline" className={`text-xs ${translationBadgeClass(detail.translation_status)}`}>
                    {detail.translation_status ?? "pending"}
                  </Badge>
                )}
                {detail.country_iso2 && (
                  <span className="text-xs text-muted-foreground">{detail.country_iso2}</span>
                )}
              </div>
            </SheetHeader>

            <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <DetailRow label="Native ID" value={<span className="font-mono text-xs">{detail.native_id}</span>} />
              {typeLabel && <DetailRow label="Type" value={typeLabel} />}
              {detail.website && (
                <DetailRow label="Website" value={
                  <a href={detail.website} target="_blank" rel="noreferrer"
                     className="text-primary underline-offset-4 hover:underline">
                    {detail.website}
                  </a>
                } />
              )}
              {detail.run_id && <DetailRow label="Run ID" value={<span className="font-mono text-xs">{detail.run_id}</span>} />}
            </section>

            <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">Timestamps</p>
              <DetailRow label="Created" value={`${new Date(detail.created_at).toLocaleString()} (${timeAgo(detail.created_at)})`} />
              <DetailRow label="First seen" value={`${new Date(detail.first_seen_at).toLocaleString()} (${timeAgo(detail.first_seen_at)})`} />
              <DetailRow label="Last seen" value={`${new Date(detail.last_seen_at).toLocaleString()} (${timeAgo(detail.last_seen_at)})`} />
              {detail.processed_at && (
                <DetailRow label="Processed" value={`${new Date(detail.processed_at).toLocaleString()} (${timeAgo(detail.processed_at)})`} />
              )}
            </section>

            <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">Processing</p>
              <DetailRow label="Attempts" value={detail.processing_attempts} />
              <DetailRow label="Hash" value={<span className="font-mono text-xs">{detail.payload_hash.slice(0, 16)}…</span>} />
              {detail.processing_error && (
                <div className="mt-2 rounded-md bg-destructive/10 px-3 py-2">
                  <p className="text-xs font-medium text-destructive mb-1">Error</p>
                  <p className="text-xs text-destructive break-all">{detail.processing_error}</p>
                </div>
              )}
            </section>

            {detail.source === "brreg" && (
              <>
                <Separator className="mb-4" />
                <section className="space-y-0.5 mb-4">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">Translation</p>
                  <DetailRow label="Status" value={detail.translation_status ?? "pending"} />
                  <DetailRow label="Attempts" value={detail.translation_attempts ?? 0} />
                  <DetailRow label="Model" value={detail.translation_model} />
                  <DetailRow label="FX" value={detail.translation_fx_source ? `${detail.translation_fx_source} ${detail.translation_fx_rate_date ?? ""}` : undefined} />
                  {detail.translated_at && (
                    <DetailRow label="Translated" value={`${new Date(detail.translated_at).toLocaleString()} (${timeAgo(detail.translated_at)})`} />
                  )}
                  {detail.translation_error && (
                    <div className="mt-2 rounded-md bg-destructive/10 px-3 py-2">
                      <p className="text-xs font-medium text-destructive mb-1">Translation error</p>
                      <p className="text-xs text-destructive break-all">{detail.translation_error}</p>
                    </div>
                  )}
                </section>
              </>
            )}

            <Separator className="mb-4" />

            <section>
              <button
                className="flex w-full items-center justify-between text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2"
                onClick={() => setJsonExpanded((v) => !v)}
              >
                Raw payload
                <span className="normal-case font-normal">{jsonExpanded ? "hide" : "show"}</span>
              </button>
              {jsonExpanded && (
                <pre className="rounded-md bg-muted p-3 text-xs overflow-auto max-h-96 whitespace-pre-wrap break-all">
                  {JSON.stringify(detail.raw_payload, null, 2)}
                </pre>
              )}
            </section>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
```

- [ ] **Step 2: Update `review.companies.tsx` to import from the shared file**

In `ui/app/routes/review.companies.tsx`:

1. Delete the entire `RawInputDetailSheet` function (including all its helper functions: `timeAgo`, `statusBadgeVariant`, `translationBadgeClass`, `DetailRow`, and `SOURCE_LABELS` constant that was only used by the sheet).
2. Add this import at the top:

```ts
import { RawInputDetailSheet } from "~/components/app/RawInputDetailSheet";
```

The `SOURCE_LABELS` constant at the top of `review.companies.tsx` is used in the table's `source` column cell — keep that one. Only remove the one inside the old `RawInputDetailSheet` closure.

- [ ] **Step 3: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add ui/app/components/app/RawInputDetailSheet.tsx ui/app/routes/review.companies.tsx
git commit -m "refactor(ui): extract RawInputDetailSheet to shared component"
```

---

### Task 5: Build `RawInputsTable` shared component

**Files:**
- Create: `ui/app/components/app/RawInputsTable.tsx`

This component encapsulates all the table logic from `review.companies.tsx`. When `sourceName` is provided it pre-filters and hides the source dropdown. When `requiresTranslation` is false the translation column and filter are hidden. When `showConfirmAction` is false (default) the Confirm button column is omitted.

- [ ] **Step 1: Create `ui/app/components/app/RawInputsTable.tsx`**

```tsx
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ArrowUpDown, CheckCircle2, Search, X } from "lucide-react";
import { toast } from "sonner";
import { api } from "~/lib/api";
import type { RawInput } from "~/types/api";
import { Input } from "~/components/ui/input";
import { Button } from "~/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Badge } from "~/components/ui/badge";
import { Skeleton } from "~/components/ui/skeleton";
import { RawInputDetailSheet } from "~/components/app/RawInputDetailSheet";

const PAGE_SIZE = 50;

const SOURCE_LABELS: Record<string, string> = {
  companies_house: "Companies House",
  brreg: "Brreg",
};

const STATUS_OPTIONS = ["pending", "processing", "processed", "failed", "ignored", "superseded"];
const TRANSLATION_OPTIONS = ["pending", "translating", "translated", "failed"];
const SOURCE_OPTIONS = ["companies_house", "brreg"];

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

function approvalBlockedReason(row: RawInput): string | undefined {
  if (row.source === "brreg" && row.translation_status !== "translated") return "Translate first";
  if (!row.company_suggestion_id) return "Process source first";
  if (row.company_suggestion_status !== "pending") return "Already confirmed";
  if (!row.can_approve_company) return "Not ready";
  return undefined;
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
  showConfirmAction?: boolean;
}

export function RawInputsTable({ sourceName, requiresTranslation, showConfirmAction = false }: RawInputsTableProps) {
  const [items, setItems] = useState<RawInput[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);

  const [sourceFilter, setSourceFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [translationFilter, setTranslationFilter] = useState("");
  const [searchQ, setSearchQ] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [sortCol, setSortCol] = useState("created_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [approvingId, setApprovingId] = useState<string | null>(null);
  const [selectedRow, setSelectedRow] = useState<RawInput | null>(null);

  const load = useCallback(
    async (p: number, src: string, st: string, tr: string, q: string, sort: string, dir: "asc" | "desc") => {
      setLoading(true);
      try {
        const res = await api.getRawInputs({
          page: p,
          limit: PAGE_SIZE,
          source: (sourceName ?? src) || undefined,
          status: st || undefined,
          translation_status: tr || undefined,
          q: q || undefined,
          sort,
          dir,
        });
        setItems(res.items);
        setTotal(res.total);
        setPage(p);
      } finally {
        setLoading(false);
      }
    },
    [sourceName],
  );

  useEffect(() => {
    load(1, sourceFilter, statusFilter, translationFilter, searchQ, sortCol, sortDir);
  }, [load, sourceFilter, statusFilter, translationFilter, searchQ, sortCol, sortDir]);

  const handleSort = (col: string) => {
    if (col === sortCol) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(col);
      setSortDir("desc");
    }
  };

  const applySearch = () => setSearchQ(searchInput);
  const clearSearch = () => { setSearchInput(""); setSearchQ(""); };
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const confirmCompany = useCallback(async (row: RawInput) => {
    if (!row.company_suggestion_id) return;
    setApprovingId(row.id);
    try {
      await api.approveCompanySuggestion(row.company_suggestion_id);
      toast.success("Company created.");
      setItems((current) => current.map((item) =>
        item.id === row.id
          ? { ...item, company_suggestion_status: "approved", can_approve_company: false }
          : item,
      ));
    } catch {
      toast.error("Failed to confirm company.");
    } finally {
      setApprovingId(null);
    }
  }, []);

  const columns = useMemo<ColumnDef<RawInput>[]>(
    () => {
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
            const v = getValue() as string;
            return <Badge variant={statusBadgeVariant(v)} className="text-xs">{v}</Badge>;
          },
        },
      );

      if (requiresTranslation) {
        cols.push({
          accessorKey: "translation_status",
          header: "Translation",
          enableSorting: false,
          cell: ({ row }) => {
            if (!sourceName && row.original.source !== "brreg") {
              return <span className="text-xs text-muted-foreground">not required</span>;
            }
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
          const v = getValue() as string;
          return (
            <div className="text-sm">
              <div>{new Date(v).toLocaleDateString()}</div>
              <div className="text-xs text-muted-foreground">{timeAgo(v)}</div>
            </div>
          );
        },
      });

      if (showConfirmAction) {
        cols.push({
          id: "actions",
          header: "",
          enableSorting: false,
          cell: ({ row }) => {
            const reason = approvalBlockedReason(row.original);
            const disabled = Boolean(reason) || approvingId === row.original.id;
            return (
              <div className="flex justify-end" onClick={(event) => event.stopPropagation()}>
                <span title={reason}>
                  <Button size="sm" disabled={disabled} onClick={() => confirmCompany(row.original)}>
                    <CheckCircle2 className="size-4" />
                    {approvingId === row.original.id ? "Confirming..." : "Confirm"}
                  </Button>
                </span>
              </div>
            );
          },
        });
      }

      return cols;
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sourceName, requiresTranslation, showConfirmAction, sortCol, sortDir, approvingId, confirmCompany],
  );

  const table = useReactTable({
    data: items,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    manualPagination: true,
    pageCount: totalPages,
  });

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex items-center">
          <Search className="absolute left-2.5 size-4 text-muted-foreground" />
          <Input
            className="h-8 w-56 pl-8 pr-8 text-sm"
            placeholder="Search by name…"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && applySearch()}
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
            onChange={(e) => setSourceFilter(e.target.value)}
          >
            <option value="">All sources</option>
            {SOURCE_OPTIONS.map((s) => (
              <option key={s} value={s}>{SOURCE_LABELS[s] ?? s}</option>
            ))}
          </select>
        )}

        <select
          className="h-8 rounded-md border bg-background px-2 text-sm"
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
        >
          <option value="">All statuses</option>
          {STATUS_OPTIONS.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        {requiresTranslation && (
          <select
            className="h-8 rounded-md border bg-background px-2 text-sm"
            value={translationFilter}
            onChange={(e) => setTranslationFilter(e.target.value)}
          >
            <option value="">All translations</option>
            {TRANSLATION_OPTIONS.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        )}

        <span className="ml-auto text-sm text-muted-foreground">
          {loading ? "Loading…" : `${total.toLocaleString()} entries`}
        </span>
      </div>

      {/* Table */}
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id}>
                {hg.headers.map((header) => (
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
              Array.from({ length: 8 }).map((_, i) => (
                <TableRow key={i}>
                  {columns.map((_, j) => (
                    <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
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

      {/* Pagination */}
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
              onClick={() => load(page - 1, sourceFilter, statusFilter, translationFilter, searchQ, sortCol, sortDir)}
            >
              Previous
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={page >= totalPages || loading}
              onClick={() => load(page + 1, sourceFilter, statusFilter, translationFilter, searchQ, sortCol, sortDir)}
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
```

- [ ] **Step 2: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add ui/app/components/app/RawInputsTable.tsx
git commit -m "feat(ui): add RawInputsTable shared component"
```

---

### Task 6: Refactor `review.companies.tsx` to use `RawInputsTable`

**Files:**
- Modify: `ui/app/routes/review.companies.tsx`

Replace the entire contents of `review.companies.tsx` with a thin wrapper that delegates to `RawInputsTable`.

- [ ] **Step 1: Replace `review.companies.tsx`**

```tsx
import { RawInputsTable } from "~/components/app/RawInputsTable";

export default function ReviewCompaniesPage() {
  return <RawInputsTable requiresTranslation={true} showConfirmAction={true} />;
}
```

- [ ] **Step 2: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0.

- [ ] **Step 3: Manually verify `/review/companies` still works**

Open `http://localhost:8094/review/companies`. Confirm: table loads, source/status/translation filters visible, Confirm button present, clicking a row opens the detail sheet.

- [ ] **Step 4: Commit**

```bash
git add ui/app/routes/review.companies.tsx
git commit -m "refactor(ui): review/companies delegates to RawInputsTable"
```

---

### Task 7: Convert `sources_.$name.tsx` to layout route

**Files:**
- Modify: `ui/app/routes/sources_.$name.tsx`

The route becomes a layout: loads source data, passes it via `useOutletContext`, renders `SourceHeader` + nav + `<Outlet>`.

- [ ] **Step 1: Replace `sources_.$name.tsx`**

```tsx
import { useEffect, useRef, useState } from "react";
import { Link, NavLink, Outlet, useParams } from "react-router";
import { ChevronLeft } from "lucide-react";
import { toast } from "sonner";
import { api, errorMessage } from "~/lib/api";
import type { DataSource } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Skeleton } from "~/components/ui/skeleton";
import { SourceHeader } from "~/components/app/source-detail/SourceHeader";
import { hasRawInputs, hasPipeline, sourceDisplayName } from "~/components/app/source-detail/sourceDetailUtils";
import { cn } from "~/lib/utils";

type SourcePatch = Parameters<typeof api.patchSource>[1];

export interface SourceDetailContext {
  source: DataSource;
  saving: boolean;
  triggering: boolean;
  processing: boolean;
  onPatch: (patch: SourcePatch) => Promise<void>;
  onTrigger: () => Promise<void>;
  onProcess: () => Promise<void>;
}

export default function SourceDetailLayout() {
  const { name } = useParams<{ name: string }>();
  const latestNameRef = useRef<string | undefined>(undefined);
  const [source, setSource] = useState<DataSource>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [saving, setSaving] = useState(false);
  const [triggering, setTriggering] = useState(false);
  const [processing, setProcessing] = useState(false);

  useEffect(() => {
    if (!name) return;
    latestNameRef.current = name;
    let ignore = false;
    setSource(undefined);
    setLoading(true);
    setError(undefined);
    setSaving(false);
    setTriggering(false);
    setProcessing(false);
    api.getSource(name)
      .then((loadedSource) => { if (!ignore) setSource(loadedSource); })
      .catch(() => { if (!ignore) setError("Source not found."); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [name]);

  async function refreshSource(sourceName: string) {
    const refreshed = await api.getSource(sourceName);
    if (latestNameRef.current === sourceName) setSource(refreshed);
  }

  async function handlePatch(patch: SourcePatch) {
    const sourceName = source?.name;
    if (!sourceName) return;
    setSaving(true);
    try {
      await api.patchSource(sourceName, patch);
      await refreshSource(sourceName);
      toast.success("Source updated.");
    } catch (err) {
      toast.error(errorMessage(err, "Failed to update source."));
    } finally {
      if (latestNameRef.current === sourceName) setSaving(false);
    }
  }

  async function handleTrigger() {
    const sourceName = source?.name;
    if (!sourceName) return;
    setTriggering(true);
    try {
      await api.triggerSource(sourceName);
      toast.success(`${sourceName} pull queued.`);
    } catch (err) {
      toast.error(errorMessage(err, `Failed to trigger ${sourceName}.`));
    } finally {
      if (latestNameRef.current === sourceName) setTriggering(false);
    }
  }

  async function handleProcess() {
    const sourceName = source?.name;
    if (!sourceName) return;
    setProcessing(true);
    try {
      await api.processSource(sourceName);
      toast.success(`${sourceName} processing queued.`);
    } catch (err) {
      toast.error(errorMessage(err, `Failed to process ${sourceName}.`));
    } finally {
      if (latestNameRef.current === sourceName) setProcessing(false);
    }
  }

  if (loading) return <Skeleton className="h-64 w-full" />;
  if (error || !source) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error ?? "Source not found."}</AlertDescription>
      </Alert>
    );
  }

  const tabs = [
    { label: "Schedule", to: `/sources/${source.name}/schedule` },
    { label: "Config", to: `/sources/${source.name}/config` },
    { label: "Logs", to: `/sources/${source.name}/logs` },
    ...(hasRawInputs(source) ? [{ label: "Raw Inputs", to: `/sources/${source.name}/raw_input` }] : []),
    ...(hasPipeline(source) ? [{ label: "Pipeline", to: `/sources/${source.name}/pipeline` }] : []),
  ];

  const context: SourceDetailContext = { source, saving, triggering, processing, onPatch: handlePatch, onTrigger: handleTrigger, onProcess: handleProcess };

  return (
    <div className="space-y-6">
      <Link
        to="/sources"
        className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:underline"
      >
        <ChevronLeft className="size-4" />
        Sources
      </Link>

      <SourceHeader source={source} />

      <nav className="flex gap-1 border-b">
        {tabs.map((tab) => (
          <NavLink
            key={tab.to}
            to={tab.to}
            className={({ isActive }) =>
              cn(
                "relative px-4 py-2 text-sm font-medium transition-colors hover:text-foreground",
                isActive
                  ? "border-b-2 border-primary text-foreground"
                  : "text-muted-foreground",
              )
            }
          >
            {tab.label}
          </NavLink>
        ))}
      </nav>

      <Outlet context={context} />
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0 (child routes don't exist yet, but the layout itself is valid).

- [ ] **Step 3: Commit**

```bash
git add ui/app/routes/sources_.$name.tsx
git commit -m "refactor(ui): sources_.\$name becomes layout route with nav and Outlet"
```

---

### Task 8: Create subpage route files

**Files:**
- Create: `ui/app/routes/sources_.$name._index.tsx`
- Create: `ui/app/routes/sources_.$name.schedule.tsx`
- Create: `ui/app/routes/sources_.$name.config.tsx`
- Create: `ui/app/routes/sources_.$name.logs.tsx`
- Create: `ui/app/routes/sources_.$name.pipeline.tsx`

- [ ] **Step 1: Create `sources_.$name._index.tsx`** (redirect to schedule)

```tsx
import { Navigate, useParams } from "react-router";

export default function SourceDetailIndex() {
  const { name } = useParams<{ name: string }>();
  return <Navigate to={`/sources/${name}/schedule`} replace />;
}
```

- [ ] **Step 2: Create `sources_.$name.schedule.tsx`**

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ScheduleTab } from "~/components/app/source-detail/ScheduleTab";

export default function SourceSchedulePage() {
  const { source, saving, triggering, processing, onPatch, onTrigger, onProcess } =
    useOutletContext<SourceDetailContext>();
  return (
    <ScheduleTab
      source={source}
      saving={saving}
      triggering={triggering}
      processing={processing}
      onPatch={onPatch}
      onTrigger={onTrigger}
      onProcess={onProcess}
    />
  );
}
```

- [ ] **Step 3: Create `sources_.$name.config.tsx`**

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ConfigTab } from "~/components/app/source-detail/ConfigTab";

export default function SourceConfigPage() {
  const { source, saving, onPatch } = useOutletContext<SourceDetailContext>();
  return <ConfigTab source={source} saving={saving} onPatch={onPatch} />;
}
```

- [ ] **Step 4: Create `sources_.$name.logs.tsx`**

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { LogsTab } from "~/components/app/source-detail/LogsTab";

export default function SourceLogsPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <LogsTab sourceName={source.name} />;
}
```

- [ ] **Step 5: Create `sources_.$name.pipeline.tsx`**

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { PipelineTab } from "~/components/app/source-detail/PipelineTab";

export default function SourcePipelinePage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <PipelineTab source={source} />;
}
```

- [ ] **Step 6: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0.

- [ ] **Step 7: Manually verify navigation**

Open `http://localhost:8094/sources/brreg`. Should redirect to `http://localhost:8094/sources/brreg/schedule`. Click each nav tab — Schedule, Config, Logs, Raw Inputs, Pipeline — and confirm the URL changes and the correct content loads. Confirm the active tab is visually highlighted.

- [ ] **Step 8: Commit**

```bash
git add ui/app/routes/sources_.\$name._index.tsx \
        ui/app/routes/sources_.\$name.schedule.tsx \
        ui/app/routes/sources_.\$name.config.tsx \
        ui/app/routes/sources_.\$name.logs.tsx \
        ui/app/routes/sources_.\$name.pipeline.tsx
git commit -m "feat(ui): add subpage routes for source detail (schedule, config, logs, pipeline)"
```

---

### Task 9: Create the `raw_input` subpage

**Files:**
- Create: `ui/app/routes/sources_.$name.raw_input.tsx`

- [ ] **Step 1: Create `sources_.$name.raw_input.tsx`**

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { RawInputsTable } from "~/components/app/RawInputsTable";

export default function SourceRawInputPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return (
    <RawInputsTable
      sourceName={source.name}
      requiresTranslation={source.requires_translation}
    />
  );
}
```

- [ ] **Step 2: Typecheck**

```bash
cd ui && pnpm typecheck
```
Expected: exits 0.

- [ ] **Step 3: Manually verify `/sources/brreg/raw_input`**

Open `http://localhost:8094/sources/brreg/raw_input`. Confirm:
- Table loads with brreg records only (no source column)
- Translation filter and column are visible
- Clicking a row opens the detail sheet
- No Confirm button

- [ ] **Step 4: Manually verify a non-translation source**

Open `http://localhost:8094/sources/companies_house/raw_input`. Confirm:
- Table loads with companies_house records only
- No translation column, no translation filter

- [ ] **Step 5: Commit**

```bash
git add ui/app/routes/sources_.\$name.raw_input.tsx
git commit -m "feat(ui): add raw_input subpage for source detail"
```
