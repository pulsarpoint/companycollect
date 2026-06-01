# Corpscout UI Design Spec

## Goal

Build the `ui/` service for corpscout: a React Router v7 SPA that provides a data browser and operations dashboard backed by the scheduler REST API. Primary daily workflow is reviewing domain candidates (approve / reject / supersede).

## Architecture

### Stack

- **React Router v7** in SPA mode (`ssr: false`) — no Node server, static build output
- **TypeScript** strict mode
- **TailwindCSS v4** + **shadcn/ui** blocks and components (same setup as pulsarprotectproweb)
- **pnpm** package manager
- **lucide-react** icons
- Data fetching: native `fetch` in `useEffect` / event handlers — no SWR or React Query

### Deployment

- Multi-stage Dockerfile: `node:20-alpine` builds → `nginx:alpine` serves `dist/`
- nginx config proxies `/api/v1/` upstream to `http://scheduler:8090`
- `docker-compose.yml` port: `8094:80`
- Client-side API calls use relative paths (`/api/v1/...`) — nginx routes them to the scheduler

### Backend additions (scheduler/)

The current `GET /api/v1/stats` endpoint is missing operational time-based metrics. Extend it with a new SQL query `GetDashboardStats` and add these fields to the stats response:

```json
{
  "total_companies": 16847,
  "total_domains": 997,
  "active_domains": 1803,
  "pending_review": 247,
  "enabled_sources": 5,
  "pull_runs_completed_today": 3,
  "pull_runs_failed_today": 0,
  "records_upserted_24h": 12400,
  "records_upserted_7d": 87000
}
```

New SQL query against `source_pull_runs`:
```sql
-- name: GetDashboardStats :one
SELECT
  (SELECT COUNT(*) FROM source_pull_runs
   WHERE status = 'completed' AND completed_at >= now() - interval '24 hours')::bigint AS pull_runs_completed_today,
  (SELECT COUNT(*) FROM source_pull_runs
   WHERE status = 'failed' AND completed_at >= now() - interval '24 hours')::bigint AS pull_runs_failed_today,
  (SELECT COALESCE(SUM(records_upserted), 0) FROM source_pull_runs
   WHERE completed_at >= now() - interval '24 hours')::bigint AS records_upserted_24h,
  (SELECT COALESCE(SUM(records_upserted), 0) FROM source_pull_runs
   WHERE completed_at >= now() - interval '7 days')::bigint AS records_upserted_7d;
```

Also add `GET /api/v1/pull-runs` endpoint returning paginated `source_pull_runs` rows (for the dashboard recent runs table):
```
GET /api/v1/pull-runs   ?page, limit
→ { items: [...], page, limit }
```

Extend the `ListDomains` SQL query to include company name via a JOIN to `companies` (currently only returns `company_id`):
```sql
SELECT d.domain, c.name AS company_name, cd.*
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
...
```
Update `GET /api/v1/domains` handler to use the extended query.

## Routes & Pages

| Route | Page | Data source |
|---|---|---|
| `/` | Redirect → `/review` | — |
| `/review` | Review queue | `GET /api/v1/review` |
| `/dashboard` | Stats overview | `GET /api/v1/stats` (extended) + `GET /api/v1/pull-runs` |
| `/companies` | Company browser | `GET /api/v1/companies` |
| `/companies/:id` | Company detail | `GET /api/v1/companies/:id` |
| `/domains` | Domains browser | `GET /api/v1/domains` |
| `/sources` | Sources management | `GET /api/v1/sources` |
| `/jobs` | Jobs monitor | `GET /api/v1/jobs` |

**Default route:** `/` redirects to `/review` — the review queue is the primary daily workflow.

## Navigation

shadcn **sidebar-07** block as the app shell. Sidebar items (top to bottom):

1. Dashboard
2. Review *(badge showing pending count)*
3. Companies
4. Domains
5. Sources
6. Jobs

## Page Designs

### Review Queue (`/review`)

Primary daily-use page.

**Table columns:** Company · Domain · Signal · Confidence · Actions

**Signal badges (colored):**
- `registry_website` → green
- `wikidata` → blue
- `certsh` → yellow
- `whois` → orange
- `search` → gray

**Actions per row:** Approve button · Reject button · View button

**View button** opens a shadcn `Sheet` (slides from right) showing:
- Company name, domain
- Signal, confidence, relationship type
- Evidence JSON (formatted)
- First seen / last seen timestamps
- Action buttons inside the Sheet: **Approve**, **Reject**, **Supersede** (the less-common superseded action is accessible here rather than cluttering every row)

**Approve / Reject / Supersede behaviour:**
- Table row buttons (Approve, Reject) call `POST /api/v1/review/:id/reviews` with `{ action: "approved"|"rejected", reviewed_by: "" }`
- Sheet Supersede button calls same endpoint with `{ action: "superseded", reviewed_by: "" }`
- On success: row removed from local state immediately; Sheet closes if open; pending badge in sidebar decrements
- On error: toast notification with error message

**`reviewed_by`:** Sent as `""` until authentication is added.

**Empty state:** Full-width message with checkmark icon — "Queue is empty. All candidates reviewed."

**Pagination:** Load 50 items at a time; "Load more" button at bottom of table.

### Dashboard (`/dashboard`)

**Row 1 — core stats (4 cards):**
- Total Companies
- Total Domains
- Active Domains
- Pending Review *(links to /review)*

**Row 2 — operational stats (4 cards):**
- Pull Runs Today (completed count, green)
- Pull Runs Failed Today (failed count, red if > 0)
- Records Upserted 24h
- Records Upserted 7d

**Row 3 — Recent Pull Runs table:**
Columns: Source · Status · Records Fetched · Records Upserted · Started · Duration

### Companies (`/companies`)

Filterable, searchable table. URL search params preserved for bookmarking.

**Filters:** Country (dropdown) · Source (dropdown) · Status (active/inactive/dissolved) · Text search (q)

**Table columns:** Name · Country · Registration # · LEI · Status · Primary Source · Created

**Row click** → navigate to `/companies/:id`

### Company Detail (`/companies/:id`)

Two sections:
1. Company info card (name, country, reg number, LEI, status, source)
2. Associated domains table (domain, signal, confidence, status, first seen)

### Domains (`/domains`)

**Filters:** Min confidence (slider or select: 50/60/75/90) · Signal

**Table columns:** Domain · Company · Signal · Confidence · Status · First Seen

### Sources (`/sources`)

**Table columns:** Name · Type · Enabled *(shadcn Switch — toggle calls PATCH inline)* · Crawl Interval (editable inline) · Last Crawled · Trigger button

**Trigger button:** calls `POST /api/v1/sources/:name/trigger`, shows toast on success/error.

### Jobs (`/jobs`)

**Filters:** Status · Source (kind filter)

**Table columns:** ID · Kind · State · Queue · Attempt · Created · Finalized

Auto-refreshes every 30 seconds.

## Component Structure

```
ui/
├── app/
│   ├── root.tsx                     # SPA shell: ThemeProvider, sidebar-07 layout, Toaster
│   ├── routes/
│   │   ├── _index.tsx               # redirect to /review
│   │   ├── review.tsx
│   │   ├── dashboard.tsx
│   │   ├── companies.tsx
│   │   ├── companies.$id.tsx
│   │   ├── domains.tsx
│   │   ├── sources.tsx
│   │   └── jobs.tsx
│   ├── components/
│   │   ├── ui/                      # shadcn components
│   │   └── app/
│   │       ├── ReviewTable.tsx
│   │       ├── ReviewSheet.tsx
│   │       ├── StatsCard.tsx
│   │       ├── PullRunsTable.tsx
│   │       ├── CompaniesTable.tsx
│   │       ├── DomainsTable.tsx
│   │       ├── SourcesTable.tsx
│   │       └── JobsTable.tsx
│   ├── lib/
│   │   ├── api.ts                   # typed fetch wrappers for every endpoint
│   │   └── utils.ts                 # cn(), date formatters, confidence colour helpers
│   └── types/
│       └── api.ts                   # TypeScript interfaces matching scheduler JSON responses
├── nginx.conf
├── Dockerfile
├── react-router.config.ts           # ssr: false
├── vite.config.ts
├── components.json                  # shadcn config
└── package.json
```

## Data Fetching Pattern

Each page component:
1. Declares data state with `useState` (data, loading, error)
2. Fetches in `useEffect` on mount (and on filter change)
3. Mutations call `api.ts` functions directly, then update local state optimistically

No global store. Each page owns its data. Shared state (sidebar pending count) fetched independently by the sidebar component on mount.

## Error & Loading States

- **Loading:** shadcn `Skeleton` rows in tables while fetching
- **Error:** shadcn `Alert` (destructive variant) with retry button
- **Empty:** Contextual empty state per page (e.g. review queue "all done" state)

## Authentication

Not in scope for this implementation. `reviewed_by` is sent as `""`. When auth is added, it will be read from session/context and injected by `api.ts`.
