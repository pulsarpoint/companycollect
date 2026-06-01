# Domain Crawl System — Design Spec

**Date:** 2026-05-18
**Status:** Approved

## Problem

Domains in corpscout are discovered through passive signals (certsh, wikidata, whois, etc.) but the actual web application behind each domain is never inspected. There is no way to see what the site looks like, what technology it runs, or to verify company data from the site itself. A future extraction pipeline (pulsarprotectpro3) needs raw web app data — HTML, headers, markdown, favicon — to perform technology detection without active scanning.

## Goal

1. Add rustfs (S3-compatible local object store) to the Docker stack for persistent file storage.
2. Create a `domain_crawl` River job that uses Crawl4AI (via the Python crawler) to fetch N pages of a domain, storing markdown, raw HTML, response headers, and favicon to rustfs.
3. Track crawl jobs in the database, linked to the `domains` table.
4. Create a new domain detail page showing domain identity, linked companies, and full crawl history with per-page content viewer.
5. Allow crawl jobs to be triggered from the domain list with a confirmation dialog.

## Non-Goals

- Extracting structured company data from crawled content (future phase).
- Crawling domains not yet linked to a company (future phase).
- Technology fingerprinting / analysis (pulsarprotectpro3 consumes the stored files directly).

## Architecture

```
domains list → action button → confirm dialog
                                    ↓
                        POST /api/v1/domains/:id/crawl
                                    ↓
                        scheduler: creates domain_crawl_jobs row
                        scheduler: enqueues DomainCrawlWorker (River)
                                    ↓
                        DomainCrawlWorker.Work()
                            ↓ POST /crawl-domain
                        Python crawler (Crawl4AI)
                            → returns pages[]{url,title,markdown,html,headers,status_code}
                            → returns favicon bytes
                                    ↓
                        scheduler: uploads to rustfs
                            crawls/{domain}/{job_id}/favicon.{ext}
                            crawls/{domain}/{job_id}/page_1.md
                            crawls/{domain}/{job_id}/page_1.html
                            crawls/{domain}/{job_id}/page_1_headers.json
                            ...
                        scheduler: writes s3_key, page rows to DB
                                    ↓
                    domain detail page (domains_.$id.tsx)
                        ├── identity card (domain, favicon, first seen)
                        ├── linked companies table
                        └── crawl history table
                              └── page list sheet
                                    └── markdown / html / headers viewer
```

## Section 1 — Infrastructure

### docker-compose.yml additions

```yaml
rustfs:
  image: rustfs/rustfs:latest
  environment:
    RUSTFS_ACCESS_KEY: corpscout
    RUSTFS_SECRET_KEY: corpscout123
    RUSTFS_VOLUMES: /data
  ports:
    - "9000:9000"   # S3 API
    - "9001:9001"   # Web console
  volumes:
    - ./data/rustfs:/data   # bind-mounted to local filesystem

scheduler:
  environment:
    # add to existing env block:
    CORPSCOUT_S3_ENDPOINT: http://rustfs:9000
    CORPSCOUT_S3_ACCESS_KEY: corpscout
    CORPSCOUT_S3_SECRET_KEY: corpscout123
    CORPSCOUT_S3_BUCKET: crawls
```

`./data/rustfs` is created on first run and persists between container restarts. Add `data/` to `.gitignore`.

### scheduler/internal/config/config.go additions

```go
S3Endpoint  string   // CORPSCOUT_S3_ENDPOINT, default "http://localhost:9000"
S3AccessKey string   // CORPSCOUT_S3_ACCESS_KEY, default "corpscout"
S3SecretKey string   // CORPSCOUT_S3_SECRET_KEY, default "corpscout123"
S3Bucket    string   // CORPSCOUT_S3_BUCKET, default "crawls"
```

S3 client (`aws-sdk-go-v2` with custom endpoint) is constructed in `app.go` and passed to `DomainCrawlWorker`.

## Section 2 — Database Schema

### New table: `domain_crawl_jobs`

```sql
CREATE TABLE domain_crawl_jobs (
    id           UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id    UUID    NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    river_job_id BIGINT,
    mode         TEXT    NOT NULL DEFAULT 'deep'
                 CHECK (mode IN ('homepage', 'deep')),
    max_pages    INTEGER NOT NULL DEFAULT 10,
    s3_prefix    TEXT,            -- e.g. "example.com/abc123/" set on job start
    favicon_s3_key TEXT,
    favicon_url  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON domain_crawl_jobs(domain_id, created_at DESC);
```

Status, errors, and timing come from `river_job` via `river_job_id` JOIN — not duplicated here.

### New table: `domain_crawl_job_pages`

```sql
CREATE TABLE domain_crawl_job_pages (
    id           UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID    NOT NULL REFERENCES domain_crawl_jobs(id) ON DELETE CASCADE,
    page_num     INTEGER NOT NULL,
    url          TEXT    NOT NULL,
    title        TEXT,
    status_code  INTEGER,
    content_type TEXT,
    md_s3_key    TEXT    NOT NULL,
    html_s3_key  TEXT    NOT NULL,
    headers_s3_key TEXT  NOT NULL
);

CREATE INDEX ON domain_crawl_job_pages(job_id, page_num);
```

### sqlc queries (`database/queries/domain_crawl.sql`)

The domain detail page fetches crawl jobs via the scheduler REST API, not PostgREST — so no PostgREST view is needed for `domain_crawl_jobs`. The scheduler API handler joins `river_job` directly (corpscout DB user has full access). The queries file exposes:

```sql
-- name: InsertDomainCrawlJob :one
-- name: SetDomainCrawlJobRiverID :exec
-- name: SetDomainCrawlJobS3Prefix :exec
-- name: SetDomainCrawlJobFavicon :exec
-- name: InsertDomainCrawlJobPage :exec
-- name: ListDomainCrawlJobs :many   (joins river_job for state/finalized_at/errors)
-- name: ListDomainCrawlJobPages :many
```

## Section 3 — Backend

### Python crawler: new endpoint `POST /crawl-domain`

**File:** `crawler/main.py` + `crawler/domain_crawler.py` (new)

```python
class DomainCrawlRequest(BaseModel):
    domain: str                               # e.g. "example.com"
    mode: Literal["homepage", "deep"] = "deep"
    max_pages: int = 10

class CrawledPage(BaseModel):
    url: str
    title: str | None
    markdown: str
    html: str
    headers: dict[str, str]
    status_code: int
    content_type: str | None

class DomainCrawlResponse(BaseModel):
    pages: list[CrawledPage]
    total_pages: int
    favicon_url: str | None
    favicon_bytes: str | None                 # base64-encoded

@app.post("/crawl-domain", response_model=DomainCrawlResponse)
async def crawl_domain(req: DomainCrawlRequest) -> DomainCrawlResponse:
    ...
```

**Behaviour:**
- `mode = "homepage"`: crawls only `https://{domain}` (1 page)
- `mode = "deep"`: uses Crawl4AI's `DeepCrawlStrategy` up to `max_pages` pages, starting from homepage
- Discovers favicon via `<link rel="icon">` or falls back to `/favicon.ico`; downloads and base64-encodes it
- Returns all pages with full HTML, markdown, headers, and status code

### Go scheduler: `DomainCrawlWorker`

**File:** `scheduler/internal/workers/domain_crawl.go` (new)

```go
type DomainCrawlArgs struct {
    DomainCrawlJobID string `json:"domain_crawl_job_id"`
    DomainID         string `json:"domain_id"`
    Domain           string `json:"domain"`
    Mode             string `json:"mode"`
    MaxPages         int    `json:"max_pages"`
}
func (DomainCrawlArgs) Kind() string { return "domain_crawl" }
```

**Work method steps:**
1. Update `domain_crawl_jobs.river_job_id` with the current River job ID
2. Set `s3_prefix = "{domain}/{job_id}/"` on the DB row
3. Call `POST /crawl-domain` on the Python crawler via `crawlerclient`
4. Upload `favicon.{ext}` to S3; update `favicon_s3_key` and `favicon_url`
5. For each page: upload `page_N.md`, `page_N.html`, `page_N_headers.json` to S3
6. Insert a `domain_crawl_job_pages` row per page with all S3 keys
7. On error: log with `slog.Error`, return error (River handles retries)

### crawlerclient additions

**File:** `scheduler/internal/crawlerclient/client.go`

```go
func (c *Client) CrawlDomain(ctx context.Context, domain, mode string, maxPages int) (*DomainCrawlResponse, error)
```

### REST API endpoints

**File:** `scheduler/internal/httpapi/domain_crawl.go` (new)

```
POST /api/v1/domains/:id/crawl
     body: { "mode": "homepage"|"deep", "max_pages": 10 }
     → 202 { "job_id": "...", "river_job_id": 123 }

GET  /api/v1/domains/:id/crawl-jobs
     → list of domain_crawl_jobs joined with river_job state

GET  /api/v1/domains/:id/crawl-jobs/:job_id/pages
     → list of domain_crawl_job_pages for the job

GET  /api/v1/domains/:id/crawl-jobs/:job_id/pages/:page_num/markdown
     → 200 text/markdown (fetched from S3)

GET  /api/v1/domains/:id/crawl-jobs/:job_id/pages/:page_num/html
     → 200 text/html (fetched from S3)

GET  /api/v1/domains/:id/crawl-jobs/:job_id/pages/:page_num/headers
     → 200 application/json (fetched from S3)

GET  /api/v1/domains/:id/crawl-jobs/:job_id/favicon
     → 200 image/* (fetched from S3, correct content-type)
```

**File:** `scheduler/internal/httpapi/handlers.go` — add route group under `/api/v1/domains/:id/crawl-jobs`.

**File:** `scheduler/internal/httpapi/domains.go` — add `GET /api/v1/domains/:id` for single domain lookup.

### s3client package

**File:** `scheduler/internal/s3client/client.go` (new)

Thin wrapper around `aws-sdk-go-v2` with custom endpoint. Exposes:
```go
func (c *Client) Upload(ctx context.Context, key string, body []byte, contentType string) error
func (c *Client) Download(ctx context.Context, key string) ([]byte, string, error)
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error
```

Bucket is created on scheduler startup if it does not exist.

## Section 4 — Frontend

### Domain list page changes (`ui/app/routes/domains.tsx`)

- Domain name column becomes a link to `/domains/:id`
- Add `···` action menu column (rightmost). One item: **Crawl domain**

**Crawl confirmation dialog** (new component `ui/app/components/app/CrawlDomainDialog.tsx`):

```
Crawl {domain}?

Mode:    ○ Homepage only
         ● Deep crawl

Max pages:  [10]    ← visible only for deep crawl, range 1–50

[Cancel]  [Start Crawl]
```

On confirm → `POST /api/v1/domains/:id/crawl` → `toast.success("Crawl job started")`.

### New domain detail page (`ui/app/routes/domains_.$id.tsx`)

**Data sources:**

| Section | Source |
|---|---|
| Domain info | `GET /api/v1/db/v_domains?id=eq.:id` (PostgREST) |
| Linked companies | `GET /api/v1/db/v_company_domains?domain_id=eq.:id` (PostgREST) |
| Crawl jobs | `GET /api/v1/domains/:id/crawl-jobs` (scheduler) |
| Page list | `GET /api/v1/domains/:id/crawl-jobs/:job_id/pages` (scheduler) |
| Content | `GET /api/v1/domains/:id/crawl-jobs/:job_id/pages/:n/markdown|html|headers` |
| Favicon | Rendered via `GET /api/v1/domains/:id/crawl-jobs/:job_id/favicon` |

**Layout:**

```
[← Domains]

[favicon] example.com [↗]                           [Trigger Crawl]
First seen: 2026-01-15  ·  Companies: 3  ·  Max confidence: 85

─── Linked Companies ──────────────────────────────────────
Company          Signal    Confidence  Status
ACME Corp        certsh    85          active
...

─── Crawl History ─────────────────────────────────────────
Date              Mode      Pages  Status     Actions
2026-05-18 14:32  deep      8      completed  [View pages]
2026-05-17 09:10  homepage  1      failed     —

```

**"View pages" sheet** — opens a side sheet listing all pages for the selected job:

```
Page 1   https://example.com            "Home"      200  [Markdown] [HTML] [Headers]
Page 2   https://example.com/about      "About Us"  200  [Markdown] [HTML] [Headers]
Page 3   https://example.com/contact    "Contact"   404  [Markdown] [HTML] [Headers]
```

Each content button opens a second nested sheet with a `<pre>` block + Copy button showing the fetched content.

Status in crawl history table is derived from `river_job.state`: `available/running` → "running" badge, `completed` → "completed", `discarded/cancelled` → "failed".

## Error Handling

- Crawl failure (crawler returns 5xx): River retries up to 3 times, then marks job as `discarded`. Error message from crawler is stored in `river_job.errors`.
- S3 upload failure: logged, job fails, River retries.
- Content fetch failure (S3 key missing): API returns 404; UI shows "Content unavailable" in the viewer.
- Trigger from UI: on API error, `toast.error("Failed to start crawl job.")`.
- Empty crawl result (crawler returns 0 pages): job completes with `pages_crawled = 0`, shown as "completed (0 pages)" in the UI.

## File Map

| File | Action |
|---|---|
| `docker-compose.yml` | Add `rustfs` service, add S3 env vars to `scheduler` |
| `.gitignore` | Add `data/` |
| `scheduler/internal/config/config.go` | Add 4 S3 fields |
| `scheduler/internal/app/app.go` | Construct S3 client, register DomainCrawlWorker |
| `scheduler/internal/s3client/client.go` | **Create** — S3 wrapper |
| `scheduler/internal/crawlerclient/client.go` | Add `CrawlDomain()` method |
| `scheduler/internal/workers/domain_crawl.go` | **Create** — River worker |
| `scheduler/internal/httpapi/domain_crawl.go` | **Create** — 6 REST handlers |
| `scheduler/internal/httpapi/handlers.go` | Register new routes |
| `scheduler/internal/httpapi/domains.go` | Add `GET /domains/:id` handler |
| `database/migrations/000023_domain_crawl_jobs.up.sql` | **Create** |
| `database/migrations/000023_domain_crawl_jobs.down.sql` | **Create** |
| `database/queries/domain_crawl.sql` | **Create** — sqlc queries |
| `crawler/domain_crawler.py` | **Create** — Crawl4AI logic |
| `crawler/main.py` | Add `POST /crawl-domain` route |
| `ui/app/routes/domains.tsx` | Add action button + link |
| `ui/app/routes/domains_.$id.tsx` | **Create** — detail page |
| `ui/app/components/app/CrawlDomainDialog.tsx` | **Create** — trigger dialog |
| `ui/app/lib/api.ts` | Add `triggerDomainCrawl`, `getDomainCrawlJobs`, `getDomainCrawlPages` |
| `ui/app/types/api.ts` | Add `DomainCrawlJob`, `DomainCrawlPage` types |
