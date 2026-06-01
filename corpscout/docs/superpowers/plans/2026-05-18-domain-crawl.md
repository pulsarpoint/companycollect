# Domain Crawl System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a domain crawl system to corpscout that stores per-page markdown, HTML, headers, and favicon to local S3 (rustfs), tracks jobs in the database via River, and surfaces crawl history on a new domain detail page with a trigger button on the domain list.

**Architecture:** A new `domain_crawl` River worker calls the Python crawler's new `POST /crawl-domain` endpoint (backed by Crawl4AI), uploads results to rustfs via a new `s3client` package, then writes DB rows linking pages to S3 keys. The scheduler exposes 6 REST endpoints for the frontend to trigger crawls, list jobs/pages, and serve S3 content. A new `domains_.$id.tsx` route shows identity, linked companies, and crawl history.

**Tech Stack:** Go (River, aws-sdk-go-v2, chi), Python (Crawl4AI, FastAPI, pydantic), React Router v7 (shadcn/ui, tanstack-table), PostgreSQL (sqlc), rustfs (S3-compatible object store)

---

## File Map

| File | Action |
|---|---|
| `docker-compose.yml` | Modify — add `rustfs` service + S3 env vars to `scheduler` |
| `.gitignore` | Modify — add `data/` |
| `scheduler/internal/config/config.go` | Modify — add 4 S3 fields |
| `scheduler/internal/app/app.go` | Modify — construct S3 client, pass to handlers + river |
| `scheduler/internal/app/river.go` | Modify — register `DomainCrawlWorker`, add `domain_crawl` queue |
| `scheduler/internal/s3client/client.go` | **Create** — thin S3 wrapper (Upload, Download, EnsureBucket) |
| `scheduler/internal/crawlerclient/client.go` | Modify — add `CrawlDomain()` + types |
| `scheduler/internal/workers/domain_crawl.go` | **Create** — River worker |
| `scheduler/internal/httpapi/handlers.go` | Modify — add `s3` field, update `NewHandlers`, register new routes |
| `scheduler/internal/httpapi/domain_crawl.go` | **Create** — 6 REST handlers |
| `scheduler/internal/httpapi/domains.go` | Modify — add `handleGetDomain` for `GET /api/v1/domains/:id` |
| `scheduler/internal/httpapi/testhelpers_test.go` | Modify — add new sqlc stubs, update `newTestHandlers` |
| `database/migrations/000023_domain_crawl_jobs.up.sql` | **Create** |
| `database/migrations/000023_domain_crawl_jobs.down.sql` | **Create** |
| `database/queries/domain_crawl.sql` | **Create** — 7 sqlc queries |
| `database/queries/domains.sql` | Modify — add `GetDomainByID` query |
| `crawler/requirements.txt` | Modify — add `crawl4ai` |
| `crawler/domain_crawler.py` | **Create** — Crawl4AI logic |
| `crawler/main.py` | Modify — add `POST /crawl-domain` route |
| `ui/app/types/api.ts` | Modify — add `DomainCrawlJob`, `DomainCrawlPage`, `DomainDetail` types |
| `ui/app/lib/api.ts` | Modify — add crawl fetch functions |
| `ui/app/components/app/CrawlDomainDialog.tsx` | **Create** — trigger dialog |
| `ui/app/routes/domains.tsx` | Modify — add domain link + action button |
| `ui/app/routes/domains_.$id.tsx` | **Create** — domain detail page |

---

### Task 1: Infrastructure — rustfs, config, .gitignore

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.gitignore`
- Modify: `scheduler/internal/config/config.go`

- [ ] **Step 1: Add rustfs service and S3 env vars to docker-compose.yml**

Read `docker-compose.yml` first. Then add the `rustfs` service block and the S3 env vars to the `scheduler` service's `environment` section:

```yaml
  rustfs:
    image: rustfs/rustfs:latest
    environment:
      RUSTFS_ACCESS_KEY: corpscout
      RUSTFS_SECRET_KEY: corpscout123
      RUSTFS_VOLUMES: /data
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - ./data/rustfs:/data

  scheduler:
    environment:
      # ... existing env vars ...
      CORPSCOUT_S3_ENDPOINT: http://rustfs:9000
      CORPSCOUT_S3_ACCESS_KEY: corpscout
      CORPSCOUT_S3_SECRET_KEY: corpscout123
      CORPSCOUT_S3_BUCKET: crawls
```

- [ ] **Step 2: Add data/ to .gitignore**

Read `.gitignore` first, then add `data/` to the end.

- [ ] **Step 3: Add S3 fields to config.go**

Read `scheduler/internal/config/config.go`. Add 4 fields to the `Config` struct with env-var bindings matching the existing pattern:

```go
S3Endpoint  string // CORPSCOUT_S3_ENDPOINT, default "http://localhost:9000"
S3AccessKey string // CORPSCOUT_S3_ACCESS_KEY, default "corpscout"
S3SecretKey string // CORPSCOUT_S3_SECRET_KEY, default "corpscout123"
S3Bucket    string // CORPSCOUT_S3_BUCKET, default "crawls"
```

Load them with the same `os.Getenv` / default pattern already used in that file.

- [ ] **Step 4: Build the scheduler to verify config compiles**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make build
```

Expected: `bin/worker` produced with no errors.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml .gitignore scheduler/internal/config/config.go
git commit -m "feat: add rustfs to docker stack and S3 config fields"
```

---

### Task 2: Database — migration + sqlc queries

**Files:**
- Create: `database/migrations/000023_domain_crawl_jobs.up.sql`
- Create: `database/migrations/000023_domain_crawl_jobs.down.sql`
- Create: `database/queries/domain_crawl.sql`
- Modify: `database/queries/domains.sql`

- [ ] **Step 1: Create up migration**

Create `database/migrations/000023_domain_crawl_jobs.up.sql`:

```sql
CREATE TABLE domain_crawl_jobs (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id      UUID        NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    river_job_id   BIGINT,
    mode           TEXT        NOT NULL DEFAULT 'deep'
                               CHECK (mode IN ('homepage', 'deep')),
    max_pages      INTEGER     NOT NULL DEFAULT 10,
    s3_prefix      TEXT,
    favicon_s3_key TEXT,
    favicon_url    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON domain_crawl_jobs(domain_id, created_at DESC);

CREATE TABLE domain_crawl_job_pages (
    id             UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID    NOT NULL REFERENCES domain_crawl_jobs(id) ON DELETE CASCADE,
    page_num       INTEGER NOT NULL,
    url            TEXT    NOT NULL,
    title          TEXT,
    status_code    INTEGER,
    content_type   TEXT,
    md_s3_key      TEXT    NOT NULL,
    html_s3_key    TEXT    NOT NULL,
    headers_s3_key TEXT    NOT NULL
);

CREATE INDEX ON domain_crawl_job_pages(job_id, page_num);
```

- [ ] **Step 2: Create down migration**

Create `database/migrations/000023_domain_crawl_jobs.down.sql`:

```sql
DROP TABLE IF EXISTS domain_crawl_job_pages;
DROP TABLE IF EXISTS domain_crawl_jobs;
```

- [ ] **Step 3: Apply migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make migrate-up
```

Expected: `migrate: 1/u 000023_domain_crawl_jobs` (no dirty state).

- [ ] **Step 4: Create sqlc queries for domain crawl**

Create `database/queries/domain_crawl.sql`:

```sql
-- name: InsertDomainCrawlJob :one
INSERT INTO domain_crawl_jobs (domain_id, mode, max_pages)
VALUES ($1, $2, $3)
RETURNING *;

-- name: SetDomainCrawlJobRiverID :exec
UPDATE domain_crawl_jobs SET river_job_id = $2 WHERE id = $1;

-- name: SetDomainCrawlJobS3Prefix :exec
UPDATE domain_crawl_jobs SET s3_prefix = $2 WHERE id = $1;

-- name: SetDomainCrawlJobFavicon :exec
UPDATE domain_crawl_jobs SET favicon_s3_key = $2, favicon_url = $3 WHERE id = $1;

-- name: InsertDomainCrawlJobPage :exec
INSERT INTO domain_crawl_job_pages (job_id, page_num, url, title, status_code, content_type, md_s3_key, html_s3_key, headers_s3_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: ListDomainCrawlJobs :many
SELECT
    j.id,
    j.domain_id,
    j.river_job_id,
    j.mode,
    j.max_pages,
    j.s3_prefix,
    j.favicon_s3_key,
    j.favicon_url,
    j.created_at,
    rj.state        AS river_state,
    rj.finalized_at AS river_finalized_at,
    rj.errors       AS river_errors
FROM domain_crawl_jobs j
LEFT JOIN river_job rj ON rj.id = j.river_job_id
WHERE j.domain_id = $1
ORDER BY j.created_at DESC;

-- name: ListDomainCrawlJobPages :many
SELECT *
FROM domain_crawl_job_pages
WHERE job_id = $1
ORDER BY page_num;

-- name: GetDomainCrawlJobPage :one
SELECT *
FROM domain_crawl_job_pages
WHERE job_id = $1 AND page_num = $2;

-- name: GetDomainCrawlJob :one
SELECT
    j.id,
    j.domain_id,
    j.river_job_id,
    j.mode,
    j.max_pages,
    j.s3_prefix,
    j.favicon_s3_key,
    j.favicon_url,
    j.created_at
FROM domain_crawl_jobs j
WHERE j.id = $1 AND j.domain_id = $2;
```

- [ ] **Step 5: Add GetDomainByID to domains.sql**

Read `database/queries/domains.sql` then append:

```sql
-- name: GetDomainByID :one
SELECT id, domain, first_seen_at, last_verified_at
FROM domains
WHERE id = $1;
```

- [ ] **Step 6: Run sqlc-generate**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make sqlc-generate
```

Expected: new files `scheduler/internal/db/gen/domain_crawl.sql.go` generated, `domains.sql.go` updated with `GetDomainByID`. No errors.

- [ ] **Step 7: Commit**

```bash
git add database/migrations/ database/queries/ scheduler/internal/db/gen/
git commit -m "feat: add domain_crawl_jobs migration and sqlc queries"
```

---

### Task 3: s3client package

**Files:**
- Create: `scheduler/internal/s3client/client.go`

- [ ] **Step 1: Add aws-sdk-go-v2 dependencies**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go get github.com/aws/aws-sdk-go-v2
GOWORK=off go get github.com/aws/aws-sdk-go-v2/config
GOWORK=off go get github.com/aws/aws-sdk-go-v2/credentials
GOWORK=off go get github.com/aws/aws-sdk-go-v2/service/s3
```

Expected: `go.mod` and `go.sum` updated.

- [ ] **Step 2: Create s3client package**

Create `scheduler/internal/s3client/client.go`:

```go
package s3client

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
)

type Client struct {
	s3     *s3.Client
	bucket string
}

func New(endpoint, accessKey, secretKey, bucket string) *Client {
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			},
		),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return &Client{s3: client, bucket: bucket}
}

func (c *Client) EnsureBucket(ctx context.Context) error {
	_, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		// BucketAlreadyOwnedByYou and BucketAlreadyExists are not errors for us
		var ae interface{ ErrorCode() string }
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
				return nil
			}
		}
		return errors.Wrap(err, "s3 create bucket")
	}
	return nil
}

func (c *Client) Upload(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return errors.Wrap(err, "s3 put object "+key)
}

func (c *Client) Download(ctx context.Context, key string) ([]byte, string, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", errors.Wrap(err, "s3 get object "+key)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", errors.Wrap(err, "s3 read body "+key)
	}
	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	return data, ct, nil
}
```

- [ ] **Step 3: Build to verify**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make build
```

Expected: builds without errors.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/s3client/ scheduler/go.mod scheduler/go.sum
git commit -m "feat: add s3client package wrapping aws-sdk-go-v2"
```

---

### Task 4: crawlerclient — CrawlDomain

**Files:**
- Modify: `scheduler/internal/crawlerclient/client.go`

- [ ] **Step 1: Read the existing client**

Read `scheduler/internal/crawlerclient/client.go` to understand the `postJSON` helper and existing types/patterns.

- [ ] **Step 2: Add CrawlDomain types and method**

Append to `scheduler/internal/crawlerclient/client.go` (after the existing code):

```go
type DomainCrawlRequest struct {
	Domain   string `json:"domain"`
	Mode     string `json:"mode"`
	MaxPages int    `json:"max_pages"`
}

type CrawledPage struct {
	URL         string            `json:"url"`
	Title       *string           `json:"title"`
	Markdown    string            `json:"markdown"`
	HTML        string            `json:"html"`
	Headers     map[string]string `json:"headers"`
	StatusCode  int               `json:"status_code"`
	ContentType *string           `json:"content_type"`
}

type DomainCrawlResponse struct {
	Pages        []CrawledPage `json:"pages"`
	TotalPages   int           `json:"total_pages"`
	FaviconURL   *string       `json:"favicon_url"`
	FaviconBytes *string       `json:"favicon_bytes"` // base64-encoded
}

func (c *Client) CrawlDomain(ctx context.Context, domain, mode string, maxPages int) (*DomainCrawlResponse, error) {
	req := DomainCrawlRequest{Domain: domain, Mode: mode, MaxPages: maxPages}
	var resp DomainCrawlResponse
	if err := c.postJSON(ctx, "/crawl-domain", req, &resp); err != nil {
		return nil, errors.Wrap(err, "crawler POST /crawl-domain")
	}
	return &resp, nil
}
```

- [ ] **Step 3: Build to verify**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make build
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/crawlerclient/client.go
git commit -m "feat: add CrawlDomain method to crawlerclient"
```

---

### Task 5: Python crawler — crawl4ai integration

**Files:**
- Modify: `crawler/requirements.txt`
- Create: `crawler/domain_crawler.py`
- Modify: `crawler/main.py`

- [ ] **Step 1: Add crawl4ai to requirements.txt**

Read `crawler/requirements.txt`. Append:

```
crawl4ai==0.6.3
```

(Use the latest stable release available at time of implementation — verify with `pip index versions crawl4ai` if needed.)

- [ ] **Step 2: Create crawler/domain_crawler.py**

Create `crawler/domain_crawler.py`:

```python
from __future__ import annotations

import asyncio
import base64
import re
from typing import Literal

import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from pydantic import BaseModel


class DomainCrawlRequest(BaseModel):
    domain: str
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
    favicon_bytes: str | None  # base64-encoded


def _extract_favicon_url(html: str, base_url: str) -> str | None:
    """Find favicon URL from <link rel="icon"> or fall back to /favicon.ico."""
    match = re.search(
        r'<link[^>]+rel=["\'](?:shortcut )?icon["\'][^>]+href=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if match:
        href = match.group(1)
        if href.startswith("http"):
            return href
        from urllib.parse import urljoin
        return urljoin(base_url, href)
    # fallback
    return f"https://{base_url.split('//')[-1].split('/')[0]}/favicon.ico"


async def _fetch_favicon(url: str) -> str | None:
    """Download favicon and return base64-encoded bytes, or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return base64.b64encode(resp.content).decode()
    except Exception:
        pass
    return None


async def crawl_domain(req: DomainCrawlRequest) -> DomainCrawlResponse:
    browser_cfg = BrowserConfig(headless=True, verbose=False)
    start_url = f"https://{req.domain}"

    if req.mode == "homepage":
        run_cfg = CrawlerRunConfig(page_timeout=30000)
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=start_url, config=run_cfg)
        results = [result] if result.success else []
    else:
        strategy = BFSDeepCrawlStrategy(
            max_depth=3,
            max_pages=req.max_pages,
            filter_links=True,
        )
        run_cfg = CrawlerRunConfig(
            deep_crawl_strategy=strategy,
            page_timeout=30000,
        )
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            results = await crawler.arun(url=start_url, config=run_cfg)
        if not isinstance(results, list):
            results = [results] if results.success else []

    pages: list[CrawledPage] = []
    favicon_url: str | None = None
    favicon_bytes: str | None = None

    for r in results:
        if not r.success:
            continue
        # Extract headers as dict[str, str]
        headers: dict[str, str] = {}
        if hasattr(r, "response_headers") and r.response_headers:
            for k, v in r.response_headers.items():
                headers[k] = str(v)
        ct = headers.get("content-type") or headers.get("Content-Type")

        pages.append(CrawledPage(
            url=r.url,
            title=getattr(r, "metadata", {}).get("title") if hasattr(r, "metadata") else None,
            markdown=r.markdown or "",
            html=r.html or "",
            headers=headers,
            status_code=getattr(r, "status_code", 200) or 200,
            content_type=ct,
        ))

        # grab favicon from first page
        if not favicon_url and r.html:
            favicon_url = _extract_favicon_url(r.html, r.url)

    if favicon_url and not favicon_bytes:
        favicon_bytes = await _fetch_favicon(favicon_url)

    return DomainCrawlResponse(
        pages=pages,
        total_pages=len(pages),
        favicon_url=favicon_url,
        favicon_bytes=favicon_bytes,
    )
```

- [ ] **Step 3: Add POST /crawl-domain route to main.py**

Read `crawler/main.py`. Add at the top of the imports (if not already present):

```python
from domain_crawler import DomainCrawlRequest, DomainCrawlResponse, crawl_domain
```

And add the endpoint in the route section:

```python
@app.post("/crawl-domain", response_model=DomainCrawlResponse)
async def crawl_domain_endpoint(req: DomainCrawlRequest) -> DomainCrawlResponse:
    return await crawl_domain(req)
```

- [ ] **Step 4: Verify crawler starts (syntax check)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
python -c "import ast; ast.parse(open('main.py').read()); ast.parse(open('domain_crawler.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 5: Commit**

```bash
git add crawler/requirements.txt crawler/domain_crawler.py crawler/main.py
git commit -m "feat: add crawl4ai domain crawler endpoint to Python service"
```

---

### Task 6: DomainCrawlWorker + app wiring

**Files:**
- Create: `scheduler/internal/workers/domain_crawl.go`
- Modify: `scheduler/internal/app/river.go`
- Modify: `scheduler/internal/app/app.go`

- [ ] **Step 1: Read existing worker and app files**

Read these files to understand exact patterns:
- `scheduler/internal/workers/source_pull.go`
- `scheduler/internal/app/river.go`
- `scheduler/internal/app/app.go`

- [ ] **Step 2: Create domain_crawl.go worker**

Create `scheduler/internal/workers/domain_crawl.go`:

```go
package workers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

type DomainCrawlArgs struct {
	DomainCrawlJobID string `json:"domain_crawl_job_id"`
	DomainID         string `json:"domain_id"`
	Domain           string `json:"domain"`
	Mode             string `json:"mode"`
	MaxPages         int    `json:"max_pages"`
}

func (DomainCrawlArgs) Kind() string { return "domain_crawl" }

type DomainCrawlWorker struct {
	river.WorkerDefaults[DomainCrawlArgs]
	db      db.Querier
	crawler *crawlerclient.Client
	s3      *s3client.Client
}

func NewDomainCrawlWorker(q db.Querier, crawler *crawlerclient.Client, s3 *s3client.Client) *DomainCrawlWorker {
	return &DomainCrawlWorker{db: q, crawler: crawler, s3: s3}
}

func (w *DomainCrawlWorker) Work(ctx context.Context, job *river.Job[DomainCrawlArgs]) error {
	args := job.Args
	jobID, err := uuid.Parse(args.DomainCrawlJobID)
	if err != nil {
		return errors.Wrap(err, "parse domain_crawl_job_id")
	}

	// 1. Link river job ID
	if err := w.db.SetDomainCrawlJobRiverID(ctx, db.SetDomainCrawlJobRiverIDParams{
		ID:          jobID,
		RiverJobID:  &job.ID,
	}); err != nil {
		slog.Error("set river job id", "error", err, "job_id", jobID)
		return errors.Wrap(err, "set river job id")
	}

	// 2. Set S3 prefix
	prefix := fmt.Sprintf("%s/%s/", args.Domain, args.DomainCrawlJobID)
	if err := w.db.SetDomainCrawlJobS3Prefix(ctx, db.SetDomainCrawlJobS3PrefixParams{
		ID:       jobID,
		S3Prefix: &prefix,
	}); err != nil {
		return errors.Wrap(err, "set s3 prefix")
	}

	// 3. Call Python crawler
	resp, err := w.crawler.CrawlDomain(ctx, args.Domain, args.Mode, args.MaxPages)
	if err != nil {
		slog.Error("domain crawl failed", "domain", args.Domain, "job_id", jobID, "error", err)
		return errors.Wrap(err, "crawl domain")
	}

	// 4. Upload favicon
	if resp.FaviconBytes != nil && resp.FaviconURL != nil {
		decoded, decErr := base64.StdEncoding.DecodeString(*resp.FaviconBytes)
		if decErr == nil {
			ext := filepath.Ext(*resp.FaviconURL)
			if ext == "" {
				ext = ".ico"
			}
			faviconKey := prefix + "favicon" + ext
			ct := faviconContentType(ext)
			if uploadErr := w.s3.Upload(ctx, faviconKey, decoded, ct); uploadErr != nil {
				slog.Error("upload favicon", "error", uploadErr, "key", faviconKey)
			} else {
				if dbErr := w.db.SetDomainCrawlJobFavicon(ctx, db.SetDomainCrawlJobFaviconParams{
					ID:           jobID,
					FaviconS3Key: &faviconKey,
					FaviconUrl:   resp.FaviconURL,
				}); dbErr != nil {
					return errors.Wrap(dbErr, "set favicon s3 key")
				}
			}
		}
	}

	// 5. Upload pages and insert page rows
	for i, page := range resp.Pages {
		pageNum := i + 1
		mdKey := fmt.Sprintf("%spage_%d.md", prefix, pageNum)
		htmlKey := fmt.Sprintf("%spage_%d.html", prefix, pageNum)
		headersKey := fmt.Sprintf("%spage_%d_headers.json", prefix, pageNum)

		headersJSON, _ := json.Marshal(page.Headers)

		if err := w.s3.Upload(ctx, mdKey, []byte(page.Markdown), "text/markdown"); err != nil {
			slog.Error("upload markdown", "error", err, "key", mdKey)
			return errors.Wrap(err, "upload page markdown")
		}
		if err := w.s3.Upload(ctx, htmlKey, []byte(page.HTML), "text/html"); err != nil {
			slog.Error("upload html", "error", err, "key", htmlKey)
			return errors.Wrap(err, "upload page html")
		}
		if err := w.s3.Upload(ctx, headersKey, headersJSON, "application/json"); err != nil {
			slog.Error("upload headers", "error", err, "key", headersKey)
			return errors.Wrap(err, "upload page headers")
		}

		if err := w.db.InsertDomainCrawlJobPage(ctx, db.InsertDomainCrawlJobPageParams{
			JobID:         jobID,
			PageNum:       int32(pageNum),
			Url:           page.URL,
			Title:         page.Title,
			StatusCode:    intPtr(int32(page.StatusCode)),
			ContentType:   page.ContentType,
			MdS3Key:       mdKey,
			HtmlS3Key:     htmlKey,
			HeadersS3Key:  headersKey,
		}); err != nil {
			return errors.Wrap(err, "insert page row")
		}
	}

	return nil
}

func faviconContentType(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".svg":
		return "image/svg+xml"
	case ".gif":
		return "image/gif"
	default:
		return "image/x-icon"
	}
}

func intPtr(v int32) *int32 { return &v }
```

**Note:** The exact field names for `SetDomainCrawlJobRiverIDParams`, `SetDomainCrawlJobS3PrefixParams`, etc. are determined by the generated sqlc code. After running `make sqlc-generate` in Task 2, read `scheduler/internal/db/gen/domain_crawl.sql.go` to see the exact struct field names and adjust this worker accordingly before building.

- [ ] **Step 3: Register worker in river.go**

Read `scheduler/internal/app/river.go`. Add `DomainCrawlWorker` to the worker registration and add the `domain_crawl` queue. The function signature needs to accept `s3 *s3client.Client`. Update `setupRiver` accordingly:

Add parameter `s3 *s3client.Client` to `setupRiver`. Inside the function:

```go
river.AddWorker(workers, workers.NewDomainCrawlWorker(q, crawler, s3))
```

And add to the queues map:

```go
"domain_crawl": {MaxWorkers: 3},
```

- [ ] **Step 4: Wire s3client in app.go**

Read `scheduler/internal/app/app.go`. In `NewServer`:

1. Construct the S3 client after config is loaded:
```go
s3 := s3client.New(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket)
if err := s3.EnsureBucket(ctx); err != nil {
    return nil, errors.Wrap(err, "ensure S3 bucket")
}
```

2. Pass `s3` to `setupRiver(ctx, cfg, pool, q, crawler, s3)`.

3. Pass `s3` to `NewHandlers` (updated in Task 7).

- [ ] **Step 5: Verify build**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make build
```

Expected: no errors. Fix any sqlc field name mismatches by reading the generated `domain_crawl.sql.go`.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/workers/domain_crawl.go scheduler/internal/app/
git commit -m "feat: add DomainCrawlWorker River job with S3 upload"
```

---

### Task 7: REST API handlers

**Files:**
- Create: `scheduler/internal/httpapi/domain_crawl.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/domains.go`
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Update handlers.go — add s3 field and new routes**

Read `scheduler/internal/httpapi/handlers.go`. Make these changes:

1. Add `s3 *s3client.Client` import and field to `Handlers`:
```go
import "github.com/pulsarpoint/corpscout/scheduler/internal/s3client"

type Handlers struct {
    db           db.Querier
    rv           *river.Client[pgx.Tx]
    pool         *pgxpool.Pool
    crawler      *crawlerclient.Client
    s3           *s3client.Client
    postgrestURL string
}
```

2. Update `NewHandlers` signature:
```go
func NewHandlers(q db.Querier, rv *river.Client[pgx.Tx], pool *pgxpool.Pool, crawler *crawlerclient.Client, s3 *s3client.Client, postgrestURL string) *Handlers {
    return &Handlers{db: q, rv: rv, pool: pool, crawler: crawler, s3: s3, postgrestURL: postgrestURL}
}
```

3. Add routes inside `RegisterRoutes` under the `/api/v1` block:
```go
r.Get("/domains/{id}", h.handleGetDomain)
r.Post("/domains/{id}/crawl", h.handleTriggerDomainCrawl)
r.Get("/domains/{id}/crawl-jobs", h.handleListDomainCrawlJobs)
r.Get("/domains/{id}/crawl-jobs/{job_id}", h.handleGetDomainCrawlJob)
r.Get("/domains/{id}/crawl-jobs/{job_id}/pages", h.handleListDomainCrawlJobPages)
r.Get("/domains/{id}/crawl-jobs/{job_id}/pages/{page_num}/markdown", h.handleGetPageMarkdown)
r.Get("/domains/{id}/crawl-jobs/{job_id}/pages/{page_num}/html", h.handleGetPageHTML)
r.Get("/domains/{id}/crawl-jobs/{job_id}/pages/{page_num}/headers", h.handleGetPageHeaders)
r.Get("/domains/{id}/crawl-jobs/{job_id}/favicon", h.handleGetJobFavicon)
```

- [ ] **Step 2: Add handleGetDomain to domains.go**

Read `scheduler/internal/httpapi/domains.go`. Append:

```go
func (h *Handlers) handleGetDomain(w http.ResponseWriter, r *http.Request) {
    idStr := chi.URLParam(r, "id")
    id, err := uuid.Parse(idStr)
    if err != nil {
        writeError(w, http.StatusBadRequest, "invalid domain id")
        return
    }
    domain, err := h.db.GetDomainByID(r.Context(), id)
    if err != nil {
        writeError(w, http.StatusNotFound, "domain not found")
        return
    }
    writeJSON(w, http.StatusOK, domain)
}
```

- [ ] **Step 3: Create domain_crawl.go handlers**

Create `scheduler/internal/httpapi/domain_crawl.go`:

```go
package httpapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

type triggerCrawlRequest struct {
	Mode     string `json:"mode"`
	MaxPages int    `json:"max_pages"`
}

func (h *Handlers) handleTriggerDomainCrawl(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	domainID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid domain id")
		return
	}

	var req triggerCrawlRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Mode == "" {
		req.Mode = "deep"
	}
	if req.MaxPages <= 0 {
		req.MaxPages = 10
	}
	if req.Mode != "homepage" && req.Mode != "deep" {
		writeError(w, http.StatusBadRequest, "mode must be homepage or deep")
		return
	}

	domain, err := h.db.GetDomainByID(r.Context(), domainID)
	if err != nil {
		writeError(w, http.StatusNotFound, "domain not found")
		return
	}

	job, err := h.db.InsertDomainCrawlJob(r.Context(), db.InsertDomainCrawlJobParams{
		DomainID: domainID,
		Mode:     req.Mode,
		MaxPages: int32(req.MaxPages),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create crawl job")
		return
	}

	riverJob, err := h.rv.Insert(r.Context(), workers.DomainCrawlArgs{
		DomainCrawlJobID: job.ID.String(),
		DomainID:         domainID.String(),
		Domain:           domain.Domain,
		Mode:             req.Mode,
		MaxPages:         req.MaxPages,
	}, &river.InsertOpts{Queue: "domain_crawl"})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to enqueue crawl job")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":       job.ID,
		"river_job_id": riverJob.Job.ID,
	})
}

func (h *Handlers) handleListDomainCrawlJobs(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	domainID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid domain id")
		return
	}
	jobs, err := h.db.ListDomainCrawlJobs(r.Context(), domainID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list crawl jobs")
		return
	}
	if jobs == nil {
		jobs = []db.ListDomainCrawlJobsRow{}
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (h *Handlers) handleGetDomainCrawlJob(w http.ResponseWriter, r *http.Request) {
	domainID, jobID, ok := parseDomainAndJobID(w, r)
	if !ok {
		return
	}
	job, err := h.db.GetDomainCrawlJob(r.Context(), db.GetDomainCrawlJobParams{
		ID:       jobID,
		DomainID: domainID,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "crawl job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handlers) handleListDomainCrawlJobPages(w http.ResponseWriter, r *http.Request) {
	_, jobID, ok := parseDomainAndJobID(w, r)
	if !ok {
		return
	}
	pages, err := h.db.ListDomainCrawlJobPages(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list pages")
		return
	}
	if pages == nil {
		pages = []db.DomainCrawlJobPage{}
	}
	writeJSON(w, http.StatusOK, pages)
}

func (h *Handlers) handleGetPageMarkdown(w http.ResponseWriter, r *http.Request) {
	page, ok := h.fetchPage(w, r)
	if !ok {
		return
	}
	data, _, err := h.s3.Download(r.Context(), page.MdS3Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "content unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/markdown")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handlers) handleGetPageHTML(w http.ResponseWriter, r *http.Request) {
	page, ok := h.fetchPage(w, r)
	if !ok {
		return
	}
	data, _, err := h.s3.Download(r.Context(), page.HtmlS3Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "content unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handlers) handleGetPageHeaders(w http.ResponseWriter, r *http.Request) {
	page, ok := h.fetchPage(w, r)
	if !ok {
		return
	}
	data, _, err := h.s3.Download(r.Context(), page.HeadersS3Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "content unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handlers) handleGetJobFavicon(w http.ResponseWriter, r *http.Request) {
	_, jobID, ok := parseDomainAndJobID(w, r)
	if !ok {
		return
	}
	job, err := h.db.GetDomainCrawlJob(r.Context(), db.GetDomainCrawlJobParams{
		ID:       jobID,
		DomainID: mustParseUUID(chi.URLParam(r, "id")),
	})
	if err != nil || job.FaviconS3Key == nil {
		writeError(w, http.StatusNotFound, "favicon not available")
		return
	}
	data, ct, err := h.s3.Download(r.Context(), *job.FaviconS3Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "favicon not available")
		return
	}
	if ct == "" {
		ct = "image/x-icon"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// helpers

func parseDomainAndJobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, bool) {
	domainID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid domain id")
		return uuid.UUID{}, uuid.UUID{}, false
	}
	jobID, err := uuid.Parse(chi.URLParam(r, "job_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return uuid.UUID{}, uuid.UUID{}, false
	}
	return domainID, jobID, true
}

func (h *Handlers) fetchPage(w http.ResponseWriter, r *http.Request) (db.DomainCrawlJobPage, bool) {
	_, jobID, ok := parseDomainAndJobID(w, r)
	if !ok {
		return db.DomainCrawlJobPage{}, false
	}
	pageNumStr := chi.URLParam(r, "page_num")
	pageNum, err := strconv.Atoi(pageNumStr)
	if err != nil || pageNum < 1 {
		writeError(w, http.StatusBadRequest, "invalid page_num")
		return db.DomainCrawlJobPage{}, false
	}
	page, err := h.db.GetDomainCrawlJobPage(r.Context(), db.GetDomainCrawlJobPageParams{
		JobID:   jobID,
		PageNum: int32(pageNum),
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "page not found")
		return db.DomainCrawlJobPage{}, false
	}
	return page, true
}

func mustParseUUID(s string) uuid.UUID {
	id, _ := uuid.Parse(s)
	return id
}

func decodeJSON(r *http.Request, v any) error {
	import "encoding/json"
	return json.NewDecoder(r.Body).Decode(v)
}
```

**Note:** Move the `decodeJSON` helper to `handlers.go` instead of defining it inside the file (Go doesn't allow inline imports). Read `handlers.go` — if `decodeJSON` doesn't exist there, add it:

```go
func decodeJSON(r *http.Request, v any) error {
    return json.NewDecoder(r.Body).Decode(v)
}
```

And remove the inline import from `domain_crawl.go`.

- [ ] **Step 4: Update testhelpers_test.go**

Read `scheduler/internal/httpapi/testhelpers_test.go`. 

1. Update `newTestHandlers` to pass `nil` for the new `s3` param:
```go
func newTestHandlers(q db.Querier) *httpapi.Handlers {
    return httpapi.NewHandlers(q, nil, nil, nil, nil, "")
}
```

2. Add stub implementations for the new sqlc methods (zero-value stubs at the bottom of the file):
```go
func (s *stubQuerier) InsertDomainCrawlJob(ctx context.Context, arg db.InsertDomainCrawlJobParams) (db.DomainCrawlJob, error) {
    return db.DomainCrawlJob{}, nil
}
func (s *stubQuerier) SetDomainCrawlJobRiverID(ctx context.Context, arg db.SetDomainCrawlJobRiverIDParams) error {
    return nil
}
func (s *stubQuerier) SetDomainCrawlJobS3Prefix(ctx context.Context, arg db.SetDomainCrawlJobS3PrefixParams) error {
    return nil
}
func (s *stubQuerier) SetDomainCrawlJobFavicon(ctx context.Context, arg db.SetDomainCrawlJobFaviconParams) error {
    return nil
}
func (s *stubQuerier) InsertDomainCrawlJobPage(ctx context.Context, arg db.InsertDomainCrawlJobPageParams) error {
    return nil
}
func (s *stubQuerier) ListDomainCrawlJobs(ctx context.Context, domainID uuid.UUID) ([]db.ListDomainCrawlJobsRow, error) {
    return nil, nil
}
func (s *stubQuerier) ListDomainCrawlJobPages(ctx context.Context, jobID uuid.UUID) ([]db.DomainCrawlJobPage, error) {
    return nil, nil
}
func (s *stubQuerier) GetDomainCrawlJobPage(ctx context.Context, arg db.GetDomainCrawlJobPageParams) (db.DomainCrawlJobPage, error) {
    return db.DomainCrawlJobPage{}, nil
}
func (s *stubQuerier) GetDomainCrawlJob(ctx context.Context, arg db.GetDomainCrawlJobParams) (db.DomainCrawlJob, error) {
    return db.DomainCrawlJob{}, nil
}
func (s *stubQuerier) GetDomainByID(ctx context.Context, id uuid.UUID) (db.Domain, error) {
    return db.Domain{}, nil
}
```

The **exact type names and param struct names** come from the generated code in `scheduler/internal/db/gen/domain_crawl.sql.go` — read that file before writing these stubs to get exact names.

- [ ] **Step 5: Build and test**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make build
GOWORK=off make test
```

Expected: builds and all tests pass.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/httpapi/
git commit -m "feat: add domain crawl REST API handlers and domain GET endpoint"
```

---

### Task 8: Frontend types and API helpers

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`

- [ ] **Step 1: Read existing type and API files**

Read `ui/app/types/api.ts` and `ui/app/lib/api.ts` to understand existing type and function patterns.

- [ ] **Step 2: Add types to api.ts**

Append to `ui/app/types/api.ts`:

```typescript
export interface DomainDetail {
  id: string;
  domain: string;
  first_seen_at: string;
  last_verified_at: string | null;
}

export interface DomainCrawlJob {
  id: string;
  domain_id: string;
  river_job_id: number | null;
  mode: "homepage" | "deep";
  max_pages: number;
  s3_prefix: string | null;
  favicon_s3_key: string | null;
  favicon_url: string | null;
  created_at: string;
  river_state: string | null;
  river_finalized_at: string | null;
  river_errors: unknown[] | null;
}

export interface DomainCrawlPage {
  id: string;
  job_id: string;
  page_num: number;
  url: string;
  title: string | null;
  status_code: number | null;
  content_type: string | null;
  md_s3_key: string;
  html_s3_key: string;
  headers_s3_key: string;
}

export interface TriggerCrawlRequest {
  mode: "homepage" | "deep";
  max_pages: number;
}

export interface TriggerCrawlResponse {
  job_id: string;
  river_job_id: number;
}
```

- [ ] **Step 3: Add API functions to api.ts**

Read `ui/app/lib/api.ts` to see the `get<T>` and `post<T>` helpers. Append:

```typescript
export function triggerDomainCrawl(domainId: string, req: TriggerCrawlRequest): Promise<TriggerCrawlResponse> {
  return post<TriggerCrawlResponse>(`/domains/${domainId}/crawl`, req);
}

export function getDomain(domainId: string): Promise<DomainDetail> {
  return get<DomainDetail>(`/domains/${domainId}`);
}

export function getDomainCrawlJobs(domainId: string): Promise<DomainCrawlJob[]> {
  return get<DomainCrawlJob[]>(`/domains/${domainId}/crawl-jobs`);
}

export function getDomainCrawlPages(domainId: string, jobId: string): Promise<DomainCrawlPage[]> {
  return get<DomainCrawlPage[]>(`/domains/${domainId}/crawl-jobs/${jobId}/pages`);
}

export function getDomainCrawlPageContent(
  domainId: string,
  jobId: string,
  pageNum: number,
  type: "markdown" | "html" | "headers"
): Promise<string> {
  return get<string>(`/domains/${domainId}/crawl-jobs/${jobId}/pages/${pageNum}/${type}`);
}

export function getDomainFaviconUrl(domainId: string, jobId: string): string {
  return `/api/v1/domains/${domainId}/crawl-jobs/${jobId}/favicon`;
}
```

**Note:** `getDomainCrawlPageContent` returns raw text for markdown/html and JSON string for headers. If the `get<T>` helper always parses JSON, you'll need a separate `getRaw` helper. Read `api.ts` to check — if needed, add:

```typescript
async function getRaw(path: string): Promise<string> {
  const resp = await fetch(`/api/v1${path}`);
  if (!resp.ok) throw new Error(`API error ${resp.status}`);
  return resp.text();
}
```

And update `getDomainCrawlPageContent` to use `getRaw`.

- [ ] **Step 4: Type-check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors related to new types.

- [ ] **Step 5: Commit**

```bash
git add ui/app/types/api.ts ui/app/lib/api.ts
git commit -m "feat: add domain crawl TypeScript types and API helpers"
```

---

### Task 9: CrawlDomainDialog component

**Files:**
- Create: `ui/app/components/app/CrawlDomainDialog.tsx`

- [ ] **Step 1: Read an existing dialog component for patterns**

Read one of the existing dialog components in `ui/app/components/app/` to understand import patterns, Dialog/Button/toast usage.

- [ ] **Step 2: Create CrawlDomainDialog.tsx**

Create `ui/app/components/app/CrawlDomainDialog.tsx`:

```tsx
import { useState } from "react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";
import { triggerDomainCrawl } from "~/lib/api";

interface Props {
  domainId: string;
  domainName: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess?: () => void;
}

export function CrawlDomainDialog({ domainId, domainName, open, onOpenChange, onSuccess }: Props) {
  const [mode, setMode] = useState<"homepage" | "deep">("deep");
  const [maxPages, setMaxPages] = useState(10);
  const [loading, setLoading] = useState(false);

  async function handleSubmit() {
    setLoading(true);
    try {
      await triggerDomainCrawl(domainId, { mode, max_pages: maxPages });
      toast.success("Crawl job started");
      onOpenChange(false);
      onSuccess?.();
    } catch {
      toast.error("Failed to start crawl job.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Crawl {domainName}?</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Mode</Label>
            <RadioGroup value={mode} onValueChange={(v) => setMode(v as "homepage" | "deep")}>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="homepage" id="homepage" />
                <Label htmlFor="homepage">Homepage only</Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="deep" id="deep" />
                <Label htmlFor="deep">Deep crawl</Label>
              </div>
            </RadioGroup>
          </div>
          {mode === "deep" && (
            <div className="space-y-2">
              <Label htmlFor="max-pages">Max pages</Label>
              <Input
                id="max-pages"
                type="number"
                min={1}
                max={50}
                value={maxPages}
                onChange={(e) => setMaxPages(Number(e.target.value))}
                className="w-24"
              />
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={loading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={loading}>
            {loading ? "Starting…" : "Start Crawl"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
```

- [ ] **Step 3: Type-check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add ui/app/components/app/CrawlDomainDialog.tsx
git commit -m "feat: add CrawlDomainDialog component"
```

---

### Task 10: domains.tsx — add link and action button

**Files:**
- Modify: `ui/app/routes/domains.tsx`

- [ ] **Step 1: Read domains.tsx**

Read `ui/app/routes/domains.tsx` to understand the exact column definitions, DataTable usage, and existing import structure.

- [ ] **Step 2: Make domain name a link to /domains/:id**

In the domain name column definition, wrap the domain name in a `Link` component:

```tsx
import { Link } from "react-router";

// In column definition:
{
  accessorKey: "domain",
  header: "Domain",
  cell: ({ row }) => (
    <Link
      to={`/domains/${row.original.domain_id}`}
      className="font-medium hover:underline"
    >
      {row.getValue("domain")}
    </Link>
  ),
},
```

**Note:** The domain ID field name (`domain_id`) must match the actual field in the `VDomain` type. Read `ui/app/types/api.ts` and the existing row type to verify.

- [ ] **Step 3: Add action column with crawl button**

Import the needed components and `CrawlDomainDialog`. Add state for the dialog at the top of the component:

```tsx
import { useState } from "react";
import { MoreHorizontal } from "lucide-react";
import { Button } from "~/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "~/components/ui/dropdown-menu";
import { CrawlDomainDialog } from "~/components/app/CrawlDomainDialog";

// Inside the component:
const [crawlTarget, setCrawlTarget] = useState<{ id: string; domain: string } | null>(null);
```

Add the action column as the last column:

```tsx
{
  id: "actions",
  cell: ({ row }) => (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon">
          <MoreHorizontal className="h-4 w-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem
          onClick={() =>
            setCrawlTarget({ id: row.original.domain_id, domain: row.original.domain })
          }
        >
          Crawl domain
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  ),
},
```

Render the dialog after the table:

```tsx
{crawlTarget && (
  <CrawlDomainDialog
    domainId={crawlTarget.id}
    domainName={crawlTarget.domain}
    open={!!crawlTarget}
    onOpenChange={(open) => { if (!open) setCrawlTarget(null); }}
  />
)}
```

- [ ] **Step 4: Type-check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add ui/app/routes/domains.tsx
git commit -m "feat: add domain link and crawl action to domains list"
```

---

### Task 11: domains_.$id.tsx — domain detail page

**Files:**
- Create: `ui/app/routes/domains_.$id.tsx`

- [ ] **Step 1: Read the companies_.$id.tsx page for layout patterns**

Read `ui/app/routes/companies_.$id.tsx` to understand how loader data is typed, how PostgREST views are used, how sheets are opened, and how the back-link is styled.

- [ ] **Step 2: Read company_domains view type**

Read `ui/app/types/api.ts` and look for `VCompanyDomain` or similar type used in the company detail page for the domain relationship table. You will use `v_company_domains?domain_id=eq.{id}` (PostgREST) for the linked companies section.

- [ ] **Step 3: Create domains_.$id.tsx**

Create `ui/app/routes/domains_.$id.tsx`:

```tsx
import { useEffect, useState } from "react";
import { Link, useParams } from "react-router";
import { ArrowLeft, ExternalLink } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { CrawlDomainDialog } from "~/components/app/CrawlDomainDialog";
import {
  getDomain,
  getDomainCrawlJobs,
  getDomainCrawlPages,
  getDomainCrawlPageContent,
  getDomainFaviconUrl,
} from "~/lib/api";
import { pgrest } from "~/lib/pgrest";
import type { DomainDetail, DomainCrawlJob, DomainCrawlPage } from "~/types/api";

// Company-domain relationship row from PostgREST v_company_domains view
interface CompanyDomainRow {
  company_id: string;
  company_name: string;
  signal: string;
  confidence: number;
  status: string;
}

function crawlJobStatus(job: DomainCrawlJob): string {
  if (!job.river_state) return "pending";
  switch (job.river_state) {
    case "completed":
      return "completed";
    case "available":
    case "running":
    case "retryable":
      return "running";
    case "discarded":
    case "cancelled":
      return "failed";
    default:
      return job.river_state;
  }
}

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "completed" ? "default" :
    status === "running" ? "secondary" :
    status === "failed" ? "destructive" : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export default function DomainDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [domain, setDomain] = useState<DomainDetail | null>(null);
  const [companies, setCompanies] = useState<CompanyDomainRow[]>([]);
  const [crawlJobs, setCrawlJobs] = useState<DomainCrawlJob[]>([]);
  const [loading, setLoading] = useState(true);

  // Sheet state: selected job pages
  const [pagesJob, setPagesJob] = useState<DomainCrawlJob | null>(null);
  const [pages, setPages] = useState<DomainCrawlPage[]>([]);
  const [pagesLoading, setPagesLoading] = useState(false);

  // Content viewer state
  const [contentPage, setContentPage] = useState<{ page: DomainCrawlPage; type: "markdown" | "html" | "headers" } | null>(null);
  const [content, setContent] = useState<string>("");
  const [contentLoading, setContentLoading] = useState(false);

  const [crawlDialogOpen, setCrawlDialogOpen] = useState(false);

  useEffect(() => {
    if (!id) return;
    Promise.all([
      getDomain(id),
      pgrest<CompanyDomainRow>("v_company_domains", { domain_id: `eq.${id}` }),
      getDomainCrawlJobs(id),
    ])
      .then(([d, c, j]) => {
        setDomain(d);
        setCompanies(c.data);
        setCrawlJobs(j);
      })
      .finally(() => setLoading(false));
  }, [id]);

  async function openPages(job: DomainCrawlJob) {
    setPagesJob(job);
    setPagesLoading(true);
    try {
      const p = await getDomainCrawlPages(id!, job.id);
      setPages(p);
    } finally {
      setPagesLoading(false);
    }
  }

  async function openContent(page: DomainCrawlPage, type: "markdown" | "html" | "headers") {
    setContentPage({ page, type });
    setContentLoading(true);
    try {
      const text = await getDomainCrawlPageContent(id!, pagesJob!.id, page.page_num, type);
      setContent(typeof text === "string" ? text : JSON.stringify(text, null, 2));
    } catch {
      setContent("Content unavailable");
    } finally {
      setContentLoading(false);
    }
  }

  function refreshJobs() {
    if (!id) return;
    getDomainCrawlJobs(id).then(setCrawlJobs);
  }

  // Find the latest job with a favicon for display
  const faviconJob = crawlJobs.find((j) => j.favicon_s3_key);

  if (loading) {
    return (
      <div className="p-8">
        <div className="text-muted-foreground">Loading…</div>
      </div>
    );
  }

  if (!domain) {
    return (
      <div className="p-8">
        <div className="text-destructive">Domain not found.</div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">
      {/* Back link */}
      <Link to="/domains" className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="h-4 w-4" /> Domains
      </Link>

      {/* Identity card */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-4">
              {faviconJob && (
                <img
                  src={getDomainFaviconUrl(id!, faviconJob.id)}
                  alt="favicon"
                  className="h-8 w-8 rounded object-contain"
                  onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
                />
              )}
              <div>
                <h1 className="text-2xl font-bold flex items-center gap-2">
                  {domain.domain}
                  <a
                    href={`https://${domain.domain}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-muted-foreground hover:text-foreground"
                  >
                    <ExternalLink className="h-4 w-4" />
                  </a>
                </h1>
                <div className="text-sm text-muted-foreground mt-1">
                  First seen: {new Date(domain.first_seen_at).toLocaleDateString()}
                  {" · "}
                  Companies: {companies.length}
                </div>
              </div>
            </div>
            <Button onClick={() => setCrawlDialogOpen(true)}>Trigger Crawl</Button>
          </div>
        </CardContent>
      </Card>

      {/* Linked companies */}
      <Card>
        <CardHeader>
          <CardTitle>Linked Companies</CardTitle>
        </CardHeader>
        <CardContent>
          {companies.length === 0 ? (
            <div className="text-sm text-muted-foreground">No linked companies.</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Company</TableHead>
                  <TableHead>Signal</TableHead>
                  <TableHead>Confidence</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {companies.map((c) => (
                  <TableRow key={c.company_id}>
                    <TableCell>
                      <Link to={`/companies/${c.company_id}`} className="hover:underline font-medium">
                        {c.company_name}
                      </Link>
                    </TableCell>
                    <TableCell>{c.signal}</TableCell>
                    <TableCell>{c.confidence}</TableCell>
                    <TableCell>{c.status}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Crawl history */}
      <Card>
        <CardHeader>
          <CardTitle>Crawl History</CardTitle>
        </CardHeader>
        <CardContent>
          {crawlJobs.length === 0 ? (
            <div className="text-sm text-muted-foreground">No crawl jobs yet.</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Date</TableHead>
                  <TableHead>Mode</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {crawlJobs.map((job) => (
                  <TableRow key={job.id}>
                    <TableCell className="text-sm">
                      {new Date(job.created_at).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{job.mode}</Badge>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={crawlJobStatus(job)} />
                    </TableCell>
                    <TableCell>
                      {crawlJobStatus(job) === "completed" && (
                        <Button variant="outline" size="sm" onClick={() => openPages(job)}>
                          View pages
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Pages sheet */}
      <Sheet open={!!pagesJob} onOpenChange={(open) => { if (!open) { setPagesJob(null); setPages([]); } }}>
        <SheetContent side="right" className="w-[600px] sm:max-w-[600px] overflow-y-auto">
          <SheetHeader>
            <SheetTitle>Pages — {pagesJob?.mode} crawl</SheetTitle>
          </SheetHeader>
          <div className="mt-4">
            {pagesLoading ? (
              <div className="text-muted-foreground text-sm">Loading pages…</div>
            ) : pages.length === 0 ? (
              <div className="text-muted-foreground text-sm">No pages recorded.</div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>#</TableHead>
                    <TableHead>URL</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Content</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pages.map((p) => (
                    <TableRow key={p.id}>
                      <TableCell className="text-muted-foreground">{p.page_num}</TableCell>
                      <TableCell className="max-w-[200px] truncate text-sm" title={p.url}>
                        {p.title || p.url}
                      </TableCell>
                      <TableCell>{p.status_code}</TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <Button variant="outline" size="sm" onClick={() => openContent(p, "markdown")}>MD</Button>
                          <Button variant="outline" size="sm" onClick={() => openContent(p, "html")}>HTML</Button>
                          <Button variant="outline" size="sm" onClick={() => openContent(p, "headers")}>Headers</Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </SheetContent>
      </Sheet>

      {/* Content viewer sheet */}
      <Sheet open={!!contentPage} onOpenChange={(open) => { if (!open) setContentPage(null); }}>
        <SheetContent side="right" className="w-[700px] sm:max-w-[700px] overflow-y-auto">
          <SheetHeader>
            <SheetTitle>
              {contentPage?.type.toUpperCase()} — Page {contentPage?.page.page_num}
            </SheetTitle>
          </SheetHeader>
          <div className="mt-4">
            {contentLoading ? (
              <div className="text-muted-foreground text-sm">Loading…</div>
            ) : (
              <div className="relative">
                <Button
                  variant="outline"
                  size="sm"
                  className="absolute top-2 right-2 z-10"
                  onClick={() => navigator.clipboard.writeText(content)}
                >
                  Copy
                </Button>
                <pre className="text-xs bg-muted rounded p-4 overflow-x-auto max-h-[70vh] whitespace-pre-wrap break-words">
                  {content}
                </pre>
              </div>
            )}
          </div>
        </SheetContent>
      </Sheet>

      {/* Trigger crawl dialog */}
      <CrawlDomainDialog
        domainId={id!}
        domainName={domain.domain}
        open={crawlDialogOpen}
        onOpenChange={setCrawlDialogOpen}
        onSuccess={refreshJobs}
      />
    </div>
  );
}
```

**Note:** The `pgrest<T>` helper and `CompanyDomainRow` type must match the actual PostgREST view. Read `ui/app/lib/pgrest.ts` (or similar) to confirm the import path. Read the `v_company_domains` view definition to confirm field names (`company_name`, `signal`, `confidence`, `status`). Adjust `CompanyDomainRow` to match actual column names.

- [ ] **Step 4: Type-check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add ui/app/routes/domains_.$id.tsx
git commit -m "feat: add domain detail page with crawl history"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Add rustfs to docker-compose with bind mount | Task 1 |
| Add `data/` to .gitignore | Task 1 |
| S3 config fields | Task 1 |
| domain_crawl_jobs table | Task 2 |
| domain_crawl_job_pages table | Task 2 |
| sqlc queries (7 queries + GetDomainByID) | Task 2 |
| s3client (Upload, Download, EnsureBucket) | Task 3 |
| EnsureBucket on startup | Task 6 (app.go) |
| aws-sdk-go-v2 dependency | Task 3 |
| crawlerclient CrawlDomain method | Task 4 |
| crawl4ai in requirements.txt | Task 5 |
| POST /crawl-domain endpoint in Python | Task 5 |
| homepage mode (1 page) | Task 5 |
| deep mode (BFSDeepCrawlStrategy) | Task 5 |
| favicon discovery + base64 | Task 5 |
| DomainCrawlWorker River worker | Task 6 |
| S3 upload per page (md, html, headers) | Task 6 |
| S3 upload favicon | Task 6 |
| river_job_id linked back | Task 6 |
| DomainCrawlWorker registered in river.go | Task 6 |
| s3client wired through app.go | Task 6 |
| POST /api/v1/domains/:id/crawl | Task 7 |
| GET /api/v1/domains/:id/crawl-jobs | Task 7 |
| GET .../crawl-jobs/:job_id/pages | Task 7 |
| GET .../pages/:n/markdown|html|headers | Task 7 |
| GET .../favicon | Task 7 |
| GET /api/v1/domains/:id | Task 7 |
| testhelpers stubs | Task 7 |
| Frontend types (DomainCrawlJob, DomainCrawlPage) | Task 8 |
| API helper functions | Task 8 |
| CrawlDomainDialog (mode, max_pages) | Task 9 |
| domain name → /domains/:id link | Task 10 |
| ··· action menu with Crawl domain | Task 10 |
| Domain detail page identity card | Task 11 |
| Domain detail linked companies table | Task 11 |
| Domain detail crawl history table | Task 11 |
| Per-page content viewer (nested sheet) | Task 11 |
| Favicon displayed in identity card | Task 11 |
| Status derived from river_job.state | Tasks 2, 11 |
| Error handling (toast.error on failure) | Tasks 9, 11 |
| "Content unavailable" on missing S3 | Tasks 7, 11 |

All spec requirements covered.
