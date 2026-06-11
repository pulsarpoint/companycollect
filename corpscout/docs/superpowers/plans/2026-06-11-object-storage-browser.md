# Object Storage Browser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only settings page that lists all RustFS/S3 buckets visible to CorpScout credentials and browses folder-like object metadata inside a selected bucket.

**Architecture:** Extend the existing concrete scheduler S3 client with read-only bucket/object listing methods, expose them through scheduler HTTP API endpoints, then build a small React settings component that stores selected bucket/prefix in URL query parameters. The browser never receives S3 credentials and does not read object bodies or expose mutating actions.

**Tech Stack:** Go 1.26, AWS SDK for Go v2 S3 client, chi HTTP routes, React Router 7, TypeScript, shadcn UI components, lucide-react icons.

---

## Scope Check

The approved spec covers one cohesive feature: a read-only object-storage browser. It spans backend API and frontend UI, but each task below produces a testable slice and commits independently.

Before starting implementation, check the worktree:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short --branch
```

There may be unrelated deletions under `companies/companysource`. Do not stage, restore, or modify those files unless the user explicitly asks.

## File Structure

Backend files:

- Modify `corpscout/scheduler/internal/s3client/client.go`
  - Adds exported read-only storage metadata types.
  - Adds `ListBuckets` and `ListObjects` methods on the existing concrete `*Client`.
- Create `corpscout/scheduler/internal/s3client/list_test.go`
  - Tests S3 ListBuckets/ListObjectsV2 mapping against an `httptest` S3-compatible endpoint.
- Create `corpscout/scheduler/internal/httpapi/object_storage.go`
  - Adds read-only HTTP handlers for buckets and object listings.
- Create `corpscout/scheduler/internal/httpapi/object_storage_test.go`
  - Tests HTTP response shape, nil-client behavior, bucket route handling, and query parsing through the real router.
- Modify `corpscout/scheduler/internal/httpapi/handlers.go`
  - Registers the new `/api/v1/object-storage/*` routes.

Frontend files:

- Modify `corpscout/ui/app/types/api.ts`
  - Adds object-storage response types.
- Modify `corpscout/ui/app/lib/api.ts`
  - Adds object-storage API client methods.
- Create `corpscout/ui/app/components/app/ObjectStorageBrowser.tsx`
  - Implements the read-only reusable browser component.
- Create `corpscout/ui/app/routes/settings.object-storage.tsx`
  - Mounts the component at `/settings/object-storage`.
- Modify `corpscout/ui/app/components/app/AppSidebar.tsx`
  - Adds the Settings sidebar link.

---

### Task 1: Add Read-Only S3 Listing Methods

**Files:**
- Modify: `corpscout/scheduler/internal/s3client/client.go`
- Create: `corpscout/scheduler/internal/s3client/list_test.go`

- [ ] **Step 1: Write failing S3 client tests**

Create `corpscout/scheduler/internal/s3client/list_test.go`:

```go
package s3client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListBucketsReturnsVisibleBuckets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/", r.URL.Path)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>test-owner</ID><DisplayName>test-owner</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>crawls</Name><CreationDate>2026-06-11T05:00:00.000Z</CreationDate></Bucket>
    <Bucket><Name>source-runs</Name><CreationDate>2026-06-11T06:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	buckets, err := client.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Len(t, buckets, 2)
	require.Equal(t, "crawls", buckets[0].Name)
	require.Equal(t, "2026-06-11T05:00:00Z", buckets[0].CreationDate.Format("2006-01-02T15:04:05Z"))
	require.Equal(t, "source-runs", buckets[1].Name)
}

func TestListObjectsReturnsFoldersObjectsAndCursor(t *testing.T) {
	var seen url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/crawls/", r.URL.Path)
		seen = r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>crawls</Name>
  <Prefix>brreg/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>2</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>next-page</NextContinuationToken>
  <CommonPrefixes><Prefix>brreg/search/</Prefix></CommonPrefixes>
  <Contents>
    <Key>brreg/index.ndjson</Key>
    <LastModified>2026-06-11T05:10:00.000Z</LastModified>
    <ETag>&quot;etag-1&quot;</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	result, err := client.ListObjects(context.Background(), ListObjectsInput{
		Bucket:    "crawls",
		Prefix:    "brreg/",
		Delimiter: "/",
		Cursor:    "cursor-1",
		Limit:     2,
	})
	require.NoError(t, err)

	require.Equal(t, "2", seen.Get("max-keys"))
	require.Equal(t, "brreg/", seen.Get("prefix"))
	require.Equal(t, "/", seen.Get("delimiter"))
	require.Equal(t, "cursor-1", seen.Get("continuation-token"))
	require.Equal(t, "list-type=2", "list-type="+seen.Get("list-type"))

	require.Equal(t, "crawls", result.Bucket)
	require.Equal(t, "brreg/", result.Prefix)
	require.Equal(t, "/", result.Delimiter)
	require.Equal(t, "next-page", result.NextCursor)
	require.Equal(t, []Folder{{Prefix: "brreg/search/", Name: "search"}}, result.Folders)
	require.Len(t, result.Objects, 1)
	require.Equal(t, "brreg/index.ndjson", result.Objects[0].Key)
	require.Equal(t, "index.ndjson", result.Objects[0].Name)
	require.Equal(t, int64(42), result.Objects[0].SizeBytes)
	require.Equal(t, `"etag-1"`, result.Objects[0].ETag)
	require.Equal(t, "STANDARD", result.Objects[0].StorageClass)

	limit, err := strconv.Atoi(seen.Get("max-keys"))
	require.NoError(t, err)
	require.Equal(t, 2, limit)
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/s3client -run 'TestList(Buckets|Objects)' -count=1
```

Expected: build fails because `ListBuckets`, `ListObjects`, `ListObjectsInput`, `Folder`, and object metadata types are not defined.

- [ ] **Step 3: Implement S3 metadata types and listing methods**

Modify `corpscout/scheduler/internal/s3client/client.go`.

Add imports:

```go
import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
)
```

Add types below `Client`:

```go
type Bucket struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creation_date"`
}

type Folder struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
}

type Object struct {
	Key          string    `json:"key"`
	Name         string    `json:"name"`
	SizeBytes    int64     `json:"size_bytes"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
}

type ListObjectsInput struct {
	Bucket    string
	Prefix    string
	Delimiter string
	Cursor    string
	Limit     int32
}

type ListObjectsResult struct {
	Bucket     string   `json:"bucket"`
	Prefix     string   `json:"prefix"`
	Delimiter  string   `json:"delimiter"`
	NextCursor string   `json:"next_cursor"`
	Folders    []Folder `json:"folders"`
	Objects    []Object `json:"objects"`
}
```

Add methods:

```go
func (c *Client) ListBuckets(ctx context.Context) ([]Bucket, error) {
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, errors.Wrap(err, "s3 list buckets")
	}
	buckets := make([]Bucket, 0, len(out.Buckets))
	for _, bucket := range out.Buckets {
		buckets = append(buckets, Bucket{
			Name:         aws.ToString(bucket.Name),
			CreationDate: aws.ToTime(bucket.CreationDate),
		})
	}
	return buckets, nil
}

func (c *Client) ListObjects(ctx context.Context, input ListObjectsInput) (ListObjectsResult, error) {
	bucket := strings.TrimSpace(input.Bucket)
	if bucket == "" {
		return ListObjectsResult{}, errors.New("s3 list objects bucket is required")
	}
	delimiter := input.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String(input.Prefix),
		Delimiter:         aws.String(delimiter),
		ContinuationToken: stringPointer(input.Cursor),
		MaxKeys:           aws.Int32(limit),
	})
	if err != nil {
		return ListObjectsResult{}, errors.Wrap(err, "s3 list objects "+bucket)
	}

	result := ListObjectsResult{
		Bucket:     bucket,
		Prefix:     input.Prefix,
		Delimiter:  delimiter,
		NextCursor: aws.ToString(out.NextContinuationToken),
		Folders:    make([]Folder, 0, len(out.CommonPrefixes)),
		Objects:    make([]Object, 0, len(out.Contents)),
	}
	for _, commonPrefix := range out.CommonPrefixes {
		prefix := aws.ToString(commonPrefix.Prefix)
		result.Folders = append(result.Folders, Folder{
			Prefix: prefix,
			Name:   prefixDisplayName(input.Prefix, prefix),
		})
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if key == input.Prefix {
			continue
		}
		result.Objects = append(result.Objects, Object{
			Key:          key,
			Name:         objectDisplayName(input.Prefix, key),
			SizeBytes:    aws.ToInt64(object.Size),
			LastModified: aws.ToTime(object.LastModified),
			ETag:         aws.ToString(object.ETag),
			StorageClass: string(object.StorageClass),
		})
	}
	return result, nil
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func prefixDisplayName(parentPrefix, prefix string) string {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(prefix, parentPrefix), "/")
	if trimmed == "" {
		return prefix
	}
	return path.Base(trimmed)
}

func objectDisplayName(parentPrefix, key string) string {
	trimmed := strings.TrimPrefix(key, parentPrefix)
	if trimmed == "" {
		return key
	}
	return path.Base(trimmed)
}
```

- [ ] **Step 4: Format and run S3 client tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/s3client/client.go internal/s3client/list_test.go
GOWORK=off go test ./internal/s3client -run 'TestList(Buckets|Objects)' -count=1
```

Expected: tests pass.

- [ ] **Step 5: Commit S3 client slice**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/s3client/client.go corpscout/scheduler/internal/s3client/list_test.go
git commit -m "feat: list s3 buckets and objects"
```

---

### Task 2: Add Object Storage HTTP API

**Files:**
- Create: `corpscout/scheduler/internal/httpapi/object_storage.go`
- Create: `corpscout/scheduler/internal/httpapi/object_storage_test.go`
- Modify: `corpscout/scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Write failing HTTP handler tests**

Create `corpscout/scheduler/internal/httpapi/object_storage_test.go`:

```go
package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

func TestListObjectStorageBucketsReturnsBuckets(t *testing.T) {
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/", r.URL.Path)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Buckets>
    <Bucket><Name>crawls</Name><CreationDate>2026-06-11T05:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`))
	}))
	t.Cleanup(s3Server.Close)

	client, err := s3client.New(s3Server.URL, "access", "secret", "crawls")
	require.NoError(t, err)
	router := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, client, "", nil, ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"items":[{"name":"crawls","creation_date":"2026-06-11T05:00:00Z"}]}`, rec.Body.String())
}

func TestListObjectStorageObjectsParsesQueryAndCapsLimit(t *testing.T) {
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/crawls/", r.URL.Path)
		require.Equal(t, "2", r.URL.Query().Get("list-type"))
		require.Equal(t, "brreg/", r.URL.Query().Get("prefix"))
		require.Equal(t, "/", r.URL.Query().Get("delimiter"))
		require.Equal(t, "1000", r.URL.Query().Get("max-keys"))
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>crawls</Name>
  <Prefix>brreg/</Prefix>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes><Prefix>brreg/search/</Prefix></CommonPrefixes>
  <Contents>
    <Key>brreg/index.ndjson</Key>
    <LastModified>2026-06-11T05:10:00.000Z</LastModified>
    <ETag>&quot;etag-1&quot;</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>`))
	}))
	t.Cleanup(s3Server.Close)

	client, err := s3client.New(s3Server.URL, "access", "secret", "crawls")
	require.NoError(t, err)
	router := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, client, "", nil, ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets/crawls/objects?prefix=brreg/&limit=5000", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
	  "bucket": "crawls",
	  "prefix": "brreg/",
	  "delimiter": "/",
	  "next_cursor": "",
	  "folders": [{"prefix": "brreg/search/", "name": "search"}],
	  "objects": [{
	    "key": "brreg/index.ndjson",
	    "name": "index.ndjson",
	    "size_bytes": 42,
	    "last_modified": "2026-06-11T05:10:00Z",
	    "etag": "\"etag-1\"",
	    "storage_class": "STANDARD"
	  }]
	}`, rec.Body.String())
}

func TestObjectStorageEndpointsRequireS3Client(t *testing.T) {
	router := routerFor(newTestHandlers(&stubQuerier{}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"object storage client not configured"}`, rec.Body.String())
}
```

- [ ] **Step 2: Register routes and verify tests fail before handler exists**

Modify `corpscout/scheduler/internal/httpapi/handlers.go` inside `r.Route("/api/v1", ...)`:

```go
r.Get("/object-storage/buckets", h.handleListObjectStorageBuckets)
r.Get("/object-storage/buckets/{bucket}/objects", h.handleListObjectStorageObjects)
```

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'Test(ListObjectStorage|ObjectStorage)' -count=1
```

Expected: build fails because `handleListObjectStorageBuckets` and `handleListObjectStorageObjects` are not defined.

- [ ] **Step 3: Implement handlers**

Create `corpscout/scheduler/internal/httpapi/object_storage.go`:

```go
package httpapi

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

const (
	defaultObjectStorageListLimit = 100
	maxObjectStorageListLimit     = 1000
)

func (h *Handlers) handleListObjectStorageBuckets(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "object storage client not configured")
		return
	}
	buckets, err := h.s3.ListBuckets(r.Context())
	if err != nil {
		slog.ErrorContext(r.Context(), "list object storage buckets", "error", err)
		writeError(w, http.StatusInternalServerError, "list buckets failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": buckets})
}

func (h *Handlers) handleListObjectStorageObjects(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "object storage client not configured")
		return
	}
	bucket := strings.TrimSpace(chi.URLParam(r, "bucket"))
	if bucket == "" {
		writeError(w, http.StatusBadRequest, "bucket is required")
		return
	}
	limit := queryInt(r, "limit", defaultObjectStorageListLimit)
	if limit > maxObjectStorageListLimit {
		limit = maxObjectStorageListLimit
	}
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}
	result, err := h.s3.ListObjects(r.Context(), s3client.ListObjectsInput{
		Bucket:    bucket,
		Prefix:    r.URL.Query().Get("prefix"),
		Delimiter: delimiter,
		Cursor:    r.URL.Query().Get("cursor"),
		Limit:     int32(limit),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list object storage objects", "bucket", bucket, "prefix", r.URL.Query().Get("prefix"), "error", err)
		writeError(w, http.StatusInternalServerError, "list objects failed")
		return
	}
	writeJSON(w, http.StatusOK, result)
}
```

- [ ] **Step 4: Format and run HTTP API tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/httpapi/handlers.go internal/httpapi/object_storage.go internal/httpapi/object_storage_test.go
GOWORK=off go test ./internal/httpapi -run 'Test(ListObjectStorage|ObjectStorage)' -count=1
```

Expected: tests pass.

- [ ] **Step 5: Run focused backend packages**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/s3client ./internal/httpapi -count=1
```

Expected: tests pass.

- [ ] **Step 6: Commit HTTP API slice**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/httpapi/handlers.go corpscout/scheduler/internal/httpapi/object_storage.go corpscout/scheduler/internal/httpapi/object_storage_test.go
git commit -m "feat: expose object storage browser api"
```

---

### Task 3: Add Frontend API Types And Client Methods

**Files:**
- Modify: `corpscout/ui/app/types/api.ts`
- Modify: `corpscout/ui/app/lib/api.ts`

- [ ] **Step 1: Add TypeScript response types**

Modify `corpscout/ui/app/types/api.ts` after `StartWorkflowResponse`:

```ts
export interface ObjectStorageBucket {
  name: string;
  creation_date: string;
}

export interface ObjectStorageBucketListResponse {
  items: ObjectStorageBucket[];
}

export interface ObjectStorageFolder {
  prefix: string;
  name: string;
}

export interface ObjectStorageObject {
  key: string;
  name: string;
  size_bytes: number;
  last_modified: string;
  etag: string;
  storage_class: string;
}

export interface ObjectStorageObjectListResponse {
  bucket: string;
  prefix: string;
  delimiter: string;
  next_cursor: string;
  folders: ObjectStorageFolder[];
  objects: ObjectStorageObject[];
}
```

- [ ] **Step 2: Add imports and API client methods**

Modify the type import list in `corpscout/ui/app/lib/api.ts` to include:

```ts
  ObjectStorageBucketListResponse,
  ObjectStorageObjectListResponse,
```

Add methods near the existing settings/reference methods:

```ts
  getObjectStorageBuckets: () =>
    get<ObjectStorageBucketListResponse>("/object-storage/buckets"),

  getObjectStorageObjects: (
    bucket: string,
    params: {
      prefix?: string;
      delimiter?: string;
      cursor?: string;
      limit?: number;
    } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.prefix) qs.set("prefix", params.prefix);
    if (params.delimiter) qs.set("delimiter", params.delimiter);
    if (params.cursor) qs.set("cursor", params.cursor);
    if (params.limit != null) qs.set("limit", String(params.limit));
    const query = qs.toString();
    return get<ObjectStorageObjectListResponse>(
      `/object-storage/buckets/${encodeURIComponent(bucket)}/objects${query ? `?${query}` : ""}`,
    );
  },
```

- [ ] **Step 3: Typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: typecheck passes.

- [ ] **Step 4: Commit frontend API slice**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/ui/app/types/api.ts corpscout/ui/app/lib/api.ts
git commit -m "feat: add object storage api client"
```

---

### Task 4: Build The ObjectStorageBrowser Component

**Files:**
- Create: `corpscout/ui/app/components/app/ObjectStorageBrowser.tsx`

- [ ] **Step 1: Create the component with read-only state and helpers**

Create `corpscout/ui/app/components/app/ObjectStorageBrowser.tsx`:

```tsx
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import { ChevronRight, Folder, RefreshCw, Search, Server } from "lucide-react";

import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { api, errorMessage } from "~/lib/api";
import type {
  ObjectStorageBucket,
  ObjectStorageObjectListResponse,
} from "~/types/api";
import { cn, formatDate } from "~/lib/utils";

const objectListLimit = 100;

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "-";
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    Math.floor(Math.log(value) / Math.log(1024)),
    units.length - 1,
  );
  const amount = value / 1024 ** exponent;
  return `${amount.toFixed(amount >= 10 || exponent === 0 ? 0 : 1)} ${units[exponent]}`;
}

function prefixCrumbs(prefix: string): Array<{ label: string; prefix: string }> {
  const parts = prefix.split("/").filter(Boolean);
  let current = "";
  return parts.map((part) => {
    current += `${part}/`;
    return { label: part, prefix: current };
  });
}

function normalizePrefix(value: string): string {
  const trimmed = value.trim().replace(/^\/+/, "");
  if (!trimmed) return "";
  return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

export function ObjectStorageBrowser() {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedBucket = searchParams.get("bucket") ?? "";
  const selectedPrefix = searchParams.get("prefix") ?? "";
  const [prefixInput, setPrefixInput] = useState(selectedPrefix);
  const [buckets, setBuckets] = useState<ObjectStorageBucket[]>([]);
  const [bucketError, setBucketError] = useState<string | null>(null);
  const [objectsError, setObjectsError] = useState<string | null>(null);
  const [loadingBuckets, setLoadingBuckets] = useState(false);
  const [loadingObjects, setLoadingObjects] = useState(false);
  const [listing, setListing] = useState<ObjectStorageObjectListResponse | null>(
    null,
  );

  const crumbs = useMemo(() => prefixCrumbs(selectedPrefix), [selectedPrefix]);

  function setLocation(bucket: string, prefix: string) {
    const next = new URLSearchParams();
    if (bucket) next.set("bucket", bucket);
    if (prefix) next.set("prefix", prefix);
    setSearchParams(next);
  }

  async function loadBuckets() {
    setLoadingBuckets(true);
    setBucketError(null);
    try {
      const response = await api.getObjectStorageBuckets();
      setBuckets(response.items);
    } catch (err) {
      setBucketError(errorMessage(err, "Failed to load object storage buckets."));
    } finally {
      setLoadingBuckets(false);
    }
  }

  async function loadObjects(cursor = "") {
    if (!selectedBucket) {
      setListing(null);
      return;
    }
    setLoadingObjects(true);
    setObjectsError(null);
    try {
      const response = await api.getObjectStorageObjects(selectedBucket, {
        prefix: selectedPrefix,
        delimiter: "/",
        cursor,
        limit: objectListLimit,
      });
      setListing(response);
    } catch (err) {
      setObjectsError(errorMessage(err, "Failed to load bucket contents."));
    } finally {
      setLoadingObjects(false);
    }
  }

  useEffect(() => {
    void loadBuckets();
  }, []);

  useEffect(() => {
    setPrefixInput(selectedPrefix);
    void loadObjects();
  }, [selectedBucket, selectedPrefix]);

  function submitPrefix() {
    if (!selectedBucket) return;
    setLocation(selectedBucket, normalizePrefix(prefixInput));
  }

  function nextPage() {
    if (!listing?.next_cursor) return;
    void loadObjects(listing.next_cursor);
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">Object Storage</h1>
          <p className="text-sm text-muted-foreground">
            Browse RustFS/S3 buckets and object metadata without reading object bodies.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => {
            void loadBuckets();
            void loadObjects();
          }}
          disabled={loadingBuckets || loadingObjects}
        >
          <RefreshCw className="size-4" />
          Refresh
        </Button>
      </div>

      {bucketError ? (
        <Alert variant="destructive">
          <AlertDescription>{bucketError}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-4 lg:grid-cols-[18rem_1fr]">
        <section className="rounded-md border">
          <div className="border-b p-3">
            <h2 className="text-sm font-medium">Buckets</h2>
            <p className="text-xs text-muted-foreground">
              {loadingBuckets
                ? "Loading buckets..."
                : `${buckets.length.toLocaleString()} visible`}
            </p>
          </div>
          <div className="flex flex-col">
            {buckets.map((bucket) => (
              <button
                key={bucket.name}
                type="button"
                onClick={() => setLocation(bucket.name, "")}
                className={cn(
                  "flex items-center gap-2 border-b px-3 py-2 text-left text-sm hover:bg-muted",
                  selectedBucket === bucket.name && "bg-muted",
                )}
              >
                <Server className="size-4 text-muted-foreground" />
                <span className="min-w-0 flex-1 truncate">{bucket.name}</span>
              </button>
            ))}
            {!loadingBuckets && buckets.length === 0 ? (
              <div className="p-3 text-sm text-muted-foreground">
                No buckets visible to the configured credentials.
              </div>
            ) : null}
          </div>
        </section>

        <section className="rounded-md border">
          <div className="flex flex-col gap-3 border-b p-3">
            <div className="flex flex-wrap items-center gap-2 text-sm">
              {selectedBucket ? (
                <>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setLocation(selectedBucket, "")}
                  >
                    {selectedBucket}
                  </Button>
                  {crumbs.map((crumb) => (
                    <span key={crumb.prefix} className="flex items-center gap-2">
                      <ChevronRight className="size-4 text-muted-foreground" />
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setLocation(selectedBucket, crumb.prefix)}
                      >
                        {crumb.label}
                      </Button>
                    </span>
                  ))}
                </>
              ) : (
                <span className="text-muted-foreground">Select a bucket</span>
              )}
            </div>
            <div className="flex flex-col gap-2 sm:flex-row">
              <div className="relative flex-1">
                <Search className="absolute left-2 top-2.5 size-4 text-muted-foreground" />
                <Input
                  value={prefixInput}
                  onChange={(event) => setPrefixInput(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") submitPrefix();
                  }}
                  placeholder="Prefix, for example brreg/search/"
                  className="pl-8"
                  disabled={!selectedBucket}
                />
              </div>
              <Button onClick={submitPrefix} disabled={!selectedBucket}>
                Open prefix
              </Button>
              <Button
                variant="outline"
                onClick={() => selectedBucket && setLocation(selectedBucket, "")}
                disabled={!selectedBucket || selectedPrefix === ""}
              >
                Clear prefix
              </Button>
            </div>
          </div>

          {objectsError ? (
            <Alert variant="destructive" className="m-3">
              <AlertDescription>{objectsError}</AlertDescription>
            </Alert>
          ) : null}

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Key / prefix</TableHead>
                <TableHead>Size</TableHead>
                <TableHead>Modified</TableHead>
                <TableHead>ETag</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {listing?.folders.map((folder) => (
                <TableRow
                  key={`folder:${folder.prefix}`}
                  className="cursor-pointer"
                  onClick={() => setLocation(selectedBucket, folder.prefix)}
                >
                  <TableCell>
                    <div className="flex items-center gap-2 font-medium">
                      <Folder className="size-4 text-muted-foreground" />
                      {folder.name}
                    </div>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{folder.prefix}</TableCell>
                  <TableCell>
                    <Badge variant="outline">Folder</Badge>
                  </TableCell>
                  <TableCell>-</TableCell>
                  <TableCell>-</TableCell>
                </TableRow>
              ))}
              {listing?.objects.map((object) => (
                <TableRow key={`object:${object.key}`}>
                  <TableCell className="font-medium">{object.name}</TableCell>
                  <TableCell className="max-w-[32rem] truncate font-mono text-xs">
                    {object.key}
                  </TableCell>
                  <TableCell>{formatBytes(object.size_bytes)}</TableCell>
                  <TableCell>{formatDate(object.last_modified)}</TableCell>
                  <TableCell className="max-w-48 truncate font-mono text-xs">
                    {object.etag || "-"}
                  </TableCell>
                </TableRow>
              ))}
              {!selectedBucket ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                    Select a bucket to browse object metadata.
                  </TableCell>
                </TableRow>
              ) : !loadingObjects &&
                listing &&
                listing.folders.length === 0 &&
                listing.objects.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                    This prefix is empty.
                  </TableCell>
                </TableRow>
              ) : loadingObjects ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                    Loading objects...
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>

          {listing?.next_cursor ? (
            <div className="flex justify-end border-t p-3">
              <Button variant="outline" onClick={nextPage} disabled={loadingObjects}>
                Next page
              </Button>
            </div>
          ) : null}
        </section>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Run typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: typecheck passes.

- [ ] **Step 3: Commit component slice**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/ui/app/components/app/ObjectStorageBrowser.tsx
git commit -m "feat: add object storage browser component"
```

---

### Task 5: Add Settings Route And Sidebar Entry

**Files:**
- Create: `corpscout/ui/app/routes/settings.object-storage.tsx`
- Modify: `corpscout/ui/app/components/app/AppSidebar.tsx`

- [ ] **Step 1: Add route file**

Create `corpscout/ui/app/routes/settings.object-storage.tsx`:

```tsx
import { ObjectStorageBrowser } from "~/components/app/ObjectStorageBrowser";

export default function ObjectStorageSettingsRoute() {
  return <ObjectStorageBrowser />;
}
```

- [ ] **Step 2: Add sidebar item**

Modify `corpscout/ui/app/components/app/AppSidebar.tsx`.

Add icon import:

```tsx
  HardDrive,
```

Add item in `SETTINGS_NAV_ITEMS`, after `FX Rates` and before `Schedules`:

```tsx
  { title: "Object Storage", url: "/settings/object-storage", icon: HardDrive },
```

- [ ] **Step 3: Run frontend checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
npm run build
```

Expected: both commands pass. Existing Vite sourcemap warnings from shadcn component sources may appear and are acceptable if the command exits with code 0.

- [ ] **Step 4: Commit route/sidebar slice**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/ui/app/routes/settings.object-storage.tsx corpscout/ui/app/components/app/AppSidebar.tsx
git commit -m "feat: add object storage settings page"
```

---

### Task 6: Full Verification

**Files:**
- No source edits expected unless verification exposes defects.

- [ ] **Step 1: Run backend focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/s3client ./internal/httpapi -count=1
```

Expected: tests pass.

- [ ] **Step 2: Run frontend checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
npm run build
```

Expected: typecheck and build pass.

- [ ] **Step 3: Run whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
```

Expected: no output.

- [ ] **Step 4: Rebuild local services for manual UI verification**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d --build scheduler ui
```

Expected: scheduler and UI containers start. If scheduler cannot reach RustFS, verify `deploy/services` RustFS is running on the companycollect host and that `CORPSCOUT_S3_ACCESS_KEY` and `CORPSCOUT_S3_SECRET_KEY` match the RustFS credentials.

- [ ] **Step 5: Verify API manually**

Run:

```bash
curl -s http://localhost:8092/api/v1/object-storage/buckets | jq .
```

Expected shape:

```json
{
  "items": [
    {
      "name": "crawls",
      "creation_date": "2026-06-11T05:00:00Z"
    }
  ]
}
```

Use an actual bucket name from the bucket list:

```bash
curl -s 'http://localhost:8092/api/v1/object-storage/buckets/crawls/objects?limit=10' | jq .
```

Expected shape:

```json
{
  "bucket": "crawls",
  "prefix": "",
  "delimiter": "/",
  "next_cursor": "",
  "folders": [],
  "objects": []
}
```

The arrays may contain real folders and objects.

- [ ] **Step 6: Verify browser UI**

Open:

```text
http://localhost:8094/settings/object-storage
```

Expected:

- Settings sidebar contains `Object Storage`.
- The page heading is `Object Storage`.
- Buckets list renders.
- Clicking a bucket updates the URL with `bucket=<name>`.
- Clicking a folder updates the URL with `prefix=<prefix>`.
- Breadcrumb buttons navigate back to parent prefixes.
- Object rows show metadata only.
- No upload, delete, rename, download, or preview controls are present.

If verification exposes a defect, return to the task that introduced the defect,
fix that task's files, rerun that task's test command, and create a focused fix
commit with only the object-storage browser files staged.

---

## Final Notes For Executor

- Keep the feature read-only.
- Do not add upload, delete, rename, download, object preview, presigned URLs, or bucket management.
- Keep S3 credentials server-side.
- Use `log/slog` only at HTTP boundary errors.
- Use `github.com/cockroachdb/errors` for lower-level wrapping.
- Do not introduce production interfaces solely for tests.
- Do not stage unrelated worktree changes under `companies/companysource`.
