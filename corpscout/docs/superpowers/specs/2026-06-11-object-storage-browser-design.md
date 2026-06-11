# Object Storage Browser Design

## Purpose

Corpscout should provide an internal, read-only browser for the S3-compatible
object storage used by the system. The immediate target is RustFS running as the
shared object-storage service on the companycollect server.

The first version is for operational inspection only:

- list buckets visible to the configured credentials
- open one bucket
- browse folder-like prefixes
- list object metadata
- preserve bucket and prefix in the page URL

This version does not upload, delete, rename, download, preview, or read object
bodies.

## Decision

Build a small custom CorpScout object-storage browser instead of adopting a
general-purpose S3 file manager or AWS Amplify Storage Browser.

The custom browser is the best fit because:

- RustFS credentials must stay server-side.
- The UI only needs read-only metadata browsing.
- CorpScout already has a scheduler-side S3 client and settings pages.
- The feature should follow existing shadcn table, button, alert, and sidebar
  patterns.
- Avoiding upload/delete-oriented components reduces accidental product scope.

## Alternatives Considered

### AWS Amplify Storage Browser

AWS Amplify provides a React storage browser, but it is designed around AWS
browser-side auth and richer storage workflows. Using it behind CorpScout and
RustFS would add dependency weight and auth adaptation work that is unnecessary
for a metadata-only admin viewer.

### Generic React File Manager

A generic file manager could be adapted with an S3 data source, but most file
manager components assume mutating actions such as upload, delete, rename, and
move. That pushes the UI toward operations that are explicitly out of scope for
the first version.

### Custom CorpScout Browser

This is the selected design. The backend exposes a small read-only API backed by
the configured S3 client. The frontend renders a focused settings page and keeps
all storage credentials private.

## Backend API

Add read-only object-storage endpoints to the Corpscout scheduler HTTP API.

### List Buckets

```text
GET /api/v1/object-storage/buckets
```

Response:

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

The endpoint returns all buckets visible to the configured RustFS/S3
credentials. There is no allowlist in the first version.

### List Bucket Objects

```text
GET /api/v1/object-storage/buckets/{bucket}/objects?prefix=&delimiter=/&cursor=&limit=
```

Query behavior:

- `prefix` defaults to empty string.
- `delimiter` defaults to `/`.
- `cursor` is an opaque continuation token from the previous response.
- `limit` defaults to `100`.
- `limit` is capped at `1000`.

Response:

```json
{
  "bucket": "crawls",
  "prefix": "brreg/",
  "delimiter": "/",
  "next_cursor": "opaque-token-or-empty",
  "folders": [
    {
      "prefix": "brreg/search/",
      "name": "search"
    }
  ],
  "objects": [
    {
      "key": "brreg/search/123.md",
      "name": "123.md",
      "size_bytes": 10432,
      "last_modified": "2026-06-11T05:10:00Z",
      "etag": "\"abc123\"",
      "storage_class": "STANDARD"
    }
  ]
}
```

The backend uses S3 `ListBuckets` and `ListObjectsV2`. It does not call
`GetObject`, create presigned URLs, or perform any mutating operation.

## Backend Implementation Notes

Extend `scheduler/internal/s3client` with read-only methods:

- `ListBuckets(ctx context.Context) ([]Bucket, error)`
- `ListObjects(ctx context.Context, input ListObjectsInput) (ListObjectsResult, error)`

The S3 client remains concrete. No new interface is needed for production code.
Handler tests can use focused fakes at the HTTP or S3 boundary.

Add `scheduler/internal/httpapi/object_storage.go` for handlers and register the
routes in `handlers.go`.

Error handling follows the project pattern:

- S3/client layer wraps and returns errors with context.
- HTTP handler logs the error once with `slog`.
- HTTP response returns safe messages such as `list buckets failed` or
  `list objects failed`.
- Bucket names and prefixes may be logged, but credentials and signed material
  are never logged.

## Frontend

Add a new settings page:

```text
/settings/object-storage
```

Add a Settings sidebar item:

```text
Object Storage
```

The page should use a reusable component named `ObjectStorageBrowser`.

Layout:

- Left area: bucket list.
- Main area: selected bucket contents.
- Breadcrumb: bucket root and prefix parts.
- Table rows: folders first, then objects.

Controls:

- refresh
- prefix input/search
- clear prefix
- next page button when `next_cursor` exists

Object table columns:

- name
- full key or prefix
- size
- last modified
- etag

Behavior:

- Opening `/settings/object-storage` loads bucket metadata.
- Clicking a bucket loads its root prefix.
- Clicking a folder navigates to that prefix.
- Clicking a breadcrumb segment navigates back to that prefix.
- The URL stores state:

```text
/settings/object-storage?bucket=crawls&prefix=brreg/search/
```

Reloading the page preserves the selected bucket and prefix.

No upload, delete, rename, download, or preview controls are shown.

## Frontend API Types

Add API client methods:

- `getObjectStorageBuckets()`
- `getObjectStorageObjects(bucket, params)`

Add TypeScript response types:

- `ObjectStorageBucket`
- `ObjectStorageBucketListResponse`
- `ObjectStorageFolder`
- `ObjectStorageObject`
- `ObjectStorageObjectListResponse`

## Edge Cases

### No Buckets

Show an empty state in the settings page.

### Bucket Missing Or Inaccessible

Show an error in the main panel while keeping the bucket list visible.

### Empty Prefix

Show an empty folder state.

### Large Bucket

Use S3 continuation tokens. Do not auto-load every page.

### Object Keys With Spaces Or Unicode

Preserve exact keys from S3. URL-encode query parameters in the frontend.

### Folder Semantics

Folders are S3 common prefixes, not real directory objects. The UI should label
them as folders for operator convenience, but the API should keep the actual
prefix string.

### RustFS Down Or Unreachable

The backend returns a safe error. The UI displays the error and leaves the
settings page usable.

## Security And Scope

This feature is an internal admin/operations view. It exposes all buckets visible
to the configured Corpscout S3 credentials. There is no bucket allowlist in the
first version.

The backend is the only component that talks to RustFS/S3. Browser code never
receives S3 credentials.

The first version is read-only. Mutating storage actions require a separate
design because they need permission, confirmation, and audit decisions.

## Testing

Backend tests:

- S3 client mapping for buckets and object listings.
- Object listing query parsing.
- default and maximum limit handling.
- folder/common-prefix mapping.
- safe error responses.
- route registration.

Frontend tests and checks:

- TypeScript typecheck.
- production build.
- browser verification at `/settings/object-storage`.

Manual browser verification:

- page loads from Settings.
- buckets render.
- selecting a bucket lists folders and objects.
- folder and breadcrumb navigation update the URL.
- object rows expose no mutating actions.

## Out Of Scope

- object download
- object body preview
- presigned URLs
- upload
- delete
- rename or move
- bucket creation/deletion
- per-bucket allowlist
- per-user storage permissions
