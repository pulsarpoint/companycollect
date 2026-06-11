# Source Package Architecture Design 2

## Purpose

This document refines the earlier source module architecture design. The core
change is that each source package should own almost everything source-specific:
configuration JSON, workflow definitions, actions, bucket layout, ClickHouse
schema design, import/query code, and source UI.

Central Corpscout should not understand source files, artifact positions,
ClickHouse table shapes, or explorer table schemas. It should discover packages,
sync their read-only source configuration, register package workflows and HTTP
routes, expose object storage browsing, and show Temporal workflow status by
package workflow prefix.

## Main Principle

Each package knows how to handle its own source JSON and its own data.

The package owns:

- `source.json`
- download URLs and resource definitions
- RustFS bucket name and object key layout
- workflows and actions
- child workflow dependency logic
- ClickHouse schema design and migration SQL content
- ClickHouse import and query code
- package-specific UI components

Central Corpscout owns:

- package discovery and registration
- source config sync into Postgres
- Temporal worker setup loop
- HTTP route mounting loop
- object storage browser
- Temporal status by package workflow prefix
- generic source list and source shell pages
- existing `golang-migrate` execution for Postgres and ClickHouse migrations

## Why This Revision

The previous design considered source outputs and dependencies as abstract
dataset names, such as:

```json
{
  "produces": ["dataset.finland.company_registry"],
  "requires": ["dataset.reference.nace"]
}
```

That is unnecessary now. A package already declares what it stores through:

- ClickHouse migration SQL content and schema tests
- importer code
- query code
- package UI components

Dependencies should be workflow dependencies, not abstract dataset dependencies.
If a package needs NACE sync before refreshing an explorer table, the package
workflow should run or check the NACE workflow before it continues.

## Package Layout

Recommended backend layout:

```text
scheduler/internal/sourcepackages/
  registry.go
  package.go
  runtime/
  sources/
    finland/
      prhytj/
        source.json
        package.go
        workflows.go
        activities.go
        download.go
        import.go
        queries.go
        clickhouse_schema.md
      prhxbrl/
        source.json
        package.go
        workflows.go
        activities.go
        download.go
        import.go
        queries.go
        clickhouse_schema.md
  references/
    nace/
      source.json
      package.go
      workflows.go
      activities.go
      clickhouse_schema.md
  projections/
    finland/
      companyexplorer/
        source.json
        package.go
        workflows.go
        activities.go
        queries.go
        clickhouse_schema.md
```

The exact top-level folder name can stay `companysources` if that is easier for
incremental migration, but the conceptual boundary should be "source package",
not "generic source handler".

ClickHouse migration files stay in the existing central
`clickhouse/migrations` directory and continue to run through `golang-migrate`.
Package folders can keep schema notes, table constants, query code, and tests
near the package, but they should not introduce a second migration runner.

## Package JSON

The package JSON should contain operational inputs and package metadata. It
should not try to describe all produced ClickHouse tables. Migrations and code
already do that.

Example for Finland PRH YTJ:

```json
{
  "key": "finland/prhytj",
  "name": "finland_prhytj",
  "display_name": "Finland PRH YTJ",
  "country": "finland",
  "group": "registry",
  "enabled": true,
  "bucket": "source-finland-prhytj",
  "description": "Finnish company registry data from PRH Open Data YTJ API v3",
  "docs_url": "https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html",
  "resources": [
    {
      "key": "companies",
      "display_name": "Company snapshot",
      "url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
      "method": "GET",
      "object_key_template": "runs/{run_id}/source.ndjson",
      "required": true
    },
    {
      "key": "codelist_REK_en",
      "display_name": "Register code list",
      "url": "https://avoindata.prh.fi/opendata-ytj-api/v3/description?code=REK&lang=en",
      "method": "GET",
      "object_key_template": "runs/{run_id}/codelists/REK.en.tsv",
      "required": true
    }
  ],
  "workflows": {
    "pull": {
      "workflow_type": "FinlandPRHYTJPullWorkflow",
      "workflow_id_prefix": "source.finland.prhytj.pull"
    },
    "import": {
      "workflow_type": "FinlandPRHYTJImportWorkflow",
      "workflow_id_prefix": "source.finland.prhytj.import"
    },
    "refresh_explorer": {
      "workflow_type": "FinlandPRHYTJRefreshExplorerWorkflow",
      "workflow_id_prefix": "source.finland.prhytj.refresh_explorer",
      "depends_on": [
        {
          "package": "reference/nace",
          "action": "sync",
          "mode": "before"
        },
        {
          "package": "finland/prh_xbrl",
          "action": "import",
          "mode": "before_optional"
        }
      ]
    }
  }
}
```

The package owns the meaning of `resources`, `workflows`, and `depends_on`.
Central Corpscout may sync and display the JSON, but it should not deeply
interpret resource keys, object key templates, or dependency semantics.

## Package API

The central package API should be small.

```go
type Package interface {
    Config() Config
    RegisterTemporal(worker worker.Worker, deps Deps)
    RegisterHTTP(router chi.Router, deps Deps)
}
```

This interface is acceptable because there are many real implementations and a
stable central-to-package boundary. Avoid larger interfaces that try to model
download, import, explorer, and artifact behavior generically.

Central app wiring should do:

```go
for _, pkg := range packages {
    syncConfigToPostgres(ctx, pkg.Config())
    pkg.RegisterTemporal(worker, deps)
    pkg.RegisterHTTP(router, deps)
}
```

Central code should not switch on source names.

## Temporal Model

Each package owns its workflows and actions. Workflow and activity boilerplate is
acceptable when it lives inside the package, because it is source-specific and
testable there.

Central registration loops packages. The package registers its own workflow
types and activity methods.

Example:

```go
func (p Package) RegisterTemporal(worker worker.Worker, deps Deps) {
    actions := NewActions(deps, p.config)

    worker.RegisterWorkflowWithOptions(
        PullWorkflow,
        workflow.RegisterOptions{Name: p.config.Workflows.Pull.WorkflowType},
    )
    worker.RegisterWorkflowWithOptions(
        ImportWorkflow,
        workflow.RegisterOptions{Name: p.config.Workflows.Import.WorkflowType},
    )
    worker.RegisterActivity(actions.DownloadResources)
    worker.RegisterActivity(actions.ImportToClickHouse)
}
```

The package workflow decides how to use dependencies:

```go
func RefreshExplorerWorkflow(ctx workflow.Context, input RefreshExplorerInput) error {
    if cfg.ShouldSyncNACEBeforeRefresh() {
        workflow.ExecuteChildWorkflow(ctx, nace.SyncWorkflow, naceInput)
    }

    if cfg.ShouldImportPRHXBRLBeforeRefresh() {
        workflow.ExecuteChildWorkflow(ctx, prhxbrl.ImportWorkflow, xbrlInput)
    }

    return workflow.ExecuteActivity(ctx, RefreshExplorerActivity, input).Get(ctx, nil)
}
```

The JSON can define default dependent workflows, but package code decides how to
apply those defaults.

## Workflow Status

Central Corpscout should not store normalized action/file run rows for every
package. Workflow status should come from Temporal visibility/history using
package workflow ID prefixes and search attributes.

Examples:

```text
source.finland.prhytj.pull.20260611T120000Z
source.finland.prhytj.import.20260611T121000Z
source.finland.prhytj.refresh_explorer.20260611T122000Z
source.finland.prhxbrl.pull.20260611T123000Z
projection.finland.company_explorer.refresh.20260611T124000Z
```

Central API can expose:

```text
GET /api/v1/packages/{package}/workflows
GET /api/v1/packages/{package}/workflows/{workflow_id}
```

Those endpoints query Temporal by prefix and return summary status.

Temporal is the source of truth for execution state. Pull, import, refresh,
mapping, enrichment, and dependency sync should all be Temporal workflows. The
central API should be a thin wrapper around Temporal start/list/describe calls
and package-specific request validation.

Workflows should set stable search attributes when the Temporal cluster supports
them:

```text
PackageKey = "finland/prhytj"
PackageName = "finland_prhytj"
PackageAction = "pull"
Country = "finland"
RunID = "20260611T120000Z"
```

Temporal logs and history are enough for execution debugging, retries, timing,
parent/child relationships, and failure inspection. They should not be treated as
the durable artifact index.

If Temporal retention later proves insufficient for product needs, add one small
generic index table:

```text
package_workflow_runs
  id
  package_key
  action
  workflow_id
  workflow_run_id
  status
  started_at
  closed_at
```

Do not add source file run or artifact tables unless there is a demonstrated
need. If a product screen needs long-term workflow history beyond Temporal
retention, prefer the `package_workflow_runs` index above over rebuilding the old
normalized source action/file model.

## Postgres Model

Postgres should store synced package configuration and possibly workflow run
indexes later. It should not store source-specific file definitions, artifact
positions, or action definitions in normalized tables.

Recommended initial table:

```text
source_package_catalog
  key
  name
  display_name
  country
  group
  bucket
  config_json
  config_hash
  enabled
  synced_at
```

This table supports:

- source list page
- source detail shell
- searching/filtering sources
- showing read-only config JSON
- joining generic UI navigation to package metadata

The package JSON remains the source of truth.

## Artifact Model

Do not create central artifact tables.

Artifacts are stored in RustFS/S3. The package knows its own bucket and object
key layout. If the package needs artifacts, it lists or reads its bucket.

The object storage browser already gives humans a generic way to inspect bucket
contents. Central Corpscout does not need duplicate artifact metadata.

Every package workflow that creates artifacts should write a package-owned run
manifest to RustFS. This manifest is the durable artifact ledger for that run.
The central app may display the manifest as JSON, but it should not normalize it
into source-specific Postgres tables.

Example source bucket:

```text
bucket: source-finland-prhytj
keys:
  runs/{run_id}/source.ndjson
  runs/{run_id}/codelists/REK.en.tsv
  runs/{run_id}/codelists/YRMU.en.tsv
  runs/{run_id}/manifest.json
  snapshots/latest/source.ndjson
  logs/{run_id}/download.json
```

For PRH XBRL, the package can choose a different layout:

```text
bucket: source-finland-prh-xbrl
keys:
  runs/{run_id}/statements.ndjson
  runs/{run_id}/xml/{business_id}/{financial_date}.xml
  runs/{run_id}/manifest.json
  discovery/{window_id}/manifest.ndjson
```

Central code only knows the package bucket name.

Example manifest shape:

```json
{
  "run_id": "20260611T120000Z",
  "package_key": "finland/prhytj",
  "workflow_id": "source.finland.prhytj.pull.20260611T120000Z",
  "artifacts": [
    {
      "resource_key": "companies",
      "object_key": "runs/20260611T120000Z/source.ndjson",
      "content_sha256": "...",
      "content_length_bytes": 123456,
      "records_written": 1000
    }
  ]
}
```

## ClickHouse Model

The source of truth for applied ClickHouse schema is the existing
`clickhouse/migrations` directory, applied with the existing `golang-migrate`
workflow. Do not build a custom package migration runner.

The package owns:

- tables
- views
- refreshed/cache tables
- dictionaries or source-specific reference tables
- source-specific query code
- schema design and migration SQL content

The migration files live centrally so `make clickhouse-migrate-up` remains the
only execution path. Packages own the design conceptually, but migration
execution stays in the existing app/tooling.

Use migration number ranges to encode dependency ordering:

```text
000001-000009  global/base databases and shared infrastructure
000010-000019  references, especially NACE
000020-000089  source package tables and source-owned caches
000100+        projections that depend on references and source tables
```

Example:

```text
clickhouse/migrations/
  000001_base_databases.up.sql
  000001_base_databases.down.sql
  000010_reference_nace_tables.up.sql
  000010_reference_nace_tables.down.sql
  000020_source_finland_prhytj_tables.up.sql
  000020_source_finland_prhytj_tables.down.sql
  000030_source_finland_prh_xbrl_tables.up.sql
  000030_source_finland_prh_xbrl_tables.down.sql
  000100_projection_finland_company_explorer.up.sql
  000100_projection_finland_company_explorer.down.sql
```

Reference packages should expose stable table or view contracts. For NACE, prefer
contract names such as `ref_nace_codes` or `ref_nace_codes_v1`. Source and
projection packages should depend on those contracts instead of reaching into
unstable intermediate tables. If the contract changes incompatibly, add a new
versioned view/table and migrate consumers deliberately.

## Source And Projection Packages

Some packages pull external data. Other packages combine data from multiple
sources into useful ClickHouse tables or UI views.

Use two conceptual package types:

```text
source package
  pulls/imports one external source

projection package
  combines existing ClickHouse tables into a view/cache/table for a product use
```

For Finland:

```text
source: finland/prhytj
source: finland/prh_xbrl
reference: reference/nace
projection: finland/company_explorer
```

The Finland company explorer projection can run child workflows for PRH YTJ,
PRH XBRL, and NACE if needed, then refresh its own ClickHouse table.

This avoids forcing PRH YTJ to own financial columns that really come from PRH
XBRL. It also avoids pretending that every source has the same explorer shape.

## UI Model

The UI should have source/package-specific components.

Recommended layout:

```text
ui/app/package-modules/
  registry.ts
  finland/
    prhytj/
      Actions.tsx
      Config.tsx
      Explorer.tsx
      Detail.tsx
    prhxbrl/
      Actions.tsx
      Statements.tsx
    company-explorer/
      Explorer.tsx
  reference/
    nace/
      Actions.tsx
      Browser.tsx
```

Generic routes remain:

```text
/sources
/sources/:source
/sources/:source/config
/sources/:source/actions
/sources/:source/explorer
```

The generic route shell looks up a UI module:

```ts
const module = packageUiRegistry[source.name]

return module?.Explorer
  ? <module.Explorer source={source} />
  : <GenericUnavailable message="Explorer is not available for this source." />
```

Each package UI knows its own table shape and backend endpoints. There is no
need for a universal explorer table contract now.

## HTTP Routing

Central Corpscout mounts package routes.

Example:

```go
for _, pkg := range packages {
    r.Route("/api/v1/packages/"+pkg.Config().Name, func(r chi.Router) {
        pkg.RegisterHTTP(r, deps)
    })
}
```

Package endpoints can be different:

```text
GET  /api/v1/packages/finland_prhytj/explorer
GET  /api/v1/packages/finland_prhytj/filter-options
POST /api/v1/packages/finland_prhytj/workflows/pull
POST /api/v1/packages/finland_prhytj/workflows/import

GET  /api/v1/packages/finland_prh_xbrl/statements
POST /api/v1/packages/finland_prh_xbrl/workflows/pull

POST /api/v1/packages/finland_company_explorer/workflows/refresh
```

Generic source pages can link to package-specific routes through package UI
metadata.

## Dependency Handling

Use dependent workflows, not dataset dependency declarations.

The package JSON can define dependency defaults:

```json
{
  "depends_on": [
    {
      "package": "reference/nace",
      "action": "sync",
      "mode": "before"
    },
    {
      "package": "finland/prh_xbrl",
      "action": "import",
      "mode": "before_optional"
    }
  ]
}
```

But this should not be a central engine in the first implementation. The package
workflow owns how and when child workflows run.

Modes can stay package-level semantics:

```text
before
before_optional
check_only
manual_only
```

Central Corpscout only needs to show the config and start the package workflow.

Dependency workflows should be idempotent and freshness-aware. A package that
depends on NACE should check whether the NACE sync workflow completed
successfully within the last day before starting a new sync. If a fresh
successful run exists, skip the dependency. If no fresh run exists, start or wait
for a deterministic dependency workflow.

Recommended NACE dependency rule:

```text
dependency package: reference/nace
dependency action: sync
freshness window: 24h
workflow id: reference.nace.sync.YYYYMMDD
```

If another package already started today's NACE sync, the dependent workflow
should use the existing execution or wait for it instead of starting duplicate
work. Optional dependencies may continue when they fail or are stale, but required
dependencies must fail the parent workflow with a safe, package/action-scoped
error.

## Error Handling And Logging

Follow the Go project rules:

- lower layers wrap and return errors with `github.com/cockroachdb/errors`
- package workflows and activities add package/action context
- worker and HTTP boundaries log once with `log/slog`
- HTTP responses return safe messages
- no secrets, tokens, cookies, or sensitive source bodies in logs

Package code should avoid logging every internal error. Return errors to the
workflow/activity boundary and let that layer record workflow failure.

## Testing Strategy

Package tests:

- config JSON decodes and validates
- workflow registration registers expected workflow names
- workflow ID prefixes are stable
- download code maps resources to expected bucket keys
- artifact-producing workflows write a valid `runs/{run_id}/manifest.json`
- importer writes expected ClickHouse rows
- central ClickHouse migrations contain required tables/views for that package
- package HTTP routes return expected source-specific response shapes
- child workflow dependencies are invoked when configured
- freshness checks skip dependencies such as NACE when a recent successful run
  exists
- deterministic dependency workflow IDs avoid duplicate concurrent syncs

Central tests:

- registry rejects duplicate package keys and names
- central startup syncs package config JSON to Postgres
- central Temporal worker loops packages and calls `RegisterTemporal`
- central HTTP setup mounts package routes
- workflow status API queries Temporal by package prefix and/or search attributes
- source list UI renders from synced package catalog
- ClickHouse migration files remain in `clickhouse/migrations` and preserve the
  agreed version ranges for base, reference, source, and projection migrations

## Incremental Migration Path

### Phase 1: Add Package Registry

- Add package registry and minimal package API.
- Register existing source packages through the new registry.
- Keep old generic source tables and actions temporarily.
- Keep existing `golang-migrate` Postgres and ClickHouse migration execution.

### Phase 2: Add Package Catalog Table

- Add `source_package_catalog`.
- Sync package JSON into the table on scheduler startup.
- Update source list page to read from package catalog or a compatibility view.

### Phase 3: Move Finland PRH YTJ JSON Into Package

- Move PRH YTJ source JSON beside the PRH YTJ package.
- Make PRH YTJ package own resource definitions.
- Keep behavior unchanged.

### Phase 4: Move PRH YTJ Workflows Into Package

- Add PRH YTJ package-owned workflows and activities.
- Trigger them through package API.
- Remove PRH YTJ assumptions from generic source actions.
- Write package-owned RustFS run manifests for artifact-producing workflows.

### Phase 5: Standardize ClickHouse Migration Ownership

- Keep ClickHouse migration files in the existing `clickhouse/migrations`
  directory.
- Rename or add future migrations using the agreed ordering bands: base,
  reference, source, then projection.
- Add schema tests that tie each package to the central migration SQL it owns
  conceptually.
- Do not add a package-aware migration runner.

### Phase 6: Move PRH YTJ UI

- Move PRH YTJ explorer UI into a package UI module.
- Generic source explorer route loads the package UI component.

### Phase 7: Migrate PRH XBRL

- Move PRH XBRL JSON, workflows, and bucket layout into its package.
- Keep PRH XBRL ClickHouse migrations in the central migration directory.
- Validate child workflow usage from Finland projection workflows.

### Phase 8: Add Finland Company Explorer Projection Package

- Create a projection package that combines PRH YTJ, PRH XBRL, and NACE data.
- Move combined explorer table/cache/query/UI there.
- Depend on stable NACE ClickHouse contracts such as `ref_nace_codes` or
  `ref_nace_codes_v1`.
- Use freshness-aware NACE sync dependency handling before refresh workflows.

### Phase 9: Migrate US Sources

- Migrate SEC EDGAR, Colorado entities, and IRS EO BMF packages.
- Use the simpler sources to validate the package pattern remains light.

## Recommended First Step

Start with the package registry and package catalog table. Do not immediately
remove existing `data_sources` tables or generic workflows. Build a compatibility
path first, migrate Finland PRH YTJ, then delete old structures once the new
package path proves itself.

The first implementation should prove:

- one package owns its JSON
- central startup syncs that JSON
- central Temporal setup calls package registration
- one package workflow can be triggered
- package status can be listed by Temporal prefix and, where available, search
  attributes
- one artifact-producing workflow writes a RustFS run manifest
- package UI can be selected by source name
- existing `golang-migrate` ClickHouse migration execution remains unchanged

After that, the rest of the system can migrate source by source.
