# BRREG Source Flow Design

Date: 2026-05-22

## Goal

Create a BRREG-only source flow view that explains where BRREG records are in the ingestion pipeline and lets users navigate to the exact filtered raw-input rows that need attention.

The Flow tab is a visual map, not an action workbench. Actions that operate on subsets of entries belong in the Raw Inputs table, where the user can see the filtered rows, select rows, and run actions against either selected rows or all rows matching the current filters.

## Scope

The first implementation is limited to `brreg`.

Add a new source detail tab:

```text
/sources/brreg/flow
```

The tab uses React Flow for graph rendering and ELKJS for vertical layout.

The existing Schedule tab should remain focused on download scheduling only. It should not imply that scheduling triggers the full processing flow.

## Page Boundaries

### Flow tab

The Flow tab shows:

- BRREG source state
- last downloaded time
- next scheduled pull
- raw input counts by `processing_status`
- translation counts by `translation_status`
- ready-for-company-suggestion count
- company suggestion counts when available
- task counters/status near transitions

The Flow tab does not execute subset actions. Counts and state chips link to filtered Raw Inputs URLs.

### Raw Inputs tab

The Raw Inputs tab owns:

- filter parsing from URL params
- detailed row inspection
- row selection
- actions for selected rows
- actions for all rows matching the current filters

Actions shown in Raw Inputs must be valid for the current filtered state.

### Review page

The Review page owns final approval from `company_suggestions` into `companies`.

The source pages must not approve directly into `companies`.

## BRREG Flow Graph

The graph is vertical. Nodes are state widgets. Edges are transitions and may show task counters, but not execution buttons.

Initial state nodes:

1. `BRREG source`
   - Last downloaded
   - Next scheduled pull
   - Link to BRREG pull runs filtered by source

2. `Raw inputs`
   - Counts by `processing_status`: `pending`, `processing`, `processed`, `failed`, `ignored`, `superseded`, `total`
   - Each non-zero count links to `/sources/brreg/raw_input` with the corresponding filter

3. `Translation states`
   - Counts by `translation_status`: `pending`, `translating`, `translated`, `failed`, `total`
   - Each non-zero count links to `/sources/brreg/raw_input` with the corresponding translation filter

4. `Ready for company suggestions`
   - Count of rows matching:

```text
translation_status = translated
processing_status = pending
has_suggestion = false
```

   - Link target:

```text
/sources/brreg/raw_input?translation_status=translated&processing_status=pending&has_suggestion=false
```

5. `Company suggestions`
   - Counts by suggestion review state if `company_suggestions.status` is available in the API
   - Links to the relevant review/suggestion page filters

## Filter Links

Examples:

```text
/sources/brreg/raw_input?processing_status=pending
/sources/brreg/raw_input?processing_status=failed
/sources/brreg/raw_input?translation_status=pending
/sources/brreg/raw_input?translation_status=failed
/sources/brreg/raw_input?translation_status=translated&processing_status=pending&has_suggestion=false
```

Zero-count links should render as muted and non-clickable.

Invalid URL params on Raw Inputs should be ignored with a small warning instead of breaking the table.

## Raw Inputs Actions

Raw Inputs should show actions based on the active filters and selected rows.

Supported BRREG actions:

- `translation_status=pending`
  - Translate selected rows
  - Translate all filtered rows

- `translation_status=failed`
  - Retry translation for selected rows
  - Retry translation for all filtered rows

- `translation_status=translated&processing_status=pending&has_suggestion=false`
  - Move selected rows to company suggestions
  - Move all filtered rows to company suggestions

- `processing_status=failed`
  - Retry processing for selected rows
  - Retry processing for all filtered rows

Action labels must state the exact scope:

```text
Translate 50 selected rows
Translate all 1,240 filtered rows
Move all 328 filtered rows to company suggestions
```

Avoid vague labels such as `Translate all` when filters are active.

## Task Ledger

Add a local task ledger that is the UI source of truth for source action task counters.

Create a table named:

```text
source_action_tasks
```

Fields:

```text
id
source_name
action_key
executor_type
temporal_workflow_id
temporal_workflow_run_id
river_job_id
status
requested_scope
requested_by
started_at
finished_at
error_message
metadata
created_at
updated_at
```

`executor_type` values:

```text
temporal
river
```

`status` values:

```text
queued
running
completed
failed
cancelled
```

The ledger should support both direct Temporal workflows and River jobs because the app still has River-backed tasks. New BRREG Raw Inputs actions introduced by this project should start Temporal workflows directly and record the Temporal workflow ID/run ID in `source_action_tasks`. Existing River-backed jobs can still be represented by filling `river_job_id`.

Task counters in the Flow graph should summarize ledger rows by `source_name` and `action_key`, for example:

```text
Download raw data: 1 running
Translation: 0 running, 1 failed
Create suggestions: 2 running
```

Clicking a task counter should open a task list or drawer filtered to that source/action. It should not execute work.

## Backend API

Add:

```text
GET /api/v1/sources/brreg/flow
```

Response shape should be a graph config:

```json
{
  "source": "brreg",
  "layout": "vertical",
  "nodes": [],
  "edges": [],
  "updated_at": "..."
}
```

Node payloads include:

- title
- description
- counts
- links
- disabled state
- summary metadata

Edge payloads include:

- action key
- label
- task counters
- task list URL/filter metadata

Raw Inputs actions should use explicit action endpoints that accept either selected IDs or a validated filter scope. Filtered-all actions should not rely on only the currently loaded page of rows.

Example request shape:

```json
{
  "ids": ["..."],
  "filters": {
    "translation_status": "translated",
    "processing_status": "pending",
    "has_suggestion": false
  }
}
```

The server must validate that the requested action is allowed for the selected IDs or filter scope.

## Frontend

Add dependencies:

```text
@xyflow/react
elkjs
```

Add a BRREG-only Flow route and tab. Other sources should not show the Flow tab until they have a graph config.

The React Flow renderer should:

- render state nodes as compact operational widgets
- use ELKJS vertical layout
- disable node dragging in the first version
- make counts keyboard-accessible links
- keep edge/task counters clickable for task drill-down only
- avoid action buttons in the graph

Raw Inputs should:

- initialize filters from URL params
- update URL params when filters change
- show current filter chips
- show selected-row and all-filtered actions where valid
- refresh rows and counts after an action starts

## Error Handling

- If Flow counts fail to load, show a destructive alert in the Flow tab.
- If a task counter cannot be resolved, show `-` or `Unknown`.
- If a count is zero, render it muted and non-clickable.
- If Raw Inputs receives unsupported filters, ignore invalid filters and show a warning.
- If an action request is invalid for the selected/filter scope, return `422` with a safe message.
- Boundary handlers should log errors once with `slog`; lower layers should wrap and return errors.

## Testing

Backend:

- `GET /api/v1/sources/brreg/flow` returns correct counts and filter URLs.
- Ready-for-company-suggestion count excludes rows that already have a suggestion link.
- Task counters aggregate `source_action_tasks` by source/action/status.
- Raw Inputs action endpoints reject invalid scopes.
- Filtered-all actions operate on all matching rows, not only the current page.

Frontend:

- Typecheck and production build.
- Flow route renders BRREG graph vertically.
- Count links navigate to Raw Inputs with the expected query params.
- Raw Inputs initializes filters from URL params.
- Valid bulk actions appear only for matching filters.
- Scope labels include selected or filtered counts.

Browser verification:

- Open `/sources/brreg/flow`.
- Click `translation_status=pending` count and verify Raw Inputs filter state.
- Click ready-for-suggestions count and verify the move-to-suggestions action appears.
- Verify final approval remains available only from Review.
