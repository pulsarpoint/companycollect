# Source Detail Subpages Design

**Date:** 2026-05-21

## Overview

Convert the tabbed `/sources/:name` detail page into separate full pages at `/sources/:name/:subpage`. Add a dedicated read-only raw inputs page per source, and introduce a `requires_translation` flag on data sources to drive translation column visibility.

## Goals

- Deep-linkable URLs for each section of a source detail (schedule, config, logs, raw inputs, pipeline)
- Raw inputs subpage that mirrors `/review/companies` table but scoped to a single source and read-only
- Data-driven translation column: any source with `requires_translation = true` shows the column, no hardcoded source name checks

## Database Change

New migration: `ALTER TABLE data_sources ADD COLUMN requires_translation BOOLEAN NOT NULL DEFAULT false;`

Seed update: set `requires_translation = true` for `brreg`.

## Backend Change

- Add `RequiresTranslation bool` to the Go `DataSource` struct
- Update the SQL query (`GetSource`, `ListSources`) to select the new column
- Run `make sqlc-generate` after query changes

## TypeScript Type Change

`DataSource` interface in `ui/app/types/api.ts` gets `requires_translation: boolean`.

All existing `source.name === "brreg"` checks that gate translation UI are replaced with `source.requires_translation`.

## Route Restructure

Follows the exact same pattern as `review.tsx` / `review._index.tsx`.

### Files

| File | URL | Purpose |
|---|---|---|
| `sources_.$name.tsx` | `/sources/:name/*` | Layout: loads source, defines actions, renders SourceHeader + nav + Outlet |
| `sources_.$name._index.tsx` | `/sources/:name` | Redirect to `./schedule` |
| `sources_.$name.schedule.tsx` | `/sources/:name/schedule` | ScheduleTab content |
| `sources_.$name.config.tsx` | `/sources/:name/config` | ConfigTab content |
| `sources_.$name.logs.tsx` | `/sources/:name/logs` | LogsTab content |
| `sources_.$name.raw_input.tsx` | `/sources/:name/raw_input` | Read-only raw inputs table |
| `sources_.$name.pipeline.tsx` | `/sources/:name/pipeline` | PipelineTab content |

### Layout Route (`sources_.$name.tsx`)

- Loads `DataSource` via `api.getSource(name)` on mount (existing logic, unchanged)
- Defines `handlePatch`, `handleTrigger`, `handleProcess` (existing logic, unchanged)
- Renders `SourceHeader` + nav strip of `<NavLink>` links + `<Outlet />`
- Passes `{ source, saving, triggering, processing, onPatch, onTrigger, onProcess }` to children via `useOutletContext()`
- Nav strip conditionally includes "Raw Inputs" (`hasRawInputs(source)`) and "Pipeline" (`hasPipeline(source)`)
- Error and loading states remain in the layout route (children only render once source is loaded)

### Child Routes

Each child calls `useOutletContext()` to receive the source and action callbacks. They are thin wrappers around the existing Tab components, which require no changes to their props interface.

## Raw Input Subpage

### Data Fetching

Uses `api.getRawInputs()` (same as `/review/companies`) with `source` pre-fixed to the route param. The source filter dropdown is omitted since it is implicit.

### Columns

| Column | Always shown | Condition |
|---|---|---|
| Name | yes | — |
| Native ID | yes | — |
| Status | yes | — |
| Translation | no | `source.requires_translation === true` |
| Created | yes | — |

No "Confirm" / action column.

### Filters

- Search by name (text input)
- Status filter dropdown
- Translation filter dropdown — shown only when `source.requires_translation === true`

### Interactions

- Click row → opens `RawInputDetailSheet` (read-only; sheet already exists in `review.companies.tsx`, extracted to shared location)
- Sort by name, status, created_at
- Pagination (PAGE_SIZE = 50, same as review/companies)

## Shared Component Extraction

`RawInputDetailSheet` is extracted from `review.companies.tsx` into `ui/app/components/app/RawInputDetailSheet.tsx`. Both `review.companies.tsx` and `sources_.$name.raw_input.tsx` import it from there.

A `RawInputsTable` component is extracted into `ui/app/components/app/RawInputsTable.tsx`. It accepts:

```ts
interface RawInputsTableProps {
  sourceName?: string;          // if set: pre-filters API call + hides source dropdown
  requiresTranslation: boolean; // controls translation column + filter
  showConfirmAction?: boolean;  // default false; review/companies passes true
}
```

`review.companies.tsx` passes `showConfirmAction={true}`, `sourceName` undefined (keeps source dropdown), and `requiresTranslation={true}` (cross-source view always shows the column; non-brreg rows show "not required"). The source detail page passes `sourceName={source.name}`, `requiresTranslation={source.requires_translation}`, and omits `showConfirmAction` (defaults false).

## Error Handling

- Layout route: existing pattern (Skeleton while loading, Alert on error)
- Raw input subpage: shows Alert on API failure, same as `review.companies.tsx`

## Testing

- Verify `/sources/brreg` redirects to `/sources/brreg/schedule`
- Verify `/sources/brreg/raw_input` loads and filters to brreg only
- Verify translation column appears for brreg, absent for companies_house
- Verify `/review/companies` still works unchanged after extraction
- Verify `requires_translation = false` sources don't show translation column or filter
