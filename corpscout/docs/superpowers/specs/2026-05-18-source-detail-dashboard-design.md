# Source Detail Dashboard Design

## Goal

Replace the existing minimal `sources_.$name.tsx` page with a four-tab dashboard that gives operators full visibility and control over each data source: its schedule, editable configuration, pull run history, and the raw input queue with per-row inspection.

## Architecture

Extend the existing source detail route (`ui/app/routes/sources_.$name.tsx`) into a tabbed page. All reads use PostgREST views via the existing `/api/v1/db/*` proxy. Mutations go through the scheduler's Go API. The scheduler's periodic enqueue logic gains a `schedule_enabled` gate so automated runs can be paused without disabling the source entirely.

## Tech Stack

- **UI**: React Router v7, shadcn/ui (Tabs, Sheet, Badge, Button, Input), TanStack Table, sonner toasts
- **Reads**: PostgREST via `/api/v1/db/v_source_raw_inputs`, `/api/v1/db/suggestion_source_links`, and `/api/v1/pull-runs`
- **Writes**: scheduler Go API (`PATCH /api/v1/sources/{name}`, two new POST endpoints)
- **DB**: PostgreSQL migrations + sqlc regeneration

---

## Section 1 — Data Model

### Migration 1: `data_sources.schedule_enabled`

`data_sources.config JSONB` already exists (added in `000017_source_ingestion_mvp.up.sql`). No config migration is needed — the API and UI add support for the existing column.

The only new column is:

```sql
ALTER TABLE data_sources ADD COLUMN schedule_enabled BOOLEAN NOT NULL DEFAULT TRUE;
```

**`enabled` semantics (current behaviour, preserved):** `enabled = false` only prevents the source from being auto-scheduled — `scheduleOnce` in `app.go` queries `ListSources` and skips sources where `enabled = false`. It does **not** block manual triggers (`handleTriggerSource` does not check `enabled`) and does not block workers (`SourcePullWorker.Work` does not check `enabled`). This spec preserves that behaviour; manual trigger and worker execution remain unaffected by the `enabled` flag.

**`schedule_enabled` semantics:** `schedule_enabled = false` prevents `scheduleOnce` from enqueuing the next periodic job. Manual trigger still works. The `PATCH` handler stores the value; `scheduleOnce` is extended with `if !src.ScheduleEnabled { continue }`.

### Migration 2: `v_source_raw_inputs` view

A unified view that UNIONs all five raw input tables with a common set of columns. **Does not include `raw_payload`** — the list view only needs metadata; the Sheet drawer fetches full payload separately.

Sources without a raw input table (`nvd_cpe`, `nvd_cve`) are not in the view. The UI hides the Raw Inputs tab when `source.input_table_name` is not in the view's known table set.

`ai_company_profile_raw_inputs` has no `source_native_id` column — it uses `normalized_domain` as the display identifier. `domain_discovery_raw_inputs` uses `domain`.

```sql
CREATE OR REPLACE VIEW v_source_raw_inputs AS
  SELECT
    id,
    'gleif'                        AS source_name,
    'gleif_company_raw_inputs'     AS source_input_table,
    lei                            AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'gleif_company_raw_inputs'
        AND ssl.source_input_key   = id::text
    ) AS has_suggestion
  FROM gleif_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'companies_house'                        AS source_name,
    'companies_house_company_raw_inputs'     AS source_input_table,
    company_number                           AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'companies_house_company_raw_inputs'
        AND ssl.source_input_key   = id::text
    ) AS has_suggestion
  FROM companies_house_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'brreg'                        AS source_name,
    'brreg_company_raw_inputs'     AS source_input_table,
    organization_number            AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'brreg_company_raw_inputs'
        AND ssl.source_input_key   = id::text
    ) AS has_suggestion
  FROM brreg_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'ai_company_profile'                     AS source_name,
    'ai_company_profile_raw_inputs'          AS source_input_table,
    COALESCE(normalized_domain, '')          AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ai_company_profile_raw_inputs'
        AND ssl.source_input_key   = id::text
    ) AS has_suggestion
  FROM ai_company_profile_raw_inputs

  UNION ALL

  SELECT
    id,
    'domain_discovery'                       AS source_name,
    'domain_discovery_raw_inputs'            AS source_input_table,
    domain                                   AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'domain_discovery_raw_inputs'
        AND ssl.source_input_key   = id::text
    ) AS has_suggestion
  FROM domain_discovery_raw_inputs;
```

The migration must grant PostgREST read access for the new view and the base raw input tables used by the Sheet drawer:

```sql
GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
GRANT SELECT ON gleif_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON companies_house_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON brreg_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON ai_company_profile_raw_inputs TO corpscout_anon;
GRANT SELECT ON domain_discovery_raw_inputs TO corpscout_anon;
GRANT SELECT ON suggestion_source_links TO corpscout_anon;
```

**Status groups for filtering:**
- `live`: `processing_status=in.(pending,processing,failed)`
- `archive`: `processing_status=in.(processed,ignored,superseded)`

**Sheet drawer detail**: the drawer queries the base table directly via PostgREST using `source_input_table` + `id` to fetch `raw_payload`. Suggestion links are fetched from `suggestion_source_links` filtered by `source_input_table=eq.{table}&source_input_key=eq.{id}`.

---

## Section 2 — API Layer

### Extended: `PATCH /api/v1/sources/{name}`

Accepts a JSON body with any combination of these optional fields:

```json
{
  "enabled": true,
  "schedule_enabled": false,
  "schedule_kind": "interval",
  "schedule_expression": "24h",
  "config": { "base_url": "https://api.gleif.org/api/v1", "rate_limit_rps": 5 }
}
```

All fields are optional (pointer types in Go). `config` is merged into the existing JSONB (not replaced). Any config key matching the regex `(?i)(key|secret|token|password)` is rejected with HTTP 422.

**Schedule contract for MVP**: only `schedule_kind = "interval"` is executed by the scheduler (`scheduleOnce` skips non-interval sources). `schedule_expression` must be a valid Go duration string (`24h`, `12h`, `168h`). The UI editor enforces this: one duration input, validated client-side before save.

The scheduler's `scheduleOnce` function is extended with `if !src.ScheduleEnabled { continue }` after the existing `if src.ScheduleKind != "interval"` guard.

### New: `POST /api/v1/sources/{name}/raw-inputs/{id}/retry`

Resets a raw input row to `pending` and, for sources handled by `SourceProcessWorker`, enqueues a `source_process` River job so the row is processed immediately.

**Table allowlist** (hardcoded switch — table names cannot be SQL parameters):

| `input_table_name` | Reset query | Enqueue `source_process`? |
|---|---|---|
| `gleif_company_raw_inputs` | `RetryGLEIFRawInput(id)` | yes |
| `companies_house_company_raw_inputs` | `RetryCHRawInput(id)` | yes |
| `brreg_company_raw_inputs` | `RetryBrregRawInput(id)` | yes |
| `ai_company_profile_raw_inputs` | `RetryAIRawInput(id)` | no (no processor yet) |
| `domain_discovery_raw_inputs` | `RetryDomainDiscoveryRawInput(id)` | no (no processor yet) |

Any source with an `input_table_name` outside this list returns HTTP 422 ("raw input retry not supported for this source").

Only rows with `processing_status IN ('failed', 'ignored')` can be retried; others return HTTP 422.

```json
{ "status": "retried" }
```

### New: `POST /api/v1/sources/{name}/raw-inputs/{id}/ignore`

Sets `processing_status = 'ignored'` on the row using the same per-table allowlist as retry. Only `pending` and `failed` rows can be ignored; others return HTTP 422.

```json
{ "status": "ignored" }
```

### Existing (unchanged): pull run history

`GET /api/v1/pull-runs?source={name}&page=1&limit=20` — already exists, used by the Logs tab.

---

## Section 3 — UI Structure

### Route

`ui/app/routes/sources_.$name.tsx` — extended from its current minimal state into a full tabbed dashboard.

### Page header

Always visible above the tabs:
- Source name (large) + `source_group` badge + `pull_task_type` label
- `enabled` status badge (green/red)

### Tabs

| Tab | Content | Shown when |
|---|---|---|
| **Schedule** | Schedule config editor, status banners, next/last run cards, manual trigger | always |
| **Config** | JSONB config key-value editor | always |
| **Logs** | Pull run history table, paginated | always |
| **Raw Inputs** | Live Queue / Archive sub-tabs, TanStack table, Sheet drawer | only when `input_table_name` is in the view allowlist |

---

## Section 4 — Tab Designs

### Schedule tab

**Two status banner rows** at the top of the tab:

- Row 1 (green / red): `● Source enabled` with **Disable source** button, or `⊘ Source disabled` with **Enable source** button. Calls `PATCH` with `{ "enabled": bool }`. Note: disabling does not stop in-progress or manual runs.
- Row 2 (blue / amber): `⏱ Schedule active` with **Pause schedule** button, or `⏸ Schedule paused — manual trigger still works` with **Resume schedule** button. Calls `PATCH` with `{ "schedule_enabled": bool }`.

**Schedule configuration card** (full width, only shown when `schedule_kind = "interval"`):
- Single input: interval duration (e.g. `24h`, `12h`). Client-side validation: must parse as a Go duration (regex `^\d+[hms]$` is sufficient for MVP).
- Save / Reset buttons
- Note: "Changes take effect on the next scheduler tick (≤5 min)."

**Next run card**: estimated next run time computed as `last_started_at + interval`. Display only — no skip/cancel controls in MVP (no API contract for queued job cancellation).

**Last run card**: result (succeeded / failed), timestamp, rows seen/new/updated.

**Manual trigger card**: "Run immediately. Works even when source is disabled or schedule is paused." + **Trigger now** button (existing `POST /api/v1/sources/{name}/trigger`).

### Config tab

Renders the current `data_sources.config` JSONB as a list of editable key-value rows. **Values are edited as JSON literals** — the value input holds the raw JSON representation (e.g. `5` for a number, `"https://..."` for a string, `true` for a boolean). Each value field is validated as parseable JSON before save; invalid JSON shows an inline error and blocks save. This preserves type fidelity: `5` stays a number, not `"5"`.

Buttons: Add field, Save, Reset. Keys matching `key|secret|token|password` are highlighted in red and blocked on save (422 from API). Empty config shows a placeholder with an Add field prompt.

### Logs tab

TanStack table sourced from `GET /api/v1/pull-runs?source={name}`. Columns: Started At, Duration, Rows Seen, New, Updated, Unchanged, Result (success badge / error message truncated). Paginated (20 per page), newest first.

### Raw Inputs tab

Only rendered when `source.input_table_name` is in the view allowlist (`gleif_company_raw_inputs`, `companies_house_company_raw_inputs`, `brreg_company_raw_inputs`, `ai_company_profile_raw_inputs`, `domain_discovery_raw_inputs`). For other sources the tab is hidden entirely.

**Sub-tabs**: Live Queue (badge count of pending+processing+failed) | Archive

**Table columns**: Status (coloured badge) · Native ID · First Seen · Attempts · Error (truncated) · Suggestion (✓ / —)

Data from PostgREST `v_source_raw_inputs` filtered by `source_name=eq.{name}` and `processing_status` group. Paginated (50 per page).

**Sheet drawer** (2× standard width, opens on row click):

- Header: native ID, `source_input_table`, status badge, `processing_attempts` count
- Action buttons (failed/ignored rows only): **Retry** + **Ignore**. For sources without a processor (`ai_company_profile`, `domain_discovery`), Retry resets to pending only — no job is enqueued. A note clarifies: "Row reset to pending. No processor is currently registered for this source."
- Metadata cards: First Seen, Last Seen, Payload Hash
- Suggestion links: fetched from `suggestion_source_links?source_input_table=eq.{table}&source_input_key=eq.{id}` — shows `suggestion_table` + `suggestion_id` per link, or "None produced"
- Last Error block (red, monospace, only if `processing_error` is set)
- Raw Payload: fetched on open from `/api/v1/db/{source_input_table}?id=eq.{id}&select=raw_payload`; syntax-highlighted JSON

After Retry or Ignore: sheet closes, table row refreshes.

---

## Error Handling

- All mutation toasts: success (green) and error (red) via sonner
- Config save with rejected secret key: inline field error, save blocked before API call
- Config value not valid JSON: inline field error, save blocked before API call
- Retry/Ignore on wrong-status row: 422 → red toast with message
- Retry on unsupported source: 422 → red toast "Raw input retry not supported for this source"
- Sheet drawer payload load error: inline error state inside the drawer, not a page crash
- Duration string validation: inline error on the schedule input before save

---

## Security Constraint

Config keys matching `key|secret|token|password` (case-insensitive) are rejected at the API layer with HTTP 422. The UI validates and highlights these before save. Raw payloads are displayed read-only and fetched only on demand (not in the list view). The DB only enforces that `raw_payload` is a valid JSON object; preventing credentials from entering raw payloads is the responsibility of the ingestion code, not the dashboard.
