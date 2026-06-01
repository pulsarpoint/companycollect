# Brreg Norwegian-to-English Translation Pipeline Design

## Goal

Translate Norwegian company data in `brreg_company_raw_inputs` to English before it can be processed into company suggestions or approved. Translation is manually triggered by the operator, either for all pending rows or a selected subset. Untranslated rows are invisible to the processor.

## Architecture

### PullBrreg flow (import only — no processing)

```
PullBrreg Temporal workflow
  → download_brreg_bulk (Python activity)
  → ImportBrregBulk (Go activity)
      writes brreg_company_raw_inputs.raw_payload
      translation_status = 'pending', raw_payload_en = NULL
  → MarkExecutionComplete (Go activity)
      marks temporal_executions row completed
      does NOT enqueue source_process
```

`source_process` is never enqueued by the import pipeline for brreg. Processing only happens after the operator explicitly runs translation.

### Translation flow (operator-triggered)

```
Operator (UI)
  │
  ├─ "Translate All"      → POST /api/v1/sources/brreg/translate
  ├─ "Translate Selected" → POST /api/v1/sources/brreg/translate { "ids": [...] }
  └─ "Translate" (detail) → POST /api/v1/sources/brreg/translate { "ids": [rowId] }
         │
         ▼
  corpscout scheduler
  → triggers TranslateBrregRawInputs Temporal workflow
         │
         ▼
  data-pipelines go-worker (task queue: corpscout-pipelines)
  → TranslateBrregBatch activity (loops with ContinueAsNew)
         │
         ├─ reads/writes brreg_company_raw_inputs (raw_payload_en, translation_status)
         └─ reads/writes translation_cache
```

Translation ends when there are no claimable pending or stale-translating rows left. Successful rows have populated `raw_payload_en`; failed rows remain `failed` with `raw_payload_en = NULL` until the operator retries them explicitly. No `source_process` job is enqueued automatically — the operator triggers company processing manually (see § Processing gate below).

### Processing gate (manual, operator-triggered)

When the operator is ready to process translated rows into company suggestions, they trigger `source_process` manually via the UI or directly via the River DB insert. `BrregProcessor` only claims rows where `raw_payload_en IS NOT NULL` — this is the hard gate ensuring Norwegian data never enters the suggestion/approval pipeline.

---

## Database Changes

### Migration: add translation columns to `brreg_company_raw_inputs`

```sql
ALTER TABLE brreg_company_raw_inputs
  ADD COLUMN raw_payload_en              JSONB,
  ADD COLUMN translation_status          TEXT NOT NULL DEFAULT 'pending',
  ADD COLUMN translation_attempts        INT  NOT NULL DEFAULT 0,
  ADD COLUMN translation_error           TEXT,
  ADD COLUMN translation_model           TEXT,
  ADD COLUMN translation_prompt_version  TEXT,
  ADD COLUMN translated_at               TIMESTAMPTZ,
  ADD COLUMN translation_lease_by        TEXT,
  ADD COLUMN translation_lease_until     TIMESTAMPTZ,
  ADD COLUMN translation_fx_source        TEXT,
  ADD COLUMN translation_fx_rate_date     DATE,
  ADD CONSTRAINT chk_brreg_translation_status CHECK (
    translation_status IN ('pending', 'translating', 'translated', 'failed')
  );

CREATE INDEX idx_brreg_raw_translation_status
  ON brreg_company_raw_inputs (translation_status, created_at);

CREATE INDEX idx_brreg_raw_translation_lease
  ON brreg_company_raw_inputs (translation_lease_until)
  WHERE translation_status = 'translating';
```

`translation_status` values:
- `pending` — newly inserted, not yet translated
- `translating` — claimed by an active activity; `translation_lease_by` and `translation_lease_until` are set
- `translated` — `raw_payload_en` is populated; this is the only success state, regardless of whether translation required LLM calls, cache lookups, or only structural renames
- `failed` — translation attempted and errored; `translation_error` is set

`translated` produces a populated `raw_payload_en` and satisfies the `IS NOT NULL` gate for `ClaimPendingBrregRawInputs`.

**Lease and stale-row recovery:** `translation_lease_by` is the Temporal workflow run ID; `translation_lease_until` is `now() + 10 minutes`. The `TranslateBrregBatch` claim step also reclaims stale leases — rows where `translation_status = 'translating' AND translation_lease_until < now()` are treated as `pending` and included in the next claim. The same workflow run may also reclaim its own active lease immediately (`translation_lease_by = <workflow_run_id>`) so a Temporal activity retry after an LLM timeout does not return `claimed=0` and prematurely stop the workflow. This means a crashed activity can never strand rows longer than 10 minutes, while a retry of the same workflow can continue immediately.

### Migration: `translation_cache` table

```sql
CREATE TABLE translation_cache (
  category           TEXT NOT NULL,
  original_hash      TEXT NOT NULL,
  source_lang        TEXT NOT NULL DEFAULT 'no',
  target_lang        TEXT NOT NULL DEFAULT 'en',
  prompt_version     TEXT NOT NULL,
  model              TEXT NOT NULL,
  original_text      TEXT NOT NULL,
  translated_text    TEXT NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (category, original_hash, source_lang, target_lang, prompt_version, model)
);
```

`original_hash` is `sha256(trim(lower(original_text)))` hex-encoded. `category` is one of: `org_form`, `sector_code`, `industry_code`, `capital_type`, `activity`, `statutory_purpose`, `vat_description`, `other`.

Using `(category, original_hash, source_lang, target_lang, prompt_version, model)` as the key means changing the prompt or upgrading the model automatically produces new cache entries without deleting historical cache rows. Runtime lookups only use the current `(source_lang, target_lang, prompt_version, model)` tuple; old translations are retained for audit/history, not as automatic fallback.

---

## Field Mapping: `raw_payload` → `raw_payload_en`

`raw_payload_en` is a **curated English-normalized payload**. It maps only the fields listed in this section, using English key names and translated values. Fields not listed are omitted. `raw_payload` remains the **authoritative lossless registry record** and is never modified — all Norwegian data is preserved there regardless of translation status.

### Structural rename (no translation, no LLM)

| Norwegian key | English key | Notes |
|---|---|---|
| `organisasjonsnummer` | `organization_number` | string, copy as-is |
| `navn` | `name` | company name, copy as-is — never translate legal names |
| `hjemmeside` | `website` | URL, copy as-is |
| `stiftelsesdato` | `founded_date` | date string, copy as-is |
| `registreringsdatoEnhetsregisteret` | `registered_date` | date string |
| `konkurs` | `is_bankrupt` | boolean |
| `underAvvikling` | `is_under_liquidation` | boolean |
| `underTvangsavviklingEllerTvangsopplosning` | `is_forced_dissolution` | boolean |
| `erIKonsern` | `is_in_group` | boolean |
| `registrertIMvaregisteret` | `in_vat_register` | boolean |
| `registrertIForetaksregisteret` | `in_business_register` | boolean |
| `harRegistrertAntallAnsatte` | `has_registered_employees` | boolean |
| `sisteInnsendteAarsregnskap` | `last_annual_report_year` | string/number |
| `kapital.belop` | `capital.amount` | converted to USD; original amount preserved as `capital.original_amount` |
| `kapital.valuta` | `capital.currency` | always `USD` in `raw_payload_en`; original currency preserved as `capital.original_currency` |
| `kapital.antallAksjer` | `capital.shares` | number |
| `forretningsadresse` | `business_address` | see address mapping below |
| `postadresse` | `postal_address` | see address mapping below |

### Address sub-object mapping (structural, no translation)

| Norwegian key | English key |
|---|---|
| `adresse` | `street` |
| `poststed` | `city` |
| `postnummer` | `postal_code` |
| `kommune` | `municipality` |
| `kommunenummer` | `municipality_number` |
| `landkode` | `country_code` |
| `land` | `country` |

### Cached translation (LLM on first occurrence, cache thereafter)

These are finite enum-like values. The first time a value is seen it goes to the LLM; subsequently it's served from `translation_cache`.

| Norwegian field | Cache category | Example |
|---|---|---|
| `organisasjonsform.beskrivelse` | `org_form` | "Enkeltpersonforetak" → "Sole proprietorship" |
| `organisasjonsform.kode` | *(copy as-is)* | "ENK" |
| `institusjonellSektorkode.beskrivelse` | `sector_code` | "Personlig næringsdrivende" → "Self-employed individuals" |
| `institusjonellSektorkode.kode` | *(copy as-is)* | "8200" |
| `naeringskode1/2/3.beskrivelse` | `industry_code` | "Elektrisk installasjonsarbeid" → "Electrical installation work" |
| `naeringskode1/2/3.kode` | *(copy as-is)* | "43.210" |
| `kapital.type` | `capital_type` | "Aksjekapital" → "Share capital" |

### Currency normalization (official exchange rate)

Monetary values in `raw_payload_en` are normalized to USD. For Brreg this applies to `kapital.belop` / `kapital.valuta` today, and any future mapped monetary field should follow the same pattern. Rows with no mapped monetary field do not need FX conversion and should leave FX metadata absent/null.

Use an official central-bank exchange-rate feed. The implementation should reuse the existing corpscout `fxrates` design, which converts through the European Central Bank daily reference feed:

`https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml`

The ECB feed includes both NOK and USD rates relative to EUR. Convert source currency to USD with:

```text
amount_usd = amount_original / rate[source_currency] * rate["USD"]
```

`raw_payload_en.capital.amount` is the converted USD amount, rounded to two decimal places for display JSON. `raw_payload_en.capital.amount_usd_cents` is the integer USD-cent value for exact comparisons. The original registry amount and currency are preserved.

If FX loading or conversion fails for a row that has a mapped monetary field, the row is marked `failed` with `translation_error` and `raw_payload_en` remains `NULL`. We should not produce an English payload with mixed or unconverted currency.

`capital` output shape:

```json
{
  "amount": 2843.48,
  "currency": "USD",
  "amount_usd_cents": 284348,
  "original_amount": 30000.00,
  "original_currency": "NOK",
  "exchange_rate": {
    "source": "ECB",
    "rate_date": "2026-05-21",
    "source_currency": "NOK",
    "target_currency": "USD",
    "source_rate_per_eur": 11.5000,
    "target_rate_per_eur": 1.0900
  },
  "shares": 100,
  "type": "Share capital"
}
```

### Always LLM (free text, unique per company)

| Norwegian field | English key | Notes |
|---|---|---|
| `aktivitet[]` | `activities[]` | array of activity descriptions |
| `vedtektsfestetFormaal[]` | `statutory_purpose[]` | array of statutory purpose lines |
| `frivilligMvaRegistrertBeskrivelser[]` | `vat_descriptions[]` | array |

If any of these arrays are empty or absent, the field is included as an empty array in `raw_payload_en` — no LLM call needed for that field.

### `raw_payload_en` example output

```json
{
  "organization_number": "831909242",
  "name": "CERI HOLDING AS",
  "website": null,
  "founded_date": "2023-11-01",
  "registered_date": "2024-01-04",
  "is_bankrupt": false,
  "is_under_liquidation": false,
  "is_forced_dissolution": false,
  "is_in_group": false,
  "in_vat_register": false,
  "in_business_register": true,
  "has_registered_employees": false,
  "last_annual_report_year": "2025",
  "organization_form": { "code": "AS", "description": "Limited company" },
  "sector_code": { "code": "2100", "description": "Private limited companies" },
  "industry_code_1": { "code": "00.000", "description": "Unspecified" },
  "industry_code_2": null,
  "industry_code_3": null,
  "activities": ["Holding company."],
  "statutory_purpose": ["Own and manage investments."],
  "vat_descriptions": [],
  "capital": {
    "amount": 2843.48,
    "currency": "USD",
    "amount_usd_cents": 284348,
    "original_amount": 30000.00,
    "original_currency": "NOK",
    "exchange_rate": {
      "source": "ECB",
      "rate_date": "2026-05-21",
      "source_currency": "NOK",
      "target_currency": "USD",
      "source_rate_per_eur": 11.5000,
      "target_rate_per_eur": 1.0900
    },
    "shares": 100,
    "type": "Share capital"
  },
  "business_address": {
    "street": ["Storengveien 50D"],
    "city": "STABEKK",
    "municipality": "BÆRUM",
    "municipality_number": "3201",
    "postal_code": "1368",
    "country_code": "NO",
    "country": "Norge"
  },
  "postal_address": null
}
```

---

## Translation Workflow: `TranslateBrregRawInputs`

### Input contract

```go
type TranslateBrregInput struct {
  IDs           []string `json:"ids,omitempty"`     // empty = all pending
  PromptVersion string   `json:"prompt_version"`    // e.g. "v1"
  Model         string   `json:"model"`             // e.g. "qwen3:6b"
  Accumulated   int      `json:"accumulated"`       // rows translated so far (ContinueAsNew)
  FXRateDate    string   `json:"fx_rate_date"`      // optional YYYY-MM-DD; empty = latest official rate
}

type TranslateBrregBatchResult struct {
  Claimed    int `json:"claimed"`     // rows locked in this batch
  Translated int `json:"translated"`  // rows where raw_payload_en was written
  Failed     int `json:"failed"`      // rows where translation_status was set to failed
}
```

### Workflow logic

```
TranslateBrregRawInputs(input):
  workflowRunID = workflow.GetInfo(ctx).WorkflowExecution.RunID
  pagesThisRun = 0
  loop:
    result = TranslateBrregBatch(input.IDs, input.PromptVersion, input.Model, input.FXRateDate, workflowRunID, batchSize=50)
    accumulated += result.Translated
    if result.Claimed == 0: break             // nothing left to work on
    if input.IDs is non-empty: break          // specific IDs → single pass only
    pagesThisRun++
    if pagesThisRun >= 50:
      ContinueAsNew with accumulated updated  // bound history to 50 batches
  // done
```

Exit condition is `claimed == 0`, not `translated == 0`. If a batch is all failures (`claimed=50, translated=0, failed=50`), the workflow continues because there may be more pending rows; it stops only when the claim query returns nothing. This prevents a batch of bad data from masking remaining work.

No `source_process` enqueue — processing is always triggered manually by the operator. No `MarkExecutionComplete` call — this workflow has no corpscout run ID. The `translation_status` column on each row is the progress record.

### `TranslateBrregBatch` activity

1. **Claim rows**:
   - All-pending path: `WHERE translation_status = 'pending' OR (translation_status = 'translating' AND (translation_lease_until < now() OR translation_lease_by = $workflow_run_id))`
   - Specific-IDs path: `WHERE (translation_status IN ('pending', 'failed') OR (translation_status = 'translating' AND (translation_lease_until < now() OR translation_lease_by = $workflow_run_id))) AND id = ANY($ids)`

   Both paths reclaim stale leases inline and let the same Temporal workflow run reclaim its own active lease during an activity retry. The workflow passes `workflow.GetInfo(ctx).WorkflowExecution.RunID` to the activity as `$workflow_run_id`. Use `FOR UPDATE SKIP LOCKED LIMIT 50`. Immediately mark claimed rows `translating`, set `translation_lease_by = <workflow_run_id>`, `translation_lease_until = now() + 10 minutes`, and increment `translation_attempts`. Return `Claimed = len(rows)`; exit early with `Claimed = 0` if the query returns nothing.

2. **Extract translatable strings and monetary values**: for each row, build three collections:
   - `cached_lookups`: map of `{category → []unique_text}` for enum-like fields
   - `llm_required`: map of `{row_id → []unique_text}` for free-text fields
   - `monetary_values`: mapped monetary fields requiring USD conversion, starting with `kapital.belop` / `kapital.valuta`

3. **Load official FX rates when needed**: if any claimed row has mapped monetary values, load official ECB rates once per activity attempt, or from a short-lived in-process cache. If `FXRateDate` is set, use the official rate for that date; if that date is unavailable or historical lookup is not implemented, fail validation before translating rows. If `FXRateDate` is empty, use the latest ECB daily feed. The activity records the effective source (`ECB`) and rate date on rows where FX conversion is used.

4. **Load cache**: use the full primary key for all lookups:
   ```sql
   SELECT category, original_hash, translated_text
   FROM translation_cache
   WHERE source_lang = $source_lang
     AND target_lang = $target_lang
     AND prompt_version = $prompt_version
     AND model = $model
     AND (category, original_hash) IN (...)
   ```
   One query for all unique strings in the batch. Cache hits from a different model or prompt version are not used; they will result in a new LLM call and a new cache row keyed to the current (model, prompt_version) pair.

5. **Identify cache misses** per category. Collect all unique strings not in cache for the current (model, prompt_version).

6. **LLM call** (if any misses): send one request per category with cache misses:
   ```json
   {
     "model": "qwen3:6b",
     "messages": [
       { "role": "system", "content": "You are a Norwegian-to-English translator. Return a JSON object where each key is the original Norwegian text and the value is the English translation. Return only the JSON object, no explanation." },
       { "role": "user", "content": "{\"Enkeltpersonforetak\": \"\", \"Aksjeselskap\": \"\"}" }
     ]
   }
   ```
   For free-text fields (`aktivitet`, etc.) send per-row strings grouped together.

7. **Validate LLM response**: parse JSON. If unparseable → fail the activity (Temporal retries). For each sent key: if the LLM returned it, use the translation; if the LLM omitted it → mark that row `failed` with `translation_error = "LLM omitted key: <term>"`. Discard any keys the LLM invented (not in the input) with a warning log — they are not written to cache.

8. **Build `raw_payload_en`** only for rows with complete translation and FX coverage, using structural renames + USD-normalized monetary values + cache hits + new LLM translations. Rows missing a required LLM translation or failing currency conversion are marked `failed` and do not get `raw_payload_en`.

9. **Write in one transaction**:
   - Upsert new translations into `translation_cache` (only for successfully translated strings)
   - For each **successfully translated** row: set `raw_payload_en`, `translation_status = 'translated'`, `translation_model`, `translation_prompt_version`, `translation_fx_source` and `translation_fx_rate_date` when FX conversion was used, `translation_attempts`, `translated_at`, clear `translation_lease_by` and `translation_lease_until`
   - For each **failed** row: set `translation_status = 'failed'`, `translation_error`, `translation_attempts`; clear lease fields; leave `raw_payload_en = NULL`

A `failed` row never has `raw_payload_en` written. It remains `NULL` and is excluded from the processor gate until the operator retries it.

The final write transaction never opens until after all LLM calls complete. The earlier claim step still uses its own short transaction to lease rows.

### Error handling

| Scenario | Behaviour |
|---|---|
| LLM call times out or HTTP error | Activity fails → Temporal retries. The retry may reclaim rows leased by the same workflow run immediately; otherwise rows become available through stale-lease recovery. |
| LLM returns invalid JSON | Activity fails → Temporal retries. The retry may reclaim same-run leases immediately; if attempts are exhausted, rows are reclaimed by a later run's stale-lease path and `translation_attempts` is incremented. |
| LLM omits a key from the response | Mark affected row `failed` with `translation_error = "LLM omitted key: <term>"`; continue with rest of batch — silent Norwegian fallback is not used |
| FX feed load fails | Activity fails → Temporal retries; rows stay leased for same-run retry or stale-lease recovery |
| FX conversion fails for a row | Mark that row `failed` with `translation_error = "FX conversion failed: <currency>"`; leave `raw_payload_en = NULL`; continue with rest of batch |
| Single row fails to build `raw_payload_en` | Mark that row `failed` with `translation_error`; continue with rest of batch |
| All rows in batch fail | Activity returns `claimed=N, translated=0, failed=N`; workflow continues because `claimed > 0` — it stops only when the next claim returns nothing |
| Stale lease (activity crashed mid-batch) | Next batch's claim query reclaims rows where `translation_lease_until < now()`; rows re-enter processing with a fresh lease |

---

## `MarkExecutionComplete` change

Remove the River `source_process` job insertion from `MarkExecutionComplete` entirely. No source enqueues `source_process` automatically — all company processing is manually triggered by the operator. This is an intentional, global change: it applies to Companies House and any future source as well as brreg.

`MarkCompleteParams` requires no `SkipSourceProcess` flag — the behaviour is uniform.

### Manual process trigger: `POST /api/v1/sources/:name/process`

Since automatic enqueuing is gone, the operator needs a first-class UI action for every source. This endpoint inserts a `source_process` River job for the named source:

Request body: none.

Response:
```json
{ "job_id": 12345, "status": "enqueued" }
```

Returns 409 if a `source_process` job for that source is already `pending` or `running`. Returns 404 if the source name is unknown.

**UI placement:** A **Process** button is added to every source detail page (the existing `sources_.$name.tsx` route), regardless of source type. It is disabled and shows "Processing..." while a job is in flight. For brreg it is also disabled until `ready_to_process > 0`, where `ready_to_process` is the count of rows with `translation_status = 'translated' AND processing_status = 'pending'`. The tooltip reads "Translate rows first" when `ready_to_process == 0` and translation `pending > 0`.

---

## `ClaimPendingBrregRawInputs` query change

Add the `raw_payload_en IS NOT NULL` gate with explicit parentheses around the existing processing-status predicate. This is the only change to the processor — no other logic changes.

```sql
WHERE (
    processing_status = 'pending'
    OR (processing_status = 'processing' AND processing_lease_until < now())
)
AND raw_payload_en IS NOT NULL
```

---

## API

### `POST /api/v1/sources/brreg/translate`

Request body (optional):
```json
{ "ids": ["uuid1", "uuid2"], "fx_rate_date": "2026-05-21" }
```

- If `ids` is absent or empty: trigger `TranslateBrregRawInputs` for all pending rows
- If `ids` is present: trigger for those specific rows only
- If `fx_rate_date` is absent or empty: use the latest official ECB daily rate
- If `fx_rate_date` is present: validate `YYYY-MM-DD` and require official rates for that date before starting the workflow; if the date is unsupported or unavailable, return HTTP 400

Response:
```json
{ "workflow_id": "translate-brreg-all", "status": "started" }
```

Workflow IDs:
- All-pending: fixed ID `translate-brreg-all` — Temporal rejects a duplicate `StartWorkflow` if one is already running, the API returns 409.
- Specific IDs: ID `translate-brreg-ids-{unix_timestamp}` — always a fresh run, no deduplication needed since the set is bounded and immediately consumed.

The specific-IDs path also accepts `failed` rows, allowing the operator to retry individual failed translations from the detail page.

### `GET /api/v1/sources/brreg/translation-stats`

Response:
```json
{
  "pending": 983241,
  "translating": 50,
  "translated": 13034,
  "failed": 17,
  "ready_to_process": 12800,
  "total": 996342
}
```

### Existing raw input detail endpoint

Add `raw_payload_en`, `translation_status`, `translation_attempts`, `translation_error`, `translation_model`, `translation_prompt_version`, `translation_fx_source`, `translation_fx_rate_date`, `translated_at` to the response for brreg rows.

### Existing raw inputs list endpoint

Add `translation_status` to list response items. Add `translation_status` as an accepted filter query param.

---

## UI Changes

### Raw inputs list page (brreg source only)

**Table additions:**
- New `Translation` column showing a badge: `Translated` (green) / `Pending` (amber) / `Failed` (red) / `Translating` (blue spinner)
- Filter dropdown alongside existing status filter: "Translation: All / Pending / Translated / Failed"

**Header actions:**
- When `pending > 0`: banner — "N items need translation" with a **Translate All** button
- Row checkboxes: when ≥1 row selected, a **Translate Selected** button appears in the bulk action bar

Clicking either button calls `POST /api/v1/sources/brreg/translate` (with or without IDs), then shows a toast and refreshes the translation stats.

### Raw input detail page (brreg source only)

Below the existing raw payload viewer, add a **Translation** section:

**Stats row**: `translation_status` badge · model · prompt version · FX source/date · attempts · `translated_at` · error message (if failed)

**Payload panels** (two columns side by side):
- Left: **Norwegian (original)** — formatted JSON of `raw_payload`
- Right: **English (translated)** — formatted JSON of `raw_payload_en`, or a placeholder "Not yet translated" with a **Translate** button

The **Translate** button (single row) calls `POST /api/v1/sources/brreg/translate` with `{ "ids": [rowId] }` and polls the row until `translation_status` changes.

### "Move to companies" action (future gating)

When implemented, this action is only enabled for brreg rows where `raw_payload_en IS NOT NULL`. Rows with `translation_status = 'pending'` or `'failed'` show the action as disabled with tooltip "Translate first".

---

## LLM Client (data-pipelines go-worker)

A new `llm` package in `data-pipelines/services/go-worker/llm/client.go` — a lean copy of the corpscout scheduler's `internal/llm/client.go`, adapted for the go-worker's config pattern.

Config via env vars added to `data-pipelines/services/go-worker/.env` (and the server's `.env`):
- `LLM_BASE_URL` (default `http://100.77.62.33:8080`)
- `LLM_MODEL` (default `qwen3:6b`)
- `LLM_PROMPT_VERSION` (default `v1`)

The client exposes one method relevant here:

```go
// TranslateMap sends a map of {Norwegian: ""} to the LLM and returns {Norwegian: English}.
// Returns error if the response is not valid JSON or if any returned key was not in the input.
func (c *Client) TranslateMap(ctx context.Context, category string, inputs map[string]string) (map[string]string, error)
```

---

## FX Rates Client (data-pipelines go-worker)

Add a small `fxrates` package to `data-pipelines/services/go-worker`, based on the existing corpscout scheduler `internal/fxrates` package.

The package exposes:

```go
type Rates struct { ... }

func Load(ctx context.Context) (*Rates, error)              // latest official ECB daily reference feed
func LoadForDate(ctx context.Context, date time.Time) (*Rates, error) // optional historical rate support
func (r *Rates) ToUSDCents(amount float64, currency string) (int64, error)
func (r *Rates) RateDate() time.Time
```

The first implementation may support latest-rate conversion only if historical ECB lookup is not already available locally. If `TranslateBrregInput.FXRateDate` is supplied before historical lookup exists, the trigger endpoint returns HTTP 400 before starting the workflow rather than silently using a different date.

---

## Translation Prompt

**Prompt version: `v1`**

```
System:
You are a Norwegian-to-English business translator. You will receive a JSON object where
each key is a Norwegian business term and each value is an empty string. Return the same
JSON object with each value filled in with the accurate English translation. Return only
the JSON object — no explanation, no markdown, no extra keys.

User:
{"Enkeltpersonforetak": "", "Aksjeselskap": "", "Elektrisk installasjonsarbeid": ""}
```

Expected response:
```json
{"Enkeltpersonforetak": "Sole proprietorship", "Aksjeselskap": "Limited company", "Elektrisk installasjonsarbeid": "Electrical installation work"}
```

---

## Testing

| Test | What it verifies |
|---|---|
| `TestBuildRawPayloadEn_KeyMapping` | All Norwegian keys produce correct English keys; no unknown keys in output; omitted Norwegian-only fields are absent |
| `TestBuildRawPayloadEn_LegalNameNotTranslated` | `navn` value is copied verbatim |
| `TestBuildRawPayloadEn_BooleansAndDatesPassThrough` | Booleans, dates, numbers, codes copied as-is |
| `TestBuildRawPayloadEn_CapitalConvertedToUSD` | `kapital.belop`/`kapital.valuta` produces USD `capital.amount`, `capital.amount_usd_cents`, original amount/currency, and exchange-rate metadata |
| `TestBuildRawPayloadEn_FXFailureMarksRowFailed` | FX conversion failure leaves `raw_payload_en = NULL` and marks the row failed |
| `TestExtractTranslatableStrings` | Only `aktivitet`, `vedtektsfestetFormaal`, etc. collected for LLM; enum fields collected for cache |
| `TestTranslateBrregBatch_CacheHitSkipsLLM` | When all strings are in cache (full key match), no LLM call is made; row marked `translated` |
| `TestTranslateBrregBatch_CacheKeyMismatch_TreatedAsMiss` | Cache row for same text but different model/prompt_version is not used |
| `TestTranslateBrregBatch_CacheMissSendsToLLM` | Cache miss strings are sent to LLM; results stored in cache keyed by full (model, prompt_version) |
| `TestTranslateBrregBatch_LLMInventedKeyDiscarded` | Key in LLM response not in input is silently dropped |
| `TestTranslateBrregBatch_InvalidLLMJSONFails` | Non-JSON LLM response causes activity error |
| `TestTranslateBrregBatch_LLMOmitsKey_MarksRowFailed` | LLM omitting a key marks that row `failed` with error; raw_payload_en stays NULL; other rows in batch succeed |
| `TestTranslateBrregBatch_StructuralOnlyRow_MarkedTranslated` | Row with no enum or free-text fields is still marked `translated` |
| `TestTranslateBrregBatch_SetsLease` | Claimed rows have `translation_lease_by` and `translation_lease_until` set |
| `TestTranslateBrregBatch_ReclaimsStaleRows` | Rows with `translating` + expired lease are included in the next claim |
| `TestTranslateBrregBatch_ReclaimsSameWorkflowLease` | A Temporal activity retry can reclaim rows leased by the same workflow run before lease expiry |
| `TestTranslateBrregBatch_ClearsLeaseOnSuccess` | Written rows have `translation_lease_by = NULL` and `translation_lease_until = NULL` |
| `TestTranslateBrregBatch_ReturnsCounts` | Result struct has correct `claimed`, `translated`, `failed` values |
| `TestTranslateBrregBatch_LoadsFXOncePerAttempt` | Batch translation loads official exchange rates once and reuses them for all rows in the activity attempt |
| `TestClaimPendingBrregRawInputs_ExcludesNullPayloadEn` | Processor query returns zero rows when `raw_payload_en IS NULL` |
| `TestClaimPendingBrregRawInputs_PreservesProcessingRetryGate` | Processor query keeps the original pending/stale-processing status logic inside parentheses before applying `raw_payload_en IS NOT NULL` |
| `TestTranslateBrregWorkflow_StopsWhenClaimedIsZero` | Workflow exits cleanly when batch returns `claimed=0` |
| `TestTranslateBrregWorkflow_ContinuesWhenAllBatchFailed` | Workflow continues if `claimed > 0` even when `translated=0, failed=N` |
| `TestTranslateBrregWorkflow_SpecificIDsSinglePass` | IDs-only run does not loop or ContinueAsNew |
| `TestMarkExecutionComplete_NoRiverJobEnqueued` | `MarkExecutionComplete` completes without inserting any River job |
| `TestProcessSourceHandler_EnqueuesJob` | `POST /api/v1/sources/:name/process` inserts a `source_process` River job |
| `TestProcessSourceHandler_409WhenAlreadyRunning` | Returns 409 if a job for that source is already pending or running |
| `TestBrregTranslationStats_ReadyToProcess` | `ready_to_process` counts translated rows that still have `processing_status = 'pending'` |
