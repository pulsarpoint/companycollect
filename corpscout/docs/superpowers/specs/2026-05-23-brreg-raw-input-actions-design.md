# BRREG Raw Input Actions Design

## Summary

BRREG raw input rows need one simple row lifecycle plus independent action
history. Translation, enhancement, and suggestion submission are not all
mutually exclusive row states. A row can be translated and enhanced at the same
time, and enhancement should improve suggestion quality without blocking
suggestion submission.

The row should keep a small `state` that describes the source input's main
business lifecycle. Work performed on the row should be recorded in append-only
action tables. The API and UI should read a current effective view that combines
the row state with the latest action status for each action type.

## Goals

- Preserve a clear row lifecycle for filtering raw inputs and understanding
  whether a row has produced suggestions.
- Track translation, enrichment, and submission as independent actions with
  full status history.
- Support retries without losing the successful output from earlier attempts.
- Allow optional enhancement to run before or after suggestion submission.
- Make UI actions rule-based from `state` plus effective action attributes.
- Keep compatibility with existing `translation_status` and
  `processing_status` while moving user-facing logic away from those columns.

## Non-Goals

- This design does not require every source table to adopt action tables at
  once. BRREG is the pilot.
- This design does not make domain enhancement mandatory before suggestion
  submission.
- This design does not replace River or Temporal task tracking. Raw input
  actions are per-row domain history, while River and Temporal are workflow/job
  execution systems.

## Row Lifecycle

`brreg_company_raw_inputs.state` should describe the business lifecycle of the
raw input row, not every worker activity.

Proposed valid states:

- `input`: imported row that has not reached terminal suggestion handling.
- `suggestion_submitted`: suggestion rows were created from this raw input.
- `completed`: processing finished with no pending suggestion action.
- `superseded`: obsolete row that should no longer be acted on.

`translating`, `enhancing`, and `submitting` should not be row states. They are
statuses of action attempts.

## Action Tables

Use one table for action attempts and one table for status events. The attempt
row identifies what work is being attempted. The event rows record status
changes over time.

```sql
CREATE TABLE brreg_raw_input_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,
    action_type TEXT NOT NULL CHECK (
        action_type IN ('translate', 'enhance', 'submit_suggestion')
    ),
    attempt INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    trigger TEXT,
    worker_id TEXT,
    workflow_id TEXT,
    workflow_run_id TEXT,
    river_job_id BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (raw_input_id, action_type, attempt)
);

CREATE TABLE brreg_raw_input_action_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_id UUID NOT NULL REFERENCES brreg_raw_input_actions(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')
    ),
    message TEXT,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Recommended indexes:

- `(raw_input_id, action_type, attempt DESC)` on `brreg_raw_input_actions`.
- `(action_id, created_at DESC)` on `brreg_raw_input_action_events`.
- Partial queue indexes for latest `queued` or `running` actions if workers
  claim directly from this table.

## Effective Attributes

Expose a DB view or API projection that returns one row per raw input with the
current effective action status for the current `payload_hash`.

Attributes:

- `translation_status`: `notdone | queued | running | succeeded | failed | skipped | cancelled`
- `enhancement_status`: `notdone | queued | running | succeeded | failed | skipped | cancelled`
- `submission_status`: `notdone | queued | running | succeeded | failed | skipped | cancelled`

The effective status should be based on the latest event of the latest action
attempt for that raw input, action type, and current `payload_hash`.

Important rule: a failed retry must not automatically erase a previously usable
successful result. The view should expose both:

- `latest_translation_status`: status of the newest translation attempt.
- `has_successful_translation`: true when a successful translation exists for
  the current `payload_hash`.

The UI can display the latest status, while action rules can use the durable
success flags.

## Materialized Outputs

Action history is the audit trail. The row can still keep materialized outputs
for fast reads and simpler downstream processing:

- `raw_payload_en` remains the materialized translation result.
- Future enhancement output can be stored in source-specific columns or a JSONB
  field such as `enhancement_payload`.
- Suggestion rows remain in the suggestion tables.

The action success event and the materialized output update should be committed
in the same transaction.

## Action Rules

Bulk and row-level actions should be derived from row state plus effective
attributes.

Examples:

```text
if state = input and has_successful_translation = false:
  show Translate

if state = input and latest_translation_status = failed:
  show Retry translation

if state = input and has_successful_translation = true and submission_status != succeeded:
  show Submit suggestions

if state = input and latest_submission_status = failed:
  show Retry submission

if state IN (input, suggestion_submitted) and has_successful_enhancement = false:
  show Enhance

if state = suggestion_submitted:
  show Open review
```

Enhancement is optional and should not block `submit_suggestion`.

## Worker Transactions

Each worker should create or claim an action attempt and then append action
events as it progresses.

Translation success transaction:

1. Write translated payload to `brreg_company_raw_inputs.raw_payload_en`.
2. Preserve compatibility columns such as `translation_status = 'translated'`.
3. Append `brreg_raw_input_action_events.status = 'succeeded'`.
4. Commit.

Translation failure transaction:

1. Preserve compatibility columns such as `translation_status = 'failed'`.
2. Append `status = 'failed'` with a safe error message.
3. Commit.

Suggestion submission success transaction:

1. Insert suggestion rows.
2. Set `brreg_company_raw_inputs.state = 'suggestion_submitted'`.
3. Preserve compatibility columns such as `processing_status = 'processed'`.
4. Append `submit_suggestion` event `status = 'succeeded'`.
5. Commit.

## API and UI

Raw input list/detail responses should include:

- `state`
- effective action attributes
- optional diagnostic legacy fields: `processing_status`, `translation_status`

The raw input table should show:

- main lifecycle column: `state`
- action attribute columns or compact badges: translation, enhancement,
  submission
- filters for lifecycle state and each action attribute

The action sheet should group by actionable buckets:

- Needs translation
- Translation failed
- Ready to submit
- Submission failed
- Needs enhancement
- Enhancement failed
- Submitted for review

Each group count should be computed from the effective action view, not from UI
CASE logic.

## Migration Path From Current State Column

The current BRREG `state` migration can be adapted rather than thrown away:

- Map current `raw`, `translating`, `translated`, `translation_failed`,
  `submitting`, and `submission_failed` rows to row `state = 'input'`.
- Map current `submitted` to `state = 'suggestion_submitted'`.
- Keep `completed` and `superseded`.
- Backfill action attempts/events from existing columns:
  - `translation_status = 'translated'` creates a successful `translate`
    action for the current payload hash.
  - `translation_status = 'failed'` creates a failed `translate` action.
  - `processing_status = 'processed'` with suggestions creates a successful
    `submit_suggestion` action.
  - `processing_status = 'failed'` creates a failed `submit_suggestion`
    action.

Existing compatibility columns stay in place until all code paths use action
tables and the effective view.

## Testing

Database tests:

- Backfill maps old row states to the new lifecycle and action events.
- Effective view returns correct latest status and durable success flags.
- A failed retry does not hide a previous successful output for the same
  payload hash.

Scheduler tests:

- Raw input filtering and sorting use the DB-backed lifecycle/effective view.
- Action sheet counts are based on effective action attributes.
- Suggestion processor commits suggestion rows, row state, and action event in
  one transaction.

Data-pipelines tests:

- Translation claim creates or claims a `translate` action and appends
  `running`.
- Translation success writes `raw_payload_en` and appends `succeeded` in one
  transaction.
- Translation failure appends `failed` in the same transaction as compatibility
  status updates.

UI tests:

- State column remains lifecycle-only.
- Translation/enhancement/submission badges can be filtered independently.
- Bulk actions trigger the correct action bucket.
