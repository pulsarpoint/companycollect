# SE company-info field values — Design Spec

Approved in chat 2026-09-01 (owner). Replaces the SE company-info correction ledger
(`corpscout.se_company_info_correction`: kinds override_field / approve_suggestion /
reject_suggestion / undo, evidence-hash staleness, kind-ranked precedence,
`liveOverrideRefusal`) with one plain rule: **a field's live value is the latest row
written for it.** The old table is dropped. The ADDRESS ledger
(`se_company_address_correction`) and the PERSON ledger are separate and unchanged.

## Why

Reviewers decide values, not approvals. The old ledger modelled decisions as kinds
bound to an evidence hash, ranked by kind then time, with an "undo" supersession chain
and a UI guard for dead writes. Dagster never approved anything — it only batch-applied
the decisions — so the state machine bought complexity without semantics. Prod check
2026-09-01: the old table held 4 rows and **zero** of 3.5M published rows applied a
correction, so nothing is lost by dropping it.

## The rule

`corpscout.se_company_info_field_value` — append-only history; the **live value per
(company_id, field) is the row with the greatest (created_at, value_id)**. A row whose
`value` is NULL *releases* the field back to the pipeline's computed default. The
published row `corpscout.se_company_info` = per field: live value if one exists (and
is non-NULL) else the pipeline default. Full history stays queryable; no staleness, no
kinds, no ranking, no undo (undo = write the previous value, or NULL).

## Table

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_field_value
(
    value_id    UUID,
    company_id  String,
    field       LowCardinality(String),        -- 'description' | 'description_sv'
    value       Nullable(String),              -- NULL = release to pipeline default
    source      LowCardinality(String),        -- scb | esef | wikidata | llm | reviewer
    source_ref  String,                        -- source_record_uid, or suggestion_id for llm; '' for reviewer
    source_at   Nullable(DateTime64(3, 'UTC')),-- the artifact's observed_at / the suggestion's created_at
    decided_by  String,
    note        String,
    created_at  DateTime64(3, 'UTC'),

    CONSTRAINT has_company  CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT known_field  CHECK field IN ('description', 'description_sv'),
    CONSTRAINT known_source CHECK source IN ('scb', 'esef', 'wikidata', 'llm', 'reviewer')
)
ENGINE = MergeTree
ORDER BY (company_id, field, created_at, value_id);

GRANT INSERT ON corpscout.se_company_info_field_value TO corpscout_person_correction_writer;

```
Down: drop the new table, revoke the grant.

The old ledger is NOT dropped by this migration (revised 2026-09-02 at whole-branch review, per the
2026-08-25 ruling that a DROP which must wait for a deploy never enters the sequential ledger: a routine
`migrate up` for unrelated work would apply it before the code that stops reading the table is live).
Retirement is a deploy-time step after backoffice + dagster run the new code: re-verify the gate
(`SELECT countIf(length(correction_ids) > 0) FROM corpscout.se_company_info` = 0; verified 2026-09-01 with
4 ledger rows), `DROP TABLE corpscout.se_company_info_correction` as direct SQL, then write the
retirement migration (`DROP TABLE IF EXISTS`, down recreates the 000297+000299 DDL empty) with the
then-next-free number so other environments follow. UNDROP window is ~480s.
Migration number = max(existing)+1 at execution (000371 at execution: main took 000368-000370 for Finland XBRL); ledger entry + content test.

## Dagster

- `common.py` (shared with address): `build_ledger_cursor_sql`, `build_touched_companies_sql`,
  `ledger_sensor` gain `id_column: str = "correction_id"` (default preserved → address/person unchanged).
- `info_rules.py`: delete `INFO_KIND_ORDER`, `ZERO_HASH`, `apply_info_ledger`, `InfoOutcome.stale_correction_ids`.
  Keep `evidence_set_hash_for`, `ArtifactRow`, `_text` (address imports them). Add:
  ```python
  @dataclass(frozen=True)
  class FieldValueRow:
      value_id: uuid.UUID; company_id: str; field: str; value: str | None
      source: str; source_ref: str; created_at: datetime

  def apply_field_values(outcome: InfoOutcome, rows: Sequence[FieldValueRow], *,
                         stored: Sequence[StoredObservation]) -> InfoOutcome:
  ```
  Per field, the live row = max (created_at, value_id). `description` live & non-NULL →
  `description=_text(value)`, `needs_model=False`, provenance by source: `llm` →
  `llm_enhanced=True`, `suggestion_id=UUID(source_ref)` when parseable, `model_provider/
  model_name/prompt_version` copied from that stored observation when present else
  (`"llm"`, `"field-value:llm"`, `""`); any other source → `llm_enhanced=False`,
  `suggestion_id=None`, `model_provider="deterministic"`, `model_name=f"field-value:{source}"`,
  `prompt_version=""`. `description_sv` live & non-NULL → `description_sv=_text(value)` only.
  `correction_ids` = ids of the live rows applied (kept name: "applied field-value ids").
  Malformed rows (unknown field/source) are skipped and counted in a new `invalid_value_count`.
- `info.py`: `SE_COMPANY_INFO_FIELD_VALUE = "se_company_info_field_value"`; read with a new
  `build_field_values_sql` (company_ids IN, ordered); the change scan's `ledger` CTE reads the
  new table (alias `latest_correction_at` and reason `ledger_pending` KEPT — the backoffice
  pipeline SQL mirrors them); `PENDING_MODEL_SQL` unchanged; the approval-keeping
  `input_hash` recomputation for model-off runs is removed (nothing needs it now — verify no
  other consumer); metrics: drop `stale_correction_count` + its log, add `invalid_value_count`;
  `assert_clickhouse_tables_exist` lists the new table; sensor renamed
  `se_company_info_field_value_sensor` (`ledger_sensor(..., id_column="value_id")`).
- Published table `se_company_info` schema unchanged (`correction_ids` reused; `evidence_*` stay
  as artifact provenance).

## Backoffice

- New client-safe `app/lib/se-info-field-values.ts`: `SE_INFO_FIELDS`, `SE_INFO_VALUE_SOURCES`,
  types, `validateSeInfoFieldValue(input) → draft` (company id regex; field/source enums; `value`
  trimmed non-empty string or null; `source_ref`: UUID for llm, non-empty for scb/esef/wikidata,
  '' for reviewer; note ≤ 1000).
- `se-company-info.server.ts`: `FIELD_VALUES_SQL` (history per company, newest first, `is_live`
  per field via argMax), `appendSeCompanyInfoFieldValues(inputs[])` (validate all, keep the
  "company is published" check, one insert), `SeCompanyInfoDetail.fieldValues` replaces
  `.corrections`; `clickhouse.server.ts`: `chInsertSeCompanyInfoFieldValues` (table
  `se_company_info_field_value`) replaces the correction wrapper.
- `se-info-review-form.ts` → `se-info-field-value-form.ts`: `buildFieldValueInputs(form, {companyId})`
  for intents `use-source` (field, value, source, source_ref, source_at), `use-suggestion`
  (suggestion_id → rows for description and, when present, description_sv, source `llm`),
  `edit` (reviewer rows for changed description / description_sv; a ticked `clear_*` box writes
  NULL = release; unchanged fields write nothing; "Nothing changed." refusal kept), `release`
  (field → NULL row).
- Route `admin-se-company-info.tsx`: dispatch on `intent`; returns `{ok:true, valueIds}` /
  `{ok:false, error}`; copy "Saved — published on the next rebuild".
- Workspace: About card gains **Use this** on every source option (writes the field matching the
  language shown: en → description, original → description_sv; wikidata → description) and
  **Edit** on Final (inline EN/SV textareas + clear boxes + note). Model-suggestions card:
  approve/reject forms → one **Use this suggestion**. The Corrections section becomes a
  **Value history** card (rows: field, source, value or "released", source_ref, decided_by ·
  created_at, live badge) with a **Release to pipeline** button per field. Delete `HiddenCommon`,
  `liveOverrideRefusal` wiring, undo forms, stale/applied badges, "Correction ids" row.
- Delete the repo-wide corrections page: route `se/company-info/corrections`, its table
  component, the ledger half of `se-company-info-lists.server.ts`, the three entry links and
  the breadcrumb branch. KEEP the shared filter vocabulary (`SE_INFO_CORRECTION_KINDS/STATUSES`,
  `parseCorrectionFilters`, `SeCompanyInfoCorrectionsFilterSheet`, …) — the address ledger uses it;
  move their direct test coverage into a standalone test file before deleting the page's test.
- `se-company-info-pipeline.server.ts`: the `ledger` CTE reads the new table (strings otherwise kept);
  `dagster.server.ts`: `SE_COMPANY_INFO_SENSOR = "se_company_info_field_value_sensor"`.

## Testing

Dagster: rewrite the ledger cases in `test_se_company_info_rules.py` as field-value cases (latest
wins, NULL releases, per-field independence, llm provenance from stored observation, invalid rows
skipped+counted); update `test_se_company_info.py` (scan SQL, model-off run keeps a field value,
sensor name, EXISTING_TABLES), `test_se_company_info_clickhouse_local.py` (NEEDED_TABLES),
`test_se_company_layout.py` (new table DDL/grant; old-table assertions retargeted), migration
ledger + content test. Backoffice: new unit tests for validation and form building; rewrite the
listed cases in `admin-se-company-info.test.tsx`, `se-company-info.server.test.ts`,
`clickhouse-writer.server.test.ts`, `se-company-info-pipeline.server.test.ts`,
`dagster.server.test.ts`, `admin-se-company-area.test.tsx`, `admin-se-company-info-pipeline-sheet.test.tsx`;
delete `se-info-corrections.test.ts`, `se-info-review-form.test.ts`; split
`admin-se-company-info-corrections.test.tsx` (keep the shared-filter describes).

## Deploy order

Backoffice and Dagster both reference the new table, so: apply migration (with the gate) →
deploy dagster_v3 (pristine worktree) → start `se_company_info_field_value_sensor` in the UI
(the old sensor disappears with the code) → backoffice picks up the merge (local dev server).

## Out of scope

Address/person ledgers; a repo-wide field-value history page; snapshot-vs-follow hints
("SCB has newer text") on the About card.
