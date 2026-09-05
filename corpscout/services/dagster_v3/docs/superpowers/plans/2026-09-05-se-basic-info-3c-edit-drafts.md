# SE Basic Info Slice 3c: Edit With Drafts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The reviewer can type a value for any basic-info field into a per-kind form; it lands as a draft (a `reviewer_draft` suggestion row the fold ignores) until activated per field into the active reviewer row, which then outranks every source and rule; Reset to default also clears typed values.

**Architecture:** No migration. Dagster: the reviewer's precedence becomes 20000, `reviewer_draft` is excluded where `reviewer` is (LLM gate) and from the change watermark. Backoffice: three new intents (`edit`, `activate`, `discard`) parsed client-side and written by the server module as suggestion-row versions (`reviewer_draft` / `reviewer`), `reset` extended to clear the active reviewer value, a right-side Sheet with a per-kind form, a "Draft" row in the panel with Activate/Discard, and a draft marker on the left card.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`, tests need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`), React Router 8 + shadcn/ui (`Sheet`, `Select`, `Textarea`, `Input`), vitest.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`, sections 3.2, 4 and 7 as amended 2026-09-05 (slice 3c).

## Global Constraints

- Dagster work in `corpscout/services/dagster_v3`; backoffice work in `corpscout/services/backoffice` (`npx vitest run <files>`, `npm run typecheck`).
- `BASIC_INFO_PRECEDENCE[field]["reviewer"] == 20000` for every folded field; every other number unchanged; `SOURCES` gains `"reviewer_draft"` last; `precedence_for(field, "reviewer_draft")` is None for every field (never in a map).
- LLM gate: both `source NOT IN ('llm', 'reviewer')` become `source NOT IN ('llm', 'reviewer', 'reviewer_draft')`. Change watermark: `suggestion_watermarks_sql()` adds `AND source != 'reviewer_draft'` so a saved draft does not wake a company; the suggestion read itself is unchanged (an unranked source can never win).
- Suggestion-row versions the backoffice writes (both sources): `company_id`, `source` (`reviewer_draft` or `reviewer`), `source_record_uid = ''`, `observed_at` = `suggested_at` = the decision instant (`YYYY-MM-DD HH:MM:SS.mmm` UTC), the nine value columns (`''` from reads becomes NULL), `decided_by = 'backoffice'`, `note` (NULL when empty), `source_run_id = 'backoffice'`, `extractor_version = 'backoffice-v1'`. A new version always starts from the current row of the same source (FINAL) and changes one field (`description` carries `description_language`).
- Validation per field kind (client parser and server agree): `legal_name` non-empty text ≤ 500; `legal_form_code` one of the SCB codes offered (`^[0-9]{2}$` and present in the options the loader supplied); `status` `active` or `inactive`; `incorporation_date` `YYYY-MM-DD`, between 1800-01-01 and today; `lei` `^[A-Z0-9]{20}$` (upper-cased); `wikidata_id` `^Q[0-9]+$`; `description` and `description_sv` non-empty text ≤ 8000, plain text; `description` also carries `language` ∈ {`en`, `sv`}. Values are trimmed; an empty value is refused ("Value cannot be empty.") since Discard is the way to clear.
- Intents: `edit {field, value, language?, note}` writes the draft; `activate {field, note}` inserts a reviewer version with the field copied from the draft (refused when the draft has no value for it) AND a draft version with the field cleared; `discard {field}` clears the field in the draft (refused when it has none); `reset {field, note}` retires the field's rules (as today) AND, when the active reviewer row has a value for the field, inserts a reviewer version with it NULL. All in one action round-trip; suggestion-row inserts go through `chInsertSeBasicInfoSuggestions` (re-added), rule rows through `chInsertSeBasicInfoPrecedence`.
- Catalogue: `BASIC_INFO_SOURCES` gains `"reviewer_draft"` last with label `Draft`; the panel never offers "Use this" on `reviewer` or `reviewer_draft` rows.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` then `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`.

---

## File structure

| File | Responsibility |
| --- | --- |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/precedence.py` (modify) | reviewer 20000, `SOURCES` + `reviewer_draft` |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/llm.py` (modify) | gate excludes `reviewer_draft` |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/batch.py` (modify) | watermark ignores `reviewer_draft` |
| `dagster_v3/tests/test_se_company_basic_info_{precedence,fold,llm,batch}.py` (modify) | pins |
| `backoffice/app/lib/se-basic-info-fields.ts` (modify) | `reviewer_draft` token, `validateSeBasicInfoValue(field, value, language)` |
| `backoffice/app/lib/se-basic-info-decision-form.ts` (modify) | `edit`, `activate`, `discard` intents |
| `backoffice/app/lib/clickhouse.server.ts` (modify) | `chInsertSeBasicInfoSuggestions` (re-added) |
| `backoffice/app/lib/se-basic-info.server.ts` (modify) | `legalFormOptions` in the detail, `appendSeBasicInfoDraft`, `activateSeBasicInfoDraft`, `discardSeBasicInfoDraft`, `reset` clearing the reviewer value |
| `backoffice/app/components/admin/se-basic-info-edit-sheet.tsx` (new) | the Sheet with the per-kind form |
| `backoffice/app/components/admin/se-basic-info-workspace.tsx` (modify) | Edit buttons, Draft row with Activate/Discard, "typed by reviewer", left-card draft marker |
| `backoffice/app/routes/admin-se-company-info.tsx` (modify) | routes the three intents |
| `backoffice/tests/{se-basic-info-fields,se-basic-info-decision-form,se-basic-info.server}.test.ts`, `tests/admin-se-company-basic-info.test.tsx`, `tests/se-basic-info-edit-sheet.test.tsx` (new) | tests |

---

### Task 1: Dagster -- reviewer 20000, `reviewer_draft` ignored

**Files:** `precedence.py`, `llm.py`, `batch.py`; tests `test_se_company_basic_info_precedence.py`, `test_se_company_basic_info_fold.py`, `test_se_company_basic_info_llm.py`, `test_se_company_basic_info_batch.py`.

- [ ] **Step 1: Write the failing tests.** In the precedence test, change every `10000` for the reviewer to `20000` (both the per-field map pins and `test_every_folded_field_has_a_map_and_the_reviewer_tops_each`, which asserts `by_source["reviewer"] == 20000` and `max(...) == 20000`), the export-order pin (`("legal_name", "reviewer", 20000)`), and add `assert precedence_for(field, "reviewer_draft") is None` for every folded field plus `assert SOURCES[-1] == "reviewer_draft"`. In the fold test add `test_a_draft_row_never_supplies_a_field`: a `reviewer_draft` suggestion with `legal_name="Draft AB"` next to an scb row -> the fold takes scb; and `test_an_activated_reviewer_value_beats_a_company_rule`: reviewer `status="dormant"` vs a rule `{"status": {"bolagsverket": 10000}}` with bolagsverket `status="inactive"` -> `("dormant", "reviewer")`. In the llm test, pin both gate lines contain `source NOT IN ('llm', 'reviewer', 'reviewer_draft')`. In the batch test, pin `suggestion_watermarks_sql()` contains `WHERE company_id IN %(company_ids)s AND source != 'reviewer_draft'`.
- [ ] **Step 2: Run** `... pytest tests/test_se_company_basic_info_precedence.py tests/test_se_company_basic_info_fold.py tests/test_se_company_basic_info_llm.py tests/test_se_company_basic_info_batch.py -q` -> FAIL.
- [ ] **Step 3: Implement**: `precedence.py` (`"reviewer": 20000` in all eight maps, docstring "the reviewer ... ranked above every automated one and above company rules (10000)", `SOURCES = (..., "llm", "reviewer", "reviewer_draft")`); `llm.py` lines 111-112; `batch.py` `suggestion_watermarks_sql` with the extra predicate (keep the exact text the test pins). Nothing else: an unranked source is already skipped by `_winner`.
- [ ] **Step 4: Run** the four files and the whole `tests/test_se_company_basic_info_*.py` set -> PASS; `uv run --frozen --no-sync dg check defs` -> loads.
- [ ] **Step 5: Commit** (`feat(dagster): SE basic-info reviewer ranks 20000 and drafts never fold`).

---

### Task 2: Catalogue, validation and the parser (client-safe)

**Files:** `app/lib/se-basic-info-fields.ts`, `app/lib/se-basic-info-decision-form.ts`; tests `tests/se-basic-info-fields.test.ts`, `tests/se-basic-info-decision-form.test.ts`.

**Interfaces:**
- `BASIC_INFO_SOURCES = ["reviewer", "llm", "scb", "bolagsverket", "esef", "wikidata", "ratsit", "reviewer_draft"]`, label `Draft`; `isBasicInfoSource` accepts it.
- `BASIC_INFO_STATUSES = ["active", "inactive"]`, `BASIC_INFO_LANGUAGES = ["en", "sv"]`.
- `validateSeBasicInfoValue(field, value, language, options): { ok: true; value: string; language: string } | { ok: false; error: string }` where `options = { legalFormCodes: readonly string[]; today: string }` and the rules are the Global Constraints' (messages: "Value cannot be empty.", "Value is longer than N characters.", "Legal form must be one of the SCB codes.", "Status must be active or inactive.", "Date must be YYYY-MM-DD.", "Date must be between 1800-01-01 and today.", "LEI must be 20 letters or digits.", "Wikidata id must be Q followed by digits.", "Language must be en or sv."). `language` is only used for `description` (returned `''` otherwise).
- Decision union gains `{ intent: "edit"; field; value; language; note }`, `{ intent: "activate"; field; note }`, `{ intent: "discard"; field }`; the parser validates `edit` through `validateSeBasicInfoValue` with `legalFormCodes` read from a hidden `legal_form_codes` form field (comma-separated, sent by the sheet) so the client-safe parser needs no server data; `activate`/`discard` need only the field.

- [ ] **Step 1: Tests** for each rule and intent (one `it` per rule, table-style), including trimming and LEI upper-casing.
- [ ] **Step 2: Run** the two files -> FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** -> PASS; `npm run typecheck` -> clean.
- [ ] **Step 5: Commit** (`feat(backoffice): basic-info value validation and the edit, activate, discard intents`).

---

### Task 3: Server writes

**Files:** `app/lib/clickhouse.server.ts`, `app/lib/se-basic-info.server.ts`; test `tests/se-basic-info.server.test.ts`.

**Interfaces:**
- `chInsertSeBasicInfoSuggestions<T extends object>(values: T[])` (table `se_company_basic_info_suggestion`, write client, JSONEachRow, no-op on empty).
- `SeBasicInfoDetail.legalFormOptions: { code: string; label_sv: string; label_en: string }[]` from `BASIC_INFO_LEGAL_FORM_OPTIONS_SQL` = `SELECT l.code AS code, argMax(l.label_sv, l.version) AS label_sv, argMax(l.label_en, l.version) AS label_en FROM corpscout.se_code_labels AS l WHERE l.code_type = 'legal_form' AND match(l.code, '^[0-9]+$') GROUP BY l.code ORDER BY l.code` (32 rows today), loaded with the other reads.
- `suggestionRowVersion(current: SeBasicInfoSuggestionRow | undefined, source, changes: Partial<values>, note, stamp)` builds the 18-key insert object (values from the current row, `''` -> NULL, changes applied).
- `appendSeBasicInfoDraft(companyId, { field, value, language, note }, now)` -> `{ decidedAt }`: reads the company's suggestion rows, builds a `reviewer_draft` version with the field set (+ `description_language` when field is `description`), inserts it.
- `activateSeBasicInfoDraft(companyId, { field, note }, now)` -> `{ decidedAt }`: reads rows; refuses `SeBasicInfoDecisionError("No draft value for <field label> to activate.")` when the draft has none; inserts, in ONE `chInsertSeBasicInfoSuggestions` call, the `reviewer` version with the field (and language) copied and the `reviewer_draft` version with it cleared.
- `discardSeBasicInfoDraft(companyId, { field }, now)`: refuses when no draft value; inserts the draft version with the field (and language) NULL, note "discarded".
- `appendSeBasicInfoRule` `reset`: after retiring the rules, if the active `reviewer` row has a value for the field, also insert a `reviewer` version with it NULL (note "reset to default"); the rule retirement and the reviewer clearing are separate inserts (two tables) in that order; when the field has neither a rule nor a reviewer value, refuse "No company rule or reviewer value to reset for <field>.".

- [ ] **Step 1: Tests**: SQL pin for the options query and its presence in `loadSeBasicInfoDetail`; each write's inserted rows (all 18 keys, NULLs where expected, the description language coupling); the refusals; reset's two inserts and its refusal message.
- [ ] **Step 2: Run** -> FAIL. **Step 3: Implement.** **Step 4: Run** the server test + typecheck -> PASS.
- [ ] **Step 5: Commit** (`feat(backoffice): draft, activate, discard and reset writes for typed reviewer values`).

---

### Task 4: The edit sheet and the panel

**Files:** `app/components/admin/se-basic-info-edit-sheet.tsx` (new), `app/components/admin/se-basic-info-workspace.tsx`, `app/routes/admin-se-company-info.tsx`; tests `tests/se-basic-info-edit-sheet.test.tsx` (new), `tests/admin-se-company-basic-info.test.tsx`.

**Interfaces:**
- `SeBasicInfoEditSheet({ companyId, field, detail, open, onOpenChange, busy })`: shadcn `Sheet side="right"`; header "Edit <field label>"; a block "Published now: <value> (<source>)" and, when a draft value exists, "Draft: <value>"; the form (`<Form method="post">`) with hidden `intent=edit`, `field`, `legal_form_codes` (comma-joined codes from `detail.legalFormOptions`), and the input by kind: `Input` for `legal_name`/`lei`/`wikidata_id`; `Select` over `legalFormOptions` (option text `legalFormOptionLabel` from `~/lib/se-legal-form`) for `legal_form_code`; `Select` active/inactive for `status`; `Input type="date"` for `incorporation_date`; `Textarea rows=8` for the descriptions plus a `Select` en/sv (`language`) for `description`; the note `Input`; footer Cancel / "Save draft". The form body is exported as `SeBasicInfoEditForm` (portal-free) for static tests, like `DecisionDialogBody`.
- Workspace: every left-card row gets an "Edit" button (opens the sheet for that field, `?field=` unchanged); rows with a draft value show a "draft" outline badge; the panel lists a "Draft" row (source `reviewer_draft`) only when it has a value for the selected field, at the bottom, with the value, "Activate" and "Discard" buttons (each opens the existing confirmation dialog with intent `activate` / `discard`; the dialog's copy: "Activate the draft for this field: the reviewer's value then outranks every source and rule at the next fold." / "Discard the draft value for this field."); the active `reviewer` row with a value shows "typed by reviewer" and never "Use this"; `PendingDecision.intent` gains `activate` | `discard`.
- Route action: `edit` -> `appendSeBasicInfoDraft`, `activate` -> `activateSeBasicInfoDraft`, `discard` -> `discardSeBasicInfoDraft`, results `{ ok: true, decidedAt }`; refusals as today.

- [ ] **Step 1: Tests**: sheet form renders the right control per kind (a select with 32 codes for legal form, the date input, the textarea + language select for description), the hidden intent/field/codes; workspace shows Edit buttons, the draft badge, the Draft row with Activate/Discard only when the draft has a value, "typed by reviewer" on an active reviewer value; route action maps the three intents.
- [ ] **Step 2: Run** -> FAIL. **Step 3: Implement.** **Step 4: Run** the five test files + typecheck -> PASS.
- [ ] **Step 5: Commit** (`feat(backoffice): edit sheet writes reviewer drafts; activate and discard per field`).

---

### Task 5: Cutover (controller)

1. [ ] Owner merges; dev server picks the backoffice up.
2. [ ] Hot-sync dagster; materialize `se_company_basic_info_precedence_clickhouse` (30 pairs, reviewer rows now 20000: `SELECT count() FROM corpscout.se_company_basic_info_precedence FINAL WHERE company_id = '' AND source = 'reviewer' AND precedence = 20000` = 8).
3. [ ] Smoke on Atlas Copco 5561552760: Edit status -> "dormant" is refused by the select (only active/inactive); Edit legal name -> "Atlas Copco Indoeuropeiska AB (test)" saved as draft -> left badge + Draft row; Discard -> gone; Edit again -> Activate -> Fold now -> legal_name from `reviewer`, history row; Reset to default on legal name -> reviewer value cleared -> Fold now -> back to SCB; a draft on description with language sv; ensure the LLM preview count is unchanged by the draft (`se_basic_info_suggestions_llm` preview `eligible` equals the earlier 78,579 give or take new register rows).
4. [ ] Record in the ledger and spec section 10; archive the ledger.

## Self-review

- Spec coverage: 3.2 (`reviewer_draft` row) Tasks 1-3; 4 (20000, drafts unranked) Task 1; 7 (sheet, per-kind form, Activate/Discard, reset clears values, left-card marker) Tasks 2-4; cutover Task 5.
- Placeholders: Tasks 2-4 describe tests by contract (the files exist with fixtures); interfaces are exact.
- Consistency: the 18-key suggestion insert matches `tables.SUGGESTION_INSERT_COLUMNS` + `suggested_at`, `source_run_id`, `extractor_version` (migration 000376); `PendingDecision.intent` union matches the parser's; `legal_form_codes` hidden field feeds the same validator on both sides.
