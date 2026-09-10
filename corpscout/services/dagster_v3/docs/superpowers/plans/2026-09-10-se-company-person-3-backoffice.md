# SE Company Person Slice 3: Backoffice People Tab and People List — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the person entity its backoffice: a People tab on the company page (the published persons, their members, roles, `data`, history and raw evidence, plus the reviewer's seven actions and the fold poller) and a plain server-paged People list at `/admin/se/people`.

**Architecture:** The Address tab's six-file split, one for one. Three client-safe modules (`se-person-tables.ts` the one table name that slice 4 renames, `se-person-fields.ts` the catalogue, the validation and a TypeScript port of the normalizer's identity tokens, `se-person-decision-form.ts` FormData -> one decision) keep the route's client bundle free of server code; one server module (`se-company-person-entity.server.ts`) reads the six entity tables through `chQuery` and writes reviewer suggestion rows and rules append-only; two components (`se-person-workspace.tsx`, `se-person-edit-sheet.tsx`) render the two-thirds/one-third layout of spec section 7; `fold-run-poller.tsx` is reused unchanged. The list is its own trio (`se-people-filters.ts`, `se-people-list.server.ts`, `se-people-table.tsx`) over `se_company_person_v2 FINAL`. No Dagster change, no migration, no serving change: `se_company_person_fold_companies` already normalizes before folding and already takes `company_ids`.

**Tech Stack:** React Router v7 (`corpscout/services/backoffice`, TypeScript, shadcn/ui, vitest, `npm run typecheck` = `react-router typegen && tsc`, `npm test` = `vitest run`), `@clickhouse/client` through `app/lib/clickhouse.server.ts`, Dagster GraphQL through `app/lib/dagster.server.ts`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md`, sections 3.1 to 3.6, 4, 5.1 to 5.6, 7, 8 and 10. Section 7 is the binding text for the tab and the list; the rulings below are recorded there by Task 4.

## Global Constraints

- Backoffice commands run from `corpscout/services/backoffice`: `npm run typecheck` and `npx vitest run <files>` must pass before every commit that touches `app/` or `tests/`. No Dagster change in this slice (Ruling 5) and no migration: the six tables are migration 000396's, unchanged.
- The route files export only `loader`, `action`, `meta` and the default component (see the comment at the top of `app/routes/admin-se-company-address.tsx`): any other export that touches a `.server` module keeps that module in the client bundle and breaks the production build. Client-safe modules never import a `.server` module.
- Every write is an append. A reviewer suggestion row is a new version of `(company_id, source, slot)`; a rule is a new version of `(company_id, rule_id)`. Nothing edits a published row: the next fold applies what the reviewer decided.
- One `stamp = clickhouseStamp(now)` per action, used for every row and every id of that action. `clickhouseStamp` is `app/lib/se-basic-info.server.ts`'s pure helper (`date.toISOString().replace("T", " ").replace("Z", "")`), giving `YYYY-MM-DD HH:MM:SS.mmm`.
- `suggestion_id` of a backoffice row is `sha256("<company_id>\n<source>\n<slot>\n<stamp>")` hex — exactly what the extractors compute in SQL (`person/suggestions.py::PERSON_TRAILING_SELECT_SQL`), with `stamp` spelled the same as `suggested_at`.
- `rule_id` is `sha256("<company_id>\n<kind>\n<sorted person_keys, comma-joined>\n<sorted slots, comma-joined>\n<stamp>")` hex. It is minted once, when the rule is created, and a Reset writes a NEW VERSION OF THE SAME `rule_id` with `active = 0` and a newer `created_at` (the table is `ReplacingMergeTree(created_at) ORDER BY (company_id, rule_id)`).
- Nullable ClickHouse columns take `null`, never `''`, on writes; on reads every nullable value is collapsed to `''` (numbers included: `birth_year`, `fiscal_year`, `first_year`, `last_year`, `role_year` all reach the page as strings) so components never tell `""` from `null`. `FixedString(64)` columns are read through `toString(...)`, arrays of them through `arrayMap(x -> toString(x), ...)`.
- The suggestion table's 18 columns are `tables.py::SUGGESTION_COLUMNS`, in that order: `company_id, source, slot, suggestion_id, suggested_at, source_record_id, full_name, first_name, last_name, birth_year, wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, document_ref, data`. There is NO `note`, NO `decided_by`, NO `replaces_key`, NO `source_run_id` and NO `extractor_version` on it (unlike the address suggestion table) — see Ruling 4.
- The rule table's 9 columns are `tables.py::RULE_COLUMNS`: `company_id, rule_id, kind, person_keys, slots, active, note, created_at, created_by`. `created_by` is `"backoffice"`.
- `data` is a String holding a JSON OBJECT under `CONSTRAINT valid_data CHECK JSONType(data) = 'Object'`. Every row this backoffice writes carries a parsed, re-serialized object — never `''`, never an array, never a scalar.
- Sources come from the entity's catalogue (`tables.py::SOURCES`): `bolagsverket, esef, wikidata, ratsit, reviewer, reviewer_draft`. Role codes come from `corpscout.company_person_role_type` (Ruling 3), read on the server per request.
- The note field allows line breaks and tabs and refuses every other control character (`NOTE_CONTROL` in `app/lib/se-address-fields.ts`); every other text field refuses all control characters.
- Commit by explicit path only; never `git add -A`. Trailers, in this order, each on its own line at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`

## Rulings (controller, 2026-09-10)

1. **Correct decides "same person" from the tokens, not from a fold.** A Correct writes a reviewer row and a hide rule on the corrected key "unless the reviewer's text folds back to the same key" (spec 7). Writing the rule unconditionally is WRONG here: `fold.py::fold_company_persons` resolves a hide whose key is gone through the previous published members, so when the reviewer's row joins the corrected person's set the rule hides that set — the reviewer's own correction included. So the server decides it: `se-person-fields.ts` ports the normalizer's identity rules (spec 4.1/4.2) to TypeScript, and `activateSePersonDraft` writes the hide rule only when the draft's tokens match NO member of the corrected person under spec 5.1's relation (equal first and last tokens with middle tokens equal or one a subset of the other, or the same QID, and never with two different birth years). The port is needed anyway: the sheet must refuse a name the normalizer would score `partial` or `no_person`, since such a row never folds into a person. Both ways of being wrong are visible and recoverable — a missed rule leaves a duplicate person (Merge or Remove it), a spurious rule hides one (Reset it).
2. **Reviewer slots are a group plus an ordinal.** One reviewer person is several suggestion rows — one per role entry — and the table is keyed `(company_id, source, slot)`, so the rows cannot share a slot. The group slot is `r` + the stamp's 17 digits (`r20260910120000123`, the address scheme); each row's slot is the group plus a two-digit ordinal (`r2026091012000012301`). The group is `rowSlot.slice(0, 18)`. A form posts the GROUP slot; the server enumerates the rows under it. A person with no role at all still writes one row, ordinal `01`.
3. **The role codes are read from the catalog, not hard-coded.** `corpscout.company_person_role_type` is a live table (12 codes seeded 000290, one more 000294, nine more from Serbia 000319) that a reviewer page already reads (`app/lib/company-roles.server.ts::getCompanyPersonRoleTypes`). The tab's loader reads the active rows and hands the client `{ code, label, group }[]`; the decision parser takes the code list as an argument and refuses anything else. Nothing in a client-safe module hard-codes a role code.
4. **The reviewer's note, author and corrected key live in `data`, under reserved keys.** The person suggestion table has no `note`, `decided_by` or `replaces_key` column. So every backoffice row's `data` carries `decided_by: "backoffice"` and `note` (the reviewer's, `""` when none), and a Correct's DRAFT rows also carry `replaces_key`. `validateSePersonInput` refuses those three keys from the reviewer's own JSON, and `activateSePersonDraft` strips `replaces_key` when it copies the draft's data onto the `reviewer` row, so a published person's merged `data` (spec 5.5) never carries it.
5. **A role entry is one row, with a year or a span.** The reviewer enters `(role code, from year, to year)`, and `fold.py::role_years_for` decides what each shape means, so the writer maps onto it exactly: from == to (both filled) is one year, written as `fiscal_year` with both dates NULL; a from year with an empty to year is an open span, `role_from = <from>-01-01` with `role_to` NULL, which the fold expands from that year to the current one; two different years are `role_from = <from>-01-01` and `role_to = <to>-12-31`; a to year alone is `role_to = <to>-12-31`, which the fold takes as that one year; and a role with no year at all carries neither, which the fold takes as held now (its 290 dateless Wikidata roles' path). `role_original` and `role_key` both carry the catalog code, because `roles.py::role_code_for` has no map for source `reviewer` and falls through to `role_original` first, `role_key` second — the code either way.
6. **Activate launches the fold; the other writes do not.** Spec 7: "Activate writes a reviewer suggestion at slot `r` plus digits and launches the targeted fold". So the route runs `launchSePersonFold` after a successful Activate and returns its `runId`, and the workspace's poller shows for both `fold-now` and `activate`. Remove, Merge, Split and Reset leave "Fold pending" up and the reviewer presses Fold now.
7. **The tab reads six tables, the global precedence included, but only the company's own precedence rows count for "Fold pending".** A global precedence export re-folds every company (`batch.py::_changed_company_ids` compares it), which is a backfill, not this tab's business; lighting "Fold pending" on 578,289 companies would make the badge meaningless. The panel still SHOWS the order, which is what explains `text_source`.
8. **Eight intents, not seven actions.** Spec 7 lists seven actions; the parser carries an
   eighth, `discard`, because Add's draft has a lifecycle (save -> activate OR discard) and a
   draft nobody activates must be clearable without publishing anything. The address parser
   carries the same intent for the same reason. Task 4's doc step names it in section 7.
9. **Split carries its caveat in the dialog.** A split rule pins slots, and Bolagsverket mints a new slot per filing, so a Split written today stops applying to next year's report. The dialog says so; there is no durable mechanism in this slice (controller ruling 2026-09-10, spec section 9 item 2's "known limits for slice 3").

---

## File map

| File | Responsibility |
| --- | --- |
| `backoffice/app/lib/se-person-tables.ts` (create) | `SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person_v2"`, the one name slice 4 renames. |
| `backoffice/app/lib/se-person-fields.ts` (create) | Catalogue, labels, `selectedPersonFromSearch`, the identity-token port (`normalizeSePersonName`, `foldsIntoPerson`), `validateSePersonInput`, `personFoldPending`, the slot helpers. |
| `backoffice/app/lib/se-person-decision-form.ts` (create) | `SePersonDecision`, `parseSePersonDecision`. |
| `backoffice/app/lib/clickhouse.server.ts` (modify) | `chInsertSeCompanyPersonSuggestions`, `chInsertSeCompanyPersonRules`. |
| `backoffice/app/lib/dagster.server.ts` (modify) | `SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET = "se_company_person_fold_companies"`. |
| `backoffice/app/lib/se-company-person-entity.server.ts` (create) | Six reads into `SePersonDetail`, the seven writes, the role options, `launchSePersonFold`. |
| `backoffice/app/components/admin/se-person-workspace.tsx` (create) | The two-column tab, the dialogs, the fold card. |
| `backoffice/app/components/admin/se-person-edit-sheet.tsx` (create) | Add / Correct / edit-draft sheet with the roles editor and the `data` editor. |
| `backoffice/app/routes/admin-se-company-person.tsx` (create) | loader, action, component. |
| `backoffice/app/lib/se-company-tabs.ts` (modify) | The `people` tab entry, beside `address`. |
| `backoffice/app/routes.ts` (modify) | `people` under the company area; `se/people` under `/admin`. |
| `backoffice/app/lib/se-people-filters.ts` (create) | The list's URL-facing filter state, client-safe. |
| `backoffice/app/lib/se-people-list.server.ts` (create) | The list SQL, the counts strip, the company-name lookup. |
| `backoffice/app/components/admin/se-people-table.tsx` (create) | The list's filters, counts strip, table and pager. |
| `backoffice/app/routes/admin-se-people.tsx` (create) | loader + component. |
| `backoffice/app/components/admin/admin-sidebar.tsx` (modify) | The Sweden group's second entry, People. |
| `backoffice/app/routes/admin-layout.tsx` (modify) | The `/admin/se/people` breadcrumb. |
| `backoffice/tests/se-person-fields.test.ts`, `se-person-decision-form.test.ts`, `se-company-person-entity.server.test.ts`, `se-person-edit-sheet.test.tsx`, `admin-se-company-person.test.tsx`, `se-people-list.server.test.ts`, `admin-se-people.test.tsx` (create); `tests/admin-se-company-area.test.tsx` (modify) | Vitest; ClickHouse and Dagster mocked with `vi.mock` as `tests/se-company-address-entity.server.test.ts` does. |
| Spec sections 7, 9 and 10 (modify, Task 4 and Task 6) | Rulings 1 to 9; the slice-3 shipped record. |

## Interfaces (defined in Tasks 1 and 2, consumed by 3, 4 and 5)

```ts
// se-person-tables.ts
export const SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person_v2";

// se-person-fields.ts
export const PERSON_SOURCES = ["bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft"] as const;
export const REVIEWER_SOURCE = "reviewer";
export const DRAFT_SOURCE = "reviewer_draft";
export const PERSON_STATUSES = ["active", "hidden", "withdrawn"] as const;
export const RESERVED_DATA_KEYS = ["decided_by", "note", "replaces_key"] as const;
export const MAX_NOTE_LENGTH = 500;
export const MAX_DATA_LENGTH = 4000;
export const MAX_ROLE_ENTRIES = 20;
export type SePersonSource = (typeof PERSON_SOURCES)[number];
export type SePersonStatus = (typeof PERSON_STATUSES)[number];
export function isPersonSource(v: string): v is SePersonSource;
export function isPersonStatus(v: string): v is SePersonStatus;
export function isPersonKey(v: string): boolean;                       // 64 hex
export function personSourceLabel(source: string): string;             // "Bolagsverket" | "ESEF" | "Wikidata" | "Ratsit" | "Reviewer" | "Reviewer draft"
export function selectedPersonFromSearch(params: URLSearchParams): string | null;   // ?person=<64 hex>
export interface SePersonRoleOption { code: string; label: string; group: string }
export function roleLabel(code: string, options: readonly SePersonRoleOption[]): string;
export const PERSON_GROUP_SLOT_PATTERN: RegExp;                        // /^r[0-9]{17}$/
export const PERSON_ROW_SLOT_PATTERN: RegExp;                          // /^r[0-9]{19}$/
export function personGroupSlot(stamp: string): string;                // "r" + the stamp's 17 digits
export function personRowSlot(group: string, index: number): string;   // group + a two-digit ordinal
export function groupOfRowSlot(rowSlot: string): string;               // rowSlot.slice(0, 18)
export interface SePersonTokens { status: "ok" | "partial" | "no_person"; reason: string; firstTokens: string[]; middleTokens: string[]; lastTokens: string[]; displayFirst: string; displayLast: string; displayName: string }
export function normalizeSePersonName(raw: { fullName?: string; firstName?: string; lastName?: string }): SePersonTokens;
export interface SePersonIdentity { firstTokens: readonly string[]; middleTokens: readonly string[]; lastTokens: readonly string[]; birthYear: string; wikidataId: string }
export function foldsIntoPerson(a: SePersonIdentity, b: SePersonIdentity): boolean;
export interface SePersonRoleInput { code: string; fromYear: string; toYear: string }
export interface SePersonInput { firstName: string; lastName: string; birthYear: string; wikidataId: string; roles: SePersonRoleInput[]; data: string; note: string }
export type SePersonValidation = { ok: true; input: SePersonInput } | { ok: false; error: string };
export function validateSePersonInput(raw: { firstName: string; lastName: string; birthYear: string; wikidataId: string; roles: SePersonRoleInput[]; data: string; note: string }, roleCodes: readonly string[], currentYear?: number): SePersonValidation;
export function personFoldPending(foldedAt: string | null, stamps: readonly string[], hasFoldable: boolean): boolean;

// se-person-decision-form.ts
export type SePersonDecision =
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SePersonInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string }
  | { intent: "remove"; personKey: string; note: string }
  | { intent: "merge"; personKeys: string[]; note: string }
  | { intent: "split"; slots: string[]; note: string }
  | { intent: "reset"; personKey: string; note: string }
  | { intent: "fold-now" };
export type SePersonDecisionRequest = { ok: true; decision: SePersonDecision } | { ok: false; error: string };
export function parseSePersonDecision(form: FormData, roleCodes: readonly string[]): SePersonDecisionRequest;

// se-company-person-entity.server.ts
export interface SePersonRow { /* the 29 main columns; every nullable one as a string, active as a number */ }
export type SePersonHistoryRow = SePersonRow & { changed_at: string; change_kind: string; fold_run_id: string };
export interface SePersonNormalizedRow { /* the 23 normalized columns as strings/arrays */ }
export interface SePersonRawRow { /* the 18 suggestion columns as strings */ }
export interface SePersonRuleRow { company_id: string; rule_id: string; kind: string; person_keys: string[]; slots: string[]; active: number; note: string; created_at: string; created_by: string }
export interface SePersonPrecedenceRow { company_id: string; field: string; source: string; precedence: number; removed: number; decided_by: string; note: string; decided_at: string }
export interface SePersonMember { source: string; slot: string; normalizedId: string; name: string; birthYear: string; wikidataId: string; data: string; current: SePersonNormalizedRow | null; raw: SePersonRawRow | null; refoldPending: boolean; precedence: number }
export interface SePersonRoleYear { code: string; year: number; sources: string[] }
export interface SePersonPublished { row: SePersonRow; members: SePersonMember[]; roles: SePersonRoleYear[]; spellingReason: "single source" | "precedence" | "most complete" | "tie-break"; rules: SePersonRuleRow[] }
export interface SePersonDraft { slot: string; rows: SePersonRawRow[]; normalized: SePersonNormalizedRow[]; name: string; note: string; replacesKey: string }
export interface SePersonDetail { published: SePersonPublished[]; drafts: SePersonDraft[]; history: SePersonHistoryRow[]; rules: SePersonRuleRow[]; precedence: SePersonPrecedenceRow[]; foldPending: boolean }
export async function loadSePersonDetail(companyId: string): Promise<SePersonDetail | null>;
export async function loadSePersonRoleOptions(): Promise<SePersonRoleOption[]>;
export class SePersonDecisionError extends Error {}
export async function saveSePersonDraft(companyId: string, decision, now?: Date): Promise<{ decidedAt: string; slot: string }>;
export async function activateSePersonDraft(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function discardSePersonDraft(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function removeSePerson(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function mergeSePersons(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function splitSePersonSlots(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function resetSePersonRules(companyId: string, decision, now?: Date): Promise<{ decidedAt: string }>;
export async function launchSePersonFold(companyId: string): Promise<{ runId: string; url: string | null }>;

// se-people-filters.ts / se-people-list.server.ts (Task 5)
export interface SePeopleFilters { company: string; name: string; source: string; role: string; year: string; status: string }
export function parseSePeopleFilters(params: URLSearchParams): SePeopleFilters;
export function sePeopleHref(filters: SePeopleFilters, page: number, pageSize: number): string;
export interface SePeopleListRow { company_id: string; legal_name: string; person_key: string; display_name: string; birth_year: string; wikidata_id: string; sources: string[]; current_roles: string[]; role_years: number[]; first_year: string; last_year: string; active: number; inactive_reason: string }
export interface SePeopleCounts { persons: number; active: number; companies: number }
export const COMPANY_MATCH_LIMIT = 200;
export const PEOPLE_LIST_SELECT_SQL: string;
export const PEOPLE_COUNTS_SQL: string;
export const PEOPLE_COMPANY_NAMES_SQL: string;
export const PEOPLE_COMPANY_SEARCH_SQL: string;
/** `null` when no company filter is set; the ids to restrict to otherwise. */
export interface SePeopleCompanyMatch { companyIds: string[] | null; truncated: boolean }
export async function resolveSePeopleCompanyIds(company: string): Promise<SePeopleCompanyMatch>;
export function buildSePeopleFilter(filters: SePeopleFilters, companyIds: string[] | null): { where: string[]; params: Record<string, unknown> };
export async function listSePeoplePage(query: SePeopleFilters & { companyIds: string[] | null; page: number; pageSize: number }): Promise<{ rows: SePeopleListRow[] }>;
export async function loadSePeopleCounts(query: SePeopleFilters & { companyIds: string[] | null }): Promise<SePeopleCounts>;
```

---

### Task 1: The table name, the client-safe catalogue and validation, the decision parser

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-person-tables.ts`, `app/lib/se-person-fields.ts`, `app/lib/se-person-decision-form.ts`
- Test: `corpscout/services/backoffice/tests/se-person-fields.test.ts`, `tests/se-person-decision-form.test.ts`

**Interfaces:**
- Consumes: nothing. Model `app/lib/se-address-fields.ts` and `app/lib/se-address-decision-form.ts` for style (no `.server` import, hand-written validation, manual FormData reading) and `app/lib/se-address-tables.ts` for the table constant's comment.
- Produces: the three modules' exports in the interface block above. Tasks 2 to 5 import them.

- [ ] **Step 1: Write the failing tests**

`tests/se-person-fields.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  foldsIntoPerson,
  groupOfRowSlot,
  isPersonKey,
  isPersonSource,
  isPersonStatus,
  normalizeSePersonName,
  personFoldPending,
  personGroupSlot,
  personRowSlot,
  personSourceLabel,
  PERSON_SOURCES,
  PERSON_STATUSES,
  roleLabel,
  selectedPersonFromSearch,
  validateSePersonInput,
} from "~/lib/se-person-fields";

const KEY = "a".repeat(64);
const ROLE_CODES = ["board_member", "board_chair", "chief_executive_officer"];
const ROLE_OPTIONS = [
  { code: "board_member", label: "Board member", group: "governance" },
  { code: "board_chair", label: "Board chair", group: "governance" },
];
const base = {
  firstName: "Anna",
  lastName: "Svensson",
  birthYear: "1975",
  wikidataId: "",
  roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
  data: "",
  note: "",
};

describe("person catalogue", () => {
  it("names the entity's six sources, three statuses and its slots", () => {
    expect([...PERSON_SOURCES]).toEqual([
      "bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft",
    ]);
    expect([...PERSON_STATUSES]).toEqual(["active", "hidden", "withdrawn"]);
    expect(isPersonSource("wikidata") && !isPersonSource("scb")).toBe(true);
    expect(isPersonStatus("withdrawn") && !isPersonStatus("gone")).toBe(true);
    expect(personSourceLabel("esef")).toBe("ESEF");
    expect(personSourceLabel("reviewer_draft")).toBe("Reviewer draft");
    expect(roleLabel("board_chair", ROLE_OPTIONS)).toBe("Board chair");
    // An unmapped code (a source's own label, published as itself) reads as itself.
    expect(roleLabel("styrelseledarmot", ROLE_OPTIONS)).toBe("styrelseledarmot");
    expect(isPersonKey(KEY) && !isPersonKey("nope")).toBe(true);
    expect(selectedPersonFromSearch(new URLSearchParams(`person=${KEY}`))).toBe(KEY);
    expect(selectedPersonFromSearch(new URLSearchParams("person=nope"))).toBeNull();
    // Ruling 2: the group slot is the stamp's 17 digits, a row slot adds an ordinal.
    expect(personGroupSlot("2026-09-10 12:00:00.123")).toBe("r20260910120000123");
    expect(personRowSlot("r20260910120000123", 1)).toBe("r2026091012000012301");
    expect(groupOfRowSlot("r2026091012000012310")).toBe("r20260910120000123");
  });
});

describe("normalizeSePersonName (the port of normalize_se.py, spec 4.1 to 4.4)", () => {
  it("splits, folds and keeps the delivered spelling", () => {
    expect(normalizeSePersonName({ firstName: "Anna", lastName: "Svensson" })).toMatchObject({
      status: "ok",
      firstTokens: ["anna"],
      middleTokens: [],
      lastTokens: ["svensson"],
      displayName: "Anna Svensson",
    });
    expect(normalizeSePersonName({ firstName: "Anna Maria", lastName: "Svensson" })).toMatchObject({
      firstTokens: ["anna"], middleTokens: ["maria"], lastTokens: ["svensson"],
    });
    // Diacritics fold, hyphens split, initials lose their periods.
    expect(normalizeSePersonName({ firstName: "Hakan", lastName: "Oberg" })).toMatchObject({
      firstTokens: ["hakan"], lastTokens: ["oberg"],
    });
    expect(normalizeSePersonName({ firstName: "Sven-Erik", lastName: "Ek" })).toMatchObject({
      firstTokens: ["sven"], middleTokens: ["erik"], lastTokens: ["ek"],
    });
    // A one-string name: the last word is the last name, particles glue to it.
    expect(normalizeSePersonName({ fullName: "Carl von Essen" })).toMatchObject({
      firstTokens: ["carl"], lastTokens: ["von", "essen"], displayLast: "von Essen",
    });
    expect(normalizeSePersonName({ fullName: "Svensson, Anna" })).toMatchObject({
      firstTokens: ["anna"], lastTokens: ["svensson"], displayName: "Anna Svensson",
    });
    // Titles are dropped from the display spelling, whole words only.
    expect(normalizeSePersonName({ firstName: "Dr Anna", lastName: "Svensson" })).toMatchObject({
      status: "ok", displayFirst: "Anna",
    });
    expect(normalizeSePersonName({ firstName: "Ing-Marie", lastName: "Ek" }).displayFirst).toBe("Ing-Marie");
  });

  it("scores partial and no_person exactly as the normalizer does", () => {
    expect(normalizeSePersonName({ lastName: "Svensson" })).toMatchObject({
      status: "partial", reason: "only one name word",
    });
    expect(normalizeSePersonName({ firstName: "A", lastName: "Svensson" })).toMatchObject({
      status: "partial", reason: "initials only",
    });
    expect(normalizeSePersonName({})).toMatchObject({ status: "no_person", reason: "empty name" });
    expect(normalizeSePersonName({ fullName: "Styrelseledamot" })).toMatchObject({
      status: "no_person", reason: "role word in the name field",
    });
    expect(normalizeSePersonName({ fullName: "Anna Svensson AB" })).toMatchObject({
      status: "no_person", reason: "company suffix in the name field",
    });
    expect(normalizeSePersonName({ fullName: "Anna 1985" })).toMatchObject({
      status: "no_person", reason: "digits in the name field",
    });
  });
});

describe("foldsIntoPerson (spec 5.1's guarded relation, Ruling 1)", () => {
  const anna = {
    firstTokens: ["anna"], middleTokens: [], lastTokens: ["svensson"], birthYear: "", wikidataId: "",
  };
  it("matches equal names, a lone middle name and a shared QID", () => {
    expect(foldsIntoPerson(anna, { ...anna })).toBe(true);
    expect(foldsIntoPerson(anna, { ...anna, middleTokens: ["maria"] })).toBe(true);
    expect(foldsIntoPerson(anna, { ...anna, lastTokens: ["svenson"] })).toBe(false);
    expect(foldsIntoPerson(anna, { ...anna, firstTokens: ["annika"] })).toBe(false);
    expect(
      foldsIntoPerson(
        { ...anna, wikidataId: "Q42" },
        { firstTokens: ["carl"], middleTokens: [], lastTokens: ["essen"], birthYear: "", wikidataId: "Q42" },
      ),
    ).toBe(true);
  });
  it("never matches two different birth years, whatever the name says", () => {
    expect(foldsIntoPerson({ ...anna, birthYear: "1975" }, { ...anna, birthYear: "1975" })).toBe(true);
    expect(foldsIntoPerson({ ...anna, birthYear: "1975" }, { ...anna, birthYear: "1980" })).toBe(false);
    expect(
      foldsIntoPerson(
        { ...anna, birthYear: "1975", wikidataId: "Q42" },
        { ...anna, birthYear: "1980", wikidataId: "Q42" },
      ),
    ).toBe(false);
  });
});

describe("validateSePersonInput", () => {
  it("accepts a first and last name with a role span and normalizes the data object", () => {
    expect(validateSePersonInput({ ...base, data: ' {"title":"Chair"} ' }, ROLE_CODES, 2026)).toEqual({
      ok: true,
      input: {
        firstName: "Anna",
        lastName: "Svensson",
        birthYear: "1975",
        wikidataId: "",
        roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
        data: '{"title":"Chair"}',
        note: "",
      },
    });
    // No roles at all is still a person: the fold publishes an empty roles block.
    expect(validateSePersonInput({ ...base, roles: [] }, ROLE_CODES, 2026).ok).toBe(true);
    // A blank role row is dropped, not refused: the sheet always renders one.
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "", fromYear: "", toYear: "" }] }, ROLE_CODES, 2026),
    ).toMatchObject({ ok: true, input: { roles: [] } });
  });

  it("refuses a name the normalizer would not fold, and says why", () => {
    expect(validateSePersonInput({ ...base, firstName: "A" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "initials only",
    });
    expect(validateSePersonInput({ ...base, lastName: "" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "only one name word",
    });
    expect(validateSePersonInput({ ...base, lastName: "AB" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "company suffix in the name field",
    });
  });

  it("refuses a bad birth year, a bad QID, an unknown role, a reversed span and a far year", () => {
    expect(validateSePersonInput({ ...base, birthYear: "1750" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Birth year must be between 1850 and 2026.",
    });
    expect(validateSePersonInput({ ...base, wikidataId: "42" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Wikidata id must look like Q42.",
    });
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "vd", fromYear: "", toYear: "" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "Unknown role: vd." });
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "2025", toYear: "2019" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "A role cannot end before it starts." });
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "2040", toYear: "" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "From year must be between 1900 and 2031." });
  });

  it("refuses data that is not an object, a reserved key, and a note with a control character", () => {
    expect(validateSePersonInput({ ...base, data: "[1,2]" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data must be a JSON object.",
    });
    expect(validateSePersonInput({ ...base, data: "not json" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data must be a JSON object.",
    });
    // Ruling 4: the three keys the backoffice sets itself are refused here.
    expect(validateSePersonInput({ ...base, data: '{"note":"mine"}' }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data may not carry the reserved key note.",
    });
    expect(validateSePersonInput({ ...base, data: '{"replaces_key":"x"}' }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data may not carry the reserved key replaces_key.",
    });
    expect(validateSePersonInput({ ...base, note: "line\nbreak" }, ROLE_CODES, 2026).ok).toBe(true);
    expect(validateSePersonInput({ ...base, note: "bell\u0007" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Note must be plain text.",
    });
    expect(validateSePersonInput({ ...base, note: "x".repeat(501) }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Note is longer than 500 characters.",
    });
  });
});

describe("personFoldPending", () => {
  it("is pending when a stamp is newer than the fold, or when nothing was folded but rows fold", () => {
    expect(personFoldPending("2026-09-10 10:00:00.000", ["2026-09-10 09:00:00.000"], true)).toBe(false);
    expect(personFoldPending("2026-09-10 10:00:00.000", ["2026-09-10 11:00:00.000"], true)).toBe(true);
    expect(personFoldPending(null, [], true)).toBe(true);
    expect(personFoldPending(null, [], false)).toBe(false);
  });
});
```

`tests/se-person-decision-form.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { parseSePersonDecision } from "~/lib/se-person-decision-form";

const KEY = "b".repeat(64);
const OTHER = "c".repeat(64);
const SLOT = "r20260910120000123";
const ROLE_CODES = ["board_member", "board_chair"];

function form(entries: Record<string, string>, repeated: [string, string][] = []): FormData {
  const data = new FormData();
  for (const [key, value] of Object.entries(entries)) data.set(key, value);
  for (const [key, value] of repeated) data.append(key, value);
  return data;
}

describe("parseSePersonDecision", () => {
  it("parses fold-now, remove, reset, activate and discard", () => {
    expect(parseSePersonDecision(form({ intent: "fold-now" }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "fold-now" },
    });
    expect(parseSePersonDecision(form({ intent: "remove", person_key: KEY, note: " gone " }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "remove", personKey: KEY, note: "gone" },
    });
    expect(parseSePersonDecision(form({ intent: "reset", person_key: KEY }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "reset", personKey: KEY, note: "" },
    });
    expect(parseSePersonDecision(form({ intent: "activate", slot: SLOT, note: "" }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "activate", slot: SLOT, note: "" },
    });
    expect(parseSePersonDecision(form({ intent: "discard", slot: SLOT }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "discard", slot: SLOT },
    });
  });

  it("parses merge over the checked keys and split over the checked slots, sorted and deduplicated", () => {
    expect(
      parseSePersonDecision(
        form({ intent: "merge", note: "same person" }, [["person_key", OTHER], ["person_key", KEY], ["person_key", KEY]]),
        ROLE_CODES,
      ),
    ).toEqual({ ok: true, decision: { intent: "merge", personKeys: [KEY, OTHER], note: "same person" } });
    expect(
      parseSePersonDecision(
        form({ intent: "split", note: "two people" }, [["slot", "uid-2:sig-1"], ["slot", "uid-1:sig-1"]]),
        ROLE_CODES,
      ),
    ).toEqual({ ok: true, decision: { intent: "split", slots: ["uid-1:sig-1", "uid-2:sig-1"], note: "two people" } });
  });

  it("parses save-draft with the validated input, the role rows and an optional replaces key", () => {
    const result = parseSePersonDecision(
      form(
        {
          intent: "save-draft", first_name: "Anna", last_name: "Svensson", birth_year: "1975",
          wikidata_id: "Q42", data: "", note: "typed", slot: "", replaces_key: KEY,
        },
        [
          ["role_code", "board_member"], ["role_from", "2023"], ["role_to", "2025"],
          ["role_code", "board_chair"], ["role_from", "2026"], ["role_to", ""],
        ],
      ),
      ROLE_CODES,
    );
    expect(result).toEqual({
      ok: true,
      decision: {
        intent: "save-draft", slot: null, replacesKey: KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "Q42",
          roles: [
            { code: "board_member", fromYear: "2023", toYear: "2025" },
            { code: "board_chair", fromYear: "2026", toYear: "" },
          ],
          data: "{}", note: "typed",
        },
      },
    });
  });

  it("refuses a bad key, a bad slot, too few merge keys, no split slot, a bad note and an unknown intent", () => {
    expect(parseSePersonDecision(form({ intent: "remove", person_key: "zz" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown person.",
    });
    expect(parseSePersonDecision(form({ intent: "activate" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown draft.",
    });
    expect(parseSePersonDecision(form({ intent: "merge" }, [["person_key", KEY]]), ROLE_CODES)).toEqual({
      ok: false, error: "Pick at least two persons to merge.",
    });
    expect(parseSePersonDecision(form({ intent: "split" }), ROLE_CODES)).toEqual({
      ok: false, error: "Pick at least one observation to split off.",
    });
    expect(parseSePersonDecision(form({ intent: "reset", person_key: KEY, note: "x".repeat(501) }), ROLE_CODES)).toEqual({
      ok: false, error: "Note is longer than 500 characters.",
    });
    // A dialog's note never passes through the sheet's validation, so the parser is
    // where spec 7's "line breaks yes, other control characters no" has to hold.
    expect(parseSePersonDecision(form({ intent: "merge", note: "two\u0007lines" }, [["person_key", KEY], ["person_key", OTHER]]), ROLE_CODES)).toEqual({
      ok: false, error: "Note must be plain text.",
    });
    expect(parseSePersonDecision(form({ intent: "merge", note: "two\nlines" }, [["person_key", KEY], ["person_key", OTHER]]), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "merge", personKeys: [KEY, OTHER], note: "two\nlines" },
    });
    expect(
      parseSePersonDecision(form({ intent: "save-draft", first_name: "Anna", last_name: "" }), ROLE_CODES),
    ).toEqual({ ok: false, error: "only one name word" });
    expect(parseSePersonDecision(form({ intent: "nope" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown intent.",
    });
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run (from `corpscout/services/backoffice`): `npx vitest run tests/se-person-fields.test.ts tests/se-person-decision-form.test.ts`
Expected: FAIL — `Failed to resolve import "~/lib/se-person-fields"`.

- [ ] **Step 3: Write the three modules**

`app/lib/se-person-tables.ts`:

```ts
/**
 * The published SE person entity (spec 2026-09-09, section 3.3), under the name
 * migration 000396 gave it. Every backoffice read of the published persons names it
 * through this constant, so slice 4's rename to `corpscout.se_company_person` is a
 * one-line change here.
 *
 * MIND THE PREFIX: after that rename `corpscout.se_company_person_suggestion`,
 * `_normalized`, `_history`, `_rule` and `_precedence` all start with this string, so a
 * match on the table -- in a test, or in a fake client's dispatch -- has to carry the
 * alias that follows it.
 */
export const SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person_v2";
```

`app/lib/se-person-fields.ts`:

```ts
/**
 * The person entity's catalogue, the client-safe validation of a typed person, and a
 * TypeScript port of the normalizer's identity rules (spec 2026-09-09 sections 3.1, 4
 * and 7). No `.server` import: the route's module must not drag ClickHouse into the
 * client bundle.
 *
 * WHY THE PORT (Ruling 1). Two things need the normalizer's own answer before any fold
 * runs: the sheet must refuse a name that would score `partial` or `no_person` (such a
 * row is stored and never folded into a person, so activating it would publish
 * nothing), and Activate must know whether a Correct's text folds back into the person
 * it corrects -- if it does, hiding that person would hide the correction with it. This
 * is `normalize_se.py` sections 4.1, 4.2 and 4.4 rule for rule; keep the two in step,
 * and when they disagree the fold wins (both ways of being wrong are visible: a missed
 * hide rule leaves a duplicate person, a spurious one hides a person Reset brings back).
 */

export const PERSON_SOURCES = [
  "bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft",
] as const;
export const REVIEWER_SOURCE = "reviewer";
export const DRAFT_SOURCE = "reviewer_draft";
export const PERSON_STATUSES = ["active", "hidden", "withdrawn"] as const;
/** Ruling 4: the three `data` keys the backoffice writes itself. */
export const RESERVED_DATA_KEYS = ["decided_by", "note", "replaces_key"] as const;
export const MAX_NOTE_LENGTH = 500;
export const MAX_DATA_LENGTH = 4000;
export const MAX_NAME_LENGTH = 100;
export const MAX_ROLE_ENTRIES = 20;
export const MIN_BIRTH_YEAR = 1850;
export const MIN_ROLE_YEAR = 1900;
/** Wikidata end dates run past today (2029 on prod), so a typed role may too. */
export const ROLE_YEAR_SLACK = 5;

export type SePersonSource = (typeof PERSON_SOURCES)[number];
export type SePersonStatus = (typeof PERSON_STATUSES)[number];

const KEY_PATTERN = /^[0-9a-f]{64}$/;
const CONTROL = /[\u0000-\u001f\u007f]/;
/** The note is a Textarea: tab, line feed and carriage return are what a browser sends
 * for a multi-line note, so only the other C0 controls and DEL are refused (the same
 * class `se-address-fields.ts` uses). */
const NOTE_CONTROL = /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/;
const QID_PATTERN = /^Q[1-9][0-9]{0,11}$/;
export const PERSON_GROUP_SLOT_PATTERN = /^r[0-9]{17}$/;
export const PERSON_ROW_SLOT_PATTERN = /^r[0-9]{19}$/;

export function isPersonSource(value: string): value is SePersonSource {
  return (PERSON_SOURCES as readonly string[]).includes(value);
}
export function isPersonStatus(value: string): value is SePersonStatus {
  return (PERSON_STATUSES as readonly string[]).includes(value);
}
export function isPersonKey(value: string): boolean {
  return KEY_PATTERN.test(value);
}

const SOURCE_LABELS: Record<SePersonSource, string> = {
  bolagsverket: "Bolagsverket", esef: "ESEF", wikidata: "Wikidata", ratsit: "Ratsit",
  reviewer: "Reviewer", reviewer_draft: "Reviewer draft",
};
export function personSourceLabel(source: string): string {
  return isPersonSource(source) ? SOURCE_LABELS[source] : source;
}

/** One row of `corpscout.company_person_role_type`, as the loader hands it to the
 * client (Ruling 3): the catalog is a live table, never a hard-coded list. */
export interface SePersonRoleOption {
  code: string;
  label: string;
  group: string;
}
/** The catalog's display name for a code, or the code itself -- an unmapped source
 * label is published as itself (spec 4.3) and must still read. */
export function roleLabel(code: string, options: readonly SePersonRoleOption[]): string {
  return options.find((option) => option.code === code)?.label ?? code;
}

export function selectedPersonFromSearch(params: URLSearchParams): string | null {
  const key = params.get("person") ?? "";
  return isPersonKey(key) ? key : null;
}

/* ------------------------------------------------------------------ */
/* Ruling 2: the reviewer's slots                                       */
/* ------------------------------------------------------------------ */

/** The group slot of one reviewer person: `r` + the stamp's 17 digits. */
export function personGroupSlot(stamp: string): string {
  return `r${stamp.replace(/\D/g, "")}`;
}
/** One row's slot inside that group: the group plus a two-digit ordinal (01..99). */
export function personRowSlot(group: string, index: number): string {
  return `${group}${String(index).padStart(2, "0")}`;
}
/** The group a row slot belongs to: `r` + 17 digits, the first 18 characters. */
export function groupOfRowSlot(rowSlot: string): string {
  return rowSlot.slice(0, 18);
}

/* ------------------------------------------------------------------ */
/* The normalizer's identity rules, ported (spec 4.1, 4.2, 4.4)         */
/* ------------------------------------------------------------------ */

const WHITESPACE = /\s+/g;
const DIGIT = /\d/;
const SUBTOKEN_SPLIT = /[.\-]+/;
const NON_TOKEN = /[^a-z0-9]+/g;
const PARTICLES = new Set(["von", "af", "de", "van", "der", "la", "le"]);
const TITLE_WORDS = new Set([
  "dr", "prof", "professor", "doktor", "herr", "fru", "froken", "mr", "mrs", "ms",
]);
const TITLE_PHRASES: readonly (readonly string[])[] = [
  ["jur", "kand"], ["civ", "ing"], ["civ", "ekon"],
  ["ekon", "dr"], ["fil", "dr"], ["med", "dr"], ["jur", "dr"],
];
const ROLE_PHRASES: readonly (readonly string[])[] = [
  ["styrelseledamot"], ["styrelseordforande"], ["styrelsesuppleant"], ["ordforande"],
  ["suppleant"], ["ledamot"], ["revisor"], ["likvidator"], ["firmatecknare"],
  ["arbetstagarrepresentant"], ["vd"], ["verkstallande", "direktor"], ["vice", "vd"],
  ["huvudansvarig", "revisor"], ["auktoriserad", "revisor"],
  ["board", "member"], ["board", "chair"], ["chairman"], ["auditor"], ["liquidator"],
  ["chief", "executive", "officer"], ["director"], ["founder"], ["owner"],
];
/** `ek` is deliberately absent: Ek is a common Swedish surname. */
const COMPANY_TOKENS = new Set([
  "ab", "hb", "kb", "aktiebolag", "handelsbolag", "kommanditbolag",
]);

function clean(text: string | undefined): string {
  return (text ?? "").trim().replace(WHITESPACE, " ");
}
/** Case-folded and diacritic-free: Hakan and Håkan meet, Ö and O meet (spec 4.2).
 * `toLowerCase` where Python casefolds -- the two differ only on characters no Swedish
 * name carries (ß), and a difference here can only mislead the Correct hint. */
function foldText(text: string): string {
  return text.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase();
}
function foldWord(word: string): string {
  return foldText(word).replace(/\./g, "");
}
/** The identity tokens of one word: folded, split on hyphens and periods, letters and
 * digits only. Sven-Erik gives sven, erik; S.E. gives s, e. */
function subtokens(word: string): string[] {
  return foldText(word)
    .split(SUBTOKEN_SPLIT)
    .map((piece) => piece.replace(NON_TOKEN, ""))
    .filter((piece) => piece !== "");
}
function dropTitles(words: string[]): { kept: string[]; dropped: string[] } {
  const folded = words.map(foldWord);
  const kept: string[] = [];
  const dropped: string[] = [];
  let index = 0;
  while (index < words.length) {
    const phrase = TITLE_PHRASES.find((candidate) =>
      candidate.every((part, offset) => folded[index + offset] === part),
    );
    if (phrase !== undefined) {
      dropped.push(phrase.join(" "));
      index += phrase.length;
      continue;
    }
    if (TITLE_WORDS.has(folded[index] ?? "")) {
      dropped.push(folded[index] ?? "");
      index += 1;
      continue;
    }
    kept.push(words[index] ?? "");
    index += 1;
  }
  return { kept, dropped };
}
function hasRolePhrase(tokens: string[]): boolean {
  return ROLE_PHRASES.some((phrase) =>
    tokens.some((_, start) => phrase.every((part, offset) => tokens[start + offset] === part)),
  );
}
/** "Last, First" for a comma form; otherwise the last word is the last name, preceded
 * by any run of particles ("Carl von Essen" is Carl / von Essen). */
function splitFullName(full: string): { firstWords: string[]; lastWords: string[] } {
  if (full.includes(",")) {
    const [lastPart = "", firstPart = ""] = full.split(/,(.*)/s);
    return {
      firstWords: clean(firstPart) === "" ? [] : dropTitles(clean(firstPart).split(" ")).kept,
      lastWords: clean(lastPart) === "" ? [] : dropTitles(clean(lastPart).split(" ")).kept,
    };
  }
  const words = dropTitles(full.split(" ")).kept;
  let index = Math.max(words.length - 1, 0);
  while (index > 0 && PARTICLES.has(foldWord(words[index - 1] ?? ""))) index -= 1;
  return { firstWords: words.slice(0, index), lastWords: words.slice(index) };
}

export interface SePersonTokens {
  status: "ok" | "partial" | "no_person";
  /** Why it is not `ok`; `""` when it is. The reviewer reads this. */
  reason: string;
  firstTokens: string[];
  middleTokens: string[];
  lastTokens: string[];
  displayFirst: string;
  displayLast: string;
  displayName: string;
}

/** `normalize_se.py::normalize_se_person` without the role half (spec 4.1, 4.2, 4.4). */
export function normalizeSePersonName(raw: {
  fullName?: string;
  firstName?: string;
  lastName?: string;
}): SePersonTokens {
  const firstIn = clean(raw.firstName);
  const lastIn = clean(raw.lastName);
  const full = clean(raw.fullName);
  const splitDelivered = firstIn !== "" || lastIn !== "";
  const sourceText = splitDelivered ? [firstIn, lastIn].filter((p) => p !== "").join(" ") : full;
  const rejected = (reason: string): SePersonTokens => ({
    status: "no_person", reason,
    firstTokens: [], middleTokens: [], lastTokens: [],
    displayFirst: "", displayLast: "", displayName: sourceText,
  });
  if (sourceText === "") return rejected("empty name");
  const tokens = sourceText.split(" ").flatMap(subtokens);
  if (DIGIT.test(sourceText)) return rejected("digits in the name field");
  if (tokens.some((token) => COMPANY_TOKENS.has(token))) {
    return rejected("company suffix in the name field");
  }
  if (hasRolePhrase(tokens)) return rejected("role word in the name field");

  const { firstWords, lastWords } = splitDelivered
    ? {
        firstWords: firstIn === "" ? [] : dropTitles(firstIn.split(" ")).kept,
        lastWords: lastIn === "" ? [] : dropTitles(lastIn.split(" ")).kept,
      }
    : splitFullName(full);
  const givenTokens = firstWords.flatMap(subtokens);
  const lastTokens = lastWords.flatMap(subtokens);
  const firstTokens = givenTokens.slice(0, 1);
  const middleTokens = givenTokens.slice(1);
  const displayFirst = firstWords.join(" ");
  const displayLast = lastWords.join(" ");
  let status: SePersonTokens["status"] = "ok";
  let reason = "";
  if (firstTokens.length === 0 || lastTokens.length === 0) {
    status = "partial";
    reason = "only one name word";
  } else if ([...firstTokens, ...middleTokens].every((token) => token.length === 1)) {
    status = "partial";
    reason = "initials only";
  }
  return {
    status, reason, firstTokens, middleTokens, lastTokens, displayFirst, displayLast,
    displayName: [displayFirst, displayLast].filter((part) => part !== "").join(" "),
  };
}

export interface SePersonIdentity {
  firstTokens: readonly string[];
  middleTokens: readonly string[];
  lastTokens: readonly string[];
  /** `""` when unknown -- a missing year never conflicts. */
  birthYear: string;
  wikidataId: string;
}

function sameTokens(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((token, index) => token === b[index]);
}
function subsetOf(a: readonly string[], b: readonly string[]): boolean {
  return a.every((token) => b.includes(token));
}

/**
 * Spec 5.1's guarded relation, as much of it as one pair can answer: never with two
 * different birth years; a shared QID is enough; otherwise the first and last tokens
 * must be equal and the middle tokens equal or one a subset of the other.
 *
 * The fold's own middle-name test is stricter -- it asks whether the fuller set is the
 * UNIQUE minimal superset among the company's rows, which no pair can know -- so this
 * answers "could fold together". Ruling 1 says where that lands: a Correct that folds
 * back gets no hide rule, and an over-eager `true` costs a duplicate person, not a
 * hidden one.
 */
export function foldsIntoPerson(a: SePersonIdentity, b: SePersonIdentity): boolean {
  if (a.birthYear !== "" && b.birthYear !== "" && a.birthYear !== b.birthYear) return false;
  if (a.wikidataId !== "" && a.wikidataId === b.wikidataId) return true;
  if (!sameTokens(a.firstTokens, b.firstTokens)) return false;
  if (!sameTokens(a.lastTokens, b.lastTokens)) return false;
  return subsetOf(a.middleTokens, b.middleTokens) || subsetOf(b.middleTokens, a.middleTokens);
}

/* ------------------------------------------------------------------ */
/* What a reviewer may type                                             */
/* ------------------------------------------------------------------ */

export interface SePersonRoleInput {
  code: string;
  /** `""` or a four-digit year. */
  fromYear: string;
  toYear: string;
}
export interface SePersonInput {
  firstName: string;
  lastName: string;
  birthYear: string;
  wikidataId: string;
  roles: SePersonRoleInput[];
  /** A JSON object, re-serialized; `"{}"` when the reviewer typed nothing. */
  data: string;
  note: string;
}
export type SePersonValidation =
  | { ok: true; input: SePersonInput }
  | { ok: false; error: string };

function plain(
  label: string,
  value: string,
  max: number,
  control: RegExp = CONTROL,
): string | { error: string } {
  const trimmed = value.trim();
  if (control.test(trimmed)) return { error: `${label} must be plain text.` };
  if (trimmed.length > max) return { error: `${label} is longer than ${max} characters.` };
  return trimmed;
}
function isYear(value: string, min: number, max: number): boolean {
  return /^[0-9]{4}$/.test(value) && Number(value) >= min && Number(value) <= max;
}

/**
 * Spec 7's validation: a name the normalizer scores `ok` (anything else never folds
 * into a person), a plausible birth year and QID, roles from the catalog with sane
 * years, a `data` object without the reserved keys, and a note.
 */
export function validateSePersonInput(
  raw: {
    firstName: string; lastName: string; birthYear: string; wikidataId: string;
    roles: SePersonRoleInput[]; data: string; note: string;
  },
  roleCodes: readonly string[],
  currentYear: number = new Date().getUTCFullYear(),
): SePersonValidation {
  const firstName = plain("First name", raw.firstName ?? "", MAX_NAME_LENGTH);
  if (typeof firstName !== "string") return { ok: false, error: firstName.error };
  const lastName = plain("Last name", raw.lastName ?? "", MAX_NAME_LENGTH);
  if (typeof lastName !== "string") return { ok: false, error: lastName.error };
  const tokens = normalizeSePersonName({ firstName, lastName });
  if (tokens.status !== "ok") return { ok: false, error: tokens.reason };

  const birthYear = (raw.birthYear ?? "").trim();
  if (birthYear !== "" && !isYear(birthYear, MIN_BIRTH_YEAR, currentYear)) {
    return { ok: false, error: `Birth year must be between ${MIN_BIRTH_YEAR} and ${currentYear}.` };
  }
  const wikidataId = (raw.wikidataId ?? "").trim();
  if (wikidataId !== "" && !QID_PATTERN.test(wikidataId)) {
    return { ok: false, error: "Wikidata id must look like Q42." };
  }

  const maxRoleYear = currentYear + ROLE_YEAR_SLACK;
  const entries = (raw.roles ?? []).filter(
    (role) => role.code.trim() !== "" || role.fromYear.trim() !== "" || role.toYear.trim() !== "",
  );
  if (entries.length > MAX_ROLE_ENTRIES) {
    return { ok: false, error: `At most ${MAX_ROLE_ENTRIES} roles.` };
  }
  const roles: SePersonRoleInput[] = [];
  for (const entry of entries) {
    const code = entry.code.trim();
    if (!roleCodes.includes(code)) return { ok: false, error: `Unknown role: ${code}.` };
    const fromYear = entry.fromYear.trim();
    const toYear = entry.toYear.trim();
    for (const [label, year] of [["From year", fromYear], ["To year", toYear]] as const) {
      if (year !== "" && !isYear(year, MIN_ROLE_YEAR, maxRoleYear)) {
        return { ok: false, error: `${label} must be between ${MIN_ROLE_YEAR} and ${maxRoleYear}.` };
      }
    }
    if (fromYear !== "" && toYear !== "" && Number(toYear) < Number(fromYear)) {
      return { ok: false, error: "A role cannot end before it starts." };
    }
    roles.push({ code, fromYear, toYear });
  }

  const dataText = (raw.data ?? "").trim();
  if (dataText.length > MAX_DATA_LENGTH) {
    return { ok: false, error: `Data is longer than ${MAX_DATA_LENGTH} characters.` };
  }
  let data = "{}";
  if (dataText !== "") {
    let parsed: unknown;
    try {
      parsed = JSON.parse(dataText);
    } catch {
      return { ok: false, error: "Data must be a JSON object." };
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ok: false, error: "Data must be a JSON object." };
    }
    for (const key of RESERVED_DATA_KEYS) {
      if (Object.hasOwn(parsed as object, key)) {
        return { ok: false, error: `Data may not carry the reserved key ${key}.` };
      }
    }
    data = JSON.stringify(parsed);
  }

  const note = plain("Note", raw.note ?? "", MAX_NOTE_LENGTH, NOTE_CONTROL);
  if (typeof note !== "string") return { ok: false, error: note.error };
  return { ok: true, input: { firstName, lastName, birthYear, wikidataId, roles, data, note } };
}

/** The note rules of spec 7, in one place: line breaks and tabs pass, every other
 * control character and anything past 500 characters does not. `validateSePersonInput`
 * uses `plain(...)` directly; the decision parser -- whose note reaches a rule row or a
 * `data.note` without ever passing through the sheet's validation -- calls this. */
export function noteError(note: string): string | null {
  const checked = plain("Note", note, MAX_NOTE_LENGTH, NOTE_CONTROL);
  return typeof checked === "string" ? null : checked.error;
}

/** Dagster's selection (`batch.py::_changed_company_ids`) in miniature: a non-draft
 * normalized version, a non-draft raw row, a rule version or one of the company's own
 * precedence rows newer than the newest fold, or foldable rows with no fold at all.
 * Drafts and the GLOBAL precedence export are never among `stamps` (Ruling 7). */
export function personFoldPending(
  foldedAt: string | null,
  stamps: readonly string[],
  hasFoldable: boolean,
): boolean {
  if (foldedAt === null) return hasFoldable;
  return stamps.some((stamp) => stamp > foldedAt);
}
```

`app/lib/se-person-decision-form.ts`:

```ts
/**
 * Turns the People tab's form posts into one decision (spec 2026-09-09 section 7).
 * Client-safe. Eight intents: `save-draft` writes or edits a reviewer draft (with
 * `replaces_key` for a Correct), `activate` and `discard` act on a draft's group slot,
 * `remove` and `reset` on a published key, `merge` on two or more keys, `split` on the
 * checked member slots, `fold-now` launches the targeted fold.
 *
 * `roleCodes` comes from the catalog the loader read (Ruling 3): a client-safe module
 * may not read ClickHouse, and a role code is only valid if the catalog has it.
 */
import {
  PERSON_GROUP_SLOT_PATTERN,
  isPersonKey,
  noteError,
  validateSePersonInput,
  type SePersonInput,
  type SePersonRoleInput,
} from "~/lib/se-person-fields";

export type SePersonDecision =
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SePersonInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string }
  | { intent: "remove"; personKey: string; note: string }
  | { intent: "merge"; personKeys: string[]; note: string }
  | { intent: "split"; slots: string[]; note: string }
  | { intent: "reset"; personKey: string; note: string }
  | { intent: "fold-now" };
export type SePersonDecisionRequest =
  | { ok: true; decision: SePersonDecision }
  | { ok: false; error: string };

/** A member slot as its source minted it (`uid:uid`, `Q1:P169:Q2`, `r...01`). */
const MAX_SLOT_LENGTH = 200;
const CONTROL = /[\u0000-\u001f\u007f]/;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}
function all(form: FormData, name: string): string[] {
  return form.getAll(name).filter((value): value is string => typeof value === "string");
}
function refuse(error: string): SePersonDecisionRequest {
  return { ok: false, error };
}
function noteOrRefuse(form: FormData): { ok: true; note: string } | { ok: false; error: string } {
  const note = text(form, "note").trim();
  // Length AND plainness: a note posted from a dialog reaches `rule.note` or a row's
  // `data.note` without passing through `validateSePersonInput`, and the Global
  // Constraint holds for both paths.
  const error = noteError(note);
  if (error !== null) return { ok: false, error };
  return { ok: true, note };
}
/** The sheet posts one `role_code`, `role_from` and `role_to` per rendered row, so the
 * three lists are index-parallel; a row with nothing in it is dropped by the validator. */
function roleRows(form: FormData): SePersonRoleInput[] {
  const codes = all(form, "role_code");
  const from = all(form, "role_from");
  const to = all(form, "role_to");
  return codes.map((code, index) => ({
    code,
    fromYear: from[index] ?? "",
    toYear: to[index] ?? "",
  }));
}

export function parseSePersonDecision(
  form: FormData,
  roleCodes: readonly string[],
): SePersonDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent === "remove" || intent === "reset") {
    const personKey = text(form, "person_key");
    if (!isPersonKey(personKey)) return refuse("Unknown person.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, personKey, note: note.note } };
  }
  if (intent === "activate" || intent === "discard") {
    const slot = text(form, "slot");
    if (!PERSON_GROUP_SLOT_PATTERN.test(slot)) return refuse("Unknown draft.");
    if (intent === "discard") return { ok: true, decision: { intent, slot } };
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slot, note: note.note } };
  }
  if (intent === "merge") {
    const personKeys = [...new Set(all(form, "person_key"))].sort();
    if (personKeys.some((key) => !isPersonKey(key))) return refuse("Unknown person.");
    if (personKeys.length < 2) return refuse("Pick at least two persons to merge.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, personKeys, note: note.note } };
  }
  if (intent === "split") {
    const slots = [...new Set(all(form, "slot").map((slot) => slot.trim()))].sort();
    if (slots.length === 0) return refuse("Pick at least one observation to split off.");
    if (slots.some((slot) => slot === "" || slot.length > MAX_SLOT_LENGTH || CONTROL.test(slot))) {
      return refuse("Unknown observation.");
    }
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slots, note: note.note } };
  }
  if (intent === "save-draft") {
    const validated = validateSePersonInput(
      {
        firstName: text(form, "first_name"),
        lastName: text(form, "last_name"),
        birthYear: text(form, "birth_year"),
        wikidataId: text(form, "wikidata_id"),
        roles: roleRows(form),
        data: text(form, "data"),
        note: text(form, "note"),
      },
      roleCodes,
    );
    if (!validated.ok) return refuse(validated.error);
    const slotText = text(form, "slot");
    if (slotText !== "" && !PERSON_GROUP_SLOT_PATTERN.test(slotText)) return refuse("Unknown draft.");
    const replacesText = text(form, "replaces_key");
    if (replacesText !== "" && !isPersonKey(replacesText)) return refuse("Unknown person.");
    return {
      ok: true,
      decision: {
        intent,
        slot: slotText === "" ? null : slotText,
        replacesKey: replacesText === "" ? null : replacesText,
        input: validated.input,
      },
    };
  }
  return refuse("Unknown intent.");
}
```

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-person-fields.test.ts tests/se-person-decision-form.test.ts && npm run typecheck`
Expected: PASS, and `tsc` clean.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-person-tables.ts app/lib/se-person-fields.ts app/lib/se-person-decision-form.ts \
  tests/se-person-fields.test.ts tests/se-person-decision-form.test.ts
git commit -m "feat(backoffice): person entity catalogue, validation and decision parser"
```

---

### Task 2: The server module over the six tables

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts`
- Modify: `app/lib/clickhouse.server.ts` (two inserters), `app/lib/dagster.server.ts` (one constant)
- Test: `corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts`

**Interfaces:**
- Consumes: `chQuery` and the `getWriteClient` insert pattern (`chInsertSeCompanyAddressSuggestions` in `clickhouse.server.ts`), `launchRun`, `dagsterRunUrl`, `ASSET_JOB_NAME` (`dagster.server.ts`), `getCompanyPersonRoleTypes` (`app/lib/company-roles.server.ts`), `clickhouseStamp` from `se-basic-info.server.ts` (a pure helper; import it), and Task 1's `SE_COMPANY_PERSON_TABLE`, `personFoldPending`, `personGroupSlot`, `personRowSlot`, `groupOfRowSlot`, `normalizeSePersonName`, `foldsIntoPerson`, `isPersonKey`, `SePersonDecision`, `SePersonInput`, `SePersonRoleOption`.
- Produces: the interface block's types and functions. Tables: `corpscout.se_company_person_v2` (main, the 29 columns of `tables.MAIN_COLUMNS`), `_normalized` (23), `_suggestion` (18), `_history` (29 + `changed_at`, `change_kind`, `fold_run_id`), `_rule` (9), `_precedence` (8).

- [ ] **Step 1: Write the failing tests**

`tests/se-company-person-entity.server.test.ts` — mocked exactly as `tests/se-company-address-entity.server.test.ts` mocks its two modules:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  query: vi.fn(),
  insertSuggestions: vi.fn(),
  insertRules: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeCompanyPersonSuggestions: clickhouse.insertSuggestions,
  chInsertSeCompanyPersonRules: clickhouse.insertRules,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => null) }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  dagsterRunUrl: dagster.dagsterRunUrl,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET: "se_company_person_fold_companies",
}));
const roles = vi.hoisted(() => ({ getCompanyPersonRoleTypes: vi.fn() }));
vi.mock("~/lib/company-roles.server", () => roles);

import {
  activateSePersonDraft,
  discardSePersonDraft,
  launchSePersonFold,
  loadSePersonDetail,
  loadSePersonRoleOptions,
  mergeSePersons,
  PERSON_HISTORY_SQL,
  PERSON_MAIN_SQL,
  PERSON_NORMALIZED_SQL,
  PERSON_PRECEDENCE_SQL,
  PERSON_RAW_SQL,
  PERSON_RULES_SQL,
  removeSePerson,
  resetSePersonRules,
  saveSePersonDraft,
  SePersonDecisionError,
  splitSePersonSlots,
  type SePersonNormalizedRow,
  type SePersonRawRow,
  type SePersonRow,
  type SePersonRuleRow,
} from "~/lib/se-company-person-entity.server";

const COMPANY = "5560000001";
const MERGED_KEY = "a".repeat(64);
const WIKI_KEY = "b".repeat(64);
const UNKNOWN_KEY = "e".repeat(64);
const NOW = new Date("2026-09-10T12:00:00.123Z");
const STAMP = "2026-09-10 12:00:00.123";
const GROUP = "r20260910120000123";
/** sha256("<company>\n<source>\n<slot>\n<stamp>"), computed outside the module so a
 * changed preimage fails here instead of agreeing with itself. */
const DRAFT_ID_1 = "e5d1712bee6930abe9769b83f408082a074c412204ebfe089fb1211869e223ec";
const DRAFT_ID_2 = "5c806db8df550ccd76e3bdd3a350d87e559ca24218601a723204db1c1ba98bff";
const REVIEWER_ID_1 = "63d6b632d69f4e0eebecbec6b020eaf48360bafc5e5e5d1c03b9aee7aa954e2c";
const REVIEWER_ID_2 = "b1c15bf09aacdf64b49b819950b9f935fad537aeb62608cf1b32d71a22e23cc7";
/** sha256("<company>\n<kind>\n<sorted keys, comma-joined>\n<sorted slots, comma-joined>\n<stamp>"). */
const HIDE_RULE_ID = "06cb08340aba69721e3f4e916b79bf183495e26dc3d745082c35846b34f35da7";
const MERGE_RULE_ID = "e8c13b618f94caab5d684d67021a88b05f89ed5847f545a53f5a0c6c529e5d4e";
const SPLIT_RULE_ID = "ffb849959a8aaaca818fc0c815eeec07f7694162488be51b6bb09137e2591ffc";

function main(over: Partial<SePersonRow>): SePersonRow {
  return {
    company_id: COMPANY, person_key: MERGED_KEY,
    display_name: "Anna Svensson", first_name: "Anna", last_name: "Svensson",
    birth_year: "1975", wikidata_id: "",
    sources: ["bolagsverket"], slots: ["uid-1:sig-1"], normalized_ids: ["n1"],
    member_sources: ["bolagsverket"], member_slots: ["uid-1:sig-1"],
    member_names: ["Anna Svensson"], member_birth_years: ["1975"],
    member_wikidata_ids: [""], member_data: ['{"role_kind":"board_member"}'],
    role_codes: ["board_member"], role_years: [2025], role_sources: [["bolagsverket"]],
    current_roles: ["board_member"], first_year: "2025", last_year: "2025",
    text_source: "bolagsverket", data: '{"role_kind":"board_member"}',
    active: 1, inactive_reason: "",
    folded_at: "2026-09-10 09:00:00.000", fold_version: "se-person-fold-v1",
    source_run_id: "run-fold",
    ...over,
  };
}
function normalized(over: Partial<SePersonNormalizedRow>): SePersonNormalizedRow {
  return {
    company_id: COMPANY, source: "bolagsverket", slot: "uid-1:sig-1",
    suggestion_id: "sid1", normalized_id: "n1", normalizer_version: "se-person-normalizer-v1",
    parse_status: "ok", parse_notes: [],
    first_tokens: ["anna"], middle_tokens: [], last_tokens: ["svensson"],
    display_first: "Anna", display_last: "Svensson", display_name: "Anna Svensson",
    birth_year: "1975", wikidata_id: "", role_code: "board_member", role_key: "board_member",
    role_year: "2025", role_from: "", role_to: "", data: '{"role_kind":"board_member"}',
    normalized_at: "2026-09-10 08:00:00.000",
    ...over,
  };
}
function raw(over: Partial<SePersonRawRow>): SePersonRawRow {
  return {
    company_id: COMPANY, source: "bolagsverket", slot: "uid-1:sig-1", suggestion_id: "sid1",
    suggested_at: "2026-09-10 07:00:00.000", source_record_id: "uid-1",
    full_name: "", first_name: "Anna", last_name: "Svensson", birth_year: "1975",
    wikidata_id: "", role_original: "Styrelseledamot", role_key: "board_member",
    fiscal_year: "2025", role_from: "", role_to: "", document_ref: "doc-1",
    data: '{"role_kind":"board_member"}',
    ...over,
  };
}

// The merged person: Bolagsverket's split spelling plus an ESEF full name whose
// current normalized version (n2b) is newer than the one the row was folded from (n2).
const ESEF_NORMALIZED = normalized({
  source: "esef", slot: "doc-9:cand-1", normalized_id: "n2b", suggestion_id: "sid2",
  first_tokens: ["anna"], middle_tokens: ["maria"], last_tokens: ["svensson"],
  display_first: "Anna Maria", display_last: "Svensson", display_name: "Anna Maria Svensson",
  role_code: "board_chair", role_key: "board_chair", role_year: "2025",
  data: '{"title":"Chair"}', normalized_at: "2026-09-10 10:00:00.000",
});
const WIKI_NORMALIZED = normalized({
  source: "wikidata", slot: "Q1:P169:Q7", normalized_id: "n3", suggestion_id: "sid3",
  first_tokens: ["carl"], middle_tokens: [], last_tokens: ["von", "essen"],
  display_first: "Carl", display_last: "von Essen", display_name: "Carl von Essen",
  birth_year: "", wikidata_id: "Q7", role_code: "chief_executive_officer",
  role_key: "P169", role_year: "", role_from: "2019-05-01", role_to: "",
  data: '{"description":"executive"}', normalized_at: "2026-09-10 08:00:00.000",
});
const DRAFT_NORMALIZED = normalized({
  source: "reviewer_draft", slot: `${GROUP}01`, normalized_id: "n4", suggestion_id: DRAFT_ID_1,
  role_code: "board_member", role_key: "board_member", role_year: "2024",
  data: '{"decided_by":"backoffice","note":"seen in the annual report"}',
  normalized_at: "2026-09-10 11:00:00.000",
});
const NORMALIZED_ROWS = [normalized({}), ESEF_NORMALIZED, WIKI_NORMALIZED, DRAFT_NORMALIZED];

const ESEF_RAW = raw({
  source: "esef", slot: "doc-9:cand-1", suggestion_id: "sid2", source_record_id: "doc-9",
  full_name: "Anna Maria Svensson", first_name: "", last_name: "", birth_year: "",
  role_original: "Chair of the board", role_key: "board_chair", fiscal_year: "2025",
  document_ref: "doc-9", data: '{"title":"Chair"}',
});
const WIKI_RAW = raw({
  source: "wikidata", slot: "Q1:P169:Q7", suggestion_id: "sid3", source_record_id: "Q7",
  full_name: "Carl von Essen", first_name: "", last_name: "", birth_year: "",
  wikidata_id: "Q7", role_original: "chief executive officer", role_key: "P169",
  fiscal_year: "", role_from: "2019-05-01", document_ref: "", data: '{"description":"executive"}',
});
/** A Correct in progress: the draft's data carries Ruling 4's reserved keys. */
const DRAFT_RAW_1 = raw({
  source: "reviewer_draft", slot: `${GROUP}01`, suggestion_id: DRAFT_ID_1,
  suggested_at: STAMP, source_record_id: "", first_name: "Anna", last_name: "Svensson",
  birth_year: "1975", role_original: "board_member", role_key: "board_member",
  fiscal_year: "2024", document_ref: "",
  data: `{"decided_by":"backoffice","note":"seen in the annual report","replaces_key":"${MERGED_KEY}"}`,
});
const DRAFT_RAW_2 = raw({
  ...DRAFT_RAW_1, slot: `${GROUP}02`, suggestion_id: DRAFT_ID_2,
  role_original: "board_chair", role_key: "board_chair", fiscal_year: "", role_from: "2025-01-01",
});
const RAW_ROWS = [raw({}), ESEF_RAW, WIKI_RAW, DRAFT_RAW_1, DRAFT_RAW_2];

const MERGED_ROW = main({
  sources: ["bolagsverket", "esef"], slots: ["uid-1:sig-1", "doc-9:cand-1"],
  normalized_ids: ["n1", "n2"],
  member_sources: ["bolagsverket", "esef"], member_slots: ["uid-1:sig-1", "doc-9:cand-1"],
  member_names: ["Anna Svensson", "Anna Maria Svensson"], member_birth_years: ["1975", ""],
  member_wikidata_ids: ["", ""],
  member_data: ['{"role_kind":"board_member"}', '{"title":"Chair"}'],
  role_codes: ["board_chair", "board_member"], role_years: [2025, 2025],
  role_sources: [["esef"], ["bolagsverket"]], current_roles: ["board_chair", "board_member"],
  data: '{"role_kind":"board_member","title":"Chair"}',
});
const WIKI_ROW = main({
  person_key: WIKI_KEY, display_name: "Carl von Essen", first_name: "Carl", last_name: "von Essen",
  birth_year: "", wikidata_id: "Q7",
  sources: ["wikidata"], slots: ["Q1:P169:Q7"], normalized_ids: ["n3"],
  member_sources: ["wikidata"], member_slots: ["Q1:P169:Q7"], member_names: ["Carl von Essen"],
  member_birth_years: [""], member_wikidata_ids: ["Q7"],
  member_data: ['{"description":"executive"}'],
  role_codes: ["chief_executive_officer"], role_years: [2026],
  role_sources: [["wikidata"]], current_roles: ["chief_executive_officer"],
  first_year: "2019", last_year: "2026", text_source: "wikidata",
  data: '{"description":"executive"}',
});
const HISTORY_ROW = {
  ...MERGED_ROW, folded_at: "2026-09-09 09:00:00.000",
  changed_at: "2026-09-10 09:00:00.000", change_kind: "updated", fold_run_id: "run-fold",
};
const MERGE_RULE: SePersonRuleRow = {
  company_id: COMPANY, rule_id: "f".repeat(64), kind: "merge",
  person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 1,
  note: "same person", created_at: "2026-09-09 12:00:00.000", created_by: "backoffice",
};
const PRECEDENCE_ROWS = [
  { company_id: "", field: "name", source: "reviewer", precedence: 20000, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
  { company_id: "", field: "name", source: "bolagsverket", precedence: 900, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
  { company_id: "", field: "name", source: "esef", precedence: 400, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
];

function answer(sql: string): unknown[] {
  if (sql.includes("FROM corpscout.se_company_person_v2 AS m FINAL")) return [MERGED_ROW, WIKI_ROW];
  if (sql.includes("FROM corpscout.se_company_person_history")) return [HISTORY_ROW];
  if (sql.includes("FROM corpscout.se_company_person_normalized")) return NORMALIZED_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_suggestion")) return RAW_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_rule")) return [MERGE_RULE];
  if (sql.includes("FROM corpscout.se_company_person_precedence")) return PRECEDENCE_ROWS;
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}
/** The rows one insert call got. */
function inserted(mock: { mock: { calls: unknown[][] } }, call = 0): Record<string, unknown>[] {
  return mock.mock.calls[call]?.[0] as Record<string, unknown>[];
}
```

Then the cases, each its own `it`:

```ts
describe("se-company-person-entity.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => answer(sql));
    clickhouse.insertSuggestions.mockReset();
    clickhouse.insertRules.mockReset();
    dagster.launchRun.mockReset().mockResolvedValue({ runId: "run-9", status: "STARTED" });
    roles.getCompanyPersonRoleTypes.mockReset().mockResolvedValue([
      { role_code: "board_member", display_name: "Board member", role_group: "governance", description: "", is_active: 1, created_at: "", updated_at: "" },
      { role_code: "retired_code", display_name: "Retired", role_group: "governance", description: "", is_active: 0, created_at: "", updated_at: "" },
    ]);
  });

  it("pins every read to the entity's tables, FINAL where a current version is needed, the company parameter and string keys", () => {
    expect(PERSON_MAIN_SQL).toContain("FROM corpscout.se_company_person_v2 AS m FINAL");
    expect(PERSON_MAIN_SQL).toContain("WHERE m.company_id = {companyId:String}");
    expect(PERSON_MAIN_SQL).toContain("toString(m.person_key) AS person_key");
    expect(PERSON_MAIN_SQL).toContain("arrayMap(x -> toString(x), m.normalized_ids) AS normalized_ids");
    expect(PERSON_MAIN_SQL).toContain("arrayMap(x -> ifNull(toString(x), ''), m.member_birth_years) AS member_birth_years");
    expect(PERSON_MAIN_SQL).toContain("toUInt8(m.active) AS active");
    expect(PERSON_MAIN_SQL).toContain("ORDER BY m.active DESC, m.inactive_reason, m.display_name");
    for (const column of ["birth_year", "wikidata_id", "first_year", "last_year"]) {
      expect(PERSON_MAIN_SQL).toContain(`AS ${column}`);
    }
    // History is append-only: never FINAL, newest first, capped, and it carries the
    // three columns the main table does not.
    expect(PERSON_HISTORY_SQL).toContain("FROM corpscout.se_company_person_history AS h");
    expect(PERSON_HISTORY_SQL).not.toContain("FINAL");
    expect(PERSON_HISTORY_SQL).toContain("ORDER BY h.changed_at DESC");
    expect(PERSON_HISTORY_SQL).toContain("LIMIT 200");
    expect(PERSON_HISTORY_SQL).toContain("toString(h.changed_at) AS changed_at");
    expect(PERSON_HISTORY_SQL).toContain("toString(h.change_kind) AS change_kind");
    expect(PERSON_NORMALIZED_SQL).toContain("FROM corpscout.se_company_person_normalized AS n FINAL");
    expect(PERSON_NORMALIZED_SQL).toContain("n.first_tokens AS first_tokens");
    expect(PERSON_NORMALIZED_SQL).toContain("ifNull(n.role_code, '') AS role_code");
    expect(PERSON_NORMALIZED_SQL).toContain("ifNull(toString(n.role_from), '') AS role_from");
    expect(PERSON_RAW_SQL).toContain("FROM corpscout.se_company_person_suggestion AS s FINAL");
    expect(PERSON_RAW_SQL).toContain("ifNull(s.full_name, '') AS full_name");
    expect(PERSON_RAW_SQL).toContain("ifNull(toString(s.fiscal_year), '') AS fiscal_year");
    expect(PERSON_RAW_SQL).toContain("s.data AS data");
    expect(PERSON_RULES_SQL).toContain("FROM corpscout.se_company_person_rule AS r FINAL");
    expect(PERSON_RULES_SQL).toContain("arrayMap(x -> toString(x), r.person_keys) AS person_keys");
    expect(PERSON_RULES_SQL).toContain("toUInt8(r.active) AS active");
    expect(PERSON_RULES_SQL).toContain("ORDER BY r.created_at DESC");
    // The global order and the company's own, one read (spec 3.6).
    expect(PERSON_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_person_precedence AS p FINAL");
    expect(PERSON_PRECEDENCE_SQL).toContain("WHERE p.company_id IN ('', {companyId:String}) AND p.field = 'name'");
    for (const sql of [PERSON_MAIN_SQL, PERSON_HISTORY_SQL, PERSON_NORMALIZED_SQL, PERSON_RAW_SQL, PERSON_RULES_SQL, PERSON_PRECEDENCE_SQL]) {
      expect(sql).toContain("{companyId:String}");
    }
  });

  it("assembles the published persons, their members, roles, rules and the drafts", async () => {
    const detail = await loadSePersonDetail(COMPANY);
    expect(detail?.published.map((entry) => entry.row.person_key)).toEqual([MERGED_KEY, WIKI_KEY]);
    const merged = detail?.published[0];
    expect(merged?.members).toEqual([
      {
        source: "bolagsverket", slot: "uid-1:sig-1", normalizedId: "n1",
        name: "Anna Svensson", birthYear: "1975", wikidataId: "",
        data: '{"role_kind":"board_member"}',
        current: NORMALIZED_ROWS[0], raw: RAW_ROWS[0], refoldPending: false, precedence: 900,
      },
      {
        source: "esef", slot: "doc-9:cand-1", normalizedId: "n2",
        name: "Anna Maria Svensson", birthYear: "", wikidataId: "",
        data: '{"title":"Chair"}',
        // The current normalized version is not the one this row was folded from.
        current: ESEF_NORMALIZED, raw: ESEF_RAW, refoldPending: true, precedence: 400,
      },
    ]);
    // The roles block, zipped out of the three parallel arrays (spec 5.4).
    expect(merged?.roles).toEqual([
      { code: "board_chair", year: 2025, sources: ["esef"] },
      { code: "board_member", year: 2025, sources: ["bolagsverket"] },
    ]);
    // Bolagsverket 900 beats ESEF 400 outright, so precedence explains the spelling.
    expect(merged?.spellingReason).toBe("precedence");
    expect(detail?.published[1]?.spellingReason).toBe("single source");
    // The active merge rule names both keys, so it is attached to both persons.
    expect(merged?.rules).toEqual([MERGE_RULE]);
    expect(detail?.published[1]?.rules).toEqual([MERGE_RULE]);
    expect(detail?.history).toEqual([HISTORY_ROW]);
    expect(detail?.rules).toEqual([MERGE_RULE]);
    expect(detail?.precedence).toEqual(PRECEDENCE_ROWS);
    // The draft's two rows are ONE draft, grouped by the slot's first 18 characters,
    // carrying Ruling 4's note and replaced key out of `data`.
    expect(detail?.drafts).toEqual([
      {
        slot: GROUP, rows: [DRAFT_RAW_1, DRAFT_RAW_2], normalized: [DRAFT_NORMALIZED],
        name: "Anna Svensson", note: "seen in the annual report", replacesKey: MERGED_KEY,
      },
    ]);
    for (const sql of [PERSON_MAIN_SQL, PERSON_HISTORY_SQL, PERSON_NORMALIZED_SQL, PERSON_RAW_SQL, PERSON_RULES_SQL, PERSON_PRECEDENCE_SQL]) {
      expect(clickhouse.query.mock.calls.find(([text]) => text === sql)?.[1]).toEqual({ companyId: COMPANY });
    }
  });

  it("calls the spelling a tie-break, or the most complete, when two members rank the same", async () => {
    // A REAL tie, the one `fold.py::_text_member` actually resolves by something other
    // than completeness: two Bolagsverket members (precedence 900 each) whose spellings
    // have the same token count, so the longer string wins and neither is "more
    // complete" than the other.
    const tied = main({
      ...MERGED_ROW,
      sources: ["bolagsverket"],
      member_sources: ["bolagsverket", "bolagsverket"],
      member_slots: ["uid-1:sig-1", "uid-2:sig-1"],
      member_names: ["Anna Svensson", "Anna Svenson"],
      display_name: "Anna Svensson",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [tied] : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.published[0]?.spellingReason).toBe("tie-break");

    // The other branch of the same tie: same precedence, and the published spelling has
    // strictly more words than the other member -- which is how the fold broke it.
    const fuller = main({
      ...tied,
      member_names: ["Anna Maria Svensson", "Anna Svensson"],
      display_name: "Anna Maria Svensson",
      first_name: "Anna Maria",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [fuller] : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.published[0]?.spellingReason).toBe("most complete");
  });

  it("is fold-pending on a newer normalized row, raw row or rule, never on a draft or the global precedence", async () => {
    // ESEF's normalized_at (10:00) is newer than the fold (09:00).
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);

    const settled = (sql: string) => {
      if (sql === PERSON_NORMALIZED_SQL) {
        return [normalized({}), { ...ESEF_NORMALIZED, normalized_at: "2026-09-10 08:00:00.000" }, WIKI_NORMALIZED, DRAFT_NORMALIZED];
      }
      if (sql === PERSON_RULES_SQL) return [];
      return answer(sql);
    };
    clickhouse.query.mockImplementation(async (sql: string) => settled(sql));
    // Only the draft (11:00) and the global precedence export (06:00) are left, and
    // neither selects a company for the fold (Ruling 7).
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(false);

    // A reviewer row the normalize step has not seen yet still owes a fold.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [...RAW_ROWS, raw({ source: "reviewer", slot: `${GROUP}01`, suggested_at: STAMP })]
        : settled(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);

    // So does a rule, active or released.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [{ ...MERGE_RULE, active: 0, created_at: "2026-09-10 11:30:00.000" }]
        : settled(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);
  });

  it("returns null only when there is no main row, no normalized row and no draft", async () => {
    clickhouse.query.mockImplementation(async () => []);
    expect(await loadSePersonDetail(COMPANY)).toBeNull();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL ? [DRAFT_RAW_1] : [],
    );
    expect((await loadSePersonDetail(COMPANY))?.drafts).toHaveLength(1);
  });

  it("offers only the catalog's active roles, as code, label and group", async () => {
    expect(await loadSePersonRoleOptions()).toEqual([
      { code: "board_member", label: "Board member", group: "governance" },
    ]);
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run: `npx vitest run tests/se-company-person-entity.server.test.ts`
Expected: FAIL — `Failed to resolve import "~/lib/se-company-person-entity.server"`.

The write cases, in the same `describe`:

```ts
  it("saves a draft as one row per role, under a group slot, with Ruling 4's data keys", async () => {
    const result = await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: null, replacesKey: MERGED_KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "",
          roles: [
            { code: "board_member", fromYear: "2024", toYear: "2024" },
            { code: "board_chair", fromYear: "2025", toYear: "" },
          ],
          data: '{"source":"annual report"}', note: "seen in the annual report",
        },
      },
      NOW,
    );
    expect(result).toEqual({ decidedAt: STAMP, slot: GROUP });
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const data = `{"source":"annual report","decided_by":"backoffice","note":"seen in the annual report","replaces_key":"${MERGED_KEY}"}`;
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY, source: "reviewer_draft", slot: `${GROUP}01`,
        suggestion_id: DRAFT_ID_1, suggested_at: STAMP, source_record_id: "",
        full_name: null, first_name: "Anna", last_name: "Svensson", birth_year: 1975,
        wikidata_id: null, role_original: "board_member", role_key: "board_member",
        // Ruling 5: one year is a fiscal year, a span is two dates.
        fiscal_year: 2024, role_from: null, role_to: null, document_ref: null, data,
      },
      {
        company_id: COMPANY, source: "reviewer_draft", slot: `${GROUP}02`,
        suggestion_id: DRAFT_ID_2, suggested_at: STAMP, source_record_id: "",
        full_name: null, first_name: "Anna", last_name: "Svensson", birth_year: 1975,
        wikidata_id: null, role_original: "board_chair", role_key: "board_chair",
        fiscal_year: null, role_from: "2025-01-01", role_to: null, document_ref: null, data,
      },
    ]);
  });

  it("writes one row with no role columns at all when the reviewer names no role", async () => {
    await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: null, replacesKey: null,
        input: { firstName: "Anna", lastName: "Svensson", birthYear: "", wikidataId: "Q7", roles: [], data: "{}", note: "" },
      },
      NOW,
    );
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      expect.objectContaining({
        slot: `${GROUP}01`, birth_year: null, wikidata_id: "Q7",
        role_original: null, role_key: null, fiscal_year: null, role_from: null, role_to: null,
        data: '{"decided_by":"backoffice","note":""}',
      }),
    ]);
  });

  it("tombstones the rows an edited draft no longer uses", async () => {
    // The stored draft has two rows; the edit keeps one, so 02 must be cleared or it
    // would stay a live observation of a role the reviewer just deleted.
    await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: GROUP, replacesKey: MERGED_KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "",
          roles: [{ code: "board_member", fromYear: "2024", toYear: "2024" }],
          data: "{}", note: "",
        },
      },
      NOW,
    );
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows).toHaveLength(2);
    expect(rows[1]).toEqual(
      expect.objectContaining({
        slot: `${GROUP}02`, source: "reviewer_draft", full_name: null, first_name: null,
        last_name: null, birth_year: null, wikidata_id: null, role_original: null,
        role_key: null, fiscal_year: null, role_from: null, role_to: null,
        data: '{"decided_by":"backoffice","note":"role removed"}',
      }),
    );
  });

  it("activates a draft into reviewer rows and clears it, in one insert", async () => {
    await activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "confirmed" }, NOW);
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows).toHaveLength(4);
    // Ruling 4: `replaces_key` never reaches the published row.
    expect(rows[0]).toEqual({
      company_id: COMPANY, source: "reviewer", slot: `${GROUP}01`, suggestion_id: REVIEWER_ID_1,
      suggested_at: STAMP, source_record_id: "", full_name: null, first_name: "Anna",
      last_name: "Svensson", birth_year: 1975, wikidata_id: null,
      role_original: "board_member", role_key: "board_member", fiscal_year: 2024,
      role_from: null, role_to: null, document_ref: null,
      data: '{"decided_by":"backoffice","note":"confirmed"}',
    });
    expect(rows[1]).toEqual(expect.objectContaining({ slot: `${GROUP}02`, suggestion_id: REVIEWER_ID_2, role_key: "board_chair", role_from: "2025-01-01" }));
    expect(rows[2]).toEqual(expect.objectContaining({ source: "reviewer_draft", slot: `${GROUP}01`, first_name: null, data: '{"decided_by":"backoffice","note":"activated"}' }));
    expect(rows[3]).toEqual(expect.objectContaining({ source: "reviewer_draft", slot: `${GROUP}02`, first_name: null }));
    // Ruling 1: this correction folds back into the person it corrects (same tokens,
    // same birth year), so hiding that person would hide the correction with it.
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("hides the corrected person only when the correction does not fold back into it", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [raw({}), ESEF_RAW, WIKI_RAW, { ...DRAFT_RAW_1, last_name: "Svenson" }, { ...DRAFT_RAW_2, last_name: "Svenson" }]
        : answer(sql),
    );
    await activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY],
        slots: [], active: 1, note: "corrected by reviewer",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
  });

  it("refuses to activate or discard a draft that is not there", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL ? [raw({})] : answer(sql),
    );
    await expect(activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW)).rejects.toBeInstanceOf(SePersonDecisionError);
    await expect(discardSePersonDraft(COMPANY, { intent: "discard", slot: GROUP }, NOW)).rejects.toBeInstanceOf(SePersonDecisionError);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("discards a draft by clearing every row of its group", async () => {
    await discardSePersonDraft(COMPANY, { intent: "discard", slot: GROUP }, NOW);
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows.map((row) => row.slot)).toEqual([`${GROUP}01`, `${GROUP}02`]);
    expect(rows.every((row) => row.first_name === null && row.data === '{"decided_by":"backoffice","note":"discarded"}')).toBe(true);
  });

  it("removes a source-backed person with a hide rule and a reviewer-only one with tombstones", async () => {
    await removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY],
        slots: [], active: 1, note: "removed by reviewer", created_at: STAMP, created_by: "backoffice",
      },
    ]);

    clickhouse.insertRules.mockReset();
    const reviewerOnly = main({
      person_key: MERGED_KEY, sources: ["reviewer"], slots: [`${GROUP}01`, `${GROUP}02`],
      member_sources: ["reviewer", "reviewer"], member_slots: [`${GROUP}01`, `${GROUP}02`],
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [reviewerOnly] : sql === PERSON_RULES_SQL ? [] : answer(sql),
    );
    await removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "left" }, NOW);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      expect.objectContaining({ source: "reviewer", slot: `${GROUP}01`, suggestion_id: REVIEWER_ID_1, first_name: null, data: '{"decided_by":"backoffice","note":"left"}' }),
      expect.objectContaining({ source: "reviewer", slot: `${GROUP}02`, suggestion_id: REVIEWER_ID_2, first_name: null }),
    ]);
  });

  it("refuses Remove on an unknown person, a hidden one, and one a rule already hides", async () => {
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: UNKNOWN_KEY, note: "" }, NOW)).rejects.toThrow("Unknown person.");
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [main({ active: 0, inactive_reason: "hidden" })] : answer(sql),
    );
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("Already hidden.");
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [{ ...MERGE_RULE, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY] }]
        : answer(sql),
    );
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("Already hidden.");
  });

  it("writes one merge rule over the chosen keys and one split rule over the chosen slots", async () => {
    await mergeSePersons(COMPANY, { intent: "merge", personKeys: [MERGED_KEY, WIKI_KEY], note: "same person" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: MERGE_RULE_ID, kind: "merge",
        person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 1, note: "same person",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
    clickhouse.insertRules.mockReset();
    await splitSePersonSlots(COMPANY, { intent: "split", slots: ["doc-9:cand-1"], note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: SPLIT_RULE_ID, kind: "split", person_keys: [],
        slots: ["doc-9:cand-1"], active: 1, note: "split by reviewer",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
  });

  it("refuses a merge naming a person that is not published, and a split that changes nothing", async () => {
    await expect(mergeSePersons(COMPANY, { intent: "merge", personKeys: [MERGED_KEY, UNKNOWN_KEY], note: "" }, NOW)).rejects.toThrow("Unknown person.");
    await expect(splitSePersonSlots(COMPANY, { intent: "split", slots: ["nope"], note: "" }, NOW)).rejects.toThrow("Unknown observation.");
    // Every observation of one person: the fold would rebuild the same set.
    await expect(
      splitSePersonSlots(COMPANY, { intent: "split", slots: ["uid-1:sig-1", "doc-9:cand-1"], note: "" }, NOW),
    ).rejects.toThrow("That is every observation of one person.");
  });

  it("resets every active rule that names the person, keeping each rule_id", async () => {
    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "not the same" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: MERGE_RULE.rule_id, kind: "merge",
        person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 0,
        note: "reset: not the same", created_at: STAMP, created_by: "backoffice",
      },
    ]);
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL ? [{ ...MERGE_RULE, active: 0 }] : answer(sql),
    );
    await expect(resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("No rule to reset.");
  });

  it("resets a split rule through the person's own slots, not only its key", async () => {
    const splitRule: SePersonRuleRow = {
      ...MERGE_RULE, rule_id: SPLIT_RULE_ID, kind: "split", person_keys: [], slots: ["doc-9:cand-1"],
    };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL ? [splitRule] : answer(sql),
    );
    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      expect.objectContaining({ rule_id: SPLIT_RULE_ID, kind: "split", active: 0, note: "reset" }),
    ]);
  });

  it("launches the targeted fold for this company alone", async () => {
    expect(await launchSePersonFold(COMPANY)).toEqual({ runId: "run-9", url: null });
    expect(dagster.launchRun).toHaveBeenCalledWith({
      job: "__ASSET_JOB",
      assetSelection: ["se_company_person_fold_companies"],
      runConfig: {
        ops: { se_company_person_fold_companies: { config: { company_ids: [COMPANY] } } },
      },
      tags: { "backoffice/person": "fold-now" },
    });
  });
```

- [ ] **Step 3: Implement**

`app/lib/clickhouse.server.ts`, next to the address inserters:

```ts
/**
 * Append a reviewer suggestion-row version to the SE person suggestion table; the
 * normalize asset parses it and the fold reads the newest version per
 * (company_id, source, slot) through FINAL.
 */
export async function chInsertSeCompanyPersonSuggestions<T extends object>(values: T[]): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_person_suggestion",
    values,
    format: "JSONEachRow",
  });
}

/** Append a rule version (hide, merge, split, or its release) to the SE person rule
 * table; the fold reads the newest version per (company_id, rule_id) through FINAL and
 * applies only `active = 1`. */
export async function chInsertSeCompanyPersonRules<T extends object>(values: T[]): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_person_rule",
    values,
    format: "JSONEachRow",
  });
}
```

`app/lib/dagster.server.ts`, beside the address constant:

```ts
/** The targeted person fold (person spec section 5): normalizes then re-folds the
 * companies named in its config, whatever their bucket. Launched by the People tab's
 * Fold now and by Activate. */
export const SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET = "se_company_person_fold_companies";
```

`app/lib/se-company-person-entity.server.ts` — the reads first. The module header explains
the shape (copy the tone of `se-company-address-entity.server.ts`'s header, naming spec
sections 3.1 to 3.6, 5 and 7, the append-only rule, the stamp, and Ruling 4's data keys).

```ts
const MAIN_COLUMNS_SQL = (a: string) => `  ${a}.company_id AS company_id, toString(${a}.person_key) AS person_key,
  ${a}.display_name AS display_name, ${a}.first_name AS first_name, ${a}.last_name AS last_name,
  ifNull(toString(${a}.birth_year), '') AS birth_year, ifNull(${a}.wikidata_id, '') AS wikidata_id,
  arrayMap(x -> toString(x), ${a}.sources) AS sources, ${a}.slots AS slots,
  arrayMap(x -> toString(x), ${a}.normalized_ids) AS normalized_ids,
  arrayMap(x -> toString(x), ${a}.member_sources) AS member_sources, ${a}.member_slots AS member_slots,
  ${a}.member_names AS member_names,
  arrayMap(x -> ifNull(toString(x), ''), ${a}.member_birth_years) AS member_birth_years,
  ${a}.member_wikidata_ids AS member_wikidata_ids, ${a}.member_data AS member_data,
  ${a}.role_codes AS role_codes, ${a}.role_years AS role_years, ${a}.role_sources AS role_sources,
  ${a}.current_roles AS current_roles,
  ifNull(toString(${a}.first_year), '') AS first_year, ifNull(toString(${a}.last_year), '') AS last_year,
  toString(${a}.text_source) AS text_source, ${a}.data AS data,
  toUInt8(${a}.active) AS active, toString(${a}.inactive_reason) AS inactive_reason,
  toString(${a}.folded_at) AS folded_at, toString(${a}.fold_version) AS fold_version,
  ${a}.source_run_id AS source_run_id`;

export const PERSON_MAIN_SQL = `SELECT
${MAIN_COLUMNS_SQL("m")}
FROM ${SE_COMPANY_PERSON_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String}
ORDER BY m.active DESC, m.inactive_reason, m.display_name`;

/** History is append-only: no FINAL, newest first, capped like the Address tab's. Its
 * own key is `changed_at` -- `folded_at` on a history row is when the PREVIOUS image was
 * published, which is not the order the reviewer reads it in. */
export const PERSON_HISTORY_SQL = `SELECT
${MAIN_COLUMNS_SQL("h")},
  toString(h.changed_at) AS changed_at, toString(h.change_kind) AS change_kind,
  h.fold_run_id AS fold_run_id
FROM corpscout.se_company_person_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.changed_at DESC
LIMIT 200`;

export const PERSON_NORMALIZED_SQL = `SELECT
  n.company_id AS company_id, toString(n.source) AS source, n.slot AS slot,
  toString(n.suggestion_id) AS suggestion_id, toString(n.normalized_id) AS normalized_id,
  toString(n.normalizer_version) AS normalizer_version, toString(n.parse_status) AS parse_status,
  n.parse_notes AS parse_notes, n.first_tokens AS first_tokens, n.middle_tokens AS middle_tokens,
  n.last_tokens AS last_tokens, n.display_first AS display_first, n.display_last AS display_last,
  n.display_name AS display_name, ifNull(toString(n.birth_year), '') AS birth_year,
  ifNull(n.wikidata_id, '') AS wikidata_id, ifNull(n.role_code, '') AS role_code,
  ifNull(n.role_key, '') AS role_key, ifNull(toString(n.role_year), '') AS role_year,
  ifNull(toString(n.role_from), '') AS role_from, ifNull(toString(n.role_to), '') AS role_to,
  n.data AS data, toString(n.normalized_at) AS normalized_at
FROM corpscout.se_company_person_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
ORDER BY n.source, n.slot`;

export const PERSON_RAW_SQL = `SELECT
  s.company_id AS company_id, toString(s.source) AS source, s.slot AS slot,
  toString(s.suggestion_id) AS suggestion_id, toString(s.suggested_at) AS suggested_at,
  s.source_record_id AS source_record_id, ifNull(s.full_name, '') AS full_name,
  ifNull(s.first_name, '') AS first_name, ifNull(s.last_name, '') AS last_name,
  ifNull(toString(s.birth_year), '') AS birth_year, ifNull(s.wikidata_id, '') AS wikidata_id,
  ifNull(s.role_original, '') AS role_original, ifNull(s.role_key, '') AS role_key,
  ifNull(toString(s.fiscal_year), '') AS fiscal_year, ifNull(toString(s.role_from), '') AS role_from,
  ifNull(toString(s.role_to), '') AS role_to, ifNull(s.document_ref, '') AS document_ref,
  s.data AS data
FROM corpscout.se_company_person_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source, s.slot`;

export const PERSON_RULES_SQL = `SELECT
  r.company_id AS company_id, toString(r.rule_id) AS rule_id, toString(r.kind) AS kind,
  arrayMap(x -> toString(x), r.person_keys) AS person_keys, r.slots AS slots,
  toUInt8(r.active) AS active, r.note AS note, toString(r.created_at) AS created_at,
  r.created_by AS created_by
FROM corpscout.se_company_person_rule AS r FINAL
WHERE r.company_id = {companyId:String}
ORDER BY r.created_at DESC`;

/** The spelling order (spec 3.6): the global export (`company_id = ''`) and this
 * company's own overrides in one read. Only the `name` field has a precedence. */
export const PERSON_PRECEDENCE_SQL = `SELECT
  p.company_id AS company_id, toString(p.field) AS field, toString(p.source) AS source,
  toUInt32(p.precedence) AS precedence, toUInt8(p.removed) AS removed,
  toString(p.decided_by) AS decided_by, p.note AS note, toString(p.decided_at) AS decided_at
FROM corpscout.se_company_person_precedence AS p FINAL
WHERE p.company_id IN ('', {companyId:String}) AND p.field = 'name'
ORDER BY p.precedence DESC`;
```

`loadSePersonDetail`: `Promise.all` of the six reads, then

- `normalizedBySlot` / `rawBySlot` keyed `source|slot`.
- `precedenceOf(source)`: the company's own row (`company_id !== ''`, `removed === 0`) first, then the global one, else 0 — `precedence.py::precedence_for` in TypeScript.
- per main row, `members` from the member arrays (`member_sources[i]`, `member_slots[i]`, `member_names[i]`, `member_birth_years[i]`, `member_wikidata_ids[i]`, `member_data[i]`, `normalized_ids[i]`): `current = normalizedBySlot.get(...) ?? null`, `raw = rawBySlot.get(...) ?? null`, `refoldPending = current !== null && current.normalized_id !== normalizedId`, `precedence = precedenceOf(source)`.
- `roles`: `row.role_codes.map((code, i) => ({ code, year: row.role_years[i] ?? 0, sources: row.role_sources[i] ?? [] }))`.
- `spellingReason`: one member -> `"single source"`; the text source's members' top precedence strictly above every OTHER member's (the text source's own members excluded from the comparison) -> `"precedence"`; otherwise, among the members at that top precedence, the published `display_name` having strictly more words than every member whose name differs from it -> `"most complete"`; else `"tie-break"`. That mirrors `fold.py::_text_member`'s sort (precedence, then token count, then string length, then alphabetical): the first two keys are what the reviewer can be told about, the last two are the tie-break.
- `rules` per person: `rules.filter(rule => rule.active === 1 && (rule.person_keys.includes(row.person_key) || rule.slots.some(slot => row.member_slots.includes(slot))))`.
- `drafts`: `reviewer_draft` raw rows grouped by `groupOfRowSlot(slot)`, a group kept when any of its rows still carries a name (`full_name`, `first_name` or `last_name` non-empty — a cleared row is the tombstone an Activate or a Discard left); rows sorted by slot; `normalized` the group's normalized rows; `name` the first row's `first_name last_name` (or `full_name`); `note` and `replacesKey` read out of the first row's `data` under Ruling 4's keys.
- `foldPending = personFoldPending(newest folded_at across the main rows or null, [...non-draft normalized `normalized_at`, ...non-draft raw `suggested_at`, ...every rule `created_at`, ...precedence rows with `company_id !== ''` `decided_at`], a non-draft normalized row with `parse_status === "ok"` exists)`. Ruling 7: the global precedence rows are read but never stamped in.
- Return `null` when `published`, `drafts` and the normalized rows are all empty.

`loadSePersonRoleOptions`: `getCompanyPersonRoleTypes()`, keep `is_active === 1`, map to `{ code: role_code, label: display_name, group: role_group }`.

The writes:

```ts
export class SePersonDecisionError extends Error {}

// DRAFT_SOURCE and REVIEWER_SOURCE are imported from `~/lib/se-person-fields`, not
// re-declared: one spelling of each source string across the six files.
const DECIDED_BY = "backoffice";

/** The 18 columns of `tables.SUGGESTION_COLUMNS`, in that order. Every Nullable one is
 * `string | null` or `number | null` so a cleared value is NULL, never ''. */
export interface SePersonRawInsertRow {
  company_id: string; source: string; slot: string; suggestion_id: string;
  suggested_at: string; source_record_id: string;
  full_name: string | null; first_name: string | null; last_name: string | null;
  birth_year: number | null; wikidata_id: string | null;
  role_original: string | null; role_key: string | null; fiscal_year: number | null;
  role_from: string | null; role_to: string | null; document_ref: string | null;
  data: string;
}
/** The 9 columns of `tables.RULE_COLUMNS`. */
export interface SePersonRuleInsertRow {
  company_id: string; rule_id: string; kind: string;
  person_keys: string[]; slots: string[]; active: number; note: string;
  created_at: string; created_by: string;
}

function suggestionId(companyId: string, source: string, slot: string, stamp: string): string {
  return createHash("sha256").update(`${companyId}\n${source}\n${slot}\n${stamp}`).digest("hex");
}
/** Minted once, when the rule is created; a Reset writes a new VERSION of this id. */
function ruleId(companyId: string, kind: string, keys: readonly string[], slots: readonly string[], stamp: string): string {
  const preimage = `${companyId}\n${kind}\n${[...keys].sort().join(",")}\n${[...slots].sort().join(",")}\n${stamp}`;
  return createHash("sha256").update(preimage).digest("hex");
}
function numberOrNull(value: string): number | null {
  return value === "" ? null : Number(value);
}
function textOrNull(value: string): string | null {
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}
/** Ruling 4: the reviewer's own object plus the keys the backoffice owns. */
function rowData(own: string, note: string, replacesKey: string | null): string {
  const parsed = JSON.parse(own || "{}") as Record<string, unknown>;
  return JSON.stringify({
    ...parsed,
    decided_by: DECIDED_BY,
    note,
    ...(replacesKey === null ? {} : { replaces_key: replacesKey }),
  });
}
/** Ruling 5: one year is a fiscal year; a span is two dates, an open end NULL; a role
 * with no year at all carries neither and the fold takes it as held now. */
function roleColumns(role: SePersonRoleInput | null): Pick<SePersonRawInsertRow, "role_original" | "role_key" | "fiscal_year" | "role_from" | "role_to"> {
  if (role === null) {
    return { role_original: null, role_key: null, fiscal_year: null, role_from: null, role_to: null };
  }
  const { code, fromYear, toYear } = role;
  const single = fromYear !== "" && toYear === fromYear;
  return {
    role_original: code,
    role_key: code,
    fiscal_year: single ? Number(fromYear) : null,
    role_from: single || fromYear === "" ? null : `${fromYear}-01-01`,
    role_to: single || toYear === "" ? null : `${toYear}-12-31`,
  };
}
function personRow(
  companyId: string, source: string, slot: string, stamp: string,
  person: { firstName: string; lastName: string; birthYear: string; wikidataId: string } | null,
  role: SePersonRoleInput | null,
  data: string,
): SePersonRawInsertRow {
  return {
    company_id: companyId, source, slot,
    suggestion_id: suggestionId(companyId, source, slot, stamp),
    suggested_at: stamp,
    source_record_id: "",
    full_name: null,
    first_name: person === null ? null : textOrNull(person.firstName),
    last_name: person === null ? null : textOrNull(person.lastName),
    birth_year: person === null ? null : numberOrNull(person.birthYear),
    wikidata_id: person === null ? null : textOrNull(person.wikidataId),
    ...roleColumns(person === null ? null : role),
    document_ref: null,
    data,
  };
}
```

- `saveSePersonDraft(companyId, decision, now)`: `stamp`; `group = decision.slot ?? personGroupSlot(stamp)`; `rows = (input.roles.length === 0 ? [null] : input.roles).map((role, index) => personRow(companyId, DRAFT_SOURCE, personRowSlot(group, index + 1), stamp, input, role, rowData(input.data, input.note, decision.replacesKey)))`. When `decision.slot` is given, read `PERSON_RAW_SQL` and append one cleared row (`personRow(..., null, null, rowData("{}", "role removed", null))`) for every stored `reviewer_draft` row of the group whose slot the new set does not use — otherwise a deleted role stays a live observation. One insert. Returns `{ decidedAt: stamp, slot: group }`.
- `activateSePersonDraft(companyId, decision, now)`: read `PERSON_RAW_SQL`, `PERSON_NORMALIZED_SQL` and `PERSON_MAIN_SQL`. The group's live draft rows, sorted by slot, else `throw new SePersonDecisionError("No draft to activate.")`. The note is `decision.note !== "" ? decision.note : the draft's own`. Insert, in ONE call, one `reviewer` row per draft row (same slot, same person and role columns, `data = rowData(draft data minus the reserved keys, note, null)`) followed by one cleared `reviewer_draft` row per slot (`rowData("{}", "activated", null)`). Then Ruling 1: with `replacesKey` set, find the published row with that key; write the hide rule (`ruleVersion(companyId, "hide", [replacesKey], [], 1, "corrected by reviewer", stamp)`) unless some member of that row `foldsIntoPerson` the draft's identity — the draft's tokens from `normalizeSePersonName({ firstName, lastName })` over its first row, the member's from its current normalized row's `first_tokens`/`middle_tokens`/`last_tokens`, `birth_year` and `wikidata_id` (a member with no current normalized row cannot match). A key naming no published row still gets the rule: the fold resolves it through the previous members.
- `discardSePersonDraft`: the group's live rows, else `throw new SePersonDecisionError("No draft to discard.")`; one cleared row per slot, note `"discarded"`.
- `removeSePerson`: read main, rules and raw. The row by key, else `"Unknown person."`; `"Already hidden."` when `inactive_reason === "hidden"` or an active `hide` rule already names the key. Reviewer-only (`row.member_sources.every(source => source === REVIEWER_SOURCE)`): one cleared `reviewer` row per member slot, note the decision's or `"removed by reviewer"`, no rule. Otherwise the hide rule alone, every member left in place — the key is computed over the whole set, so retiring one member would re-key the person and orphan the rule.
- `mergeSePersons`: read main; every key must name a published row, else `"Unknown person."`; one `merge` rule, `person_keys` the sorted keys, `slots` `[]`, note the decision's or `"merged by reviewer"`.
- `splitSePersonSlots`: read main; every slot must appear in some published row's `member_slots`, else `"Unknown observation."`; refuse `"That is every observation of one person."` when the chosen slots are exactly one published row's members; one `split` rule, `slots` sorted, `person_keys` `[]`, note the decision's or `"split by reviewer"`.
- `resetSePersonRules`: read main and rules; the row by key, else `"Unknown person."`; the active rules naming the key or one of the row's member slots, else `"No rule to reset."`; one new version per rule with the SAME `rule_id`, `kind`, `person_keys` and `slots`, `active: 0`, note `decision.note === "" ? "reset" : "reset: " + decision.note`, `created_at: stamp`.
- `launchSePersonFold`: `launchRun` with `assetSelection: [SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET]`, `runConfig.ops[<asset>].config.company_ids = [companyId]` (the asset's `changed_only` already defaults to `false` in `PersonFoldCompaniesConfig`, so the tab never sends it), tag `{ "backoffice/person": "fold-now" }`; returns `{ runId, url: dagsterRunUrl(runId) }`.

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-company-person-entity.server.test.ts tests/se-company-address-entity.server.test.ts && npm run typecheck`
Expected: PASS (the address suite proves the shared `clickhouse.server.ts` edit broke nothing).

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-company-person-entity.server.ts app/lib/clickhouse.server.ts app/lib/dagster.server.ts \
  tests/se-company-person-entity.server.test.ts
git commit -m "feat(backoffice): person entity server module with the reviewer writes and the targeted fold launch"
```

---

### Task 3: The workspace and the edit sheet

**Files:**
- Create: `corpscout/services/backoffice/app/components/admin/se-person-workspace.tsx`, `app/components/admin/se-person-edit-sheet.tsx`
- Test: `corpscout/services/backoffice/tests/se-person-edit-sheet.test.tsx`

**Interfaces:**
- Consumes: Task 2's `SePersonDetail`, `SePersonPublished`, `SePersonMember`, `SePersonDraft`, `SePersonRow`, `SePersonRawRow`, `SePersonNormalizedRow`, `SePersonRuleRow`, `SePersonPrecedenceRow` (type-only imports from the `.server` module are allowed, and are what the Address tab does); Task 1's `personSourceLabel`, `roleLabel`, `MAX_NOTE_LENGTH`, `MAX_DATA_LENGTH`, `SePersonRoleOption`; `FoldRunPoller` from `~/components/admin/fold-run-poller` (unchanged, already extracted for the Address tab); `DefinitionList`, `EMPTY_VALUE`, `text` from `~/components/admin/definition-list`; shadcn `Accordion`, `Alert`, `Badge`, `Button`, `Card`, `Dialog`, `Empty`, `Input`, `Sheet`, `Textarea`.
- Produces: `SePersonWorkspace({ companyId, detail, roleOptions, selectedKey, result })`, `SePersonResult` (`{ ok: true; intent: string; runId?: string; url?: string | null; slot?: string } | { ok: false; intent?: string; error: string } | null`), `PendingPersonDecision`, `PersonDecisionDialogBody` (exported for the route test, as `AddressDecisionDialogBody` is), `SPLIT_CAVEAT` (the Ruling 9 sentence, exported as a plain string so a test can assert it — an open dialog renders nothing server-side, which is why `se-address-workspace.tsx` exports `removeDescription`/`removeSuccessCopy` the same way), `SePersonEditSheet`, `SePersonEditForm`, `EMPTY_PERSON_INITIAL`, `SePersonEditInitial`, `SePersonEditMode`.

Layout (spec section 7), mirroring the Address tab's structure.

**THE EXACT STRINGS OTHER TASKS ASSERT.** Task 4's route test and this task's sheet test grep
these literals, and they are written before this component is; spell them exactly:

| String | Where it lives | Asserted by |
| --- | --- | --- |
| `No people published yet` | the Persons card's `Empty` state (`EmptyTitle`), shown when `published` and `drafts` are both empty | Task 4 route test |
| `Add person` | the panel's Add button, rendered in the empty state too | Task 4 route test |
| `Fold pending` | the fold card's `AlertTitle` | Task 4 route test |
| `aria-current="true"` | on the selected person's row link (`<Link ... aria-current={isSelected ? "true" : undefined}>`), the selection being `selectedKey` or the first active row | Task 4 route test |
| `Correct person` | the edit sheet's title in `correct` mode (`add` is `Add person`, `edit-draft` is `Edit draft person`) | this task's sheet test |
| `Bolagsverket`, `ESEF` | source badges, from `personSourceLabel` | Task 4 route test |
| `Board member` | a role badge, from `roleLabel(code, roleOptions)` | Task 4 route test |
| `A split pins the observations you tick. Bolagsverket mints a new slot per annual report, so this split holds for today's observations and may need writing again after next year's report.` | the Split dialog's description, verbatim (Ruling 9) | this task's sheet test greps `Bolagsverket mints a new slot per annual report` |
| `von Essen` | the last-name field's particle hint | this task's sheet test |

- **Left (two-thirds).** *Persons card*: active rows first, one row each with the display name, a birth-year badge, the QID (linked to `https://www.wikidata.org/wiki/<qid>`), the current roles as `roleLabel` badges, one badge per distinct source (`personSourceLabel`), a `hidden` / `withdrawn` badge from `inactive_reason`, and the years `first_year`-`last_year`. The name links to `?person=<key>` (a `Link` on the line) and carries `aria-current="true"` when it is the selected row; Correct sits OUTSIDE the link (an anchor may not wrap a button) and opens the sheet in `correct` mode prefilled from the row. With nothing published and nothing drafted the card renders the `Empty` state titled `No people published yet` with the `Add person` button still in reach — a company no source names a person for is a normal pipeline state. Each row carries a native `<input type="checkbox" name="person_key" value=<key>>` inside the Merge form (a Base UI checkbox posts no form value; the sheet's kind `<select>` is the same precedent), and the card's footer has the Merge button, enabled at two or more ticks. Withdrawn and hidden rows sit under a collapsed `Accordion` group "Hidden and withdrawn (N)".
- *Drafts card* when `drafts` is non-empty: one entry per group slot with the typed name, birth year, the role entries, the parse of each row when a normalized row exists else "parses on Fold now", the replaced person's line when `replacesKey` names a published row, and Edit (sheet `edit-draft`), Activate and Discard behind the confirmation dialog.
- *History card*: `Accordion`, newest first, each item `changed_at`, `change_kind`, the display name, `active`/`inactive_reason`, the sources and the roles of that image.
- *Fold card*: the "Fold pending" `Alert` when `detail.foldPending`, the Fold now `<Form method="post">`, and `FoldRunPoller` keyed by run id — shown for `fold-now` AND for `activate` (Ruling 6), both of which return a `runId`.
- **Right (one-third, sticky).** The selected person (`selectedKey`, else the first active row): the display name, key (shortened, full in `title`), birth year, QID, `text_source` with `spellingReason`, and then
  - *Members*: one entry per member with `personSourceLabel`, the slot, the spelling, the birth year, the QID, the member's `data` in a `<pre>`, the parse status and notes of its current normalized row, a "re-fold pending" badge when `refoldPending`, the precedence number, the raw evidence row behind it (`role_original`, `role_key`, `fiscal_year` / `role_from`-`role_to`, `document_ref`, `suggested_at`), and a native checkbox `name="slot"` inside the Split form.
  - *Roles*: the timeline, `roles` grouped by year descending, each `roleLabel(code)` with its sources.
  - *Data*: the merged object in a `<pre>`.
  - *Rules*: the active rules naming this person (`kind`, note, `created_at`), or "none".
  - *Precedence*: the order the `name` field carries, company overrides marked.
  - Actions: **Add person** (sheet `add`), **Remove** (dialog, disabled when hidden), **Split** (a dialog listing the members with their checkboxes, whose description is Ruling 9's caveat verbatim: "A split pins the observations you tick. Bolagsverket mints a new slot per annual report, so this split holds for today's observations and may need writing again after next year's report."), **Reset** (only when `rules` is non-empty), **Correct** (also on the row).
- Every action is a plain `<Form method="post">` with a hidden `intent`, the hidden `person_key` / `slot` / `replaces_key` it acts on, the checkboxes where the decision takes several, and the dialog's note `Input` — the `AddressDecisionDialogBody` pattern, copied rather than imported (it is the Address tab's private component).
- The edit sheet: `SheetContent` with `className="data-[side=right]:sm:max-w-3xl"`; titles `Add person` / `Correct person` / `Edit draft person` by mode; fields `first_name`, `last_name` (Inputs, with the hint that particles belong to the last name: "A particle belongs to the last name: von Essen"), `birth_year`, `wikidata_id`, a roles editor rendering `MAX_ROLE_ENTRIES` slots' worth of rows — each a native `<select name="role_code">` over `roleOptions` grouped by `group`, plus `role_from` and `role_to` number inputs — a `data` `Textarea` (JSON object, `maxLength={MAX_DATA_LENGTH}`), a `note` `Textarea`, and hidden `intent=save-draft`, `slot`, `replaces_key`. Save draft submits; the workspace closes the sheet on a successful `save-draft` result; a refusal keeps it open with the typed values and the error.

- [ ] **Step 1: Write the failing sheet test** (`tests/se-person-edit-sheet.test.tsx`, modelled on `tests/se-address-edit-sheet.test.tsx`)

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  EMPTY_PERSON_INITIAL,
  SePersonEditForm,
  type SePersonEditInitial,
} from "~/components/admin/se-person-edit-sheet";
import { SPLIT_CAVEAT } from "~/components/admin/se-person-workspace";

const COMPANY = "5560000001";
const KEY = "b7".repeat(32);
const SLOT = "r20260910120000123";
const ROLE_OPTIONS = [
  { code: "board_member", label: "Board member", group: "governance" },
  { code: "chief_executive_officer", label: "Chief executive officer", group: "executive" },
];
const published: SePersonEditInitial = {
  firstName: "Anna",
  lastName: "Svensson",
  birthYear: "1975",
  wikidataId: "Q7",
  roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
  data: '{"title":"Chair"}',
  note: "",
};

function render(element: React.ReactElement): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/people", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/people`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SePersonEditForm", () => {
  it("renders the person fields, the roles editor and the catalog's options in add mode", () => {
    const html = render(
      <SePersonEditForm
        mode="add"
        initial={EMPTY_PERSON_INITIAL}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    for (const name of ["first_name", "last_name", "birth_year", "wikidata_id", "role_code", "role_from", "role_to", "data", "note"]) {
      expect(html).toContain(`name="${name}"`);
    }
    expect(html).toContain('value="save-draft"');
    // Ruling 3: the options are the catalog's, by label, never a hard-coded list.
    expect(html).toContain('value="board_member"');
    expect(html).toContain("Board member");
    expect(html).toContain("Chief executive officer");
    // The particle hint, so a reviewer does not type "von Essen" as a first name.
    expect(html).toContain("von Essen");
  });

  it("prefills a Correct from the published person and carries the key it replaces", () => {
    const html = render(
      <SePersonEditForm
        mode="correct"
        initial={published}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={KEY}
        result={null}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('value="Anna"');
    expect(html).toContain('value="Svensson"');
    expect(html).toContain('value="1975"');
    expect(html).toContain('value="Q7"');
    expect(html).toContain('value="2023"');
    expect(html).toContain(`name="replaces_key" value="${KEY}"`);
    expect(html).toContain("Correct person");
  });

  it("carries the draft's slot in edit-draft mode and shows this form's own refusal", () => {
    const html = render(
      <SePersonEditForm
        mode="edit-draft"
        initial={published}
        roleOptions={ROLE_OPTIONS}
        slot={SLOT}
        replacesKey={null}
        result={{ ok: false, intent: "save-draft", error: "initials only" }}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain(`name="slot" value="${SLOT}"`);
    expect(html).toContain('role="alert"');
    expect(html).toContain("initials only");
  });

  it("warns, in the Split dialog's own words, that Bolagsverket re-slots every filing", () => {
    // Ruling 9 / spec 7's known limit for this slice. The dialog itself renders
    // nothing server-side (an open Dialog is client state), so the sentence is
    // exported as a string and pinned here -- the same trick `se-address-workspace.tsx`
    // uses for `removeDescription`.
    expect(SPLIT_CAVEAT).toContain("Bolagsverket mints a new slot per annual report");
    expect(SPLIT_CAVEAT).toContain("may need writing again after next year's report");
  });

  it("keeps another action's refusal out of the sheet", () => {
    const html = render(
      <SePersonEditForm
        mode="add"
        initial={EMPTY_PERSON_INITIAL}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={null}
        result={{ ok: false, intent: "remove", error: "Already hidden." }}
        onCancel={() => {}}
      />,
    );
    expect(html).not.toContain("Already hidden.");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/se-person-edit-sheet.test.tsx`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the two components** as laid out above. `se-person-edit-sheet.tsx` follows `se-address-edit-sheet.tsx` line for line: the exported portal-free `SePersonEditForm` (a Base UI `SheetTitle` needs the Sheet root, so the header lives only in `SePersonEditSheet`), `key={`${mode}:${slot ?? ""}:${replacesKey ?? ""}`}` on the `<Form>` so reopening re-runs every `defaultValue`, and no closing effect inside the sheet (the workspace closes it on the save's result). `se-person-workspace.tsx` follows `se-address-workspace.tsx`: `busy` from `useNavigation` compared case-insensitively, `pending` and `sheet` state, an effect closing the dialog on any result and the sheet on a successful `save-draft`.

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-person-edit-sheet.test.tsx tests/se-address-edit-sheet.test.tsx tests/admin-se-company-basic-info.test.tsx && npm run typecheck`
Expected: PASS (the two neighbouring suites prove the shared poller and sheet patterns still hold).

- [ ] **Step 5: Commit**

```bash
git add app/components/admin/se-person-workspace.tsx app/components/admin/se-person-edit-sheet.tsx \
  tests/se-person-edit-sheet.test.tsx
git commit -m "feat(backoffice): person workspace and edit sheet on the person entity"
```

---

### Task 4: The company route, the tab, and the docs

**Files:**
- Create: `corpscout/services/backoffice/app/routes/admin-se-company-person.tsx`
- Modify: `app/routes.ts`, `app/lib/se-company-tabs.ts`
- Modify (fallback, only if a string Step 1 asserts is missing): `app/components/admin/se-person-workspace.tsx` — Task 3 owns it, and Task 3's description names every string this task's test greps, so this should not be needed.
- Test: `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx` (create), `tests/admin-se-company-area.test.tsx` (modify — THREE cases, see Step 3b), `tests/se-company-tabs.server.test.ts` (untouched, must stay green)
- Modify: the spec `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` sections 7 and 10 (Rulings 1 to 9 as sentences under "Actions"; section 10 gains `se-person-tables.ts`, `se-people-filters.ts`, `se-people-list.server.ts`, `se-people-table.tsx` and `fold-run-poller.tsx`)

**Interfaces:** consumes Tasks 1 to 3. `loader({ request, params })` returns `{ detail, roleOptions, selectedKey }`; `action({ request, params })` returns `SePersonResult` shapes.

- [ ] **Step 1: Write the failing route test** (`tests/admin-se-company-person.test.tsx`)

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSePersonDetail: vi.fn(),
  loadSePersonRoleOptions: vi.fn(),
  saveSePersonDraft: vi.fn(),
  activateSePersonDraft: vi.fn(),
  discardSePersonDraft: vi.fn(),
  removeSePerson: vi.fn(),
  mergeSePersons: vi.fn(),
  splitSePersonSlots: vi.fn(),
  resetSePersonRules: vi.fn(),
  launchSePersonFold: vi.fn(),
  SePersonDecisionError: class SePersonDecisionError extends Error {},
}));
vi.mock("~/lib/se-company-person-entity.server", () => server);

import { action, loader } from "~/routes/admin-se-company-person";
import { SePersonWorkspace } from "~/components/admin/se-person-workspace";
import type { SePersonDetail, SePersonPublished } from "~/lib/se-company-person-entity.server";

const COMPANY = "5560125220";
const KEY = "a".repeat(64);
const OTHER = "b".repeat(64);
const SLOT = "r20260910120000123";
const ROLE_OPTIONS = [{ code: "board_member", label: "Board member", group: "governance" }];
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [], foldPending: false,
};
const row = {
  company_id: COMPANY, person_key: KEY, display_name: "Anna Svensson",
  first_name: "Anna", last_name: "Svensson", birth_year: "1975", wikidata_id: "",
  sources: ["bolagsverket", "esef"], slots: ["uid-1:sig-1", "doc-9:cand-1"],
  normalized_ids: ["n1", "n2"],
  member_sources: ["bolagsverket", "esef"], member_slots: ["uid-1:sig-1", "doc-9:cand-1"],
  member_names: ["Anna Svensson", "Anna Maria Svensson"], member_birth_years: ["1975", ""],
  member_wikidata_ids: ["", ""], member_data: ["{}", "{}"],
  role_codes: ["board_member"], role_years: [2025], role_sources: [["bolagsverket"]],
  current_roles: ["board_member"], first_year: "2025", last_year: "2025",
  text_source: "bolagsverket", data: "{}", active: 1, inactive_reason: "",
  folded_at: "2026-09-10 09:00:00.000", fold_version: "se-person-fold-v1",
  source_run_id: "run-fold",
};
const published: SePersonPublished = {
  row,
  members: [
    {
      source: "bolagsverket", slot: "uid-1:sig-1", normalizedId: "n1", name: "Anna Svensson",
      birthYear: "1975", wikidataId: "", data: "{}", current: null, raw: null,
      refoldPending: false, precedence: 900,
    },
    {
      source: "esef", slot: "doc-9:cand-1", normalizedId: "n2", name: "Anna Maria Svensson",
      birthYear: "", wikidataId: "", data: "{}", current: null, raw: null,
      refoldPending: false, precedence: 400,
    },
  ],
  roles: [{ code: "board_member", year: 2025, sources: ["bolagsverket"] }],
  spellingReason: "precedence",
  rules: [],
};
const detail: SePersonDetail = { ...EMPTY_DETAIL, published: [published] };

function post(body: Record<string, string>, repeated: [string, string][] = []): Request {
  const form = new URLSearchParams();
  for (const [key, value] of Object.entries(body)) form.set(key, value);
  for (const [key, value] of repeated) form.append(key, value);
  return new Request(`http://x/admin/se/company/${COMPANY}/people`, {
    method: "POST",
    body: form,
    headers: { "content-type": "application/x-www-form-urlencoded" },
  });
}
function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/people", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/people${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("admin-se-company-person route", () => {
  beforeEach(() => {
    server.loadSePersonDetail.mockReset().mockResolvedValue(detail);
    server.loadSePersonRoleOptions.mockReset().mockResolvedValue(ROLE_OPTIONS);
    server.saveSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s", slot: SLOT });
    server.activateSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.discardSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.removeSePerson.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.mergeSePersons.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.splitSePersonSlots.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.resetSePersonRules.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.launchSePersonFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail with the role catalog, and opens on an empty one for a company with no persons", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people`),
      params: { companyId: COMPANY },
    } as never);
    expect(response).toEqual({ detail, roleOptions: ROLE_OPTIONS, selectedKey: null });

    // No 404: Add person must stay reachable. The company layout 404s an unknown company.
    server.loadSePersonDetail.mockResolvedValueOnce(null);
    const missing = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people`),
      params: { companyId: COMPANY },
    } as never);
    expect(missing.detail).toEqual(EMPTY_DETAIL);
  });

  it("passes the selected key from ?person=, ignoring anything malformed", async () => {
    const selected = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people?person=${KEY}`),
      params: { companyId: COMPANY },
    } as never);
    expect(selected.selectedKey).toBe(KEY);
    const malformed = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people?person=nope`),
      params: { companyId: COMPANY },
    } as never);
    expect(malformed.selectedKey).toBeNull();
  });

  it("refuses a bad company id and an unparseable post before touching the store", async () => {
    expect(await action({ request: post({ intent: "fold-now" }), params: { companyId: "12" } } as never)).toEqual({
      ok: false, intent: "", error: "Company id must be 10 or 12 digits.",
    });
    expect(await action({ request: post({ intent: "remove", person_key: "zz" }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: false, intent: "remove", error: "Unknown person.",
    });
    expect(server.removeSePerson).not.toHaveBeenCalled();
  });

  it("dispatches every intent to its function with the parsed decision", async () => {
    await action({ request: post({ intent: "remove", person_key: KEY, note: "gone" }), params: { companyId: COMPANY } } as never);
    expect(server.removeSePerson).toHaveBeenCalledWith(COMPANY, { intent: "remove", personKey: KEY, note: "gone" });
    await action({ request: post({ intent: "reset", person_key: KEY }), params: { companyId: COMPANY } } as never);
    expect(server.resetSePersonRules).toHaveBeenCalledWith(COMPANY, { intent: "reset", personKey: KEY, note: "" });
    await action({ request: post({ intent: "merge" }, [["person_key", KEY], ["person_key", OTHER]]), params: { companyId: COMPANY } } as never);
    expect(server.mergeSePersons).toHaveBeenCalledWith(COMPANY, { intent: "merge", personKeys: [KEY, OTHER], note: "" });
    await action({ request: post({ intent: "split" }, [["slot", "doc-9:cand-1"]]), params: { companyId: COMPANY } } as never);
    expect(server.splitSePersonSlots).toHaveBeenCalledWith(COMPANY, { intent: "split", slots: ["doc-9:cand-1"], note: "" });
    await action({ request: post({ intent: "discard", slot: SLOT }), params: { companyId: COMPANY } } as never);
    expect(server.discardSePersonDraft).toHaveBeenCalledWith(COMPANY, { intent: "discard", slot: SLOT });
    const saved = await action({
      request: post({ intent: "save-draft", first_name: "Anna", last_name: "Svensson" }, [["role_code", "board_member"], ["role_from", "2025"], ["role_to", "2025"]]),
      params: { companyId: COMPANY },
    } as never);
    expect(saved).toEqual({ ok: true, intent: "save-draft", slot: SLOT });
    expect(server.saveSePersonDraft).toHaveBeenCalledWith(COMPANY, expect.objectContaining({ intent: "save-draft", replacesKey: null }));
  });

  it("launches the fold on Fold now and, per Ruling 6, on Activate too", async () => {
    expect(await action({ request: post({ intent: "fold-now" }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: true, intent: "fold-now", runId: "run-9", url: null,
    });
    const activated = await action({ request: post({ intent: "activate", slot: SLOT }), params: { companyId: COMPANY } } as never);
    expect(server.activateSePersonDraft).toHaveBeenCalledWith(COMPANY, { intent: "activate", slot: SLOT, note: "" });
    expect(activated).toEqual({ ok: true, intent: "activate", runId: "run-9", url: null });
    expect(server.launchSePersonFold).toHaveBeenCalledTimes(2);
  });

  it("turns a store refusal into a form error and lets anything else through", async () => {
    server.removeSePerson.mockRejectedValueOnce(new server.SePersonDecisionError("Already hidden."));
    expect(await action({ request: post({ intent: "remove", person_key: KEY }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: false, intent: "remove", error: "Already hidden.",
    });
    server.removeSePerson.mockRejectedValueOnce(new Error("ClickHouse is down"));
    await expect(action({ request: post({ intent: "remove", person_key: KEY }), params: { companyId: COMPANY } } as never)).rejects.toThrow("ClickHouse is down");
  });

  it("renders the workspace: the persons, their sources, roles and the fold state", () => {
    const html = render(
      <SePersonWorkspace companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    expect(html).toContain("Anna Svensson");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain("Board member");
    expect(html).toContain("Add person");
    expect(html).toContain('aria-current="true"');
    const pending = render(
      <SePersonWorkspace companyId={COMPANY} detail={{ ...detail, foldPending: true }} roleOptions={ROLE_OPTIONS} selectedKey={null} result={null} />,
    );
    expect(pending).toContain("Fold pending");
    const empty = render(
      <SePersonWorkspace companyId={COMPANY} detail={EMPTY_DETAIL} roleOptions={ROLE_OPTIONS} selectedKey={null} result={null} />,
    );
    expect(empty).toContain("No people published yet");
    expect(empty).toContain("Add person");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/admin-se-company-person.test.tsx`
Expected: FAIL — the route module does not exist.

- [ ] **Step 3: Write the route**

`app/routes/admin-se-company-person.tsx`:

```tsx
import type { Route } from "./+types/admin-se-company-person";
import { SePersonWorkspace } from "~/components/admin/se-person-workspace";
import { parseSePersonDecision } from "~/lib/se-person-decision-form";
import { selectedPersonFromSearch } from "~/lib/se-person-fields";
import {
  activateSePersonDraft,
  discardSePersonDraft,
  launchSePersonFold,
  loadSePersonDetail,
  loadSePersonRoleOptions,
  mergeSePersons,
  removeSePerson,
  resetSePersonRules,
  saveSePersonDraft,
  SePersonDecisionError,
  splitSePersonSlots,
  type SePersonDetail,
} from "~/lib/se-company-person-entity.server";

/** Swedish org numbers are 10 digits, or 12 with the century prefix. */
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;

/** A company no source has named a person for is a normal pipeline state, not a broken
 * link -- and the reviewer must still be able to type one. So the tab opens on an empty
 * detail rather than a 404; the company layout already 404s a company that does not
 * exist at all. */
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [], foldPending: false,
};

// Only `loader`, `action`, `meta` and the component live here. Any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and break the
// production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  const [detail, roleOptions] = await Promise.all([
    loadSePersonDetail(params.companyId),
    loadSePersonRoleOptions(),
  ]);
  return {
    detail: detail ?? EMPTY_DETAIL,
    roleOptions,
    selectedKey: selectedPersonFromSearch(new URL(request.url).searchParams),
  };
}

/**
 * One of the entity's eight decisions (spec section 7). The store's refusals are the
 * reviewer's to read; anything else is a real failure and must not be dressed up as a
 * form error. The role catalog is read before parsing: a role code is only valid if
 * `corpscout.company_person_role_type` has it (Ruling 3).
 */
export async function action({ request, params }: Route.ActionArgs) {
  if (!COMPANY_ID_PATTERN.test(params.companyId)) {
    return { ok: false as const, intent: "", error: "Company id must be 10 or 12 digits." };
  }
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");
  const roleOptions = await loadSePersonRoleOptions();
  const parsed = parseSePersonDecision(form, roleOptions.map((option) => option.code));
  if (!parsed.ok) return { ok: false as const, intent, error: parsed.error };
  const { decision } = parsed;
  if (decision.intent === "fold-now") {
    const { runId, url } = await launchSePersonFold(params.companyId);
    return { ok: true as const, intent, runId, url };
  }
  try {
    if (decision.intent === "save-draft") {
      const { slot } = await saveSePersonDraft(params.companyId, decision);
      return { ok: true as const, intent, slot };
    }
    if (decision.intent === "activate") {
      await activateSePersonDraft(params.companyId, decision);
      // Ruling 6 (spec 7): Activate launches the targeted fold itself, so the reviewer
      // sees the person appear without a second click.
      const { runId, url } = await launchSePersonFold(params.companyId);
      return { ok: true as const, intent, runId, url };
    }
    if (decision.intent === "discard") {
      await discardSePersonDraft(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "remove") {
      await removeSePerson(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "merge") {
      await mergeSePersons(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "split") {
      await splitSePersonSlots(params.companyId, decision);
      return { ok: true as const, intent };
    }
    await resetSePersonRules(params.companyId, decision);
    return { ok: true as const, intent };
  } catch (error) {
    if (error instanceof SePersonDecisionError) {
      return { ok: false as const, intent, error: error.message };
    }
    throw error;
  }
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.companyId} people | CompanyCollect` }];
}

export default function AdminSwedenCompanyPeople({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  return (
    <SePersonWorkspace
      companyId={params.companyId}
      detail={loaderData.detail}
      roleOptions={loaderData.roleOptions}
      selectedKey={loaderData.selectedKey}
      result={actionData ?? null}
    />
  );
}
```

`app/lib/se-company-tabs.ts`: add `{ value: "people", label: "People" },` after the `address` entry — the header, the breadcrumb and `seCompanyTabFromPath` all read that list, so nothing else changes.

`app/routes.ts`: inside the `se/company/:companyId` layout, after the `address` line,
`route("people", "routes/admin-se-company-person.tsx"),`; update the comment above the
layout from "nine tabs" to "ten tabs".

- [ ] **Step 3b: Update the three cases of `tests/admin-se-company-area.test.tsx` that the new tab breaks**

The `describe("tab labels")` block pins the tab list exactly, so adding `people` to
`SE_COMPANY_TABS` fails two of its cases the moment Step 3 lands; a third only needs
renaming. Run `npx vitest run tests/admin-se-company-area.test.tsx` BEFORE editing it and
confirm the two failures are the two named here — a third failure means Step 3 changed
something it should not have.

1. The ordered-label case (`tests/admin-se-company-area.test.tsx`, currently
   "is exactly Info, Address, Financial, ESEF, Domains, Technology, Contracts, Jobs, Listed,
   in that order"): rename it and insert `"People"` after `"Address"`:

```ts
  it("is exactly Info, Address, People, Financial, ESEF, Domains, Technology, Contracts, Jobs, Listed, in that order", () => {
    expect(SE_COMPANY_TABS.map((tab) => tab.label)).toEqual([
      "Info",
      "Address",
      "People",
      "Financial",
      "ESEF",
      "Domains",
      "Technology",
      "Contracts",
      "Jobs",
      "Publicly traded",
    ]);
  });
```

2. The case "has no People tab: the 2026-08-19 people model was retired and slice 3 adds the
   new one" is exactly the assertion this slice invalidates. Replace it with its positive
   twin:

```ts
  it("carries the People tab of the new person entity, third, beside Address", () => {
    const values = SE_COMPANY_TABS.map((tab) => tab.value);
    expect(values).toContain("people");
    expect(values.indexOf("people")).toBe(values.indexOf("address") + 1);
    expect(seCompanyTabPath("5560125220", "people")).toBe("/admin/se/company/5560125220/people");
    expect(seCompanyTabFromPath("/admin/se/company/5560125220/people")).toBe("people");
  });
```

   (`seCompanyTabFromPath` and `seCompanyTabPath` are already imported by that file.)

3. The case "renders all nine tabs and marks exactly the active one" iterates
   `SE_COMPANY_TABS`, so it stays green — rename it "renders every tab and marks exactly the
   active one" so the name does not lie.

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/admin-se-company-person.test.tsx tests/admin-se-company-area.test.tsx tests/se-company-tabs.server.test.ts && npm run typecheck`
Expected: PASS — all three suites, the two rewritten cases included.
Then the whole suite: `npm test` — the only failures allowed are pre-existing ones (name them in the report).

- [ ] **Step 5: Docs**: spec section 7 gains Rulings 1 to 9 as sentences under "Actions" (the eighth intent, `discard`, named there beside the seven actions); section 10 gains the four list files, `se-person-tables.ts` and `fold-run-poller.tsx`; and **section 3.1.1 is amended** — it says "Reviewer: `r` plus 17 digits", which is the GROUP; a stored reviewer slot is that group plus a two-digit ordinal (19 digits), because one reviewer person is one suggestion row per role entry and the table is keyed `(company_id, source, slot)` (Ruling 2).

- [ ] **Step 6: Commit**

```bash
git add app/routes/admin-se-company-person.tsx app/routes.ts app/lib/se-company-tabs.ts \
  tests/admin-se-company-person.test.tsx tests/admin-se-company-area.test.tsx \
  ../dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md
git commit -m "feat(backoffice): People tab on the person entity"
```

---

### Task 5: The People list at `/admin/se/people`

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-people-filters.ts`, `app/lib/se-people-list.server.ts`, `app/components/admin/se-people-table.tsx`, `app/routes/admin-se-people.tsx`
- Modify: `app/routes.ts`, `app/components/admin/admin-sidebar.tsx`, `app/routes/admin-layout.tsx`
- Test: `corpscout/services/backoffice/tests/se-people-list.server.test.ts`, `tests/admin-se-people.test.tsx` (create)

**Interfaces:**
- Consumes: Task 1's `SE_COMPANY_PERSON_TABLE`, `isPersonSource`, `isPersonStatus`, `personSourceLabel`, `PERSON_STATUSES`; `chQuery`; `clampPage`/`clampPageSize` (`~/lib/paging`); `SE_COMPANIES_SERVING_TABLE` and `PAGE_LIMIT_OFFSET_SQL` (`~/lib/se-company-info-lists.server`); `DataTable` (`~/components/data-table/data-table`) and `DataTablePagination` (`~/components/data-table/pagination`), as `se-company-geocoding-table.tsx` uses them.
- Produces: `SePeopleFilters`, `parseSePeopleFilters`, `sePeopleHref`, `sePersonHref` (client-safe); `COMPANY_MATCH_LIMIT`, `PEOPLE_LIST_SELECT_SQL`, `PEOPLE_COUNTS_SQL`, `PEOPLE_COMPANY_NAMES_SQL`, `PEOPLE_COMPANY_SEARCH_SQL`, `SePeopleListRow`, `SePeopleCounts`, `SePeopleCompanyMatch`, `resolveSePeopleCompanyIds`, `buildSePeopleFilter`, `listSePeoplePage`, `loadSePeopleCounts` (server); `SePeopleTable`.
- **The company-name filter is resolved ONCE, in the loader** (pre-flight review 3.3): `listSePeoplePage` and `loadSePeopleCounts` both take the resolved `companyIds`, so the counts strip and the pager total honour the filter instead of reporting the whole 1,126,402 / 578,289.

Spec section 7's list, plainly: no writes, no fold, one row per published person, 50 per page,
sorted by company then display name, each row linking into that company's People tab.

- [ ] **Step 1: Write the failing tests**

`tests/se-people-list.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  buildSePeopleFilter,
  COMPANY_MATCH_LIMIT,
  listSePeoplePage,
  loadSePeopleCounts,
  resolveSePeopleCompanyIds,
  PEOPLE_COMPANY_NAMES_SQL,
  PEOPLE_COMPANY_SEARCH_SQL,
  PEOPLE_COUNTS_SQL,
  PEOPLE_LIST_SELECT_SQL,
} from "~/lib/se-people-list.server";

const EMPTY = { company: "", name: "", source: "", role: "", year: "", status: "" };
const ROW = {
  company_id: "5560125220", person_key: "a".repeat(64), display_name: "Anna Svensson",
  birth_year: "1975", wikidata_id: "", sources: ["bolagsverket"],
  current_roles: ["board_member"], role_years: [2025], first_year: "2024", last_year: "2025",
  active: 1, inactive_reason: "",
};

describe("se-people-list.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset().mockResolvedValue([]);
  });

  it("reads the main table through FINAL, sorted by company then name, paged by parameter", () => {
    expect(PEOPLE_LIST_SELECT_SQL).toContain("FROM corpscout.se_company_person_v2 AS p FINAL");
    expect(PEOPLE_LIST_SELECT_SQL).toContain("toString(p.person_key) AS person_key");
    expect(PEOPLE_LIST_SELECT_SQL).toContain("ifNull(toString(p.birth_year), '') AS birth_year");
    expect(PEOPLE_COUNTS_SQL).toContain("toString(countIf(p.active = 1)) AS active");
    expect(PEOPLE_COUNTS_SQL).toContain("toString(uniqExact(p.company_id)) AS companies");
    expect(PEOPLE_COMPANY_NAMES_SQL).toContain("FROM corpscout.se_companies_serving");
    expect(PEOPLE_COMPANY_NAMES_SQL).toContain("WHERE company_id IN {companyIds:Array(String)}");
    expect(PEOPLE_COMPANY_SEARCH_SQL).toContain("legal_name ILIKE {name:String}");
  });

  it("builds one predicate per filter, parameterized", async () => {
    expect(buildSePeopleFilter({ ...EMPTY, name: "svens" })).toEqual({
      where: ["p.display_name ILIKE {name:String}"], params: { name: "svens%" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, source: "esef" })).toEqual({
      where: ["has(p.sources, {source:String})"], params: { source: "esef" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, role: "board_member" })).toEqual({
      where: ["has(p.role_codes, {role:String})"], params: { role: "board_member" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, year: "2025" })).toEqual({
      where: ["has(p.role_years, {year:UInt16})"], params: { year: 2025 },
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "active" })).toEqual({
      where: ["p.active = 1"], params: {},
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "hidden" })).toEqual({
      where: ["p.inactive_reason = 'hidden'"], params: {},
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "withdrawn" })).toEqual({
      where: ["p.inactive_reason = 'withdrawn'"], params: {},
    });
    // The company filter is ALWAYS an id set by the time it reaches the SQL: the
    // loader resolved it once, for the page and the counts alike.
    expect(buildSePeopleFilter(EMPTY, ["5560125220"])).toEqual({
      where: ["p.company_id IN {companyIds:Array(String)}"],
      params: { companyIds: ["5560125220"] },
    });
    expect(buildSePeopleFilter(EMPTY, null)).toEqual({ where: [], params: {} });
  });

  it("resolves a company id without a query and a company name through the view, capped", async () => {
    // All digits is the company itself: no lookup at all.
    expect(await resolveSePeopleCompanyIds("5560125220")).toEqual({
      companyIds: ["5560125220"], truncated: false,
    });
    expect(await resolveSePeopleCompanyIds("")).toEqual({ companyIds: null, truncated: false });
    expect(clickhouse.query).not.toHaveBeenCalled();

    clickhouse.query.mockResolvedValue([{ company_id: "5560125220" }]);
    expect(await resolveSePeopleCompanyIds("beijer")).toEqual({
      companyIds: ["5560125220"], truncated: false,
    });
    expect(clickhouse.query).toHaveBeenCalledWith(PEOPLE_COMPANY_SEARCH_SQL, {
      name: "beijer%", limit: COMPANY_MATCH_LIMIT,
    });

    clickhouse.query.mockResolvedValue(
      Array.from({ length: COMPANY_MATCH_LIMIT }, (_, index) => ({ company_id: String(index) })),
    );
    expect((await resolveSePeopleCompanyIds("a")).truncated).toBe(true);
  });

  it("pages the persons under the resolved ids and names every company of the page in one lookup", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (String(sql).includes("se_company_person_v2")) return [ROW];
      if (sql === PEOPLE_COMPANY_NAMES_SQL) return [{ company_id: "5560125220", legal_name: "Beijer" }];
      return [];
    });
    const page = await listSePeoplePage({ ...EMPTY, companyIds: ["5560125220"], page: 2, pageSize: 50 });
    expect(page.rows).toEqual([{ ...ROW, legal_name: "Beijer" }]);
    const listCall = clickhouse.query.mock.calls.find(([sql]) => String(sql).includes("se_company_person_v2"));
    expect(listCall?.[0]).toContain("WHERE p.company_id IN {companyIds:Array(String)}");
    expect(listCall?.[0]).toContain("ORDER BY p.company_id, p.display_name");
    expect(listCall?.[0]).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
    expect(listCall?.[1]).toMatchObject({ companyIds: ["5560125220"], limit: 50, offset: 50 });
    expect(clickhouse.query.mock.calls.find(([sql]) => sql === PEOPLE_COMPANY_NAMES_SQL)?.[1]).toEqual({
      companyIds: ["5560125220"],
    });
    // A company the serving view has no row for reads blank, never `undefined`.
    clickhouse.query.mockImplementation(async (sql: string) =>
      String(sql).includes("se_company_person_v2") ? [ROW] : [],
    );
    expect((await listSePeoplePage({ ...EMPTY, companyIds: null, page: 1, pageSize: 50 })).rows[0]?.legal_name).toBe("");
  });

  it("counts persons, active persons and companies under the SAME filter and ids as the page", async () => {
    clickhouse.query.mockResolvedValue([{ persons: "1126402", active: "1126402", companies: "578289" }]);
    expect(await loadSePeopleCounts({ ...EMPTY, source: "esef", companyIds: null })).toEqual({
      persons: 1126402, active: 1126402, companies: 578289,
    });
    const [sql, params] = clickhouse.query.mock.calls[0] ?? [];
    expect(String(sql)).toContain("WHERE has(p.sources, {source:String})");
    expect(params).toEqual({ source: "esef" });

    // The pager total is `persons`, so a company-name filter MUST reach the counts too
    // (pre-flight review 3.3): without the ids the strip would say 1,126,402 over a page
    // of one company and the pager would offer 22,528 empty pages.
    clickhouse.query.mockReset().mockResolvedValue([{ persons: "7", active: "6", companies: "1" }]);
    expect(await loadSePeopleCounts({ ...EMPTY, companyIds: ["5560125220"] })).toEqual({
      persons: 7, active: 6, companies: 1,
    });
    const [countsSql, countsParams] = clickhouse.query.mock.calls[0] ?? [];
    expect(String(countsSql)).toContain("WHERE p.company_id IN {companyIds:Array(String)}");
    expect(countsParams).toEqual({ companyIds: ["5560125220"] });
  });
});
```

`tests/admin-se-people.test.tsx`:

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  listSePeoplePage: vi.fn(),
  loadSePeopleCounts: vi.fn(),
  resolveSePeopleCompanyIds: vi.fn(),
}));
vi.mock("~/lib/se-people-list.server", () => server);

import { loader } from "~/routes/admin-se-people";
import { SePeopleTable } from "~/components/admin/se-people-table";
import { parseSePeopleFilters, sePeopleHref, sePersonHref } from "~/lib/se-people-filters";

const KEY = "a".repeat(64);
const ROW = {
  company_id: "5560125220", legal_name: "Beijer Byggmaterial Aktiebolag", person_key: KEY,
  display_name: "Anna Svensson", birth_year: "1975", wikidata_id: "Q7",
  sources: ["bolagsverket", "esef"], current_roles: ["board_member"], role_years: [2024, 2025],
  first_year: "2024", last_year: "2025", active: 1, inactive_reason: "",
};
const COUNTS = { persons: 2, active: 1, companies: 1 };

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/people", element }], {
    initialEntries: [`/admin/se/people${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("se people filters", () => {
  it("reads and rebuilds the six filters, dropping anything the catalogue does not know", () => {
    const params = new URLSearchParams("company=beijer&name=svens&source=esef&role=board_member&year=2025&status=hidden");
    expect(parseSePeopleFilters(params)).toEqual({
      company: "beijer", name: "svens", source: "esef", role: "board_member", year: "2025", status: "hidden",
    });
    expect(parseSePeopleFilters(new URLSearchParams("source=scb&status=gone&year=20xx"))).toEqual({
      company: "", name: "", source: "", role: "", year: "", status: "",
    });
    expect(sePeopleHref({ company: "", name: "svens", source: "", role: "", year: "", status: "" }, 2, 50))
      .toBe("/admin/se/people?name=svens&page=2");
    expect(sePersonHref("5560125220", KEY)).toBe(`/admin/se/company/5560125220/people?person=${KEY}`);
  });
});

describe("admin-se-people route", () => {
  beforeEach(() => {
    server.listSePeoplePage.mockReset().mockResolvedValue({ rows: [ROW] });
    server.loadSePeopleCounts.mockReset().mockResolvedValue(COUNTS);
    server.resolveSePeopleCompanyIds.mockReset().mockResolvedValue({ companyIds: null, truncated: false });
  });

  it("pages and counts under the same filters", async () => {
    const data = await loader({
      request: new Request("http://x/admin/se/people?source=esef&page=3&pageSize=50"),
    } as never);
    expect(data).toEqual({
      rows: [ROW], counts: COUNTS, truncatedCompanies: false, page: 3, pageSize: 50,
      filters: { company: "", name: "", source: "esef", role: "", year: "", status: "" },
    });
    expect(server.listSePeoplePage).toHaveBeenCalledWith(
      expect.objectContaining({ source: "esef", companyIds: null, page: 3, pageSize: 50 }),
    );
    expect(server.loadSePeopleCounts).toHaveBeenCalledWith(
      expect.objectContaining({ source: "esef", companyIds: null }),
    );
  });

  it("resolves a company-name filter once and hands the same ids to the page and the counts", async () => {
    server.resolveSePeopleCompanyIds.mockResolvedValue({ companyIds: ["5560125220"], truncated: true });
    const data = await loader({
      request: new Request("http://x/admin/se/people?company=beijer"),
    } as never);
    expect(server.resolveSePeopleCompanyIds).toHaveBeenCalledTimes(1);
    expect(server.resolveSePeopleCompanyIds).toHaveBeenCalledWith("beijer");
    expect(server.listSePeoplePage).toHaveBeenCalledWith(
      expect.objectContaining({ companyIds: ["5560125220"] }),
    );
    expect(server.loadSePeopleCounts).toHaveBeenCalledWith(
      expect.objectContaining({ companyIds: ["5560125220"] }),
    );
    expect(data.truncatedCompanies).toBe(true);
  });

  it("renders the counts strip, the row and its link into the company tab", () => {
    const html = render(
      <SePeopleTable
        rows={[ROW]}
        counts={COUNTS}
        truncatedCompanies={false}
        page={1}
        pageSize={50}
        filters={{ company: "", name: "", source: "", role: "", year: "", status: "" }}
      />,
    );
    expect(html).toContain("Anna Svensson");
    expect(html).toContain("Beijer Byggmaterial Aktiebolag");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain(`/admin/se/company/5560125220/people?person=${KEY}`);
    // The strip's three numbers.
    expect(html).toContain("Persons");
    expect(html).toContain("Active");
    expect(html).toContain("Companies");
  });

  it("says when a company-name filter matched more companies than it could carry", () => {
    const html = render(
      <SePeopleTable
        rows={[ROW]}
        counts={COUNTS}
        truncatedCompanies
        page={1}
        pageSize={50}
        filters={{ company: "aktiebolag", name: "", source: "", role: "", year: "", status: "" }}
      />,
    );
    expect(html).toContain("first 200 matching companies");
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run: `npx vitest run tests/se-people-list.server.test.ts tests/admin-se-people.test.tsx`
Expected: FAIL — modules not found.

- [ ] **Step 3: Write the four modules**

`app/lib/se-people-filters.ts` — client-safe: `parseSePeopleFilters` trims each param, keeps
`source` only when `isPersonSource`, `status` only when `isPersonStatus`, `year` only on
`/^[0-9]{4}$/`, and caps `company`, `name` and `role` at 100 characters; `sePeopleHref` writes
back only the non-empty ones plus `page` (omitted at 1) and `pageSize` (omitted at the default);
`sePersonHref(companyId, personKey)` is `/admin/se/company/<id>/people?person=<key>`, both
segments `encodeURIComponent`d.

`app/lib/se-people-list.server.ts`:

```ts
/**
 * The `/admin/se/people` list (person spec section 7): one row per published person of
 * `se_company_person_v2`, read through FINAL, filtered, counted and paged server-side.
 * No writes and no fold -- the company's People tab owns every decision.
 *
 * The company NAME is not on the person table (there is no cross-company person
 * identity to hang it on), so it arrives in two reads instead of a join: a name filter
 * resolves to company ids through the serving view first, and the page's own rows are
 * named afterwards, at most 50 ids at a time. A join would make ClickHouse read the
 * 3.5M-row view for every page.
 */
export const COMPANY_MATCH_LIMIT = 200;

export const PEOPLE_LIST_SELECT_SQL = `SELECT
  p.company_id AS company_id, toString(p.person_key) AS person_key,
  p.display_name AS display_name, ifNull(toString(p.birth_year), '') AS birth_year,
  ifNull(p.wikidata_id, '') AS wikidata_id, arrayMap(x -> toString(x), p.sources) AS sources,
  p.current_roles AS current_roles, p.role_years AS role_years,
  ifNull(toString(p.first_year), '') AS first_year, ifNull(toString(p.last_year), '') AS last_year,
  toUInt8(p.active) AS active, toString(p.inactive_reason) AS inactive_reason
FROM ${SE_COMPANY_PERSON_TABLE} AS p FINAL`;

export const PEOPLE_COUNTS_SQL = `SELECT
  toString(count()) AS persons,
  toString(countIf(p.active = 1)) AS active,
  toString(uniqExact(p.company_id)) AS companies
FROM ${SE_COMPANY_PERSON_TABLE} AS p FINAL`;

export const PEOPLE_COMPANY_NAMES_SQL = `SELECT company_id, legal_name
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE company_id IN {companyIds:Array(String)}`;

export const PEOPLE_COMPANY_SEARCH_SQL = `SELECT company_id
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE legal_name ILIKE {name:String}
ORDER BY legal_name
LIMIT {limit:UInt32}`;
```

`resolveSePeopleCompanyIds(company)` runs FIRST and ONCE, in the route's loader: `""` gives
`{ companyIds: null, truncated: false }` (no company predicate at all), an all-digits company
gives `{ companyIds: [company] }` without any query, and anything else is a name prefix read
through `PEOPLE_COMPANY_SEARCH_SQL` (`name: "<company>%"`, `limit: COMPANY_MATCH_LIMIT`) with
`truncated` set when the read comes back full. The ids then go to BOTH readers, which is what
keeps the counts strip and the pager total honest under a name filter (pre-flight review 3.3:
without it the strip reports the whole 1,126,402 / 578,289 over a page of one company, and the
pager offers 22,528 empty pages).

`buildSePeopleFilter(filters, companyIds)` returns `{ where, params }` exactly as the test pins
it: no company predicate when `companyIds` is `null`, `p.company_id IN {companyIds:Array(String)}`
otherwise, plus one predicate per remaining filter. `listSePeoplePage` reads the page
(`${PEOPLE_LIST_SELECT_SQL}\n${where}\nORDER BY p.company_id, p.display_name\n${PAGE_LIMIT_OFFSET_SQL}`
with `clampPage`/`clampPageSize`), then names the page's distinct company ids in one
`PEOPLE_COMPANY_NAMES_SQL` read and attaches `legal_name` (`""` when the view has no row).
`loadSePeopleCounts` runs `PEOPLE_COUNTS_SQL` under the same `where` and the same params and
returns the three numbers as `Number(...)`; `persons` is also the pager's total.

`app/components/admin/se-people-table.tsx`: a counts strip of three `Badge`s (Persons, Active,
Companies), a `<Form method="get">` filter bar (company, name, source `<select>` over
`PERSON_SOURCES`, role text, year, status `<select>` over `PERSON_STATUSES`, plus Apply and
Clear), the `truncatedCompanies` note ("Showing people from the first 200 matching
companies."), a `DataTable` with columns Company (`legal_name` + the org number), Person
(`display_name`, birth year, QID), Roles (`current_roles` badges + `first_year`-`last_year`),
Sources (`personSourceLabel` badges) and Status (active / hidden / withdrawn), `rowHref={(row)
=> sePersonHref(row.company_id, row.person_key)}`, and `DataTablePagination` with
`itemsLabel="people"`.

`app/routes/admin-se-people.tsx`: `loader({ request })` parses the filters and the page,
awaits `resolveSePeopleCompanyIds(filters.company)` ONCE, then `Promise.all`s
`listSePeoplePage({ ...filters, companyIds, page, pageSize })` and
`loadSePeopleCounts({ ...filters, companyIds })`, and returns `{ rows, counts,
truncatedCompanies: truncated, page, pageSize, filters }`; `meta` titles it "People |
CompanyCollect admin"; the component renders a header ("People", one line of description) and
`SePeopleTable`.

`app/routes.ts`: `route("se/people", "routes/admin-se-people.tsx"),` beside the
`se/companies` layout (it is a sibling list area, not a companies tab).

`app/components/admin/admin-sidebar.tsx`: a second entry in `COUNTRY_NAVIGATION`'s Sweden
`items`, after Companies: `{ title: "People", to: "/admin/se/people", icon: UsersIcon, exact: false }`
(`UsersIcon` from `lucide-react`).

`app/routes/admin-layout.tsx`: a breadcrumb branch for `pathname === "/admin/se/people"` —
Admin / Sweden / People — beside the `onCompaniesPage` one.

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-people-list.server.test.ts tests/admin-se-people.test.tsx && npm run typecheck && npm test`
Expected: PASS; `npm test` green apart from pre-existing failures (name them in the report).

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-people-filters.ts app/lib/se-people-list.server.ts \
  app/components/admin/se-people-table.tsx app/routes/admin-se-people.tsx app/routes.ts \
  app/components/admin/admin-sidebar.tsx app/routes/admin-layout.tsx \
  tests/se-people-list.server.test.ts tests/admin-se-people.test.tsx
git commit -m "feat(backoffice): the SE People list over the person entity"
```

---

### Task 6: Smoke on prod data (controller with the owner)

No deploy step: this slice touches no Dagster code (Ruling 5), and
`se_company_person_fold_companies` has been live since slice 2 (main cdcb0cf6). The
backoffice runs locally from the main checkout (memory: "Backoffice runs locally").

1. [ ] Whole-branch review; the owner merges the branch (the main checkout is on `main`);
   the owner starts `npm run dev` there and opens `http://localhost:5183`.
2. [ ] `/admin/se/company/5020077862/people` (the owner's example): the persons list, the
   right panel's members, roles timeline, `data`, history and raw evidence render; the
   People tab sits beside Address in the sub-menu and the breadcrumb says People.
3. [ ] Two multi-source companies: `5568108988` (Håkan Samuelsson, wikidata + esef) and
   `5590701545` (Joakim Alexander Lundell, bolagsverket + wikidata) — one person, several
   sources, each member's spelling side by side, `text_source` and the spelling reason
   agreeing with the precedence block.
4. [ ] `/admin/se/people`: the counts strip (persons, active, companies), then each filter
   on its own — a company id, a company name prefix, a person-name prefix, `source=esef`,
   `role=board_member`, `year=2025`, `status=active` — and a row click landing on that
   company's People tab with the person selected.
5. [ ] One small test company (bucket 0, e.g. `5592501521`), every action in order, with the
   fold poller watched each time:
   - **Add** a person with two role years, Save draft (the draft card shows it, "parses on
     Fold now"), **Activate** (Ruling 6: the fold launches itself) — the reviewer person
     appears with source `reviewer` and its own spelling.
   - **Correct** a Bolagsverket person's spelling in a way that keeps the identity tokens
     (a diacritic, a casing fix): Activate writes NO hide rule and the person keeps its key
     with the reviewer's spelling (Ruling 1's first branch).
   - **Correct** another one changing the last name: Activate writes the hide rule, and
     after the fold the old person is `hidden` and the reviewer's is active.
   - **Merge** two persons the fold kept apart; **Split** one member back out (the dialog
     shows Ruling 9's caveat); **Remove** a source-backed person (hidden) and the reviewer
     person added in step 5 (withdrawn); **Reset** the rules on one of them; **Fold now**
     after each and read the result.
6. [ ] Read the rows back on prod:
   ```sql
   SELECT source, slot, first_name, last_name, birth_year, role_key, fiscal_year,
          role_from, role_to, data, suggested_at
   FROM corpscout.se_company_person_suggestion FINAL
   WHERE company_id = '5592501521' AND source IN ('reviewer', 'reviewer_draft')
   ORDER BY source, slot;

   SELECT rule_id, kind, person_keys, slots, active, note, created_at, created_by
   FROM corpscout.se_company_person_rule FINAL
   WHERE company_id = '5592501521' ORDER BY created_at;

   SELECT person_key, display_name, sources, slots, role_codes, role_years, text_source,
          active, inactive_reason, folded_at
   FROM corpscout.se_company_person_v2 FINAL
   WHERE company_id = '5592501521' ORDER BY active DESC, display_name;

   SELECT changed_at, change_kind, display_name, active, inactive_reason
   FROM corpscout.se_company_person_history
   WHERE company_id = '5592501521' ORDER BY changed_at;
   ```
   Every reviewer row must carry `JSONType(data) = 'Object'`, the sha256 of its own
   `(company_id, source, slot, suggested_at)` as `suggestion_id`, and a `role_key` that is a
   catalog code; every rule must carry a `rule_id` that is 64 hex and `created_by =
   'backoffice'`.
7. [ ] Leave the test company clean: Reset every rule written during the smoke, tombstone
   the reviewer persons, Fold now, and confirm the published set matches what the sources
   deliver again.
8. [ ] Record the slice-3 shipped record in the spec's section 9 item 3 (what shipped, the
   readouts, the rulings that moved); archive the ledger; update memory
   (`se-address-entity.md`'s PEOPLE entity line: slice 3 live, slice 4 next).

---

## Self-review

**1. Spec coverage.**
- 7 "the tab, six files": `se-company-person-entity.server.ts` (Task 2), `se-person-workspace.tsx`
  and `se-person-edit-sheet.tsx` (Task 3), `se-person-fields.ts` and `se-person-decision-form.ts`
  (Task 1), `fold-run-poller.tsx` reused (Task 3); the route `admin-se-company-person.tsx` at
  `/admin/se/company/:companyId/people` (Task 4).
- 7 "left column / right panel": Task 3's layout — persons active first with name, birth year,
  QID, current roles and sources; the members block, the roles timeline, the `data`, the history
  and the raw evidence rows.
- 7 "the seven actions": Add and Activate (Tasks 1 to 4, Rulings 2, 5, 6), Correct (Ruling 1),
  Remove (Task 2's two branches), Merge, Split (Ruling 9), Reset, Fold now — plus the eighth
  intent `discard` (Ruling 8), which the spec's section 7 gains in Task 4's doc step.
- **A deviation to record, not to hide**: spec 3.1.1 says a reviewer slot is `r` plus 17
  digits. That is the GROUP; a stored reviewer slot is 19 digits — the group plus a two-digit
  ordinal — because one reviewer person is one suggestion row per role entry and the table is
  keyed `(company_id, source, slot)` (Ruling 2). Task 4 Step 5 amends 3.1.1, and the slice
  record in section 9 repeats it.
- 7 "every write is an append": every writer inserts a new version; Global Constraints and Task 2.
- 7 "the `data` JSON editor and the note rules": Task 1's `validateSePersonInput` (an object, the
  reserved keys refused) and `NOTE_CONTROL`.
- 7 "the People list": Task 5.
- 3.1 "the reviewer's suggestion row": Task 2's `SePersonRawInsertRow`, the 18 columns of
  `tables.SUGGESTION_COLUMNS` in order, `source = reviewer` / `reviewer_draft`, `slot` = `r` +
  digits (Ruling 2), `suggestion_id` and `suggested_at` as the extractors compute them.
- 3.3 "the 29 main columns": `MAIN_COLUMNS_SQL` and `SePersonRow` (Task 2), pinned by the SQL test.
- 3.4 to 3.6: history (read, never written, `changed_at DESC`), rules (hide, merge, split,
  `active = 0` for Reset, `rule_id` stable across versions), precedence (read for the panel and
  for the member order, never written).
- 4 "what normalizes `ok`": Task 1's port and its test; the sheet refuses anything else.
- 5.2 "how the fold applies rules": Ruling 1 (a hide resolves through the previous members, which
  is why Correct must not write one when the text folds back), Ruling 9 (a split pins slots).
- 5.4/5.5: the roles timeline and the merged `data` are read from the folded row, never recomputed.
- 8 "readers": no reader changes; the serving view's people flags are untouched.
- 10 "names": every file and route name in the file map matches section 10, plus the four list
  files and `se-person-tables.ts` that Task 4 adds to that section.
- 9 items 0 to 2 (what is on prod): 1,126,402 persons over 578,289 companies, all active, no
  rules — so the tab must read well with zero rules and zero reviewer rows, which is why the
  empty states, "single source" and "no rule to reset" are all covered by tests.

**2. Placeholders.** Tasks 1, 2, 4 and 5 carry the modules' and tests' real code (the SQL
constants, the 18-column and 9-column payloads, the eight intents, the list's predicates); Task 3
describes the two components structurally against the address components that exist in the repo,
with the sheet's own test spelled out in full — the same shape the address plan used, and the
reason it is not literal JSX here is that the workspace is 1,100 lines of layout whose contract
(the props, the intents it posts, the strings the route test asserts) is pinned by Tasks 2, 4 and
the sheet test. No "TBD", no "add validation", no "similar to Task N".

**3. Type consistency.** `SePersonInput` and `SePersonRoleInput` (Task 1) feed `save-draft`
(Task 1) and `saveSePersonDraft` (Task 2); `SePersonRoleOption` (Task 1) is produced by
`loadSePersonRoleOptions` (Task 2), passed by the loader (Task 4) and consumed by the sheet
(Task 3) and `roleLabel` (Task 1); `SePersonDetail` (Task 2) feeds the workspace (Task 3) and the
loader (Task 4); `SePersonResult` (Task 3) is what the route's `action` returns (Task 4) and what
both the workspace and the sheet read; `SE_COMPANY_PERSON_TABLE` (Task 1) is the only place the
main table is named, by both the entity module (Task 2) and the list (Task 5); the slot helpers
(`personGroupSlot`, `personRowSlot`, `groupOfRowSlot`) are defined once and used by the writer,
the reader's draft grouping and the decision parser's pattern.

**4. Open question for the owner (spec 9 item 2's leftover).** A roleless observation contributes
no `(code, year)` pair, so a person whose only rows are roleless Bolagsverket signatures
publishes no `role_years` — 2.02M such rows on prod. This slice shows the members and their raw
`role_original`/`role_key` in the panel, so the reviewer can SEE the roleless years; it does not
change what the fold publishes. If the owner wants those years counted, that is a fold change in
a later slice, not here.
