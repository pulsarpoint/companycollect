# SE Basic Info Slice 3: Backoffice Info Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the admin company page's Info tab with a two-column page that shows the company's `se_company_basic_info` row field by field (left, two thirds) and, for the selected field, every source's suggestion row with the winner marked active (right, one third), with "Use this", "Release", a fold-pending marker and "Fold now".

**Architecture:** One new client-safe field catalogue, one new `.server` module (four SELECTs, one reviewer-row INSERT, one Dagster launch), one client-safe form parser, one workspace component, the rewritten route module and one resource route for polling a fold run. The old `se_company_info` review workspace and its form helper are deleted; the old `se-company-info.server.ts` stays because the companies list page and the pipeline sheet still read it (slice 4 retires it).

**Tech Stack:** React Router 8 framework mode, TypeScript, shadcn/ui (Card, Badge, Button, Accordion, Alert, Input), Tailwind, `@clickhouse/client` through `~/lib/clickhouse.server`, Dagster GraphQL through `~/lib/dagster.server`, vitest with `vi.hoisted` mocks and `renderToStaticMarkup`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`, sections 3.2 (suggestion table), 4 (precedence), 5 (fold, `folded_at`), 7 (Backoffice, amended 2026-09-04).

## Global Constraints

- All work is in `corpscout/services/backoffice`. Run commands from that directory: `npm run typecheck`, `npx vitest run <file>`, `npx vitest run` (whole suite), `npm run dev`.
- Route modules export only `loader`, `action`, `meta`, `shouldRevalidate`, `headers` and the default component. A component must never use a value from a `~/lib/*.server` module; `import type` from one is fine (backoffice CLAUDE.md).
- User-supplied values go to ClickHouse only through named query parameters (`{companyId:String}`), never interpolated (`chQuery` docstring).
- Reads use `FINAL` on `se_company_basic_info` and `se_company_basic_info_suggestion` (both ReplacingMergeTree); a decision is a NEW reviewer-row version, never an update (spec 3.2).
- The reviewer row: `source = 'reviewer'`, `source_record_uid = ''`, `observed_at` = the decision instant, `decided_by = 'backoffice'`, `suggested_at` = the same instant, `source_run_id = 'backoffice'`, `extractor_version = 'backoffice-v1'`. Release sets the field to NULL in a new version (spec 7).
- Fields: `legal_name`, `legal_form_code`, `status`, `incorporation_date`, `lei`, `wikidata_id`, `description`, `description_sv` are decidable; `description_language` travels with `description` (Use this on `description` copies both; Release on `description` clears both).
- Sources: `reviewer`, `llm`, `scb`, `bolagsverket`, `esef`, `wikidata`, `ratsit` (spec 11). "Use this" never names `reviewer`.
- `foldPending` = the company has no main row but has suggestions, or any suggestion row's `suggested_at` is newer than the main row's `folded_at` (both `YYYY-MM-DD HH:MM:SS.mmm` strings from ClickHouse, comparable as strings).
- Fold now launches asset `se_company_basic_info_fold_companies` through `launchRun` with `job: "__ASSET_JOB"`, `assetSelection: ["se_company_basic_info_fold_companies"]` and run config `{ ops: { se_company_basic_info_fold_companies: { config: { company_ids: [companyId] } } } }`.
- Commit by explicit path after every task; never `git add -A`. Commit trailers: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`, contiguous at the end.
- Deleted with the switch (Task 6): `app/components/admin/se-company-info-review-workspace.tsx`, `app/lib/se-info-field-value-form.ts`, `tests/admin-se-company-info.test.tsx`, `tests/se-info-field-value-form.test.ts`. Kept: `app/lib/se-company-info.server.ts`, `app/lib/se-info-field-values.ts`, `app/components/admin/company-description-card.tsx`, `app/components/admin/company-source-strip.tsx` (other tabs and the list page import them).

---

## File structure

| File | Responsibility |
| --- | --- |
| `app/lib/se-basic-info-fields.ts` (new, client-safe) | The nine fields with labels and kinds, the seven sources with labels, `selectedFieldFromSearch`, `foldPending` |
| `app/lib/se-basic-info-decision-form.ts` (new, client-safe) | Parses the Info tab's form posts into one of three intents |
| `app/lib/se-basic-info.server.ts` (new) | Row types, the four SELECTs + label lookup, `loadSeBasicInfoDetail`, `appendSeBasicInfoReviewerDecision`, `launchSeBasicInfoFold` |
| `app/lib/clickhouse.server.ts` (modify) | `chInsertSeBasicInfoSuggestions` |
| `app/lib/dagster.server.ts` (modify) | `ASSET_JOB_NAME`, `SE_BASIC_INFO_FOLD_COMPANIES_ASSET` |
| `app/components/admin/se-basic-info-workspace.tsx` (new) | `SeBasicInfoWorkspace`, `SeBasicInfoNotFolded`, the fields card, the suggestions panel, the history card, the fold-run poller |
| `app/routes/admin-se-company-info.tsx` (rewrite) | loader / action / meta / component on the new module |
| `app/routes/admin-se-company-info-run.ts` (new resource route) | `GET .../info/run/:runId` -> the run's status for the poller |
| `app/routes.ts` (modify) | registers the resource route |
| `tests/se-basic-info-fields.test.ts`, `tests/se-basic-info-decision-form.test.ts`, `tests/se-basic-info.server.test.ts`, `tests/admin-se-company-basic-info.test.tsx` (new) | one test file per module |

---

### Task 1: Field and source catalogue (client-safe)

**Files:**
- Create: `app/lib/se-basic-info-fields.ts`
- Test: `tests/se-basic-info-fields.test.ts`

**Interfaces:**
- Produces: `BASIC_INFO_FIELDS`, `SeBasicInfoField`, `isBasicInfoField(value): value is SeBasicInfoField`, `basicInfoFieldLabel(field): string`, `BASIC_INFO_SOURCES`, `SeBasicInfoSource`, `isBasicInfoSource(value)`, `basicInfoSourceLabel(source): string`, `DEFAULT_BASIC_INFO_FIELD = "legal_name"`, `selectedFieldFromSearch(search: URLSearchParams): SeBasicInfoField`, `foldPending(foldedAt: string | null, suggestedAts: readonly string[]): boolean`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/se-basic-info-fields.test.ts
import { describe, expect, it } from "vitest";
import {
  BASIC_INFO_FIELDS,
  BASIC_INFO_SOURCES,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  DEFAULT_BASIC_INFO_FIELD,
  foldPending,
  isBasicInfoField,
  isBasicInfoSource,
  selectedFieldFromSearch,
} from "~/lib/se-basic-info-fields";

describe("basic-info field catalogue", () => {
  it("lists the nine entity fields in display order", () => {
    expect(BASIC_INFO_FIELDS.map((field) => field.name)).toEqual([
      "legal_name",
      "legal_form_code",
      "status",
      "incorporation_date",
      "lei",
      "wikidata_id",
      "description",
      "description_sv",
    ]);
    expect(basicInfoFieldLabel("legal_form_code")).toBe("Legal form");
    expect(basicInfoFieldLabel("description_sv")).toBe("Description (Swedish)");
  });

  it("guards field and source names", () => {
    expect(isBasicInfoField("lei")).toBe(true);
    expect(isBasicInfoField("description_language")).toBe(false);
    expect(isBasicInfoField("")).toBe(false);
    expect(isBasicInfoSource("ratsit")).toBe(true);
    expect(isBasicInfoSource("scb ")).toBe(false);
  });

  it("names the seven sources with the reviewer first", () => {
    expect(BASIC_INFO_SOURCES).toEqual([
      "reviewer",
      "llm",
      "scb",
      "bolagsverket",
      "esef",
      "wikidata",
      "ratsit",
    ]);
    expect(basicInfoSourceLabel("scb")).toBe("SCB");
    expect(basicInfoSourceLabel("llm")).toBe("Model");
    expect(basicInfoSourceLabel("reviewer")).toBe("Reviewer");
    // An unknown token reads as itself rather than crashing the page.
    expect(basicInfoSourceLabel("somewhere")).toBe("somewhere");
  });

  it("reads the selected field from the URL and falls back to legal name", () => {
    expect(selectedFieldFromSearch(new URLSearchParams("field=status"))).toBe("status");
    expect(selectedFieldFromSearch(new URLSearchParams("field=nope"))).toBe(DEFAULT_BASIC_INFO_FIELD);
    expect(selectedFieldFromSearch(new URLSearchParams(""))).toBe("legal_name");
  });

  it("marks a fold pending when a suggestion is newer than the fold", () => {
    expect(foldPending("2026-09-04 17:04:01.293", ["2026-09-04 17:46:53.852"])).toBe(true);
    expect(foldPending("2026-09-04 17:04:01.293", ["2026-09-03 18:16:21.117"])).toBe(false);
    // Never folded but suggested: pending. Never folded, nothing suggested: not.
    expect(foldPending(null, ["2026-09-03 18:16:21.117"])).toBe(true);
    expect(foldPending(null, [])).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/se-basic-info-fields.test.ts`
Expected: FAIL, cannot resolve `~/lib/se-basic-info-fields`.

- [ ] **Step 3: Write the module**

```ts
// app/lib/se-basic-info-fields.ts
/**
 * The basic-info entity as the Info tab reads it: the nine decidable fields in
 * display order and the seven sources in the order the suggestions panel lists
 * them when the precedence table does not rank a source for a field.
 *
 * Client-safe on purpose (no `.server` import): the workspace component, the
 * form parser and the route all render from these.
 */

export const BASIC_INFO_FIELDS = [
  { name: "legal_name", label: "Legal name", kind: "text" },
  { name: "legal_form_code", label: "Legal form", kind: "code" },
  { name: "status", label: "Status", kind: "text" },
  { name: "incorporation_date", label: "Incorporated", kind: "date" },
  { name: "lei", label: "LEI", kind: "identifier" },
  { name: "wikidata_id", label: "Wikidata", kind: "identifier" },
  { name: "description", label: "Description", kind: "paragraph" },
  { name: "description_sv", label: "Description (Swedish)", kind: "paragraph" },
] as const;

export type SeBasicInfoField = (typeof BASIC_INFO_FIELDS)[number]["name"];
export type SeBasicInfoFieldKind = (typeof BASIC_INFO_FIELDS)[number]["kind"];

const FIELD_BY_NAME = new Map<string, (typeof BASIC_INFO_FIELDS)[number]>(
  BASIC_INFO_FIELDS.map((field) => [field.name, field]),
);

export function isBasicInfoField(value: string): value is SeBasicInfoField {
  return FIELD_BY_NAME.has(value);
}

export function basicInfoFieldLabel(field: SeBasicInfoField): string {
  return FIELD_BY_NAME.get(field)?.label ?? field;
}

export function basicInfoFieldKind(field: SeBasicInfoField): SeBasicInfoFieldKind {
  return FIELD_BY_NAME.get(field)?.kind ?? "text";
}

/** Spec section 11's source names; the reviewer first because it outranks all. */
export const BASIC_INFO_SOURCES = [
  "reviewer",
  "llm",
  "scb",
  "bolagsverket",
  "esef",
  "wikidata",
  "ratsit",
] as const;

export type SeBasicInfoSource = (typeof BASIC_INFO_SOURCES)[number];

const SOURCE_LABELS: Record<SeBasicInfoSource, string> = {
  reviewer: "Reviewer",
  llm: "Model",
  scb: "SCB",
  bolagsverket: "Bolagsverket",
  esef: "ESEF",
  wikidata: "Wikidata",
  ratsit: "Ratsit",
};

export function isBasicInfoSource(value: string): value is SeBasicInfoSource {
  return (BASIC_INFO_SOURCES as readonly string[]).includes(value);
}

/** What a reader calls a source token; an unknown token reads as itself. */
export function basicInfoSourceLabel(source: string): string {
  return isBasicInfoSource(source) ? SOURCE_LABELS[source] : source;
}

export const DEFAULT_BASIC_INFO_FIELD: SeBasicInfoField = "legal_name";

/** The suggestions panel's field is the URL (`?field=status`), so a link can
 * open the page on one field; anything else falls back to the legal name. */
export function selectedFieldFromSearch(search: URLSearchParams): SeBasicInfoField {
  const value = search.get("field") ?? "";
  return isBasicInfoField(value) ? value : DEFAULT_BASIC_INFO_FIELD;
}

/**
 * Whether the next fold would change this company: a suggestion row newer than
 * the fold, or suggestions for a company that has never been folded. Both
 * stamps are ClickHouse `YYYY-MM-DD HH:MM:SS.mmm` strings (UTC), so string
 * order is time order.
 */
export function foldPending(
  foldedAt: string | null,
  suggestedAts: readonly string[],
): boolean {
  if (suggestedAts.length === 0) return false;
  if (foldedAt === null) return true;
  return suggestedAts.some((suggestedAt) => suggestedAt > foldedAt);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/se-basic-info-fields.test.ts`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-basic-info-fields.ts tests/se-basic-info-fields.test.ts
git commit -m "feat(backoffice): basic-info field and source catalogue for the Info tab"
```

---

### Task 2: Server module -- reading the four tables

**Files:**
- Create: `app/lib/se-basic-info.server.ts`
- Test: `tests/se-basic-info.server.test.ts`

**Interfaces:**
- Consumes: `chQuery` from `~/lib/clickhouse.server`; `SHELL_LEGAL_FORM_LABEL_SQL` is NOT reused (it takes one code); this module has its own multi-code label query.
- Produces: `SeBasicInfoRow`, `SeBasicInfoSuggestionRow`, `SeBasicInfoHistoryRow`, `SeBasicInfoPrecedenceRow`, `SeBasicInfoLegalFormLabel`, `SeBasicInfoDetail`, `BASIC_INFO_SQL`, `BASIC_INFO_SUGGESTIONS_SQL`, `BASIC_INFO_HISTORY_SQL`, `BASIC_INFO_PRECEDENCE_SQL`, `BASIC_INFO_LEGAL_FORM_LABELS_SQL`, `loadSeBasicInfoDetail(companyId): Promise<SeBasicInfoDetail | null>`.

Every Nullable value column is collapsed to `''` with `ifNull` so the component never distinguishes `""` from null (the company area's rule, see `definition-list.tsx`); `Date32` and `DateTime64` are `toString`-ed; `LowCardinality(String)` columns are `toString`-ed so JSONEachRow yields plain strings.

- [ ] **Step 1: Write the failing test**

```ts
// tests/se-basic-info.server.test.ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn(), insert: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeBasicInfoSuggestions: clickhouse.insert,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn() }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_BASIC_INFO_FOLD_COMPANIES_ASSET: "se_company_basic_info_fold_companies",
}));

import {
  BASIC_INFO_HISTORY_SQL,
  BASIC_INFO_LEGAL_FORM_LABELS_SQL,
  BASIC_INFO_PRECEDENCE_SQL,
  BASIC_INFO_SQL,
  BASIC_INFO_SUGGESTIONS_SQL,
  loadSeBasicInfoDetail,
  type SeBasicInfoRow,
  type SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

export const MAIN_ROW: SeBasicInfoRow = {
  company_id: COMPANY,
  legal_name: "Fastighetsföreningen Sportstugan nr 1 upa",
  legal_name_source: "bolagsverket",
  legal_form_code: "51",
  legal_form_code_source: "bolagsverket",
  status: "inactive",
  status_source: "bolagsverket",
  incorporation_date: "1937-05-12",
  incorporation_date_source: "bolagsverket",
  lei: "",
  lei_source: "",
  wikidata_id: "",
  wikidata_id_source: "",
  description: "Föreningen har till ändamål att förvalta fastigheter.",
  description_source: "bolagsverket",
  description_language: "sv",
  description_sv: "Föreningen har till ändamål att förvalta fastigheter.",
  description_sv_source: "bolagsverket",
  folded_at: "2026-09-04 17:04:01.293",
  fold_version: "fold-v1",
  source_run_id: "da0c49db-d285-410e-8bed-cceed86ab82c",
};

export const BOLAGSVERKET_ROW: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  source_record_uid: "abc",
  observed_at: "2026-09-03 18:16:21.117",
  suggested_at: "2026-09-04 17:46:53.852",
  legal_name: "Fastighetsföreningen Sportstugan nr 1 upa",
  legal_form_code: "51",
  status: "inactive",
  incorporation_date: "1937-05-12",
  lei: "",
  wikidata_id: "",
  description: "Föreningen har till ändamål att förvalta fastigheter.",
  description_language: "sv",
  description_sv: "Föreningen har till ändamål att förvalta fastigheter.",
  decided_by: "",
  note: "",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};

function answer(sql: string): unknown[] {
  if (sql === BASIC_INFO_SQL) return [MAIN_ROW];
  if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW];
  if (sql === BASIC_INFO_HISTORY_SQL) {
    return [{ ...MAIN_ROW, changed_fields: ["legal_form_code"] }];
  }
  if (sql === BASIC_INFO_PRECEDENCE_SQL) {
    return [
      { field: "legal_name", source: "reviewer", precedence: 10000 },
      { field: "legal_name", source: "scb", precedence: 1000 },
      { field: "legal_name", source: "bolagsverket", precedence: 900 },
    ];
  }
  if (sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL) {
    return [{ code: "51", label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" }];
  }
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}

describe("se-basic-info.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => answer(sql));
  });

  it("pins the SQL to FINAL reads keyed on the company parameter", () => {
    expect(BASIC_INFO_SQL).toContain("FROM corpscout.se_company_basic_info AS b FINAL");
    expect(BASIC_INFO_SQL).toContain("WHERE b.company_id = {companyId:String}");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_basic_info_suggestion AS s FINAL");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("WHERE s.company_id = {companyId:String}");
    expect(BASIC_INFO_HISTORY_SQL).toContain("FROM corpscout.se_company_basic_info_history AS h");
    expect(BASIC_INFO_HISTORY_SQL).toContain("ORDER BY h.folded_at DESC");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_basic_info_precedence AS p FINAL");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("l.code IN {codes:Array(String)}");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("code_type = 'legal_form'");
    // Every nullable value column reaches the page as '' (never null).
    for (const column of ["legal_form_code", "lei", "wikidata_id", "description", "description_language", "description_sv"]) {
      expect(BASIC_INFO_SQL).toContain(`ifNull(b.${column}, '') AS ${column}`);
      expect(BASIC_INFO_SUGGESTIONS_SQL).toContain(`ifNull(s.${column}, '') AS ${column}`);
    }
    expect(BASIC_INFO_SQL).toContain("ifNull(toString(b.incorporation_date), '') AS incorporation_date");
  });

  it("loads the detail and labels every legal-form code it saw", async () => {
    const detail = await loadSeBasicInfoDetail(COMPANY);
    expect(detail).not.toBeNull();
    expect(detail?.info?.legal_form_code).toBe("51");
    expect(detail?.suggestions).toEqual([BOLAGSVERKET_ROW]);
    expect(detail?.history[0]?.changed_fields).toEqual(["legal_form_code"]);
    expect(detail?.precedence).toHaveLength(3);
    expect(detail?.legalFormLabels["51"]?.label_sv).toBe("Ekonomisk förening");
    expect(detail?.foldPending).toBe(true);
    const labelCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL);
    expect(labelCall?.[1]).toEqual({ codes: ["51"] });
  });

  it("is null only when neither the main row nor a suggestion exists", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SQL || sql === BASIC_INFO_SUGGESTIONS_SQL ? [] : answer(sql),
    );
    expect(await loadSeBasicInfoDetail(COMPANY)).toBeNull();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SQL ? [] : answer(sql),
    );
    const unfolded = await loadSeBasicInfoDetail(COMPANY);
    expect(unfolded?.info).toBeNull();
    expect(unfolded?.foldPending).toBe(true);
  });

  it("skips the label lookup when no code is in play", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_SQL) return [{ ...MAIN_ROW, legal_form_code: "" }];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [{ ...BOLAGSVERKET_ROW, legal_form_code: "" }];
      return answer(sql);
    });
    const detail = await loadSeBasicInfoDetail(COMPANY);
    expect(detail?.legalFormLabels).toEqual({});
    expect(clickhouse.query.mock.calls.some(([sql]) => sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL)).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/se-basic-info.server.test.ts`
Expected: FAIL, cannot resolve `~/lib/se-basic-info.server` (the dagster mock names constants that Task 4 adds; the mock is complete now so the file needs no edit later).

- [ ] **Step 3: Write the module (reads only; Task 3 and Task 4 append to it)**

```ts
// app/lib/se-basic-info.server.ts
import { chQuery } from "~/lib/clickhouse.server";
import { foldPending } from "~/lib/se-basic-info-fields";

/**
 * The Info tab's reads over the basic-info entity (spec 2026-09-03, sections
 * 3.2, 4, 5, 7): the folded main row with the source of every value, every
 * current suggestion row (one per source, reviewer included), the history
 * newest first, the exported precedence table, and the legal-form labels for
 * every code on the page.
 *
 * Every nullable value column is collapsed to '' so the component never has to
 * tell "" from null; dates and stamps arrive as ClickHouse's own strings.
 */

export interface SeBasicInfoRow {
  company_id: string;
  legal_name: string;
  legal_name_source: string;
  legal_form_code: string;
  legal_form_code_source: string;
  status: string;
  status_source: string;
  incorporation_date: string;
  incorporation_date_source: string;
  lei: string;
  lei_source: string;
  wikidata_id: string;
  wikidata_id_source: string;
  description: string;
  description_source: string;
  description_language: string;
  description_sv: string;
  description_sv_source: string;
  /** `YYYY-MM-DD HH:MM:SS.mmm` UTC. */
  folded_at: string;
  fold_version: string;
  source_run_id: string;
}

export interface SeBasicInfoSuggestionRow {
  company_id: string;
  source: string;
  source_record_uid: string;
  observed_at: string;
  suggested_at: string;
  legal_name: string;
  legal_form_code: string;
  status: string;
  incorporation_date: string;
  lei: string;
  wikidata_id: string;
  description: string;
  description_language: string;
  description_sv: string;
  decided_by: string;
  note: string;
  source_run_id: string;
  extractor_version: string;
}

export interface SeBasicInfoHistoryRow extends SeBasicInfoRow {
  changed_fields: string[];
}

export interface SeBasicInfoPrecedenceRow {
  field: string;
  source: string;
  precedence: number;
}

export interface SeBasicInfoLegalFormLabel {
  label_en: string;
  label_sv: string;
}

export interface SeBasicInfoDetail {
  /** Null when the company has suggestions but has never been folded (or
   * has no register legal name, spec 5's publish rule). */
  info: SeBasicInfoRow | null;
  suggestions: SeBasicInfoSuggestionRow[];
  history: SeBasicInfoHistoryRow[];
  precedence: SeBasicInfoPrecedenceRow[];
  /** Keyed by legal-form code: every code on the main row or any suggestion. */
  legalFormLabels: Record<string, SeBasicInfoLegalFormLabel>;
  foldPending: boolean;
}

const VALUE_COLUMNS_SQL = (alias: string) => `  ${alias}.legal_name AS legal_name,
  ifNull(${alias}.legal_form_code, '') AS legal_form_code,
  toString(${alias}.status) AS status,
  ifNull(toString(${alias}.incorporation_date), '') AS incorporation_date,
  ifNull(${alias}.lei, '') AS lei,
  ifNull(${alias}.wikidata_id, '') AS wikidata_id,
  ifNull(${alias}.description, '') AS description,
  ifNull(${alias}.description_language, '') AS description_language,
  ifNull(${alias}.description_sv, '') AS description_sv`;

const SOURCE_COLUMNS_SQL = (alias: string) => `  toString(${alias}.legal_name_source) AS legal_name_source,
  toString(${alias}.legal_form_code_source) AS legal_form_code_source,
  toString(${alias}.status_source) AS status_source,
  toString(${alias}.incorporation_date_source) AS incorporation_date_source,
  toString(${alias}.lei_source) AS lei_source,
  toString(${alias}.wikidata_id_source) AS wikidata_id_source,
  toString(${alias}.description_source) AS description_source,
  toString(${alias}.description_sv_source) AS description_sv_source,
  toString(${alias}.folded_at) AS folded_at,
  toString(${alias}.fold_version) AS fold_version,
  ${alias}.source_run_id AS source_run_id`;

export const BASIC_INFO_SQL = `SELECT
  b.company_id AS company_id,
${VALUE_COLUMNS_SQL("b")},
${SOURCE_COLUMNS_SQL("b")}
FROM corpscout.se_company_basic_info AS b FINAL
WHERE b.company_id = {companyId:String}
LIMIT 1`;

export const BASIC_INFO_SUGGESTIONS_SQL = `SELECT
  s.company_id AS company_id,
  toString(s.source) AS source,
  s.source_record_uid AS source_record_uid,
  toString(s.observed_at) AS observed_at,
  toString(s.suggested_at) AS suggested_at,
${VALUE_COLUMNS_SQL("s").replace("s.legal_name AS legal_name", "ifNull(s.legal_name, '') AS legal_name").replace("toString(s.status) AS status", "ifNull(s.status, '') AS status")},
  ifNull(s.decided_by, '') AS decided_by,
  ifNull(s.note, '') AS note,
  s.source_run_id AS source_run_id,
  toString(s.extractor_version) AS extractor_version
FROM corpscout.se_company_basic_info_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source`;

export const BASIC_INFO_HISTORY_SQL = `SELECT
  h.company_id AS company_id,
${VALUE_COLUMNS_SQL("h")},
${SOURCE_COLUMNS_SQL("h")},
  h.changed_fields AS changed_fields
FROM corpscout.se_company_basic_info_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.folded_at DESC
LIMIT 200`;

export const BASIC_INFO_PRECEDENCE_SQL = `SELECT
  toString(p.field) AS field,
  toString(p.source) AS source,
  toUInt32(p.precedence) AS precedence
FROM corpscout.se_company_basic_info_precedence AS p FINAL
ORDER BY p.field, p.precedence DESC`;

/** The curated dictionary for every code on the page at once; argMax over
 * `version` for the same reason as SHELL_LEGAL_FORM_LABEL_SQL. */
export const BASIC_INFO_LEGAL_FORM_LABELS_SQL = `SELECT
  l.code AS code,
  argMax(l.label_en, l.version) AS label_en,
  argMax(l.label_sv, l.version) AS label_sv
FROM corpscout.se_code_labels AS l
WHERE l.code_type = 'legal_form' AND l.code IN {codes:Array(String)}
GROUP BY l.code`;

interface LegalFormLabelQueryRow extends SeBasicInfoLegalFormLabel {
  code: string;
}

export async function loadSeBasicInfoDetail(
  companyId: string,
): Promise<SeBasicInfoDetail | null> {
  const [infoRows, suggestions, history, precedence] = await Promise.all([
    chQuery<SeBasicInfoRow>(BASIC_INFO_SQL, { companyId }),
    chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId }),
    chQuery<SeBasicInfoHistoryRow>(BASIC_INFO_HISTORY_SQL, { companyId }),
    chQuery<SeBasicInfoPrecedenceRow>(BASIC_INFO_PRECEDENCE_SQL),
  ]);
  const info = infoRows[0] ?? null;
  if (!info && suggestions.length === 0) return null;
  const codes = [
    ...new Set(
      [info?.legal_form_code ?? "", ...suggestions.map((row) => row.legal_form_code)].filter(
        (code) => code !== "",
      ),
    ),
  ];
  const labelRows =
    codes.length === 0
      ? []
      : await chQuery<LegalFormLabelQueryRow>(BASIC_INFO_LEGAL_FORM_LABELS_SQL, { codes });
  const legalFormLabels: Record<string, SeBasicInfoLegalFormLabel> = {};
  for (const row of labelRows) {
    legalFormLabels[row.code] = { label_en: row.label_en, label_sv: row.label_sv };
  }
  return {
    info,
    suggestions,
    history,
    precedence,
    legalFormLabels,
    foldPending: foldPending(
      info?.folded_at ?? null,
      suggestions.map((row) => row.suggested_at),
    ),
  };
}
```

Note the suggestion SELECT: `legal_name` and `status` are Nullable on the suggestion table but not on the main table, so the shared column snippet is adjusted for the suggestion alias with two `.replace` calls; the test's `ifNull(s.<column>, '')` loop covers the six always-nullable columns, and the implementer must add two assertions for `ifNull(s.legal_name, '') AS legal_name` and `ifNull(s.status, '') AS status` to the pin test.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/se-basic-info.server.test.ts`
Expected: PASS (4 tests). Then `npm run typecheck`: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-basic-info.server.ts tests/se-basic-info.server.test.ts
git commit -m "feat(backoffice): read the SE basic-info entity for the Info tab"
```

---

### Task 3: Reviewer decisions -- form parser and the reviewer-row write

**Files:**
- Create: `app/lib/se-basic-info-decision-form.ts`
- Modify: `app/lib/clickhouse.server.ts` (append `chInsertSeBasicInfoSuggestions` after `chInsertSeCompanyInfoFieldValues`)
- Modify: `app/lib/se-basic-info.server.ts` (append the write)
- Test: `tests/se-basic-info-decision-form.test.ts`, extend `tests/se-basic-info.server.test.ts`

**Interfaces:**
- Produces (client-safe): `SeBasicInfoDecision = { intent: "use-this"; field; source; note } | { intent: "release"; field; note } | { intent: "fold-now" }`, `parseSeBasicInfoDecision(form: FormData): { ok: true; decision } | { ok: false; error: string }`.
- Produces (server): `SeBasicInfoDecisionError` (Error subclass), `appendSeBasicInfoReviewerDecision(companyId, decision, now = new Date()): Promise<{ suggestedAt: string }>`, `chInsertSeBasicInfoSuggestions(values)`.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/se-basic-info-decision-form.test.ts
import { describe, expect, it } from "vitest";
import { parseSeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";

function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [key, value] of Object.entries(entries)) data.set(key, value);
  return data;
}

describe("parseSeBasicInfoDecision", () => {
  it("accepts use-this with a field, a non-reviewer source and an optional note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "scb", note: " keep " }))).toEqual({
      ok: true,
      decision: { intent: "use-this", field: "status", source: "scb", note: "keep" },
    });
  });

  it("refuses use-this from the reviewer or an unknown source or field", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "reviewer" }))).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "elsewhere" }))).toEqual({ ok: false, error: "Unknown source." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "description_language", source: "scb" }))).toEqual({ ok: false, error: "Unknown field." });
  });

  it("accepts release with a field, fold-now with nothing else", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "release", field: "description" }))).toEqual({
      ok: true,
      decision: { intent: "release", field: "description", note: "" },
    });
    expect(parseSeBasicInfoDecision(form({ intent: "fold-now" }))).toEqual({ ok: true, decision: { intent: "fold-now" } });
  });

  it("refuses an unknown intent and an over-long note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "edit", field: "status" }))).toEqual({ ok: false, error: "Unknown intent." });
    expect(parseSeBasicInfoDecision(form({ intent: "release", field: "status", note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
  });
});
```

Append to `tests/se-basic-info.server.test.ts` (inside the same `describe`, reusing `MAIN_ROW`, `BOLAGSVERKET_ROW`, `answer`, and the hoisted `clickhouse.insert`):

```ts
import { appendSeBasicInfoReviewerDecision, SeBasicInfoDecisionError } from "~/lib/se-basic-info.server";

const NOW = new Date("2026-09-04T19:30:00.123Z");

it("use-this copies the chosen source's value into a new reviewer-row version", async () => {
  clickhouse.insert.mockReset();
  const result = await appendSeBasicInfoReviewerDecision(
    COMPANY,
    { intent: "use-this", field: "legal_form_code", source: "bolagsverket", note: "register is right" },
    NOW,
  );
  expect(result).toEqual({ suggestedAt: "2026-09-04 19:30:00.123" });
  expect(clickhouse.insert).toHaveBeenCalledTimes(1);
  const [rows] = clickhouse.insert.mock.calls[0] as [Record<string, unknown>[]];
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    company_id: COMPANY,
    source: "reviewer",
    source_record_uid: "",
    observed_at: "2026-09-04 19:30:00.123",
    suggested_at: "2026-09-04 19:30:00.123",
    legal_form_code: "51",
    // Fields the reviewer never decided stay NULL: no opinion.
    legal_name: null,
    status: null,
    incorporation_date: null,
    description: null,
    description_language: null,
    decided_by: "backoffice",
    note: "register is right",
    source_run_id: "backoffice",
    extractor_version: "backoffice-v1",
  });
});

it("use-this on description carries the language; release clears both", async () => {
  clickhouse.insert.mockReset();
  await appendSeBasicInfoReviewerDecision(COMPANY, { intent: "use-this", field: "description", source: "bolagsverket", note: "" }, NOW);
  const [[first]] = clickhouse.insert.mock.calls as [Record<string, unknown>[]][];
  expect(first).toMatchObject({ description: BOLAGSVERKET_ROW.description, description_language: "sv" });
  // The next version starts from the current reviewer row.
  clickhouse.query.mockImplementation(async (sql: string) =>
    sql === BASIC_INFO_SUGGESTIONS_SQL
      ? [BOLAGSVERKET_ROW, { ...BOLAGSVERKET_ROW, source: "reviewer", legal_name: "", legal_form_code: "", status: "", incorporation_date: "", description: "Kept text", description_language: "sv", description_sv: "", decided_by: "backoffice" }]
      : answer(sql),
  );
  clickhouse.insert.mockReset();
  await appendSeBasicInfoReviewerDecision(COMPANY, { intent: "release", field: "description", note: "" }, NOW);
  const [[second]] = clickhouse.insert.mock.calls as [Record<string, unknown>[]][];
  expect(second).toMatchObject({ description: null, description_language: null, note: null });
});

it("refuses a source with no opinion on the field", async () => {
  await expect(
    appendSeBasicInfoReviewerDecision(COMPANY, { intent: "use-this", field: "lei", source: "bolagsverket", note: "" }, NOW),
  ).rejects.toBeInstanceOf(SeBasicInfoDecisionError);
  await expect(
    appendSeBasicInfoReviewerDecision(COMPANY, { intent: "use-this", field: "status", source: "scb", note: "" }, NOW),
  ).rejects.toThrow("SCB has no status for this company.");
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/se-basic-info-decision-form.test.ts tests/se-basic-info.server.test.ts`
Expected: FAIL (module missing; `appendSeBasicInfoReviewerDecision` not exported).

- [ ] **Step 3: Write the parser, the insert helper and the write**

```ts
// app/lib/se-basic-info-decision-form.ts
/**
 * Turns the Info tab's form posts into one decision. Client-safe (no `.server`
 * import): the route's own module must not drag the server module into the
 * client bundle, and the refusals are unit-testable without ClickHouse.
 */
import {
  isBasicInfoField,
  isBasicInfoSource,
  type SeBasicInfoField,
  type SeBasicInfoSource,
} from "~/lib/se-basic-info-fields";

export type SeBasicInfoDecision =
  | { intent: "use-this"; field: SeBasicInfoField; source: Exclude<SeBasicInfoSource, "reviewer">; note: string }
  | { intent: "release"; field: SeBasicInfoField; note: string }
  | { intent: "fold-now" };

export type SeBasicInfoDecisionRequest =
  | { ok: true; decision: SeBasicInfoDecision }
  | { ok: false; error: string };

export const MAX_NOTE_LENGTH = 500;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function refuse(error: string): SeBasicInfoDecisionRequest {
  return { ok: false, error };
}

export function parseSeBasicInfoDecision(form: FormData): SeBasicInfoDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent !== "use-this" && intent !== "release") return refuse("Unknown intent.");
  const field = text(form, "field");
  if (!isBasicInfoField(field)) return refuse("Unknown field.");
  const note = text(form, "note").trim();
  if (note.length > MAX_NOTE_LENGTH) return refuse(`Note is longer than ${MAX_NOTE_LENGTH} characters.`);
  if (intent === "release") return { ok: true, decision: { intent, field, note } };
  const source = text(form, "source");
  if (!isBasicInfoSource(source)) return refuse("Unknown source.");
  if (source === "reviewer") return refuse("Use this needs a source other than the reviewer.");
  return { ok: true, decision: { intent, field, source, note } };
}
```

Append to `app/lib/clickhouse.server.ts` right after `chInsertSeCompanyInfoFieldValues`:

```ts
/** Append a reviewer-row version to the SE basic-info suggestion table; the
 * fold reads the newest version per (company_id, source) through FINAL. */
export async function chInsertSeBasicInfoSuggestions<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_basic_info_suggestion",
    values,
    format: "JSONEachRow",
  });
}
```

Append to `app/lib/se-basic-info.server.ts` (add `chInsertSeBasicInfoSuggestions` to the clickhouse import, and import `basicInfoSourceLabel`, `basicInfoFieldLabel`, `type SeBasicInfoField` from `~/lib/se-basic-info-fields` and `type SeBasicInfoDecision` from `~/lib/se-basic-info-decision-form`):

```ts
export class SeBasicInfoDecisionError extends Error {}

/** ClickHouse's own DateTime64(3) text form, UTC. */
export function clickhouseStamp(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

const VALUE_FIELDS = [
  "legal_name",
  "legal_form_code",
  "status",
  "incorporation_date",
  "lei",
  "wikidata_id",
  "description",
  "description_language",
  "description_sv",
] as const;

type ReviewerRowValues = Record<(typeof VALUE_FIELDS)[number], string | null>;

/** The row as inserted: '' from the reads becomes NULL ("no opinion") here. */
function reviewerValues(row: SeBasicInfoSuggestionRow | undefined): ReviewerRowValues {
  const values = {} as ReviewerRowValues;
  for (const field of VALUE_FIELDS) {
    const value = row?.[field] ?? "";
    values[field] = value === "" ? null : value;
  }
  return values;
}

/**
 * One reviewer decision = one new version of this company's reviewer row
 * (spec 3.2, 7): the current reviewer row's values, one field changed,
 * `observed_at`/`suggested_at` = the decision instant. Use this copies the
 * chosen source's value (and the language with a description); Release sets
 * the field (and that language) back to NULL.
 */
export async function appendSeBasicInfoReviewerDecision(
  companyId: string,
  decision: Exclude<SeBasicInfoDecision, { intent: "fold-now" }>,
  now: Date = new Date(),
): Promise<{ suggestedAt: string }> {
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const values = reviewerValues(suggestions.find((row) => row.source === "reviewer"));
  const field: SeBasicInfoField = decision.field;
  if (decision.intent === "use-this") {
    const chosen = suggestions.find((row) => row.source === decision.source);
    const value = chosen?.[field] ?? "";
    if (value === "") {
      throw new SeBasicInfoDecisionError(
        `${basicInfoSourceLabel(decision.source)} has no ${basicInfoFieldLabel(field).toLowerCase()} for this company.`,
      );
    }
    values[field] = value;
    if (field === "description") {
      values.description_language = chosen?.description_language === "" ? null : (chosen?.description_language ?? null);
    }
  } else {
    values[field] = null;
    if (field === "description") values.description_language = null;
  }
  const stamp = clickhouseStamp(now);
  await chInsertSeBasicInfoSuggestions([
    {
      company_id: companyId,
      source: "reviewer",
      source_record_uid: "",
      observed_at: stamp,
      ...values,
      decided_by: "backoffice",
      note: decision.note === "" ? null : decision.note,
      suggested_at: stamp,
      source_run_id: "backoffice",
      extractor_version: "backoffice-v1",
    },
  ]);
  return { suggestedAt: stamp };
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/se-basic-info-decision-form.test.ts tests/se-basic-info.server.test.ts`
Expected: PASS (4 + 7 tests). `npm run typecheck`: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-basic-info-decision-form.ts app/lib/clickhouse.server.ts app/lib/se-basic-info.server.ts tests/se-basic-info-decision-form.test.ts tests/se-basic-info.server.test.ts
git commit -m "feat(backoffice): reviewer decisions write SE basic-info reviewer-row versions"
```

---

### Task 4: Fold now -- the launch and the run-status resource route

**Files:**
- Modify: `app/lib/dagster.server.ts` (constants next to `SE_COMPANY_INFO_ASSET`, line 50)
- Modify: `app/lib/se-basic-info.server.ts` (append `launchSeBasicInfoFold`)
- Create: `app/routes/admin-se-company-info-run.ts`
- Modify: `app/routes.ts` (register the resource route as the first child after `route("info", ...)`)
- Test: extend `tests/se-basic-info.server.test.ts`; create `tests/admin-se-company-info-run.test.ts`

**Interfaces:**
- Produces: `ASSET_JOB_NAME = "__ASSET_JOB"`, `SE_BASIC_INFO_FOLD_COMPANIES_ASSET = "se_company_basic_info_fold_companies"`, `launchSeBasicInfoFold(companyId): Promise<{ runId: string; url: string | null }>`, resource route loader returning `{ runId, status, finished }` where `finished` is true for `SUCCESS`, `FAILURE`, `CANCELED`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/se-basic-info.server.test.ts`:

```ts
import { launchSeBasicInfoFold } from "~/lib/se-basic-info.server";

it("fold-now launches the per-company fold asset for exactly this company", async () => {
  dagster.launchRun.mockResolvedValue({ runId: "run-9", status: "QUEUED" });
  const launched = await launchSeBasicInfoFold(COMPANY);
  expect(launched.runId).toBe("run-9");
  expect(dagster.launchRun).toHaveBeenCalledWith({
    job: "__ASSET_JOB",
    assetSelection: ["se_company_basic_info_fold_companies"],
    runConfig: { ops: { se_company_basic_info_fold_companies: { config: { company_ids: [COMPANY] } } } },
    tags: { "backoffice/basic-info": "fold-now" },
  });
});
```

```ts
// tests/admin-se-company-info-run.test.ts
import { describe, expect, it, vi } from "vitest";

const dagster = vi.hoisted(() => ({ runStatus: vi.fn() }));
vi.mock("~/lib/dagster.server", () => ({ runStatus: dagster.runStatus }));

import { loader } from "~/routes/admin-se-company-info-run";

describe("info run resource route", () => {
  it("reports whether the run reached a terminal state", async () => {
    dagster.runStatus.mockResolvedValueOnce({ runId: "run-9", status: "STARTED" });
    const running = await loader({ params: { companyId: "0113004022", runId: "run-9" } } as never);
    expect(running).toEqual({ runId: "run-9", status: "STARTED", finished: false });
    dagster.runStatus.mockResolvedValueOnce({ runId: "run-9", status: "SUCCESS" });
    const done = await loader({ params: { companyId: "0113004022", runId: "run-9" } } as never);
    expect(done).toEqual({ runId: "run-9", status: "SUCCESS", finished: true });
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/se-basic-info.server.test.ts tests/admin-se-company-info-run.test.ts`
Expected: FAIL (`launchSeBasicInfoFold` not exported; route module missing).

- [ ] **Step 3: Write the code**

In `app/lib/dagster.server.ts`, after `export const SE_COMPANY_INFO_ASSET = "se_company_info_clickhouse";`:

```ts
/** Dagster's implicit job for materializing assets by selection; what the
 * GraphQL launcher wants when no named job wraps the asset. */
export const ASSET_JOB_NAME = "__ASSET_JOB";
/** The targeted basic-info fold (spec 5): re-folds the companies named in its
 * config whatever their bucket. Launched by the Info tab's Fold now. */
export const SE_BASIC_INFO_FOLD_COMPANIES_ASSET = "se_company_basic_info_fold_companies";
```

Append to `app/lib/se-basic-info.server.ts` (import `ASSET_JOB_NAME`, `SE_BASIC_INFO_FOLD_COMPANIES_ASSET`, `launchRun`, `dagsterRunUrl` from `~/lib/dagster.server`):

```ts
export const FOLD_NOW_TAG = { "backoffice/basic-info": "fold-now" } as const;

/** Fold now (spec 7): one run of the targeted fold for this company alone. */
export async function launchSeBasicInfoFold(
  companyId: string,
): Promise<{ runId: string; url: string | null }> {
  const run = await launchRun({
    job: ASSET_JOB_NAME,
    assetSelection: [SE_BASIC_INFO_FOLD_COMPANIES_ASSET],
    runConfig: {
      ops: { [SE_BASIC_INFO_FOLD_COMPANIES_ASSET]: { config: { company_ids: [companyId] } } },
    },
    tags: { ...FOLD_NOW_TAG },
  });
  return { runId: run.runId, url: dagsterRunUrl(run.runId) };
}
```

Add `dagsterRunUrl: vi.fn(() => null)` to the dagster mock at the top of `tests/se-basic-info.server.test.ts`.

```ts
// app/routes/admin-se-company-info-run.ts
import type { Route } from "./+types/admin-se-company-info-run";
import { runStatus } from "~/lib/dagster.server";

/** Terminal Dagster run states: the poller stops and the page reloads on these. */
const FINISHED = new Set(["SUCCESS", "FAILURE", "CANCELED"]);

/**
 * Resource route (no component): the Info tab's Fold now poller reads one run's
 * status here every few seconds until it finishes, then revalidates the page so
 * the freshly folded row appears. Only `loader` lives here.
 */
export async function loader({ params }: Route.LoaderArgs) {
  const run = await runStatus(params.runId);
  return { runId: run.runId, status: run.status, finished: FINISHED.has(run.status) };
}
```

In `app/routes.ts`, directly after `route("info", "routes/admin-se-company-info.tsx"),` add:

```ts
      // Resource route behind the Info tab's Fold now: the run poller.
      route("info/run/:runId", "routes/admin-se-company-info-run.ts"),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm run typecheck && npx vitest run tests/se-basic-info.server.test.ts tests/admin-se-company-info-run.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/dagster.server.ts app/lib/se-basic-info.server.ts app/routes/admin-se-company-info-run.ts app/routes.ts tests/se-basic-info.server.test.ts tests/admin-se-company-info-run.test.ts
git commit -m "feat(backoffice): Fold now launches the targeted SE basic-info fold and polls its run"
```

---

### Task 5: The workspace component

**Files:**
- Create: `app/components/admin/se-basic-info-workspace.tsx`
- Test: `tests/admin-se-company-basic-info.test.tsx` (component half; Task 6 adds the route half)

**Interfaces:**
- Consumes: `SeBasicInfoDetail`, `SeBasicInfoSuggestionRow` (`import type` from the server module), the Task 1 catalogue, shadcn `Card`, `Badge`, `Button`, `Accordion`, `Alert`, `Input`, `DefinitionList`/`EMPTY_VALUE`/`text`, `LegalForm` with `LegalFormLabels`.
- Produces: `SeBasicInfoWorkspace({ detail, selectedField, result })`, `SeBasicInfoNotFolded({ companyId })`, `SeBasicInfoResult` type: `{ ok: true; suggestedAt: string } | { ok: true; launched: { runId: string; url: string | null } } | { ok: false; error: string } | null`.

Layout: `<div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">`; left column a `Card` "Basic info" plus the history `Card`; right column `<aside className="lg:sticky lg:top-4 lg:self-start">` with the suggestions `Card`. Each left row is a `<Link to={{ search: \`?field=${name}\` }} preventScrollReset>` around a `<dt>/<dd>` pair, `aria-current="true"` and a `bg-muted` background on the selected one. The panel lists, for the selected field, one row per source in this order: the precedence rows for that field (already sorted by precedence DESC), then any source present in the suggestions but not in the precedence, then the remaining catalogue sources. A row with a value shows it, the source label, `observed_at`, an "Active" `Badge` when `source === info[`${field}_source`]`, and a "Use this" button (a `<Form method="post">` with hidden `intent=use-this`, `field`, `source`, and the shared `note` input's value via a hidden field mirrored from state) unless active, reviewer, or `info === null` (no main row: Use this still allowed -- a reviewer may decide before the first fold). A row without a value shows the label greyed with "no opinion". The reviewer row additionally shows "Release" (`intent=release`) when it has a value. Above the list: when `detail.foldPending`, an `Alert` "Fold pending" with the "Fold now" `<Form method="post">` (`intent=fold-now`). After `result` is a launch, `FoldRunPoller` polls the resource route.

- [ ] **Step 1: Write the failing test (component half)**

```ts
// tests/admin-se-company-basic-info.test.tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  SeBasicInfoNotFolded,
  SeBasicInfoWorkspace,
} from "~/components/admin/se-basic-info-workspace";
import type { SeBasicInfoDetail, SeBasicInfoSuggestionRow } from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

const bolagsverket: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  source_record_uid: "abc",
  observed_at: "2026-09-03 18:16:21.117",
  suggested_at: "2026-09-04 17:46:53.852",
  legal_name: "Sportstugan upa",
  legal_form_code: "51",
  status: "inactive",
  incorporation_date: "1937-05-12",
  lei: "",
  wikidata_id: "",
  description: "Förvaltar fastigheter.",
  description_language: "sv",
  description_sv: "Förvaltar fastigheter.",
  decided_by: "",
  note: "",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};
const scb: SeBasicInfoSuggestionRow = {
  ...bolagsverket,
  source: "scb",
  legal_form_code: "51",
  status: "active",
  description: "",
  description_language: "",
  description_sv: "",
  suggested_at: "2026-09-04 11:20:00.000",
};

const detail: SeBasicInfoDetail = {
  info: {
    company_id: COMPANY,
    legal_name: "Sportstugan upa",
    legal_name_source: "scb",
    legal_form_code: "51",
    legal_form_code_source: "scb",
    status: "active",
    status_source: "scb",
    incorporation_date: "1937-05-12",
    incorporation_date_source: "scb",
    lei: "",
    lei_source: "",
    wikidata_id: "",
    wikidata_id_source: "",
    description: "Förvaltar fastigheter.",
    description_source: "bolagsverket",
    description_language: "sv",
    description_sv: "Förvaltar fastigheter.",
    description_sv_source: "bolagsverket",
    folded_at: "2026-09-04 17:04:01.293",
    fold_version: "fold-v1",
    source_run_id: "run-f",
  },
  suggestions: [bolagsverket, scb],
  history: [],
  precedence: [
    { field: "status", source: "reviewer", precedence: 10000 },
    { field: "status", source: "scb", precedence: 1000 },
    { field: "status", source: "bolagsverket", precedence: 900 },
    { field: "status", source: "ratsit", precedence: 300 },
  ],
  legalFormLabels: { "51": { label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" } },
  foldPending: true,
};

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/info", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/info${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeBasicInfoWorkspace", () => {
  it("lists every field with its winning source and marks the selected row", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="status" result={null} />, "?field=status");
    for (const label of ["Legal name", "Legal form", "Status", "Incorporated", "LEI", "Wikidata", "Description", "Description (Swedish)"]) {
      expect(html).toContain(label);
    }
    expect(html).toContain("Ekonomisk förening");
    expect(html).toContain('aria-current="true"');
    expect(html).toContain("fold-v1");
    expect(html).toContain("2026-09-04 17:04:01.293");
  });

  it("orders the panel by precedence, marks the winner active and greys silent sources", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="status" result={null} />, "?field=status");
    const scbAt = html.indexOf('data-source="scb"');
    const bolagsverketAt = html.indexOf('data-source="bolagsverket"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    const reviewerAt = html.indexOf('data-source="reviewer"');
    expect(reviewerAt).toBeLessThan(scbAt);
    expect(scbAt).toBeLessThan(bolagsverketAt);
    expect(bolagsverketAt).toBeLessThan(ratsitAt);
    expect(html).toMatch(/data-source="scb"[^]*?Active/);
    expect(html).toMatch(/data-source="ratsit"[^]*?no opinion/);
    // Bolagsverket has a different status, so it offers Use this; SCB is active and does not.
    expect(html).toMatch(/data-source="bolagsverket"[^]*?Use this/);
    expect(html).not.toMatch(/data-source="scb"[^]*?<button[^>]*>Use this/);
  });

  it("shows the fold-pending alert with Fold now, and the poller after a launch", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="legal_name" result={null} />);
    expect(html).toContain("Fold pending");
    expect(html).toContain('value="fold-now"');
    const launched = render(
      <SeBasicInfoWorkspace detail={detail} selectedField="legal_name" result={{ ok: true, launched: { runId: "run-9", url: null } }} />,
    );
    expect(launched).toContain("run-9");
    const settled = render(<SeBasicInfoWorkspace detail={{ ...detail, foldPending: false }} selectedField="legal_name" result={null} />);
    expect(settled).not.toContain("Fold pending");
  });

  it("renders an error result and the not-folded state", () => {
    expect(render(<SeBasicInfoWorkspace detail={detail} selectedField="lei" result={{ ok: false, error: "Unknown source." }} />)).toContain("Unknown source.");
    expect(renderToStaticMarkup(<SeBasicInfoNotFolded companyId={COMPANY} />)).toContain("not in se_company_basic_info yet");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/admin-se-company-basic-info.test.tsx`
Expected: FAIL, cannot resolve the component module.

- [ ] **Step 3: Write the component**

```tsx
// app/components/admin/se-basic-info-workspace.tsx
import { CheckCircle2Icon, FileSearchIcon, TriangleAlertIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { Form, Link, useFetcher, useNavigation, useRevalidator } from "react-router";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Input } from "~/components/ui/input";
import { EMPTY_VALUE, text } from "~/components/admin/definition-list";
import { LegalForm } from "~/components/admin/legal-form";
import {
  BASIC_INFO_FIELDS,
  BASIC_INFO_SOURCES,
  basicInfoFieldKind,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  type SeBasicInfoField,
} from "~/lib/se-basic-info-fields";
import type {
  SeBasicInfoDetail,
  SeBasicInfoRow,
  SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";
import { cn } from "~/lib/utils";

export type SeBasicInfoResult =
  | { ok: true; suggestedAt: string }
  | { ok: true; launched: { runId: string; url: string | null } }
  | { ok: false; error: string }
  | null;

/** Shown when the company has neither a folded row nor a suggestion. */
export function SeBasicInfoNotFolded({ companyId }: { companyId: string }) {
  return (
    <div className="flex flex-col gap-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not folded yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} is not in se_company_basic_info yet and no source
            has suggested anything for it. The extractors write suggestions from
            the registers; the fold publishes the row.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <a
            className={buttonVariants({ variant: "outline" })}
            href={`/company/se/${encodeURIComponent(companyId)}`}
          >
            Back to company
          </a>
        </EmptyContent>
      </Empty>
    </div>
  );
}

function sourceOf(info: SeBasicInfoRow | null, field: SeBasicInfoField): string {
  return info ? info[`${field}_source`] : "";
}

function valueOf(
  row: SeBasicInfoRow | SeBasicInfoSuggestionRow | null,
  field: SeBasicInfoField,
): string {
  return row ? row[field] : "";
}

function FieldValue({
  field,
  value,
  language,
  labels,
}: {
  field: SeBasicInfoField;
  value: string;
  language: string;
  labels: SeBasicInfoDetail["legalFormLabels"];
}) {
  if (value === "") return EMPTY_VALUE;
  const kind = basicInfoFieldKind(field);
  if (kind === "code") {
    const label = labels[value] ?? { label_en: "", label_sv: "" };
    return <LegalForm form={{ code: value, ...label }} />;
  }
  if (kind === "paragraph") {
    return (
      <span className="whitespace-pre-line">
        {value}
        {language === "" ? null : (
          <Badge variant="outline" className="ml-2 align-middle">
            {language}
          </Badge>
        )}
      </span>
    );
  }
  if (kind === "identifier") return <span className="font-mono break-all">{value}</span>;
  return text(value);
}

function FieldsCard({
  detail,
  selectedField,
}: {
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
}) {
  const { info } = detail;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Basic info</CardTitle>
        <CardDescription>
          The folded row: every value with the source that won it. Click a row to
          see what each source suggests for it.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {info ? null : (
          <Alert className="mb-4">
            <TriangleAlertIcon />
            <AlertTitle>Not folded yet</AlertTitle>
            <AlertDescription>
              Sources have suggested values but no fold has published this company.
            </AlertDescription>
          </Alert>
        )}
        {/* A list of links, not a <dl>: an anchor may not wrap dt/dd pairs. */}
        <ul className="grid grid-cols-1 gap-y-1 text-sm">
          {BASIC_INFO_FIELDS.map((field) => {
            const selected = field.name === selectedField;
            const source = sourceOf(info, field.name);
            return (
              <li key={field.name}>
                <Link
                  to={{ search: `?field=${field.name}` }}
                  preventScrollReset
                  aria-current={selected ? "true" : undefined}
                  className={cn(
                    "grid grid-cols-1 gap-x-6 rounded-md px-2 py-2 hover:bg-muted/60 sm:grid-cols-[minmax(11rem,auto)_1fr_auto]",
                    selected && "bg-muted",
                  )}
                >
                  <span className="text-muted-foreground text-xs uppercase tracking-wide sm:pt-0.5">
                    {field.label}
                  </span>
                  <span>
                    <FieldValue
                      field={field.name}
                      value={valueOf(info, field.name)}
                      language={field.name === "description" ? (info?.description_language ?? "") : ""}
                      labels={detail.legalFormLabels}
                    />
                  </span>
                  <span className="sm:text-right">
                    {source === "" ? null : (
                      <Badge variant="secondary">{basicInfoSourceLabel(source)}</Badge>
                    )}
                  </span>
                </Link>
              </li>
            );
          })}
        </ul>
        {info ? (
          <p className="text-muted-foreground mt-4 text-xs">
            Folded {info.folded_at} · {info.fold_version} · run{" "}
            <span className="font-mono">{info.source_run_id}</span>
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

/** The panel's rows for one field: precedence order first, then any suggesting
 * source the table does not rank, then the rest of the catalogue. */
function panelSources(detail: SeBasicInfoDetail, field: SeBasicInfoField): string[] {
  const ranked = detail.precedence.filter((row) => row.field === field).map((row) => row.source);
  const suggesting = detail.suggestions.map((row) => row.source);
  const ordered: string[] = [];
  for (const source of [...ranked, ...suggesting, ...BASIC_INFO_SOURCES]) {
    if (!ordered.includes(source)) ordered.push(source);
  }
  return ordered;
}

function SuggestionsPanel({
  detail,
  selectedField,
  result,
}: {
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
  result: SeBasicInfoResult;
}) {
  const navigation = useNavigation();
  const busy = navigation.state !== "idle";
  const [note, setNote] = useState("");
  const winner = sourceOf(detail.info, selectedField);
  const rows = panelSources(detail, selectedField).map((source) => ({
    source,
    row: detail.suggestions.find((row) => row.source === source) ?? null,
  }));
  return (
    <Card>
      <CardHeader>
        <CardTitle>{basicInfoFieldLabel(selectedField)}</CardTitle>
        <CardDescription>
          What each source suggests, highest precedence first. The active one is
          what the fold published.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {detail.foldPending ? (
          <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Fold pending</AlertTitle>
            <AlertDescription>
              A suggestion is newer than the last fold. Fold now publishes it.
              <Form method="post" className="mt-2">
                <input type="hidden" name="intent" value="fold-now" />
                <Button type="submit" size="sm" disabled={busy}>
                  Fold now
                </Button>
              </Form>
            </AlertDescription>
          </Alert>
        ) : null}
        {result && result.ok && "launched" in result ? (
          <FoldRunPoller companyId={detail.suggestions[0]?.company_id ?? detail.info?.company_id ?? ""} runId={result.launched.runId} url={result.launched.url} />
        ) : null}
        {result && !result.ok ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>Not saved</AlertTitle>
            <AlertDescription>{result.error}</AlertDescription>
          </Alert>
        ) : null}
        {result && result.ok && "suggestedAt" in result ? (
          <Alert>
            <CheckCircle2Icon />
            <AlertTitle>Decision saved</AlertTitle>
            <AlertDescription>Reviewer row written at {result.suggestedAt}. Fold now to publish it.</AlertDescription>
          </Alert>
        ) : null}
        <label className="text-muted-foreground text-xs" htmlFor="basic-info-note">
          Note (saved with the next decision)
        </label>
        <Input
          id="basic-info-note"
          value={note}
          maxLength={500}
          onChange={(event) => setNote(event.target.value)}
          placeholder="Why this value"
        />
        <ul className="flex flex-col gap-2">
          {rows.map(({ source, row }) => {
            const value = valueOf(row, selectedField);
            const active = source !== "" && source === winner;
            const hasValue = value !== "";
            return (
              <li
                key={source}
                data-source={source}
                className={cn(
                  "rounded-md border p-3 text-sm",
                  active && "border-primary bg-primary/5",
                  !hasValue && "text-muted-foreground opacity-70",
                )}
              >
                <div className="flex items-center gap-2">
                  <span className="font-medium">{basicInfoSourceLabel(source)}</span>
                  {active ? <Badge>Active</Badge> : null}
                  {row ? (
                    <span className="text-muted-foreground ml-auto text-xs">{row.observed_at}</span>
                  ) : null}
                </div>
                <div className="mt-1">
                  {hasValue ? (
                    <FieldValue
                      field={selectedField}
                      value={value}
                      language={selectedField === "description" ? (row?.description_language ?? "") : ""}
                      labels={detail.legalFormLabels}
                    />
                  ) : (
                    <span>no opinion</span>
                  )}
                </div>
                {row?.note ? <p className="text-muted-foreground mt-1 text-xs">{row.note}</p> : null}
                {hasValue && !active && source !== "reviewer" ? (
                  <Form method="post" className="mt-2">
                    <input type="hidden" name="intent" value="use-this" />
                    <input type="hidden" name="field" value={selectedField} />
                    <input type="hidden" name="source" value={source} />
                    <input type="hidden" name="note" value={note} />
                    <Button type="submit" size="sm" variant="outline" disabled={busy}>
                      Use this
                    </Button>
                  </Form>
                ) : null}
                {hasValue && source === "reviewer" ? (
                  <Form method="post" className="mt-2">
                    <input type="hidden" name="intent" value="release" />
                    <input type="hidden" name="field" value={selectedField} />
                    <input type="hidden" name="note" value={note} />
                    <Button type="submit" size="sm" variant="outline" disabled={busy}>
                      Release
                    </Button>
                  </Form>
                ) : null}
              </li>
            );
          })}
        </ul>
      </CardContent>
    </Card>
  );
}

/** Polls the run resource route until the fold finishes, then reloads the page. */
function FoldRunPoller({ companyId, runId, url }: { companyId: string; runId: string; url: string | null }) {
  const fetcher = useFetcher<{ status: string; finished: boolean }>();
  const revalidator = useRevalidator();
  const finished = fetcher.data?.finished ?? false;
  useEffect(() => {
    if (finished) {
      revalidator.revalidate();
      return;
    }
    const path = `/admin/se/company/${encodeURIComponent(companyId)}/info/run/${encodeURIComponent(runId)}`;
    fetcher.load(path);
    const timer = setInterval(() => fetcher.load(path), 3000);
    return () => clearInterval(timer);
    // fetcher is stable per React Router's contract; re-run only on identity changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [companyId, runId, finished]);
  return (
    <Alert>
      <CheckCircle2Icon />
      <AlertTitle>{finished ? `Fold ${fetcher.data?.status?.toLowerCase() ?? "finished"}` : "Folding"}</AlertTitle>
      <AlertDescription>
        Run <span className="font-mono">{runId}</span>
        {url ? (
          <>
            {" "}
            (<a className="underline" href={url} target="_blank" rel="noreferrer">open in Dagster</a>)
          </>
        ) : null}
        {finished ? " -- reloading." : " -- the page reloads when it finishes."}
      </AlertDescription>
    </Alert>
  );
}

function HistoryCard({ detail }: { detail: SeBasicInfoDetail }) {
  return (
    <Card>
      <Accordion type="single" collapsible>
        <AccordionItem value="history" className="border-0">
          <CardHeader>
            <AccordionTrigger className="py-0">
              <div className="text-left">
                <CardTitle>History</CardTitle>
                <CardDescription>
                  Every fold that changed a value, newest first ({detail.history.length}).
                </CardDescription>
              </div>
            </AccordionTrigger>
          </CardHeader>
          <AccordionContent>
            <CardContent>
              {detail.history.length === 0 ? (
                <p className="text-muted-foreground text-sm">No fold has published this company yet.</p>
              ) : (
                <ul className="flex flex-col gap-2 text-sm">
                  {detail.history.map((row) => (
                    <li key={`${row.folded_at}-${row.source_run_id}`} className="grid gap-x-4 sm:grid-cols-[auto_1fr_auto]">
                      <span className="font-mono text-xs">{row.folded_at}</span>
                      <span>
                        {row.changed_fields.map((field) => (
                          <Badge key={field} variant="outline" className="mr-1">
                            {field}
                          </Badge>
                        ))}
                      </span>
                      <span className="text-muted-foreground text-xs">{row.fold_version}</span>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </Card>
  );
}

export function SeBasicInfoWorkspace({
  detail,
  selectedField,
  result,
}: {
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
  result: SeBasicInfoResult;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">
      <div className="flex flex-col gap-6">
        <FieldsCard detail={detail} selectedField={selectedField} />
        <HistoryCard detail={detail} />
      </div>
      <aside className="lg:sticky lg:top-4 lg:self-start">
        <SuggestionsPanel detail={detail} selectedField={selectedField} result={result} />
      </aside>
    </div>
  );
}
```

Note on `changed_fields`: a first-publish history row lists every non-NULL field (`fold.py`, `changed_fields_against(None)`), so the card simply lists the badges; no row ever carries an empty array.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/admin-se-company-basic-info.test.tsx && npm run typecheck`
Expected: PASS (4 tests), typecheck clean. If `Accordion` inside `CardHeader` does not type-check against the shadcn versions in this repo, move the `AccordionTrigger` inside `CardContent` -- the collapsed-by-default behaviour is what matters.

- [ ] **Step 5: Commit**

```bash
git add app/components/admin/se-basic-info-workspace.tsx tests/admin-se-company-basic-info.test.tsx
git commit -m "feat(backoffice): two-column SE basic-info workspace with the suggestions panel"
```

---

### Task 6: Switch the Info tab, delete the old workspace, verify the whole suite

**Files:**
- Rewrite: `app/routes/admin-se-company-info.tsx`
- Delete: `app/components/admin/se-company-info-review-workspace.tsx`, `app/lib/se-info-field-value-form.ts`, `tests/admin-se-company-info.test.tsx`, `tests/se-info-field-value-form.test.ts`
- Test: extend `tests/admin-se-company-basic-info.test.tsx` (route half)

**Interfaces:**
- Consumes: everything above. The route's `loader` reads `?field` via `selectedFieldFromSearch(new URL(request.url).searchParams)`; `action` parses with `parseSeBasicInfoDecision`, then `launchSeBasicInfoFold` or `appendSeBasicInfoReviewerDecision`, catching `SeBasicInfoDecisionError` into `{ ok: false, error }` and letting anything else throw.

- [ ] **Step 1: Write the failing test (route half)**

Append to `tests/admin-se-company-basic-info.test.tsx`, ABOVE the existing imports add the hoisted mock (vi.mock must precede the route import):

```ts
import { beforeEach, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSeBasicInfoDetail: vi.fn(),
  appendSeBasicInfoReviewerDecision: vi.fn(),
  launchSeBasicInfoFold: vi.fn(),
  SeBasicInfoDecisionError: class SeBasicInfoDecisionError extends Error {},
}));
vi.mock("~/lib/se-basic-info.server", () => server);

import { action, loader } from "~/routes/admin-se-company-info";
```

and the tests:

```ts
describe("admin-se-company-info route", () => {
  beforeEach(() => {
    server.loadSeBasicInfoDetail.mockReset().mockResolvedValue(detail);
    server.appendSeBasicInfoReviewerDecision.mockReset().mockResolvedValue({ suggestedAt: "2026-09-04 19:30:00.123" });
    server.launchSeBasicInfoFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail and the selected field from the URL", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/info?field=status`),
      params: { companyId: COMPANY },
    } as never);
    expect(response.data).toEqual({ detail, selectedField: "status" });
    expect(response.init?.status).toBeUndefined();
    server.loadSeBasicInfoDetail.mockResolvedValueOnce(null);
    const missing = await loader({ request: new Request("http://x/info"), params: { companyId: COMPANY } } as never);
    expect(missing.data).toEqual({ detail: null, selectedField: "legal_name" });
    expect(missing.init?.status).toBe(404);
  });

  it("writes a reviewer decision, launches a fold, and reports refusals", async () => {
    const post = (entries: Record<string, string>) => {
      const body = new FormData();
      for (const [key, value] of Object.entries(entries)) body.set(key, value);
      return action({ request: new Request("http://x/info", { method: "POST", body }), params: { companyId: COMPANY } } as never);
    };
    expect(await post({ intent: "use-this", field: "status", source: "bolagsverket" })).toEqual({ ok: true, suggestedAt: "2026-09-04 19:30:00.123" });
    expect(server.appendSeBasicInfoReviewerDecision).toHaveBeenCalledWith(COMPANY, { intent: "use-this", field: "status", source: "bolagsverket", note: "" });
    expect(await post({ intent: "fold-now" })).toEqual({ ok: true, launched: { runId: "run-9", url: null } });
    expect(await post({ intent: "use-this", field: "status", source: "reviewer" })).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    server.appendSeBasicInfoReviewerDecision.mockRejectedValueOnce(new server.SeBasicInfoDecisionError("SCB has no LEI for this company."));
    expect(await post({ intent: "use-this", field: "lei", source: "scb" })).toEqual({ ok: false, error: "SCB has no LEI for this company." });
    server.appendSeBasicInfoReviewerDecision.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(post({ intent: "release", field: "lei" })).rejects.toThrow("clickhouse down");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/admin-se-company-basic-info.test.tsx`
Expected: FAIL, `loader`'s data has no `selectedField` and `action` refuses the new intents.

- [ ] **Step 3: Rewrite the route, delete the old files**

```tsx
// app/routes/admin-se-company-info.tsx
import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeBasicInfoNotFolded,
  SeBasicInfoWorkspace,
} from "~/components/admin/se-basic-info-workspace";
import {
  appendSeBasicInfoReviewerDecision,
  launchSeBasicInfoFold,
  loadSeBasicInfoDetail,
  SeBasicInfoDecisionError,
} from "~/lib/se-basic-info.server";
import { parseSeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";
import { selectedFieldFromSearch } from "~/lib/se-basic-info-fields";

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  const detail = await loadSeBasicInfoDetail(params.companyId);
  const selectedField = selectedFieldFromSearch(new URL(request.url).searchParams);
  // A company no extractor has suggested and no fold has published is a
  // normal pipeline state, not a broken link: the page says so under a 404.
  return data({ detail, selectedField }, detail ? undefined : { status: 404 });
}

/**
 * One reviewer decision (a new reviewer-row version) or one Fold now launch.
 * The store's refusals are the reviewer's to read; anything else is a real
 * failure and must not be dressed up as a form error.
 */
export async function action({ request, params }: Route.ActionArgs) {
  const parsed = parseSeBasicInfoDecision(await request.formData());
  if (!parsed.ok) return { ok: false as const, error: parsed.error };
  if (parsed.decision.intent === "fold-now") {
    const launched = await launchSeBasicInfoFold(params.companyId);
    return { ok: true as const, launched };
  }
  try {
    const { suggestedAt } = await appendSeBasicInfoReviewerDecision(params.companyId, parsed.decision);
    return { ok: true as const, suggestedAt };
  } catch (error) {
    if (error instanceof SeBasicInfoDecisionError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.detail?.info?.legal_name ?? params.companyId} basic info | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenCompanyInfo({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.detail) {
    return <SeBasicInfoNotFolded companyId={params.companyId} />;
  }
  return (
    <SeBasicInfoWorkspace
      detail={loaderData.detail}
      selectedField={loaderData.selectedField}
      result={actionData ?? null}
    />
  );
}
```

Then delete the four old files:

```bash
git rm app/components/admin/se-company-info-review-workspace.tsx app/lib/se-info-field-value-form.ts tests/admin-se-company-info.test.tsx tests/se-info-field-value-form.test.ts
```

Before deleting, confirm with `rg -n "se-company-info-review-workspace|se-info-field-value-form" app tests` that the only importers are the route (rewritten above) and the two deleted tests. If any other file imports them, stop and report (the plan's premise is wrong).

- [ ] **Step 4: Run the whole suite and the typecheck**

Run: `npm run typecheck && npx vitest run`
Expected: typecheck clean; every test passes (the pre-existing suites for the companies list, the pipeline sheet and `se-company-info.server.test.ts` still pass because their modules were kept).

- [ ] **Step 5: Commit**

```bash
git add app/routes/admin-se-company-info.tsx tests/admin-se-company-basic-info.test.tsx
git commit -m "feat(backoffice): Info tab reads SE basic info with per-field suggestions, Use this, Release and Fold now"
```

(The `git rm` in Step 3 already staged the deletions; they ride in this commit.)

---

### Task 7: Owner-gated smoke on the dev server against production ClickHouse

Not a subagent task: the controller runs it with the owner. No code changes.

- [ ] Start `npm run dev` (or use the running one on `http://localhost:5183`) and open `http://localhost:5183/admin/se/company/0113004022/info`. Expect: two columns, eight field rows (Bolagsverket badges), `?field=legal_name` default; clicking "Status" shows the panel with bolagsverket active and the other sources greyed; the history card is collapsed and opens to one first-publish row.
- [ ] Open a company both registers know (any `55…` company; find one with `SELECT company_id FROM corpscout.se_company_basic_info FINAL WHERE legal_name_source = 'scb' LIMIT 1`): the status row's panel shows scb active, bolagsverket with its own value and "Use this".
- [ ] Click "Use this" on the Bolagsverket status with a note. Expect "Decision saved"; `SELECT * FROM corpscout.se_company_basic_info_suggestion FINAL WHERE company_id = … AND source = 'reviewer' FORMAT Vertical` shows the new row with `status` set, everything else NULL, `decided_by = 'backoffice'`, the note. The panel shows the reviewer row with "Release" and "Fold pending".
- [ ] Click "Fold now". Expect the run alert, then the page reloading with the status row's badge reading "Reviewer" and the history card gaining a row with `status` in `changed_fields`. Check Dagster: run tagged `backoffice/basic-info = fold-now` succeeded (`companies 1, changed 1`).
- [ ] Click "Release" on the reviewer row, then "Fold now" again. Expect the badge back to "SCB" and a second history row.
- [ ] Record the smoke's company id and run ids in the ledger and the spec's section 10 slice-3 line; merge to main from the owner's checkout; deploy the backoffice by its own recipe (its `ansible/` directory; the controller reads `corpscout/services/backoffice/README.md` for the deploy command before running it).

---

## Self-review

- Spec coverage: section 7's reads (main row, suggestions reviewer first, history, precedence) -> Task 2; Use this / Release -> Task 3; fold pending + Fold now -> Tasks 4 and 5; the 2026-09-04 layout amendment -> Task 5; the deletions -> Task 6; Edit and the pipeline sheet are explicitly deferred by the amendment.
- Placeholders: none; every step carries its code.
- Type consistency: `SeBasicInfoResult` (Task 5) matches the route's `actionData` union (Task 6: `{ok:true, suggestedAt}`, `{ok:true, launched}`, `{ok:false, error}`); `SeBasicInfoDecision` (Task 3) is what the route passes to `appendSeBasicInfoReviewerDecision` minus `fold-now`; `selectedFieldFromSearch` (Task 1) feeds `selectedField: SeBasicInfoField` on loader data and the component prop; the run resource route's `{ runId, status, finished }` is what `FoldRunPoller` reads.
