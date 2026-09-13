# Address tab paging — a slim list, a detail for one row, a paged Workplaces card

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/admin/se/company/:companyId/address` render a kommun with 1,502 `workplace` rows in under 400 KB instead of 7.8 MB, by returning list rows without their members, resolving members for the selected row alone, and moving workplace-only rows into their own server-paged, server-filtered card.

**Architecture:** One loader split into small reads. `loadSeAddressDetail(companyId, { selectedKey, workplacePage, workplaceQuery })` runs nine parallel queries — the Addresses list (every row except an ACTIVE row whose `kinds` are exactly `['workplace']`), the workplace page, the workplace count, history, rules, the draft raw rows, the draft normalized rows, one fold-state row of five scalar aggregates, and the selected row — then two more reads, bound to that one row's own `(source, slot)` pairs, to build its members. The selected row is the `?address=` row when the key names one of this company's, else the first active row of the list (owner ruling 2026-09-13: the panel keeps the default it has always had), and it is null only for a company with no active company address at all. The re-fold-pending mark the list used to compute in TypeScript over every normalized row of the company is now one ClickHouse expression per row against two `groupArray` join keys. The page's search string (`?address`, `?workplaces`, `?workplace_q`) is built by one helper, so selecting an address keeps the workplace page and paging keeps the selected address. No schema change, no fold change, no change to the five reviewer writes.

**Tech Stack:** TypeScript 5.9 on React Router 8.0.0 (v7-style route modules, `app/routes/`, `~/` alias), React 19, vitest 4 (`npm test` = `vitest run`, `npm run typecheck` = `react-router typegen && tsc`), `@clickhouse/client` 1.23 against ClickHouse 26.5 (`corpscout` database, `{name:Type}` named query parameters, `readonly=2` on the read client), shadcn/ui + Tailwind, leaflet behind a client-only wrapper.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` — this plan is section 8's paragraph "Amended 2026-09-13 (Address tab paging, after Ratsit slice 3 …)", rules 1 and 2, resting on section 8's original description of the tab and on the `workplace` split migration 000403 gave the serving view.

## Global Constraints

- Work only in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-address-tab-paging`. **Never `git stash`.** Never `git add -A` or `git add .` — stage by explicit path, every time.
- Commit with a message file, never `-m`: `git commit -F "$MSGFILE"`. Every message ends with the two trailers, in this order:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- **No schema change and no migration.** Every table this slice reads already exists: `corpscout.se_company_address`, `…_history`, `…_normalized`, `…_suggestion`, `…_rule`. Nothing here writes to ClickHouse except the five reviewer writes that already exist, which are **not changed**.
- **Only `corpscout/services/backoffice` and two docs change.** No Dagster asset, no Python, no `corpscout/clickhouse/migrations`.
- Every user-supplied value reaches ClickHouse as a **named query parameter** (`{name:Type}`), never interpolated into SQL. `chQuery` (`app/lib/clickhouse.server.ts:70`) binds `params` as `query_params`.
- Exact URL contract, unchanged for `address` and new for the other two: `?address=<64-hex key>`, `?workplaces=<page>` (1-based, default 1, anything else 1), `?workplace_q=<text>` (trimmed, capped at 100 characters, case-insensitive CONTAINS over `normalized_address`). Page size is the exported constant `WORKPLACE_PAGE_SIZE = 50`.
- The workplace split is **exactly** the serving view's (`sweden_company/companies_current.py:125`, migration 000403): a row whose `kinds` are EXACTLY `['workplace']`. A merged row that carries `workplace` beside another kind (`['postal','visiting_or_postal','workplace']`) is a company address and stays in the Addresses card.
- Verification after every task: `npm run typecheck` clean, and `npm test` with **no new failures** — the suite has pre-existing failures unrelated to addresses. Record the failing-test names before the first change and compare; **do not "fix" a pre-existing failure.**
- Run every npm command from `corpscout/services/backoffice` in this worktree (it has its own `node_modules` and its own `.env`).
- The route module `app/routes/admin-se-company-address.tsx` may export only `loader`, `action`, `meta` and the component. Any other export that touched `~/lib/*.server` would keep that module in the client bundle and break the production build.
- The five reviewer writes (`saveSeAddressDraft`, `activateSeAddressDraft`, `discardSeAddressDraft`, `removeSeAddress`, `resetSeAddress`) keep reading `ADDRESS_MAIN_SQL`, `ADDRESS_RAW_SQL`, `ADDRESS_NORMALIZED_SQL` and `ADDRESS_RULES_SQL` whole. They are POST paths, one per reviewer click, and `removeSeAddress` must be able to find a workplace row by key. **Those four SQL constants keep their current text, byte for byte.**
- Out of scope, deliberately: the schema, the fold, the People tab, the corrections list, virtualised lists, client-side fetching, and any change to what Correct / Remove / Reset / Fold now write.

## File Structure

| file | what happens |
| --- | --- |
| `corpscout/services/backoffice/app/lib/se-address-fields.ts` | modified — gains `WORKPLACE_PAGE_SIZE`, `MAX_WORKPLACE_QUERY_LENGTH`, `workplacePageFromSearch`, `workplaceQueryFromSearch`, `SeAddressSearchState`, `addressSearchString`; everything already there is untouched |
| `corpscout/services/backoffice/app/lib/se-company-address-entity.server.ts` | modified — `SeAddressPublished` renamed `SeAddressPublishedDetail`; new `SeAddressListEntry`, `SeAddressWorkplacePage`, `SeAddressDetailOptions`, `SeAddressListRow`, `SeAddressFoldStateRow`; new SQL `ADDRESS_LIST_SQL`, `ADDRESS_WORKPLACES_SQL`, `ADDRESS_WORKPLACES_COUNT_SQL`, `ADDRESS_SELECTED_SQL`, `ADDRESS_MEMBER_NORMALIZED_SQL`, `ADDRESS_MEMBER_RAW_SQL`, `ADDRESS_DRAFT_RAW_SQL`, `ADDRESS_DRAFT_NORMALIZED_SQL`, `ADDRESS_FOLD_STATE_SQL`; `loadSeAddressDetail` rewritten; the five writes untouched |
| `corpscout/services/backoffice/app/routes/admin-se-company-address.tsx` | modified — the loader reads three search params and passes them on; `EMPTY_DETAIL` becomes `emptyAddressDetail(page, query)` |
| `corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx` | modified — `AddressLine`/`AddressesCard` take a `SeAddressListEntry`, `selectAddress` is deleted (the loader selects), `AddressPanel` takes the loader's `selected`, new `WorkplacesCard`, new exported `listMapPoints`, every link's search built by `addressSearchString` |
| `corpscout/services/backoffice/tests/se-address-fields.test.ts` | modified — one new `describe` for the three search helpers |
| `corpscout/services/backoffice/tests/se-company-address-entity.server.test.ts` | modified — the load block (the eight tests from "assembles published rows…" to "ignores a cleared reviewer_draft row…") is replaced; the SQL-shape test gains the new constants; every write test stays as it is |
| `corpscout/services/backoffice/tests/admin-se-company-address.test.tsx` | modified — fixtures follow the new detail shape (Task 1), then five new render tests for the Workplaces card, the map and the preserved parameters (Task 2) |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md` | modified in Task 3 — a paragraph under "Backoffice (slice 3, 2026-09-07)" |
| `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` | modified in Task 3 — one "Shipped" line at the end of the 2026-09-13 amendment |
| this plan | ticked as the tasks land |

Not touched, deliberately: `app/lib/se-address-decision-form.ts` (the six intents are unchanged), `app/components/admin/se-address-edit-sheet.tsx`, `app/components/detail/address-map.tsx` and `address-map-inner.tsx` (`AddressMapPoint` already carries everything the workplace points need), `app/lib/se-address-tables.ts`, `app/lib/address-quality.server.ts`, `app/lib/address-companies.server.ts`, `tests/se-address-decision-form.test.ts`, `tests/se-address-edit-sheet.test.tsx`, `tests/se-address-map.test.tsx`.

---

### Task 1: The search helpers and the slim loader

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-address-fields.ts` (append after `selectedAddressFromSearch`, line 60)
- Modify: `corpscout/services/backoffice/app/lib/se-company-address-entity.server.ts` (types at lines 151-177, SQL after line 237, `loadSeAddressDetail` at lines 302-381)
- Modify: `corpscout/services/backoffice/app/routes/admin-se-company-address.tsx` (lines 1-42)
- Modify: `corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx` (the type-level follow-through only; the Workplaces card is Task 2)
- Test: `corpscout/services/backoffice/tests/se-address-fields.test.ts`
- Test: `corpscout/services/backoffice/tests/se-company-address-entity.server.test.ts`
- Test: `corpscout/services/backoffice/tests/admin-se-company-address.test.tsx` (fixtures only)

**Interfaces:**
- Consumes: `chQuery<T>(sql: string, params?: Record<string, unknown>): Promise<T[]>` from `~/lib/clickhouse.server`; `isAddressKey(value: string): boolean` and `addressFoldPending(foldedAt: string | null, stamps: readonly string[], hasNormalized: boolean): boolean` from `~/lib/se-address-fields`; `SE_COMPANY_ADDRESS_TABLE` (`"corpscout.se_company_address"`) from `~/lib/se-address-tables`; the existing types `SeAddressRow`, `SeAddressHistoryRow`, `SeAddressNormalizedRow`, `SeAddressRawRow`, `SeAddressRuleRow`, `SeAddressMember`, `SeAddressDraft`.
- Produces, for Task 2 and Task 3:
  - `se-address-fields.ts`: `WORKPLACE_PAGE_SIZE: number` (50), `MAX_WORKPLACE_QUERY_LENGTH: number` (100), `workplacePageFromSearch(params: URLSearchParams): number`, `workplaceQueryFromSearch(params: URLSearchParams): string`, `interface SeAddressSearchState { address: string | null; workplacePage: number; workplaceQuery: string }`, `addressSearchString(state: SeAddressSearchState): string`.
  - `se-company-address-entity.server.ts`: `interface SeAddressListEntry { row: SeAddressRow; refoldPending: boolean; hideRule: SeAddressRuleRow | null }`; `interface SeAddressPublishedDetail { row: SeAddressRow; members: SeAddressMember[]; textSourceReason: "most complete" | "tie-break" | "single source"; hideRule: SeAddressRuleRow | null }` (the former `SeAddressPublished`); `interface SeAddressWorkplacePage { rows: SeAddressListEntry[]; total: number; page: number; pageSize: number; query: string }`; `interface SeAddressDetail { published: SeAddressListEntry[]; selected: SeAddressPublishedDetail | null; workplaces: SeAddressWorkplacePage; drafts: SeAddressDraft[]; history: SeAddressHistoryRow[]; rules: SeAddressRuleRow[]; foldPending: boolean }`; `interface SeAddressDetailOptions { selectedKey: string | null; workplacePage: number; workplaceQuery: string }`; `loadSeAddressDetail(companyId: string, options: SeAddressDetailOptions): Promise<SeAddressDetail | null>`; the exported SQL constants `ADDRESS_LIST_SQL`, `ADDRESS_WORKPLACES_SQL`, `ADDRESS_WORKPLACES_COUNT_SQL`, `ADDRESS_SELECTED_SQL`, `ADDRESS_MEMBER_NORMALIZED_SQL`, `ADDRESS_MEMBER_RAW_SQL`, `ADDRESS_DRAFT_RAW_SQL`, `ADDRESS_DRAFT_NORMALIZED_SQL`, `ADDRESS_FOLD_STATE_SQL`.

- [x] **Step 1: Record the suite's pre-existing failures**

From `corpscout/services/backoffice`:

```bash
npm test 2>&1 | tail -40
```

Write the failing test file names into the task's notes. Every later "no new failures" check compares against this list. Do not fix any of them.

- [x] **Step 2: Write the failing search-helper tests**

In `tests/se-address-fields.test.ts`, extend the import list at the top with the five new names:

```ts
import {
  ADDRESS_KINDS, ADDRESS_SOURCES, REVIEWER_KINDS, addressFoldPending, addressKindLabel, addressSearchString,
  addressSourceLabel, geocodeStatusLabel, isAddressKind, isAddressSource, MAX_WORKPLACE_QUERY_LENGTH,
  selectedAddressFromSearch, validateSeAddressInput, WORKPLACE_PAGE_SIZE, workplacePageFromSearch,
  workplaceQueryFromSearch,
} from "~/lib/se-address-fields";
```

and append this describe block at the end of the file:

```ts
describe("workplace paging search params", () => {
  it("pages fifty rows at a time", () => {
    // Spec 8 (amended 2026-09-13): the Workplaces card is paged server-side,
    // fifty rows a page. The number is a contract between the loader's OFFSET
    // and the card's "<from>-<to> of <total>" footer, so it is pinned here.
    expect(WORKPLACE_PAGE_SIZE).toBe(50);
    expect(MAX_WORKPLACE_QUERY_LENGTH).toBe(100);
  });

  it("reads a 1-based page and calls anything that is not a whole number page 1", () => {
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=3"))).toBe(3);
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=31"))).toBe(31);
    expect(workplacePageFromSearch(new URLSearchParams())).toBe(1);
    for (const raw of ["0", "-2", "2.5", "abc", "", "%20", "1e3", "01x"]) {
      expect(workplacePageFromSearch(new URLSearchParams(`workplaces=${raw}`))).toBe(1);
    }
  });

  it("trims the filter and caps it at a hundred characters", () => {
    // `+` is a space in a query string, so this is the filter box's own posting.
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=+storgatan+"))).toBe("storgatan");
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=Box%20123"))).toBe("Box 123");
    expect(workplaceQueryFromSearch(new URLSearchParams())).toBe("");
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=   "))).toBe("");
    const long = workplaceQueryFromSearch(new URLSearchParams(`workplace_q=${"x".repeat(150)}`));
    expect(long).toHaveLength(100);
  });

  it("builds the tab's whole search string and leaves the defaults out", () => {
    expect(addressSearchString({ address: null, workplacePage: 1, workplaceQuery: "" })).toBe("");
    expect(addressSearchString({ address: KEY, workplacePage: 1, workplaceQuery: "" })).toBe(
      `?address=${KEY}`,
    );
    expect(addressSearchString({ address: null, workplacePage: 4, workplaceQuery: "" })).toBe(
      "?workplaces=4",
    );
    // Everything at once, in a stable order: the selected address, the page, the filter.
    expect(addressSearchString({ address: KEY, workplacePage: 2, workplaceQuery: "Box 1" })).toBe(
      `?address=${KEY}&workplaces=2&workplace_q=Box+1`,
    );
    // What it builds is what the readers read back.
    const round = new URLSearchParams(
      addressSearchString({ address: KEY, workplacePage: 2, workplaceQuery: "Box 1" }).slice(1),
    );
    expect(selectedAddressFromSearch(round)).toBe(KEY);
    expect(workplacePageFromSearch(round)).toBe(2);
    expect(workplaceQueryFromSearch(round)).toBe("Box 1");
  });
});
```

- [x] **Step 3: Run the helper tests and watch them fail**

```bash
npm test -- tests/se-address-fields.test.ts
```

Expected: FAIL — `workplacePageFromSearch is not a function` (and the other four names undefined).

- [x] **Step 4: Add the three helpers to `se-address-fields.ts`**

Append after `selectedAddressFromSearch` (line 60), before `export interface SeAddressInput`:

```ts
/** The Workplaces card's page size (spec 8, amended 2026-09-13): fifty rows a
 * page, counted and cut in ClickHouse, never in the browser. */
export const WORKPLACE_PAGE_SIZE = 50;
/** The filter box is a contains search, not a query language: a hundred
 * characters is longer than any `normalized_address` and caps what a
 * hand-typed URL can push into the query parameter. */
export const MAX_WORKPLACE_QUERY_LENGTH = 100;

/** `?workplaces=<page>`, 1-based. Anything that is not a whole number of at
 * least 1 -- absent, empty, `0`, `-2`, `2.5`, `abc` -- is page 1. */
export function workplacePageFromSearch(params: URLSearchParams): number {
  const raw = (params.get("workplaces") ?? "").trim();
  if (!/^[0-9]+$/.test(raw)) return 1;
  const page = Number(raw);
  return Number.isSafeInteger(page) && page >= 1 ? page : 1;
}

/** `?workplace_q=<text>`: trimmed and capped. The loader hands it to ClickHouse
 * as a query parameter, so it is never SQL. */
export function workplaceQueryFromSearch(params: URLSearchParams): string {
  return (params.get("workplace_q") ?? "").trim().slice(0, MAX_WORKPLACE_QUERY_LENGTH);
}

/** The three parameters the Address tab keeps in its URL. */
export interface SeAddressSearchState {
  /** The `?address=` key, or null for no selection. */
  address: string | null;
  /** 1-based; page 1 is the absent parameter. */
  workplacePage: number;
  /** The workplace filter; `''` is the absent parameter. */
  workplaceQuery: string;
}

/**
 * The tab's whole query string, leading `?` included (`''` when everything is
 * at its default). Every link on the page builds its search here -- selecting
 * an address keeps the workplace page and its filter, paging keeps the selected
 * address -- so there is one place where the parameter names live.
 */
export function addressSearchString(state: SeAddressSearchState): string {
  const params = new URLSearchParams();
  if (state.address !== null && state.address !== "") params.set("address", state.address);
  if (state.workplacePage > 1) params.set("workplaces", String(state.workplacePage));
  if (state.workplaceQuery !== "") params.set("workplace_q", state.workplaceQuery);
  const search = params.toString();
  return search === "" ? "" : `?${search}`;
}
```

- [x] **Step 5: Run the helper tests and watch them pass**

```bash
npm test -- tests/se-address-fields.test.ts
```

Expected: PASS, the whole file.

- [x] **Step 6: Write the failing loader tests**

In `tests/se-company-address-entity.server.test.ts`, three edits.

**(a)** Extend the import from the module under test (lines 21-39) with the nine new SQL constants and the options type — keep every name already there:

```ts
import {
  activateSeAddressDraft,
  ADDRESS_DRAFT_NORMALIZED_SQL,
  ADDRESS_DRAFT_RAW_SQL,
  ADDRESS_FOLD_STATE_SQL,
  ADDRESS_HISTORY_SQL,
  ADDRESS_LIST_SQL,
  ADDRESS_MAIN_SQL,
  ADDRESS_MEMBER_NORMALIZED_SQL,
  ADDRESS_MEMBER_RAW_SQL,
  ADDRESS_NORMALIZED_SQL,
  ADDRESS_RAW_SQL,
  ADDRESS_RULES_SQL,
  ADDRESS_SELECTED_SQL,
  ADDRESS_WORKPLACES_COUNT_SQL,
  ADDRESS_WORKPLACES_SQL,
  discardSeAddressDraft,
  launchSeAddressFold,
  loadSeAddressDetail,
  removeSeAddress,
  resetSeAddress,
  saveSeAddressDraft,
  SeAddressDecisionError,
  type SeAddressDetailOptions,
  type SeAddressNormalizedRow,
  type SeAddressRawRow,
  type SeAddressRow,
  type SeAddressRuleRow,
} from "~/lib/se-company-address-entity.server";
```

**(b)** Replace `answer` (lines 314-321) with the fixtures and the dispatcher below. `MERGED_ROW`, `BOX_ROW`, `HISTORY_ROW`, `BOX_HIDE_RULE`, `NORMALIZED_ROWS`, `RAW_ROWS` and every `normalized()` / `raw()` / `main()` fixture above them stay exactly as they are:

```ts
/** An establishment row: `kinds` is EXACTLY `['workplace']`, so it belongs to
 * the Workplaces card and not to the Addresses card (migration 000403's split).
 * Its member's current normalized version (n5b) is not the one it was folded
 * from (n5), so ClickHouse marks the row re-fold pending. */
const WORKPLACE_KEY = "f".repeat(64);
const WORKPLACE_ROW = main({
  address_key: WORKPLACE_KEY,
  street_name: "Verkstadsgatan",
  house_number: "3",
  postal_code: "11124",
  city: "Stockholm",
  normalized_address: "Verkstadsgatan 3, 111 24 Stockholm",
  kinds: ["workplace"],
  sources: ["ratsit"],
  slots: ["est:9"],
  normalized_ids: ["n5"],
  text_source: "ratsit",
});
const WORKPLACE_NORMALIZED = normalized({
  source: "ratsit",
  slot: "est:9",
  normalized_id: "n5b",
  suggestion_id: "sid9",
  kind: "workplace",
  street_name: "Verkstadsgatan",
  house_number: "3",
  postal_code: "11124",
  city: "Stockholm",
  normalized_address: "Verkstadsgatan 3, 111 24 Stockholm",
  address_key: WORKPLACE_KEY,
});
const WORKPLACE_RAW = raw({
  source: "ratsit",
  slot: "est:9",
  suggestion_id: "sid9",
  kind: "workplace",
  street_address: "Verkstadsgatan 3",
  postal_code: "11124",
  post_town: "Stockholm",
  source_run_id: "run-ratsit",
  extractor_version: "ratsit-address-v2",
});

/** One row of the list / workplace queries: the published columns plus the
 * `refold_pending` UInt8 ClickHouse computes per row. */
function listRow(row: SeAddressRow, refoldPending: 0 | 1): Record<string, unknown> {
  return { ...row, refold_pending: refoldPending };
}

/** The one row `ADDRESS_FOLD_STATE_SQL` returns: the newest fold, the newest
 * non-draft normalized and raw stamps, whether any non-draft normalized row is
 * publishable, and how many normalized rows the company has at all. */
const FOLD_STATE = {
  folded_at: "2026-09-07 09:00:00.000",
  newest_normalized_at: "2026-09-07 10:00:00.000",
  has_publishable: 1,
  newest_suggested_at: "2026-09-06 07:00:00.000",
  normalized_rows: 4,
};
function foldState(over: Record<string, unknown>): Record<string, unknown> {
  return { ...FOLD_STATE, ...over };
}

function answer(sql: string, params?: Record<string, unknown>): unknown[] {
  // The tab's reads, each dispatched by identity: several of them select from
  // the same table, so a substring match would answer the wrong one.
  if (sql === ADDRESS_LIST_SQL) return [listRow(MERGED_ROW, 0), listRow(BOX_ROW, 0)];
  if (sql === ADDRESS_WORKPLACES_SQL) return [listRow(WORKPLACE_ROW, 1)];
  if (sql === ADDRESS_WORKPLACES_COUNT_SQL) return [{ total: 1 }];
  if (sql === ADDRESS_SELECTED_SQL) {
    const row = [MERGED_ROW, BOX_ROW, WORKPLACE_ROW].find(
      (candidate) => candidate.address_key === params?.addressKey,
    );
    return row === undefined ? [] : [row];
  }
  if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) {
    const wanted = new Set((params?.memberSlots as string[]) ?? []);
    return [SCB_NORMALIZED, BV_NORMALIZED, RATSIT_NORMALIZED, WORKPLACE_NORMALIZED].filter((row) =>
      wanted.has(row.slot),
    );
  }
  if (sql === ADDRESS_MEMBER_RAW_SQL) {
    const wanted = new Set((params?.memberSlots as string[]) ?? []);
    return [SCB_RAW, BV_RAW, RATSIT_RAW, WORKPLACE_RAW].filter((row) => wanted.has(row.slot));
  }
  if (sql === ADDRESS_DRAFT_RAW_SQL) return [DRAFT_RAW];
  if (sql === ADDRESS_DRAFT_NORMALIZED_SQL) return [DRAFT_NORMALIZED];
  if (sql === ADDRESS_FOLD_STATE_SQL) return [FOLD_STATE];
  if (sql === ADDRESS_HISTORY_SQL) return [HISTORY_ROW];
  if (sql === ADDRESS_RULES_SQL) return [BOX_HIDE_RULE];
  // The five reviewer writes still read the company whole.
  if (sql === ADDRESS_MAIN_SQL) return [MERGED_ROW, BOX_ROW, WORKPLACE_ROW];
  if (sql === ADDRESS_NORMALIZED_SQL) return NORMALIZED_ROWS;
  if (sql === ADDRESS_RAW_SQL) return RAW_ROWS;
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}

/** The loader's options, with the tab's defaults. */
const DEFAULT_OPTIONS: SeAddressDetailOptions = {
  selectedKey: null,
  workplacePage: 1,
  workplaceQuery: "",
};
function load(over: Partial<SeAddressDetailOptions> = {}) {
  return loadSeAddressDetail(COMPANY, { ...DEFAULT_OPTIONS, ...over });
}

/** Every `chQuery` call that ran this SQL, with the params it was given. */
function paramsOf(sql: string): Record<string, unknown> | undefined {
  return clickhouse.query.mock.calls.find(([text]) => text === sql)?.[1] as
    | Record<string, unknown>
    | undefined;
}
function ranSql(sql: string): boolean {
  return clickhouse.query.mock.calls.some(([text]) => text === sql);
}
```

**(c)** In `beforeEach` (line 331), let the mock see the params:

```ts
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      answer(sql, params),
    );
```

**(d)** Replace the whole load block — the eight tests from `it("assembles published rows, members, the text-source reason, the hide rule and the drafts"` (line 374) through the end of `it("ignores a cleared reviewer_draft row: it is a tombstone, not a draft"` (line 557) — with these eleven:

```ts
  it("pins the new reads to the workplace split, the filter, the page and one row's pairs", () => {
    // The list is everything EXCEPT an active workplace-only row -- the exact
    // predicate migration 000403 gave the serving view.
    expect(ADDRESS_LIST_SQL).toContain("FROM corpscout.se_company_address AS m FINAL");
    expect(ADDRESS_LIST_SQL).toContain("WHERE m.company_id = {companyId:String}");
    expect(ADDRESS_LIST_SQL).toContain("AND NOT (m.active = 1 AND m.kinds = ['workplace'])");
    expect(ADDRESS_LIST_SQL).toContain("AS refold_pending");
    // The re-fold mark is computed in ClickHouse against the company's current
    // normalized versions: pending only when the slot HAS a current version and
    // it is not the one the row was folded from.
    expect(ADDRESS_LIST_SQL).toContain("groupArray(concat(toString(n.source), '\\n', n.slot))");
    expect(ADDRESS_LIST_SQL).toContain("has(current_pairs,");
    expect(ADDRESS_LIST_SQL).toContain("AND NOT has(current_triples,");

    // The workplaces are the other half of the same split, paged and filtered.
    expect(ADDRESS_WORKPLACES_SQL).toContain("AND m.active = 1");
    expect(ADDRESS_WORKPLACES_SQL).toContain("AND m.kinds = ['workplace']");
    expect(ADDRESS_WORKPLACES_SQL).toContain(
      "positionCaseInsensitiveUTF8(m.normalized_address, {workplaceQuery:String}) > 0",
    );
    expect(ADDRESS_WORKPLACES_SQL).toContain("ORDER BY m.normalized_address, m.address_key");
    expect(ADDRESS_WORKPLACES_SQL).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
    // The count carries the same WHERE, and comes back as a NUMBER: a bare
    // count() is UInt64, which ClickHouse quotes as a string in JSONEachRow.
    expect(ADDRESS_WORKPLACES_COUNT_SQL).toContain("SELECT toUInt32(count()) AS total");
    expect(ADDRESS_WORKPLACES_COUNT_SQL).toContain("AND m.kinds = ['workplace']");
    expect(ADDRESS_WORKPLACES_COUNT_SQL).not.toContain("LIMIT");

    // The members of ONE row, by the pairs that row carries.
    for (const sql of [ADDRESS_MEMBER_NORMALIZED_SQL, ADDRESS_MEMBER_RAW_SQL]) {
      expect(sql).toContain(
        "has(arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)}),",
      );
    }
    expect(ADDRESS_SELECTED_SQL).toContain("toString(m.address_key) = {addressKey:String}");
    expect(ADDRESS_SELECTED_SQL).toContain("LIMIT 1");
    // Drafts stand on their own, filtered in SQL.
    expect(ADDRESS_DRAFT_RAW_SQL).toContain("AND s.source = 'reviewer_draft'");
    expect(ADDRESS_DRAFT_NORMALIZED_SQL).toContain("AND n.source = 'reviewer_draft'");
    // Fold state: five scalar aggregates, drafts excluded from the stamps.
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS folded_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS newest_normalized_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS has_publishable");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS newest_suggested_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS normalized_rows");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("n.source != 'reviewer_draft'");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("s.source != 'reviewer_draft'");
  });

  it("returns slim list rows -- no members anywhere but the selected row", async () => {
    const detail = await load();
    expect(detail?.published).toEqual([
      { row: MERGED_ROW, refoldPending: false, hideRule: null },
      { row: BOX_ROW, refoldPending: false, hideRule: BOX_HIDE_RULE },
    ]);
    expect(detail?.published[0]).not.toHaveProperty("members");
    expect(detail?.workplaces).toEqual({
      rows: [{ row: WORKPLACE_ROW, refoldPending: true, hideRule: null }],
      total: 1,
      page: 1,
      pageSize: 50,
      query: "",
    });
    expect(detail?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: DRAFT_NORMALIZED, replacesKey: MERGED_KEY },
    ]);
    expect(detail?.history).toEqual([HISTORY_ROW]);
    expect(detail?.rules).toEqual([BOX_HIDE_RULE]);
    // The whole-company reads are what made the kommun 7.8 MB. The tab's
    // loader must not run any of them any more.
    expect(ranSql(ADDRESS_NORMALIZED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_RAW_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MAIN_SQL)).toBe(false);
    expect(paramsOf(ADDRESS_LIST_SQL)).toEqual({ companyId: COMPANY });
    expect(clickhouse.query.mock.calls.some(([text]) =>
      String(text).includes("se_company_address_precedence"),
    )).toBe(false);
  });

  it("resolves the selected row's members from that row's own (source, slot) pairs", async () => {
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(paramsOf(ADDRESS_SELECTED_SQL)).toEqual({
      companyId: COMPANY,
      addressKey: MERGED_KEY,
    });
    // Two parallel Array(String) parameters, zipped back into pairs in
    // ClickHouse: the merged row's two members and nobody else's.
    const memberParams = { companyId: COMPANY, memberSources: ["scb", "bolagsverket"], memberSlots: ["s1", "b1"] };
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual(memberParams);
    expect(paramsOf(ADDRESS_MEMBER_RAW_SQL)).toEqual(memberParams);
    expect(detail?.selected?.row).toEqual(MERGED_ROW);
    expect(detail?.selected?.members).toEqual([
      {
        source: "scb",
        slot: "s1",
        normalizedId: "n1",
        current: SCB_NORMALIZED,
        raw: SCB_RAW,
        refoldPending: false,
        completeness: 5,
      },
      {
        source: "bolagsverket",
        slot: "b1",
        normalizedId: "n2",
        current: BV_NORMALIZED,
        raw: BV_RAW,
        // The current normalized version is not the one this row was folded from.
        refoldPending: true,
        completeness: 4,
      },
    ]);
    expect(detail?.selected?.textSourceReason).toBe("most complete");
    expect(detail?.selected?.hideRule).toBeNull();
  });

  it("selects a workplace row exactly as it selects a company address", async () => {
    const detail = await load({ selectedKey: WORKPLACE_KEY });
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["ratsit"],
      memberSlots: ["est:9"],
    });
    expect(detail?.selected?.row).toEqual(WORKPLACE_ROW);
    expect(detail?.selected?.members).toEqual([
      {
        source: "ratsit",
        slot: "est:9",
        normalizedId: "n5",
        current: WORKPLACE_NORMALIZED,
        raw: WORKPLACE_RAW,
        refoldPending: true,
        completeness: 4,
      },
    ]);
    expect(detail?.selected?.textSourceReason).toBe("single source");
  });

  it("falls back to the first active company address for no key, a foreign key and a malformed one", async () => {
    // Owner ruling 2026-09-13: the tab keeps today's default. With no
    // `?address=` the panel describes the first active row of the Addresses
    // card -- `ADDRESS_LIST_SQL` sorts `active DESC` first, so that is the
    // first active entry of the list already read -- and its members load
    // exactly as they would for an explicit key. No extra row read: the row is
    // already in hand.
    const none = await load();
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(none?.selected?.row).toEqual(MERGED_ROW);
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "bolagsverket"],
      memberSlots: ["s1", "b1"],
    });
    expect(paramsOf(ADDRESS_MEMBER_RAW_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "bolagsverket"],
      memberSlots: ["s1", "b1"],
    });
    expect(none?.selected?.members).toHaveLength(2);

    // A well-formed key that is not this company's: the row read comes back
    // empty, and the fallback takes over rather than leaving the panel blank.
    clickhouse.query.mockClear();
    const foreign = await load({ selectedKey: UNKNOWN_KEY });
    expect(paramsOf(ADDRESS_SELECTED_SQL)).toEqual({ companyId: COMPANY, addressKey: UNKNOWN_KEY });
    expect(foreign?.selected?.row).toEqual(MERGED_ROW);

    // A malformed key never reaches ClickHouse at all, and falls back too.
    clickhouse.query.mockClear();
    const malformed = await load({ selectedKey: "not-a-key" });
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(malformed?.selected?.row).toEqual(MERGED_ROW);
  });

  it("selects nothing when the company has no active company address", async () => {
    // A workplace-only company (or one whose every company address is
    // withdrawn): there is no first active row to fall back to, so the panel
    // says nothing is published and no member read runs at all.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_LIST_SQL ? [listRow(BOX_ROW, 0)] : answer(sql, params),
    );
    const detail = await load();
    expect(detail?.published).toEqual([{ row: BOX_ROW, refoldPending: false, hideRule: BOX_HIDE_RULE }]);
    expect(detail?.selected).toBeNull();
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MEMBER_NORMALIZED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MEMBER_RAW_SQL)).toBe(false);
  });

  it("binds the workplace page, its offset and its filter, and echoes them back", async () => {
    const detail = await load({ workplacePage: 3, workplaceQuery: "storgatan" });
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "storgatan",
      limit: 50,
      offset: 100,
    });
    expect(paramsOf(ADDRESS_WORKPLACES_COUNT_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "storgatan",
    });
    expect(detail?.workplaces.page).toBe(3);
    expect(detail?.workplaces.pageSize).toBe(50);
    expect(detail?.workplaces.query).toBe("storgatan");
    expect(detail?.workplaces.total).toBe(1);

    // Page 1 is offset 0, and the count is what `total` reports.
    clickhouse.query.mockClear();
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_WORKPLACES_COUNT_SQL ? [{ total: 1502 }] : answer(sql, params),
    );
    const first = await load();
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "",
      limit: 50,
      offset: 0,
    });
    expect(first?.workplaces.total).toBe(1502);
  });

  it("calls the text source a tie-break when another member is just as complete", async () => {
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) return [{ ...SCB_NORMALIZED, unit: "" }, BV_NORMALIZED];
      return answer(sql, params);
    });
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(detail?.selected?.members.map((member) => member.completeness)).toEqual([4, 4]);
    expect(detail?.selected?.textSourceReason).toBe("tie-break");
  });

  it("attributes the published text to the most complete member of the text source", async () => {
    // SCB and Bolagsverket both write slot '' (ratsit writes 'company'), so one
    // source can hold two members; the text came from the fuller of them.
    const twoScb = main({
      ...MERGED_ROW,
      sources: ["scb", "scb"],
      slots: ["", "x"],
      normalized_ids: ["n6", "n7"],
      text_source: "scb",
    });
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_SELECTED_SQL) return [twoScb];
      if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) {
        return [
          // The first member is the thinner one: picking it would read as a tie.
          normalized({ slot: "", normalized_id: "n6", postal_code: "11122", city: "Stockholm" }),
          normalized({ ...SCB_NORMALIZED, slot: "x", normalized_id: "n7" }),
        ];
      }
      if (sql === ADDRESS_MEMBER_RAW_SQL) return [];
      return answer(sql, params);
    });
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "scb"],
      memberSlots: ["", "x"],
    });
    expect(detail?.selected?.members.map((member) => member.completeness)).toEqual([2, 5]);
    expect(detail?.selected?.textSourceReason).toBe("most complete");
  });

  it("reads fold-pending from the fold-state row, with the drafts already excluded", async () => {
    // Newest non-draft normalized stamp (10:00) is newer than the fold (09:00).
    expect((await load())?.foldPending).toBe(true);

    const state = (over: Record<string, unknown>) => async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_FOLD_STATE_SQL ? [foldState(over)] : answer(sql, params);

    // Every non-draft stamp older than the fold: settled. The draft's own
    // 21:00 normalized_at is not in the fold state at all (the SQL excludes
    // `reviewer_draft`), so it cannot raise the flag.
    clickhouse.query.mockImplementation(
      state({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "2026-09-06 07:00:00.000" }),
    );
    expect((await load())?.foldPending).toBe(false);

    // A rule newer than the fold counts, released or not: the release is not
    // applied until the fold runs.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_FOLD_STATE_SQL) {
        return [foldState({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "" })];
      }
      if (sql === ADDRESS_RULES_SQL) {
        return [{ ...BOX_HIDE_RULE, removed: 1, decided_at: "2026-09-07 12:00:00.000" }];
      }
      return answer(sql, params);
    });
    expect((await load())?.foldPending).toBe(true);

    // A reviewer raw row the normalize step has not seen yet -- what an
    // Activate or a Remove tombstone leaves -- speaks through its own stamp.
    clickhouse.query.mockImplementation(
      state({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "2026-09-07 20:33:55.123" }),
    );
    expect((await load())?.foldPending).toBe(true);

    // Nothing folded yet, but a publishable normalized row exists.
    clickhouse.query.mockImplementation(
      state({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 1 }),
    );
    expect((await load())?.foldPending).toBe(true);

    // Nothing folded and nothing publishable (every row parses `no_address`):
    // no fold is owed.
    clickhouse.query.mockImplementation(
      state({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0 }),
    );
    expect((await load())?.foldPending).toBe(false);
  });

  it("returns null only when there is no list row, no workplace, no normalized row and no draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_WORKPLACES_COUNT_SQL
        ? [{ total: 0 }]
        : sql === ADDRESS_FOLD_STATE_SQL
          ? [foldState({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0, normalized_rows: 0 })]
          : [],
    );
    expect(await load()).toBeNull();

    // A typed draft alone is enough to open the tab.
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_DRAFT_RAW_SQL) return [DRAFT_RAW];
      if (sql === ADDRESS_WORKPLACES_COUNT_SQL) return [{ total: 0 }];
      if (sql === ADDRESS_FOLD_STATE_SQL) {
        return [foldState({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0, normalized_rows: 0 })];
      }
      return [];
    });
    const draftOnly = await load();
    expect(draftOnly?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: null, replacesKey: MERGED_KEY },
    ]);
    expect(draftOnly?.foldPending).toBe(false);

    // A company whose only rows are workplaces has an empty Addresses card and
    // is still very much a company with addresses.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_LIST_SQL ? [] : answer(sql, params),
    );
    const workplacesOnly = await load();
    expect(workplacesOnly?.published).toEqual([]);
    expect(workplacesOnly?.workplaces.total).toBe(1);
  });

  it("ignores a cleared reviewer_draft row: it is a tombstone, not a draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_DRAFT_RAW_SQL) {
        return [{ ...DRAFT_RAW, care_of: "", street_address: "", postal_code: "", post_town: "", note: "discarded" }];
      }
      return answer(sql, params);
    });
    expect((await load())?.drafts).toEqual([]);
  });
```

- [x] **Step 7: Run the loader tests and watch them fail**

```bash
npm test -- tests/se-company-address-entity.server.test.ts
```

Expected: FAIL at import time — `ADDRESS_LIST_SQL` and the eight other constants are not exported yet (vitest reports `SyntaxError: The requested module does not provide an export named 'ADDRESS_LIST_SQL'`, or `undefined` in the SQL-shape test).

- [x] **Step 8: Give `se-company-address-entity.server.ts` its new types and its new SQL**

**(a)** Replace the `SeAddressPublished` interface (lines 151-158) and the `SeAddressDetail` interface (lines 170-177) with these five. `SeAddressComponents`, `SeAddressRow`, `SeAddressHistoryRow`, `SeAddressNormalizedRow`, `SeAddressRawRow`, `SeAddressRuleRow`, `SeAddressMember` and `SeAddressDraft` above them do not change:

```ts
/**
 * One row of a list -- the Addresses card or the Workplaces card -- WITHOUT its
 * members (spec 8, amended 2026-09-13, rule 1). A kommun publishes 1,503 of
 * these; shipping each one's normalized and raw rows is what made the page 7.8
 * MB. The panel asks for the selected row's members on its own.
 */
export interface SeAddressListEntry {
  row: SeAddressRow;
  /** Some member's slot has a current normalized version that is not the one
   * this row was folded from -- computed in ClickHouse, per row. */
  refoldPending: boolean;
  /** The hide rule in force for this key, `null` when there is none. */
  hideRule: SeAddressRuleRow | null;
}

/** The `?address=<key>` row with everything the right-hand panel shows. */
export interface SeAddressPublishedDetail {
  row: SeAddressRow;
  members: SeAddressMember[];
  /** Why the published text came from `row.text_source` (spec 5.2's sort). */
  textSourceReason: "most complete" | "tie-break" | "single source";
  /** The hide rule in force for this key, `null` when there is none. */
  hideRule: SeAddressRuleRow | null;
}

/** One page of the company's workplace-only addresses (spec 8, rule 2). */
export interface SeAddressWorkplacePage {
  rows: SeAddressListEntry[];
  /** Matching rows in the whole company, not on this page. */
  total: number;
  /** 1-based, as asked for -- echoed back so the card can build its links. */
  page: number;
  pageSize: number;
  /** The filter in force, `''` when there is none. */
  query: string;
}

/** What the tab's URL asks the loader for. */
export interface SeAddressDetailOptions {
  /** The `?address=` key; a malformed one is treated as no selection. */
  selectedKey: string | null;
  /** `?workplaces=`, 1-based. */
  workplacePage: number;
  /** `?workplace_q=`, already trimmed and capped. */
  workplaceQuery: string;
}

export interface SeAddressDetail {
  /** The company's own addresses: every row except an ACTIVE workplace-only one. */
  published: SeAddressListEntry[];
  /** The row the panel describes, with its members: the `?address=` row when
   * the key names one of this company's, else the FIRST ACTIVE row of
   * `published` (owner ruling 2026-09-13 -- the tab keeps the default it has
   * always had). Null only when the company has no active company address at
   * all: a workplace-only company, or one whose every address is withdrawn. */
  selected: SeAddressPublishedDetail | null;
  workplaces: SeAddressWorkplacePage;
  drafts: SeAddressDraft[];
  history: SeAddressHistoryRow[];
  /** Every current rule version of this company, released ones included. */
  rules: SeAddressRuleRow[];
  foldPending: boolean;
}

/** A list row as ClickHouse returns it: the published columns plus the flag. */
interface SeAddressListRow extends SeAddressRow {
  refold_pending: number;
}

/** The one row `ADDRESS_FOLD_STATE_SQL` returns. Stamps are `''` when the
 * aggregate had no row to take a maximum of. */
interface SeAddressFoldStateRow {
  folded_at: string;
  newest_normalized_at: string;
  has_publishable: number;
  newest_suggested_at: string;
  normalized_rows: number;
}
```

**(b)** Pull the normalized and raw column lists out of their queries so the member reads can share them, keeping both existing SQL texts byte-identical. Replace `ADDRESS_NORMALIZED_SQL` (lines 210-218) and `ADDRESS_RAW_SQL` (lines 220-230) with:

```ts
const NORMALIZED_COLUMNS_SQL = `  n.company_id AS company_id, toString(n.source) AS source, n.slot AS slot, toString(n.normalized_id) AS normalized_id,
  toString(n.suggestion_id) AS suggestion_id, toString(n.suggested_at) AS suggested_at, toString(n.kind) AS kind,
${COMPONENTS_SQL("n")},
  n.normalized_address AS normalized_address, toString(n.address_key) AS address_key, toString(n.parse_status) AS parse_status,
  n.parse_notes AS parse_notes, toString(n.normalizer_version) AS normalizer_version, toString(n.normalized_at) AS normalized_at`;

export const ADDRESS_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
ORDER BY n.source, n.slot`;

const RAW_COLUMNS_SQL = `  s.company_id AS company_id, toString(s.source) AS source, s.slot AS slot, toString(s.suggestion_id) AS suggestion_id,
  s.source_record_uid AS source_record_uid, toString(s.observed_at) AS observed_at, toString(s.kind) AS kind,
  ifNull(s.raw_address, '') AS raw_address, ifNull(s.care_of, '') AS care_of, ifNull(s.street_address, '') AS street_address,
  ifNull(s.postal_code, '') AS postal_code, ifNull(s.post_town, '') AS post_town, ifNull(s.county, '') AS county,
  ifNull(s.country_code, '') AS country_code, ifNull(s.decided_by, '') AS decided_by, ifNull(s.note, '') AS note,
  ifNull(toString(s.replaces_key), '') AS replaces_key, toString(s.suggested_at) AS suggested_at,
  s.source_run_id AS source_run_id, toString(s.extractor_version) AS extractor_version`;

export const ADDRESS_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source, s.slot`;
```

**(c)** Append the tab's new reads after `ADDRESS_RULES_SQL` (after line 237), before `const DRAFT_SOURCE = "reviewer_draft";`:

```ts
/**
 * The company's CURRENT normalized versions as two arrays of join keys: one of
 * `source \n slot`, one of `source \n slot \n normalized_id`. A published row is
 * re-fold pending when one of its members' slots HAS a current version (the
 * first array) that is not the version the row was folded from (the second) --
 * exactly what the detail used to decide in TypeScript after reading every
 * normalized row of the company.
 *
 * Concatenated keys rather than tuples: `concat` over a `LowCardinality(String)`
 * column yields a plain `String`, so `has` never has to reconcile a
 * LowCardinality element type with a String one. No source (a catalogue value)
 * and no slot (`''`, `company`, `est:<id>`, `r<17 digits>`) holds a newline, so
 * the keys cannot collide.
 */
const CURRENT_PAIRS_SQL = `  (SELECT groupArray(concat(toString(n.source), '\\n', n.slot))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS current_pairs`;

const CURRENT_TRIPLES_SQL = `  (SELECT groupArray(concat(toString(n.source), '\\n', n.slot, '\\n', toString(n.normalized_id)))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS current_triples`;

/** `sources`, `slots` and `normalized_ids` are index-parallel, so the flag is
 * an `arrayExists` over the row's own member indexes. An index past the end of
 * a shorter array yields that type's default, which matches no key. */
const REFOLD_PENDING_SQL = (a: string) => `  toUInt8(arrayExists(
    i -> has(current_pairs, concat(toString(${a}.sources[i]), '\\n', ${a}.slots[i]))
      AND NOT has(current_triples, concat(toString(${a}.sources[i]), '\\n', ${a}.slots[i], '\\n', toString(${a}.normalized_ids[i]))),
    range(1, length(${a}.sources) + 1))) AS refold_pending`;

/**
 * The Addresses card's rows: everything this company has published EXCEPT an
 * active row whose `kinds` are exactly `['workplace']` -- the split migration
 * 000403 gave the serving view (`a.kinds = ['workplace']`). A merged row that
 * carries `workplace` beside another kind is the company's address and stays.
 * A withdrawn or hidden workplace row stays too, in the collapsed group, so a
 * reviewer can still see what a Remove did. No members: the panel loads those
 * for the selected row alone. `active DESC` first, so the first entry of this
 * list is also the row the panel falls back to with no `?address=`.
 */
export const ADDRESS_LIST_SQL = `WITH
${CURRENT_PAIRS_SQL},
${CURRENT_TRIPLES_SQL}
SELECT
${MAIN_COLUMNS_SQL("m")},
${REFOLD_PENDING_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String}
  AND NOT (m.active = 1 AND m.kinds = ['workplace'])
ORDER BY m.active DESC, m.inactive_reason, m.normalized_address`;

/** The other half of the split, filtered. `positionCaseInsensitiveUTF8` takes
 * its needle as a LITERAL, so `%` and `_` are ordinary characters and there is
 * nothing to escape -- an `ilike` would have to escape both before binding. */
const WORKPLACE_WHERE_SQL = `WHERE m.company_id = {companyId:String}
  AND m.active = 1
  AND m.kinds = ['workplace']
  AND ({workplaceQuery:String} = '' OR positionCaseInsensitiveUTF8(m.normalized_address, {workplaceQuery:String}) > 0)`;

export const ADDRESS_WORKPLACES_SQL = `WITH
${CURRENT_PAIRS_SQL},
${CURRENT_TRIPLES_SQL}
SELECT
${MAIN_COLUMNS_SQL("m")},
${REFOLD_PENDING_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
${WORKPLACE_WHERE_SQL}
ORDER BY m.normalized_address, m.address_key
LIMIT {limit:UInt32} OFFSET {offset:UInt32}`;

/** `toUInt32`, not a bare `count()`: ClickHouse quotes 64-bit integers in
 * JSONEachRow, so an unwrapped count would reach the page as the string
 * "1502" and every arithmetic on it would be string arithmetic. */
export const ADDRESS_WORKPLACES_COUNT_SQL = `SELECT toUInt32(count()) AS total
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
${WORKPLACE_WHERE_SQL}`;

/** The selected row itself, whichever card it belongs to. `toString` on the key
 * rather than a FixedString comparison: `company_id` has already narrowed the
 * read to this company's few rows, so nothing is lost by not matching the key
 * against the index. */
export const ADDRESS_SELECTED_SQL = `SELECT
${MAIN_COLUMNS_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String} AND toString(m.address_key) = {addressKey:String}
LIMIT 1`;

/** The selected row's members, by the `(source, slot)` pairs the row carries:
 * two parallel `Array(String)` parameters zipped back into pairs in ClickHouse.
 * `CAST(... AS String)` drops the column's LowCardinality wrapper so both sides
 * of `has` are the same tuple type. */
const MEMBER_PAIRS_PREDICATE_SQL = (a: string) =>
  `  AND has(arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)}), (CAST(${a}.source AS String), ${a}.slot))`;

export const ADDRESS_MEMBER_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
${MEMBER_PAIRS_PREDICATE_SQL("n")}
ORDER BY n.source, n.slot`;

export const ADDRESS_MEMBER_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
${MEMBER_PAIRS_PREDICATE_SQL("s")}
ORDER BY s.source, s.slot`;

/** The reviewer's drafts, filtered in SQL rather than in the page: a company
 * has at most a handful of them, however many addresses it publishes. */
export const ADDRESS_DRAFT_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String} AND s.source = 'reviewer_draft'
ORDER BY s.slot`;

export const ADDRESS_DRAFT_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String} AND n.source = 'reviewer_draft'
ORDER BY n.slot`;

/**
 * Everything "Fold pending" needs, in one row of five scalar aggregates, so the
 * page never has to read a company's normalized and raw rows to decide it: the
 * newest fold, the newest non-draft normalized stamp, whether any non-draft
 * normalized row is publishable (spec 5.5), the newest non-draft raw stamp (an
 * activated reviewer address or a Remove tombstone the normalize step has not
 * seen yet), and how many normalized rows the company has at all -- which is
 * what tells an empty tab from a company that exists at no layer.
 *
 * Drafts are excluded from the stamps because they are never folded (spec 5.5),
 * and each `max` is guarded by its own `count()`: a maximum over no rows is the
 * type's zero value, not NULL, and `1970-01-01` would read as a real stamp.
 */
export const ADDRESS_FOLD_STATE_SQL = `SELECT
  (SELECT if(count() = 0, '', toString(max(m.folded_at)))
   FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
   WHERE m.company_id = {companyId:String}) AS folded_at,
  (SELECT if(count() = 0, '', toString(max(n.normalized_at)))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String} AND n.source != 'reviewer_draft') AS newest_normalized_at,
  (SELECT toUInt8(countIf(n.parse_status IN ('ok', 'partial', 'foreign')) > 0)
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String} AND n.source != 'reviewer_draft') AS has_publishable,
  (SELECT if(count() = 0, '', toString(max(s.suggested_at)))
   FROM corpscout.se_company_address_suggestion AS s FINAL
   WHERE s.company_id = {companyId:String} AND s.source != 'reviewer_draft') AS newest_suggested_at,
  (SELECT toUInt32(count())
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS normalized_rows`;
```

`PUBLISHABLE_PARSE_STATUS` (line 257) is now unused by the loader, but the literal list lives in `ADDRESS_FOLD_STATE_SQL` above; delete the constant so nothing claims two homes:

```ts
// Deleted -- the fold state answers `has_publishable` in ClickHouse now:
// const PUBLISHABLE_PARSE_STATUS = new Set(["ok", "partial", "foreign"]);
```

- [x] **Step 9: Rewrite `loadSeAddressDetail`**

Replace lines 302-381 — the doc comment and the whole function — with these four pieces. `slotKey`, `completenessOf`, `activeHideRule` and `textSourceReason` above them stay as they are and are all used:

```ts
const EMPTY_FOLD_STATE: SeAddressFoldStateRow = {
  folded_at: "",
  newest_normalized_at: "",
  has_publishable: 0,
  newest_suggested_at: "",
  normalized_rows: 0,
};

/** A list row as the page wants it: the published columns, the ClickHouse flag
 * as a boolean, and the rule in force for its key. */
function listEntry(
  queryRow: SeAddressListRow,
  rules: readonly SeAddressRuleRow[],
): SeAddressListEntry {
  const { refold_pending, ...row } = queryRow;
  return {
    row,
    refoldPending: refold_pending === 1,
    hideRule: activeHideRule(rules, row.address_key),
  };
}

/** One published row's `(source, slot)` pairs, as the two parallel array
 * parameters the member reads bind. */
function memberPairParams(
  companyId: string,
  row: SeAddressRow,
): { companyId: string; memberSources: string[]; memberSlots: string[] } {
  return {
    companyId,
    memberSources: [...row.sources],
    memberSlots: row.sources.map((_, index) => row.slots[index] ?? ""),
  };
}

/** The selected address's panel: two reads, both bound to that row's own pairs,
 * then the members assembled through `normalized_ids` exactly as before. */
async function loadSelectedDetail(
  companyId: string,
  row: SeAddressRow,
  rules: readonly SeAddressRuleRow[],
): Promise<SeAddressPublishedDetail> {
  const params = memberPairParams(companyId, row);
  const [normalizedRows, rawRows] = await Promise.all([
    chQuery<SeAddressNormalizedRow>(ADDRESS_MEMBER_NORMALIZED_SQL, params),
    chQuery<SeAddressRawRow>(ADDRESS_MEMBER_RAW_SQL, params),
  ]);
  const normalizedBySlot = new Map(normalizedRows.map((entry) => [slotKey(entry.source, entry.slot), entry]));
  const rawBySlot = new Map(rawRows.map((entry) => [slotKey(entry.source, entry.slot), entry]));
  const members = row.sources.map((source, index) => {
    const slot = row.slots[index] ?? "";
    const normalizedId = row.normalized_ids[index] ?? "";
    const current = normalizedBySlot.get(slotKey(source, slot)) ?? null;
    return {
      source,
      slot,
      normalizedId,
      current,
      raw: rawBySlot.get(slotKey(source, slot)) ?? null,
      refoldPending: current !== null && current.normalized_id !== normalizedId,
      completeness: completenessOf(current),
    };
  });
  return {
    row,
    members,
    textSourceReason: textSourceReason(row, members),
    hideRule: activeHideRule(rules, row.address_key),
  };
}

/**
 * The tab in two round trips (spec 8, amended 2026-09-13). The first runs nine
 * small reads in parallel: the Addresses list without members, one page of the
 * workplaces with its count, history, rules, the drafts, the fold state, and --
 * only when `?address=` carried a well-formed key -- that one row. The second
 * resolves the selected row's members from its own `(source, slot)` pairs: the
 * `?address=` row when it is this company's, else the first active row of the
 * list, which is already in hand and needs no read of its own (owner ruling
 * 2026-09-13: the panel keeps the default selection it has always had).
 *
 * Null when the company has no address at any layer -- no list row, no
 * workplace, no draft and no normalized row -- which the route turns into the
 * workspace's empty state.
 */
export async function loadSeAddressDetail(
  companyId: string,
  options: SeAddressDetailOptions,
): Promise<SeAddressDetail | null> {
  const { workplacePage, workplaceQuery } = options;
  // A hand-typed key never reaches ClickHouse: the route already filters one,
  // and this is the store's own guard.
  const selectedKey =
    options.selectedKey !== null && isAddressKey(options.selectedKey) ? options.selectedKey : null;
  const offset = (workplacePage - 1) * WORKPLACE_PAGE_SIZE;
  const [
    listRows,
    workplaceRows,
    workplaceCount,
    history,
    rules,
    draftRawRows,
    draftNormalizedRows,
    foldStateRows,
    selectedRows,
  ] = await Promise.all([
    chQuery<SeAddressListRow>(ADDRESS_LIST_SQL, { companyId }),
    chQuery<SeAddressListRow>(ADDRESS_WORKPLACES_SQL, {
      companyId,
      workplaceQuery,
      limit: WORKPLACE_PAGE_SIZE,
      offset,
    }),
    chQuery<{ total: number }>(ADDRESS_WORKPLACES_COUNT_SQL, { companyId, workplaceQuery }),
    chQuery<SeAddressHistoryRow>(ADDRESS_HISTORY_SQL, { companyId }),
    chQuery<SeAddressRuleRow>(ADDRESS_RULES_SQL, { companyId }),
    chQuery<SeAddressRawRow>(ADDRESS_DRAFT_RAW_SQL, { companyId }),
    chQuery<SeAddressNormalizedRow>(ADDRESS_DRAFT_NORMALIZED_SQL, { companyId }),
    chQuery<SeAddressFoldStateRow>(ADDRESS_FOLD_STATE_SQL, { companyId }),
    selectedKey === null
      ? Promise.resolve([] as SeAddressRow[])
      : chQuery<SeAddressRow>(ADDRESS_SELECTED_SQL, { companyId, addressKey: selectedKey }),
  ]);
  const published = listRows.map((row) => listEntry(row, rules));
  const workplaces: SeAddressWorkplacePage = {
    rows: workplaceRows.map((row) => listEntry(row, rules)),
    total: workplaceCount[0]?.total ?? 0,
    page: workplacePage,
    pageSize: WORKPLACE_PAGE_SIZE,
    query: workplaceQuery,
  };
  const draftNormalizedBySlot = new Map(draftNormalizedRows.map((row) => [row.slot, row]));
  const drafts = draftRawRows
    .filter((row) => DRAFT_TEXT_FIELDS.some((field) => row[field] !== ""))
    .map((raw) => ({
      slot: raw.slot,
      raw,
      normalized: draftNormalizedBySlot.get(raw.slot) ?? null,
      replacesKey: raw.replaces_key,
    }));
  const state = foldStateRows[0] ?? EMPTY_FOLD_STATE;
  if (
    published.length === 0 &&
    workplaces.total === 0 &&
    drafts.length === 0 &&
    state.normalized_rows === 0
  ) {
    return null;
  }
  // The `?address=` row, else the first active company address (the list is
  // sorted `active DESC` first). A key that is absent, malformed or not this
  // company's lands on the same fallback, and only a company with no active
  // company address at all -- workplace-only, or every address withdrawn --
  // leaves the panel with nothing to describe.
  const selectedRow =
    selectedRows[0] ?? published.find((entry) => entry.row.active === 1)?.row ?? null;
  return {
    published,
    selected: selectedRow === null ? null : await loadSelectedDetail(companyId, selectedRow, rules),
    workplaces,
    drafts,
    history,
    rules,
    foldPending: addressFoldPending(
      state.folded_at === "" ? null : state.folded_at,
      // A released rule counts too: the release is not applied until the fold.
      [state.newest_normalized_at, state.newest_suggested_at, ...rules.map((rule) => rule.decided_at)].filter(
        (stamp) => stamp !== "",
      ),
      state.has_publishable === 1,
    ),
  };
}
```

Finally, extend the module's import from `~/lib/se-address-fields` (line 30) with the page size:

```ts
import { addressFoldPending, isAddressKey, WORKPLACE_PAGE_SIZE } from "~/lib/se-address-fields";
```

- [x] **Step 10: Let the route read the three parameters**

In `app/routes/admin-se-company-address.tsx`, replace the `selectedAddressFromSearch` import (line 4), the `EMPTY_DETAIL` constant with its comment (lines 20-31) and the `loader` (lines 37-41):

```ts
import {
  selectedAddressFromSearch,
  WORKPLACE_PAGE_SIZE,
  workplacePageFromSearch,
  workplaceQueryFromSearch,
} from "~/lib/se-address-fields";
```

```ts
/** A company no source has suggested an address for is a normal pipeline
 * state, not a broken link -- and the reviewer must still be able to type one.
 * So the tab opens on an empty detail rather than a 404: the workspace says
 * nothing is published and keeps Add address, Correct's counterpart, in reach.
 * The company layout already 404s a company that does not exist at all. The
 * workplace page echoes what was asked for, so the card's links stay honest
 * even on a company that has none. */
function emptyAddressDetail(workplacePage: number, workplaceQuery: string): SeAddressDetail {
  return {
    published: [],
    selected: null,
    workplaces: {
      rows: [],
      total: 0,
      page: workplacePage,
      pageSize: WORKPLACE_PAGE_SIZE,
      query: workplaceQuery,
    },
    drafts: [],
    history: [],
    rules: [],
    foldPending: false,
  };
}
```

```ts
export async function loader({ request, params }: Route.LoaderArgs) {
  // Three parameters, all read here and all passed on: the loader does the
  // selection, the filtering, the offset and the count in ClickHouse.
  const search = new URL(request.url).searchParams;
  const selectedKey = selectedAddressFromSearch(search);
  const workplacePage = workplacePageFromSearch(search);
  const workplaceQuery = workplaceQueryFromSearch(search);
  const detail = await loadSeAddressDetail(params.companyId, {
    selectedKey,
    workplacePage,
    workplaceQuery,
  });
  return { detail: detail ?? emptyAddressDetail(workplacePage, workplaceQuery), selectedKey };
}
```

- [x] **Step 11: Follow the new shape through the workspace component**

`app/components/admin/se-address-workspace.tsx`, six edits. The Workplaces card, the map wiring and the parameter-preserving links are Task 2; this step only makes the component compile against the split loader and keeps the panel's copy honest.

**(a)** The type import (lines 50-58):

```ts
import type {
  SeAddressComponents,
  SeAddressDetail,
  SeAddressDraft,
  SeAddressListEntry,
  SeAddressMember,
  SeAddressPublishedDetail,
  SeAddressRawRow,
  SeAddressRow,
} from "~/lib/se-company-address-entity.server";
```

**(b)** Delete `selectAddress` (lines 162-173) entirely — the loader makes the same choice now, in the same order (the `?address=` row, else the first active row), and it is the only place that can make it: only the selected row's members are read.

**(c)** `AddressLine` (line 338) and `AddressesCard` (line 394) take list entries. Only the types change here:

```tsx
function AddressLine({
  entry,
  selectedKey,
  busy,
  onCorrect,
}: {
  entry: SeAddressListEntry;
  selectedKey: string | null;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
```

```tsx
function AddressesCard({
  detail,
  selectedKey,
  busy,
  onCorrect,
}: {
  detail: SeAddressDetail;
  selectedKey: string | null;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
```

**(d)** In `DraftsCard`, the row a Correct replaces may be a workplace, which is no longer in `published` — look in both lists (line 515):

```tsx
            const replaced =
              draft.replacesKey === ""
                ? null
                : ([...detail.published, ...detail.workplaces.rows].find(
                    (entry) => entry.row.address_key === draft.replacesKey,
                  ) ?? null);
```

**(e)** `AddressPanel` (line 818) takes the loader's selected detail. Only the `entry` type changes — the loader now falls back to the first active row, so `entry === null` still means what it always meant (the company has no active company address) and the header's two-branch copy stays exactly as it is:

```tsx
function AddressPanel({
  entry,
  busy,
  onAdd,
  onDecide,
}: {
  entry: SeAddressPublishedDetail | null;
  busy: boolean;
  onAdd: () => void;
  onDecide: (pending: PendingAddressDecision) => void;
}) {
```

Leave the `CardHeader` (lines 839-845) untouched:

```tsx
      <CardHeader>
        <CardTitle>{entry === null ? "No address selected" : "Address"}</CardTitle>
        <CardDescription>
          {entry === null
            ? "Nothing published for this company yet."
            : "Where the published line came from, and what the reviewer decided about it."}
        </CardDescription>
```

**(f)** In `SeAddressWorkspace`, the selection comes from the loader (replace line 1052, `const selected = selectAddress(detail, selectedKey);`):

```tsx
  // The loader made the selection: the `?address=` row when the key names one
  // of this company's, else the first active row -- the same default the
  // deleted `selectAddress` applied -- and only that row carries members.
  const selected = detail.selected;
```

The panel call (line 1092) needs no change at all: `entry={selected}` already reads the right thing.

The `AddressesCard` call keeps `selectedKey={selected?.row.address_key ?? null}` — the effective selection, which is now exactly what the loader resolved, default included, so the list marks the row the panel describes just as it did before. The route still passes its own `selectedKey` prop to the workspace; it stays in the signature and is unused by the render, so prefix nothing and change nothing there: Task 2 uses it for the links.

- [x] **Step 12: Make the route/component test fixtures the new shape**

In `tests/admin-se-company-address.test.tsx`:

**(a)** the type import (lines 23-29):

```ts
import type {
  SeAddressDetail,
  SeAddressDraft,
  SeAddressListEntry,
  SeAddressPublishedDetail,
  SeAddressRawRow,
  SeAddressRow,
  SeAddressWorkplacePage,
} from "~/lib/se-company-address-entity.server";
```

**(b)** replace `published`, `secondPublished`, `detail` and `EMPTY_DETAIL` (lines 88-143) with:

```ts
/** The company's one published address, as the Addresses card gets it. */
const listEntry: SeAddressListEntry = { row, refoldPending: false, hideRule: null };

/** The same address as the panel gets it, with its one member. */
const selectedDetail: SeAddressPublishedDetail = {
  row,
  members: [
    {
      source: "bolagsverket",
      slot: "",
      normalizedId: "norm-1",
      current: null,
      raw: rawRow,
      refoldPending: false,
      completeness: 6,
    },
  ],
  textSourceReason: "single source",
  hideRule: null,
};

/** A second active address of the same company, geocoded no finer than its
 * postcode -- two rows for the list's one map to carry. */
const SECOND_KEY = "b".repeat(64);
const secondListEntry: SeAddressListEntry = {
  ...listEntry,
  row: {
    ...row,
    address_key: SECOND_KEY,
    street_name: "Kungsgatan",
    house_number: "12",
    postal_code: "11143",
    normalized_address: "kungsgatan 12|11143|stockholm|se",
    latitude: 59.34,
    longitude: 18.07,
    geocode_precision: "postcode",
  },
};

/** An establishment: `kinds` is exactly `['workplace']`, so it lives in the
 * Workplaces card and never in the Addresses card. */
const WORKPLACE_KEY = "c".repeat(64);
const workplaceEntry: SeAddressListEntry = {
  row: {
    ...row,
    address_key: WORKPLACE_KEY,
    street_name: "Verkstadsgatan",
    house_number: "3",
    postal_code: "11124",
    normalized_address: "verkstadsgatan 3|11124|stockholm|se",
    kinds: ["workplace"],
    sources: ["ratsit"],
    slots: ["est:9"],
    normalized_ids: ["norm-9"],
    text_source: "ratsit",
    latitude: 59.35,
    longitude: 18.08,
  },
  refoldPending: false,
  hideRule: null,
};

/** No workplaces at all: the card is absent (Task 2). Typed, not `as const`:
 * `as const` would make `rows` a `readonly []`, which no `SeAddressDetail`
 * accepts. */
const NO_WORKPLACES: SeAddressWorkplacePage = {
  rows: [],
  total: 0,
  page: 1,
  pageSize: 50,
  query: "",
};

const detail: SeAddressDetail = {
  published: [listEntry],
  selected: selectedDetail,
  workplaces: { ...NO_WORKPLACES },
  drafts: [],
  history: [],
  rules: [],
  foldPending: false,
};

/** What the route hands the workspace for a company no source has an address
 * for (and what `loadSeAddressDetail` returning null becomes). */
const EMPTY_DETAIL: SeAddressDetail = {
  published: [],
  selected: null,
  workplaces: { ...NO_WORKPLACES },
  drafts: [],
  history: [],
  rules: [],
  foldPending: false,
};
```

**(c)** in the render test "maps the active addresses once for the whole list…", the detail now carries two list rows and the panel's selection:

```tsx
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [listEntry, secondListEntry] }}
        selectedKey={KEY}
        result={null}
      />,
    );
```

**(d)** in the route's loader tests, assert the options the route builds. Replace the body of `it("loads the detail, and opens on an empty one when the company has no rows"…)`'s first call and the `?address=` test with:

```ts
  it("loads the detail, and opens on an empty one when the company has no rows", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address`),
      params: { companyId: COMPANY },
    } as never);
    expect(response).toEqual({ detail, selectedKey: null });
    // The tab's three parameters, at their defaults.
    expect(server.loadSeAddressDetail).toHaveBeenCalledWith(COMPANY, {
      selectedKey: null,
      workplacePage: 1,
      workplaceQuery: "",
    });

    // No 404: Add address must stay reachable for a company nothing has
    // suggested an address for. The company layout 404s an unknown company.
    server.loadSeAddressDetail.mockResolvedValueOnce(null);
    const missing = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address?workplaces=3&workplace_q=box`),
      params: { companyId: COMPANY },
    } as never);
    expect(missing).toEqual({
      detail: {
        ...EMPTY_DETAIL,
        workplaces: { rows: [], total: 0, page: 3, pageSize: 50, query: "box" },
      },
      selectedKey: null,
    });
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={missing.detail}
        selectedKey={missing.selectedKey}
        result={null}
      />,
    );
    expect(html).toContain("No addresses published yet");
    expect(html).toContain("Add address");
  });

  it("passes the selected key and the workplace page and filter through, ignoring anything malformed", async () => {
    const selected = await loader({
      request: new Request(
        `http://x/admin/se/company/${COMPANY}/address?address=${KEY}&workplaces=4&workplace_q=+Box+1+`,
      ),
      params: { companyId: COMPANY },
    } as never);
    expect(selected).toEqual({ detail, selectedKey: KEY });
    expect(server.loadSeAddressDetail).toHaveBeenLastCalledWith(COMPANY, {
      selectedKey: KEY,
      workplacePage: 4,
      workplaceQuery: "Box 1",
    });

    const malformed = await loader({
      request: new Request(
        `http://x/admin/se/company/${COMPANY}/address?address=not-a-key&workplaces=zero`,
      ),
      params: { companyId: COMPANY },
    } as never);
    expect(malformed).toEqual({ detail, selectedKey: null });
    expect(server.loadSeAddressDetail).toHaveBeenLastCalledWith(COMPANY, {
      selectedKey: null,
      workplacePage: 1,
      workplaceQuery: "",
    });
  });
```

- [x] **Step 13: Run the address tests, then the whole suite and the typecheck**

```bash
npm test -- tests/se-company-address-entity.server.test.ts tests/admin-se-company-address.test.tsx tests/se-address-fields.test.ts
npm run typecheck
npm test 2>&1 | tail -40
```

Expected: the three address files PASS in full; `typecheck` prints nothing and exits 0; the suite shows the Step 1 failures and no others. The Workplaces card does not exist yet, so a company's workplace rows are simply absent from the page — Task 2 gives them their card.

- [x] **Step 14: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'EOF'
feat(backoffice): slim the Address tab's loader and page the workplaces

Spec 2026-09-06 section 8, amended 2026-09-13. The loader returned every
published row with its members, their normalized rows and their raw rows: a
kommun with 1,502 workplace addresses rendered as 7.8 MB of HTML.

It now returns list rows without members -- the row's own columns, the hide
rule and a re-fold-pending flag computed in ClickHouse against the company's
current normalized versions -- and resolves members for the ?address= row
alone, from that row's own (source, slot) pairs. Active workplace-only rows
(kinds exactly ['workplace'], migration 000403's split) leave the list for a
page of fifty, counted and filtered in ClickHouse over normalized_address.
Fold-pending is one row of five scalar aggregates instead of two full reads.

?workplaces= and ?workplace_q= join ?address= in the URL; addressSearchString
builds the tab's whole search string. The five reviewer writes and their SQL
are untouched. No schema change.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add corpscout/services/backoffice/app/lib/se-address-fields.ts \
  corpscout/services/backoffice/app/lib/se-company-address-entity.server.ts \
  corpscout/services/backoffice/app/routes/admin-se-company-address.tsx \
  corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx \
  corpscout/services/backoffice/tests/se-address-fields.test.ts \
  corpscout/services/backoffice/tests/se-company-address-entity.server.test.ts \
  corpscout/services/backoffice/tests/admin-se-company-address.test.tsx
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 2: The Workplaces card, the shared map and the links that keep the page

**Files:**
- Modify: `corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx`
- Test: `corpscout/services/backoffice/tests/admin-se-company-address.test.tsx`

**Interfaces:**
- Consumes, from Task 1: `addressSearchString(state: SeAddressSearchState): string`, `MAX_WORKPLACE_QUERY_LENGTH`, `workplacePageFromSearch`/`workplaceQueryFromSearch` (through the route, not here), and the types `SeAddressListEntry`, `SeAddressPublishedDetail`, `SeAddressWorkplacePage`, `SeAddressDetail` with its `published` / `selected` / `workplaces` fields. The fixtures `listEntry`, `secondListEntry`, `workplaceEntry`, `NO_WORKPLACES`, `detail`, `EMPTY_DETAIL` and the `render(element, search)` helper already exist in the test file from Task 1.
- Produces: `listMapPoints(published: readonly SeAddressListEntry[], workplaces: readonly SeAddressListEntry[]): AddressMapPoint[]` (exported from `se-address-workspace.tsx`); the module-internal `WorkplacesCard` and the `addressHref: (addressKey: string) => string` prop carried by `AddressLine`, `AddressesCard` and `WorkplacesCard`.

- [x] **Step 1: Write the failing component tests**

In `tests/admin-se-company-address.test.tsx`, extend the component import (line 22) with the map helper:

```ts
import {
  listMapPoints,
  removeSuccessCopy,
  SeAddressWorkspace,
} from "~/components/admin/se-address-workspace";
```

and add these six tests at the end of the `describe("SeAddressWorkspace")` block:

```tsx
  /** The page as a kommun sees it: 1,502 workplaces, page 2, a filter in force
   * and a company address selected. */
  const PAGED_SEARCH = `?address=${KEY}&workplaces=2&workplace_q=verkstad`;
  const paged: SeAddressDetail = {
    ...detail,
    workplaces: { rows: [workplaceEntry], total: 1502, page: 2, pageSize: 50, query: "verkstad" },
  };
  const BASE = `/admin/se/company/${COMPANY}/address`;

  it("renders the Workplaces card with its total, its rows and its page footer", () => {
    const html = render(
      <SeAddressWorkspace companyId={COMPANY} detail={paged} selectedKey={KEY} result={null} />,
      PAGED_SEARCH,
    );
    expect(html).toContain("Workplaces (1502)");
    expect(html).toContain(`data-address-key="${WORKPLACE_KEY}"`);
    expect(html).toContain("verkstadsgatan 3|11124|stockholm|se");
    expect(html).toContain("Workplace");
    // Page 2 of fifty, one row on it.
    expect(html).toContain("51–51 of 1502");
    expect(html).toContain("Previous");
    expect(html).toContain("Next");
  });

  it("keeps the selected address and the filter in every workplace link, and the page in every address link", () => {
    const html = render(
      <SeAddressWorkspace companyId={COMPANY} detail={paged} selectedKey={KEY} result={null} />,
      PAGED_SEARCH,
    );
    // Previous and Next move only the page. `&` is escaped in an attribute.
    expect(html).toContain(`href="${BASE}?address=${KEY}&amp;workplaces=1&amp;workplace_q=verkstad"`);
    expect(html).toContain(`href="${BASE}?address=${KEY}&amp;workplaces=3&amp;workplace_q=verkstad"`);
    // Selecting a workplace row keeps the page and the filter.
    expect(html).toContain(
      `href="${BASE}?address=${WORKPLACE_KEY}&amp;workplaces=2&amp;workplace_q=verkstad"`,
    );
    // So does selecting a company address in the card above.
    expect(html).toContain(`href="${BASE}?address=${KEY}&amp;workplaces=2&amp;workplace_q=verkstad"`);
    // The filter form is a GET that carries the selection and drops the page:
    // a new filter starts at the top.
    expect(html).toContain('name="workplace_q"');
    expect(html).toContain('value="verkstad"');
    expect(html).toContain(`name="address" value="${KEY}"`);
    // Every POST (Fold now, Remove, Activate, Discard, Save draft) defaults its
    // action to the current URL, search included, so the round trip comes back
    // to this page of workplaces.
    expect(html).toContain(
      `action="${BASE}?address=${KEY}&amp;workplaces=2&amp;workplace_q=verkstad"`,
    );
  });

  it("hides the Workplaces card when there are none, and says so when a filter finds none", () => {
    const none = render(
      <SeAddressWorkspace companyId={COMPANY} detail={detail} selectedKey={KEY} result={null} />,
    );
    expect(none).not.toContain("Workplaces (");

    const filtered = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{
          ...detail,
          workplaces: { rows: [], total: 0, page: 1, pageSize: 50, query: "nowhere" },
        }}
        selectedKey={KEY}
        result={null}
      />,
      "?workplace_q=nowhere",
    );
    expect(filtered).toContain("Workplaces (0)");
    expect(filtered).toContain("No workplace matches.");
  });

  it("keeps a merged postal+workplace row in the Addresses card", () => {
    // The fold merges an establishment that repeats the company's own street
    // and postcode: `kinds` carries both, so the row IS the company's address
    // (migration 000403's rule) and never moves to the Workplaces card.
    const MERGED_KEY = "d".repeat(64);
    const merged: SeAddressListEntry = {
      ...listEntry,
      row: {
        ...row,
        address_key: MERGED_KEY,
        kinds: ["postal", "workplace"],
        sources: ["bolagsverket", "ratsit"],
        slots: ["", "est:4"],
        normalized_ids: ["norm-1", "norm-4"],
        normalized_address: "storgatan 5|11122|stockholm|se",
      },
    };
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [listEntry, merged] }}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(html).toContain(`data-address-key="${MERGED_KEY}"`);
    expect(html).toContain("Postal");
    expect(html).toContain("Workplace");
    expect(html).not.toContain("Workplaces (");
  });

  it("maps the company's active addresses and the current workplace page, once", () => {
    expect(listMapPoints([listEntry, secondListEntry], [workplaceEntry]).map((point) => point.key)).toEqual([
      KEY,
      SECOND_KEY,
      WORKPLACE_KEY,
    ]);
    // A withdrawn row is not where the company is, and a row without a usable
    // geocode has nothing to put on a map.
    expect(
      listMapPoints([{ ...listEntry, row: { ...row, active: 0, inactive_reason: "hidden" } }], []),
    ).toEqual([]);
    expect(
      listMapPoints([], [{ ...workplaceEntry, row: { ...workplaceEntry.row, geocode_status: "" } }]),
    ).toEqual([]);

    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...paged, published: [listEntry, secondListEntry] }}
        selectedKey={KEY}
        result={null}
      />,
      PAGED_SEARCH,
    );
    // The map is client-only, so what renders is its placeholder: one for the
    // whole list -- not one per row, and not one per card -- and one for the
    // panel's single point.
    const placeholders = html.match(/bg-muted text-muted-foreground flex h-56/g) ?? [];
    expect(placeholders).toHaveLength(2);
  });

  it("marks a list row the fold has yet to catch up with", () => {
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [{ ...listEntry, refoldPending: true }] }}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(html).toContain("re-fold pending");
  });
```

- [x] **Step 2: Run the component tests and watch them fail**

```bash
npm test -- tests/admin-se-company-address.test.tsx
```

Expected: FAIL — `listMapPoints is not a function`, and the six new tests miss "Workplaces (1502)", the hrefs and the badge.

- [x] **Step 3: Add the card, the map helper and the link builder**

`app/components/admin/se-address-workspace.tsx`, seven edits.

**(a)** the field imports (lines 44-49) and the type import from Task 1 gain three names:

```ts
import {
  addressKindLabel,
  addressSearchString,
  addressSourceLabel,
  geocodeStatusLabel,
  MAX_NOTE_LENGTH,
  MAX_WORKPLACE_QUERY_LENGTH,
} from "~/lib/se-address-fields";
import type {
  SeAddressComponents,
  SeAddressDetail,
  SeAddressDraft,
  SeAddressListEntry,
  SeAddressMember,
  SeAddressPublishedDetail,
  SeAddressRawRow,
  SeAddressRow,
  SeAddressWorkplacePage,
} from "~/lib/se-company-address-entity.server";
```

**(b)** after `mapPoint` (which ends at line 153), the list's points:

```tsx
/** Every point the list's one map carries: the company's ACTIVE addresses and
 * the workplace page's rows (spec 8, amended -- "the map shows the company
 * addresses and the current workplace page"). A withdrawn or hidden row is not
 * where this company is, and a row with no usable geocode has nothing to put on
 * a map, so the map may hold fewer points than the cards hold rows. Exported
 * for the test: the map itself never renders server-side. */
export function listMapPoints(
  published: readonly SeAddressListEntry[],
  workplaces: readonly SeAddressListEntry[],
): AddressMapPoint[] {
  return [...published.filter((entry) => entry.row.active === 1), ...workplaces]
    .map((entry) => mapPoint(entry.row))
    .filter((point): point is AddressMapPoint => point !== null);
}
```

**(c)** `AddressLine` takes the link builder and shows the flag:

```tsx
/** One published address: the line links to its panel, Correct sits outside
 * the link (an anchor may not wrap a button). The same row renders in the
 * Addresses card and in the Workplaces card. */
function AddressLine({
  entry,
  selectedKey,
  addressHref,
  busy,
  onCorrect,
}: {
  entry: SeAddressListEntry;
  selectedKey: string | null;
  /** The search string that selects this row, with the workplace page and its
   * filter kept. */
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const { row } = entry;
  const selected = row.address_key === selectedKey;
  return (
    <li className="flex items-center gap-1" data-address-key={row.address_key}>
      <Link
        to={{ search: addressHref(row.address_key) }}
        preventScrollReset
        aria-current={selected ? "true" : undefined}
        className={cn(
          "grid flex-1 grid-cols-1 gap-x-6 rounded-md px-2 py-2 hover:bg-muted/60 sm:grid-cols-[1fr_auto]",
          selected && "bg-muted",
        )}
      >
```

and, inside the badge span, after the geocode badge and before the `hidden` badge:

```tsx
          {entry.refoldPending ? <Badge variant="outline">re-fold pending</Badge> : null}
```

**(d)** `AddressesCard` takes the points and the link builder instead of computing points itself. Replace its signature, its `activePoints` block (lines 411-416, the comment and the const) and its map block:

```tsx
function AddressesCard({
  detail,
  selectedKey,
  mapPoints,
  addressHref,
  busy,
  onCorrect,
}: {
  detail: SeAddressDetail;
  selectedKey: string | null;
  /** The company's addresses AND the current workplace page. */
  mapPoints: readonly AddressMapPoint[];
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const navigate = useNavigate();
  const active = detail.published.filter((entry) => entry.row.active === 1);
  const inactive = detail.published.filter((entry) => entry.row.active !== 1);
  // A withdrawn or hidden address the panel is describing must be visible in
  // the list too: linking to one opens the group it hides in.
  const selectedIsInactive = inactive.some((entry) => entry.row.address_key === selectedKey);
```

```tsx
        {/* Nothing active and nothing on the map -- not even the empty frame,
            which would only repeat what the empty state above already says. */}
        {active.length === 0 && mapPoints.length === 0 ? null : (
          <div className="mb-3">
            <AddressMap
              points={mapPoints}
              selectedKey={selectedKey}
              onSelect={(key) => navigate({ search: addressHref(key) }, { preventScrollReset: true })}
            />
          </div>
        )}
```

Both `AddressLine` call sites inside this card (the active list and the inactive accordion) gain `addressHref={addressHref}`.

**(e)** the new card, right after `AddressesCard`:

```tsx
/**
 * The company's workplace addresses (spec 8, amended 2026-09-13, rule 2): rows
 * whose kinds are exactly `['workplace']`. Ratsit's establishments put hundreds
 * of them on some companies -- a kommun has 1,502 -- so they are counted, cut
 * and filtered in ClickHouse, fifty a page, and every link here keeps both the
 * page and the filter. The card is absent when the company has none and no
 * filter is in force; with a filter and no hit it stays, and says so.
 */
function WorkplacesCard({
  workplaces,
  selectedKey,
  addressHref,
  busy,
  onCorrect,
}: {
  workplaces: SeAddressWorkplacePage;
  selectedKey: string | null;
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const { rows, total, page, pageSize, query } = workplaces;
  if (total === 0 && query === "") return null;
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const from = rows.length === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = (page - 1) * pageSize + rows.length;
  const pageSearch = (nextPage: number) =>
    addressSearchString({ address: selectedKey, workplacePage: nextPage, workplaceQuery: query });
  return (
    <Card>
      <CardHeader>
        <CardTitle>{`Workplaces (${total})`}</CardTitle>
        <CardDescription>
          Establishments this company runs, from Ratsit. They are the entity's addresses but
          not the address it publishes as itself, so they are listed apart -- and the serving
          view leaves them out.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {/* A GET form REPLACES the search, which is exactly how a new filter
            resets to page 1; the selected address rides along in a hidden field
            so filtering does not empty the panel. */}
        <Form method="get" className="mb-3 flex flex-wrap items-center gap-2">
          {selectedKey === null ? null : <input type="hidden" name="address" value={selectedKey} />}
          <Input
            name="workplace_q"
            defaultValue={query}
            maxLength={MAX_WORKPLACE_QUERY_LENGTH}
            placeholder="Filter by address"
            aria-label="Filter workplaces"
            className="max-w-xs"
          />
          <Button type="submit" size="sm" variant="outline" disabled={busy}>
            Filter
          </Button>
          {query === "" ? null : (
            <Link
              className="text-muted-foreground text-xs underline"
              to={{
                search: addressSearchString({
                  address: selectedKey,
                  workplacePage: 1,
                  workplaceQuery: "",
                }),
              }}
              preventScrollReset
            >
              Clear
            </Link>
          )}
        </Form>
        {rows.length === 0 ? (
          <p className="text-muted-foreground text-sm">No workplace matches.</p>
        ) : (
          <ul className="grid grid-cols-1 gap-y-1 text-sm">
            {rows.map((entry) => (
              <AddressLine
                key={entry.row.address_key}
                entry={entry}
                selectedKey={selectedKey}
                addressHref={addressHref}
                busy={busy}
                onCorrect={onCorrect}
              />
            ))}
          </ul>
        )}
        <div className="text-muted-foreground mt-3 flex items-center justify-between text-xs">
          <span>{`${from}–${to} of ${total}`}</span>
          <span className="flex gap-3">
            {page > 1 ? (
              <Link className="underline" to={{ search: pageSearch(page - 1) }} preventScrollReset>
                Previous
              </Link>
            ) : null}
            {page < lastPage ? (
              <Link className="underline" to={{ search: pageSearch(page + 1) }} preventScrollReset>
                Next
              </Link>
            ) : null}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}
```

**(f)** in `SeAddressWorkspace`, build the link helper once and hand it down. The first line below is already there from Task 1 step 11(f) — the three after it are new. The `selectedKey` prop stays in the signature (the route passes it and the route test asserts it) and the render deliberately does not use it: the loader's resolved selection is the truth, and it already carries the default the URL did not name.

```tsx
  const selected = detail.selected;                            // already there
  const selectedAddressKey = selected?.row.address_key ?? null;
  // One builder for every link on the page: selecting an address keeps the
  // workplace page and its filter, paging keeps the selected address.
  const addressHref = (addressKey: string) =>
    addressSearchString({
      address: addressKey,
      workplacePage: detail.workplaces.page,
      workplaceQuery: detail.workplaces.query,
    });
  const correct = (entry: SeAddressListEntry) =>
    setSheet({
      mode: "correct",
      initial: initialFromRow(entry.row),
      slot: null,
      replacesKey: entry.row.address_key,
    });
```

**(g)** the render, replacing the `AddressesCard` element and adding the new card below it:

```tsx
        <AddressesCard
          detail={detail}
          // The effective selection, not the raw query string: the loader's
          // resolved row, default included, so the list marks what the panel
          // describes.
          selectedKey={selectedAddressKey}
          mapPoints={listMapPoints(detail.published, detail.workplaces.rows)}
          addressHref={addressHref}
          busy={busy}
          onCorrect={correct}
        />
        <WorkplacesCard
          workplaces={detail.workplaces}
          selectedKey={selectedAddressKey}
          addressHref={addressHref}
          busy={busy}
          onCorrect={correct}
        />
```

- [x] **Step 4: Run the component tests and watch them pass**

```bash
npm test -- tests/admin-se-company-address.test.tsx
```

Expected: PASS, the whole file — the six new tests and the seven that were already there.

- [x] **Step 5: Run the whole suite and the typecheck**

```bash
npm run typecheck
npm test 2>&1 | tail -40
```

Expected: `typecheck` exits 0; the suite shows the Task 1 Step 1 failures and no others.

- [x] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'EOF'
feat(backoffice): a paged Workplaces card on the Address tab

Spec 2026-09-06 section 8, amended 2026-09-13, rule 2. Active workplace-only
rows now render in their own card below the Addresses card -- "Workplaces
(<total>)", fifty a page, a filter box over the published line, Previous/Next
links -- and the card is absent when the company has none.

Every link on the page builds its search with addressSearchString, so
selecting an address keeps the workplace page and its filter and paging keeps
the selected address; a POST keeps both through React Router's default form
action. The list's one map carries the company's active addresses plus the
current workplace page, and a list row whose members have moved on wears a
re-fold pending badge.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx \
  corpscout/services/backoffice/tests/admin-se-company-address.test.tsx
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 3: The smoke on real data, and the docs

**Files:**
- Read-only: the whole backoffice tree (a dev server over the worktree's code)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md`
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`
- Modify: this plan (tick the boxes)

**Interfaces:**
- Consumes: the finished tab — the route `/admin/se/company/:companyId/address` with `?address=`, `?workplaces=` and `?workplace_q=`; `loadSeAddressDetail(companyId, options)`; the six POST intents, unchanged, of which this task exercises `save-draft` and `discard`.
- Produces: the measured sizes and times for the three companies, and two documentation paragraphs. No code.

**The owner's own dev server is on `[::1]:5183` and serves `main` from the main checkout. Never stop it, never restart it, never `pkill -f "react-router dev"`.** It is useful exactly as it is: it is the "before" measurement. The worktree's server goes on 5184.

- [x] **Step 1: Start a dev server over the worktree's code on 5184**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
ls -l .env                                  # must exist: ClickHouse credentials
lsof -nP -iTCP:5184 -sTCP:LISTEN            # must print nothing
lsof -nP -iTCP:5183 -sTCP:LISTEN            # the owner's server; leave it alone
npm run dev -- --port 5184 > /tmp/se-address-paging-dev.log 2>&1 &
```

Wait for it to answer (react-router dev compiles the route on the first request, so this also warms it):

```bash
timeout 180 bash -c 'until curl -6 -sf -o /dev/null "http://[::1]:5184/admin/se/company/5592303829/address"; do sleep 3; done' && echo UP
```

Expected: `UP`. If it never comes up, read `/tmp/se-address-paging-dev.log`.

- [x] **Step 2: Measure the kommun, before and after**

Company 2120000142 has 1,503 active rows, 1,502 of them workplace-only. Request each URL twice and record the SECOND number: the first request through a dev server pays for compilation.

```bash
for round in 1 2; do
  for port in 5183 5184; do
    printf 'port %s round %s: ' "$port" "$round"
    curl -6 -s -o /dev/null -w '%{size_download} bytes  %{time_total} s\n' \
      "http://[::1]:$port/admin/se/company/2120000142/address"
  done
done
```

Expected: port 5183 (main, the "before") around 7.8 MB and several seconds; port 5184 (this branch) **under 400 KB** — the spec's target — and a fraction of the time. Write both numbers into the task notes; Task 3's commit message carries them.

If 5184 is still megabytes, the loader is still shipping members: check that `ADDRESS_LIST_SQL` — not `ADDRESS_MAIN_SQL` — is what the page ran, and that `detail.published` holds no `members` key.

- [x] **Step 3: Measure the two ordinary companies and check the merged row**

```bash
for company in 5594121039 5592303829; do
  for round in 1 2; do
    printf '%s round %s: ' "$company" "$round"
    curl -6 -s -o /dev/null -w '%{size_download} bytes  %{time_total} s\n' \
      "http://[::1]:5184/admin/se/company/$company/address"
  done
done
# 5594121039 (Oxie) has an establishment the fold merged into the company's own
# postal address: kinds ['postal','workplace'] -- it must be in the Addresses
# card, badged both ways, and it must NOT pull a Workplaces card into being
# unless that company also has workplace-only rows.
curl -6 -s "http://[::1]:5184/admin/se/company/5594121039/address" | grep -o 'Workplace</span>\|Postal</span>\|Workplaces ([0-9]*)' | sort | uniq -c
```

Expected: both companies render in tens of kilobytes; the Oxie page shows both badges on one row. Record what the `grep` printed.

- [x] **Step 4: Page, filter and select a workplace**

```bash
BASE="http://[::1]:5184/admin/se/company/2120000142/address"
# The card's title and the first page's footer.
curl -6 -s "$BASE" | grep -o 'Workplaces ([0-9]*)\|1–[0-9]* of [0-9]*'
# Page 2 starts at 51.
curl -6 -s "$BASE?workplaces=2" | grep -o '51–[0-9]* of [0-9]*'
# A filter cuts the total and the footer follows it.
curl -6 -s "$BASE?workplace_q=gatan" | grep -o 'Workplaces ([0-9]*)\|1–[0-9]* of [0-9]*'
# A filter that matches nothing keeps the card and says so.
curl -6 -s "$BASE?workplace_q=zzzznotanaddress" | grep -o 'Workplaces (0)\|No workplace matches.'
# Selecting the last workplace row of page 2 keeps the page and the filter.
KEY=$(curl -6 -s "$BASE?workplaces=2" | grep -o 'data-address-key="[0-9a-f]\{64\}"' | tail -1 | cut -d'"' -f2)
echo "workplace key: $KEY"
curl -6 -s -o /dev/null -w 'selected: %{size_download} bytes  %{time_total} s\n' \
  "$BASE?address=$KEY&workplaces=2"
curl -6 -s "$BASE?address=$KEY&workplaces=2" | grep -o 'aria-current="true"\|Published text\|51–[0-9]* of [0-9]*' | sort | uniq -c
```

Expected: the totals and footers line up (1,502 with no filter, smaller with `gatan`), the empty filter says `No workplace matches.`, and the selected page still weighs under 400 KB while showing `aria-current="true"`, the panel's `Published text` heading and page 2's footer.

- [x] **Step 5: Correct a workplace row, then discard the draft**

This writes two `reviewer_draft` versions (a draft, then its tombstone) for company 2120000142 on prod ClickHouse. Nothing publishes: the fold is not run, and a `reviewer_draft` row is never folded.

```bash
POST="$BASE?address=$KEY&workplaces=2"
curl -6 -s -X POST "$POST" \
  --data-urlencode "intent=save-draft" \
  --data-urlencode "care_of=" \
  --data-urlencode "street_line=Verkstadsgatan 3" \
  --data-urlencode "postal_code=11124" \
  --data-urlencode "city=Stockholm" \
  --data-urlencode "country=SE" \
  --data-urlencode "kind=postal" \
  --data-urlencode "note=address paging smoke" \
  --data-urlencode "slot=" \
  --data-urlencode "replaces_key=$KEY" \
  > /tmp/se-address-paging-correct.html
grep -o 'Draft saved[^<]*\|data-slot-id="r[0-9]\{17\}"\|workplaces=2' /tmp/se-address-paging-correct.html | sort | uniq -c
SLOT=$(grep -o 'data-slot-id="r[0-9]\{17\}"' /tmp/se-address-paging-correct.html | head -1 | cut -d'"' -f2)
echo "draft slot: $SLOT"
curl -6 -s -X POST "$POST" \
  --data-urlencode "intent=discard" \
  --data-urlencode "slot=$SLOT" \
  | grep -o 'Draft discarded.\|data-slot-id="r[0-9]\{17\}"' | sort | uniq -c
```

Expected: the first POST answers with `Draft saved…`, a `data-slot-id`, and `workplaces=2` still in the page's links (the action round trip kept the page); the second answers `Draft discarded.` and no `data-slot-id` at all. Record the slot in the task notes.

- [x] **Step 6: Stop the worktree's dev server only**

```bash
lsof -nP -iTCP:5184 -sTCP:LISTEN -t | xargs -r kill
lsof -nP -iTCP:5183 -sTCP:LISTEN | head -2   # the owner's server is still up
```

Expected: 5184 is free, 5183 still listening. **Do not use `pkill -f "react-router dev"`** — it would take the owner's server with it.

- [x] **Step 7: Write the address package's design-doc paragraph**

In `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md`, insert this section at the end of "Backoffice (slice 3, 2026-09-07)" — after the paragraph ending "so a draft saved a moment earlier parses before it folds." and before `## Readers (slice 4a, 2026-09-08)`:

```markdown
### The tab is slim, and the workplaces are paged (2026-09-13)

Ratsit's establishments put 1,502 `workplace` rows on one kommun, and the tab rendered them
as 7.8 MB of HTML: markup for every row plus a hydration payload carrying every row's
members with their normalized and raw rows. `loadSeAddressDetail(companyId, {selectedKey,
workplacePage, workplaceQuery})` now returns list rows WITHOUT members -- the row's own
columns, the hide rule in force and a re-fold-pending flag computed in ClickHouse
(`has(current_pairs, …) AND NOT has(current_triples, …)` over the company's current
normalized versions, `groupArray`-ed once per query) -- and resolves members, the
`text_source` reason and the raw text for ONE row, from two reads bound to that row's own
`(source, slot)` pairs (`arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)})`).
That row is the `?address=<key>` row when the key names one of the company's, else the first
active row of the list -- the default the tab has always had -- and there is none only when
the company has no active company address at all. "Fold pending" is one row of five scalar aggregates
(`ADDRESS_FOLD_STATE_SQL`) instead of two whole-company reads. Drafts load on their own
(`source = 'reviewer_draft'`), History keeps its cap of 200.

Rows whose `kinds` are exactly `['workplace']` and that are active leave the Addresses card
for a "Workplaces (<total>)" card below it -- migration 000403's split, so the tab and the
serving view draw the same line -- paged 50 a page and filtered over `normalized_address`
with `positionCaseInsensitiveUTF8` (a literal needle: nothing to escape). Page and filter
live in the URL as `?workplaces=<page>` and `?workplace_q=<text>` beside `?address=`, and
`addressSearchString` in `app/lib/se-address-fields.ts` builds every link on the page from
all three, so selecting an address keeps the workplace page and paging keeps the selected
address; a POST keeps both through React Router's default form action. The map shows the
company's active addresses plus the current workplace page. The five reviewer writes still
read the company whole -- they are one POST per click, and Remove must be able to find a
workplace row by key.
```

- [x] **Step 8: Write the spec's Shipped line**

In `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, append this paragraph immediately after the amendment's last line ("…every other company gets the lighter loader as a side effect.") and before `## 9. Slices, parity, cutover`:

```markdown
Shipped 2026-09-13 (plan `2026-09-13-se-address-tab-paging.md`): the loader returns list
rows without members and resolves the selected row's members -- the `?address=` row, else
the first active one -- from that row's own `(source, slot)` pairs; workplace-only rows moved to a 50-a-page "Workplaces" card with a
`normalized_address` filter, `?workplaces=` and `?workplace_q=` joining `?address=` in every
link; fold-pending became one row of scalar aggregates. No schema change.
```

Leave the measured prod/smoke record to the controller: it appends the sizes and times after the merge.

- [x] **Step 9: Verify the whole suite one last time and tick this plan**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
npm run typecheck
npm test 2>&1 | tail -40
```

Expected: `typecheck` exits 0; the suite shows exactly the failures recorded in Task 1 Step 1. Then tick every `- [ ]` box in this plan that the work actually did.

- [x] **Step 10: Commit**

The five `<…>` marks in the message below are the numbers Steps 2, 3 and 5 printed: substitute each one for the measurement before committing. They are the reason the slice exists, so the commit must carry them rather than the marks.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'EOF'
docs(address): record the Address tab's paging in the design doc and the spec

Smoked on a dev server over this branch (port 5184) against prod ClickHouse,
with the owner's server on 5183 serving main as the "before":

- 2120000142 (1,502 workplaces): BEFORE <before> -> AFTER <after>
- 5594121039 (Oxie): <size>, the merged ['postal','workplace'] row still in
  the Addresses card
- 5592303829 (Bromma): <size>
- page 2 footer, a filter, an empty filter, and a workplace row selected by
  ?address= with the page kept; a Correct saved and discarded on one of them

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
  corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md \
  corpscout/services/dagster_v3/docs/superpowers/plans/2026-09-13-se-address-tab-paging.md
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```
