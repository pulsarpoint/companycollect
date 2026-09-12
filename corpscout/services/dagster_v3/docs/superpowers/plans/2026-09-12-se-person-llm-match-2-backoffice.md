# SE Person LLM Match Slice 2: The Backoffice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show the LLM's identity decisions on the People tab — a `matched by LLM · 0.93` badge on every observation the model joined, the fold's `llm_match` record as a readable list instead of raw JSON, and a "Possible matches" card whose `Merge` button turns a 0.5–0.8 pair into the reviewer merge rule the fold already understands.

**Architecture:** One seventh FINAL read (`PERSON_MATCH_SQL`) joins `se_company_person_match` to the company's `se_company_person_match_state` row on `input_hash` — exactly the certification `batch.py::match_pairs_sql` applies — and returns every pair at or above `POSSIBLE_MATCH_FLOOR`. The loader turns those rows into three derived shapes: `matchedBy` per published person (pairs at or above `MATCH_THRESHOLD` whose two sides both sit inside that person's `normalized_ids`, mirroring `fold.py::pairs_within`), a `match` field per member (the strongest pair naming that observation), and `possibleMatches` on the detail (pairs in the band whose sides sit in two **different active** persons). A new client-safe module `app/lib/se-person-match.ts` holds the two constants, the read row, the derived types and the pure helpers (`formatConfidence`, `matchNote`, `parseLlmMatch`, `stripLlmMatch`) so the workspace can render them without importing a `.server` module. The workspace adds a badge, a section and one card. **No new intent, no new rule kind, no new route, no Dagster change, no migration:** the card posts the existing `merge` intent.

**Tech Stack:** React Router v7 (`corpscout/services/backoffice`, TypeScript strict + `verbatimModuleSyntax`, shadcn/ui over Base UI, vitest 3), `@clickhouse/client` through `app/lib/clickhouse.server.ts`. Typecheck `pnpm typecheck` (= `react-router typegen && tsc`); suite `CI=1 pnpm exec vitest run --reporter=dot`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-person-llm-matching-design.md` — this plan is section 10 item 2, and its content is **section 5** (the backoffice) plus its bullet of section 6 (tests). Sections 1, 3.4, 4, 9 and 10 item 1 are the ground it stands on: slice 1 shipped on 2026-09-11, so prod already holds 149,334 pairs at or above 0.8, **9,529 pairs in the 0.5–0.8 band** this slice finally shows, and 124,451 persons carrying `data.llm_match`.

## Global Constraints

- Work only in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-person-llm-match-2`. Always use absolute paths. **Never `git stash`.** Never `git add -A` or `git add .` — stage by explicit path, every time.
- Commit with a message file, never `-m`: write the message to a file and run `git commit -F "$MSGFILE"`. Every message ends with these two trailers, contiguous, in this order:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- One commit per task. A task is not done until `pnpm typecheck` is green and its own test files pass.
- All backoffice commands run from `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice`.
- **No new intent and no new rule kind.** Spec section 5: the possible-matches Merge "submits the existing `merge` intent with the two person keys pre-filled". `parseSePersonDecision`'s eight intents and `tables.py::RULE_KINDS` (`hide|merge|split`) are untouched; the reviewer merge rule is the whole mechanism, and it outranks the threshold on the next fold (spec section 4).
- **`RESERVED_DATA_KEYS` stays the reviewer's three** (`decided_by`, `note`, `replaces_key`, `app/lib/se-person-fields.ts:24`). `llm_match` is fold-owned: `fold.py::_with_llm_match` writes it onto the PUBLISHED row's `data` after `merge_member_data`, no member ever carries it, and this slice only reads it. It must **not** join `RESERVED_DATA_KEYS`, because that list means "a key the sheet may not post back", and `ownData()` (entity server) / `reviewerData()` (workspace) strip exactly that list from a **draft or member** `data` before re-stamping or re-editing it — neither ever sees a published row's `data` (a Correct opens with `data` empty on purpose, `initialFromRow`'s comment). So nothing strips `llm_match` on a write path, and nothing needs to: the only place it is rendered is the published person's panel, where `stripLlmMatch` takes it out of the JSON block and the new section renders it instead.
- **No new dependencies.** No package.json change.
- **Nothing touches dagster**: no file under `corpscout/services/dagster_v3/src/`, no migration, no serving view. The only dagster-tree file this plan writes is the spec's shipped record (Task 4).
- **Do not fix the pre-existing failing tests.** Red on this branch before any change and **not** this slice's regressions: `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx`, plus an occasional load-flaky timeout in `tests/esef-financial-reports.server.test.ts`. Baseline: **125 files, 1,340 tests**.
- The route file exports only `loader`, `action`, `meta` and the default component — any other export that touches a `.server` module keeps that module in the client bundle and breaks the production build. `app/lib/se-person-match.ts` is client-safe and **never** imports a `.server` module.
- Every read of the entity's tables keeps the file's house style: `FINAL` where a current version is needed, the company bound as `{companyId:String}`, `FixedString(64)` through `toString(...)` and arrays of them through `arrayMap(x -> toString(x), ...)`, nullable values collapsed to `''`.
- `confidence` is `Float64` and both band comparisons are inclusive at the floor (spec section 8, and the DDL comment in `000399_corpscout_se_company_person_match.up.sql`): never round a confidence before comparing it, only before displaying it.

## File map

| File | Responsibility |
| --- | --- |
| `backoffice/app/lib/se-person-match.ts` (create) | `MATCH_THRESHOLD`, `POSSIBLE_MATCH_FLOOR`, `LLM_MATCH_KEY`; the read row `SePersonMatchRow`; the derived `SePersonMatch` / `SePersonPossibleMatch`; the `llm_match` block types; `isMatch`, `isPossibleMatch`, `formatConfidence`, `matchNote`, `parseLlmMatch`, `stripLlmMatch`. Client-safe. |
| `backoffice/app/lib/se-company-person-entity.server.ts` (modify) | `PERSON_MATCH_SQL`, the seventh read in the `Promise.all`, `matchesWithin`, `possibleMatchesOf`, `membersOf`'s new argument, and the three new fields on `SePersonMember` / `SePersonPublished` / `SePersonDetail`. |
| `backoffice/app/components/admin/se-person-workspace.tsx` (modify) | The `matched by LLM · 0.93` badge in `MemberEntry`, the "LLM match" section and the `llm_match`-free Data block in `PersonPanel`, the new `PossibleMatchesCard` and its place under `PersonsCard`. |
| `backoffice/app/routes/admin-se-company-person.tsx` (modify) | `EMPTY_DETAIL` gains `possibleMatches: []`. Nothing else: the action's intent switch is unchanged. |
| `backoffice/tests/se-person-match.test.ts` (create) | The constants, the bands, the note, the `llm_match` parse and strip. |
| `backoffice/tests/se-company-person-entity.server.test.ts` (modify) | The `PERSON_MATCH_SQL` pins, the `answer(sql)` branch, the derived shapes, the two drop rules. |
| `backoffice/tests/admin-se-company-person.test.tsx` (modify) | The fixtures' new fields, the render assertions, and the possible-match Merge post through the action. |
| Spec section 10 item 2 (modify, Task 4) | The shipped record. |

## Interfaces (defined in Task 1, consumed by Tasks 2 and 3)

```ts
// app/lib/se-person-match.ts
export const MATCH_THRESHOLD = 0.8;
export const POSSIBLE_MATCH_FLOOR = 0.5;
export const LLM_MATCH_KEY = "llm_match";

/** One row of PERSON_MATCH_SQL. */
export interface SePersonMatchRow {
  company_id: string;
  candidate_a: string;
  candidate_b: string;
  members_a: string[];
  members_b: string[];
  name_a: string;
  name_b: string;
  confidence: number;
  reason: string;
}

/** A pair the loader resolved INSIDE one published person. */
export interface SePersonMatch {
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
  /** Both sides' normalized ids, so a member can tell whether it is named. */
  members: string[];
}

/** A pair BETWEEN two active published persons, scored inside the band. */
export interface SePersonPossibleMatch {
  personKeyA: string;
  personKeyB: string;
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
}

export interface SeLlmMatchPair { a: string; b: string; confidence: number; reason: string }
export interface SeLlmMatchBlock { pairs: SeLlmMatchPair[]; model: string; promptVersion: string }

export function isMatch(confidence: number): boolean;
export function isPossibleMatch(confidence: number): boolean;
export function formatConfidence(confidence: number): string;      // 0.93 -> "0.93"
export function matchNote(confidence: number, reason: string): string; // "LLM match 0.64: <reason>"
export function parseLlmMatch(data: string): SeLlmMatchBlock | null;
export function stripLlmMatch(data: string): string;

// app/lib/se-company-person-entity.server.ts
export const PERSON_MATCH_SQL: string;
// SePersonMember keeps source, slot, normalizedId, name, birthYear, wikidataId, data,
// current, raw, refoldPending and precedence, and gains:
//   match: SePersonMatch | null
// SePersonPublished keeps row, members, roles, spellingReason and rules, and gains:
//   matchedBy: SePersonMatch[]
// SePersonDetail keeps published, drafts, history, rules, precedence and foldPending,
// and gains:
//   possibleMatches: SePersonPossibleMatch[]
```

---

### Task 1: The match read and the three derived shapes

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-person-match.ts`
- Create: `corpscout/services/backoffice/tests/se-person-match.test.ts`
- Modify: `corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts` (the row interfaces block ~line 185, `membersOf` ~line 410, `loadSePersonDetail` ~line 538)
- Modify: `corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts`
- Modify: `corpscout/services/backoffice/app/routes/admin-se-company-person.tsx:27` (`EMPTY_DETAIL`)
- Modify: `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx` (fixtures only — the render assertions are Task 2)

**Interfaces:**
- Consumes: nothing from earlier tasks. `MAX_NOTE_LENGTH` (500) from `~/lib/se-person-fields`; `chQuery` from `~/lib/clickhouse.server`.
- Produces: every name in the Interfaces block above. Task 2 imports `formatConfidence`, `matchNote`, `parseLlmMatch`, `stripLlmMatch` and `type SePersonPossibleMatch` from `~/lib/se-person-match`, and reads `member.match`, `entry.matchedBy` and `detail.possibleMatches`.

- [ ] **Step 1: Write the failing test for the client-safe module**

Create `corpscout/services/backoffice/tests/se-person-match.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { noteError } from "~/lib/se-person-fields";
import {
  formatConfidence,
  isMatch,
  isPossibleMatch,
  LLM_MATCH_KEY,
  matchNote,
  MATCH_THRESHOLD,
  parseLlmMatch,
  POSSIBLE_MATCH_FLOOR,
  stripLlmMatch,
} from "~/lib/se-person-match";

/** Exactly what `fold.py::_with_llm_match` writes: sorted keys, no spaces, the
 * confidence rounded to four decimals. */
const FOLD_DATA =
  '{"llm_match":{"model":"deepseek-v4-flash","pairs":[{"a":"Erik Bo Bengtsson","b":"Bo Bengtsson","confidence":0.93,"reason":"call name"}],"prompt_version":"se-person-match-v1"},"role_kind":"board_member"}';

describe("se-person-match", () => {
  it("pins the two bands to the fold's own constants, inclusive at the floor", () => {
    expect(MATCH_THRESHOLD).toBe(0.8);
    expect(POSSIBLE_MATCH_FLOOR).toBe(0.5);
    expect(LLM_MATCH_KEY).toBe("llm_match");
    // The fold merged everything at or above the threshold, so the tab reports it.
    expect(isMatch(0.8)).toBe(true);
    expect(isMatch(0.7999)).toBe(false);
    // The band is [0.5, 0.8): the reviewer's to decide, nothing merged it.
    expect(isPossibleMatch(0.5)).toBe(true);
    expect(isPossibleMatch(0.79)).toBe(true);
    expect(isPossibleMatch(0.8)).toBe(false);
    expect(isPossibleMatch(0.49)).toBe(false);
  });

  it("shows a confidence with two decimals", () => {
    expect(formatConfidence(0.93)).toBe("0.93");
    expect(formatConfidence(0.6)).toBe("0.60");
    expect(formatConfidence(1)).toBe("1.00");
  });

  it("writes a merge note the decision parser accepts, whatever the model said", () => {
    expect(matchNote(0.64, "same call name")).toBe("LLM match 0.64: same call name");
    // A reason is model text: it can carry a newline or a tab, which `noteError`
    // refuses on a rule note, and it can be longer than the 500-character limit.
    expect(matchNote(0.64, "two\nlines\tapart")).toBe("LLM match 0.64: two lines apart");
    const long = matchNote(0.64, "x".repeat(900));
    expect(long.length).toBe(500);
    expect(noteError(long)).toBeNull();
    expect(noteError(matchNote(0.64, "two\nlines"))).toBeNull();
    // No reason at all still names the score.
    expect(matchNote(0.5, "   ")).toBe("LLM match 0.50");
  });

  it("reads the fold's llm_match block", () => {
    expect(parseLlmMatch(FOLD_DATA)).toEqual({
      pairs: [
        { a: "Erik Bo Bengtsson", b: "Bo Bengtsson", confidence: 0.93, reason: "call name" },
      ],
      model: "deepseek-v4-flash",
      promptVersion: "se-person-match-v1",
    });
    expect(parseLlmMatch('{"role_kind":"board_member"}')).toBeNull();
    expect(parseLlmMatch("")).toBeNull();
    expect(parseLlmMatch("not json")).toBeNull();
  });

  it("takes the fold-owned key out of the data block and leaves everything else byte for byte", () => {
    expect(stripLlmMatch(FOLD_DATA)).toBe('{"role_kind":"board_member"}');
    // Nothing to strip: the stored text is shown exactly as it is stored.
    expect(stripLlmMatch('{"b":1,"a":2}')).toBe('{"b":1,"a":2}');
    expect(stripLlmMatch("")).toBe("");
    expect(stripLlmMatch("not json")).toBe("not json");
    // The only key: an empty object, which `hasData` already renders as "none".
    expect(stripLlmMatch('{"llm_match":{"pairs":[]}}')).toBe("{}");
  });
});
```

- [ ] **Step 2: Run it to make sure it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-person-match.test.ts --reporter=dot
```
Expected: FAIL — `Failed to resolve import "~/lib/se-person-match"`.

- [ ] **Step 3: Write the module**

Create `corpscout/services/backoffice/app/lib/se-person-match.ts`:

```ts
/**
 * The LLM identity-matching band, the read row and the pure helpers the People tab
 * needs (spec 2026-09-11 sections 5 and 9). Client-safe: no `.server` import, so the
 * workspace renders a badge and a merge note without dragging ClickHouse into the
 * client bundle.
 *
 * The two constants are the pipeline's own. At or above `MATCH_THRESHOLD`
 * (`fold.py::MATCH_THRESHOLD`) the fold has ALREADY merged the pair into one person, so
 * the tab only reports what happened; between `POSSIBLE_MATCH_FLOOR` and the threshold
 * nothing was merged and the reviewer decides with a merge rule. `confidence` is
 * Float64 in ClickHouse and the comparison is inclusive at the floor, so a threshold
 * edit stays exact (spec section 8).
 *
 * `llm_match` is FOLD-OWNED: `fold.py::_with_llm_match` writes it onto the published
 * row's `data` after `merge_member_data`, no member ever carries it, and nothing here
 * writes it back. It is deliberately NOT in `RESERVED_DATA_KEYS` -- that list is the
 * three keys the reviewer's own sheet may not post, stripped from a draft or member
 * `data` by `ownData()`/`reviewerData()`, and neither of those ever sees a published
 * row's `data`.
 */
import { MAX_NOTE_LENGTH } from "~/lib/se-person-fields";

export const MATCH_THRESHOLD = 0.8;
export const POSSIBLE_MATCH_FLOOR = 0.5;
export const LLM_MATCH_KEY = "llm_match";

/** One scored pair as `PERSON_MATCH_SQL` delivers it. `members_a`/`members_b` are the
 * normalized ids each side stands for -- a candidate is a group of same-name rows
 * within one source -- which is what maps a pair onto a published person's
 * `normalized_ids`. */
export interface SePersonMatchRow {
  company_id: string;
  candidate_a: string;
  candidate_b: string;
  members_a: string[];
  members_b: string[];
  name_a: string;
  name_b: string;
  confidence: number;
  reason: string;
}

/** A pair at or above the threshold, resolved INSIDE one published person. */
export interface SePersonMatch {
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
  /** Both sides' normalized ids, so one member can tell whether it is named. */
  members: string[];
}

/** A pair inside the band whose two sides sit in two DIFFERENT active persons: the one
 * thing the reviewer can act on, and what the possible-matches card lists. */
export interface SePersonPossibleMatch {
  personKeyA: string;
  personKeyB: string;
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
}

/** One entry of the fold's own record (`data.llm_match.pairs`). */
export interface SeLlmMatchPair {
  a: string;
  b: string;
  confidence: number;
  reason: string;
}
export interface SeLlmMatchBlock {
  pairs: SeLlmMatchPair[];
  model: string;
  promptVersion: string;
}

export function isMatch(confidence: number): boolean {
  return confidence >= MATCH_THRESHOLD;
}
export function isPossibleMatch(confidence: number): boolean {
  return confidence >= POSSIBLE_MATCH_FLOOR && confidence < MATCH_THRESHOLD;
}

/** The spec's own spelling of a score (`0.93`, `0.64`). */
export function formatConfidence(confidence: number): string {
  return confidence.toFixed(2);
}

/**
 * The note a Merge from the possible-matches card writes onto the merge rule, spec
 * section 5's `LLM match 0.64: <reason>`. The reason is model text, so it is flattened
 * to plain single-spaced text and the whole note is clamped to `MAX_NOTE_LENGTH`:
 * `parseSePersonDecision` runs every note through `noteError`, which refuses a control
 * character and anything past 500 characters -- a refusal here would read to the
 * reviewer as "the Merge button is broken".
 */
export function matchNote(confidence: number, reason: string): string {
  const plain = reason.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
  const head = `LLM match ${formatConfidence(confidence)}`;
  return (plain === "" ? head : `${head}: ${plain}`).slice(0, MAX_NOTE_LENGTH).trim();
}

/** A `data` string as an object; a hand-written row that is not one reads as empty
 * rather than throwing the whole tab. */
function parseObject(data: string): Record<string, unknown> {
  try {
    const parsed: unknown = JSON.parse(data === "" ? "{}" : data);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
}

/** The fold's record of what it merged, or null when this person was not joined by the
 * model. Every field is defended: the block is stored JSON, not a typed column. */
export function parseLlmMatch(data: string): SeLlmMatchBlock | null {
  const block = parseObject(data)[LLM_MATCH_KEY];
  if (block === null || typeof block !== "object" || Array.isArray(block)) return null;
  const record = block as Record<string, unknown>;
  const pairs: unknown[] = Array.isArray(record.pairs) ? record.pairs : [];
  return {
    pairs: pairs
      .filter(
        (pair): pair is Record<string, unknown> =>
          pair !== null && typeof pair === "object" && !Array.isArray(pair),
      )
      .map((pair) => ({
        a: typeof pair.a === "string" ? pair.a : "",
        b: typeof pair.b === "string" ? pair.b : "",
        confidence: typeof pair.confidence === "number" ? pair.confidence : 0,
        reason: typeof pair.reason === "string" ? pair.reason : "",
      })),
    model: typeof record.model === "string" ? record.model : "",
    promptVersion: typeof record.prompt_version === "string" ? record.prompt_version : "",
  };
}

/** `data` without the fold-owned key, for the panel's JSON block. A `data` with nothing
 * to strip comes back BYTE FOR BYTE, so the reviewer still reads what is stored rather
 * than a re-serialization of it. */
export function stripLlmMatch(data: string): string {
  const parsed = parseObject(data);
  if (!Object.hasOwn(parsed, LLM_MATCH_KEY)) return data;
  delete parsed[LLM_MATCH_KEY];
  return JSON.stringify(parsed);
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-person-match.test.ts --reporter=dot
```
Expected: PASS, 5 tests.

- [ ] **Step 5: Write the failing loader tests**

Four edits to `corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts`.

**(a)** Extend the import block from `~/lib/se-company-person-entity.server` with `PERSON_MATCH_SQL` (keep the list alphabetical: it goes between `PERSON_HISTORY_SQL` and `PERSON_MAIN_SQL`), and add a second import above `const COMPANY = "5560000001";`:

```ts
import type { SePersonMatch, SePersonMatchRow } from "~/lib/se-person-match";
```

**(b)** After `const PRECEDENCE_ROWS = [...];` add the pair fixtures:

```ts
/** The model's answer for this company, as PERSON_MATCH_SQL delivers it. n1 and n2 are
 * the merged person's two observations, n3 is the Wikidata person's one. */
const MATCH_ROWS: SePersonMatchRow[] = [
  {
    company_id: COMPANY, candidate_a: "n1", candidate_b: "n2",
    members_a: ["n1"], members_b: ["n2"],
    name_a: "Anna Svensson", name_b: "Anna Maria Svensson",
    confidence: 0.93, reason: "call name",
  },
  {
    company_id: COMPANY, candidate_a: "n1", candidate_b: "n3",
    members_a: ["n1"], members_b: ["n3"],
    name_a: "Anna Svensson", name_b: "Carl von Essen",
    confidence: 0.72, reason: "same household",
  },
  {
    company_id: COMPANY, candidate_a: "n2", candidate_b: "n3",
    members_a: ["n2"], members_b: ["n3"],
    name_a: "Anna Maria Svensson", name_b: "Carl von Essen",
    confidence: 0.64, reason: "same surname",
  },
  {
    company_id: COMPANY, candidate_a: "n3", candidate_b: "zz",
    members_a: ["n3"], members_b: ["zz"],
    name_a: "Carl von Essen", name_b: "Carla Essen",
    confidence: 0.66, reason: "an observation no published person holds",
  },
];
/** The one pair at or above the threshold, as the loader derives it. */
const CALL_NAME_MATCH: SePersonMatch = {
  nameA: "Anna Svensson", nameB: "Anna Maria Svensson", confidence: 0.93,
  reason: "call name", members: ["n1", "n2"],
};
```

**(c)** In `answer(sql)`, add one branch after the precedence line (the match table's own
name carries the `_match` suffix, so it cannot collide with the main table's branch):

```ts
  if (sql.includes("FROM corpscout.se_company_person_match AS p FINAL")) return MATCH_ROWS;
```

**(d)** Add `PERSON_MATCH_SQL` to both `for (const sql of [...])` loops (the one in the
pinning test and the one at the end of "assembles the published persons..."), add these
assertions at the end of the pinning test's body, give both members of the "assembles"
expectation their new field, and add the two new `it` blocks:

```ts
    // The seventh read (spec section 5): the pairs the company's CURRENT input
    // certifies, exactly the join `batch.py::match_pairs_sql` makes.
    expect(PERSON_MATCH_SQL).toContain("FROM corpscout.se_company_person_match AS p FINAL");
    expect(PERSON_MATCH_SQL).toContain("FROM corpscout.se_company_person_match_state FINAL");
    expect(PERSON_MATCH_SQL).toContain("WHERE company_id = {companyId:String} AND error = ''");
    expect(PERSON_MATCH_SQL).toContain(
      "ON s.company_id = p.company_id AND s.input_hash = p.input_hash",
    );
    // The floor, not the threshold: the band is what the reviewer acts on.
    expect(PERSON_MATCH_SQL).toContain("AND p.confidence >= 0.5");
    expect(PERSON_MATCH_SQL).toContain("toString(p.candidate_a) AS candidate_a");
    expect(PERSON_MATCH_SQL).toContain("arrayMap(x -> toString(x), p.members_a) AS members_a");
    expect(PERSON_MATCH_SQL).toContain("arrayMap(x -> toString(x), p.members_b) AS members_b");
    expect(PERSON_MATCH_SQL).toContain("toFloat64(p.confidence) AS confidence");
```

In the members expectation of "assembles the published persons, their members, roles,
rules and the drafts", both entries gain `match: CALL_NAME_MATCH` as their last field
(the 0.93 pair names n1 on one side and n2 on the other, so both observations are
badged), and the same test gains one line after `expect(merged?.rules).toEqual([MERGE_RULE]);`:

```ts
    expect(merged?.matchedBy).toEqual([CALL_NAME_MATCH]);
    expect(detail?.published[1]?.matchedBy).toEqual([]);
    expect(detail?.published[1]?.members[0]?.match).toBeNull();
```

Then the two new tests, after "returns null only when there is no main row...":

```ts
  it("offers the band's cross-person pairs, strongest first, and drops the ones nobody can act on", async () => {
    const detail = await loadSePersonDetail(COMPANY);
    // 0.72 and 0.64 join the merged person to the Wikidata person: two Merges the
    // reviewer can click. 0.93 is already merged (the fold did it), the 0.66 pair
    // names an observation no published person holds.
    expect(detail?.possibleMatches).toEqual([
      {
        personKeyA: MERGED_KEY, personKeyB: WIKI_KEY,
        nameA: "Anna Svensson", nameB: "Carl von Essen",
        confidence: 0.72, reason: "same household",
      },
      {
        personKeyA: MERGED_KEY, personKeyB: WIKI_KEY,
        nameA: "Anna Maria Svensson", nameB: "Carl von Essen",
        confidence: 0.64, reason: "same surname",
      },
    ]);
  });

  it("keeps a pair inside one person out of the band, and resolves a side through active persons only", async () => {
    // One person holding all three observations: every pair is internal now, so there
    // is nothing to merge -- and the 0.93 pair is still the record of what joined it.
    const whole = main({
      sources: ["bolagsverket", "esef", "wikidata"],
      slots: ["uid-1:sig-1", "doc-9:cand-1", "Q1:P169:Q7"],
      normalized_ids: ["n1", "n2", "n3"],
      member_sources: ["bolagsverket", "esef", "wikidata"],
      member_slots: ["uid-1:sig-1", "doc-9:cand-1", "Q1:P169:Q7"],
      member_names: ["Anna Svensson", "Anna Maria Svensson", "Carl von Essen"],
      member_birth_years: ["1975", "", ""], member_wikidata_ids: ["", "", "Q7"],
      member_data: ["{}", "{}", "{}"],
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [whole] : answer(sql),
    );
    const one = await loadSePersonDetail(COMPANY);
    expect(one?.possibleMatches).toEqual([]);
    expect(one?.published[0]?.matchedBy).toEqual([CALL_NAME_MATCH]);

    // The Wikidata person withdrawn: n3 has no ACTIVE owner, so a Merge naming it
    // would name a key nothing publishes any more.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL
        ? [MERGED_ROW, { ...WIKI_ROW, active: 0, inactive_reason: "withdrawn" }]
        : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.possibleMatches).toEqual([]);
  });
```

- [ ] **Step 6: Run the loader tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-company-person-entity.server.test.ts --reporter=dot
```
Expected: FAIL — `PERSON_MATCH_SQL` is not exported, so the whole file errors on import.

- [ ] **Step 7: Add the read and the derivations**

Four edits to `corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts`.

**(a)** After the `~/lib/se-person-fields` import block, add:

```ts
import {
  isMatch,
  isPossibleMatch,
  POSSIBLE_MATCH_FLOOR,
  type SePersonMatch,
  type SePersonMatchRow,
  type SePersonPossibleMatch,
} from "~/lib/se-person-match";
```

**(b)** The three interface fields. In `SePersonMember`, after `precedence`:

```ts
  /** The strongest pair at or above the threshold that names this observation, `null`
   * when the model did not join it (spec section 5's member badge). */
  match: SePersonMatch | null;
```

In `SePersonPublished`, after `rules`:

```ts
  /** The pairs at or above the threshold that joined THIS person, strongest first --
   * `fold.py::pairs_within` read back out of the pair table. */
  matchedBy: SePersonMatch[];
```

In `SePersonDetail`, after `precedence`:

```ts
  /** Pairs in `[POSSIBLE_MATCH_FLOOR, MATCH_THRESHOLD)` whose two sides sit in two
   * DIFFERENT active persons: what the possible-matches card offers a Merge for. */
  possibleMatches: SePersonPossibleMatch[];
```

**(c)** After `PERSON_PRECEDENCE_SQL`, the seventh read:

```ts
/**
 * The LLM's scored pairs for this company (spec 2026-09-11 section 5), certified by the
 * company's own state row. The join on `input_hash` is the same one the fold makes
 * (`batch.py::match_pairs_sql`): the state table holds one row per company, so a pair
 * left behind by an input the company no longer has -- a candidate list that changed,
 * a pair the model stopped scoring -- is simply never read again. `error = ''` for the
 * same reason the fold has it: a company whose last call failed certifies nothing.
 *
 * The floor is `POSSIBLE_MATCH_FLOOR`, not the threshold: the band between the two is
 * exactly what the possible-matches card offers, and the pairs above it are what the
 * member badges report. Only the nine columns the tab renders are read -- the model,
 * the prompt version and the stamp of a MERGED person are already in its
 * `data.llm_match`, which the panel reads from the row it has.
 */
export const PERSON_MATCH_SQL = `SELECT
  p.company_id AS company_id, toString(p.candidate_a) AS candidate_a,
  toString(p.candidate_b) AS candidate_b,
  arrayMap(x -> toString(x), p.members_a) AS members_a,
  arrayMap(x -> toString(x), p.members_b) AS members_b,
  p.name_a AS name_a, p.name_b AS name_b, toFloat64(p.confidence) AS confidence,
  p.reason AS reason
FROM corpscout.se_company_person_match AS p FINAL
INNER JOIN (
  SELECT company_id, input_hash
  FROM corpscout.se_company_person_match_state FINAL
  WHERE company_id = {companyId:String} AND error = ''
) AS s ON s.company_id = p.company_id AND s.input_hash = p.input_hash
WHERE p.company_id = {companyId:String} AND p.confidence >= ${POSSIBLE_MATCH_FLOOR}
ORDER BY p.confidence DESC, p.candidate_a, p.candidate_b`;
```

**(d)** The derivations. Above `membersOf`, add:

```ts
/**
 * The pairs at or above the threshold that joined THIS person, `fold.py::pairs_within`
 * in TypeScript: both sides must have at least one member among the row's normalized
 * ids. A pair with one side only is a pair a split rule pulled apart, or one reaching
 * into another person -- neither is something this person was merged by.
 */
function matchesWithin(row: SePersonRow, pairs: readonly SePersonMatchRow[]): SePersonMatch[] {
  const ids = new Set(row.normalized_ids);
  return pairs
    .filter(
      (pair) =>
        isMatch(pair.confidence) &&
        pair.members_a.some((id) => ids.has(id)) &&
        pair.members_b.some((id) => ids.has(id)),
    )
    .map((pair) => ({
      nameA: pair.name_a,
      nameB: pair.name_b,
      confidence: pair.confidence,
      reason: pair.reason,
      members: [...pair.members_a, ...pair.members_b],
    }))
    .sort((a, b) => b.confidence - a.confidence);
}

/**
 * The band's pairs the reviewer can actually act on: both sides resolve to an ACTIVE
 * published person, and to two different ones. Active only, because a Merge names
 * person keys -- a withdrawn row keeps the member arrays of the key it published under
 * before, so resolving through it would offer a Merge onto a key nothing publishes.
 * A pair inside one person is already one person, and a pair naming a normalized id no
 * published person holds has nothing to merge. Strongest first.
 */
function possibleMatchesOf(
  mainRows: readonly SePersonRow[],
  pairs: readonly SePersonMatchRow[],
): SePersonPossibleMatch[] {
  const owner = new Map<string, string>();
  for (const row of mainRows) {
    if (row.active !== 1) continue;
    for (const id of row.normalized_ids) owner.set(id, row.person_key);
  }
  const keyOf = (members: readonly string[]): string | undefined => {
    for (const id of members) {
      const key = owner.get(id);
      if (key !== undefined) return key;
    }
    return undefined;
  };
  const found: SePersonPossibleMatch[] = [];
  for (const pair of pairs) {
    if (!isPossibleMatch(pair.confidence)) continue;
    const personKeyA = keyOf(pair.members_a);
    const personKeyB = keyOf(pair.members_b);
    if (personKeyA === undefined || personKeyB === undefined) continue;
    if (personKeyA === personKeyB) continue;
    found.push({
      personKeyA,
      personKeyB,
      nameA: pair.name_a,
      nameB: pair.name_b,
      confidence: pair.confidence,
      reason: pair.reason,
    });
  }
  return found.sort((a, b) => b.confidence - a.confidence);
}
```

`membersOf` takes the person's own matches and gives each member the strongest pair
naming it — its signature gains a fifth parameter and its returned object one field:

```ts
function membersOf(
  row: SePersonRow,
  normalizedBySlot: Map<string, SePersonNormalizedRow>,
  rawBySlot: Map<string, SePersonRawRow>,
  precedenceOf: (source: string) => number,
  matchedBy: readonly SePersonMatch[],
): SePersonMember[] {
```

and inside the returned object, after `precedence: precedenceOf(source),`:

```ts
      // `matchedBy` is confidence-descending, so the first hit is the strongest.
      match: matchedBy.find((pair) => pair.members.includes(normalizedId)) ?? null,
```

Finally `loadSePersonDetail`: the seventh read, the per-person derivation and the
detail field.

```ts
  const [mainRows, history, normalizedRows, rawRows, rules, precedence, matchRows] =
    await Promise.all([
      chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
      chQuery<SePersonHistoryRow>(PERSON_HISTORY_SQL, { companyId }),
      chQuery<SePersonNormalizedRow>(PERSON_NORMALIZED_SQL, { companyId }),
      chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId }),
      chQuery<SePersonRuleRow>(PERSON_RULES_SQL, { companyId }),
      chQuery<SePersonPrecedenceRow>(PERSON_PRECEDENCE_SQL, { companyId }),
      chQuery<SePersonMatchRow>(PERSON_MATCH_SQL, { companyId }),
    ]);
```

```ts
  const published = mainRows.map((row) => {
    const matchedBy = matchesWithin(row, matchRows);
    const members = membersOf(row, normalizedBySlot, rawBySlot, precedenceOf, matchedBy);
    return {
      row,
      members,
      roles: row.role_codes.map((code, index) => ({
        code,
        year: row.role_years[index] ?? 0,
        sources: row.role_sources[index] ?? [],
      })),
      spellingReason: spellingReason(row, members),
      rules: rulesFor(rules, row, mainRows, history),
      matchedBy,
    };
  });
```

and in the returned object, after `precedence,`:

```ts
    possibleMatches: possibleMatchesOf(mainRows, matchRows),
```

- [ ] **Step 8: Run the loader tests to verify they pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-company-person-entity.server.test.ts tests/se-person-match.test.ts --reporter=dot
```
Expected: PASS, both files (25 tests in the entity file, 5 in the match file).

- [ ] **Step 9: Give the two other `SePersonDetail` literals their new field**

`SePersonDetail` and `SePersonPublished` gained required fields, so the two hand-built
literals must carry them or `tsc` fails. In
`corpscout/services/backoffice/app/routes/admin-se-company-person.tsx:27`:

```ts
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [],
  possibleMatches: [], foldPending: false,
};
```

In `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx`, the same
three fixtures (the render assertions come in Task 2):

```ts
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [],
  possibleMatches: [], foldPending: false,
};
```

each of the two members in `published.members` gains `match: null` as its last field,
and `published` itself gains `matchedBy: []` after `rules: []`.

- [ ] **Step 10: Typecheck and run the three files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
pnpm typecheck
CI=1 pnpm exec vitest run tests/se-person-match.test.ts tests/se-company-person-entity.server.test.ts tests/admin-se-company-person.test.tsx --reporter=dot
```
Expected: typecheck silent (exit 0); all three files PASS.

- [ ] **Step 11: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
feat(backoffice): read the LLM match pairs into the person detail

PERSON_MATCH_SQL is the People tab's seventh FINAL read: the company's
scored pairs at or above 0.5, certified by its match state row's input_hash
exactly as batch.py::match_pairs_sql certifies them. The loader derives
three shapes from them -- matchedBy per published person (the pairs at or
above 0.8 whose two sides both sit inside its normalized ids, fold.py's
pairs_within read back), match per member, and possibleMatches (the band's
pairs across two ACTIVE persons, strongest first). The constants, the row
type and the pure helpers live in the client-safe app/lib/se-person-match.ts.

Spec 2026-09-11 section 5, slice 2.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/backoffice/app/lib/se-person-match.ts \
        corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts \
        corpscout/services/backoffice/app/routes/admin-se-company-person.tsx \
        corpscout/services/backoffice/tests/se-person-match.test.ts \
        corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts \
        corpscout/services/backoffice/tests/admin-se-company-person.test.tsx
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 2: The badge, the `llm_match` list and the Possible matches card

**Files:**
- Modify: `corpscout/services/backoffice/app/components/admin/se-person-workspace.tsx` (`MemberEntry` ~lines 1008-1060, `PersonPanel` ~lines 1062-1270, `SePersonWorkspace` ~lines 1284-1399, and one new component)
- Modify: `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx`

**Interfaces:**
- Consumes from Task 1: `formatConfidence`, `matchNote`, `parseLlmMatch`, `stripLlmMatch` and `type SePersonPossibleMatch` from `~/lib/se-person-match`; `member.match: SePersonMatch | null`, `detail.possibleMatches: SePersonPossibleMatch[]` from the loader. (`entry.matchedBy` is NOT rendered: the panel's list comes from the person's own `data.llm_match`, which is what the fold recorded at merge time — `matchedBy` exists for the member lookup and for a future column.)
- Produces: nothing another task imports. The card posts the `merge` intent that `app/lib/se-person-decision-form.ts` already parses: one `intent`, two `person_key` values, one `note`.

- [ ] **Step 1: Write the failing component test**

In `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx`, add two fixtures
after `const detail: SePersonDetail = { ...EMPTY_DETAIL, published: [published] };`:

```tsx
/** The strongest pair naming the Bolagsverket observation, as the loader derives it. */
const MATCH = {
  nameA: "Anna Svensson", nameB: "Anna Maria Svensson", confidence: 0.93,
  reason: "call name", members: ["n1", "n2"],
};
/** What `fold.py::_with_llm_match` wrote onto the published row. */
const FOLD_DATA =
  '{"llm_match":{"model":"deepseek-v4-flash","pairs":[{"a":"Anna Svensson","b":"Anna Maria Svensson","confidence":0.93,"reason":"call name"}],"prompt_version":"se-person-match-v1"},"role_kind":"board_member"}';
```

and one test after "renders the workspace: the persons, their sources, roles and the fold state":

```tsx
  it("badges the matched member, reads the llm_match record as a list, and offers a Merge for a possible match", () => {
    const matched: SePersonDetail = {
      ...detail,
      published: [
        {
          ...published,
          row: { ...row, data: FOLD_DATA },
          members: [{ ...published.members[0], match: MATCH }, published.members[1]],
          matchedBy: [MATCH],
        },
      ],
      possibleMatches: [
        {
          personKeyA: KEY, personKeyB: OTHER, nameA: "Anna Maria Svensson",
          nameB: "Carl von Essen", confidence: 0.64, reason: "same surname",
        },
      ],
    };
    const html = render(
      <SePersonWorkspace
        companyId={COMPANY} detail={matched} roleOptions={ROLE_OPTIONS}
        selectedKey={KEY} result={null}
      />,
    );
    // The badge and its reason. Asserted in two pieces, not as one string: React puts
    // the score in its own text node, so the rendered markup may separate them.
    expect(html).toContain("matched by LLM");
    expect(html).toContain("0.93");
    expect(html).toContain('title="call name"');
    // The fold's record reads as a list -- the model and the prompt version included --
    // and the raw key never reaches the Data block, while the rest of `data` still does.
    expect(html).toContain("deepseek-v4-flash");
    expect(html).toContain("se-person-match-v1");
    expect(html).not.toContain("llm_match");
    expect(html).toContain("role_kind");
    // The card and one Merge, posting the EXISTING merge intent with both keys and the
    // note spec section 5 prescribes.
    expect(html).toContain("Possible matches");
    expect(html).toContain("same surname");
    expect(html).toContain("0.64");
    expect(html).toContain(`type="hidden" name="person_key" value="${KEY}"`);
    expect(html).toContain(`type="hidden" name="person_key" value="${OTHER}"`);
    expect(html).toContain('name="note" value="LLM match 0.64: same surname"');
    // A company with no pairs shows no card at all.
    expect(
      render(
        <SePersonWorkspace
          companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS}
          selectedKey={KEY} result={null}
        />,
      ),
    ).not.toContain("Possible matches");
  });
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/admin-se-company-person.test.tsx --reporter=dot
```
Expected: FAIL on the new test — `expect(html).toContain("matched by LLM")` finds nothing
(the other tests in the file still pass).

- [ ] **Step 3: Import the helpers in the workspace**

In `corpscout/services/backoffice/app/components/admin/se-person-workspace.tsx`, add an
import after the `~/lib/se-person-fields` block:

```ts
import {
  formatConfidence,
  matchNote,
  parseLlmMatch,
  stripLlmMatch,
  type SePersonPossibleMatch,
} from "~/lib/se-person-match";
```

- [ ] **Step 4: Badge the matched member**

In `MemberEntry`, inside the first `<div className="flex flex-wrap items-center gap-2">`,
between the precedence badge and the re-fold badge:

```tsx
        <Badge variant="outline">precedence {member.precedence}</Badge>
        {member.match === null ? null : (
          // The fold merged this observation into the person because the model scored
          // it against another one; the reason is the model's own sentence.
          <Badge variant="outline" title={member.match.reason}>
            matched by LLM · {formatConfidence(member.match.confidence)}
          </Badge>
        )}
        {member.refoldPending ? <Badge variant="outline">re-fold pending</Badge> : null}
```

- [ ] **Step 5: Render the `llm_match` record as a list, and keep it out of the JSON block**

In `PersonPanel`, after `const order = namePrecedence(precedence);`:

```tsx
  // The fold's own record of what it merged (`fold.py::_with_llm_match`), read from the
  // published row rather than from the live pair table: this is what was true when the
  // person was published. The JSON block below shows `data` WITHOUT it -- one fact is
  // not rendered twice, and a reviewer reading JSON is reading the sources' extras.
  const llmMatch = entry === null ? null : parseLlmMatch(entry.row.data);
  const dataWithoutMatch = entry === null ? "" : stripLlmMatch(entry.row.data);
```

Then a new `<section>` immediately after the Members section's closing `</section>`:

```tsx
          {llmMatch === null ? null : (
            <section>
              <h3 className="text-muted-foreground text-xs uppercase tracking-wide">
                LLM match
              </h3>
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {llmMatch.pairs.map((pair, index) => (
                  <li key={`${pair.a}-${pair.b}-${index}`}>
                    <span>
                      {pair.a} ↔ {pair.b}
                    </span>
                    <span className="text-muted-foreground ml-2 text-xs">
                      {formatConfidence(pair.confidence)}
                      {pair.reason === "" ? "" : ` · ${pair.reason}`}
                    </span>
                  </li>
                ))}
              </ul>
              <p className="text-muted-foreground mt-1 text-xs">
                {llmMatch.model}
                {llmMatch.promptVersion === "" ? "" : ` · ${llmMatch.promptVersion}`}
              </p>
            </section>
          )}
```

and the Data section reads the stripped text — two words changed, nothing else:

```tsx
            {hasData(dataWithoutMatch) ? (
              <pre className="bg-muted mt-2 overflow-x-auto rounded-md p-2 text-xs">
                {prettyJson(dataWithoutMatch)}
              </pre>
            ) : (
              <p className="mt-1 text-sm">none</p>
            )}
```

- [ ] **Step 6: The Possible matches card**

Add the component directly after `PersonsCard` (before `DraftsCard`):

```tsx
/**
 * Spec 2026-09-11 section 5: the pairs the model scored between the floor and the
 * merge threshold whose two sides sit in two different published persons. Nothing was
 * merged by them -- the reviewer decides, and the Merge writes the SAME merge rule the
 * Persons card writes (no new intent, no new rule kind), which outranks the threshold
 * on the next fold. One `<Form>` per row: a single form with several submit buttons
 * would have to carry every row's keys at once.
 */
function PossibleMatchesCard({
  matches,
  busy,
}: {
  matches: readonly SePersonPossibleMatch[];
  busy: boolean;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Possible matches</CardTitle>
        <CardDescription>
          Pairs the model scored below the merge threshold, each between two published
          persons. Merge writes a merge rule; Fold now folds them into one person.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="flex flex-col gap-2 text-sm">
          {matches.map((match, index) => (
            <li
              key={`${match.personKeyA}-${match.personKeyB}-${index}`}
              className="flex flex-wrap items-center gap-2"
            >
              <span className="flex-1">
                {match.nameA === "" ? EMPTY_VALUE : match.nameA} ↔{" "}
                {match.nameB === "" ? EMPTY_VALUE : match.nameB}
                <span className="text-muted-foreground ml-2 text-xs">
                  {formatConfidence(match.confidence)}
                  {match.reason === "" ? "" : ` · ${match.reason}`}
                </span>
              </span>
              <Form method="post">
                <input type="hidden" name="intent" value="merge" />
                <input type="hidden" name="person_key" value={match.personKeyA} />
                <input type="hidden" name="person_key" value={match.personKeyB} />
                <input
                  type="hidden"
                  name="note"
                  value={matchNote(match.confidence, match.reason)}
                />
                <Button type="submit" size="sm" variant="outline" disabled={busy}>
                  Merge
                </Button>
              </Form>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 7: Put the card under the persons list**

In `SePersonWorkspace`'s left column, between `<PersonsCard ... />` and the drafts block:

```tsx
        {detail.possibleMatches.length === 0 ? null : (
          <PossibleMatchesCard matches={detail.possibleMatches} busy={busy} />
        )}
```

The success alert needs nothing: `SUCCESS_COPY.merge` ("Merge rule written. Fold now folds
them into one person.") already answers this form, because it posts the `merge` intent.

- [ ] **Step 8: Run the component tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/admin-se-company-person.test.tsx --reporter=dot
pnpm typecheck
```
Expected: PASS, 10 tests in the file (the 9 it had plus this one); typecheck silent.

- [ ] **Step 9: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
feat(backoffice): show the LLM matches on the People tab

A member the model joined carries a "matched by LLM · 0.93" badge with the
reason on hover; the person panel reads the fold's data.llm_match record as
a short list (name ↔ name · confidence · reason, model and prompt version
underneath) and shows the rest of `data` without that key; and a Possible
matches card under the persons list offers the 0.5-0.8 band's cross-person
pairs with a Merge that posts the existing merge intent, both person keys
and the note "LLM match 0.64: <reason>".

Spec 2026-09-11 section 5, slice 2.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/backoffice/app/components/admin/se-person-workspace.tsx \
        corpscout/services/backoffice/tests/admin-se-company-person.test.tsx
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 3: The route, and the whole suite green

**Files:**
- Modify: `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx`
- (Check only, no edit expected: `corpscout/services/backoffice/app/routes/admin-se-company-person.tsx` — Task 1 already gave `EMPTY_DETAIL` its `possibleMatches: []`, and the action's intent switch is untouched.)

**Interfaces:**
- Consumes: the `merge` intent of `app/lib/se-person-decision-form.ts`
  (`{ intent: "merge"; personKeys: string[]; note: string }`, keys deduplicated and
  sorted by the parser) and `mergeSePersons(companyId, decision)` in the entity server.
- Produces: nothing. This task is the gate: the route needs no new code, and the suite's
  counts move only by the tests this slice added.

- [ ] **Step 1: Write the failing route test**

The card's Merge is only worth shipping if the post it makes survives the parser — the
note it carries is model text, and `parseSePersonDecision` runs every note through
`noteError`. Add this test to `corpscout/services/backoffice/tests/admin-se-company-person.test.tsx`
after "dispatches every intent to its function with the parsed decision":

```tsx
  it("takes a possible match's Merge through the existing merge intent, note and all", async () => {
    // Exactly what PossibleMatchesCard posts: no new intent, two person keys, and the
    // note `matchNote` built. A note the parser refused would read to the reviewer as
    // a broken button, so the whole post is pinned here.
    await action({
      request: post({ intent: "merge", note: "LLM match 0.64: same surname" }, [
        ["person_key", KEY],
        ["person_key", OTHER],
      ]),
      params: { companyId: COMPANY },
    } as never);
    expect(server.mergeSePersons).toHaveBeenCalledWith(COMPANY, {
      intent: "merge",
      personKeys: [KEY, OTHER],
      note: "LLM match 0.64: same surname",
    });
  });
```

- [ ] **Step 2: Run it**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/admin-se-company-person.test.tsx --reporter=dot
```
Expected: PASS — **this one goes green immediately**, and that is the point: it proves
the route and the parser already carry the card's post, so no route change is needed. If
it fails, the card is posting something the parser refuses; fix the card (Task 2), never
the parser (Global Constraints: no new intent).

- [ ] **Step 3: Read the route back and confirm it needs nothing else**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
rg -n "possibleMatches|EMPTY_DETAIL|intent ===" app/routes/admin-se-company-person.tsx
```
Expected: `EMPTY_DETAIL` carries `possibleMatches: []` (Task 1, step 9) and the action's
`decision.intent === "merge"` branch is unchanged. The loader hands the detail through
untouched, so nothing else in the route knows about matches. The route test's
`expect(missing.detail).toEqual(EMPTY_DETAIL)` is what would catch the two `EMPTY_DETAIL`
literals drifting apart.

- [ ] **Step 4: Typecheck and run the whole suite**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
pnpm typecheck
CI=1 pnpm exec vitest run --reporter=dot
```
Expected (about 3 minutes): **126 files** (the one new `tests/se-person-match.test.ts`)
and **1,349 tests** — the baseline 1,340 plus 5 in the match module, 2 in the entity
loader, 1 component test and 1 route test, with nothing removed. The only failures are
the two pre-existing files, `tests/queries.server.test.ts` and
`tests/admin-se-company-esef.test.tsx` (plus, under load, a timeout in
`tests/esef-financial-reports.server.test.ts`). **Do not fix them.** If the count differs
from 1,349, find out why before committing: a test file that failed to import counts as
one failed file, not as missing tests.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
test(backoffice): pin the possible-match Merge to the existing merge intent

The card's post -- intent=merge, two person_key values and the generated
"LLM match 0.64: <reason>" note -- reaches mergeSePersons through the
route's unchanged intent switch and the decision parser's note validation.
No new intent, no route change.

Spec 2026-09-11 section 5, slice 2.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/backoffice/tests/admin-se-company-person.test.tsx
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 4: Owner smoke, the shipped record, the ticks

**The owner runs this task; a task subagent never touches prod or the owner's dev
server.** Every step is a read-only `SELECT`, a page load, one Merge click, one Fold now,
or a doc edit. The backoffice has no server deployment (memory `backoffice-runs-locally`):
"deploy" is the merge to `main`, and the smoke happens on the owner's local dev server
reading prod ClickHouse.

**Files:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-person-llm-matching-design.md` (the shipped record, step 6) and this plan (the ticks).

- [ ] **Step 1: Review the branch and merge it**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info diff main...se-person-llm-match-2
```
Read it end to end, then merge to `main`. If the main checkout sits on another branch,
merge through a worktree that has `main` checked out (memory `se-worktree-deploy-recipe`).

- [ ] **Step 2: Start the dev server on the main checkout**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice
pnpm dev
```
It serves `http://localhost:5183` (`vite.config.ts` pins the port) against prod
ClickHouse.

- [ ] **Step 3: Swedbank — the badges and the record**

Open `http://localhost:5183/admin/se/company/5020177753/people`.

Expect: the page is 200 and lists Swedbank's 129 persons. Slice 1's fold left **10
Ratsit+ESEF merges** on this company, so click through several of the merged persons and
expect, on the panel:
- a `matched by LLM · 0.9x` badge on each observation the model joined, beside
  `precedence`, with the model's reason on hover;
- an **LLM match** section under Members listing `name ↔ name · confidence · reason`,
  with `deepseek-v4-flash · se-person-match-v1` under it;
- a **Data** block that no longer contains the `llm_match` key, while the sources' own
  extras (`role_kind`, `title`, `age`, …) are still there.

- [ ] **Step 4: A company with a possible match — one Merge, then Fold now**

Find candidates (the query approximates the loader by taking each side's FIRST member; the
tab itself is the authority on which pairs are cross-person):

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
WITH banded AS (
    SELECT p.company_id AS company_id, p.members_a AS members_a, p.members_b AS members_b,
           p.confidence AS confidence, p.name_a AS name_a, p.name_b AS name_b
    FROM corpscout.se_company_person_match AS p FINAL
    INNER JOIN (
        SELECT company_id, input_hash
        FROM corpscout.se_company_person_match_state FINAL
        WHERE error = ''
    ) AS s ON s.company_id = p.company_id AND s.input_hash = p.input_hash
    WHERE p.confidence >= 0.5 AND p.confidence < 0.8
),
owners AS (
    SELECT company_id, arrayJoin(normalized_ids) AS normalized_id,
           toString(person_key) AS person_key
    FROM corpscout.se_company_person FINAL
    WHERE active = 1 AND company_id IN (SELECT DISTINCT company_id FROM banded)
)
SELECT b.company_id, round(b.confidence, 2) AS confidence, b.name_a, b.name_b
FROM banded AS b
INNER JOIN owners AS oa ON oa.company_id = b.company_id AND oa.normalized_id = b.members_a[1]
INNER JOIN owners AS ob ON ob.company_id = b.company_id AND ob.normalized_id = b.members_b[1]
WHERE oa.person_key != ob.person_key
ORDER BY b.confidence DESC
LIMIT 20
SQL
```
Expect rows: prod holds 9,529 pairs in the band (spec section 10 item 1).

Open one of those companies' People tab. Expect a **Possible matches** card under the
persons list with `name_a ↔ name_b · 0.6x · reason` rows. Click one `Merge`:
- the page answers "Merge rule written. Fold now folds them into one person.";
- the panel of either person now shows a `merge` rule with the note
  `LLM match 0.6x: <reason>`;
- "Fold pending" is up.

Press **Fold now**, wait for the poller, reload: the two persons are one, the merged
person's Members list holds both observations, and the pair is gone from the Possible
matches card (it is now inside one person). Confirm in ClickHouse that the rule landed as
a normal reviewer merge — no new kind:

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT kind, active, note, created_by, created_at
FROM corpscout.se_company_person_rule FINAL
WHERE company_id = '<the company you merged in>'
ORDER BY created_at DESC
LIMIT 5
SQL
```
Expect one `merge` row, `active = 1`, `created_by = 'backoffice'`, the `LLM match …` note.

- [ ] **Step 5: The People list is untouched**

Open `http://localhost:5183/admin/se/people`. Expect 200, the same columns as before (spec
section 5: "The People list gets no new column; the Source filter is unchanged"), and the
Source filter still working.

- [ ] **Step 6: The shipped record, the ticks, the memory**

1. Append the shipped record to spec section 10 item 2, in the shape item 1 uses: this
   plan's filename, the merge commit, what shipped (`PERSON_MATCH_SQL` and the three
   derived shapes, the member badge, the `llm_match` list and the stripped Data block,
   the Possible matches card posting the existing `merge` intent), the three task gates
   and any review findings fixed, the suite counts before and after, and what the smoke
   of steps 3 to 5 showed (Swedbank's badged merges, the company merged through the card,
   the rule row, the list still 200). Name every ruling this plan made — the self-review's
   list below is the source.
2. Tick this plan's checkboxes.
3. Update the memory file `se-person-entity.md`: the People tab now shows the LLM's
   decisions, the band `[0.5, 0.8)` is the reviewer's through the possible-matches card,
   the Merge writes an ordinary reviewer merge rule, and `llm_match` is fold-owned and
   read-only in the backoffice (`RESERVED_DATA_KEYS` unchanged).

---

## Self-review

### Spec coverage (section 5, the binding text, plus section 6's backoffice bullet)

| spec section 5 | where |
| --- | --- |
| "a seventh FINAL read, `PERSON_MATCH_SQL` over `se_company_person_match` joined to the company's state row on `input_hash`, returning every pair with `confidence >= 0.5`" | Task 1, steps 5-7 (the SQL, its pins, the `Promise.all`) |
| "a member whose `normalized_id` is in a pair at or above the threshold shows a `matched by LLM · 0.93` badge beside the precedence badge, with the reason on hover" | Task 1 (`matchesWithin` + `membersOf`'s `match`), Task 2 step 4 (the badge and its `title`) |
| "the card's `llm_match` key is rendered as a small list, not as raw JSON" | Task 2 step 5 (`parseLlmMatch` list section, `stripLlmMatch` on the Data block) |
| "a 'Possible matches' panel under the persons list: the pairs between 0.5 and the threshold whose two candidates sit in different published persons" | Task 1 (`possibleMatchesOf`), Task 2 steps 6-7 |
| "each as `name_a ↔ name_b · 0.64 · reason` with a `Merge` button that submits the existing `merge` intent with the two person keys pre-filled and the note `LLM match 0.64: <reason>`" | Task 2 step 6 (the row and the form), Task 1 (`matchNote`), Task 3 (the post through the action) |
| "Pairs already inside one person are not listed" | Task 1 `possibleMatchesOf` (`personKeyA === personKeyB` dropped), pinned by the second new loader test |
| "The People list gets no new column; the Source filter is unchanged" | no task touches `se-people-*`; Task 4 step 5 loads the list to prove it |
| section 6: "Backoffice vitest: loader stub for the match query, the badge, the panel and its merge submission" | Task 1 (`answer(sql)` branch + derived shapes), Task 2 (badge, panel, form), Task 3 (the submission through `action`) |
| section 4's "the People tab's possible-matches panel is the reviewer's way to merge a pair the LLM scored below the threshold" | Task 2 step 6 (the reviewer merge rule outranks the threshold on the next fold — the card's own description says so) |

Sections 1, 3.x, 7, 8 and 10 item 1 are slice 1's, already shipped; nothing here re-opens
them. No spec requirement of section 5 is without a task.

### Decisions this plan made where the spec left room

1. **`possibleMatches` resolves a side only through ACTIVE published rows.** A withdrawn
   row keeps the member arrays of the key it published under before (`fold.py:754-765`),
   so a pair could otherwise "sit in two different persons" where one of them is a dead
   key — and `mergeSePersons` would happily write a rule naming it, because its guard is
   "is this key in the main table", not "is it active". Badges (`matchedBy`) do NOT use
   the ownership map: they are per-row containment, so an inactive person still explains
   what joined it.
2. **`matchedBy` mirrors `fold.py::pairs_within`: at least one member of EACH side inside
   the row.** Not "every member", which would drop a pair whose other side a split rule
   pulled out — the fold takes the same view, and the two must agree or the badge would
   claim a merge the fold did not make.
3. **The panel's list comes from `data.llm_match`, the member badge from the live pair
   read.** They can disagree, and that is information: `data.llm_match` is what the fold
   recorded when it published the person, the pair table is what the model says now
   (a re-match between folds). The spec asks for both ("the card's `llm_match` key is
   rendered as a small list" and "a member … shows a badge"), so neither is derived from
   the other.
4. **`PERSON_MATCH_SQL` reads nine columns, not fifteen.** `model`, `prompt_version`,
   `input_hash`, `matched_at`, `source_a` and `source_b` are not selected: the panel gets
   the model and the prompt version from the person's own `data.llm_match`, the hash is
   spent inside the join, and nothing renders the sources or the stamp. `candidate_a`
   and `candidate_b` stay — they order the read and identify a pair.
5. **`llm_match` is NOT added to `RESERVED_DATA_KEYS`** (a Global Constraint, spelled out
   there): that list is "keys the reviewer's sheet may not post back", stripped by
   `ownData()`/`reviewerData()` from a *draft or member* `data`, and neither of those
   paths ever carries a published row's `data` — `initialFromRow` deliberately opens a
   Correct with `data` empty. So `llm_match` is simply never on a write path; it is
   stripped from the *display* of `data` by `stripLlmMatch` and rendered as its own
   section instead.
6. **`matchNote` flattens and clamps the reason.** The spec's note is
   `LLM match 0.64: <reason>`, but a reason is model text and every note goes through
   `noteError` (500 characters, no control characters except newline and tab in the
   sheet's path, and the rule note takes the same check). An unclamped note would make
   the Merge button fail for exactly the verbose reasons a reviewer most wants to read.
   The score keeps two decimals via `formatConfidence`, so the note reads as the spec
   writes it.
7. **One `<Form>` per possible-match row**, rather than one card-wide form with per-row
   submit buttons: a shared form would have to carry every row's `person_key` values at
   once, and the parser deduplicates and sorts them — a click would merge everything
   listed. `PersonsCard`'s single form is right for ITS job (tick many, merge once).
8. **Duplicate person-pairs are listed once per scored pair.** Two pairs can name the
   same two persons (a person is several candidates); each keeps its own reason and
   confidence, and merging on either writes the same rule. Collapsing them would hide a
   reason the reviewer is being asked to judge.
9. **`isMatch`/`isPossibleMatch` are the only band comparisons.** The loader calls them
   instead of open-coding `>= 0.8` / `>= 0.5 && < 0.8`, so a threshold edit is one
   constant in one client-safe file — matching the spec's "a threshold change is a
   constant edit and a re-fold, not a re-match".
10. **The route test for the card's post is expected to pass the moment it is written**
    (Task 3, step 2). It is a regression pin, not a red-green step: the whole point of
    spec section 5's "existing `merge` intent" is that the route needs no change, and a
    failing version of this test would mean the card posts something the parser refuses.
11. **The badge assertions are split into two `toContain` calls** (`"matched by LLM"` and
    `"0.93"`): React renders the interpolated score as its own text node, and asserting
    the joined string would pin a renderer detail rather than the badge.

### Placeholder scan

No `TBD`, `TODO`, "implement later", "similar to Task N", "add appropriate error
handling" or elided code remains. Every code step shows the code to write, every SQL step
shows the statement, every test step shows the assertions, and every command shows what to
expect. The one bracketed value in the plan, `'<the company you merged in>'` in Task 4
step 4, is an owner-supplied id from the step above it, in a step the owner runs by hand.

### Name consistency

Checked across tasks: `MATCH_THRESHOLD` / `POSSIBLE_MATCH_FLOOR` / `LLM_MATCH_KEY`,
`SePersonMatchRow` / `SePersonMatch` / `SePersonPossibleMatch` / `SeLlmMatchPair` /
`SeLlmMatchBlock`, `isMatch` / `isPossibleMatch` / `formatConfidence` / `matchNote` /
`parseLlmMatch` / `stripLlmMatch` (Task 1 → Tasks 2, 3); `PERSON_MATCH_SQL` /
`matchesWithin` / `possibleMatchesOf` / `membersOf` (Task 1); the three field names
`member.match`, `entry.matchedBy`, `detail.possibleMatches` (Task 1 → Task 2);
`PossibleMatchesCard` (Task 2). The fixture names are per file and do not collide:
`FOLD_DATA` appears in both `tests/se-person-match.test.ts` and
`tests/admin-se-company-person.test.tsx` as that file's own constant, `MATCH_ROWS` /
`CALL_NAME_MATCH` live only in `tests/se-company-person-entity.server.test.ts`, `MATCH`
only in the component test. The form field names are the parser's own and are never
renamed: `intent`, `person_key`, `note`.
