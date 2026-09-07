# SE Company Address Slice 3: Backoffice Address Tab — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the backoffice's Address tab with one over the new address entity: the published addresses with their sources, parse and geocode detail, history, the reviewer's five actions (Remove, Reset to default, Add address, Correct, Fold now) and the fold poller.

**Architecture:** The tab mirrors the Info tab one-to-one. Two client-safe modules (`se-address-fields.ts` catalogue + validation, `se-address-decision-form.ts` FormData → decision) keep the route's client bundle free of server code; one server module (`se-company-address-entity.server.ts`) reads the six entity tables through `chQuery` and writes reviewer rows and hide rules with the same append-only, stamp-per-write pattern as `se-basic-info.server.ts`; two components (`se-address-workspace.tsx`, `se-address-edit-sheet.tsx`) render the two-thirds/one-third layout of spec section 8; the route file holds only `loader`, `action` and the component. The Dagster targeted fold gains one step: it normalizes the company's raw rows before folding, so a reviewer's draft parses on Fold now.

**Tech Stack:** React Router v7 (`corpscout/services/backoffice`, TypeScript, shadcn/ui, vitest, `npm run typecheck` = `react-router typegen && tsc`, `npm test` = `vitest run`), `@clickhouse/client` through `app/lib/clickhouse.server.ts`, Dagster GraphQL through `app/lib/dagster.server.ts`; Dagster 1.13.9 (`uv run --frozen --no-sync`) for Task 1.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, sections 3.1 to 3.6, 5.5, 8, 10. Section 8 is the binding text for every action; the two rulings in "Rulings" below are recorded there by Task 5.

## Global Constraints

- Backoffice commands run from `corpscout/services/backoffice`: `npm run typecheck` and `npx vitest run <files>` must pass before every commit that touches `app/` or `tests/`. Dagster commands run from `corpscout/services/dagster_v3` with `uv run --frozen --no-sync ...` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`; `uv run --frozen --no-sync dg check defs` before the Task 1 commit.
- The route file exports only `loader`, `action`, `meta` and the default component (see the comment at the top of `app/routes/admin-se-company-address.tsx`): any other export that touches a `.server` module breaks the production build. Client-safe modules never import a `.server` module.
- Every write is append-only: a new version row with `decided_by = 'backoffice'`, `source_run_id = 'backoffice'`, `extractor_version = 'backoffice-v1'`, one `stamp = clickhouseStamp(now)` per action used for every row and id of that action. Never delete or update.
- `suggestion_id` of a backoffice raw row is `sha256("<company_id>\n<source>\n<slot>\n<stamp>")` hex, exactly what the extractors compute in SQL (`address/suggestions.py::ADDRESS_TRAILING_SELECT_SQL`), with `stamp` the same `YYYY-MM-DD HH:MM:SS.mmm` string as `suggested_at`.
- Nullable ClickHouse String columns take `null`, never `''`, on writes; on reads every nullable value is collapsed to `''` so components never tell `""` from `null`. `FixedString(64)` columns are read through `toString(...)`.
- Sources and kinds come from the entity's catalogue: sources `scb, bolagsverket, ratsit, reviewer, reviewer_draft`; kinds `postal, visiting, visiting_or_postal, registered, workplace, unknown`; the reviewer's sheet offers `postal, visiting, visiting_or_postal, registered`.
- Validation (spec 8): a box or a street line, a five-digit postcode (`111 22` accepted, stored as `11122`), a city, lengths street 200 / care-of 200 / city 100 / note 500, plain text (no control characters), `kind` from the catalogue, country `SE` only.
- `reviewer_draft` rows never raise "Fold pending" and are never published; `foldPending` mirrors Dagster's selection (`batch._changed_company_ids`): a non-draft normalized row or a rule version newer than the newest `folded_at`, or normalized rows with no main row.
- The Dagster tables are fixed (migrations 000382 to 000387); no migration in this slice.
- Commit by explicit path only; never `git add -A`. Trailers, in this order, each on its own line at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`

## Rulings (controller, 2026-09-07)

1. **Remove on a mixed row.** Spec 8 gives the source-delivered case (hide rule) and the reviewer-typed case (tombstone the reviewer slot). A published row can carry both a reviewer member and source members. Ruling: Remove tombstones every `reviewer` member slot of the row AND inserts the hide rule when the row has any non-reviewer member; otherwise the source members would republish it on the next fold.
2. **Reviewer slots.** A new address gets slot `r<digits of the stamp>` (for example `r20260907203355123`); editing a draft keeps its slot (hidden field); Activate writes the `reviewer` row under the draft's slot, so the pair `(reviewer, slot)` and `(reviewer_draft, slot)` share one lineage.
3. **Targeted fold normalizes first.** Spec 8: "the targeted fold normalizes the company's raw rows before folding". `se_company_address_fold_companies` calls `normalize_companies(..., changed_only=True)` for its ids before `fold_companies`; the bucket fold does not (the weekly normalizes).
4. **The poller route is shared.** The Info tab's resource route `info/run/:runId` only reports a run's status; the Address tab's poller fetches the same route. No new route.
5. **Old modules stay until the cutover.** The rewrite leaves `app/lib/se-company-address.server.ts`, `app/components/admin/se-company-address.tsx`, `app/lib/se-address-review-form.ts` and their tests in place (the corrections queue page still reads the old ledger); only the route file and the route test change. Slice 4 deletes them.

---

## File map

| File | Responsibility |
| --- | --- |
| `dagster_v3/src/dagster_v3/defs/se_company/address/assets.py` (modify) | `se_company_address_fold_companies` normalizes the ids first; `targeted_fold(...)` helper. |
| `dagster_v3/tests/test_se_company_address_assets.py` (modify) | `targeted_fold` normalizes then folds, in that order, with the same ids. |
| `backoffice/app/lib/se-address-fields.ts` (create) | Catalogue, labels, `selectedAddressFromSearch`, `validateSeAddressInput`, `addressFoldPending`. |
| `backoffice/app/lib/se-address-decision-form.ts` (create) | `SeAddressDecision`, `parseSeAddressDecision`. |
| `backoffice/app/lib/clickhouse.server.ts` (modify) | `chInsertSeCompanyAddressSuggestions`, `chInsertSeCompanyAddressRules`. |
| `backoffice/app/lib/dagster.server.ts` (modify) | `SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET = "se_company_address_fold_companies"`. |
| `backoffice/app/lib/se-company-address-entity.server.ts` (create) | Reads over the six tables into `SeAddressDetail`; the writes; `launchSeAddressFold`. |
| `backoffice/app/components/admin/fold-run-poller.tsx` (create) | `FoldRunPoller` moved out of `se-basic-info-workspace.tsx` (unchanged behaviour), imported by both workspaces. |
| `backoffice/app/components/admin/se-address-workspace.tsx` (create) | The two-column tab. |
| `backoffice/app/components/admin/se-address-edit-sheet.tsx` (create) | Add / Correct / edit-draft sheet. |
| `backoffice/app/routes/admin-se-company-address.tsx` (rewrite) | loader, action, component over the entity. |
| `backoffice/tests/se-address-fields.test.ts`, `se-address-decision-form.test.ts`, `se-company-address-entity.server.test.ts`, `se-address-edit-sheet.test.tsx` (create); `tests/admin-se-company-address.test.tsx` (rewrite) | Vitest, ClickHouse and Dagster mocked with `vi.mock` as `tests/se-basic-info.server.test.ts` does. |
| Spec section 8 and 10; `dagster_v3/.../address/docs/address-design.md` | Rulings 1 to 3 recorded; the backoffice file list. |

Interfaces (defined in Tasks 2 and 3, consumed by 4 and 5):

```ts
// se-address-fields.ts
export const ADDRESS_SOURCES = ["scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft"] as const;
export const ADDRESS_KINDS = ["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"] as const;
export const REVIEWER_KINDS = ["postal", "visiting", "visiting_or_postal", "registered"] as const;
export type SeAddressSource = (typeof ADDRESS_SOURCES)[number];
export type SeAddressKind = (typeof ADDRESS_KINDS)[number];
export function isAddressSource(v: string): v is SeAddressSource;
export function isAddressKind(v: string): v is SeAddressKind;
export function addressSourceLabel(source: string): string;   // "SCB", "Bolagsverket", "Ratsit", "Reviewer", "Reviewer draft"
export function addressKindLabel(kind: string): string;
export function geocodeStatusLabel(status: string): string;   // matched_exact -> "Exact", matched_street -> "Street", matched_corrected -> "Corrected", matched_site -> "Site", matched_area -> "Area (centroid)", unmatched -> "Unmatched", ambiguous -> "Ambiguous", postal_box -> "Box", property_identifier -> "Property", invalid_address -> "Invalid", foreign -> "Foreign", '' -> "Not geocoded"
export function selectedAddressFromSearch(params: URLSearchParams): string | null;  // ?address=<64 hex> else null
export interface SeAddressInput { careOf: string; streetLine: string; postalCode: string; city: string; country: string; kind: SeAddressKind; note: string; }
export type SeAddressValidation = { ok: true; input: SeAddressInput } | { ok: false; error: string };
export function validateSeAddressInput(raw: Record<string, string>): SeAddressValidation;
export function addressFoldPending(foldedAt: string | null, stamps: readonly string[], hasNormalized: boolean): boolean;
export const MAX_NOTE_LENGTH = 500;

// se-address-decision-form.ts
export type SeAddressDecision =
  | { intent: "remove"; addressKey: string; note: string }
  | { intent: "reset"; addressKey: string; note: string }
  | { intent: "fold-now" }
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SeAddressInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string };
export type SeAddressDecisionRequest = { ok: true; decision: SeAddressDecision } | { ok: false; error: string };
export function parseSeAddressDecision(form: FormData): SeAddressDecisionRequest;

// se-company-address-entity.server.ts
export interface SeAddressRow { /* the 31 main columns as strings, numbers for latitude/longitude/geocode_confidence (null allowed), active: number */ }
export interface SeAddressNormalizedRow { /* the 21 normalized columns as strings */ }
export interface SeAddressRawRow { /* the 20 raw columns as strings */ }
export interface SeAddressHistoryRow { /* the 31 history columns, same shape as SeAddressRow */ }
export interface SeAddressRuleRow { company_id; address_key; action; removed: number; decided_by; note; decided_at }
export interface SeAddressPrecedenceRow { company_id; field; source; precedence: number; removed: number; decided_by; note; decided_at }
export interface SeAddressMember { source; slot; normalizedId; current: SeAddressNormalizedRow | null; raw: SeAddressRawRow | null; refoldPending: boolean; completeness: number }
export interface SeAddressPublished { row: SeAddressRow; members: SeAddressMember[]; textSourceReason: "most complete" | "tie-break" | "single source"; hideRule: SeAddressRuleRow | null }
export interface SeAddressDraft { slot: string; raw: SeAddressRawRow; normalized: SeAddressNormalizedRow | null; replacesKey: string }
export interface SeAddressDetail { published: SeAddressPublished[]; drafts: SeAddressDraft[]; history: SeAddressHistoryRow[]; rules: SeAddressRuleRow[]; precedence: SeAddressPrecedenceRow[]; foldPending: boolean }
export async function loadSeAddressDetail(companyId: string): Promise<SeAddressDetail | null>;
export class SeAddressDecisionError extends Error {}
export async function removeSeAddress(companyId, decision, now?): Promise<{ decidedAt: string }>;
export async function resetSeAddress(companyId, decision, now?): Promise<{ decidedAt: string }>;
export async function saveSeAddressDraft(companyId, decision, now?): Promise<{ decidedAt: string; slot: string }>;
export async function activateSeAddressDraft(companyId, decision, now?): Promise<{ decidedAt: string }>;
export async function discardSeAddressDraft(companyId, decision, now?): Promise<{ decidedAt: string }>;
export async function launchSeAddressFold(companyId: string): Promise<{ runId: string; url: string | null }>;
```

---

### Task 1: The targeted fold normalizes the company's raw rows first (Dagster)

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_assets.py`

**Interfaces:**
- Consumes: `normalize.normalize_companies(client, company_ids, *, changed_only, normalized_at, page_size, log) -> NormalizeCounts` (already imported in `assets.py`), `batch.fold_companies(client, duckdb, ids, *, changed_only, source_run_id, folded_at, page_size, log) -> FoldCounts`.
- Produces: `targeted_fold(client, duckdb, company_ids, *, changed_only, source_run_id, folded_at, page_size, log) -> tuple[NormalizeCounts, FoldCounts]` in `assets.py`; the asset's metadata gains the normalize counters under a `normalize_` prefix.

- [ ] **Step 1: Write the failing test** (append to `tests/test_se_company_address_assets.py`)

```python
def test_targeted_fold_normalizes_the_ids_before_folding_them(monkeypatch) -> None:
    from datetime import UTC, datetime

    calls: list[tuple] = []

    def fake_normalize(client, ids, *, changed_only, normalized_at, page_size, log):
        calls.append(("normalize", list(ids), changed_only))
        return SimpleNamespace(as_metadata=lambda: {"rows": 3})

    def fake_fold(client, duckdb, ids, *, changed_only, source_run_id, folded_at, page_size, log):
        calls.append(("fold", list(ids), changed_only, source_run_id))
        return SimpleNamespace(as_metadata=lambda: {"published": 2})

    monkeypatch.setattr(assets, "normalize_companies", fake_normalize)
    monkeypatch.setattr(assets, "fold_companies", fake_fold)
    now = datetime(2026, 9, 7, 20, 0, tzinfo=UTC)
    normalized, folded = assets.targeted_fold(
        object(), object(), ["5560000001"], changed_only=False, source_run_id="run-1", folded_at=now,
        page_size=20_000, log=None,
    )
    assert calls == [("normalize", ["5560000001"], True), ("fold", ["5560000001"], False, "run-1")]
    assert normalized.as_metadata() == {"rows": 3} and folded.as_metadata() == {"published": 2}
```

(`from types import SimpleNamespace` at the top of the test module.)

- [ ] **Step 2: Run it to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_assets.py -q`
Expected: FAIL with `AttributeError: ... has no attribute 'targeted_fold'`.

- [ ] **Step 3: Implement**

In `assets.py`, above the fold assets:

```python
def targeted_fold(
    client: Any, duckdb: Any, company_ids: Sequence[str], *, changed_only: bool, source_run_id: str,
    folded_at: datetime, page_size: int, log: Callable[..., object] | None,
) -> tuple[NormalizeCounts, FoldCounts]:
    """The targeted fold normalizes the companies' raw rows first (spec section 8: a
    reviewer's draft parses on Fold now), always changed_only, so only rows never
    normalized, newer than their normalized row or on an older normalizer version are
    touched; then folds the same ids with the caller's changed_only."""
    normalized = normalize_companies(
        client, company_ids, changed_only=True, normalized_at=folded_at, page_size=page_size, log=log,
    )
    folded = fold_companies(
        client, duckdb, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
    return normalized, folded
```

Import `NormalizeCounts` from `address.normalize` and `FoldCounts` from `address.batch`, `Callable`, `Sequence` from `collections.abc`. In `se_company_address_fold_companies`, replace the `fold_companies(...)` call with `normalized, counts = targeted_fold(client, duckdb, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id, folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info)`; note `normalize_companies` takes `log=context.log` in the normalize asset (a logger) while the fold takes `context.log.info` (a callable): check `normalize.py`'s use of `log` and pass what it expects (if it calls `log.info(...)`, pass `context.log` to the normalize call inside `targeted_fold` by giving the helper a `logger` argument; say what you did in the report). Metadata: `{**_fold_metadata(counts, config), **{f"normalize_{k}": v for k, v in normalized.as_metadata().items()}}`. Extend the asset's description with "Normalizes the companies' raw rows first, so a reviewer's draft parses on Fold now."

- [ ] **Step 4: Run the tests and the defs check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_assets.py tests/test_se_company_address_batch.py tests/test_se_company_address_normalize.py -q && uv run --frozen --no-sync dg check defs`
Expected: PASS, OK.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/assets.py tests/test_se_company_address_assets.py
git commit -m "feat(dagster): targeted address fold normalizes the company's raw rows first"
```

---

### Task 2: Client-safe catalogue, validation and decision parser

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-address-fields.ts`, `corpscout/services/backoffice/app/lib/se-address-decision-form.ts`
- Test: `corpscout/services/backoffice/tests/se-address-fields.test.ts`, `corpscout/services/backoffice/tests/se-address-decision-form.test.ts`

**Interfaces:** the two modules' exports in the file map's interface block; model `app/lib/se-basic-info-fields.ts` and `app/lib/se-basic-info-decision-form.ts` for style (no `.server` imports, hand-written validation, manual FormData reading).

- [ ] **Step 1: Write the failing tests**

`tests/se-address-fields.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  ADDRESS_KINDS, ADDRESS_SOURCES, REVIEWER_KINDS, addressFoldPending, addressKindLabel, addressSourceLabel,
  geocodeStatusLabel, isAddressKind, isAddressSource, selectedAddressFromSearch, validateSeAddressInput,
} from "~/lib/se-address-fields";

const KEY = "a".repeat(64);
const base = { careOf: "", streetLine: "Storgatan 5", postalCode: "111 22", city: "Stockholm", country: "SE", kind: "postal", note: "" };

describe("address catalogue", () => {
  it("names the five sources and six kinds of the entity", () => {
    expect([...ADDRESS_SOURCES]).toEqual(["scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft"]);
    expect([...ADDRESS_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"]);
    expect([...REVIEWER_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered"]);
    expect(isAddressSource("ratsit") && !isAddressSource("llm")).toBe(true);
    expect(isAddressKind("registered") && !isAddressKind("home")).toBe(true);
    expect(addressSourceLabel("bolagsverket")).toBe("Bolagsverket");
    expect(addressSourceLabel("reviewer_draft")).toBe("Reviewer draft");
    expect(addressKindLabel("visiting_or_postal")).toBe("Visiting or postal");
    expect(geocodeStatusLabel("matched_area")).toBe("Area (centroid)");
    expect(geocodeStatusLabel("")).toBe("Not geocoded");
  });
  it("reads the selected address key from the search params", () => {
    expect(selectedAddressFromSearch(new URLSearchParams(`address=${KEY}`))).toBe(KEY);
    expect(selectedAddressFromSearch(new URLSearchParams("address=nope"))).toBeNull();
    expect(selectedAddressFromSearch(new URLSearchParams())).toBeNull();
  });
});

describe("validateSeAddressInput", () => {
  it("accepts a street line with a spaced postcode and normalizes the postcode", () => {
    const result = validateSeAddressInput(base);
    expect(result).toEqual({ ok: true, input: { ...base, postalCode: "11122" } });
  });
  it("accepts a box line and a care-of", () => {
    expect(validateSeAddressInput({ ...base, streetLine: "Box 123", careOf: "Anna Svensson" }).ok).toBe(true);
  });
  it("refuses a missing street line, a bad postcode, a missing city, a bad kind, a foreign country", () => {
    expect(validateSeAddressInput({ ...base, streetLine: " " })).toEqual({ ok: false, error: "A street line or a box is required." });
    expect(validateSeAddressInput({ ...base, postalCode: "1112" })).toEqual({ ok: false, error: "Postcode must be five digits." });
    expect(validateSeAddressInput({ ...base, city: "" })).toEqual({ ok: false, error: "City is required." });
    expect(validateSeAddressInput({ ...base, kind: "home" })).toEqual({ ok: false, error: "Unknown address kind." });
    expect(validateSeAddressInput({ ...base, country: "NO" })).toEqual({ ok: false, error: "Only Swedish addresses can be typed here." });
  });
  it("caps lengths and refuses control characters", () => {
    expect(validateSeAddressInput({ ...base, streetLine: "x".repeat(201) })).toEqual({ ok: false, error: "Street line is longer than 200 characters." });
    expect(validateSeAddressInput({ ...base, careOf: "x".repeat(201) })).toEqual({ ok: false, error: "Care-of is longer than 200 characters." });
    expect(validateSeAddressInput({ ...base, city: "x".repeat(101) })).toEqual({ ok: false, error: "City is longer than 100 characters." });
    expect(validateSeAddressInput({ ...base, note: "x".repeat(501) })).toEqual({ ok: false, error: "Note is longer than 500 characters." });
    expect(validateSeAddressInput({ ...base, streetLine: "Storgatan\u00075" })).toEqual({ ok: false, error: "Street line must be plain text." });
  });
  it("trims every field and keeps the reviewer's casing", () => {
    const result = validateSeAddressInput({ ...base, streetLine: "  Storgatan 5 ", city: " Stockholm " });
    expect(result.ok && result.input.streetLine).toBe("Storgatan 5");
    expect(result.ok && result.input.city).toBe("Stockholm");
  });
});

describe("addressFoldPending", () => {
  it("is pending when a stamp is newer than the fold, or when nothing was folded but rows exist", () => {
    expect(addressFoldPending("2026-09-07 10:00:00.000", ["2026-09-07 09:00:00.000"], true)).toBe(false);
    expect(addressFoldPending("2026-09-07 10:00:00.000", ["2026-09-07 11:00:00.000"], true)).toBe(true);
    expect(addressFoldPending(null, [], true)).toBe(true);
    expect(addressFoldPending(null, [], false)).toBe(false);
  });
});
```

`tests/se-address-decision-form.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { parseSeAddressDecision } from "~/lib/se-address-decision-form";

const KEY = "b".repeat(64);
function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [k, v] of Object.entries(entries)) data.set(k, v);
  return data;
}

describe("parseSeAddressDecision", () => {
  it("parses fold-now, remove, reset, activate and discard", () => {
    expect(parseSeAddressDecision(form({ intent: "fold-now" }))).toEqual({ ok: true, decision: { intent: "fold-now" } });
    expect(parseSeAddressDecision(form({ intent: "remove", address_key: KEY, note: " why " }))).toEqual({ ok: true, decision: { intent: "remove", addressKey: KEY, note: "why" } });
    expect(parseSeAddressDecision(form({ intent: "reset", address_key: KEY }))).toEqual({ ok: true, decision: { intent: "reset", addressKey: KEY, note: "" } });
    expect(parseSeAddressDecision(form({ intent: "activate", slot: "r20260907", note: "" }))).toEqual({ ok: true, decision: { intent: "activate", slot: "r20260907", note: "" } });
    expect(parseSeAddressDecision(form({ intent: "discard", slot: "r20260907" }))).toEqual({ ok: true, decision: { intent: "discard", slot: "r20260907" } });
  });
  it("parses save-draft with the validated input, an optional slot and an optional replaces key", () => {
    const result = parseSeAddressDecision(form({
      intent: "save-draft", care_of: "", street_line: "Storgatan 5", postal_code: "111 22", city: "Stockholm",
      country: "SE", kind: "postal", note: "typed", slot: "", replaces_key: KEY,
    }));
    expect(result).toEqual({
      ok: true,
      decision: {
        intent: "save-draft", slot: null, replacesKey: KEY,
        input: { careOf: "", streetLine: "Storgatan 5", postalCode: "11122", city: "Stockholm", country: "SE", kind: "postal", note: "typed" },
      },
    });
  });
  it("refuses a bad key, a missing slot, a bad note and an unknown intent", () => {
    expect(parseSeAddressDecision(form({ intent: "remove", address_key: "zz" }))).toEqual({ ok: false, error: "Unknown address." });
    expect(parseSeAddressDecision(form({ intent: "activate" }))).toEqual({ ok: false, error: "Unknown draft." });
    expect(parseSeAddressDecision(form({ intent: "reset", address_key: KEY, note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
    expect(parseSeAddressDecision(form({ intent: "save-draft", street_line: "", postal_code: "11122", city: "S", country: "SE", kind: "postal" }))).toEqual({ ok: false, error: "A street line or a box is required." });
    expect(parseSeAddressDecision(form({ intent: "nope" }))).toEqual({ ok: false, error: "Unknown intent." });
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run (from `corpscout/services/backoffice`): `npx vitest run tests/se-address-fields.test.ts tests/se-address-decision-form.test.ts`
Expected: FAIL, modules not found.

- [ ] **Step 3: Write the two modules**

`app/lib/se-address-fields.ts`:

```ts
/**
 * The address entity's catalogue and the client-safe validation of a typed
 * address (spec 2026-09-06 sections 3.1, 8). No `.server` import: the route's
 * module must not drag ClickHouse into the client bundle.
 */

export const ADDRESS_SOURCES = ["scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft"] as const;
export const ADDRESS_KINDS = ["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"] as const;
export const REVIEWER_KINDS = ["postal", "visiting", "visiting_or_postal", "registered"] as const;
export type SeAddressSource = (typeof ADDRESS_SOURCES)[number];
export type SeAddressKind = (typeof ADDRESS_KINDS)[number];
export const MAX_NOTE_LENGTH = 500;
const MAX_STREET = 200;
const MAX_CARE_OF = 200;
const MAX_CITY = 100;
const KEY_PATTERN = /^[0-9a-f]{64}$/;
const CONTROL = /[\u0000-\u001f\u007f]/;

export function isAddressSource(value: string): value is SeAddressSource {
  return (ADDRESS_SOURCES as readonly string[]).includes(value);
}
export function isAddressKind(value: string): value is SeAddressKind {
  return (ADDRESS_KINDS as readonly string[]).includes(value);
}
export function isAddressKey(value: string): boolean {
  return KEY_PATTERN.test(value);
}

const SOURCE_LABELS: Record<SeAddressSource, string> = {
  scb: "SCB", bolagsverket: "Bolagsverket", ratsit: "Ratsit", reviewer: "Reviewer", reviewer_draft: "Reviewer draft",
};
export function addressSourceLabel(source: string): string {
  return isAddressSource(source) ? SOURCE_LABELS[source] : source;
}
const KIND_LABELS: Record<SeAddressKind, string> = {
  postal: "Postal", visiting: "Visiting", visiting_or_postal: "Visiting or postal", registered: "Registered",
  workplace: "Workplace", unknown: "Unknown",
};
export function addressKindLabel(kind: string): string {
  return isAddressKind(kind) ? KIND_LABELS[kind] : kind;
}
const GEOCODE_LABELS: Record<string, string> = {
  matched_exact: "Exact", matched_street: "Street", matched_corrected: "Corrected", matched_site: "Site",
  matched_area: "Area (centroid)", unmatched: "Unmatched", ambiguous: "Ambiguous", postal_box: "Box",
  property_identifier: "Property", invalid_address: "Invalid", foreign: "Foreign", "": "Not geocoded",
};
export function geocodeStatusLabel(status: string): string {
  return GEOCODE_LABELS[status] ?? status;
}

export function selectedAddressFromSearch(params: URLSearchParams): string | null {
  const key = params.get("address") ?? "";
  return isAddressKey(key) ? key : null;
}

export interface SeAddressInput {
  careOf: string;
  streetLine: string;
  postalCode: string;
  city: string;
  country: string;
  kind: SeAddressKind;
  note: string;
}
export type SeAddressValidation = { ok: true; input: SeAddressInput } | { ok: false; error: string };

function plain(label: string, value: string, max: number): string | { error: string } {
  const trimmed = value.trim();
  if (CONTROL.test(trimmed)) return { error: `${label} must be plain text.` };
  if (trimmed.length > max) return { error: `${label} is longer than ${max} characters.` };
  return trimmed;
}

/** Spec 8's validation: a box or a street line, a five-digit postcode, a city,
 * capped lengths, plain text, a catalogue kind, Sweden only. */
export function validateSeAddressInput(raw: Record<string, string>): SeAddressValidation {
  const streetLine = plain("Street line", raw.streetLine ?? "", MAX_STREET);
  if (typeof streetLine !== "string") return { ok: false, error: streetLine.error };
  if (streetLine === "") return { ok: false, error: "A street line or a box is required." };
  const careOf = plain("Care-of", raw.careOf ?? "", MAX_CARE_OF);
  if (typeof careOf !== "string") return { ok: false, error: careOf.error };
  const postalCode = (raw.postalCode ?? "").replace(/\s+/g, "");
  if (!/^[0-9]{5}$/.test(postalCode)) return { ok: false, error: "Postcode must be five digits." };
  const city = plain("City", raw.city ?? "", MAX_CITY);
  if (typeof city !== "string") return { ok: false, error: city.error };
  if (city === "") return { ok: false, error: "City is required." };
  const country = (raw.country ?? "SE").trim().toUpperCase() || "SE";
  if (country !== "SE") return { ok: false, error: "Only Swedish addresses can be typed here." };
  const kind = raw.kind ?? "";
  if (!isAddressKind(kind)) return { ok: false, error: "Unknown address kind." };
  const note = plain("Note", raw.note ?? "", MAX_NOTE_LENGTH);
  if (typeof note !== "string") return { ok: false, error: note.error };
  return { ok: true, input: { careOf, streetLine, postalCode, city, country, kind, note } };
}

/** Dagster's selection (batch._changed_company_ids) in miniature: a non-draft
 * normalized version or a rule version newer than the newest fold, or normalized
 * rows with no fold at all. Drafts are never among `stamps`. */
export function addressFoldPending(foldedAt: string | null, stamps: readonly string[], hasNormalized: boolean): boolean {
  if (foldedAt === null) return hasNormalized;
  return stamps.some((stamp) => stamp > foldedAt);
}
```

`app/lib/se-address-decision-form.ts`:

```ts
/**
 * Turns the Address tab's form posts into one decision (spec 8). Client-safe.
 * Six intents: `remove` and `reset` act on a published key, `save-draft` writes
 * or edits a reviewer draft (with `replaces_key` for a Correct), `activate` and
 * `discard` act on a draft slot, `fold-now` launches the targeted fold.
 */
import { MAX_NOTE_LENGTH, isAddressKey, validateSeAddressInput, type SeAddressInput } from "~/lib/se-address-fields";

export type SeAddressDecision =
  | { intent: "remove"; addressKey: string; note: string }
  | { intent: "reset"; addressKey: string; note: string }
  | { intent: "fold-now" }
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SeAddressInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string };
export type SeAddressDecisionRequest = { ok: true; decision: SeAddressDecision } | { ok: false; error: string };

const SLOT_PATTERN = /^r[0-9]{17}$/;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}
function refuse(error: string): SeAddressDecisionRequest {
  return { ok: false, error };
}
function noteOrRefuse(form: FormData): { ok: true; note: string } | { ok: false; error: string } {
  const note = text(form, "note").trim();
  if (note.length > MAX_NOTE_LENGTH) return { ok: false, error: `Note is longer than ${MAX_NOTE_LENGTH} characters.` };
  return { ok: true, note };
}

export function parseSeAddressDecision(form: FormData): SeAddressDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent === "remove" || intent === "reset") {
    const addressKey = text(form, "address_key");
    if (!isAddressKey(addressKey)) return refuse("Unknown address.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, addressKey, note: note.note } };
  }
  if (intent === "activate" || intent === "discard") {
    const slot = text(form, "slot");
    if (!SLOT_PATTERN.test(slot)) return refuse("Unknown draft.");
    if (intent === "discard") return { ok: true, decision: { intent, slot } };
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slot, note: note.note } };
  }
  if (intent === "save-draft") {
    const validated = validateSeAddressInput({
      careOf: text(form, "care_of"), streetLine: text(form, "street_line"), postalCode: text(form, "postal_code"),
      city: text(form, "city"), country: text(form, "country"), kind: text(form, "kind"), note: text(form, "note"),
    });
    if (!validated.ok) return refuse(validated.error);
    const slotText = text(form, "slot");
    if (slotText !== "" && !SLOT_PATTERN.test(slotText)) return refuse("Unknown draft.");
    const replacesText = text(form, "replaces_key");
    if (replacesText !== "" && !isAddressKey(replacesText)) return refuse("Unknown address.");
    return {
      ok: true,
      decision: { intent, slot: slotText === "" ? null : slotText, replacesKey: replacesText === "" ? null : replacesText, input: validated.input },
    };
  }
  return refuse("Unknown intent.");
}
```

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-address-fields.test.ts tests/se-address-decision-form.test.ts && npm run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-address-fields.ts app/lib/se-address-decision-form.ts tests/se-address-fields.test.ts tests/se-address-decision-form.test.ts
git commit -m "feat(backoffice): address entity catalogue, validation and decision parser"
```

---

### Task 3: The server module over the six tables

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-company-address-entity.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/clickhouse.server.ts` (two inserters), `corpscout/services/backoffice/app/lib/dagster.server.ts` (one constant)
- Test: `corpscout/services/backoffice/tests/se-company-address-entity.server.test.ts`

**Interfaces:**
- Consumes: `chQuery`, `getWriteClient` pattern (`chInsertSeBasicInfoSuggestions` in `clickhouse.server.ts`), `launchRun`, `dagsterRunUrl`, `ASSET_JOB_NAME` (`dagster.server.ts`), Task 2's `addressFoldPending`, `SeAddressDecision`, `SeAddressInput`; `clickhouseStamp` from `se-basic-info.server.ts` (import it; it is a pure helper).
- Produces: the interface block's types and functions. Tables: `corpscout.se_company_address_v2` (main, 31 columns), `se_company_address_normalized` (21), `se_company_address_suggestion` (20), `se_company_address_history` (31), `se_company_address_rule` (7), `se_company_address_precedence` (8).

- [ ] **Step 1: Write the failing tests**

Mock exactly as `tests/se-basic-info.server.test.ts` does (`vi.hoisted` + `vi.mock("~/lib/clickhouse.server", ...)` with `chQuery`, `chInsertSeCompanyAddressSuggestions`, `chInsertSeCompanyAddressRules` as `vi.fn()`; `vi.mock("~/lib/dagster.server", ...)` with `launchRun`, `dagsterRunUrl`, `ASSET_JOB_NAME`, `SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET`). Route `chQuery` by SQL text (`sql.includes("FROM corpscout.se_company_address_v2")` etc.). Cases:

1. `loadSeAddressDetail` returns null when there are no main rows, no normalized rows and no drafts; otherwise assembles `published` (active first), each row's `members` resolved through `normalized_ids` with `refoldPending` true when the current normalized row of that (source, slot) has another `normalized_id`, `textSourceReason` "most complete" when the text_source member has strictly more non-empty components, "tie-break" when equal, "single source" for one member; `hideRule` the active hide rule for the key; `drafts` from `reviewer_draft` raw rows whose address columns are not all empty, with their normalized row when present and `replacesKey`; `foldPending` per `addressFoldPending` with the non-draft normalized `normalized_at`s and every rule `decided_at`.
2. `saveSeAddressDraft` with `slot: null` inserts one `reviewer_draft` raw row: `slot = "r" + stamp digits` (17 digits), `suggestion_id = sha256(company\nreviewer_draft\nslot\nstamp)`, `source_record_uid: ""`, `observed_at = suggested_at = stamp`, `kind`, `raw_address: null`, `care_of` null when empty else the text, `street_address` the street line, `postal_code`, `post_town` the city, `county: null`, `country_code: "SE"`, `decided_by: "backoffice"`, `note` null when empty, `replaces_key` null or the key, `source_run_id: "backoffice"`, `extractor_version: "backoffice-v1"`; with a `slot` given it reuses it. Assert the exact object.
3. `activateSeAddressDraft` refuses (`SeAddressDecisionError`) when no draft exists for the slot or its street_address is empty; otherwise inserts, in ONE `chInsertSeCompanyAddressSuggestions` call, the `reviewer` row (same slot, same address columns, note the decision's note) and the `reviewer_draft` row cleared (all seven address columns null, note "activated"); when the draft carries `replaces_key`, also inserts a hide rule `{company_id, address_key: replaces_key, action: "hide", removed: 0, decided_by: "backoffice", note: "corrected by reviewer", decided_at: stamp}`.
4. `discardSeAddressDraft` refuses when no draft; otherwise inserts the cleared draft version with note "discarded".
5. `removeSeAddress`: the published row is found by key (refuse "Unknown address." otherwise); with only source members it inserts the hide rule (removed 0, the note); with a reviewer member it inserts a cleared `reviewer` row for that slot (note "removed by reviewer"); with both, both writes (Ruling 1); on an already hidden row it refuses "Already hidden.".
6. `resetSeAddress` refuses when no active hide rule exists for the key; otherwise inserts the rule with `removed: 1` and the note.
7. `launchSeAddressFold` calls `launchRun` with `assetSelection: ["se_company_address_fold_companies"]`, run config `{ ops: { se_company_address_fold_companies: { config: { company_ids: [companyId] } } } }`, tag `{"backoffice/address": "fold-now"}`, and returns `{ runId, url }`.
8. Every SQL constant reads `FINAL` where a current version is needed (main, normalized, suggestion, rule, precedence), binds `{companyId:String}`, reads `FixedString` keys through `toString`, and orders history `folded_at DESC` with `LIMIT 200`.

Write the tests with concrete fixture rows (one company, two published rows: a two-member merged street address whose text_source is scb with a unit the bolagsverket member lacks, and a single-member box address; one draft with `replaces_key` set; one active hide rule on the box key).

- [ ] **Step 2: Run them to verify they fail**

Run: `npx vitest run tests/se-company-address-entity.server.test.ts` → module not found.

- [ ] **Step 3: Implement**

`clickhouse.server.ts`: add, next to `chInsertSeBasicInfoSuggestions`:

```ts
/** Append a reviewer raw-row version to the SE address suggestion table; the
 * normalize asset parses it and the fold reads the newest version per
 * (company_id, source, slot) through FINAL. */
export async function chInsertSeCompanyAddressSuggestions<T extends object>(values: T[]): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({ table: "se_company_address_suggestion", values, format: "JSONEachRow" });
}
/** Append a hide-rule version (or its release) to the SE address rule table. */
export async function chInsertSeCompanyAddressRules<T extends object>(values: T[]): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({ table: "se_company_address_rule", values, format: "JSONEachRow" });
}
```

`dagster.server.ts`: `export const SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET = "se_company_address_fold_companies";` beside the basic-info constant.

`se-company-address-entity.server.ts`: the SQL constants below, the types of the interface block, and the functions. Reads:

```ts
const COMPONENTS_SQL = (a: string) => `  ifNull(${a}.care_of, '') AS care_of, ifNull(${a}.box, '') AS box, ifNull(${a}.street_name, '') AS street_name,
  ifNull(${a}.house_number, '') AS house_number, ifNull(${a}.unit, '') AS unit, ifNull(${a}.postal_code, '') AS postal_code,
  ifNull(${a}.city, '') AS city, toString(${a}.country_code) AS country_code`;

const MAIN_COLUMNS_SQL = (a: string) => `  ${a}.company_id AS company_id, toString(${a}.address_key) AS address_key,
${COMPONENTS_SQL(a)},
  ${a}.normalized_address AS normalized_address,
  arrayMap(x -> toString(x), ${a}.kinds) AS kinds, arrayMap(x -> toString(x), ${a}.sources) AS sources, ${a}.slots AS slots,
  arrayMap(x -> toString(x), ${a}.normalized_ids) AS normalized_ids, toString(${a}.text_source) AS text_source,
  toUInt8(${a}.active) AS active, toString(${a}.inactive_reason) AS inactive_reason,
  ${a}.latitude AS latitude, ${a}.longitude AS longitude, toString(${a}.geocode_status) AS geocode_status,
  toString(${a}.geocode_method) AS geocode_method, ${a}.geocode_confidence AS geocode_confidence,
  toString(${a}.geocode_precision) AS geocode_precision, toString(${a}.geocode_policy) AS geocode_policy,
  ${a}.geocode_reference AS geocode_reference, ifNull(toString(${a}.geocoded_at), '') AS geocoded_at,
  toString(${a}.normalizer_version) AS normalizer_version, toString(${a}.folded_at) AS folded_at,
  toString(${a}.fold_version) AS fold_version, ${a}.source_run_id AS source_run_id`;

export const ADDRESS_MAIN_SQL = `SELECT\n${MAIN_COLUMNS_SQL("m")}\nFROM corpscout.se_company_address_v2 AS m FINAL\nWHERE m.company_id = {companyId:String}\nORDER BY m.active DESC, m.inactive_reason, m.normalized_address`;
export const ADDRESS_HISTORY_SQL = `SELECT\n${MAIN_COLUMNS_SQL("h")}\nFROM corpscout.se_company_address_history AS h\nWHERE h.company_id = {companyId:String}\nORDER BY h.folded_at DESC\nLIMIT 200`;
export const ADDRESS_NORMALIZED_SQL = `SELECT
  n.company_id AS company_id, toString(n.source) AS source, n.slot AS slot, toString(n.normalized_id) AS normalized_id,
  toString(n.suggestion_id) AS suggestion_id, toString(n.suggested_at) AS suggested_at, toString(n.kind) AS kind,
${COMPONENTS_SQL("n")},
  n.normalized_address AS normalized_address, toString(n.address_key) AS address_key, toString(n.parse_status) AS parse_status,
  n.parse_notes AS parse_notes, toString(n.normalizer_version) AS normalizer_version, toString(n.normalized_at) AS normalized_at
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
ORDER BY n.source, n.slot`;
export const ADDRESS_RAW_SQL = `SELECT
  s.company_id AS company_id, toString(s.source) AS source, s.slot AS slot, toString(s.suggestion_id) AS suggestion_id,
  s.source_record_uid AS source_record_uid, toString(s.observed_at) AS observed_at, toString(s.kind) AS kind,
  ifNull(s.raw_address, '') AS raw_address, ifNull(s.care_of, '') AS care_of, ifNull(s.street_address, '') AS street_address,
  ifNull(s.postal_code, '') AS postal_code, ifNull(s.post_town, '') AS post_town, ifNull(s.county, '') AS county,
  ifNull(s.country_code, '') AS country_code, ifNull(s.decided_by, '') AS decided_by, ifNull(s.note, '') AS note,
  ifNull(toString(s.replaces_key), '') AS replaces_key, toString(s.suggested_at) AS suggested_at,
  s.source_run_id AS source_run_id, toString(s.extractor_version) AS extractor_version
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source, s.slot`;
export const ADDRESS_RULES_SQL = `SELECT
  r.company_id AS company_id, toString(r.address_key) AS address_key, toString(r.action) AS action, toUInt8(r.removed) AS removed,
  toString(r.decided_by) AS decided_by, r.note AS note, toString(r.decided_at) AS decided_at
FROM corpscout.se_company_address_rule AS r FINAL
WHERE r.company_id = {companyId:String}
ORDER BY r.decided_at DESC`;
export const ADDRESS_PRECEDENCE_SQL = `SELECT
  p.company_id AS company_id, toString(p.field) AS field, toString(p.source) AS source, toUInt32(p.precedence) AS precedence,
  toUInt8(p.removed) AS removed, toString(p.decided_by) AS decided_by, p.note AS note, toString(p.decided_at) AS decided_at
FROM corpscout.se_company_address_precedence AS p FINAL
WHERE p.company_id IN ('', {companyId:String})
ORDER BY p.precedence DESC`;
```

`loadSeAddressDetail`: `Promise.all` of the six reads; index normalized rows by `normalized_id` and by `source|slot`, raw rows by `source|slot`; for each main row build `members` from the parallel arrays (`sources[i]`, `slots[i]`, `normalized_ids[i]`): `current = normalizedBySlot.get(...) ?? null`, `raw = rawBySlot.get(...) ?? null`, `refoldPending = current !== null && current.normalized_id !== normalized_ids[i]`, `completeness = count of the seven components of current that are non-empty` (0 when no current); `textSourceReason`: one member → "single source"; else the text_source member's completeness strictly greater than every other → "most complete", else "tie-break"; `hideRule = rules.find(r => r.address_key === key && r.action === "hide" && r.removed === 0) ?? null` (the newest version per key comes first from ORDER BY, and FINAL already collapsed versions). `drafts`: raw rows with `source === "reviewer_draft"` and any of `street_address, care_of, postal_code, post_town` non-empty → `{ slot, raw, normalized: normalizedBySlot.get("reviewer_draft|" + slot) ?? null, replacesKey: raw.replaces_key }`. `foldPending = addressFoldPending(newest folded_at across main rows or null, [...normalized non-draft normalized_at, ...rules decided_at], normalized non-draft rows exist)`. Return null when `published`, `drafts` and the normalized rows are all empty.

Writes: a private `stampSlot(stamp)` = `"r" + stamp.replace(/\D/g, "")` (17 digits); `suggestionId(companyId, source, slot, stamp)` with `createHash("sha256")` from `node:crypto` over `${companyId}\n${source}\n${slot}\n${stamp}`; `rawRowVersion(companyId, source, slot, stamp, columns, note, replacesKey)` returning the twenty-column insert object (address columns `string | null`, `raw_address: null`, `county: null`, `country_code: "SE"` when any address column is set else `null`, `kind` from the input or the draft, `source_record_uid: ""`, `observed_at: stamp`, `suggested_at: stamp`, `decided_by: "backoffice"`, `source_run_id: "backoffice"`, `extractor_version: "backoffice-v1"`); `ruleVersion(companyId, key, removed, note, stamp)`.

- `saveSeAddressDraft`: slot = decision.slot ?? stampSlot(stamp); insert the draft version with the input's columns (`care_of` null when empty), `replaces_key` = decision.replacesKey; return `{ decidedAt: stamp, slot }`.
- `activateSeAddressDraft`: read raw rows; find `reviewer_draft` with the slot and a non-empty `street_address`, else refuse "No draft to activate."; insert `[reviewerVersion(same columns, kind, note), draftVersionCleared(note "activated")]` in one call; if `replaces_key !== ""` insert the hide rule (note "corrected by reviewer") in a second call to the rule inserter.
- `discardSeAddressDraft`: refuse "No draft to discard." when absent; insert the cleared draft version (note "discarded").
- `removeSeAddress`: read main rows and rules; find the row by key, refuse "Unknown address." when absent, "Already hidden." when `inactive_reason === "hidden"`; for every member with `source === "reviewer"` insert a cleared `reviewer` version for that slot (note "removed by reviewer"); if any member has another source insert the hide rule (removed 0, the note or "removed by reviewer").
- `resetSeAddress`: refuse "No rule to reset." when no active hide rule; insert the rule version with `removed: 1`, note `note === "" ? "reset to default" : "reset to default: " + note`.
- `launchSeAddressFold`: as `launchSeBasicInfoFold` with the address constant and `FOLD_NOW_TAG = { "backoffice/address": "fold-now" }`.

- [ ] **Step 4: Run the tests and the typecheck**

Run: `npx vitest run tests/se-company-address-entity.server.test.ts tests/se-basic-info.server.test.ts && npm run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-company-address-entity.server.ts app/lib/clickhouse.server.ts app/lib/dagster.server.ts tests/se-company-address-entity.server.test.ts
git commit -m "feat(backoffice): address entity server module with reviewer writes and the targeted fold launch"
```

---

### Task 4: Workspace and edit sheet

**Files:**
- Create: `corpscout/services/backoffice/app/components/admin/fold-run-poller.tsx` (moved from `se-basic-info-workspace.tsx`, which imports it from there afterwards; behaviour unchanged)
- Create: `corpscout/services/backoffice/app/components/admin/se-address-workspace.tsx`, `corpscout/services/backoffice/app/components/admin/se-address-edit-sheet.tsx`
- Test: `corpscout/services/backoffice/tests/se-address-edit-sheet.test.tsx`; the existing `tests/admin-se-company-basic-info.test.tsx` must stay green after the poller move.

**Interfaces:**
- Consumes: Task 3's `SeAddressDetail` and friends (type imports only, from the `.server` module are allowed with `import type`), Task 2's labels and `REVIEWER_KINDS`, shadcn `Accordion`, `Alert`, `Badge`, `Button`, `Card`, `Dialog`, `Empty`, `Input`, `Sheet`, `Textarea` as `se-basic-info-workspace.tsx` and `se-basic-info-edit-sheet.tsx` use them.
- Produces: `SeAddressWorkspace({ companyId, detail, selectedKey, result })`, `SeAddressResult` type (`{ ok: true; intent; runId?; url?; slot? } | { ok: false; error } | null`), `SeAddressEditSheet({ open, onOpenChange, mode: "add" | "correct" | "edit-draft", initial, slot, replacesKey, result })` and the portal-free `SeAddressEditForm` for tests.

Layout (spec 8), mirroring the Info tab's structure:

- Left (two-thirds): **Addresses card**: active rows first, one row each with the `normalized_address` line, kind badges, source badges (`addressSourceLabel`), `geocodeStatusLabel` + `geocode_precision`, a `hidden` badge (`inactive_reason === "hidden"`); the row links to `?address=<key>` (a `Link` on the line), with a Correct button OUTSIDE the link (opens the sheet in `correct` mode prefilled from the row's components: care-of, `street_name + house_number + unit` or `Box <box>`, postcode, city, the first kind); withdrawn and hidden rows under a collapsed `Accordion` group "Withdrawn and hidden (N)". **Drafts card** when `drafts` is non-empty: one row per draft with the raw line (`care_of`, `street_address`, `postal_code`, `post_town`), a `draft` badge, its parsed line and `parse_status` when `normalized` is present else "parses on Fold now", the replaced key's line when `replacesKey` matches a published row, Edit (sheet `edit-draft` mode), Activate and Discard buttons through the confirmation dialog. **History card**: `Accordion`, newest first, each item `folded_at`, the line, `active`/`inactive_reason`, sources. **Fold now** button + `FoldRunPoller` (props as today: `runId`, `url`, poll route `../info/run/<runId>` relative to the company page: use the same `useFetcher` load path the basic-info workspace uses) and the "Fold pending" `Alert` when `detail.foldPending`.
- Right (one-third, sticky): the selected address (`selectedKey`, else the first active row): the line and key (short), then **Sources**: one entry per member with `addressSourceLabel`, slot, the raw text (`raw.street_address` etc. or `raw.raw_address`), the parsed components of `current` and its `parse_status` and `parse_notes`, a "re-fold pending" badge when `refoldPending`; **Published text**: `text_source` label and `textSourceReason`; **Geocode**: `geocodeStatusLabel`, method, precision, confidence, lat/lon, policy, reference (short), `geocoded_at`; **Rule**: the hide rule in force (note, decided_at) or "none"; actions: **Remove** (confirmation dialog with note; disabled when hidden), **Reset to default** (only when `hideRule`), **Add address** button at the top of the panel (sheet in `add` mode).
- Every action is a plain `<Form method="post">` with hidden `intent`, `address_key` / `slot` / `replaces_key`, and the dialog's note input, exactly like `DecisionDialogBody` in the basic-info workspace (copy that pattern; do not import the private component).
- The edit sheet: `SheetContent` with `className="data-[side=right]:sm:max-w-3xl"` (the wide sheet precedent), fields `care_of` (Input), `street_line` (Input, placeholder "Storgatan 5 or Box 123"), `postal_code` (Input), `city` (Input), `country` (Input, value `SE`, readOnly), `kind` (native `<select>` over `REVIEWER_KINDS`), `note` (Textarea), hidden `intent=save-draft`, `slot`, `replaces_key`; Save draft submits; the sheet closes on a successful result (`result?.ok && result.intent === "save-draft"`), shows the error inline otherwise.

- [ ] **Step 1: Write the failing sheet test** (`tests/se-address-edit-sheet.test.tsx`, model `tests/se-basic-info-edit-sheet.test.tsx`): renders `SeAddressEditForm` in `add` mode with the seven fields, `country` read-only `SE`, the kind select's options equal to `REVIEWER_KINDS`, hidden `intent` `save-draft`; in `correct` mode the fields are prefilled from `initial` and `replaces_key` carries the key; in `edit-draft` mode `slot` carries the slot; an error result renders in an alert.
- [ ] **Step 2: Move `FoldRunPoller`** into `fold-run-poller.tsx` (export it, keep its props and polling unchanged), import it in `se-basic-info-workspace.tsx`; run `npx vitest run tests/admin-se-company-basic-info.test.tsx` → still green.
- [ ] **Step 3: Write the two components** as specified; `npm run typecheck` clean.
- [ ] **Step 4: Run** `npx vitest run tests/se-address-edit-sheet.test.tsx tests/admin-se-company-basic-info.test.tsx && npm run typecheck` → PASS.
- [ ] **Step 5: Commit**

```bash
git add app/components/admin/fold-run-poller.tsx app/components/admin/se-basic-info-workspace.tsx app/components/admin/se-address-workspace.tsx app/components/admin/se-address-edit-sheet.tsx tests/se-address-edit-sheet.test.tsx
git commit -m "feat(backoffice): address workspace and edit sheet on the address entity"
```

---

### Task 5: The route, its test, and the docs

**Files:**
- Rewrite: `corpscout/services/backoffice/app/routes/admin-se-company-address.tsx`
- Rewrite: `corpscout/services/backoffice/tests/admin-se-company-address.test.tsx`
- Modify: spec section 8 (Rulings 1 to 3 as three sentences under "Actions") and section 10 (add `fold-run-poller.tsx`); `dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md` (a "Backoffice" paragraph listing the files and the write shapes).

**Interfaces:** consumes Tasks 2 to 4. The route keeps the comment header about exports; `loader({ request, params })` returns `data({ detail, selectedKey }, detail ? undefined : { status: 404 })` with `selectedKey = selectedAddressFromSearch(url.searchParams)`; `action({ request, params })` validates `COMPANY_ID_PATTERN` as the Info route does, parses with `parseSeAddressDecision`, dispatches: `fold-now` → `launchSeAddressFold` → `{ ok: true, intent, runId, url }`; `save-draft` → `saveSeAddressDraft` → `{ ok: true, intent, slot }`; `activate` / `discard` / `remove` / `reset` → their functions → `{ ok: true, intent }`; `SeAddressDecisionError` → `{ ok: false, error }`; anything else rethrown. `meta` from the shell like the Info route (or a fixed "Address" title if the shell is not available here; check what the old route did).

- [ ] **Step 1: Rewrite the route test**: mock `~/lib/se-company-address-entity.server` wholesale (as `tests/admin-se-company-basic-info.test.tsx` mocks the basic-info server) and assert: loader returns 404 when detail is null; loader passes `selectedKey` from `?address=`; action refuses a bad company id and a bad form; each intent dispatches to its function with the parsed decision; a `SeAddressDecisionError` becomes `{ ok: false, error }`; the component renders the workspace with the published lines.
- [ ] **Step 2: Rewrite the route**, run `npx vitest run tests/admin-se-company-address.test.tsx tests/se-company-tabs.server.test.ts tests/admin-se-company-area.test.tsx && npm run typecheck` → PASS. Then the whole suite `npm test` → the only failures allowed are pre-existing ones (say which in the report).
- [ ] **Step 3: Docs**: spec section 8 gets the three ruling sentences; section 10 gets the poller file; `address-design.md` gets the Backoffice paragraph.
- [ ] **Step 4: Commit**

```bash
git add app/routes/admin-se-company-address.tsx tests/admin-se-company-address.test.tsx ../dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md ../dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md
git commit -m "feat(backoffice): Address tab on the address entity"
```

---

### Task 6: Smoke on prod data (controller with the owner)

1. [ ] Whole-branch review; the owner merges; hot-sync the dagster host (Task 1's asset change); the owner starts the backoffice locally from the main checkout (`localhost:5183`).
2. [ ] Open `/admin/se/company/<id>/address` for a bucket-00 company with two published addresses and one with a merged row: the lines, sources, geocode labels and history render; the right panel resolves the members.
3. [ ] Add address → Save draft → Fold now: the run normalizes and folds; the draft shows its parsed components; Activate → Fold now: the address is published with source `reviewer`, precedence text_source reviewer.
4. [ ] Correct a source address: the draft carries the replaced key; Activate → Fold now: the old key is hidden (rule), the new one active. Reset to default on the hidden key → Fold now: it is active again.
5. [ ] Remove a source address → hidden; Reset → active. Remove the reviewer address → withdrawn after Fold now.
6. [ ] Record in the ledger and spec section 9 (slice 3 shipped); archive the ledger; update memory.

## Self-review

- Spec coverage: section 8 layout (Task 4), the five actions (Tasks 2 to 5), validation (Task 2), the poller (Task 4, ruling 4), the targeted fold normalizing first (Task 1, ruling 3), Remove semantics (ruling 1), reviewer slots (ruling 2), section 10 file names (Tasks 2 to 4; `se-address-workspace.tsx`, `se-address-edit-sheet.tsx`, `se-company-address-entity.server.ts`, `se-address-fields.ts`, `se-address-decision-form.ts`, route `admin-se-company-address.tsx`).
- Placeholders: Tasks 2 and 3 carry complete code for the modules and SQL; Task 4 describes the components structurally against the basic-info files that exist in the repo, with the sheet test spelled out; Task 5's route mirrors the Info route line for line.
- Type consistency: `SeAddressInput` (Task 2) feeds `save-draft` (Task 2) and `saveSeAddressDraft` (Task 3); `SeAddressDetail` (Task 3) feeds the workspace (Task 4) and the loader (Task 5); the poller keeps its props.
