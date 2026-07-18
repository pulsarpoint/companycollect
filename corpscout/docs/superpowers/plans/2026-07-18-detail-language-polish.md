# Company Detail Language Toggle + Structured Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Company detail pages (`/company/:country/:id`) get an English ↔ Original language toggle that collapses paired translated fields to one variant, long free-text fields render as readable prose sections, and the record card gains a key-facts strip — a structured visual upgrade without losing the fidelity-first guarantee (every field still reachable).

**Architecture:** Pure presentation change — the detail loader already returns BOTH variants of every paired field (`SELECT *` + translated-view joins), so no server/query work. A new pure module (`app/components/detail/language.ts`) implements pair detection and language resolution; `CompanyRecordSection` restructures around it (key-facts strip → prose sections for long texts → grouped field grid → lineage `<details>`); the toggle is a two-option segmented control writing `?lang=original` to the URL (default English, matching the app's URL-driven idiom). User decisions (2026-07-18): default English; structured polish (not a bold redesign); detail pages only.

**Tech Stack:** React Router 8, TypeScript, shadcn/ui (Base UI), vitest. The UI task's implementer MUST load the `frontend-design` skill before writing component code.

## Global Constraints

- **Fidelity first (standing user rule):** no information becomes unreachable. Collapsing a pair hides the *other* variant only while the toggle points away from it; unpaired fields always render; lineage stays in the collapsible details block.
- **Pair rule (grounded in the live column audit, 2026-07-18):** a language pair exists ONLY when BOTH `<base>_en` AND `<base>_original` are present in the record. Unpaired `_en` fields (cz `legal_form_en`, fr `status_en`/`legal_form_en`, br `status_en`/`company_size_en`) and unpaired `_original` fields (fr `denomination_original`) render as-is under BOTH languages. **`_amount_original` fields are the financial-currency convention, never language pairs** — the both-sides rule excludes them automatically (no `_amount_en` exists), but the unit tests must pin this with br's `share_capital_amount_original`.
- Fallback within a pair: if the selected variant is empty and the other is not, show the other with a muted `(original)` / `(english)` suffix marker — never a blank cell where data exists.
- `<base>_language` keys and `_translated_at`/`_translation_provider`/`_translation_model` keys belong to the lineage block (extend `isLineageKey` if it doesn't already catch `_language`).
- Toggle state = `?lang=original` URL param (absence = English); read via the existing `useEffectiveSearchParams`; switching must preserve all other params and NOT reset scroll (`preventScrollReset`).
- Long-text detection: key base in `{articles_purpose, activity_text}` OR resolved value length > 240 chars → prose section (label + `<p className="whitespace-pre-wrap">`), removed from the grid.
- Key-facts strip (top of record card, only facts that exist for the country): legal form (language-resolved), status (raw value as today), registered date if a date-kind field exists, website as a link. Derived generically from the record — no per-country hardcoding beyond key-name candidates.
- Existing detail sections (industries, financials incl. NoFinancialsSection, contacts/map, domains) are OUT of scope except that the toggle control lives in the page header area above them.
- Live paired-data reality to test against: no (3 pairs incl. long texts), ee (3 pairs incl. status), fi/sk (legal form pair), lv (activity_text pair + unpaired legal_form_description_en), gb/se (zero pairs — the toggle may render but must be harmless/hidden when no pairs exist: hide it when `pairCount === 0`).
- Gates: `pnpm typecheck` + full `pnpm test` (260 expected + new units); SSR checks on a THROWAWAY dev port (5183 USER-OWNED, never touch); Conventional Commits, explicit-path adds, trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Pure language/pairing + long-text helpers

**Files:**
- Create: `corpscout/services/backoffice/app/components/detail/language.ts`
- Modify: `corpscout/services/backoffice/app/components/detail/fields.tsx` (only if `isLineageKey` needs `_language`)
- Test: `corpscout/services/backoffice/app/components/detail/language.test.ts`

**Interfaces (Task 2 consumes exactly these):**

```ts
export type Lang = "en" | "original";
export type ResolvedField = {
  key: string;            // base key for paired ("articles_purpose"), original key otherwise
  label: string;          // humanizeFieldKey of the display key
  value: unknown;
  fromOtherLang: boolean; // true when the fallback kicked in (render the muted marker)
  isLongText: boolean;
};
export function resolveRecordFields(record: Record<string, unknown>, lang: Lang): {
  fields: ResolvedField[];   // grid fields, pair-collapsed, lineage excluded, long texts excluded
  longTexts: ResolvedField[];
  pairCount: number;         // number of collapsed pairs (0 → hide the toggle)
};
export function keyFacts(record: Record<string, unknown>, lang: Lang): { label: string; value: string; href?: string }[];
```

- [ ] **Step 1 (TDD): tests first.** Cover with realistic fixtures: NO record (articles_purpose/activity_text/legal_form_description pairs + `last_submitted_accounts_year` passthrough) — en mode shows `_en` values under base labels, original mode shows `_original`; empty `_en` falls back with `fromOtherLang: true`; EE status pair collapses; BR `share_capital_amount_original` + `status_en` (no original) pass through un-collapsed with `pairCount` not counting them; `_language`/`_translated_at`/`_translation_provider`/`_translation_model` excluded from fields (lineage); long-text via key-set AND via >240 length; `pairCount === 0` for a gb-like record; keyFacts picks legal form (resolved by lang), status, website href, skips absentees. Run → FAIL (module missing).
- [ ] **Step 2: implement.** Pair scan: for each `${base}_en` key with `${base}_original` present → collapse; selected variant per lang with cross-fallback; everything else passes through `splitFields`-style (reuse `isLineageKey` — extend it for `_language` suffix if needed, keeping its existing tests green). Long-text rule per Global Constraints. `keyFacts`: candidate key lists (`legal_form_description`/`legal_form` resolved via the pair mechanism, `status`/`lifecycle_status`/`company_status`, registered-date candidates, `website`/`primary_website_url`), first match wins per fact.
- [ ] **Step 3:** `pnpm vitest run app/components/detail/language.test.ts` green; `pnpm typecheck` clean. Commit (explicit paths): `feat(backoffice): language pair resolution helpers`.

---

### Task 2: Detail page UI — toggle, key facts, prose, grouped grid

**Files:**
- Modify: `corpscout/services/backoffice/app/routes/country-company-detail.tsx` (toggle control in header)
- Modify: `corpscout/services/backoffice/app/components/detail/detail-sections.tsx` (CompanyRecordSection restructure)
- Create: `corpscout/services/backoffice/app/components/detail/lang-toggle.tsx`

**REQUIRED:** the implementer loads the `frontend-design` skill BEFORE writing component code, and applies it within the app's existing shadcn/Tailwind system (this is polish inside an existing design system — restraint and hierarchy, not a new aesthetic).

- [ ] **Step 1: `LangToggle`** — segmented two-option control (Base UI/shadcn idiom; buttons or ToggleGroup if present in ui/): "English" | "Original", active state visually distinct, navigates via `Link` with `preventScrollReset` writing/removing `lang=original` while preserving other params (reuse the `tableSearch`-style param helper or a tiny local one). Hidden entirely when `pairCount === 0`.
- [ ] **Step 2: CompanyRecordSection restructure** (receives `lang` prop): (1) key-facts strip — a compact row of labeled facts from `keyFacts()` at the card top, visually distinct (muted labels, medium values, website as link); (2) prose sections — each `longTexts` entry as a titled block with `whitespace-pre-wrap` paragraph and the `fromOtherLang` marker when set; (3) the remaining `fields` in the existing `FieldGrid`; (4) lineage `<details>` unchanged. Route passes `lang` (parsed from `useEffectiveSearchParams`, `"original"` only when exactly that value) and renders `LangToggle` in the page header row (near the status badge).
- [ ] **Step 3: Gate.** `pnpm typecheck`; full `pnpm test` green. Throwaway dev server: `/company/no/936560288` (Coop Norge) — toggle visible, English default shows `articles_purpose_en` as prose, switching to Original swaps to Norwegian text and back, URL carries `lang=original`, other params survive, key-facts strip renders; `/company/ee/<any>` — status pair collapses per language; `/company/gb/<any>` — NO toggle rendered, page intact; `/company/fi/<any>` — legal-form pair works. Capture what you observed. Kill your server.
- [ ] **Step 4: Commit** (explicit paths): `feat(backoffice): detail language toggle and structured record card`.

---

### Task 3: README + final gate

**Files:** `corpscout/services/backoffice/README.md`

- [ ] Document the toggle (URL param, default English, pair rule incl. the `_amount_original` exclusion, fallback markers, hidden-when-no-pairs) in the detail-pages section. Full `pnpm typecheck && pnpm test` green. Commit: `docs(backoffice): detail language toggle notes`.
