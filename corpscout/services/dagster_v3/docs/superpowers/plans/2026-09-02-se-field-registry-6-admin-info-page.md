# SE Field Registry 6 -- Admin Info Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild `/admin/se/company/:companyId/info` as a registry-driven page: three `FieldGroupCard`s (Identity / Activity / Scale) read `se_company_field` + `se_company_field_candidate`, the description card keeps its language toggle inside the Activity group, the artifact cards fold into a Sources drawer, and eight of the wide row's legacy provenance columns are dropped once nothing reads them (`correction_ids` stays, spec 8.3).

**Architecture:** Phase B of the field-registry design (spec section 11). A new server module (`se-company-fields.server.ts`) loads the registry, the resolved rows and the candidates for one company and applies source precedence in TypeScript; a client-safe module (`se-company-field-groups.ts`) groups and formats fields; one presentational `FieldGroupCard` renders a group with Use-this / Edit / Release slots the page fills with the existing `use-source` / `edit` / `release` forms plus one new `edit-field` intent. The serving MV stops reading `description_sources` (candidate-set subqueries instead, staged swap per 000347), and the last task is the owner-gated drop of the legacy columns.

**Tech Stack:** TypeScript strict, React Router 8 framework mode (loaders read, actions write, turbo-stream v2 carries `Map` in loader data), shadcn/base-ui (`@base-ui/react` 1.6: Tabs, Collapsible with `hiddenUntilFound`), vitest + `renderToStaticMarkup`, ClickHouse (golang-migrate ledger), Python 3.14 / pytest for the MV builder and ledger tests.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-02-se-company-field-registry-design.md` -- section 11 (all), section 8.3 last paragraph (legacy columns), section 12 backoffice tests. Read it before any task.

## Global Constraints

- Backoffice commands run from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck` (both clean before every commit). Dagster commands run from `corpscout/services/dagster_v3`: `uv run --frozen --no-sync pytest tests/<file> -q -p no:warnings`.
- TypeScript strict; shadcn/base-ui components from `app/components/ui/`; `~/` imports; loaders read, actions write; no `.skip` / `.only`.
- base-ui Tabs render only the active panel on SSR unless `keepMounted` -- `CompanyDescriptionCard` already sets it, keep it. base-ui `Collapsible.Panel` is unmounted when closed unless `keepMounted` or `hiddenUntilFound`; every collapsed list on this page uses `hiddenUntilFound` so SSR tests see its content and find-in-page works.
- Phase A (plans 1-5) is live: tables `corpscout.se_company_field_registry` (ignore the `field = '*'` projection row), `corpscout.se_company_field` (ReplacingMergeTree(resolved_at), read FINAL), `corpscout.se_company_field_candidate` (ReplacingMergeTree(extracted_at), read FINAL); backoffice modules `app/lib/se-company-field-registry.server.ts` (`loadFieldRegistry(): Promise<FieldRegistry>`), `app/lib/se-company-field-resolve.server.ts` (`resolveCompanyFields(companyId, fields, opts?)`); the route action already handles `use-source`, `use-suggestion`, `edit`, `release` and resolves synchronously after insert.
- Source precedence is applied in TypeScript from `registry.fields[].sources`, never in SQL.
- Fixed labels: groups Identity / Activity / Scale; sources scb -> "SCB register", bolagsverket -> "Bolagsverket", esef -> "ESEF filing", wikidata -> "Wikidata", ratsit -> "Ratsit", domains -> "Domain match", llm -> "LLM", reviewer -> "Reviewer".
- Migrations: first line `CREATE DATABASE IF NOT EXISTS corpscout;`, last non-blank line ends with `;`, name appended to `EXPECTED_MIGRATIONS` in `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` plus a content test; migration number = the next free number at execution time (`ls corpscout/clickhouse/migrations | tail`, plus one; written as `<NNNNNN>` below -- phase A's plans consume numbers up to about 000377 first). Additive only, except Task 8's owner-gated column drop, which is written and applied at the deploy step -- never committed ahead of its gate (2026-08-25 ruling, same as 000371/000372).
- Conventional Commits; stage by explicit path; commit trailers on their own lines:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`

## File structure

| File | Responsibility |
| --- | --- |
| `app/lib/se-company-fields.server.ts` (new) | Load registry + resolved rows + candidates + decisions for one company; precedence ordering; winner flag. Exports `RESOLVED_FIELDS_SQL`, `FIELD_CANDIDATES_SQL`, `loadCompanyFields`. |
| `app/lib/se-company-field-groups.ts` (new, client-safe) | `groupFields`, `formatFieldValue`, `sourceLabel`, `candidateDescriptionProposals`. No server imports. |
| `app/components/admin/field-group-card.tsx` (new) | One display group: rows per field, source/decision chip, collapsible candidates with slots. Knows nothing about forms. |
| `app/lib/se-info-field-value-form.ts` | Gains the `edit-field` intent. |
| `app/lib/se-company-info.server.ts` | `INFO_SQL` / `SeCompanyInfoRow` lose the nine legacy columns; `SUGGESTIONS_SQL.is_published` reads `se_company_field`. |
| `app/routes/admin-se-company-info.tsx` | Loader adds `loadCompanyFields`; action passes the registry into the form builder. |
| `app/components/admin/se-company-info-review-workspace.tsx` | Three `FieldGroupCard`s, inline `edit-field` editor, Sources drawer; Published version + Company facts cards deleted. |
| `tests/fixtures/se-field-registry.ts` (new) | The spec 4.2 registry as a `FieldRegistry` fixture, shared by three test files. |
| `src/dagster_v3/defs/sweden_company/companies_current.py` + migration | `desc_esef` / `desc_wikidata` from candidate sets (the only builder change on top of plan 3's render); staged-swap migration. |
| `src/dagster_v3/defs/se_company/fields/sql.py` + `registry.py` | `render_projection_sql` stops writing the eight legacy columns; `INFO_REGISTRY.version` -> `se-info-v2`. |
| Final migration (gated) | `DROP VIEW` of the swap's `_retired` view, then `DROP COLUMN` x8 on `se_company_info` (`correction_ids` kept). |

---

### Task 1: `loadCompanyFields` -- resolved rows, candidates, precedence, winner flag

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-company-fields.server.ts`
- Test: `corpscout/services/backoffice/tests/se-company-fields.server.test.ts`

**Interfaces:**
- Consumes: `chQuery<T>(sql, params)` from `~/lib/clickhouse.server`; `loadFieldRegistry(): Promise<FieldRegistry>` and types `FieldRegistry`, `FieldRegistryEntry` from `~/lib/se-company-field-registry.server` (plan 4); `FIELD_VALUES_SQL` and `SeCompanyInfoFieldValueRow` from `~/lib/se-company-info.server`.
- Produces:
  ```ts
  export type ResolvedField = { field: string; value: string; valueJson: Record<string, unknown>; source: string; sourceRecordUid: string; observedAt: string; decisionId: string | null; policyName: string; policyVersion: string; candidateCount: number; agreeingSources: string[]; resolvedAt: string };
  export type FieldCandidate = { field: string; source: string; sourceRecordUid: string; value: string; valueJson: Record<string, unknown>; observedAt: string; extractedAt: string; isWinner: boolean };
  export type CompanyFields = { registry: FieldRegistry; resolved: Map<string, ResolvedField>; candidates: Map<string, FieldCandidate[]>; decisions: SeCompanyInfoFieldValueRow[] };
  export function loadCompanyFields(companyId: string): Promise<CompanyFields>;
  export const RESOLVED_FIELDS_SQL: string;   // {companyId:String}, FINAL
  export const FIELD_CANDIDATES_SQL: string;  // {companyId:String}, FINAL, ORDER BY field, observed_at DESC
  export function orderCandidates(entry: FieldRegistryEntry | undefined, rows: FieldCandidate[]): FieldCandidate[];
  export function parseValueJson(raw: string): Record<string, unknown>;
  ```

- [ ] **Step 1: Write the failing test**

```ts
// tests/se-company-fields.server.test.ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);

import { REGISTRY } from "./fixtures/se-field-registry";
import {
  FIELD_CANDIDATES_SQL,
  loadCompanyFields,
  orderCandidates,
  parseValueJson,
  RESOLVED_FIELDS_SQL,
  type FieldCandidate,
} from "~/lib/se-company-fields.server";
import { FIELD_VALUES_SQL } from "~/lib/se-company-info.server";

const COMPANY = "5565200028";

const resolvedRow = {
  field: "legal_name",
  value: "Alpha AB",
  value_json: "",
  source: "bolagsverket",
  source_record_uid: "bv:1",
  observed_at: "2026-08-01 00:00:00.000",
  decision_id: null,
  policy_name: "source_precedence",
  policy_version: "source_precedence-v1",
  candidate_count: 2,
  agreeing_sources: ["bolagsverket", "scb"],
  registry_version: "se-info-v1",
  resolved_at: "2026-09-01 10:00:00.000",
};

const candidateRow = (over: Partial<Record<string, string>>) => ({
  field: "legal_name",
  source: "scb",
  source_record_uid: "scb:1",
  value: "Alpha AB",
  value_json: '{"compare_key":"alpha ab"}',
  observed_at: "2026-08-01 00:00:00.000",
  extracted_at: "2026-08-30 00:00:00.000",
  ...over,
});

describe("company field queries", () => {
  it("read both long tables FINAL for one company and leave precedence to TypeScript", () => {
    expect(RESOLVED_FIELDS_SQL).toContain("FROM corpscout.se_company_field AS f FINAL");
    expect(RESOLVED_FIELDS_SQL).toContain("WHERE f.company_id = {companyId:String}");
    expect(RESOLVED_FIELDS_SQL).toContain("toString(f.decision_id) AS decision_id");
    expect(RESOLVED_FIELDS_SQL).toContain("toUInt32(f.candidate_count) AS candidate_count");
    expect(FIELD_CANDIDATES_SQL).toContain("FROM corpscout.se_company_field_candidate AS c FINAL");
    expect(FIELD_CANDIDATES_SQL).toContain("WHERE c.company_id = {companyId:String}");
    expect(FIELD_CANDIDATES_SQL).toContain("ORDER BY c.field, c.observed_at DESC, c.source, c.source_record_uid");
    expect(FIELD_CANDIDATES_SQL).toContain("LIMIT 2000");
    // Rank is a registry concern: the SQL must not know source order.
    expect(FIELD_CANDIDATES_SQL).not.toContain("indexOf(");
    expect(FIELD_CANDIDATES_SQL).not.toContain("rank");
  });
});

describe("orderCandidates", () => {
  const c = (source: string, observedAt: string, uid = `${source}:1`): FieldCandidate => ({
    field: "legal_name", source, sourceRecordUid: uid, value: "x", valueJson: {},
    observedAt, extractedAt: "2026-08-30 00:00:00.000", isWinner: false,
  });
  const legalName = REGISTRY.fields.find((f) => f.field === "legal_name");

  it("orders by registry precedence, then observed_at DESC, then uid; unknown sources last", () => {
    const rows = [
      c("wikidata", "2026-08-09 00:00:00.000"),
      c("scb", "2026-08-01 00:00:00.000"),
      c("hearsay", "2026-08-31 00:00:00.000"),
      c("bolagsverket", "2026-07-01 00:00:00.000", "bv:old"),
      c("bolagsverket", "2026-08-01 00:00:00.000", "bv:new"),
    ];
    expect(orderCandidates(legalName, rows).map((r) => r.sourceRecordUid)).toEqual([
      "bv:new", "bv:old", "scb:1", "wikidata:1", "hearsay:1",
    ]);
  });

  it("falls back to observed_at DESC when the field is not in the registry", () => {
    const rows = [c("scb", "2026-08-01 00:00:00.000"), c("wikidata", "2026-08-09 00:00:00.000")];
    expect(orderCandidates(undefined, rows).map((r) => r.source)).toEqual(["wikidata", "scb"]);
  });
});

describe("parseValueJson", () => {
  it("returns {} for '', malformed text and non-objects", () => {
    expect(parseValueJson("")).toEqual({});
    expect(parseValueJson("not json")).toEqual({});
    expect(parseValueJson("[1]")).toEqual({});
    expect(parseValueJson('{"count":120,"as_of":"2025-12-31"}')).toEqual({ count: 120, as_of: "2025-12-31" });
  });
});

describe("loadCompanyFields", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    registryModule.loadFieldRegistry.mockReset();
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY);
  });

  it("threads the company into the three queries and shapes the maps", async () => {
    clickhouse.query
      .mockResolvedValueOnce([resolvedRow])
      .mockResolvedValueOnce([
        candidateRow({ source: "scb", source_record_uid: "scb:1" }),
        candidateRow({ source: "bolagsverket", source_record_uid: "bv:1" }),
        candidateRow({ field: "website", source: "domains", source_record_uid: "fp:1", value: "https://alpha.se", value_json: "" }),
      ])
      .mockResolvedValueOnce([]);

    const fields = await loadCompanyFields(COMPANY);

    expect(clickhouse.query).toHaveBeenNthCalledWith(1, RESOLVED_FIELDS_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, FIELD_CANDIDATES_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(3, FIELD_VALUES_SQL, { companyId: COMPANY });
    expect(fields.registry).toBe(REGISTRY);
    expect(fields.resolved.get("legal_name")).toEqual({
      field: "legal_name", value: "Alpha AB", valueJson: {}, source: "bolagsverket",
      sourceRecordUid: "bv:1", observedAt: "2026-08-01 00:00:00.000", decisionId: null,
      policyName: "source_precedence", policyVersion: "source_precedence-v1",
      candidateCount: 2, agreeingSources: ["bolagsverket", "scb"], resolvedAt: "2026-09-01 10:00:00.000",
    });
    // Precedence from the registry (bolagsverket before scb), not arrival order.
    expect(fields.candidates.get("legal_name")?.map((c) => c.source)).toEqual(["bolagsverket", "scb"]);
    // The winner is the candidate the resolved row came from.
    expect(fields.candidates.get("legal_name")?.map((c) => c.isWinner)).toEqual([true, false]);
    expect(fields.candidates.get("legal_name")?.[1].valueJson).toEqual({ compare_key: "alpha ab" });
    expect(fields.candidates.get("website")?.[0]).toMatchObject({ value: "https://alpha.se", valueJson: {}, isWinner: false });
    expect(fields.candidates.has("status")).toBe(false);
    expect(fields.decisions).toEqual([]);
  });

  it("keeps a decision's id on the resolved row", async () => {
    clickhouse.query
      .mockResolvedValueOnce([{ ...resolvedRow, field: "status", value: "active", source: "reviewer", source_record_uid: "", decision_id: "22222222-2222-4222-8222-222222222222" }])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    const fields = await loadCompanyFields(COMPANY);
    expect(fields.resolved.get("status")?.decisionId).toBe("22222222-2222-4222-8222-222222222222");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/se-company-fields.server.test.ts`
Expected: FAIL -- `Cannot find module '~/lib/se-company-fields.server'` (and `./fixtures/se-field-registry` until Task 2; create Task 2's fixture file first if you execute tasks out of order -- its content is in Task 2 Step 1).

- [ ] **Step 3: Write the module**

```ts
// app/lib/se-company-fields.server.ts
import { chQuery } from "~/lib/clickhouse.server";
import {
  loadFieldRegistry,
  type FieldRegistry,
  type FieldRegistryEntry,
} from "~/lib/se-company-field-registry.server";
import {
  FIELD_VALUES_SQL,
  type SeCompanyInfoFieldValueRow,
} from "~/lib/se-company-info.server";

/** One row of corpscout.se_company_field: what the resolve step picked for a field. */
export interface ResolvedField {
  field: string;
  value: string;
  valueJson: Record<string, unknown>;
  source: string;
  sourceRecordUid: string;
  observedAt: string;
  /** Set when a reviewer decision, not the policy, decided the field. */
  decisionId: string | null;
  policyName: string;
  policyVersion: string;
  candidateCount: number;
  agreeingSources: string[];
  resolvedAt: string;
}

/** One row of corpscout.se_company_field_candidate, in display order. */
export interface FieldCandidate {
  field: string;
  source: string;
  sourceRecordUid: string;
  value: string;
  valueJson: Record<string, unknown>;
  observedAt: string;
  extractedAt: string;
  /** The candidate the resolved row came from: the policy winner, or the
   * candidate a reviewer decision copied (`source` + `source_record_uid` match). */
  isWinner: boolean;
}

export interface CompanyFields {
  registry: FieldRegistry;
  resolved: Map<string, ResolvedField>;
  candidates: Map<string, FieldCandidate[]>;
  decisions: SeCompanyInfoFieldValueRow[];
}

interface ResolvedQueryRow {
  field: string;
  value: string;
  value_json: string;
  source: string;
  source_record_uid: string;
  observed_at: string;
  decision_id: string | null;
  policy_name: string;
  policy_version: string;
  candidate_count: number;
  agreeing_sources: string[];
  registry_version: string;
  resolved_at: string;
}

interface CandidateQueryRow {
  field: string;
  source: string;
  source_record_uid: string;
  value: string;
  value_json: string;
  observed_at: string;
  extracted_at: string;
}

/**
 * LowCardinality / DateTime64 / UUID columns are wrapped in toString() so
 * JSONEachRow yields one predictable shape (the se-company-info.server.ts
 * convention); toString() over a NULL decision_id is still JS null.
 */
export const RESOLVED_FIELDS_SQL = `SELECT
  toString(f.field) AS field,
  f.value AS value,
  f.value_json AS value_json,
  toString(f.source) AS source,
  f.source_record_uid AS source_record_uid,
  toString(f.observed_at) AS observed_at,
  toString(f.decision_id) AS decision_id,
  toString(f.policy_name) AS policy_name,
  f.policy_version AS policy_version,
  toUInt32(f.candidate_count) AS candidate_count,
  f.agreeing_sources AS agreeing_sources,
  f.registry_version AS registry_version,
  toString(f.resolved_at) AS resolved_at
FROM corpscout.se_company_field AS f FINAL
WHERE f.company_id = {companyId:String}
ORDER BY f.field`;

/**
 * Newest observation first within a field; the registry's source precedence is
 * applied afterwards in TypeScript (orderCandidates), so this statement never
 * has to know the rank of a source. Bounded: ESEF adds one candidate per filing
 * per field, so a long-listed company can carry a few hundred rows.
 */
export const FIELD_CANDIDATES_SQL = `SELECT
  toString(c.field) AS field,
  toString(c.source) AS source,
  c.source_record_uid AS source_record_uid,
  c.value AS value,
  c.value_json AS value_json,
  toString(c.observed_at) AS observed_at,
  toString(c.extracted_at) AS extracted_at
FROM corpscout.se_company_field_candidate AS c FINAL
WHERE c.company_id = {companyId:String}
ORDER BY c.field, c.observed_at DESC, c.source, c.source_record_uid
LIMIT 2000`;

/** `value_json` is '' for unstructured fields and a JSON object otherwise; a
 * malformed payload must not 500 the page. */
export function parseValueJson(raw: string): Record<string, unknown> {
  if (raw === "") return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
  return parsed as Record<string, unknown>;
}

/** Registry precedence (first source wins), then newest observation, then uid
 * for a stable order. A source the registry does not list for the field sorts
 * after every listed one. */
export function orderCandidates(
  entry: FieldRegistryEntry | undefined,
  rows: FieldCandidate[],
): FieldCandidate[] {
  const rank = (source: string): number => {
    const index = entry ? entry.sources.indexOf(source) : -1;
    return index === -1 ? Number.MAX_SAFE_INTEGER : index;
  };
  return [...rows].sort(
    (a, b) =>
      rank(a.source) - rank(b.source) ||
      b.observedAt.localeCompare(a.observedAt) ||
      a.sourceRecordUid.localeCompare(b.sourceRecordUid),
  );
}

export async function loadCompanyFields(companyId: string): Promise<CompanyFields> {
  const [registry, resolvedRows, candidateRows, decisions] = await Promise.all([
    loadFieldRegistry(),
    chQuery<ResolvedQueryRow>(RESOLVED_FIELDS_SQL, { companyId }),
    chQuery<CandidateQueryRow>(FIELD_CANDIDATES_SQL, { companyId }),
    chQuery<SeCompanyInfoFieldValueRow>(FIELD_VALUES_SQL, { companyId }),
  ]);
  const resolved = new Map<string, ResolvedField>();
  for (const row of resolvedRows) {
    resolved.set(row.field, {
      field: row.field,
      value: row.value,
      valueJson: parseValueJson(row.value_json),
      source: row.source,
      sourceRecordUid: row.source_record_uid,
      observedAt: row.observed_at,
      decisionId: row.decision_id,
      policyName: row.policy_name,
      policyVersion: row.policy_version,
      candidateCount: Number(row.candidate_count),
      agreeingSources: row.agreeing_sources,
      resolvedAt: row.resolved_at,
    });
  }
  const byField = new Map<string, FieldCandidate[]>();
  for (const row of candidateRows) {
    const winner = resolved.get(row.field);
    const candidate: FieldCandidate = {
      field: row.field,
      source: row.source,
      sourceRecordUid: row.source_record_uid,
      value: row.value,
      valueJson: parseValueJson(row.value_json),
      observedAt: row.observed_at,
      extractedAt: row.extracted_at,
      isWinner:
        winner !== undefined &&
        winner.source === row.source &&
        winner.sourceRecordUid === row.source_record_uid,
    };
    const list = byField.get(row.field) ?? [];
    list.push(candidate);
    byField.set(row.field, list);
  }
  const candidates = new Map<string, FieldCandidate[]>();
  for (const [field, rows] of byField) {
    const entry = registry.fields.find((candidate) => candidate.field === field);
    candidates.set(field, orderCandidates(entry, rows));
  }
  return { registry, resolved, candidates, decisions };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run tests/se-company-fields.server.test.ts && npm run typecheck`
Expected: PASS (6 tests), typecheck clean.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-company-fields.server.ts corpscout/services/backoffice/tests/se-company-fields.server.test.ts
git commit -m "feat(backoffice): load resolved fields and candidates for one SE company" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---
### Task 2: `se-company-field-groups.ts` -- grouping, formatting, labels, description proposals from candidates

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-company-field-groups.ts`
- Create: `corpscout/services/backoffice/tests/fixtures/se-field-registry.ts`
- Test: `corpscout/services/backoffice/tests/se-company-field-groups.test.ts`

**Interfaces:**
- Consumes: types `FieldRegistry`, `FieldRegistryEntry` (type-only import from `~/lib/se-company-field-registry.server` -- a `import type` is erased and keeps this module client-safe); types `ResolvedField`, `FieldCandidate` (type-only from `~/lib/se-company-fields.server`); `DescriptionProposal` type from `~/lib/se-company-info-payload`.
- Produces:
  ```ts
  export type FieldGroupName = "identity" | "activity" | "scale";
  export type FieldGroup = { group: FieldGroupName; label: string; fields: FieldRegistryEntry[] };
  export const GROUP_LABELS: Record<FieldGroupName, string>; // Identity / Activity / Scale
  export const DESCRIPTION_FIELDS: readonly ["description", "description_sv"];
  export function groupFields(registry: FieldRegistry): FieldGroup[];
  export function fieldLabel(field: string): string;              // "legal_name" -> "Legal name", with overrides below
  export function formatFieldValue(entry: FieldRegistryEntry, resolved: ResolvedField | undefined): string;
  export function sourceLabel(source: string): string;
  export function observedDate(timestamp: string): string;         // first 10 chars
  export function candidateDescriptionProposals(description: FieldCandidate[], descriptionSv: FieldCandidate[]): DescriptionProposal[];
  ```

- [ ] **Step 1: Write the fixture and the failing test**

```ts
// tests/fixtures/se-field-registry.ts -- the spec 4.2 `info` registry as plan 4 exports it.
import type {
  FieldRegistry,
  FieldRegistryEntry,
} from "~/lib/se-company-field-registry.server";

function entry(
  field: string,
  valueType: string,
  displayGroup: string,
  sources: string[],
  over: Partial<FieldRegistryEntry> = {},
): FieldRegistryEntry {
  return {
    field,
    valueType,
    displayGroup,
    structured: valueType === "json",
    pythonOnly: false,
    sources,
    policyName: "source_precedence",
    policyVersion: "source_precedence-v1",
    resolveSql: `-- resolve ${field}`,
    registryVersion: "se-info-v1",
    ...over,
  };
}

export const REGISTRY: FieldRegistry = {
  version: "se-info-v1",
  projectionSql: "-- projection",
  fields: [
    entry("legal_name", "text", "identity", ["bolagsverket", "scb", "wikidata"]),
    entry("legal_form_code", "code", "identity", ["bolagsverket", "scb"]),
    entry("status", "code", "identity", ["bolagsverket", "scb"]),
    entry("incorporation_date", "date", "identity", ["bolagsverket", "scb", "wikidata"]),
    entry("description", "text", "activity", ["llm", "esef", "wikidata", "scb"]),
    entry("description_sv", "text", "activity", ["llm", "scb"]),
    entry("primary_sni_code", "code", "activity", ["scb", "ratsit"]),
    entry("primary_nace_code", "code", "activity", ["scb", "ratsit"]),
    entry("industry_label_en", "text", "activity", ["scb", "ratsit", "wikidata"]),
    entry("website", "url", "scale", ["domains", "wikidata"]),
    entry("employee_count", "json", "scale", ["esef", "bolagsverket", "ratsit", "wikidata"]),
    entry("latest_revenue", "json", "scale", ["esef", "bolagsverket", "ratsit"], { pythonOnly: true }),
  ],
};
```

```ts
// tests/se-company-field-groups.test.ts
import { describe, expect, it } from "vitest";
import { REGISTRY } from "./fixtures/se-field-registry";
import {
  candidateDescriptionProposals,
  fieldLabel,
  formatFieldValue,
  groupFields,
  sourceLabel,
} from "~/lib/se-company-field-groups";
import type { FieldCandidate, ResolvedField } from "~/lib/se-company-fields.server";

const entryOf = (field: string) => {
  const found = REGISTRY.fields.find((f) => f.field === field);
  if (!found) throw new Error(`no ${field} in fixture`);
  return found;
};

const resolved = (field: string, value: string, valueJson: Record<string, unknown> = {}): ResolvedField => ({
  field, value, valueJson, source: "scb", sourceRecordUid: "scb:1",
  observedAt: "2026-08-01 00:00:00.000", decisionId: null, policyName: "source_precedence",
  policyVersion: "source_precedence-v1", candidateCount: 1, agreeingSources: ["scb"],
  resolvedAt: "2026-09-01 10:00:00.000",
});

const candidate = (field: string, source: string, uid: string, value: string, valueJson: Record<string, unknown> = {}): FieldCandidate => ({
  field, source, sourceRecordUid: uid, value, valueJson,
  observedAt: "2026-08-01 00:00:00.000", extractedAt: "2026-08-30 00:00:00.000", isWinner: false,
});

describe("groupFields", () => {
  it("yields the three groups in fixed order with fixed labels, registry order within each", () => {
    const groups = groupFields(REGISTRY);
    expect(groups.map((g) => [g.group, g.label])).toEqual([
      ["identity", "Identity"], ["activity", "Activity"], ["scale", "Scale"],
    ]);
    expect(groups[0].fields.map((f) => f.field)).toEqual([
      "legal_name", "legal_form_code", "status", "incorporation_date",
    ]);
    expect(groups[1].fields.map((f) => f.field)).toEqual([
      "description", "description_sv", "primary_sni_code", "primary_nace_code", "industry_label_en",
    ]);
    expect(groups[2].fields.map((f) => f.field)).toEqual(["website", "employee_count", "latest_revenue"]);
  });

  it("ignores the projection row and any group the registry does not use", () => {
    const groups = groupFields({
      ...REGISTRY,
      fields: [
        { ...entryOf("legal_name") },
        { ...entryOf("legal_name"), field: "*", valueType: "projection", displayGroup: "" },
      ],
    });
    expect(groups.map((g) => g.group)).toEqual(["identity"]);
    expect(groups[0].fields.map((f) => f.field)).toEqual(["legal_name"]);
  });
});

describe("fieldLabel", () => {
  it("humanises snake_case and keeps the abbreviations a reviewer reads", () => {
    expect(fieldLabel("legal_name")).toBe("Legal name");
    expect(fieldLabel("primary_sni_code")).toBe("SNI code");
    expect(fieldLabel("primary_nace_code")).toBe("NACE code");
    expect(fieldLabel("industry_label_en")).toBe("Industry (en)");
    expect(fieldLabel("description_sv")).toBe("Description (sv)");
    expect(fieldLabel("some_future_field")).toBe("Some future field");
  });
});

describe("formatFieldValue", () => {
  it("renders every value_type", () => {
    expect(formatFieldValue(entryOf("legal_name"), resolved("legal_name", "Alpha AB"))).toBe("Alpha AB");
    expect(formatFieldValue(entryOf("status"), resolved("status", "active"))).toBe("active");
    expect(formatFieldValue(entryOf("incorporation_date"), resolved("incorporation_date", "2001-02-03 00:00:00.000"))).toBe("2001-02-03");
    expect(formatFieldValue(entryOf("website"), resolved("website", "https://alpha.se/"))).toBe("https://alpha.se/");
    expect(
      formatFieldValue(entryOf("employee_count"), resolved("employee_count", "1234", { count: 1234, as_of: "2025-12-31", period: "FY2025" })),
    ).toBe("1 234 (as of 2025-12-31)");
    expect(
      formatFieldValue(entryOf("latest_revenue"), resolved("latest_revenue", "12345678", { amount: "12345678.00", currency: "SEK", amount_usd: "1100000.00", fiscal_year: 2025, period_end: "2025-12-31" })),
    ).toBe("SEK 12 345 678 (FY2025)");
  });

  it("falls back to the display value when a json member is missing, and to '' when nothing is resolved", () => {
    expect(formatFieldValue(entryOf("employee_count"), resolved("employee_count", "120", {}))).toBe("120");
    expect(formatFieldValue(entryOf("latest_revenue"), resolved("latest_revenue", "5000", { currency: "SEK" }))).toBe("SEK 5 000");
    expect(formatFieldValue(entryOf("legal_name"), undefined)).toBe("");
  });
});

describe("sourceLabel", () => {
  it("names every registry source and passes an unknown one through", () => {
    expect(["scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm", "reviewer"].map(sourceLabel)).toEqual([
      "SCB register", "Bolagsverket", "ESEF filing", "Wikidata", "Ratsit", "Domain match", "LLM", "Reviewer",
    ]);
    expect(sourceLabel("hearsay")).toBe("hearsay");
  });
});

describe("candidateDescriptionProposals", () => {
  it("pairs a source's english and swedish candidates by record uid, and reads the language marker", () => {
    const proposals = candidateDescriptionProposals(
      [
        candidate("description", "llm", "11111111-1111-4111-8111-111111111111", "Alpha builds payment software.", { language: "en" }),
        candidate("description", "esef", "esef:doc-2", "Alpha levererar betalinfrastruktur.", { language: "sv", fiscal_year: "2024" }),
        candidate("description", "esef", "esef:doc-1", "Alpha provides payment infrastructure.", { language: "en", fiscal_year: "2025" }),
        candidate("description", "wikidata", "wikidata:Q1", "Swedish fintech company"),
        candidate("description", "scb", "scb:1", "IT consultancy.", { language: "en" }),
      ],
      [
        candidate("description_sv", "llm", "11111111-1111-4111-8111-111111111111", "Alpha bygger betalprogramvara."),
        candidate("description_sv", "scb", "scb:1", "IT-konsulter."),
      ],
    );
    expect(proposals.map((p) => p.key)).toEqual([
      "llm:11111111-1111-4111-8111-111111111111", "esef:esef:doc-2", "esef:esef:doc-1", "wikidata:wikidata:Q1", "scb:scb:1",
    ]);
    expect(proposals[0]).toMatchObject({
      source: "llm", sourceLabel: "LLM", english: "Alpha builds payment software.",
      original: "Alpha bygger betalprogramvara.", originalLanguage: "sv",
      sourceRecordUid: "11111111-1111-4111-8111-111111111111", observedAt: "2026-08-01 00:00:00.000", meta: "2026-08-01",
    });
    // An esef description marked sv is an original, its fiscal year is the meta line.
    expect(proposals[1]).toMatchObject({ english: "", original: "Alpha levererar betalinfrastruktur.", originalLanguage: "sv", meta: "fiscal 2024" });
    expect(proposals[2]).toMatchObject({ english: "Alpha provides payment infrastructure.", original: "", meta: "fiscal 2025" });
    // No language marker: an unmarked original, exactly as the artifact path treated Wikidata.
    expect(proposals[3]).toMatchObject({ english: "", original: "Swedish fintech company", originalLanguage: "" });
    expect(proposals[4]).toMatchObject({ english: "IT consultancy.", original: "IT-konsulter.", originalLanguage: "sv" });
  });

  it("keeps a swedish-only candidate as its own proposal", () => {
    const proposals = candidateDescriptionProposals([], [candidate("description_sv", "scb", "scb:1", "IT-konsulter.")]);
    expect(proposals).toHaveLength(1);
    expect(proposals[0]).toMatchObject({ key: "scb:scb:1", english: "", original: "IT-konsulter.", originalLanguage: "sv" });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/se-company-field-groups.test.ts`
Expected: FAIL -- `Cannot find module '~/lib/se-company-field-groups'`.

- [ ] **Step 3: Write the module**

```ts
// app/lib/se-company-field-groups.ts
/**
 * Client-safe helpers for the registry-driven Info page: how the registry's
 * fields are grouped and labelled, how a resolved value reads, and what a
 * source is called. Only `import type` from the server modules -- erased at
 * build time, so nothing here drags ClickHouse into the client bundle.
 */
import type {
  FieldRegistry,
  FieldRegistryEntry,
} from "~/lib/se-company-field-registry.server";
import type { FieldCandidate, ResolvedField } from "~/lib/se-company-fields.server";
import type { DescriptionProposal } from "~/lib/se-company-info-payload";

export type FieldGroupName = "identity" | "activity" | "scale";

export interface FieldGroup {
  group: FieldGroupName;
  label: string;
  fields: FieldRegistryEntry[];
}

export const GROUP_LABELS: Record<FieldGroupName, string> = {
  identity: "Identity",
  activity: "Activity",
  scale: "Scale",
};

const GROUP_ORDER: FieldGroupName[] = ["identity", "activity", "scale"];

/** The pair the description card renders; every other field is a plain row. */
export const DESCRIPTION_FIELDS = ["description", "description_sv"] as const;

const SOURCE_LABELS: Record<string, string> = {
  scb: "SCB register",
  bolagsverket: "Bolagsverket",
  esef: "ESEF filing",
  wikidata: "Wikidata",
  ratsit: "Ratsit",
  domains: "Domain match",
  llm: "LLM",
  reviewer: "Reviewer",
};

export function sourceLabel(source: string): string {
  return SOURCE_LABELS[source] ?? source;
}

const FIELD_LABELS: Record<string, string> = {
  primary_sni_code: "SNI code",
  primary_nace_code: "NACE code",
  industry_label_en: "Industry (en)",
  description_sv: "Description (sv)",
  legal_form_code: "Legal form",
  latest_revenue: "Latest revenue",
};

export function fieldLabel(field: string): string {
  const known = FIELD_LABELS[field];
  if (known) return known;
  const spaced = field.replace(/_/g, " ").trim();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

/** Fixed group order, registry order inside a group. The `*` projection row
 * and any group outside the three known ones are dropped. */
export function groupFields(registry: FieldRegistry): FieldGroup[] {
  return GROUP_ORDER.flatMap((group) => {
    const fields = registry.fields.filter(
      (entry) => entry.field !== "*" && entry.displayGroup === group,
    );
    return fields.length === 0 ? [] : [{ group, label: GROUP_LABELS[group], fields }];
  });
}

/** ClickHouse prints DateTime64 as "YYYY-MM-DD hh:mm:ss.SSS" and Date32 as
 * "YYYY-MM-DD"; either way the first ten characters are the date. */
export function observedDate(timestamp: string): string {
  return timestamp.slice(0, 10);
}

/** "1234567.89" -> "1 234 568": integer part, regular spaces (not the
 * non-breaking ones Intl emits, which a reviewer cannot copy-paste cleanly). */
function groupThousands(raw: unknown): string {
  const number = Number(raw);
  if (!Number.isFinite(number)) return String(raw ?? "");
  return String(Math.round(number)).replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}

function member(json: Record<string, unknown>, key: string): string {
  const value = json[key];
  if (value === undefined || value === null) return "";
  return typeof value === "string" ? value : String(value);
}

/**
 * The one-line rendering of a resolved value per value_type. JSON fields read
 * their structured members (spec 4.2) and fall back to the display value when a
 * member is missing, so a candidate written by an older extractor still shows.
 */
export function formatFieldValue(
  entry: FieldRegistryEntry,
  resolved: ResolvedField | undefined,
): string {
  if (!resolved) return "";
  if (entry.valueType === "date") return observedDate(resolved.value);
  if (entry.valueType !== "json") return resolved.value;
  const json = resolved.valueJson;
  if (entry.field === "employee_count") {
    const count = member(json, "count");
    const asOf = member(json, "as_of");
    const shown = count === "" ? resolved.value : groupThousands(count);
    return asOf === "" ? shown : `${shown} (as of ${observedDate(asOf)})`;
  }
  if (entry.field === "latest_revenue") {
    const amount = member(json, "amount");
    const currency = member(json, "currency");
    const fiscalYear = member(json, "fiscal_year");
    const shown = groupThousands(amount === "" ? resolved.value : amount);
    const withCurrency = currency === "" ? shown : `${currency} ${shown}`;
    return fiscalYear === "" ? withCurrency : `${withCurrency} (FY${fiscalYear})`;
  }
  return resolved.value;
}

/**
 * The description card's menu, built from candidates instead of artifact rows.
 * A source's `description` and `description_sv` candidates with the same record
 * uid are one proposal (SCB's translation + original, the LLM's two halves); a
 * `description` candidate marked `language: sv` (an ESEF filing) is an original,
 * one with no marker (Wikidata) is an unmarked original, anything English is
 * the english block. Menu order is the candidates' order, i.e. precedence.
 */
export function candidateDescriptionProposals(
  description: FieldCandidate[],
  descriptionSv: FieldCandidate[],
): DescriptionProposal[] {
  const key = (candidate: FieldCandidate) => `${candidate.source}:${candidate.sourceRecordUid}`;
  const proposals = new Map<string, DescriptionProposal>();
  const proposalFor = (candidate: FieldCandidate): DescriptionProposal => {
    const existing = proposals.get(key(candidate));
    if (existing) return existing;
    const fiscalYear = member(candidate.valueJson, "fiscal_year");
    const created: DescriptionProposal = {
      key: key(candidate),
      source: candidate.source,
      sourceLabel: sourceLabel(candidate.source),
      meta: fiscalYear === "" ? observedDate(candidate.observedAt) : `fiscal ${fiscalYear}`,
      english: "",
      original: "",
      originalLanguage: "",
      sourceRecordUid: candidate.sourceRecordUid,
      observedAt: candidate.observedAt,
    };
    proposals.set(created.key, created);
    return created;
  };
  for (const candidate of description) {
    const proposal = proposalFor(candidate);
    const language = member(candidate.valueJson, "language");
    if (language === "en" || language.startsWith("en-")) {
      proposal.english = candidate.value;
    } else {
      proposal.original = candidate.value;
      proposal.originalLanguage = language;
    }
  }
  for (const candidate of descriptionSv) {
    const proposal = proposalFor(candidate);
    proposal.original = candidate.value;
    proposal.originalLanguage = "sv";
  }
  return [...proposals.values()];
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/se-company-field-groups.test.ts tests/se-company-fields.server.test.ts && npm run typecheck`
Expected: PASS, typecheck clean.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-company-field-groups.ts corpscout/services/backoffice/tests/fixtures/se-field-registry.ts corpscout/services/backoffice/tests/se-company-field-groups.test.ts
git commit -m "feat(backoffice): group, label and format registry fields for the SE info page" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: the `edit-field` intent

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-info-field-value-form.ts` (the `SeInfoFieldValueContext` interface at :47-50, the `switch` in `buildFieldValueInputs` at :215-226)
- Test: `corpscout/services/backoffice/tests/se-info-field-value-form.test.ts`

**Interfaces:**
- Consumes: `FieldRegistry` type (plan 4). Plan 4 made the validator registry-driven; whether it put the registry on `SeInfoFieldValueContext` under the name `registry` is checked in Step 3 -- if it did, the interface edit is a no-op.
- Produces: intent `edit-field` with form fields `field`, `value`, `original_value`, `clear` (= `"yes"`), `note`. Refusals: `"Unknown field."` (not in the registry), `"Use the description editor for this field."` (`description` / `description_sv`), `"Nothing changed."` (absent `original_value`, or unchanged text). `SeInfoFieldValueContext.registry: FieldRegistry`.

- [ ] **Step 1: Write the failing tests** (append to `tests/se-info-field-value-form.test.ts`; the `build` helper at :33-36 passes `{ companyId, suggestions }` -- extend it to pass `registry: REGISTRY` too, importing `REGISTRY` from `./fixtures/se-field-registry`)

```ts
describe("buildFieldValueInputs -- edit-field", () => {
  it("writes the reviewer's value for one registry field when it changed", () => {
    expect(
      build({ intent: "edit-field", field: "legal_name", value: "  Alpha Aktiebolag  ", original_value: "Alpha AB", note: "Registered name" }),
    ).toEqual({
      ok: true,
      inputs: [{ companyId: COMPANY_ID, field: "legal_name", value: "Alpha Aktiebolag", source: "reviewer", note: "Registered name" }],
    });
  });

  it("writes a release row for a ticked clear box, which beats the text", () => {
    expect(
      build({ intent: "edit-field", field: "website", value: "https://elsewhere.se", original_value: "https://alpha.se", clear: "yes", note: "" }),
    ).toEqual({ ok: true, inputs: [{ companyId: COMPANY_ID, field: "website", value: null, source: "reviewer", note: "" }] });
  });

  it("refuses unchanged text and a post with no original to diff against", () => {
    expect(build({ intent: "edit-field", field: "status", value: " active ", original_value: "active" })).toEqual({ ok: false, error: "Nothing changed." });
    expect(build({ intent: "edit-field", field: "status", value: "dissolved" })).toEqual({ ok: false, error: "Nothing changed." });
  });

  it("emits an empty value for an emptied input without the box, for the validator to refuse", () => {
    expect(build({ intent: "edit-field", field: "status", value: "   ", original_value: "active" })).toEqual({
      ok: true,
      inputs: [{ companyId: COMPANY_ID, field: "status", value: "", source: "reviewer", note: "" }],
    });
  });

  it("refuses a field outside the registry, the projection row, and the description pair", () => {
    expect(build({ intent: "edit-field", field: "hearsay", value: "x", original_value: "" })).toEqual({ ok: false, error: "Unknown field." });
    expect(build({ intent: "edit-field", field: "*", value: "x", original_value: "" })).toEqual({ ok: false, error: "Unknown field." });
    for (const field of ["description", "description_sv"]) {
      expect(build({ intent: "edit-field", field, value: "x", original_value: "" })).toEqual({
        ok: false,
        error: "Use the description editor for this field.",
      });
    }
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/se-info-field-value-form.test.ts`
Expected: FAIL -- the new describe's five cases get `{ ok: false, error: "Unknown info action." }` (and a type error on `registry` in the context if plan 4 did not add it).

- [ ] **Step 3: Implement**

In `app/lib/se-info-field-value-form.ts`, ensure the context carries the registry (skip if plan 4 already declares `registry: FieldRegistry` on it):

```ts
import type { FieldRegistry } from "~/lib/se-company-field-registry.server";

export interface SeInfoFieldValueContext {
  companyId: string;
  suggestions: SeCompanyInfoSuggestionRow[];
  /** The registry export: which fields exist. Read, never edited, here. */
  registry: FieldRegistry;
}
```

Add the builder above `buildFieldValueInputs` and the `case`:

```ts
const DESCRIPTION_PAIR: readonly string[] = ["description", "description_sv"];

/**
 * The reviewer's own value for ONE non-description field (a name, a code, a
 * date, a URL), diffed against the value the page rendered it with -- the same
 * absent-original guard `edit` has: a post that forgot `original_value` would
 * otherwise pin today's resolved value as a permanent reviewer value. The
 * description pair keeps its two-language editor (`edit`), so it is refused
 * here rather than half-edited.
 */
function editField(
  form: FormData,
  context: SeInfoFieldValueContext,
): SeInfoFieldValueRequest {
  const field = text(form, "field");
  const known = context.registry.fields.some(
    (entry) => entry.field === field && entry.field !== "*",
  );
  if (!known) return refuse("Unknown field.");
  if (DESCRIPTION_PAIR.includes(field)) {
    return refuse("Use the description editor for this field.");
  }
  const note = text(form, "note");
  if (text(form, "clear") === "yes") {
    return {
      ok: true,
      inputs: [{ companyId: context.companyId, field, value: null, source: "reviewer", note }],
    };
  }
  if (!form.has("original_value")) return refuse("Nothing changed.");
  const value = text(form, "value").trim();
  if (value === text(form, "original_value").trim()) return refuse("Nothing changed.");
  return {
    ok: true,
    inputs: [{ companyId: context.companyId, field, value, source: "reviewer", note }],
  };
}
```

```ts
    case "edit-field":
      return editField(form, context);
```

Update the module doc comment's intent list (":9-12") to add `- \`edit-field\`  the reviewer's own value for one non-description field`.

- [ ] **Step 4: Run the tests and typecheck**

Run: `npx vitest run tests/se-info-field-value-form.test.ts tests/admin-se-company-info.test.tsx && npm run typecheck`
Expected: PASS. If typecheck reports the route action's `buildFieldValueInputs(form, { companyId, suggestions })` call missing `registry`, add `registry: await loadFieldRegistry()` there now (Task 6 rewrites that action in full; this keeps the tree green in between):

```ts
import { loadFieldRegistry } from "~/lib/se-company-field-registry.server";
// in action():
  const registry = await loadFieldRegistry();
  const built = buildFieldValueInputs(form, {
    companyId: params.companyId,
    suggestions: detail.suggestions,
    registry,
  });
```

and in `tests/admin-se-company-info.test.tsx` add the mock beside the existing `server` mock:

```ts
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);
// in the action describe's beforeEach:
registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY);
```

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-info-field-value-form.ts corpscout/services/backoffice/tests/se-info-field-value-form.test.ts corpscout/services/backoffice/app/routes/admin-se-company-info.tsx corpscout/services/backoffice/tests/admin-se-company-info.test.tsx
git commit -m "feat(backoffice): edit-field intent for single-value registry fields" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---
### Task 4: `FieldGroupCard`

**Files:**
- Create: `corpscout/services/backoffice/app/components/admin/field-group-card.tsx`
- Test: `corpscout/services/backoffice/tests/field-group-card.test.tsx`

**Interfaces:**
- Consumes: `FieldGroup`, `DESCRIPTION_FIELDS`, `fieldLabel`, `formatFieldValue`, `sourceLabel`, `observedDate` (Task 2); `ResolvedField`, `FieldCandidate` types (Task 1); `SeCompanyInfoFieldValueRow` type; `Card*`, `Badge`, `Button`, `Collapsible`, `CollapsibleTrigger`, `CollapsibleContent` from `~/components/ui/`.
- Produces:
  ```tsx
  export interface FieldGroupCardProps {
    group: FieldGroup;
    resolved: Map<string, ResolvedField>;
    candidates: Map<string, FieldCandidate[]>;
    decisions: SeCompanyInfoFieldValueRow[];
    renderUseThis: (field: string, candidate: FieldCandidate) => ReactNode;
    renderEdit: (field: string) => ReactNode;
    renderRelease: (field: string) => ReactNode;
    /** Rendered first, in place of the description / description_sv rows, when the group holds them. */
    descriptionCard?: ReactNode;
  }
  export function FieldGroupCard(props: FieldGroupCardProps): JSX.Element;
  export function sourceChip(resolved: ResolvedField, decisions: SeCompanyInfoFieldValueRow[]): string;
  ```
  Copy: absent value = `Not available` (italic); chip = `${sourceLabel(source)} · ${observedDate(observedAt)}` or `Reviewer decision · ${observedDate(decision.created_at)}`; python-only hint = `Applies on next run`; candidates trigger = `${n} candidate(s)`; winner badge = `winner`.

- [ ] **Step 1: Write the failing test**

```tsx
// tests/field-group-card.test.tsx
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { FieldGroupCard, sourceChip } from "~/components/admin/field-group-card";
import { groupFields } from "~/lib/se-company-field-groups";
import type { FieldCandidate, ResolvedField } from "~/lib/se-company-fields.server";
import type { SeCompanyInfoFieldValueRow } from "~/lib/se-company-info.server";
import { REGISTRY } from "./fixtures/se-field-registry";

const DECISION_ID = "22222222-2222-4222-8222-222222222222";
const [identity, , scale] = groupFields(REGISTRY);

const resolved = (field: string, value: string, over: Partial<ResolvedField> = {}): ResolvedField => ({
  field, value, valueJson: {}, source: "bolagsverket", sourceRecordUid: "bv:1",
  observedAt: "2026-08-01 00:00:00.000", decisionId: null, policyName: "source_precedence",
  policyVersion: "source_precedence-v1", candidateCount: 2, agreeingSources: ["bolagsverket", "scb"],
  resolvedAt: "2026-09-01 10:00:00.000", ...over,
});

const candidate = (field: string, source: string, uid: string, value: string, isWinner = false): FieldCandidate => ({
  field, source, sourceRecordUid: uid, value, valueJson: {},
  observedAt: "2026-08-01 00:00:00.000", extractedAt: "2026-08-30 00:00:00.000", isWinner,
});

const decision: SeCompanyInfoFieldValueRow = {
  value_id: DECISION_ID, field: "status", value: "dissolved", source: "reviewer", source_ref: "",
  source_at: null, decided_by: "backoffice", note: "", created_at: "2026-08-23 09:00:00.000", is_live: 1,
};

function render(over: Partial<Parameters<typeof FieldGroupCard>[0]> = {}) {
  return renderToStaticMarkup(
    <FieldGroupCard
      group={identity}
      resolved={new Map([
        ["legal_name", resolved("legal_name", "Alpha AB")],
        ["status", resolved("status", "dissolved", { source: "reviewer", sourceRecordUid: "", decisionId: DECISION_ID })],
      ])}
      candidates={new Map([
        ["legal_name", [candidate("legal_name", "bolagsverket", "bv:1", "Alpha AB", true), candidate("legal_name", "scb", "scb:1", "ALPHA AB")]],
      ])}
      decisions={[decision]}
      renderUseThis={(field, c) => <span data-use={`${field}:${c.source}:${c.sourceRecordUid}`} />}
      renderEdit={(field) => <span data-edit={field} />}
      renderRelease={(field) => <span data-release={field} />}
      {...over}
    />,
  );
}

describe("FieldGroupCard", () => {
  it("titles the group and renders one row per registry field, absent ones as Not available", () => {
    const html = render();
    expect(html).toContain("Identity");
    for (const label of ["Legal name", "Legal form", "Status", "Incorporation date"]) expect(html).toContain(label);
    expect(html).toContain("Alpha AB");
    // legal_form_code and incorporation_date have no resolved row.
    expect(html.match(/Not available/g)).toHaveLength(2);
    // Every field gets its Edit and Release slot, absent or not.
    for (const field of ["legal_name", "legal_form_code", "status", "incorporation_date"]) {
      expect(html).toContain(`data-edit="${field}"`);
      expect(html).toContain(`data-release="${field}"`);
    }
  });

  it("chips a policy-resolved value with its source and observed date, a decision with its decided date", () => {
    const html = render();
    expect(html).toContain("Bolagsverket · 2026-08-01");
    expect(html).toContain("Reviewer decision · 2026-08-23");
    expect(sourceChip(resolved("legal_name", "x"), [])).toBe("Bolagsverket · 2026-08-01");
    // A decision the history no longer lists (older than the 200-row window) still chips as a decision.
    expect(sourceChip(resolved("status", "x", { decisionId: DECISION_ID }), [])).toBe("Reviewer decision · 2026-09-01");
  });

  it("lists every candidate with source, date, value and the Use-this slot, winner badged, inside a collapsed list that SSR still renders", () => {
    const html = render();
    expect(html).toContain("2 candidates");
    expect(html).toContain('data-use="legal_name:bolagsverket:bv:1"');
    expect(html).toContain('data-use="legal_name:scb:scb:1"');
    expect(html).toContain("SCB register");
    expect(html).toContain("ALPHA AB");
    expect(html.match(/>winner</g)).toHaveLength(1);
    expect(html).toContain('hidden="until-found"');
    // A field with no candidates says so instead of offering an empty list.
    expect(html).toContain("No candidates");
  });

  it("hints that a python-only field's edit applies on the next run", () => {
    const html = renderToStaticMarkup(
      <FieldGroupCard group={scale} resolved={new Map()} candidates={new Map()} decisions={[]}
        renderUseThis={() => null} renderEdit={(f) => <span data-edit={f} />} renderRelease={() => null} />,
    );
    // latest_revenue is pythonOnly in the fixture; website and employee_count are not.
    expect(html.match(/Applies on next run/g)).toHaveLength(1);
    expect(html.indexOf('data-edit="latest_revenue"')).toBeLessThan(html.indexOf("Applies on next run"));
  });

  it("renders the description card first and drops the pair's own rows", () => {
    const [, activity] = groupFields(REGISTRY);
    const html = renderToStaticMarkup(
      <FieldGroupCard group={activity} resolved={new Map()} candidates={new Map()} decisions={[]}
        renderUseThis={() => null} renderEdit={(f) => <span data-edit={f} />} renderRelease={() => null}
        descriptionCard={<section data-description-card="" />} />,
    );
    expect(html).toContain('data-description-card=""');
    expect(html.indexOf("data-description-card")).toBeLessThan(html.indexOf("SNI code"));
    expect(html).not.toContain('data-edit="description"');
    expect(html).not.toContain('data-edit="description_sv"');
    expect(html).toContain('data-edit="industry_label_en"');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/field-group-card.test.tsx`
Expected: FAIL -- `Cannot find module '~/components/admin/field-group-card'`.

- [ ] **Step 3: Write the component**

```tsx
// app/components/admin/field-group-card.tsx
import type { ReactNode } from "react";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "~/components/ui/collapsible";
import { buttonVariants } from "~/components/ui/button";
import {
  DESCRIPTION_FIELDS,
  fieldLabel,
  formatFieldValue,
  observedDate,
  sourceLabel,
  type FieldGroup,
} from "~/lib/se-company-field-groups";
import type { FieldCandidate, ResolvedField } from "~/lib/se-company-fields.server";
import type { SeCompanyInfoFieldValueRow } from "~/lib/se-company-info.server";
import type { FieldRegistryEntry } from "~/lib/se-company-field-registry.server";

export interface FieldGroupCardProps {
  group: FieldGroup;
  resolved: Map<string, ResolvedField>;
  candidates: Map<string, FieldCandidate[]>;
  decisions: SeCompanyInfoFieldValueRow[];
  renderUseThis: (field: string, candidate: FieldCandidate) => ReactNode;
  renderEdit: (field: string) => ReactNode;
  renderRelease: (field: string) => ReactNode;
  /** Rendered first, in place of the description / description_sv rows,
   * when the group holds them (the Activity group). */
  descriptionCard?: ReactNode;
}

/** What decided the value: the winning source and when it observed the value,
 * or the reviewer and when they decided. The decision's own date comes from
 * the history when it is still listed there, else from the resolve stamp. */
export function sourceChip(
  resolved: ResolvedField,
  decisions: SeCompanyInfoFieldValueRow[],
): string {
  if (resolved.decisionId !== null) {
    const decision = decisions.find((row) => row.value_id === resolved.decisionId);
    return `Reviewer decision · ${observedDate(decision?.created_at ?? resolved.resolvedAt)}`;
  }
  return `${sourceLabel(resolved.source)} · ${observedDate(resolved.observedAt)}`;
}

function CandidateList({
  entry,
  rows,
  renderUseThis,
}: {
  entry: FieldRegistryEntry;
  rows: FieldCandidate[];
  renderUseThis: FieldGroupCardProps["renderUseThis"];
}) {
  if (rows.length === 0) {
    return <span className="text-xs text-muted-foreground">No candidates</span>;
  }
  return (
    <Collapsible>
      <CollapsibleTrigger
        className={buttonVariants({ variant: "ghost", size: "sm" })}
      >
        {rows.length === 1 ? "1 candidate" : `${rows.length} candidates`}
      </CollapsibleTrigger>
      {/* hiddenUntilFound keeps the list in the document while closed: SSR
          renders it, find-in-page opens it, and every Use-this form exists
          before any click. */}
      <CollapsibleContent hiddenUntilFound>
        <ul className="mt-2 flex flex-col gap-2">
          {rows.map((candidate) => (
            <li
              key={`${candidate.source}:${candidate.sourceRecordUid}`}
              className="flex flex-wrap items-center gap-2 rounded-lg border p-2 text-sm"
            >
              <Badge variant="secondary">{sourceLabel(candidate.source)}</Badge>
              <span className="text-xs text-muted-foreground">
                {observedDate(candidate.observedAt)}
              </span>
              {candidate.isWinner ? <Badge>winner</Badge> : null}
              <span className="max-w-[70ch] whitespace-pre-wrap">
                {formatFieldValue(entry, {
                  ...candidate,
                  decisionId: null,
                  policyName: "",
                  policyVersion: "",
                  candidateCount: 0,
                  agreeingSources: [],
                  resolvedAt: candidate.extractedAt,
                })}
              </span>
              <span className="ml-auto">{renderUseThis(entry.field, candidate)}</span>
            </li>
          ))}
        </ul>
      </CollapsibleContent>
    </Collapsible>
  );
}

function FieldRow({
  entry,
  resolved,
  candidates,
  decisions,
  renderUseThis,
  renderEdit,
  renderRelease,
}: {
  entry: FieldRegistryEntry;
  resolved: ResolvedField | undefined;
  candidates: FieldCandidate[];
  decisions: SeCompanyInfoFieldValueRow[];
} & Pick<FieldGroupCardProps, "renderUseThis" | "renderEdit" | "renderRelease">) {
  const shown = formatFieldValue(entry, resolved);
  return (
    <div className="flex flex-col gap-2 border-b py-3 last:border-b-0">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <dt className="min-w-[11rem] text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {fieldLabel(entry.field)}
        </dt>
        <dd className="flex flex-wrap items-center gap-2 text-sm">
          {resolved === undefined || shown === "" ? (
            <span className="italic text-muted-foreground">Not available</span>
          ) : entry.valueType === "url" ? (
            <a className="underline underline-offset-2 break-all" href={shown} target="_blank" rel="noreferrer">
              {shown}
            </a>
          ) : (
            <span className="font-medium">{shown}</span>
          )}
          {resolved ? (
            <Badge variant="outline">{sourceChip(resolved, decisions)}</Badge>
          ) : null}
        </dd>
      </div>
      <div className="flex flex-wrap items-center gap-2 pl-0 sm:pl-[calc(11rem+0.75rem)]">
        {renderEdit(entry.field)}
        {entry.pythonOnly ? (
          <span className="text-xs text-muted-foreground">Applies on next run</span>
        ) : null}
        {renderRelease(entry.field)}
        <CandidateList entry={entry} rows={candidates} renderUseThis={renderUseThis} />
      </div>
    </div>
  );
}

/**
 * One display group of the registry: a row per field with its resolved value,
 * the chip saying what decided it, the reviewer's Edit / Release slots and the
 * collapsed list of every candidate with a Use-this slot each. The description
 * pair is handed in as a card of its own and leads the group.
 */
export function FieldGroupCard({
  group,
  resolved,
  candidates,
  decisions,
  renderUseThis,
  renderEdit,
  renderRelease,
  descriptionCard,
}: FieldGroupCardProps) {
  const pair = new Set<string>(DESCRIPTION_FIELDS);
  const rows = descriptionCard
    ? group.fields.filter((entry) => !pair.has(entry.field))
    : group.fields;
  return (
    <Card>
      <CardHeader>
        <CardTitle>{group.label}</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {descriptionCard ?? null}
        <dl className="flex flex-col">
          {rows.map((entry) => (
            <FieldRow
              key={entry.field}
              entry={entry}
              resolved={resolved.get(entry.field)}
              candidates={candidates.get(entry.field) ?? []}
              decisions={decisions}
              renderUseThis={renderUseThis}
              renderEdit={renderEdit}
              renderRelease={renderRelease}
            />
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 4: Run the test and typecheck**

Run: `npx vitest run tests/field-group-card.test.tsx && npm run typecheck`
Expected: PASS (5 tests). If `CollapsibleTrigger` rejects `className` from `buttonVariants`, keep the class and drop nothing else -- `CollapsiblePrimitive.Trigger.Props` extends the button's HTML props.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/components/admin/field-group-card.tsx corpscout/services/backoffice/tests/field-group-card.test.tsx
git commit -m "feat(backoffice): FieldGroupCard renders one registry display group" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 5: the detail loader stops reading the legacy columns

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-company-info.server.ts` (`SeCompanyInfoRow` :19-63, `INFO_SQL` :140-171, `SUGGESTIONS_SQL` :238-257, `loadSeCompanyInfoDetail` :345-368)
- Test: `corpscout/services/backoffice/tests/se-company-info.server.test.ts` (:103-124 `INFO_SQL` pins, :145-160 `SUGGESTIONS_SQL` pins, :349-433 loader cases)

**Interfaces:**
- Produces: `SeCompanyInfoRow` without `llm_enhanced`, `description_sources`, `description_source_record_uids`, `description_source_count`, `suggestion_id`, `model_provider`, `model_name`, `prompt_version`, `correction_ids`; `SUGGESTIONS_SQL` takes only `{companyId:String}`; `loadSeCompanyInfoDetail(companyId)` unchanged in signature. `is_published` now means "the resolved `description` row is this LLM candidate".

- [ ] **Step 1: Rewrite the failing pins**

In `tests/se-company-info.server.test.ts` replace the `INFO_SQL` loop at :106-123 and the two `SUGGESTIONS_SQL` pins at :149-160 with:

```ts
    for (const c of [
      "toString(i.evidence_set_hash) AS evidence_set_hash",
      "i.description AS description",
      "i.description_sv AS description_sv",
      "i.legal_form_label_en AS legal_form_label_en",
      "i.legal_form_label_sv AS legal_form_label_sv",
    ]) {
      expect(INFO_SQL).toContain(c);
    }
    // Phase B (spec 8.3): the provenance the page needs lives in
    // se_company_field / se_company_field_candidate now; the wide row's legacy
    // columns are dropped after this page ships, so nothing may read them.
    for (const legacy of [
      "llm_enhanced", "description_sources", "description_source_record_uids",
      "description_source_count", "suggestion_id", "model_provider", "model_name",
      "prompt_version", "correction_ids",
    ]) {
      expect(INFO_SQL).not.toContain(`i.${legacy}`);
    }
    expect(INFO_SQL).not.toContain("description_source AS");
```

```ts
    // A suggestion is published when the resolved description row is that LLM
    // candidate: IN over the (possibly empty) set, never a scalar subquery that
    // could be NULL for a company with no LLM row.
    expect(SUGGESTIONS_SQL).toContain(
      `toUInt8(s.suggestion_id IN (
    SELECT toUUIDOrZero(source_record_uid)
    FROM corpscout.se_company_field FINAL
    WHERE company_id = {companyId:String} AND field = 'description' AND source = 'llm'
  )) AS is_published`,
    );
    expect(SUGGESTIONS_SQL).not.toContain("publishedSuggestionId");
    expect(SUGGESTIONS_SQL).toContain(
      `toUInt8(s.suggestion_id = (
    SELECT suggestion_id
    FROM corpscout.se_company_info_enrichment_observation
    WHERE company_id = {companyId:String}
    ORDER BY created_at DESC, suggestion_id DESC
    LIMIT 1
  )) AS is_newest`,
    );
```

In the loader cases (:349-433, :435-470) drop `suggestion_id` and `correction_ids` from every mocked `INFO_SQL` row and change the third-call assertion at :423-426 to `expect(clickhouse.query).toHaveBeenNthCalledWith(3, SUGGESTIONS_SQL, { companyId: COMPANY });`.

- [ ] **Step 2: Run to verify they fail**

Run: `npx vitest run tests/se-company-info.server.test.ts`
Expected: FAIL on the `not.toContain("i.llm_enhanced")` pin and the `is_published` pin.

- [ ] **Step 3: Implement**

`SeCompanyInfoRow`: delete the nine members (and their doc comments :37-48, :56-60). `INFO_SQL`: delete lines :151-154 and :162-166 (`toUInt8(i.llm_enhanced) ...` through `i.description_source_count ...`, and `arrayMap(id -> toString(id), i.correction_ids) ...` through `i.prompt_version ...`), and trim its doc comment to:

```ts
/**
 * Every column is aliased explicitly so the projected shape never depends on
 * ClickHouse's own naming for a wrapped expression; the MATERIALIZED
 * FixedString evidence_set_hash and the LowCardinality status /
 * description_language are wrapped in toString() for one predictable JSON
 * shape. The description-provenance columns (llm_enhanced, description_sources,
 * suggestion_id, model_*, correction_ids) are NOT read: the page takes that
 * from se_company_field, and the columns are dropped once it does.
 */
```

`SUGGESTIONS_SQL`: replace the `is_published` expression with

```sql
  toUInt8(s.suggestion_id IN (
    SELECT toUUIDOrZero(source_record_uid)
    FROM corpscout.se_company_field FINAL
    WHERE company_id = {companyId:String} AND field = 'description' AND source = 'llm'
  )) AS is_published,
```

and its doc comment's last sentence to `is_published is membership in the resolved description row's LLM candidate (source_record_uid is the suggestion id by the extractor contract; toUUIDOrZero guards a malformed one).` In `loadSeCompanyInfoDetail` change the call to `chQuery<SeCompanyInfoSuggestionRow>(SUGGESTIONS_SQL, { companyId })`.

- [ ] **Step 4: Run the suite and typecheck**

Run: `npx vitest run tests/se-company-info.server.test.ts && npm run typecheck`
Expected: the server test PASSES. Typecheck FAILS in `se-company-info-review-workspace.tsx` (`info.llm_enhanced`, `info.description_sources`, ...) and in `tests/admin-se-company-info.test.tsx`'s fixture -- expected, Task 6 rewrites both. Do not commit a red typecheck: proceed straight into Task 6 and commit both together, or (if executing with a reviewer gate) commit this task with `git commit --no-verify` is NOT allowed -- instead stub the workspace by deleting `PublishedCard` (:237-391) and its `<PublishedCard info={info} />` call (:835), replacing `contributing` (:747) with `new Set<string>()`, and deleting the nine members from the test fixture (:46-51, :59-63); then typecheck is green and Task 6 finishes the rewrite.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-company-info.server.ts corpscout/services/backoffice/tests/se-company-info.server.test.ts corpscout/services/backoffice/app/components/admin/se-company-info-review-workspace.tsx corpscout/services/backoffice/tests/admin-se-company-info.test.tsx
git commit -m "refactor(backoffice): SE info detail stops reading the wide row's provenance columns" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---
### Task 6: the page -- loader, action, workspace, route tests

**Files:**
- Modify: `corpscout/services/backoffice/app/routes/admin-se-company-info.tsx` (whole file, 84 lines)
- Modify: `corpscout/services/backoffice/app/components/admin/se-company-info-review-workspace.tsx` (imports :1-65; delete `PublishedCard` :237-391, `Fact` :408-424, `CompanyFactsCard` :426-490; `FinalDescriptionEditor` :590-642 takes strings; `ValueHistoryCard` :654-734 loses its release row :669-692; `SeCompanyInfoReviewWorkspace` :736-966 rewritten)
- Modify: `corpscout/services/backoffice/app/lib/se-info-field-value-form.ts` (`useSource` :85-118 -- only if it still restricts `source` to `ARTIFACT_SOURCES`)
- Test: `corpscout/services/backoffice/tests/admin-se-company-info.test.tsx` (rewritten), `corpscout/services/backoffice/tests/se-info-field-value-form.test.ts` (:87-114 use-source refusals)

**Interfaces:**
- Consumes: `loadCompanyFields`, `CompanyFields`, `FieldCandidate`, `ResolvedField` (Task 1); `groupFields`, `candidateDescriptionProposals`, `fieldLabel`, `sourceLabel`, `DESCRIPTION_FIELDS` (Task 2); `buildFieldValueInputs` with `registry` in its context and the `edit-field` intent (Task 3); `FieldGroupCard`, `sourceChip` (Task 4); trimmed `SeCompanyInfoDetail` (Task 5); `loadFieldRegistry` (plan 4); `resolveCompanyFields(companyId, fields, opts?)` (plan 4 -- keep the exact call plan 4's action already makes; the shape below is the documented signature).
- Produces: `SeCompanyInfoReviewWorkspace({ detail, fields, result })`; loader data `{ detail, fields }`; every field on the page has a `release` form, every non-description field an `edit-field` form, every candidate a `use-source` form.

- [ ] **Step 1: Rewrite the failing route test**

Replace `tests/admin-se-company-info.test.tsx` with this file. The `detail` fixture is the current one (:34-182) minus the nine legacy members; keep the `formContaining` helper (:206-214) and the whole action `describe` (:775-934) verbatim below the new page cases, adding the registry mock and the one new `edit-field` case shown.

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSeCompanyInfoDetail: vi.fn(),
  appendSeCompanyInfoFieldValues: vi.fn(),
}));
vi.mock("~/lib/se-company-info.server", () => server);
const fieldsModule = vi.hoisted(() => ({ loadCompanyFields: vi.fn() }));
vi.mock("~/lib/se-company-fields.server", () => fieldsModule);
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);
const resolveModule = vi.hoisted(() => ({ resolveCompanyFields: vi.fn() }));
vi.mock("~/lib/se-company-field-resolve.server", () => resolveModule);

import { action, loader } from "~/routes/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import type { SeCompanyInfoDetail } from "~/lib/se-company-info.server";
import type { CompanyFields, FieldCandidate, ResolvedField } from "~/lib/se-company-fields.server";
import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";
import { REGISTRY } from "./fixtures/se-field-registry";

const COMPANY_ID = "5565200028";
const EVIDENCE_HASH = "e".repeat(64);
const SUGGESTION_ID = "11111111-1111-4111-8111-111111111111";
const LIVE_VALUE_ID = "22222222-2222-4222-8222-222222222222";
const RELEASED_VALUE_ID = "33333333-3333-4333-8333-333333333333";

const detail: SeCompanyInfoDetail = {
  info: {
    company_id: COMPANY_ID,
    legal_name: "Alpha AB",
    legal_form_code: "AB-ORGFO",
    legal_form_label_en: "Limited company (aktiebolag)",
    legal_form_label_sv: "Aktiebolag",
    status: "active",
    incorporation_date: "2001-02-03",
    description: "Alpha builds payment software.",
    description_sv: "Alpha bygger betalprogramvara.",
    description_language: "en",
    primary_nace_code: "62.01",
    primary_sni_code: "62010",
    wikidata_id: "Q1",
    lei: null,
    source_record_uids: ["scb:1", "wikidata:Q1"],
    evidence_hashes: ["a".repeat(64), "c".repeat(64)],
    evidence_set_hash: EVIDENCE_HASH,
    source_run_id: "run-1",
    resolved_at: "2026-08-22 09:00:00.000",
  },
  artifacts: [ /* the three artifact rows of the current fixture, :69-137, unchanged */ ],
  suggestions: [ /* the one suggestion of the current fixture, :138-151, unchanged */ ],
  fieldValues: [ /* the two rows of the current fixture, :155-180, unchanged */ ],
  naceLabel: "Computer programming activities",
};

const resolved = (field: string, value: string, over: Partial<ResolvedField> = {}): ResolvedField => ({
  field, value, valueJson: {}, source: "scb", sourceRecordUid: "scb:1",
  observedAt: "2026-08-01 00:00:00.000", decisionId: null, policyName: "source_precedence",
  policyVersion: "source_precedence-v1", candidateCount: 1, agreeingSources: ["scb"],
  resolvedAt: "2026-09-01 10:00:00.000", ...over,
});
const candidate = (field: string, source: string, uid: string, value: string, over: Partial<FieldCandidate> = {}): FieldCandidate => ({
  field, source, sourceRecordUid: uid, value, valueJson: {},
  observedAt: "2026-08-01 00:00:00.000", extractedAt: "2026-08-30 00:00:00.000", isWinner: false, ...over,
});

const fields: CompanyFields = {
  registry: REGISTRY,
  resolved: new Map([
    ["legal_name", resolved("legal_name", "Alpha AB", { source: "bolagsverket", sourceRecordUid: "bv:1" })],
    ["status", resolved("status", "active")],
    ["incorporation_date", resolved("incorporation_date", "2001-02-03")],
    // The live decision on the English description: the SCB text, copied.
    ["description", resolved("description", "Alpha builds payment software.", { decisionId: LIVE_VALUE_ID })],
    ["description_sv", resolved("description_sv", "Alpha bygger betalprogramvara.", { source: "llm", sourceRecordUid: SUGGESTION_ID })],
    ["primary_nace_code", resolved("primary_nace_code", "62.01")],
    ["primary_sni_code", resolved("primary_sni_code", "62010")],
    ["employee_count", resolved("employee_count", "120", { source: "wikidata", sourceRecordUid: "wikidata:Q1", valueJson: { count: 120, as_of: "2025-12-31" } })],
  ]),
  candidates: new Map([
    ["legal_name", [
      candidate("legal_name", "bolagsverket", "bv:1", "Alpha AB", { isWinner: true }),
      candidate("legal_name", "scb", "scb:1", "ALPHA AB"),
    ]],
    ["description", [
      candidate("description", "llm", SUGGESTION_ID, "Alpha builds payment software.", { valueJson: { language: "en" }, observedAt: "2026-08-22 08:59:00.000" }),
      candidate("description", "esef", "esef:doc-1", "Alpha provides payment infrastructure.", { valueJson: { language: "en", fiscal_year: "2025" }, observedAt: "2026-08-02 00:00:00.000" }),
      candidate("description", "wikidata", "wikidata:Q1", "Swedish fintech company"),
      candidate("description", "scb", "scb:1", "IT consultancy.", { valueJson: { language: "en" }, isWinner: true }),
    ]],
    ["description_sv", [
      candidate("description_sv", "llm", SUGGESTION_ID, "Alpha AB bygger betalprogramvara i Sverige.", { observedAt: "2026-08-22 08:59:00.000", isWinner: true }),
      candidate("description_sv", "scb", "scb:1", "IT-konsulter."),
    ]],
    ["employee_count", [candidate("employee_count", "wikidata", "wikidata:Q1", "120", { valueJson: { count: 120, as_of: "2025-12-31" }, isWinner: true })]],
  ]),
  decisions: detail.fieldValues,
};

function render(
  workspaceDetail: SeCompanyInfoDetail = detail,
  result: Parameters<typeof SeCompanyInfoReviewWorkspace>[0]["result"] = null,
  workspaceFields: CompanyFields = fields,
) {
  const router = createMemoryRouter(
    [{ path: "*", element: <SeCompanyInfoReviewWorkspace detail={workspaceDetail} fields={workspaceFields} result={result} />, action: () => null }],
    { initialEntries: ["/admin/se/company/5565200028/info"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

function formContaining(html: string, needle: string): string { /* :206-214 verbatim */ }

describe("company info review page (registry-driven)", () => {
  it("lays the page out as identity, activity (description card first), scale, suggestions, value history, sources drawer", () => {
    const html = render();
    let cursor = -1;
    for (const heading of ["Identity", "Activity", "About the company", "SNI code", "Scale", "Model suggestions", "Value history", "Sources"]) {
      const at = html.indexOf(heading, cursor + 1);
      expect(at, `${heading} in order`).toBeGreaterThan(cursor);
      cursor = at;
    }
    for (const gone of ["Published version", "Company facts", "Additional information", "LLM enhanced", "Description sources", "contributes to description"]) {
      expect(html).not.toContain(gone);
    }
    expect(html).toContain('data-source-strip="SCB,ESEF,Wikidata"');
  });

  it("shows every registry field with its resolved value or Not available, chipped by source or decision", () => {
    const html = render();
    expect(html).toContain("Bolagsverket · 2026-08-01");
    expect(html).toContain("120 (as of 2025-12-31)");
    expect(html).toContain("Wikidata · 2026-08-01");
    // legal_form_code, industry_label_en, website, latest_revenue have no resolved row.
    expect(html.match(/Not available/g)).toHaveLength(4);
    expect(html).toContain("Applies on next run");
  });

  it("offers Use this on every candidate, posting use-source with the candidate's record and moment", () => {
    const html = render();
    const scb = formContaining(html, 'name="source_ref" value="scb:1"');
    expect(scb).toContain('name="intent" value="use-source"');
    expect(scb).toContain('name="source" value="scb"');
    const bv = formContaining(html, 'name="source_ref" value="bv:1"');
    expect(bv).toContain('name="field" value="legal_name"');
    expect(bv).toContain('name="value" value="Alpha AB"');
    expect(bv).toContain('name="source_at" value="2026-08-01 00:00:00.000"');
    expect(html).toContain('aria-label="Use the SCB register value for Legal name"');
    // legal_name and employee_count: the description pair's candidates render in
    // the description card's menu, which has no winner badge.
    expect(html.match(/>winner</g)).toHaveLength(2);
  });

  it("builds the description card from candidates, resolved pair first, with the language toggle and Use this per option", () => {
    const html = render();
    const menuStart = html.indexOf("About the company");
    expect(html.indexOf("Resolved", menuStart)).toBeLessThan(html.indexOf("LLM", menuStart));
    expect(html).toContain('aria-label="Show english descriptions"');
    expect(html).toContain("ESEF filing");
    expect(html).toContain("Alpha provides payment infrastructure.");
    // The LLM candidate is a source option now: use-source with the suggestion id as its record.
    const llm = formContaining(html, `name="source_ref" value="${SUGGESTION_ID}"`);
    expect(llm).toContain('name="intent" value="use-source"');
    expect(llm).toContain('name="source" value="llm"');
    expect(llm).toContain('name="field" value="description"');
    expect(html).toContain('aria-label="Use the SCB register text as the English description"');
    // The resolved option carries the decision chip and the editor, not a copy button.
    expect(html).toContain("Reviewer decision · 2026-08-23");
    expect(html).not.toContain('name="source" value="resolved"');
  });

  it("keeps the two-language editor on the resolved option and puts an inline edit-field form on every other field", () => {
    const html = render();
    const editForm = formContaining(html, 'name="intent" value="edit"');
    expect(editForm).toContain('name="original_description" value="Alpha builds payment software."');
    expect(editForm).toContain('name="original_description_sv" value="Alpha bygger betalprogramvara."');
    expect(html.match(/name="intent" value="edit-field"/g)).toHaveLength(REGISTRY.fields.length - 2);
    const status = formContaining(html, 'name="field" value="status"');
    expect(status).toContain('name="intent" value="edit-field"');
    expect(status).toContain('name="original_value" value="active"');
    expect(status).toContain('name="value"');
    expect(status).toContain('name="clear" value="yes"');
    expect(status).toContain('name="note"');
    const website = formContaining(html, 'name="field" value="website"');
    expect(website).toContain('name="original_value" value=""');
  });

  it("offers Release on every registry field, once, and nowhere else", () => {
    const html = render();
    expect(html.match(/name="intent" value="release"/g)).toHaveLength(REGISTRY.fields.length);
    const sv = formContaining(html, 'aria-label="Release description_sv to the pipeline"');
    expect(sv).toContain('name="intent" value="release"');
    expect(sv).toContain('name="field" value="description_sv"');
  });

  it("keeps the value history (rows only) and the suggestions card", () => {
    const html = render();
    const history = html.slice(html.indexOf("Value history"));
    expect(history).toContain("released to pipeline");
    expect(history).toContain("Copied from the register.");
    expect(history.match(/>live</g)).toHaveLength(1);
    const use = formContaining(html, 'name="intent" value="use-suggestion"');
    expect(use).toContain(`name="suggestion_id" value="${SUGGESTION_ID}"`);
    expect(html).toContain("Swedish: Alpha AB bygger betalprogramvara i Sverige.");
  });

  it("folds the artifact cards into a collapsed Sources drawer that SSR still renders", () => {
    const html = render();
    const drawer = html.slice(html.indexOf("Sources · 3 artifact rows"));
    expect(drawer).toContain('hidden="until-found"');
    expect(drawer).toContain("IT-konsulter.");
    expect(drawer).toContain("<li>Payment terminals");
    expect(drawer).toContain('title="when the pipeline recorded this version"');
    // Which artifacts fed the description now comes from the candidates, not a wide-row list.
    expect(drawer.match(/description candidate/g)).toHaveLength(3); // scb:1, wikidata:Q1, esef:doc-1
  });

  it("renders a company with nothing resolved and no candidates", () => {
    const html = render(
      { ...detail, info: { ...detail.info, description: null, description_sv: null }, artifacts: [], fieldValues: [] },
      null,
      { ...fields, resolved: new Map(), candidates: new Map(), decisions: [] },
    );
    expect(html.match(/Not available/g)).toHaveLength(REGISTRY.fields.length - 2);
    expect(html).toContain("No description published.");
    expect(html).toContain(">Edit<");
    expect(html).toContain("No candidates");
    expect(html).toContain("No source artifacts.");
    expect(html).toContain("No values decided yet.");
  });

  it("confirms a save and renders errors and the not-published state", () => {
    expect(render(detail, { ok: true, valueIds: [LIVE_VALUE_ID, RELEASED_VALUE_ID] })).toContain("2 value rows saved");
    expect(render(detail, { ok: false, error: "This company is not published." })).toContain("This company is not published.");
    expect(renderToStaticMarkup(<SeCompanyInfoNotPublished companyId="5565200028" />)).toContain("not published");
  });
});

describe("admin-se-company-info loader", () => {
  beforeEach(() => {
    server.loadSeCompanyInfoDetail.mockReset().mockResolvedValue(detail);
    fieldsModule.loadCompanyFields.mockReset().mockResolvedValue(fields);
  });

  it("loads the detail and the registry fields for the company", async () => {
    const response = await loader({ params: { companyId: COMPANY_ID } } as unknown as Parameters<typeof loader>[0]);
    expect(fieldsModule.loadCompanyFields).toHaveBeenCalledWith(COMPANY_ID);
    expect(response.data).toEqual({ detail, fields });
    expect(response.init).toBeUndefined();
  });

  it("answers 404 with the fields when the company is not published", async () => {
    server.loadSeCompanyInfoDetail.mockResolvedValue(null);
    const response = await loader({ params: { companyId: COMPANY_ID } } as unknown as Parameters<typeof loader>[0]);
    expect(response.data.detail).toBeNull();
    expect(response.init).toEqual({ status: 404 });
  });
});

describe("admin-se-company-info action (field-value intents, mocked server module)", () => {
  beforeEach(() => {
    server.loadSeCompanyInfoDetail.mockReset().mockResolvedValue(detail);
    server.appendSeCompanyInfoFieldValues.mockReset().mockResolvedValue({ valueIds: [] });
    registryModule.loadFieldRegistry.mockReset().mockResolvedValue(REGISTRY);
    resolveModule.resolveCompanyFields.mockReset().mockResolvedValue(undefined);
  });

  function postAction(entries: Record<string, string>) { /* :783-794 verbatim */ }

  /* :796-933 verbatim: writes one artifact's text; builds both languages; refuses an
     unchanged edit; sends an emptied textarea; hands the store's refusal back; rethrows */

  it("writes an edit-field value and resolves that field synchronously", async () => {
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({ valueIds: ["66666666-6666-4666-8666-666666666666"] });
    const result = await postAction({ intent: "edit-field", field: "legal_name", value: "Alpha Aktiebolag", original_value: "Alpha AB", note: "" });
    expect(result).toEqual({ ok: true, valueIds: ["66666666-6666-4666-8666-666666666666"] });
    expect(server.appendSeCompanyInfoFieldValues).toHaveBeenCalledWith([
      { companyId: COMPANY_ID, field: "legal_name", value: "Alpha Aktiebolag", source: "reviewer", note: "" },
    ]);
    expect(resolveModule.resolveCompanyFields).toHaveBeenCalledWith(COMPANY_ID, ["legal_name"]);
  });
});
```

If plan 4's `use-source` still refuses `llm` / `bolagsverket`, also replace the two use-source refusal cases in `tests/se-info-field-value-form.test.ts` (:87-114) with:

```ts
  it("refuses a field the registry does not know, and a source the registry does not list for the field", () => {
    expect(build({ intent: "use-source", field: "hearsay", value: "x", source: "scb", source_ref: "scb:1" })).toEqual({ ok: false, error: "Unknown field." });
    // reviewer is never a candidate source; ratsit does not offer legal_name; esef does offer description.
    expect(build({ intent: "use-source", field: "legal_name", value: "x", source: "reviewer", source_ref: "r" })).toEqual({ ok: false, error: "Unknown source." });
    expect(build({ intent: "use-source", field: "legal_name", value: "x", source: "ratsit", source_ref: "rt:1" })).toEqual({ ok: false, error: "Unknown source." });
    expect(build({ intent: "use-source", field: "legal_name", value: "Alpha AB", source: "bolagsverket", source_ref: "bv:1" })).toMatchObject({ ok: true });
    expect(build({ intent: "use-source", field: "description", value: "x", source: "llm", source_ref: SUGGESTION_ID })).toMatchObject({ ok: true });
  });
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/admin-se-company-info.test.tsx`
Expected: FAIL -- `fields` is not a prop of `SeCompanyInfoReviewWorkspace`, `loader` returns no `fields`, headings missing.

- [ ] **Step 3: The route module**

```tsx
// app/routes/admin-se-company-info.tsx
import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import {
  appendSeCompanyInfoFieldValues,
  loadSeCompanyInfoDetail,
} from "~/lib/se-company-info.server";
import { loadCompanyFields } from "~/lib/se-company-fields.server";
import { loadFieldRegistry } from "~/lib/se-company-field-registry.server";
import { resolveCompanyFields } from "~/lib/se-company-field-resolve.server";
import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";
import { buildFieldValueInputs } from "~/lib/se-info-field-value-form";

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ params }: Route.LoaderArgs) {
  // Two independent reads: the wide row + artifacts + suggestions + history,
  // and the registry-driven long tables. `fields` carries Maps; React Router's
  // single-fetch transport (turbo-stream v2) serialises them as Maps.
  const [detail, fields] = await Promise.all([
    loadSeCompanyInfoDetail(params.companyId),
    loadCompanyFields(params.companyId),
  ]);
  return data({ detail, fields }, detail ? undefined : { status: 404 });
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const [detail, registry] = await Promise.all([
    loadSeCompanyInfoDetail(params.companyId),
    loadFieldRegistry(),
  ]);
  if (!detail) {
    throw data({ detail: null }, { status: 404 });
  }
  const built = buildFieldValueInputs(form, {
    companyId: params.companyId,
    suggestions: detail.suggestions,
    registry,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  try {
    const { valueIds } = await appendSeCompanyInfoFieldValues(built.inputs);
    // Spec section 9: resolve the decided fields for this company right away so
    // the loader shows the outcome; python_only fields are skipped inside.
    await resolveCompanyFields(
      params.companyId,
      [...new Set(built.inputs.map((input) => input.field))],
    );
    return { ok: true as const, valueIds };
  } catch (error) {
    if (error instanceof SeInfoFieldValueValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.detail?.info.legal_name ?? "Company"} info review | CompanyCollect` }];
}

export default function AdminSwedenCompanyInfo({ loaderData, actionData, params }: Route.ComponentProps) {
  if (!loaderData.detail) {
    return <SeCompanyInfoNotPublished companyId={params.companyId} />;
  }
  return (
    <SeCompanyInfoReviewWorkspace
      detail={loaderData.detail}
      fields={loaderData.fields}
      result={actionData ?? null}
    />
  );
}
```

If plan 4's action calls `resolveCompanyFields` with an options argument (e.g. `{ registry }`), keep that call exactly and adjust the new action test's `toHaveBeenCalledWith` to it.

- [ ] **Step 4: The workspace**

Edit `se-company-info-review-workspace.tsx`:

1. Imports (:1-65): drop `DefinitionList`, `text`, `LegalForm`, `Accordion*`, `Separator`, `SeCompanyInfoRow`, `SE_INFO_FIELDS`, `descriptionProposals`, `DescriptionProposal` (keep the type if `UseSourceForm` still uses it -- it does), `CompanySourceStrip` stays, `companySourceLabels` goes. Add:

```tsx
import { FieldGroupCard, sourceChip } from "~/components/admin/field-group-card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "~/components/ui/collapsible";
import {
  candidateDescriptionProposals,
  DESCRIPTION_FIELDS,
  fieldLabel,
  groupFields,
  sourceLabel,
} from "~/lib/se-company-field-groups";
import type { CompanyFields, FieldCandidate } from "~/lib/se-company-fields.server";
```

2. Delete `PublishedCard` (:237-391), `Fact` (:408-424), `CompanyFactsCard` (:426-490).

3. `FinalDescriptionEditor` (:590-642): props become `{ description, descriptionSv, busy }: { description: string; descriptionSv: string; busy: boolean }`; the two `EditField`s take `value={description}` / `value={descriptionSv}`.

4. `ValueHistoryCard` (:654-734): delete the release row (:669-692, the `<div className="flex flex-wrap ...">` with the `SE_INFO_FIELDS.map` forms, and the `<Separator />` after it) and the `busy` prop; the description line becomes `"Every value decided for this company, newest first. The live row per field is what the resolve step applies."`.

5. Add these components above `SeCompanyInfoReviewWorkspace`:

```tsx
/** "Use this" beside one candidate: its value written as this company's value
 * for the field, with the candidate's record and moment as provenance. */
function UseCandidateForm({ field, candidate, busy }: { field: string; candidate: FieldCandidate; busy: boolean }) {
  return (
    <Form method="post" className="flex items-center gap-2">
      <input type="hidden" name="intent" value="use-source" />
      <input type="hidden" name="field" value={field} />
      <input type="hidden" name="value" value={candidate.value} />
      <input type="hidden" name="source" value={candidate.source} />
      <input type="hidden" name="source_ref" value={candidate.sourceRecordUid} />
      <input type="hidden" name="source_at" value={candidate.observedAt} />
      <Button size="sm" type="submit" disabled={busy} aria-busy={busy}
        aria-label={`Use the ${sourceLabel(candidate.source)} value for ${fieldLabel(field)}`}>
        Use this
      </Button>
    </Form>
  );
}

/** Inline single-value editor for a non-description field (intent edit-field).
 * Hidden rather than unmounted while closed, so a half-typed value survives. */
function FieldEditor({ field, value, busy }: { field: string; value: string; busy: boolean }) {
  const [editing, setEditing] = useState(false);
  return (
    <div className="flex flex-col gap-2">
      <div>
        <Button type="button" size="sm" variant="outline" aria-expanded={editing} onClick={() => setEditing((open) => !open)}>
          Edit
        </Button>
      </div>
      <div hidden={!editing}>
        <Form method="post" className="flex max-w-xl flex-wrap items-center gap-2">
          <input type="hidden" name="intent" value="edit-field" />
          <input type="hidden" name="field" value={field} />
          {/* ALWAYS posted, even empty: the builder refuses a post with no original. */}
          <input type="hidden" name="original_value" value={value} />
          <Input name="value" defaultValue={value} aria-label={`${fieldLabel(field)} value`} />
          <label className="flex items-center gap-2 text-sm">
            <Checkbox name="clear" value="yes" />
            <span>Clear</span>
          </label>
          <Input name="note" placeholder="Note (optional)" aria-label={`${fieldLabel(field)} note`} />
          <Button type="submit" size="sm" disabled={busy} aria-busy={busy}>Save</Button>
          <Button type="button" size="sm" variant="ghost" onClick={() => setEditing(false)}>Cancel</Button>
        </Form>
      </div>
    </div>
  );
}

function ReleaseForm({ field, busy }: { field: string; busy: boolean }) {
  return (
    <Form method="post" className="flex items-center gap-2">
      <input type="hidden" name="intent" value="release" />
      <input type="hidden" name="field" value={field} />
      <Button size="sm" variant="outline" type="submit" disabled={busy} aria-busy={busy}
        aria-label={`Release ${field} to the pipeline`}>
        Release to pipeline
      </Button>
    </Form>
  );
}

/** The artifact cards, collapsed: the raw material is one click away rather
 * than the bulk of the page. `contributing` = the record uids the description
 * candidates were extracted from, which is what the old wide-row list held. */
function SourcesDrawer({ artifacts, contributing }: { artifacts: SeCompanyInfoArtifactRow[]; contributing: Set<string> }) {
  const groups = groupArtifactsBySource(artifacts);
  return (
    <Collapsible>
      <CollapsibleTrigger className={buttonVariants({ variant: "outline", size: "sm" })}>
        Sources · {artifacts.length === 1 ? "1 artifact row" : `${artifacts.length} artifact rows`}
      </CollapsibleTrigger>
      <CollapsibleContent hiddenUntilFound>
        <section className="mt-4 flex flex-col gap-4">
          <SectionHeading title="Sources" description="Every artifact row connected to this company, in full." />
          {groups.length === 0 ? <p className="text-sm text-muted-foreground">No source artifacts.</p> : null}
          {groups.map((group) => (
            <div key={group.source} className="flex flex-col gap-3">
              <h3 className="flex items-center gap-2 text-sm font-semibold">
                {artifactSourceLabel(group.source)}
                <Badge variant="outline">{group.rows.length}</Badge>
              </h3>
              {group.rows.map((artifact) => (
                <ArtifactCard key={`${artifact.source}:${artifact.source_record_uid}`} artifact={artifact}
                  contributes={contributing.has(artifact.source_record_uid)} />
              ))}
            </div>
          ))}
        </section>
      </CollapsibleContent>
    </Collapsible>
  );
}
```

In `ArtifactCard` (:185-187) the badge text becomes `description candidate` (it was `contributes to description`).

6. Replace `SeCompanyInfoReviewWorkspace` (:736-966) with:

```tsx
export function SeCompanyInfoReviewWorkspace({
  detail,
  fields,
  result,
}: {
  detail: SeCompanyInfoDetail;
  fields: CompanyFields;
  result: SeCompanyInfoReviewResult;
}) {
  const { artifacts, suggestions } = detail;
  const busy = useNavigation().state !== "idle";
  const groups = groupFields(fields.registry);
  const descriptionCandidates = fields.candidates.get("description") ?? [];
  const descriptionSvCandidates = fields.candidates.get("description_sv") ?? [];
  const contributing = new Set(
    [...descriptionCandidates, ...descriptionSvCandidates].map((c) => c.sourceRecordUid),
  );
  const resolvedDescription = fields.resolved.get("description");
  const resolvedDescriptionSv = fields.resolved.get("description_sv");
  const proposals: DescriptionProposal[] = [
    // The resolved pair leads the menu: it is what surfaces serve, and it hosts
    // the editor. Offered even when nothing is resolved -- that company is the
    // one a reviewer opens this page to fix.
    {
      key: "resolved",
      source: "resolved",
      sourceLabel: "Resolved",
      meta: resolvedDescription ? sourceChip(resolvedDescription, fields.decisions) : "nothing resolved",
      english: resolvedDescription?.value ?? "",
      original: resolvedDescriptionSv?.value ?? "",
      originalLanguage: resolvedDescriptionSv ? "sv" : "",
      sourceRecordUid: "",
      observedAt: "",
    },
    ...candidateDescriptionProposals(descriptionCandidates, descriptionSvCandidates),
  ];
  const sourceCounts = new Map<string, number>();
  for (const proposal of proposals) {
    sourceCounts.set(proposal.source, (sourceCounts.get(proposal.source) ?? 0) + 1);
  }
  const descriptionCard = (
    <CompanyDescriptionCard
      proposals={proposals}
      renderAction={(proposal, shown) =>
        proposal.source === "resolved" ? (
          <div className="flex flex-col gap-3">
            <FinalDescriptionEditor
              description={resolvedDescription?.value ?? ""}
              descriptionSv={resolvedDescriptionSv?.value ?? ""}
              busy={busy}
            />
            <div className="flex flex-wrap gap-2">
              {DESCRIPTION_FIELDS.map((field) => (
                <ReleaseForm key={field} field={field} busy={busy} />
              ))}
            </div>
          </div>
        ) : (
          <UseSourceForm proposal={proposal} shown={shown} busy={busy}
            repeats={(sourceCounts.get(proposal.source) ?? 0) > 1} />
        )
      }
    />
  );
  const slots = {
    renderUseThis: (field: string, candidate: FieldCandidate) => (
      <UseCandidateForm field={field} candidate={candidate} busy={busy} />
    ),
    renderEdit: (field: string) => (
      <FieldEditor field={field} value={fields.resolved.get(field)?.value ?? ""} busy={busy} />
    ),
    renderRelease: (field: string) => <ReleaseForm field={field} busy={busy} />,
  };

  return (
    <div className="flex flex-col gap-6">
      <CompanySourceStrip sources={artifacts.map((artifact) => artifact.source)} />
      {result?.ok ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Saved</AlertTitle>
          <AlertDescription>
            {result.valueIds.length === 1 ? "1 value row saved" : `${result.valueIds.length} value rows saved`} — resolved for this company; the bulk run repeats it.
          </AlertDescription>
        </Alert>
      ) : null}
      {result && !result.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Not saved</AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}

      {groups.map((group) => (
        <FieldGroupCard
          key={group.group}
          group={group}
          resolved={fields.resolved}
          candidates={fields.candidates}
          decisions={fields.decisions}
          descriptionCard={group.group === "activity" ? descriptionCard : undefined}
          {...slots}
        />
      ))}

      <SuggestionsCard suggestions={suggestions} busy={busy} />

      <ValueHistoryCard rows={detail.fieldValues} />

      <SourcesDrawer artifacts={artifacts} contributing={contributing} />
    </div>
  );
}
```

where `SuggestionsCard({ suggestions, busy })` is the existing "Model suggestions" `<Card>` (:862-961) lifted verbatim into a function whose props are `{ suggestions: SeCompanyInfoSuggestionRow[]; busy: boolean }` (import the row type from `~/lib/se-company-info.server`).

7. If `rg -n "ARTIFACT_SOURCES" app/lib/se-info-field-value-form.ts` still hits (plan 4 left the artifact-only guard), replace `useSource`'s field/source checks (:89-97) with the registry-driven ones and drop the `ARTIFACT_SOURCES` import:

```ts
function useSource(form: FormData, context: SeInfoFieldValueContext): SeInfoFieldValueRequest {
  const field = text(form, "field");
  const entry = context.registry.fields.find((candidate) => candidate.field === field && candidate.field !== "*");
  if (!entry) return refuse("Unknown field.");
  const source = text(form, "source");
  // Only a source the registry lists for this field: `reviewer` is never a
  // candidate source, and a source that never offers the field cannot be copied.
  if (!entry.sources.includes(source)) return refuse("Unknown source.");
  const value = text(form, "value").trim();
  if (value === "") return refuse("Value cannot be empty.");
  const sourceRef = text(form, "source_ref").trim();
  if (sourceRef === "") return refuse("source_ref is required.");
  const sourceAt = text(form, "source_at").trim();
  return { ok: true, inputs: [{ companyId: context.companyId, field, value, source, sourceRef, sourceAt: sourceAt === "" ? null : sourceAt }] };
}
```

and change its call in the `switch` to `useSource(form, context)`.

- [ ] **Step 5: Run everything and typecheck**

Run: `npx vitest run tests/admin-se-company-info.test.tsx tests/se-info-field-value-form.test.ts tests/company-description-card.test.tsx tests/field-group-card.test.tsx && npm run typecheck`
Expected: PASS; typecheck clean. `tests/company-description-card.test.tsx` is untouched: the card's contract did not change.

- [ ] **Step 6: Look at it**

Run: `npm run dev` from `corpscout/services/backoffice`, open `http://localhost:5173/admin/se/company/5565200028/info` (any published company id). Check: three group cards, the description card first inside Activity with its en/original toggle, `N candidates` opens a list with Use this per row, Edit opens the inline input, Sources at the bottom collapsed, and a Use-this click lands as a chip change on reload (the synchronous resolve).

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/backoffice/app/routes/admin-se-company-info.tsx corpscout/services/backoffice/app/components/admin/se-company-info-review-workspace.tsx corpscout/services/backoffice/app/lib/se-info-field-value-form.ts corpscout/services/backoffice/tests/admin-se-company-info.test.tsx corpscout/services/backoffice/tests/se-info-field-value-form.test.ts
git commit -m "feat(backoffice): registry-driven SE company info page" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---
### Task 7: `se_companies_serving` reads description candidates, not `description_sources`

The serving MV is the one reader of a legacy column outside the page: `build_se_companies_serving_sql()` computes `desc_esef` / `desc_wikidata` from `has(i.description_sources, ...)`. Spec 8.3 defines `description_sources` after the cutover as "candidates present for `description`", so the exact equivalent is a membership test on `se_company_field_candidate`.

This task starts from the POST-phase-A builder: plan 3 already shipped a staged-swap migration (planned as 000377) that extended `build_se_companies_serving_sql()` with the new wide columns (`industry_label_en`, `website`, `employee_count`, `employee_count_as_of`, `latest_revenue_amount`, `latest_revenue_currency`, `latest_revenue_amount_usd`, `latest_revenue_fiscal_year`) and retargeted the drift pin to itself. The only builder change here is the two `description_sources` reads; everything else in the render is inherited. Same staged swap as 000347 / plan 3's migration (build under `_next`, `SYSTEM WAIT VIEW`, one `RENAME`).

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py` (constants after `JOB_ADS_SET`; the two `has(i.description_sources, ...)` lines in `build_se_companies_serving_sql` -- find them with `rg -n "description_sources" src/dagster_v3/defs/sweden_company/companies_current.py`, they were :338 and :341 before phase A)
- Create: `corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_companies_serving_description_candidates.up.sql` and `.down.sql`, where `<NNNNNN>` = the next free number at execution time: `ls corpscout/clickhouse/migrations | tail` and add one to the highest
- Test: `corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py` (`MIGRATION` constant, vacuity pins, discard name), `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` + a content test)

**Interfaces:**
- Consumes: plan 3's builder and its migration, `<PHASE_A>` below = the latest `se_companies_serving` migration at execution time: `ls corpscout/clickhouse/migrations | rg "se_companies_serving" | tail -1`.
- Produces: `DESCRIPTION_CANDIDATE_ESEF_SET`, `DESCRIPTION_CANDIDATE_WIKIDATA_SET` module constants; `build_se_companies_serving_sql()` output no longer mentions `description_sources`; `/tmp/serving-pre.sql` (the phase-A render, kept for Task 8's down).

- [ ] **Step 1: Pin the pre-change render before touching anything**

```bash
cd corpscout/services/dagster_v3
git status --short src/dagster_v3/defs/sweden_company/companies_current.py   # must be clean
uv run python -c "from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql; print(build_se_companies_serving_sql())" > /tmp/serving-pre.sql
PHASE_A=$(ls ../../clickhouse/migrations | rg "se_companies_serving" | rg "\.up\.sql$" | tail -1)
echo "$PHASE_A"
uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py -q -p no:warnings   # green: the pin points at $PHASE_A and matches /tmp/serving-pre.sql
```

`/tmp/serving-pre.sql` is, by the drift pin, the embedded SELECT of `$PHASE_A`; Task 8's down recreates the parked view from it.

- [ ] **Step 2: Retarget the drift pin and add the failing assertions**

In `tests/test_se_companies_serving_mv.py` set `MIGRATION = "<NNNNNN>_corpscout_se_companies_serving_description_candidates"` and update the docstring's first line to name it. Keep every assertion plan 3 added for the new wide columns. In `test_the_pin_is_not_vacuous` append:

```python
    # Plan 6 Task 7: which sources offered a description is the candidate table's
    # business now; the wide row's description_sources is dropped in Task 8.
    assert "se_company_field_candidate" in embedded
    assert "field = 'description' AND source = 'esef'" in embedded
    assert "field = 'description' AND source = 'wikidata'" in embedded
    assert "description_sources" not in embedded
```

In the down test replace the discard name with `corpscout.se_companies_serving_description_candidates_discard`. The REFRESH pin stays whatever plan 3 set it to (`REFRESH EVERY 1 HOUR OFFSET 45 MINUTE`, 000366's cadence, restated by every fresh CREATE).

In `tests/test_clickhouse_migrations.py` append the name to `EXPECTED_MIGRATIONS` and add:

```python
def test_se_companies_serving_reads_description_candidates_not_the_wide_row() -> None:
    """Plan 6 Task 7: desc_esef / desc_wikidata come from se_company_field_candidate so
    the wide row's description_sources can be dropped (Task 8). Staged swap as 000347 and
    plan 3's migration; nothing dropped here -- the parked _retired view goes with the
    column drop."""
    up = _migration_sql("<NNNNNN>_corpscout_se_companies_serving_description_candidates.up.sql")
    down = _migration_sql("<NNNNNN>_corpscout_se_companies_serving_description_candidates.down.sql")

    assert "SYSTEM STOP VIEW corpscout.se_companies_serving;" in up
    assert "CREATE MATERIALIZED VIEW corpscout.se_companies_serving_next" in up
    assert "REFRESH EVERY 1 HOUR OFFSET 45 MINUTE" in up
    assert "se_company_field_candidate" in up
    assert "description_sources" not in up
    # Plan 3's columns survive the re-render untouched.
    for column in ("industry_label_en", "website", "employee_count_as_of", "latest_revenue_fiscal_year"):
        assert column in up
    assert "SYSTEM WAIT VIEW corpscout.se_companies_serving_next;" in up
    assert "corpscout.se_companies_serving TO corpscout.se_companies_serving_retired" in up
    assert "corpscout.se_companies_serving_next TO corpscout.se_companies_serving" in up
    assert "DROP VIEW" not in up and "DROP TABLE" not in up

    assert "corpscout.se_companies_serving_retired TO corpscout.se_companies_serving" in down
    assert "SYSTEM START VIEW corpscout.se_companies_serving;" in down
    assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_description_candidates_discard;" in down
```

- [ ] **Step 3: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: FAIL -- the migration files do not exist; the vacuity pin fails on `description_sources`.

- [ ] **Step 4: The builder**

After `JOB_ADS_SET` in `companies_current.py`:

```python
# Which sources offered a description: the long-form successor of the wide row's
# description_sources column (field-registry spec 2026-09-02, section 8.3 -- "candidates
# present for description"). Read from the candidate table so that column can be dropped.
DESCRIPTION_CANDIDATE_ESEF_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_company_field_candidate "
    "WHERE field = 'description' AND source = 'esef'"
)
DESCRIPTION_CANDIDATE_WIKIDATA_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_company_field_candidate "
    "WHERE field = 'description' AND source = 'wikidata'"
)
```

and in `build_se_companies_serving_sql` replace ONLY the two `has(i.description_sources, ...)` lines with:

```python
    toUInt8(i.company_id IN ({DESCRIPTION_CANDIDATE_ESEF_SET})) AS desc_esef,
```
```python
    toUInt8(i.company_id IN ({DESCRIPTION_CANDIDATE_WIKIDATA_SET})) AS desc_wikidata,
```

Confirm the change is those two lines and nothing else: `git diff src/dagster_v3/defs/sweden_company/companies_current.py | rg "^[-+] " | rg -v "^[-+]\s*#"` shows two `-` and two `+` lines inside the builder plus the new constants.

- [ ] **Step 5: The migration**

Render the builder once and paste it -- never hand-edit the SELECT:

```bash
cd corpscout/services/dagster_v3 && uv run python -c "from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql; print(build_se_companies_serving_sql())" > /tmp/serving.sql
diff /tmp/serving-pre.sql /tmp/serving.sql   # exactly the two desc_* lines differ
```

`<NNNNNN>_corpscout_se_companies_serving_description_candidates.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Plan 6 Task 7 (field-registry spec 2026-09-02, section 8.3): desc_esef and desc_wikidata
-- come from corpscout.se_company_field_candidate ("candidates present for description")
-- instead of the wide row's description_sources, which is dropped once nothing reads it.
-- Same staged swap as 000347 and the phase-A serving migration (SYSTEM STOP VIEW guard,
-- build under _next, wait, one atomic RENAME); the REFRESH clause restates 000366's hourly
-- cadence because a fresh CREATE would otherwise fall back to the 15-minute default.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

CREATE MATERIALIZED VIEW corpscout.se_companies_serving_next
REFRESH EVERY 1 HOUR OFFSET 45 MINUTE
ENGINE = MergeTree
ORDER BY company_id
AS <paste /tmp/serving.sql here, ending with the SETTINGS clause>;

SYSTEM WAIT VIEW corpscout.se_companies_serving_next;

RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_retired,
    corpscout.se_companies_serving_next TO corpscout.se_companies_serving;
```

`.down.sql` -- restores the phase-A render under the serving name: the view parked as `_retired` IS `<PHASE_A>`'s SELECT (equal to `/tmp/serving-pre.sql`), so the down swaps it back rather than re-typing it, exactly as 000347's down does:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Swaps the parked phase-A render (the latest se_companies_serving migration before this
-- one) back under the serving name, restarts its refresh loop (the up-file stopped it), and
-- discards the candidate-set render. Only meaningful while _retired still exists -- after
-- plan 6 Task 8's drop, roll forward instead (its down recreates _retired first).
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_description_candidates_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_description_candidates_discard;
```

- [ ] **Step 6: Run the tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q -p no:warnings && uv run dg check defs`
Expected: PASS (the drift pin compares the pasted SELECT to a fresh render).

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_companies_serving_description_candidates.up.sql corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_companies_serving_description_candidates.down.sql
git commit -m "feat(clickhouse): se_companies_serving reads description candidates instead of description_sources" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 8: projection stops writing the legacy columns; deploy, verify, then the gated drop (owner-gated)

Destructive at the end; the migration is written at the apply step, exactly like 000372, and nothing in Steps 5-8 is committed before the gate holds. The `_retired` view parked by Task 7 goes in the same migration (000348's precedent), BEFORE the column drop: its stored SELECT still names `description_sources`.

**The drop list is eight columns:** `llm_enhanced`, `description_sources`, `description_source_record_uids`, `description_source_count`, `suggestion_id`, `model_provider`, `model_name`, `prompt_version`. **`correction_ids` stays.** Spec 8.3 gives it a live meaning under the new model ("decision ids applied across all fields" -- the projection writes it from decisions, not from the retired publisher), and on 2026-09-02 it still has a backoffice reader on `se_company_info` (`app/lib/se-company-info-pipeline.server.ts:131,183`, pinned by `app/lib/se-company-info-pipeline.server.test.ts:71`); the grep in Step 1 re-checks this at execution and its result is recorded in the migration header.

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/sql.py` (`render_projection_sql(registry)` -- plan 1), `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/registry.py` (`INFO_REGISTRY.version`), `corpscout/services/dagster_v3/tests/test_se_company_field_sql.py` (projection pins), the registry test that pins the version literal
- Create (at apply time): `corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_company_info_drop_description_provenance.up.sql` and `.down.sql` (`<NNNNNN>` = next free number at execution: `ls corpscout/clickhouse/migrations | tail`, plus one)
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` + content test)

**Interfaces:**
- Consumes: `render_projection_sql(registry) -> str` (plan 1, `fields/sql.py`); `registry_rows(registry, *, rendered_at)` (plan 1, `fields/export.py`) which stores that render as the `field = '*'` row of `corpscout.se_company_field_registry`; the asset `se_company_field_registry_clickhouse`; both executors (plan 3's resolve asset, plan 4's `resolveCompanyFields`) read the statement from the registry table and never re-render. Every earlier task deployed; phase A's cutover complete (spec 12 step 5: `info.py` and `se_company_info_clickhouse` deleted).
- Produces: `INFO_REGISTRY.version == "se-info-v2"`; a projection statement that writes none of the eight columns; `corpscout.se_company_info` without them; no `se_companies_serving_retired`.

- [ ] **Step 1: Record the `correction_ids` readers** (informational; decides nothing unless the spec changes)

```bash
rg -n "correction_ids" corpscout/services/backoffice/app corpscout/services/backoffice/tests
```

Hits in `se-company-address*`, `se-company-person*`, `se-company-people*`, `se-people-simple-sync*` are those modules' OWN `correction_ids` columns (address / person ledgers) and do not count. Hits naming `published.correction_ids` / `final.correction_ids` in `se-company-info-pipeline.server.ts` (+ its test) and any `correction_ids` in `tests/*.live.test.ts` are readers of `se_company_info.correction_ids`. List what you found in the migration header (Step 6). `correction_ids` is not in the drop list regardless (spec 8.3).

- [ ] **Step 2: Write the failing projection pins** (RED)

In `tests/test_se_company_field_sql.py` add, next to plan 1's projection pin:

```python
# Plan 6 Task 8: written by the retired publisher's provenance model, replaced by the long
# tables. The projection must stop naming them before the columns are dropped.
DROPPED_PROVENANCE_COLUMNS = (
    "llm_enhanced",
    "description_sources",
    "description_source_record_uids",
    "description_source_count",
    "suggestion_id",
    "model_provider",
    "model_name",
    "prompt_version",
)


def test_projection_no_longer_writes_the_dropped_provenance_columns() -> None:
    sql = render_projection_sql(INFO_REGISTRY)
    for column in DROPPED_PROVENANCE_COLUMNS:
        assert column not in sql, column
    # Still projected: the applied decision ids across all fields (spec 8.3).
    assert "correction_ids" in sql
    assert "resolved_at" in sql and "source_run_id" in sql


def test_info_registry_version_bumped_for_the_projection_change() -> None:
    assert INFO_REGISTRY.version == "se-info-v2"
```

Update plan 1's pinned projection text in the same file (the literal that `render_projection_sql(INFO_REGISTRY)` is compared against) by deleting the eight columns from its INSERT column list and the eight matching SELECT expressions -- the spec 8.3 expressions being removed are: `llm_enhanced` (`source = 'llm'` on the description row), `description_sources` / `description_source_record_uids` / `description_source_count` (the description candidates' sources, uids and count), `suggestion_id` / `model_provider` / `model_name` / `prompt_version` (the LLM candidate's observation, `deterministic` otherwise). Wherever the registry test pins the version literal (`"se-info-v1"`), change it to `"se-info-v2"`.

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sql.py tests/test_se_company_field_registry.py -q -p no:warnings` -> FAIL on the eight names and the version.

- [ ] **Step 3: Edit the projection and bump the version** (GREEN)

In `fields/sql.py`, `render_projection_sql`: remove the eight columns from the `INSERT INTO corpscout.se_company_info (...)` column list and their SELECT expressions. After the edit the column list reads (spec 8.3 minus the eight, plus plan 3's new columns):

```python
PROJECTION_COLUMNS = (
    "company_id",
    "legal_name", "legal_form_code", "legal_form_label_en", "legal_form_label_sv",
    "status", "incorporation_date",
    "description", "description_sv", "description_language",
    "primary_nace_code", "primary_sni_code",
    "wikidata_id", "lei",
    "source_record_uids", "evidence_hashes",
    "correction_ids",
    "industry_label_en", "website",
    "employee_count", "employee_count_as_of",
    "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd", "latest_revenue_fiscal_year",
    "resolved_at", "source_run_id",
)
```

(if plan 1 keeps the list inline rather than as a tuple, apply the same deletion inline; the SELECT list must match positionally, so delete the eight expressions at the same positions). The `llm` CTE that only served `suggestion_id` / `model_*` / `prompt_version` goes with them if nothing else reads it.

In `fields/registry.py` set `INFO_REGISTRY`'s `version="se-info-v2"` -- the export row changes, and the resolver stamps `registry_version` on every resolved row, so the next resolve re-resolves every company (spec 8.4, third bullet). That is intended: it rewrites every wide row through the new projection.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_sql.py tests/test_se_company_field_registry.py tests/test_clickhouse_migrations.py -q -p no:warnings && uv run dg check defs` -> PASS. Commit:

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/sql.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/registry.py corpscout/services/dagster_v3/tests/test_se_company_field_sql.py corpscout/services/dagster_v3/tests/test_se_company_field_registry.py
git commit -m "refactor(dagster): se_company_info projection stops writing description provenance" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

- [ ] **Step 4: Deploy Tasks 1-7 and this projection change**

1. Merge. Apply Task 7's migration (`migrate up`) -- the swap takes about one full refresh (~150 s under the SETTINGS cap).
2. Deploy dagster_v3 (pristine-worktree recipe incl. dbt-state refresh); `uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py -q` green on the deployed checkout.
3. Materialize `se_company_field_registry_clickhouse` on prod (UI or `dg launch --assets se_company_field_registry_clickhouse`). Verify the stored projection:
   ```sql
   SELECT registry_version,
          multiSearchAny(resolve_sql, ['llm_enhanced', 'description_sources', 'description_source_record_uids', 'description_source_count', 'suggestion_id', 'model_provider', 'model_name', 'prompt_version']) AS names_a_dropped_column,
          position(resolve_sql, 'correction_ids') > 0 AS keeps_correction_ids
   FROM corpscout.se_company_field_registry FINAL
   WHERE field = '*';
   -- expected: one row, registry_version = 'se-info-v2', names_a_dropped_column = 0, keeps_correction_ids = 1
   SELECT resolve_sql FROM corpscout.se_company_field_registry FINAL WHERE field = '*' FORMAT TSVRaw;
   -- read it once by eye: the INSERT column list is PROJECTION_COLUMNS above
   ```
4. Let the resolve asset run once (sensor tick, or launch `se_company_field_resolved_clickhouse` with `execute`): with the version bump every company re-resolves, so the wide rows are rewritten by the new projection while the columns still exist (they keep their old values, unread).
5. Restart the backoffice on the merged code; open one company's Info tab: three group cards render, `N candidates` opens, a Use-this click changes the chip on reload (this exercises `resolveCompanyFields` against the new projection statement).

- [ ] **Step 5: Verify the swap** (direct SQL on companycollect ClickHouse)

```sql
SELECT count() AS live FROM corpscout.se_companies_serving;
SELECT count() AS parked FROM corpscout.se_companies_serving_retired;
SELECT countIf(source_esef) AS esef, countIf(source_wikidata) AS wikidata FROM corpscout.se_companies_serving;
SELECT countIf(source_esef) AS esef, countIf(source_wikidata) AS wikidata FROM corpscout.se_companies_serving_retired;
```

Gate: `live` = `parked` (± the companies published between the two refreshes), and the two flag counts agree within the same tolerance. A larger gap means a source's description candidates are missing from `se_company_field_candidate` -- stop, fix the extractor (phase A), do not proceed.

- [ ] **Step 6: Verify no reader is left** (all five must hold)

1. Backoffice page: `rg -n "i\.(llm_enhanced|description_sources|description_source_record_uids|description_source_count|suggestion_id|model_provider|model_name|prompt_version)" corpscout/services/backoffice/app/lib/se-company-info.server.ts` -> no hits (Task 5).
2. Backoffice pipeline sheet: `rg -n "final\.(description_source_count|suggestion_id)|published\.(description_source_count|suggestion_id)" corpscout/services/backoffice/app/lib/se-company-info-pipeline.server.ts` -> no hits. That module is the backoffice port of Dagster's change scan; phase A plan 5 retargets the scan to the candidates CTE and the resolve asset, and the sheet's port moves with it. If this still hits, the phase A port is not deployed -- stop; do not write a divergent scan here.
3. Stored projection (the executors read it from the table, never re-render):
   ```sql
   SELECT count() AS stale_projection_rows
   FROM corpscout.se_company_field_registry FINAL
   WHERE field = '*'
     AND multiSearchAny(resolve_sql, ['llm_enhanced', 'description_sources', 'description_source_record_uids', 'description_source_count', 'suggestion_id', 'model_provider', 'model_name', 'prompt_version']);
   -- expected: 0
   SELECT registry_version, count() FROM corpscout.se_company_field FINAL GROUP BY registry_version;
   -- expected: only 'se-info-v2' (Step 4.4 completed)
   ```
   Also: `test -f corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/info.py && echo STOP` prints nothing (spec 12 step 5 done).
4. Public serving: `rg -n "llm_enhanced|description_sources|description_source_count|suggestion_id|model_provider|prompt_version" corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving corpscout/services/dagster_v3/src/dagster_v3/defs/company_markets corpscout/services/dagster_v3/src/dagster_v3/defs/technology_catalog` -> no hits (verified 2026-09-02; re-run).
5. The serving view itself: `SELECT create_table_query FROM system.tables WHERE database = 'corpscout' AND name = 'se_companies_serving' AND create_table_query LIKE '%description_sources%'` -> 0 rows (Task 7 applied).

- [ ] **Step 7: Write the ledger test first** (RED)

Append the name to `EXPECTED_MIGRATIONS` and add:

```python
def test_description_provenance_columns_are_dropped_reversibly() -> None:
    """Plan 6 Task 8, written at the apply step: eight of the wide row's provenance columns
    (000297 / 000304) go once the backoffice reads se_company_field, the serving view reads
    the candidate sets and the projection (se-info-v2) stops writing them. correction_ids
    stays: spec 8.3 gives it a live meaning (applied decision ids). The swap's parked view
    is dropped FIRST: its stored SELECT still names description_sources."""
    up = _migration_sql("<NNNNNN>_corpscout_se_company_info_drop_description_provenance.up.sql")
    down = _migration_sql("<NNNNNN>_corpscout_se_company_info_drop_description_provenance.down.sql")
    columns = (
        "llm_enhanced", "description_sources", "description_source_record_uids",
        "description_source_count", "suggestion_id", "model_provider", "model_name",
        "prompt_version",
    )

    assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;" in up
    assert up.index("DROP VIEW IF EXISTS") < up.index("ALTER TABLE corpscout.se_company_info")
    for column in columns:
        assert f"DROP COLUMN IF EXISTS {column}" in up
        assert f"ADD COLUMN IF NOT EXISTS {column} " in down
    assert "correction_ids" not in _normalize_sql(up.split("ALTER TABLE")[1])
    assert "DROP TABLE" not in up

    assert "CREATE MATERIALIZED VIEW corpscout.se_companies_serving_retired" in down
    assert "SYSTEM STOP VIEW corpscout.se_companies_serving_retired;" in down
```

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q -p no:warnings` -> FAIL (files missing).

- [ ] **Step 8: Write the migration** (GREEN)

`.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Plan 6 Task 8 (field-registry spec 2026-09-02, section 8.3, last paragraph): the
-- backoffice Info page reads se_company_field / se_company_field_candidate, the serving
-- view reads the candidate sets, and the projection (registry se-info-v2) no longer writes
-- these columns. Written at the apply step, after every reader was deployed (2026-08-25
-- ruling: a DROP that must wait for a deploy never sits in the ledger). Gate re-verified
-- <YYYY-MM-DD> immediately before apply (plan 6 Task 8, Steps 5-6).
-- correction_ids is NOT dropped: spec 8.3 keeps it as the applied decision ids, and on
-- <YYYY-MM-DD> it was still read by <list the Step 1 hits on se_company_info, or "no code">.
--
-- The view parked by the staged swap goes first: its stored SELECT still names
-- description_sources. 000348 precedent -- transitional machinery, zero readers.
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;

-- No MATERIALIZED expression reads any of these (evidence_set_hash covers the artifacts'
-- hashes only, see 000304), so they drop in one statement (000021 precedent).
ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS llm_enhanced,
    DROP COLUMN IF EXISTS description_sources,
    DROP COLUMN IF EXISTS description_source_record_uids,
    DROP COLUMN IF EXISTS description_source_count,
    DROP COLUMN IF EXISTS suggestion_id,
    DROP COLUMN IF EXISTS model_provider,
    DROP COLUMN IF EXISTS model_name,
    DROP COLUMN IF EXISTS prompt_version;
```

`.down.sql` -- the eight columns in their 000297 / 000304 shapes (empty; defaults only), then the parked view recreated from the phase-A render so Task 7's down can still run (000348's down is the precedent):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Schema only: the columns come back empty, in their 000297 / 000304 shapes. Roll forward
-- is the sane path; this exists so the ledger can walk backwards.
ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS llm_enhanced Bool DEFAULT false,
    ADD COLUMN IF NOT EXISTS description_sources Array(String),
    ADD COLUMN IF NOT EXISTS description_source_record_uids Array(String),
    ADD COLUMN IF NOT EXISTS description_source_count UInt8 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS suggestion_id Nullable(UUID),
    ADD COLUMN IF NOT EXISTS model_provider LowCardinality(String),
    ADD COLUMN IF NOT EXISTS model_name String,
    ADD COLUMN IF NOT EXISTS prompt_version String;

-- The phase-A render, parked and stopped as Task 7's swap left it. THE SELECT IS NOT
-- HAND-WRITTEN: it is /tmp/serving-pre.sql from Task 7 Step 1, i.e. the embedded SELECT of
-- the latest se_companies_serving migration before Task 7's (copy it from that file's
-- CREATE MATERIALIZED VIEW ... AS statement). The guard DROP makes the recreate idempotent
-- on a half-applied down.
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;

CREATE MATERIALIZED VIEW corpscout.se_companies_serving_retired
REFRESH EVERY 1 HOUR OFFSET 45 MINUTE
ENGINE = MergeTree
ORDER BY company_id
AS <paste /tmp/serving-pre.sql here, ending with its SETTINGS clause>;

SYSTEM STOP VIEW corpscout.se_companies_serving_retired;
```

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q -p no:warnings` -> PASS (including `test_every_migration_ends_with_a_statement_not_a_comment`).

- [ ] **Step 9: Apply and confirm**

Apply with `migrate up` on companycollect. Then:

```sql
SELECT name FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info'
  AND name IN ('llm_enhanced','description_sources','description_source_record_uids','description_source_count','suggestion_id','model_provider','model_name','prompt_version');
-- expected: 0 rows
SELECT count() FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info' AND name = 'correction_ids';
-- expected: 1
SELECT count() FROM system.tables WHERE database = 'corpscout' AND name = 'se_companies_serving_retired';
-- expected: 0
```

Open one company's Info tab and the companies list once more; make one Use-this decision from the backoffice and confirm the projection write succeeds (no `NO_SUCH_COLUMN_IN_TABLE` in the action's error alert or the ClickHouse log); wait one sensor tick and confirm the bulk resolve run for that company succeeds too.

- [ ] **Step 10: Commit**

```bash
git add corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_company_info_drop_description_provenance.up.sql corpscout/clickhouse/migrations/<NNNNNN>_corpscout_se_company_info_drop_description_provenance.down.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): drop se_company_info description provenance columns" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

## Self-review

**Spec coverage (section 11, 8.3, 12):**
- `FieldGroupCard` per display group: value, source chip with observed date, expandable candidates with Use this, Edit / Release per field -> Tasks 4 + 6. Description card as the first instance with its language toggle -> Task 6 (`descriptionCard` in the Activity group, `CompanyDescriptionCard` unchanged).
- Groups identity / activity / scale, absent fields rendered as absent -> Task 2 (`groupFields`), Task 4 (`Not available`).
- Artifact cards into a Sources drawer; Published version card gone; Value history, pipeline sheet, LLM suggestions kept -> Task 6 (the pipeline sheet lives on the companies list, `se-company-info-table.tsx:363`, untouched).
- Loader reads `se_company_field` / `se_company_field_candidate` instead of the legacy columns -> Tasks 1, 5, 6; the columns dropped by migration -> Tasks 7 (MV reader off `description_sources`), 8 (projection `se-info-v2` stops writing them, then the eight-column drop; `correction_ids` kept per spec 8.3).
- `se-info-field-values.ts` registry-driven -> plan 4; this plan's `edit-field` validates the field against the registry and refuses the description pair -> Task 3.
- Section 12 backoffice tests: single-company resolve under `VITEST_LIVE` is plan 4's; the validator reading the registry is plan 4's. This plan pins the SQL, precedence, grouping, formatting, the card, the page and the intent.
- `python_only` -> "Applies on next run" hint (Task 4); the action's synchronous resolve skips such fields inside `resolveCompanyFields` (plan 4).

**Placeholder scan:** `<NNNNNN>` tokens are migration numbers assigned at execution (`ls corpscout/clickhouse/migrations | tail`, plus one -- Global Constraints); `<PHASE_A>` is the latest `se_companies_serving` migration at execution time, found by the command in Task 7; the `<paste ...>` spots in Tasks 7 and 8 are rendered SQL that must be generated, not typed, and the commands producing each are given; the `<YYYY-MM-DD>` / `<list ...>` fields in Task 8's migration header are filled at the apply step, as 000372's were. Task 6's fixture references the current test file's artifact/suggestion/history rows by line, which exist in the tree.

**Type consistency:** `FieldCandidate.sourceRecordUid` / `observedAt` are the names Task 2's `candidateDescriptionProposals` and Task 6's `UseCandidateForm` read; `sourceChip(resolved, decisions)` is Task 4's export used in Task 6; `SeInfoFieldValueContext.registry` (Task 3) is what Task 6's action and `useSource` read; `CompanyFields.decisions` is the same `SeCompanyInfoFieldValueRow[]` shape as `detail.fieldValues`, loaded twice (one bounded query each) rather than threaded through -- accepted for a self-contained `loadCompanyFields`.
