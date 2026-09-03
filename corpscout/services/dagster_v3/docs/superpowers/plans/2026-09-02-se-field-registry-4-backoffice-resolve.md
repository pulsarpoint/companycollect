# SE Field Registry, part 4: Backoffice Resolve-After-Decision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a reviewer's decision is written, the backoffice resolves that one company synchronously with the registry's exported SQL and re-pivots its wide row, and validates decisions against the registry export instead of hard-coded enums.

**Architecture:** The backoffice reads `corpscout.se_company_field_registry` (one `argMax(..., version)` row per field plus the `field = '*'` projection row) through a version-keyed module cache. `appendSeCompanyInfoFieldValues` validates against that export, inserts the decision rows, then runs each decided field's `resolve_sql` and the projection statement for `[companyId]` through the writer client, so the loader's next read of `se_company_info FINAL` already shows the resolved value. The page itself is not redesigned (phase B); only the result banner changes.

**Tech Stack:** TypeScript (strict) / React Router 8 framework mode / vitest 4 (`npx vitest run <files>` from `corpscout/services/backoffice`; `npm run typecheck` = `react-router typegen && tsc`); `@clickhouse/client` 1.23 (`client.query` for reads with `readonly=2`, `client.command` / `client.insert` on the writer client); ClickHouse 26.5.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-02-se-company-field-registry-design.md` — sections 4.3 (export table, read with `argMax(..., version)`), 6 (decisions), 9 (backoffice resolve after a decision), 11 last bullet (validator reads the registry export), 12 (backoffice tests). This plan is part 4 of phase A; parts 1–3 (migrations, registry export, candidates, resolve asset, projection) are separate plans and are assumed deployed.

## Global Constraints

- All paths below are relative to `corpscout/services/backoffice` unless they start with `corpscout/`. Run tests from that directory: `npx vitest run <file>`; `npm run typecheck` must be clean before every commit.
- TypeScript strict; `~/` import alias; shadcn/base-ui components only (`~/components/ui/*`); loaders read, actions write; a route module exports only `loader`, `action`, `meta` and the component (anything else that touches a `.server` module breaks the client bundle).
- ClickHouse 26.5 named-parameter syntax `{name:Type}` (`{field:String}`, `{company_ids:Array(String)}`, `{source_run_id:String}`, `{resolved_at:DateTime64(3, 'UTC')}`); user values are bound as `query_params`, never interpolated. Inserts of rows use JSONEachRow through the existing `chInsert*` helpers; `INSERT ... SELECT` statements run through the new `chCommand`.
- DateTime64 text format everywhere a timestamp is bound or inserted: `YYYY-MM-DD HH:MM:SS.mmm` in UTC (the driver would render a JS `Date` as epoch seconds, which is NOT this form, so timestamps are formatted as strings before binding).
- The read client sends `readonly=2` and cannot run `INSERT ... SELECT`; every write goes through `getWriteClient()` in `app/lib/clickhouse.server.ts` — the same account as the reads (owner decision 2026-08-23: one credential set), which holds the `corpscout_person_correction_writer` role with INSERT on `se_company_info_field_value`, `se_company_field`, `se_company_field_candidate` and `se_company_info` (granted by parts 1–3).
- Table and column names exactly as the spec: `corpscout.se_company_field_registry` (columns `datatype, country, field, value_type, display_group, structured, python_only, sources, policy_name, policy_version, resolve_sql, registry_version, version`), `corpscout.se_company_field` (spec 8.1), `corpscout.se_company_field_candidate` (spec 5.1), `corpscout.se_company_info_field_value` (unchanged shape).
- **Do not touch** address- or person-ledger code (`se-address-*.ts`, `se-person-*.ts`, `se-company-address*.ts(x)`, their `chInsert*` helpers) beyond adding one exported function to `clickhouse.server.ts`.
- Conventional Commits, staged by explicit path (`git add <paths>`), each commit message ending with these two trailer lines:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```

---

## File structure

| File | Responsibility |
| --- | --- |
| `app/lib/clickhouse.server.ts` (modify) | + `chCommand(sql, params)`: one parameterised statement on the writer client. |
| `app/lib/se-company-field-registry.server.ts` (create) | Read the registry export; module cache keyed by `registry_version`; `FIELD_REGISTRY_SQL` / `FIELD_REGISTRY_VERSION_SQL`. |
| `app/lib/se-company-field-resolve.server.ts` (create) | `resolveCompanyFields`, `formatClickHouseDateTime64`, `SeCompanyFieldResolveError`. |
| `app/lib/se-info-field-values.ts` (modify) | Client-safe validator driven by a `{ fields, sources }` vocabulary; `fieldVocabulary(registry)` derives it. Enums removed. |
| `app/lib/se-info-field-value-form.ts` (modify) | Intents check the posted field against `context.fields`. |
| `app/lib/se-company-info.server.ts` (modify) | `appendSeCompanyInfoFieldValues(inputs, opts)` validates against the registry, inserts, resolves, returns `{ valueIds, resolved, skipped }`. |
| `app/routes/admin-se-company-info.tsx` (modify) | Action loads the registry once, threads it, maps the resolve failure to `{ ok: false, error, valueIds }`. |
| `app/components/admin/se-company-info-review-workspace.tsx` (modify) | Result type + banner copy; local `RELEASABLE_FIELDS` for the phase-A release buttons. |
| `tests/se-field-registry.fixture.ts` (create) | One `FieldRegistry` fixture shared by every test below (not collected: vitest includes `*.test.{ts,tsx}` only). |
| `tests/clickhouse-writer.server.test.ts`, `tests/se-company-field-registry.server.test.ts`, `tests/se-company-field-resolve.server.test.ts`, `tests/se-info-field-values.test.ts`, `tests/se-info-field-value-form.test.ts`, `tests/se-company-info.server.test.ts`, `tests/admin-se-company-info.test.tsx` | Unit tests (mocked ClickHouse). |
| `tests/se-company-field-resolve.live.test.ts` (create), `package.json` | Live test under `VITEST_LIVE=1`. |

---

### Task 1: `chCommand` — a parameterised statement runner on the writer client

**Files:**
- Modify: `app/lib/clickhouse.server.ts:133-144` (add the new export right after `chInsertSeCompanyInfoFieldValues`)
- Test: `tests/clickhouse-writer.server.test.ts`

**Interfaces:**
- Consumes: `getWriteClient(): ClickHouseClient` (module-private, `clickhouse.server.ts:27`), `ClickHouseClient.command({ query, query_params })` from `@clickhouse/client` 1.23.
- Produces: `export async function chCommand(sql: string, params?: Record<string, unknown>): Promise<void>` — Tasks 3 and 6 call it.

- [ ] **Step 1: Write the failing tests**

In `tests/clickhouse-writer.server.test.ts`, extend the hoisted mock and the fake client, then add two assertions. Apply these edits:

1. Replace the hoisted block at the top of the file:

```ts
const clickhouse = vi.hoisted(() => ({
  createClient: vi.fn(),
  insert: vi.fn(),
  command: vi.fn(),
}));
```

2. The write client is a module-level singleton created by whichever test first triggers it, so every fake client must carry `command` too. Replace all occurrences in this file:

```bash
sed -i '' 's/mockReturnValue({ insert: clickhouse.insert })/mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command })/g' tests/clickhouse-writer.server.test.ts
```

3. Add `chCommand` to the import list from `~/lib/clickhouse.server`, and add `clickhouse.command.mockReset();` to the `afterEach`.

4. In the existing first test (`"fails closed without ClickHouse credentials"`), add after the existing `rejects.toThrow(...)` expectation:

```ts
    await expect(chCommand("SELECT 1")).rejects.toThrow(
      "CLICKHOUSE_USER and CLICKHOUSE_PASSWORD",
    );
```

5. Append a new test at the end of the `describe`:

```ts
  // The registry's generated statements are INSERT ... SELECT: no rows to
  // hand to insert(), so they go through command(), on the SAME writer client
  // (the read client sends readonly=2 and would refuse them). Values are
  // bound as named parameters, never interpolated.
  it("runs a parameterised statement through the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({
      insert: clickhouse.insert,
      command: clickhouse.command,
    });
    clickhouse.command.mockResolvedValue({ query_id: "q1" });

    await chCommand(
      "INSERT INTO corpscout.se_company_field SELECT {field:String}",
      { field: "description", company_ids: ["5565200028"] },
    );

    expect(clickhouse.command).toHaveBeenCalledWith({
      query: "INSERT INTO corpscout.se_company_field SELECT {field:String}",
      query_params: { field: "description", company_ids: ["5565200028"] },
    });
  });
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/clickhouse-writer.server.test.ts`
Expected: FAIL — `chCommand` is not exported (`TypeError: chCommand is not a function` / `SyntaxError: does not provide an export named 'chCommand'`).

- [ ] **Step 3: Implement `chCommand`**

In `app/lib/clickhouse.server.ts`, insert after the `chInsertSeCompanyInfoFieldValues` function (line 144, before the `chInsertSeCompanyAddressCorrections` doc comment):

```ts
/**
 * Runs one write-side statement that carries no row data of its own -- the
 * field registry's generated `INSERT INTO ... SELECT` resolve and projection
 * statements -- on the writer client. Values are bound as ClickHouse named
 * parameters (`{company_ids:Array(String)}`), never interpolated. The read
 * client cannot run these: it sends readonly=2. The writer's async_insert
 * settings do not apply to INSERT ... SELECT (ClickHouse only coalesces
 * data-carrying inserts), so the statement has completed when this resolves.
 */
export async function chCommand(
  sql: string,
  params?: Record<string, unknown>,
): Promise<void> {
  await getWriteClient().command({ query: sql, query_params: params });
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/clickhouse-writer.server.test.ts && npm run typecheck`
Expected: PASS, typecheck clean.

- [ ] **Step 5: Commit**

```bash
git add app/lib/clickhouse.server.ts tests/clickhouse-writer.server.test.ts
git commit -m "feat(backoffice): parameterised write-side command runner for ClickHouse

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: Registry reader with a version-keyed cache

**Files:**
- Create: `app/lib/se-company-field-registry.server.ts`
- Create: `tests/se-field-registry.fixture.ts`
- Test: `tests/se-company-field-registry.server.test.ts`

**Interfaces:**
- Consumes: `chQuery<T>(sql, params)` from `~/lib/clickhouse.server` (read client).
- Produces (fixed; parts 5–6 and the phase-B plan depend on these names):
  ```ts
  export interface FieldRegistryEntry { field: string; valueType: string; displayGroup: string; structured: boolean; pythonOnly: boolean; sources: string[]; policyName: string; policyVersion: string; resolveSql: string; registryVersion: string }
  export interface FieldRegistry { version: string; fields: FieldRegistryEntry[]; projectionSql: string }
  export const FIELD_REGISTRY_VERSION_SQL: string
  export const FIELD_REGISTRY_SQL: string
  export async function loadFieldRegistry(): Promise<FieldRegistry>
  export function resetFieldRegistryCache(): void
  ```
  and the test fixture `REGISTRY_FIXTURE: FieldRegistry`, `REGISTRY_VERSION = "se-info-v1"`, `registryEntry(over)` from `tests/se-field-registry.fixture.ts`.

Cache semantics (resolves spec 9 "cached per registry version"): every call runs the cheap version probe; the full row read runs only when the probe's `registry_version` differs from the cached one (not only when "newer" — version strings such as `se-info-v1` have no order). Rows of a field that a later registry version dropped stay in the ReplacingMergeTree forever, so the row read is filtered to the probed version.

- [ ] **Step 1: Write the shared fixture**

Create `tests/se-field-registry.fixture.ts`:

```ts
import type {
  FieldRegistry,
  FieldRegistryEntry,
} from "~/lib/se-company-field-registry.server";

/**
 * A registry export as loadFieldRegistry() returns it, small enough to read
 * in one glance. Field names and source orders follow spec section 4.2;
 * `website` is marked python_only here ONLY so the skip path has a case --
 * the real registry marks nothing python_only today. The resolve SQL is a
 * stand-in that names the four parameters every generated statement binds.
 */
export const REGISTRY_VERSION = "se-info-v1";

export function registryEntry(
  over: Partial<FieldRegistryEntry> &
    Pick<FieldRegistryEntry, "field" | "sources">,
): FieldRegistryEntry {
  return {
    valueType: "text",
    displayGroup: "activity",
    structured: false,
    pythonOnly: false,
    policyName: "source_precedence",
    policyVersion: "source_precedence-v1",
    resolveSql: `INSERT INTO corpscout.se_company_field /* ${over.field} */ SELECT {field:String}, {company_ids:Array(String)}, {source_run_id:String}, {resolved_at:DateTime64(3, 'UTC')}`,
    registryVersion: REGISTRY_VERSION,
    ...over,
  };
}

export const REGISTRY_FIXTURE: FieldRegistry = {
  version: REGISTRY_VERSION,
  fields: [
    registryEntry({
      field: "description",
      sources: ["llm", "esef", "wikidata", "scb"],
    }),
    registryEntry({ field: "description_sv", sources: ["llm", "scb"] }),
    registryEntry({
      field: "legal_name",
      displayGroup: "identity",
      sources: ["bolagsverket", "scb", "wikidata"],
    }),
    registryEntry({
      field: "website",
      displayGroup: "scale",
      valueType: "url",
      sources: ["domains", "wikidata"],
      pythonOnly: true,
    }),
  ],
  projectionSql:
    "INSERT INTO corpscout.se_company_info /* projection */ SELECT {company_ids:Array(String)}",
};
```

- [ ] **Step 2: Write the failing tests**

Create `tests/se-company-field-registry.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  FIELD_REGISTRY_SQL,
  FIELD_REGISTRY_VERSION_SQL,
  loadFieldRegistry,
  resetFieldRegistryCache,
} from "~/lib/se-company-field-registry.server";

const SCOPE = { datatype: "info", country: "SE" };

/** One row as FIELD_REGISTRY_SQL returns it: LowCardinality columns wrapped to
 * plain strings, the two Bools cast to 0/1, sources as a JSON array. */
function row(field: string, over: Record<string, unknown> = {}) {
  return {
    field,
    value_type: "text",
    display_group: "activity",
    structured: 0,
    python_only: 0,
    sources: ["llm", "scb"],
    policy_name: "source_precedence",
    policy_version: "source_precedence-v1",
    resolve_sql: `INSERT INTO corpscout.se_company_field /* ${field} */ SELECT {field:String}`,
    registry_version: "se-info-v1",
    ...over,
  };
}

/** The `field = '*'` row: the wide projection statement (spec 4.3). */
const PROJECTION_ROW = row("*", {
  value_type: "projection",
  display_group: "",
  sources: [],
  policy_name: "",
  policy_version: "",
  resolve_sql:
    "INSERT INTO corpscout.se_company_info SELECT {company_ids:Array(String)}",
});

function stubRegistry(version: string, rows: ReturnType<typeof row>[]) {
  clickhouse.query.mockImplementation(async (sql: string) => {
    if (sql === FIELD_REGISTRY_VERSION_SQL) return [{ registry_version: version }];
    if (sql === FIELD_REGISTRY_SQL) return rows;
    throw new Error(`unexpected query: ${sql}`);
  });
}

describe("loadFieldRegistry", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    resetFieldRegistryCache();
  });

  it("pins the argMax-per-field read, scoped to the probed registry version", () => {
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "argMax(registry_version, version) AS registry_version",
    );
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "FROM corpscout.se_company_field_registry",
    );
    expect(FIELD_REGISTRY_VERSION_SQL).toContain(
      "WHERE datatype = {datatype:String} AND country = {country:String}",
    );
    // Consumers read the ReplacingMergeTree with argMax(..., version) per
    // (datatype, country, field), like se_code_labels (spec 4.3).
    for (const column of [
      "value_type",
      "display_group",
      "sources",
      "policy_name",
      "policy_version",
      "resolve_sql",
      "registry_version",
    ]) {
      expect(FIELD_REGISTRY_SQL).toContain(`argMax(${column}, version)`);
    }
    expect(FIELD_REGISTRY_SQL).toContain(
      "toUInt8(argMax(structured, version)) AS structured",
    );
    expect(FIELD_REGISTRY_SQL).toContain(
      "toUInt8(argMax(python_only, version)) AS python_only",
    );
    expect(FIELD_REGISTRY_SQL).toContain("GROUP BY field");
    // A field a later registry dropped keeps its old rows for ever; only the
    // rows stamped with the probed version are the registry.
    expect(FIELD_REGISTRY_SQL).toContain(
      "WHERE registry_version = {registryVersion:String}",
    );
    expect(FIELD_REGISTRY_SQL).toContain("ORDER BY field");
  });

  it("splits the projection row out and maps the field rows", async () => {
    stubRegistry("se-info-v1", [
      PROJECTION_ROW,
      row("description", { sources: ["llm", "esef", "wikidata", "scb"] }),
      row("website", {
        display_group: "scale",
        value_type: "url",
        python_only: 1,
        sources: ["domains", "wikidata"],
      }),
    ]);

    const registry = await loadFieldRegistry();

    expect(registry.version).toBe("se-info-v1");
    expect(registry.projectionSql).toBe(PROJECTION_ROW.resolve_sql);
    expect(registry.fields.map((entry) => entry.field)).toEqual([
      "description",
      "website",
    ]);
    expect(registry.fields[1]).toEqual({
      field: "website",
      valueType: "url",
      displayGroup: "scale",
      structured: false,
      pythonOnly: true,
      sources: ["domains", "wikidata"],
      policyName: "source_precedence",
      policyVersion: "source_precedence-v1",
      resolveSql:
        "INSERT INTO corpscout.se_company_field /* website */ SELECT {field:String}",
      registryVersion: "se-info-v1",
    });
    expect(clickhouse.query).toHaveBeenNthCalledWith(
      1,
      FIELD_REGISTRY_VERSION_SQL,
      SCOPE,
    );
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, FIELD_REGISTRY_SQL, {
      ...SCOPE,
      registryVersion: "se-info-v1",
    });
  });

  it("serves the cached registry while the probe answers the same version", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);

    const first = await loadFieldRegistry();
    const second = await loadFieldRegistry();

    expect(second).toBe(first);
    expect(clickhouse.query.mock.calls.map(([sql]) => sql)).toEqual([
      FIELD_REGISTRY_VERSION_SQL,
      FIELD_REGISTRY_SQL,
      FIELD_REGISTRY_VERSION_SQL,
    ]);
  });

  it("re-reads when the probe reports a different version", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);
    await loadFieldRegistry();

    stubRegistry("se-info-v2", [
      { ...PROJECTION_ROW, registry_version: "se-info-v2" },
      row("description", { registry_version: "se-info-v2" }),
      row("legal_name", {
        display_group: "identity",
        sources: ["bolagsverket", "scb", "wikidata"],
        registry_version: "se-info-v2",
      }),
    ]);
    const refreshed = await loadFieldRegistry();

    expect(refreshed.version).toBe("se-info-v2");
    expect(refreshed.fields.map((entry) => entry.field)).toEqual([
      "description",
      "legal_name",
    ]);
    expect(clickhouse.query).toHaveBeenLastCalledWith(FIELD_REGISTRY_SQL, {
      ...SCOPE,
      registryVersion: "se-info-v2",
    });
  });

  it("resetFieldRegistryCache makes the next call read the rows again", async () => {
    stubRegistry("se-info-v1", [PROJECTION_ROW, row("description")]);
    const first = await loadFieldRegistry();
    resetFieldRegistryCache();

    const second = await loadFieldRegistry();

    expect(second).not.toBe(first);
    expect(second).toEqual(first);
    expect(clickhouse.query).toHaveBeenCalledTimes(4);
  });

  it("refuses an empty registry table instead of resolving with nothing", async () => {
    // argMax over no rows is '' for a String column.
    stubRegistry("", []);
    await expect(loadFieldRegistry()).rejects.toThrow(
      "materialize se_company_field_registry_clickhouse",
    );
  });

  it("refuses a registry version without its projection row", async () => {
    stubRegistry("se-info-v1", [row("description")]);
    await expect(loadFieldRegistry()).rejects.toThrow(
      "no projection row (field = '*')",
    );
  });
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `npx vitest run tests/se-company-field-registry.server.test.ts`
Expected: FAIL — cannot resolve `~/lib/se-company-field-registry.server`.

- [ ] **Step 4: Implement the reader**

Create `app/lib/se-company-field-registry.server.ts`:

```ts
import { chQuery } from "~/lib/clickhouse.server";

/**
 * The SE company field registry as Dagster exports it into
 * corpscout.se_company_field_registry (spec 2026-09-02, section 4.3): one row
 * per field carrying the generated resolve statement, plus one row with
 * `field = '*'` whose resolve_sql is the wide-projection statement. The
 * backoffice READS this; the registry is owned by dagster_v3 code.
 */
export interface FieldRegistryEntry {
  field: string;
  valueType: string;
  displayGroup: string;
  structured: boolean;
  pythonOnly: boolean;
  /** Precedence order; position is the rank. */
  sources: string[];
  policyName: string;
  policyVersion: string;
  /** INSERT INTO corpscout.se_company_field ... SELECT, binding {field:String},
   * {company_ids:Array(String)}, {source_run_id:String}, {resolved_at:DateTime64(3, 'UTC')}. */
  resolveSql: string;
  registryVersion: string;
}

export interface FieldRegistry {
  version: string;
  fields: FieldRegistryEntry[];
  /** INSERT INTO corpscout.se_company_info ... SELECT, binding {company_ids:Array(String)}. */
  projectionSql: string;
}

const DATATYPE = "info";
const COUNTRY = "SE";
const PROJECTION_FIELD = "*";

/** The cheap probe every load runs: which registry version is current. The
 * table is a ReplacingMergeTree(version), so the newest `version` stamp
 * carries the current registry_version string. '' when the table is empty. */
export const FIELD_REGISTRY_VERSION_SQL = `SELECT argMax(registry_version, version) AS registry_version
FROM corpscout.se_company_field_registry
WHERE datatype = {datatype:String} AND country = {country:String}`;

/**
 * Every row of one registry version, argMax(..., version) per (datatype,
 * country, field) like se_code_labels. LowCardinality columns are wrapped in
 * toString() and the two Bools cast to UInt8 for one predictable JSON shape
 * (the INFO_SQL convention). Filtered to the probed registry_version: a field
 * a later registry dropped keeps its old rows in the table for ever.
 */
export const FIELD_REGISTRY_SQL = `WITH latest AS (
  SELECT
    field,
    toString(argMax(value_type, version)) AS value_type,
    toString(argMax(display_group, version)) AS display_group,
    toUInt8(argMax(structured, version)) AS structured,
    toUInt8(argMax(python_only, version)) AS python_only,
    argMax(sources, version) AS sources,
    toString(argMax(policy_name, version)) AS policy_name,
    argMax(policy_version, version) AS policy_version,
    argMax(resolve_sql, version) AS resolve_sql,
    argMax(registry_version, version) AS registry_version
  FROM corpscout.se_company_field_registry
  WHERE datatype = {datatype:String} AND country = {country:String}
  GROUP BY field
)
SELECT *
FROM latest
WHERE registry_version = {registryVersion:String}
ORDER BY field`;

interface FieldRegistryQueryRow {
  field: string;
  value_type: string;
  display_group: string;
  structured: number;
  python_only: number;
  sources: string[];
  policy_name: string;
  policy_version: string;
  resolve_sql: string;
  registry_version: string;
}

function toEntry(row: FieldRegistryQueryRow): FieldRegistryEntry {
  return {
    field: row.field,
    valueType: row.value_type,
    displayGroup: row.display_group,
    structured: row.structured === 1,
    pythonOnly: row.python_only === 1,
    sources: row.sources,
    policyName: row.policy_name,
    policyVersion: row.policy_version,
    resolveSql: row.resolve_sql,
    registryVersion: row.registry_version,
  };
}

let cached: FieldRegistry | undefined;

/** Tests only: forget the cached registry. */
export function resetFieldRegistryCache(): void {
  cached = undefined;
}

/**
 * The current registry. Every call probes the version (one aggregate over a
 * dozen rows); the full read runs only when the probed version differs from
 * the cached one, so a re-export under a new version is picked up on the next
 * decision without a restart, and the common case costs one small query.
 */
export async function loadFieldRegistry(): Promise<FieldRegistry> {
  const [probe] = await chQuery<{ registry_version: string }>(
    FIELD_REGISTRY_VERSION_SQL,
    { datatype: DATATYPE, country: COUNTRY },
  );
  const version = probe?.registry_version ?? "";
  if (version === "") {
    throw new Error(
      "corpscout.se_company_field_registry holds no info/SE rows; materialize se_company_field_registry_clickhouse first.",
    );
  }
  if (cached && cached.version === version) return cached;

  const rows = await chQuery<FieldRegistryQueryRow>(FIELD_REGISTRY_SQL, {
    datatype: DATATYPE,
    country: COUNTRY,
    registryVersion: version,
  });
  const projection = rows.find((row) => row.field === PROJECTION_FIELD);
  if (!projection) {
    throw new Error(
      `Registry ${version} has no projection row (field = '*'); the export is incomplete.`,
    );
  }
  cached = {
    version,
    fields: rows.filter((row) => row.field !== PROJECTION_FIELD).map(toEntry),
    projectionSql: projection.resolve_sql,
  };
  return cached;
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `npx vitest run tests/se-company-field-registry.server.test.ts && npm run typecheck`
Expected: PASS (7 tests), typecheck clean.

- [ ] **Step 6: Commit**

```bash
git add app/lib/se-company-field-registry.server.ts tests/se-company-field-registry.server.test.ts tests/se-field-registry.fixture.ts
git commit -m "feat(backoffice): read the SE field registry export with a version-keyed cache

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: `resolveCompanyFields` — run the registry's SQL for one company

**Files:**
- Create: `app/lib/se-company-field-resolve.server.ts`
- Test: `tests/se-company-field-resolve.server.test.ts`

**Interfaces:**
- Consumes: `chCommand` (Task 1); `loadFieldRegistry`, `FieldRegistry` (Task 2); `REGISTRY_FIXTURE` (Task 2 fixture).
- Produces (fixed):
  ```ts
  export type SkippedField = { field: string; reason: "python_only" | "unknown_field" };
  export interface ResolveCompanyFieldsResult { resolved: string[]; skipped: SkippedField[] }
  export function formatClickHouseDateTime64(date: Date): string   // 'YYYY-MM-DD HH:MM:SS.mmm' UTC
  export class SeCompanyFieldResolveError extends Error { readonly valueIds: string[] }  // message "Saved, but not resolved: <cause>"
  export async function resolveCompanyFields(companyId: string, fields: string[], opts?: { registry?: FieldRegistry; now?: Date; sourceRunId?: string; project?: boolean }): Promise<ResolveCompanyFieldsResult>
  ```
  `project` (default `true`) runs the wide projection after the field statements; `project: false` writes the long table only. It exists for the live test's production branch (Task 6, coordinator ruling 2026-09-02: never publish a synthetic company into `se_company_info` on production). The action path (`appendSeCompanyInfoFieldValues`, Task 5) never sets it.
  `formatClickHouseDateTime64` is exactly what `valueTimestamp()` at `se-company-info.server.ts:372-374` computes today (`toISOString().replace("T", " ").replace("Z", "")`); Task 5 replaces that private helper with this export.

- [ ] **Step 1: Write the failing tests**

Create `tests/se-company-field-resolve.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

// The fake ClickHouse command runner: every statement the resolver executes
// lands here, in order, with the exact parameters it bound.
const clickhouse = vi.hoisted(() => ({ command: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chCommand: clickhouse.command }));
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);

import {
  formatClickHouseDateTime64,
  resolveCompanyFields,
  SeCompanyFieldResolveError,
} from "~/lib/se-company-field-resolve.server";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const COMPANY = "5565200028";
const NOW = new Date("2026-09-02T10:15:30.123Z");
const entry = (field: string) =>
  REGISTRY_FIXTURE.fields.find((candidate) => candidate.field === field)!;

describe("formatClickHouseDateTime64", () => {
  // The driver renders a JS Date as epoch seconds ("1788...") which ClickHouse
  // accepts but which is not the text form the field-value rows carry; the
  // resolver binds this text so resolved_at reads like created_at.
  it("renders the DateTime64(3) text form in UTC, millisecond precision kept", () => {
    expect(formatClickHouseDateTime64(NOW)).toBe("2026-09-02 10:15:30.123");
    expect(
      formatClickHouseDateTime64(new Date("2026-01-05T00:00:00.000Z")),
    ).toBe("2026-01-05 00:00:00.000");
  });
});

describe("resolveCompanyFields", () => {
  beforeEach(() => {
    clickhouse.command.mockReset();
    clickhouse.command.mockResolvedValue(undefined);
    registryModule.loadFieldRegistry.mockReset();
  });

  it("runs each decided field's statement in field order, then the projection, with exact parameters", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["description", "legal_name"],
      { registry: REGISTRY_FIXTURE, now: NOW, sourceRunId: "backoffice:test" },
    );

    expect(result).toEqual({
      resolved: ["description", "legal_name"],
      skipped: [],
    });
    expect(clickhouse.command.mock.calls).toEqual([
      [
        entry("description").resolveSql,
        {
          field: "description",
          company_ids: [COMPANY],
          source_run_id: "backoffice:test",
          resolved_at: "2026-09-02 10:15:30.123",
        },
      ],
      [
        entry("legal_name").resolveSql,
        {
          field: "legal_name",
          company_ids: [COMPANY],
          source_run_id: "backoffice:test",
          resolved_at: "2026-09-02 10:15:30.123",
        },
      ],
      [REGISTRY_FIXTURE.projectionSql, { company_ids: [COMPANY] }],
    ]);
    expect(registryModule.loadFieldRegistry).not.toHaveBeenCalled();
  });

  it("stamps one backoffice:<uuid> run id on every statement when none is given", async () => {
    await resolveCompanyFields(COMPANY, ["description", "description_sv"], {
      registry: REGISTRY_FIXTURE,
    });

    const runIds = clickhouse.command.mock.calls
      .slice(0, 2)
      .map(([, params]) => (params as { source_run_id: string }).source_run_id);
    expect(runIds[0]).toMatch(
      /^backoffice:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(runIds[1]).toBe(runIds[0]);
    for (const [, params] of clickhouse.command.mock.calls.slice(0, 2)) {
      expect((params as { resolved_at: string }).resolved_at).toMatch(
        /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/,
      );
    }
  });

  // A python_only field is resolved by Dagster alone (spec 4.1 / 9): the
  // backoffice reports it so the page can say "applies on the next run".
  it("skips a python_only field with its reason and still projects the rest", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["website", "description"],
      { registry: REGISTRY_FIXTURE, now: NOW, sourceRunId: "backoffice:test" },
    );

    expect(result).toEqual({
      resolved: ["description"],
      skipped: [{ field: "website", reason: "python_only" }],
    });
    expect(clickhouse.command.mock.calls.map(([sql]) => sql)).toEqual([
      entry("description").resolveSql,
      REGISTRY_FIXTURE.projectionSql,
    ]);
  });

  it("skips a field the registry does not know", async () => {
    const result = await resolveCompanyFields(COMPANY, ["not_a_field"], {
      registry: REGISTRY_FIXTURE,
    });

    expect(result).toEqual({
      resolved: [],
      skipped: [{ field: "not_a_field", reason: "unknown_field" }],
    });
  });

  it("does not re-pivot the wide row when nothing was resolved", async () => {
    await resolveCompanyFields(COMPANY, ["website"], {
      registry: REGISTRY_FIXTURE,
    });
    expect(clickhouse.command).not.toHaveBeenCalled();
  });

  // The live test's production branch resolves a synthetic company into the
  // long table only: the wide row would flow into se_companies_serving.
  it("writes the long table only when project is false", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["description", "legal_name"],
      {
        registry: REGISTRY_FIXTURE,
        now: NOW,
        sourceRunId: "backoffice:test",
        project: false,
      },
    );

    expect(result).toEqual({
      resolved: ["description", "legal_name"],
      skipped: [],
    });
    expect(clickhouse.command.mock.calls.map(([sql]) => sql)).toEqual([
      entry("description").resolveSql,
      entry("legal_name").resolveSql,
    ]);
  });

  it("loads the registry when the caller does not pass one", async () => {
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);

    await resolveCompanyFields(COMPANY, ["description"], {
      now: NOW,
      sourceRunId: "backoffice:test",
    });

    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(1);
    expect(clickhouse.command).toHaveBeenCalledTimes(2);
  });

  it("lets a ClickHouse failure propagate untouched", async () => {
    clickhouse.command.mockRejectedValueOnce(new Error("Code: 241. DB::Exception: Memory limit"));
    await expect(
      resolveCompanyFields(COMPANY, ["description"], { registry: REGISTRY_FIXTURE }),
    ).rejects.toThrow("Memory limit");
  });
});

describe("SeCompanyFieldResolveError", () => {
  it("names what was saved and why resolving failed", () => {
    const error = new SeCompanyFieldResolveError(
      ["66666666-6666-4666-8666-666666666666"],
      new Error("Code: 241. DB::Exception: Memory limit"),
    );
    expect(error.name).toBe("SeCompanyFieldResolveError");
    expect(error.valueIds).toEqual(["66666666-6666-4666-8666-666666666666"]);
    expect(error.message).toBe(
      "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
    );
    expect(error.cause).toBeInstanceOf(Error);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/se-company-field-resolve.server.test.ts`
Expected: FAIL — cannot resolve `~/lib/se-company-field-resolve.server`.

- [ ] **Step 3: Implement the resolver**

Create `app/lib/se-company-field-resolve.server.ts`:

```ts
import { randomUUID } from "node:crypto";
import { chCommand } from "~/lib/clickhouse.server";
import {
  loadFieldRegistry,
  type FieldRegistry,
} from "~/lib/se-company-field-registry.server";

/**
 * Resolves ONE company's fields with the registry's generated SQL right after
 * a decision (spec 2026-09-02, section 9), so the reviewer sees the resolved
 * value on the reload instead of waiting for the sensor. Dagster runs the same
 * statements in bulk later; the bulk result is identical and lands as a
 * same-value version of the ReplacingMergeTree rows.
 */

export type SkippedField = {
  field: string;
  reason: "python_only" | "unknown_field";
};

export interface ResolveCompanyFieldsResult {
  resolved: string[];
  skipped: SkippedField[];
}

/**
 * ClickHouse's DateTime64(3) text form, `YYYY-MM-DD HH:MM:SS.mmm` in UTC.
 * Bound as a string on purpose: the driver renders a JS Date as epoch seconds,
 * which ClickHouse also parses but which is not the form the field-value
 * rows' created_at carries, and one form is easier to grep for than two.
 */
export function formatClickHouseDateTime64(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

/**
 * Raised by appendSeCompanyInfoFieldValues when the decision rows were
 * inserted but resolving them failed. The decision is NOT lost: it is in
 * se_company_info_field_value, and se_company_info_field_value_sensor
 * re-resolves the company in bulk. Carries the ids so the page can say so.
 */
export class SeCompanyFieldResolveError extends Error {
  readonly valueIds: string[];

  constructor(valueIds: string[], cause: unknown) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    super(
      `Saved, but not resolved: ${reason}. The decision is kept and applies on the next pipeline run.`,
      { cause },
    );
    this.name = "SeCompanyFieldResolveError";
    this.valueIds = valueIds;
  }
}

/**
 * Executes each field's resolve statement for `[companyId]` in the order
 * given, then the wide projection once (one statement for the company, not
 * one per field). A python_only field belongs to Dagster alone and is
 * reported as skipped; so is a field the registry does not know (the
 * validator refuses those earlier, but this function is also called directly).
 * The projection is skipped when nothing was resolved: the long table did not
 * change, so the wide row would not either. `project: false` skips it
 * unconditionally -- the live test's production branch resolves a synthetic
 * company into the long table only, because a wide row would flow into
 * se_companies_serving; the action path never sets it.
 */
export async function resolveCompanyFields(
  companyId: string,
  fields: string[],
  opts: {
    registry?: FieldRegistry;
    now?: Date;
    sourceRunId?: string;
    project?: boolean;
  } = {},
): Promise<ResolveCompanyFieldsResult> {
  const registry = opts.registry ?? (await loadFieldRegistry());
  const sourceRunId = opts.sourceRunId ?? `backoffice:${randomUUID()}`;
  const resolvedAt = formatClickHouseDateTime64(opts.now ?? new Date());
  const byName = new Map(registry.fields.map((entry) => [entry.field, entry]));

  const resolved: string[] = [];
  const skipped: SkippedField[] = [];
  for (const field of fields) {
    const entry = byName.get(field);
    if (!entry) {
      skipped.push({ field, reason: "unknown_field" });
      continue;
    }
    if (entry.pythonOnly) {
      skipped.push({ field, reason: "python_only" });
      continue;
    }
    await chCommand(entry.resolveSql, {
      field,
      company_ids: [companyId],
      source_run_id: sourceRunId,
      resolved_at: resolvedAt,
    });
    resolved.push(field);
  }
  if (opts.project !== false && resolved.length > 0) {
    await chCommand(registry.projectionSql, { company_ids: [companyId] });
  }
  return { resolved, skipped };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/se-company-field-resolve.server.test.ts && npm run typecheck`
Expected: PASS (10 tests), typecheck clean.

- [ ] **Step 5: Commit**

```bash
git add app/lib/se-company-field-resolve.server.ts tests/se-company-field-resolve.server.test.ts
git commit -m "feat(backoffice): resolve one company's fields with the registry's SQL

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 4: Validate decisions against the registry export

**Files:**
- Modify: `app/lib/se-info-field-values.ts:1-72` (header comment, enums, draft type), `:85-91` (guards), `:101` (`sourceRefFor` signature), `:112-120` (validator signature and checks)
- Modify: `app/lib/se-info-field-value-form.ts:33-37` (import), `:47-50` (context), `:61-63` (`isField`), `:85-90` (`useSource`), `:177-180` (`edit`), `:196-198` (`release`), `:211-227` (dispatcher)
- Modify: `app/lib/se-company-info.server.ts:1-10` (imports), `:393-396` (append signature and validation)
- Modify: `app/routes/admin-se-company-info.tsx:1-12` (imports), `:35-49` (action)
- Modify: `app/components/admin/se-company-info-review-workspace.tsx:64` (import), `:670` (release-button loop)
- Test: `tests/se-info-field-values.test.ts`, `tests/se-info-field-value-form.test.ts`, `tests/se-company-info.server.test.ts`, `tests/admin-se-company-info.test.tsx`

**Interfaces:**
- Consumes: `FieldRegistry`, `loadFieldRegistry` (Task 2); `REGISTRY_FIXTURE` (Task 2 fixture).
- Produces:
  ```ts
  // app/lib/se-info-field-values.ts (client-safe)
  export const REVIEWER_SOURCE = "reviewer";
  export interface SeInfoFieldVocabulary { fields: string[]; sources: string[] }
  export function fieldVocabulary(registry: { fields: ReadonlyArray<{ field: string; sources: ReadonlyArray<string> }> }): SeInfoFieldVocabulary
  export function validateSeInfoFieldValue(input: SeInfoFieldValueInput, registry: SeInfoFieldVocabulary): SeInfoFieldValueDraft   // draft.field / draft.source are now `string`
  // app/lib/se-info-field-value-form.ts
  export interface SeInfoFieldValueContext { companyId: string; suggestions: SeCompanyInfoSuggestionRow[]; fields: string[] }
  // app/lib/se-company-info.server.ts (Task 5 extends the return type)
  export async function appendSeCompanyInfoFieldValues(inputs: SeInfoFieldValueInput[], opts?: { registry?: FieldRegistry }): Promise<{ valueIds: string[] }>
  ```
- Decision on the enums: `SE_INFO_FIELDS`, `SE_INFO_VALUE_SOURCES`, `SeInfoField`, `SeInfoValueSource` are **removed** from `se-info-field-values.ts`. Nothing hard-codes the vocabulary any more: the validator takes it from `fieldVocabulary(registry)`, the form builder takes `context.fields` from the same registry, and the tests take it from `REGISTRY_FIXTURE`. The only literal list left is `RELEASABLE_FIELDS = ["description", "description_sv"]` inside the workspace component — a UI fact about which two release buttons the phase-A page renders (phase B renders the registry's display groups instead), not a validation rule.

- [ ] **Step 1: Rewrite the validator test**

FIRST route every call in `tests/se-info-field-values.test.ts` through a local helper (the import line has no `(` and is untouched; this must run before the header below is written, or the helper's own body would be rewritten into a recursive call):

```bash
sed -i '' 's/validateSeInfoFieldValue(/validate(/g' tests/se-info-field-values.test.ts
```

THEN replace the top of the file (lines 1–31: imports, `SUGGESTION`, `base`, the `"se-info field-value vocabulary"` describe) with:

```ts
import { describe, expect, it } from "vitest";
import {
  fieldVocabulary,
  REVIEWER_SOURCE,
  SeInfoFieldValueValidationError,
  validateSeInfoFieldValue,
  type SeInfoFieldValueInput,
} from "~/lib/se-info-field-values";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const SUGGESTION = "11111111-1111-4111-8111-111111111111";
const VOCABULARY = fieldVocabulary(REGISTRY_FIXTURE);
const validate = (input: SeInfoFieldValueInput) =>
  validateSeInfoFieldValue(input, VOCABULARY);
const base = {
  companyId: "5565200028",
  field: "description",
  source: "scb",
  sourceRef: "scb:5565200028",
};

describe("fieldVocabulary", () => {
  // The vocabulary is DERIVED from the registry export (spec 11): the field
  // names as listed, and the union of every field's sources plus `reviewer`
  // -- reviewer decisions are not a source in the registry (spec 4.1) but
  // are a source of a decision row (spec 6).
  it("lists the registry's fields and the union of their sources plus reviewer", () => {
    expect(VOCABULARY.fields).toEqual([
      "description",
      "description_sv",
      "legal_name",
      "website",
    ]);
    expect(VOCABULARY.sources).toEqual([
      "llm",
      "esef",
      "wikidata",
      "scb",
      "bolagsverket",
      "domains",
      REVIEWER_SOURCE,
    ]);
  });

  it("names reviewer exactly once even when a registry lists it", () => {
    expect(
      fieldVocabulary({ fields: [{ field: "x", sources: ["reviewer"] }] })
        .sources,
    ).toEqual(["reviewer"]);
  });
});
```

Then, in the `"accepts only the known sources"` test, replace `for (const source of SE_INFO_VALUE_SOURCES) {` with `for (const source of VOCABULARY.sources) {`. Finally, in `"accepts only the known fields"`, the field that must be refused is no longer `legal_name` (the fixture registry knows it); change that expectation to:

```ts
    expect(validate({ ...base, field: "legal_name", value: "x" }).field).toBe(
      "legal_name",
    );
    expect(() => validate({ ...base, field: "not_a_field", value: "x" })).toThrow(
      "Unknown field.",
    );
```

- [ ] **Step 2: Update the form-builder test**

In `tests/se-info-field-value-form.test.ts`, replace the `build` helper (lines 33–36) with:

```ts
/** The phase-A page decides only the two description columns; the registry
 * lists more, and the builder must accept whatever list it is handed. */
const PHASE_A_FIELDS = ["description", "description_sv"];

const build = (
  entries: Record<string, string>,
  suggestions: SeCompanyInfoSuggestionRow[] = [],
  fields: string[] = PHASE_A_FIELDS,
) =>
  buildFieldValueInputs(form(entries), {
    companyId: COMPANY_ID,
    suggestions,
    fields,
  });
```

and add one test at the end of the `"buildFieldValueInputs -- use-source"` describe:

```ts
  it("accepts any field the registry hands it", () => {
    expect(
      build(
        {
          intent: "use-source",
          field: "legal_name",
          value: "Alpha AB",
          source: "scb",
          source_ref: "scb:1",
        },
        [],
        [...PHASE_A_FIELDS, "legal_name"],
      ),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "legal_name",
          value: "Alpha AB",
          source: "scb",
          sourceRef: "scb:1",
          sourceAt: null,
        },
      ],
    });
  });
```

- [ ] **Step 3: Update the server test**

In `tests/se-company-info.server.test.ts`:

1. After the existing `vi.mock("~/lib/clickhouse.server", ...)` block (line 7) add:

```ts
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);
```

and add `import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";` after the other imports.

2. In the `"appendSeCompanyInfoFieldValues"` describe's `beforeEach`, add:

```ts
    registryModule.loadFieldRegistry.mockReset();
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);
```

3. In `"validates every input before touching ClickHouse"`, the fixture registry knows `legal_name`, so change `{ ...scbValue, field: "legal_name" }` to `{ ...scbValue, field: "not_a_field" }`.

4. Add two tests to that describe:

```ts
  it("validates against the registry it is handed, without loading one", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);

    await appendSeCompanyInfoFieldValues(
      [{ ...scbValue, field: "legal_name", value: "Alpha AB" }],
      { registry: REGISTRY_FIXTURE },
    );

    expect(registryModule.loadFieldRegistry).not.toHaveBeenCalled();
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
  });

  it("loads the registry when none is handed in, and refuses a source it does not list", async () => {
    await expect(
      appendSeCompanyInfoFieldValues([{ ...scbValue, source: "ratsit" }]),
    ).rejects.toThrow("Unknown source.");
    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(1);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });
```

- [ ] **Step 4: Update the route test**

In `tests/admin-se-company-info.test.tsx`:

1. After the `vi.mock("~/lib/se-company-info.server", () => server);` line add:

```ts
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);
```

and `import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";` after the other imports.

2. In the action describe's `beforeEach` (line 776) add:

```ts
    registryModule.loadFieldRegistry.mockReset();
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);
```

3. The two `expect(server.appendSeCompanyInfoFieldValues).toHaveBeenCalledWith([ ... ]);` assertions (lines 816 and 897) gain the registry as the second argument — change each closing `]);` of those two calls to `], { registry: REGISTRY_FIXTURE });`.

4. Add one test to the action describe:

```ts
  it("checks the posted field against the registry, not a built-in list", async () => {
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: ["66666666-6666-4666-8666-666666666666"],
    });

    const known = await postAction({
      intent: "use-source",
      field: "legal_name",
      value: "Alpha AB",
      source: "scb",
      source_ref: "scb:1",
    });
    expect(known).toMatchObject({ ok: true });

    const unknown = await postAction({
      intent: "release",
      field: "not_a_field",
    });
    expect(unknown).toEqual({ ok: false, error: "Unknown field." });
    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(2);
  });
```

- [ ] **Step 5: Run the four test files to verify they fail**

Run: `npx vitest run tests/se-info-field-values.test.ts tests/se-info-field-value-form.test.ts tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx`
Expected: FAIL — `fieldVocabulary` is not exported; `validate` calls pass a second argument that is ignored (`"Unknown field."` for `legal_name`); the action never calls `loadFieldRegistry`.

- [ ] **Step 6: Rewrite the validator module**

Replace lines 1–72 of `app/lib/se-info-field-values.ts` (everything before `// Legal entities carry a 10-digit ...`) with:

```ts
/**
 * Client-safe validator for the Sweden company-info field-value store
 * (`corpscout.se_company_info_field_value`, migration 000371).
 *
 * The store replaced the company-info correction ledger: there are no kinds,
 * no evidence hashes and no undo chain here, because a field's live value is
 * simply the row written last for it (greatest `(created_at, value_id)`), and
 * an undo is just the previous value written again -- or NULL, which releases
 * the field back to the value the pipeline computes. So this validator only
 * has to answer "is this one row insertable?".
 *
 * WHICH fields and sources exist is not this module's to say: the field
 * registry (dagster_v3 code, exported to corpscout.se_company_field_registry,
 * spec 2026-09-02 section 4) declares them, and the table's CHECK constraints
 * are widened to the same lists by migration. The caller hands the registry's
 * vocabulary in (`fieldVocabulary(await loadFieldRegistry())`), so this file
 * stays importable by the client bundle. The per-source `source_ref` rule
 * mirrors what Dagster reads back out of the column.
 *
 * The ADDRESS and PERSON ledgers are unrelated and still correction-shaped;
 * their validators (`se-address-corrections.ts`, `se-person-corrections.ts`)
 * are untouched by this module.
 */

/** A reviewer's own wording. Not a registry source (decisions win by
 * construction, spec 4.1), but a valid `source` of a decision row (spec 6). */
export const REVIEWER_SOURCE = "reviewer";

/** What the validator checks a row against: the registry's field names, and
 * the union of every field's sources plus `reviewer`. */
export interface SeInfoFieldVocabulary {
  fields: string[];
  sources: string[];
}

/** Derives the vocabulary from a registry export (`FieldRegistry` from
 * se-company-field-registry.server.ts, typed structurally so this module
 * never imports a `.server` module). Sources keep first-seen order. */
export function fieldVocabulary(registry: {
  fields: ReadonlyArray<{ field: string; sources: ReadonlyArray<string> }>;
}): SeInfoFieldVocabulary {
  const sources = new Set<string>();
  for (const entry of registry.fields) {
    for (const source of entry.sources) sources.add(source);
  }
  sources.add(REVIEWER_SOURCE);
  return {
    fields: registry.fields.map((entry) => entry.field),
    sources: [...sources],
  };
}

export class SeInfoFieldValueValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeInfoFieldValueValidationError";
  }
}

export interface SeInfoFieldValueInput {
  companyId: string;
  field: string;
  /** The text to publish, or null to release the field to the pipeline. */
  value: string | null;
  source: string;
  /** The record this text came from: a source_record_uid, a suggestion_id for
   * `llm`, and nothing at all for `reviewer`. */
  sourceRef?: string;
  /** When the source observed it (the artifact's observed_at / the
   * suggestion's created_at); null when there is no such moment. */
  sourceAt?: string | null;
  note?: string;
}

/** One insertable `se_company_info_field_value` row, minus the columns the
 * server fills (`value_id`, `decided_by`, `created_at`). */
export interface SeInfoFieldValueDraft {
  company_id: string;
  field: string;
  value: string | null;
  source: string;
  source_ref: string;
  source_at: string | null;
  note: string;
}
```

Then, further down in the same file:

1. Delete the two guards `isField` and `isSource` (old lines 85–91).
2. Change `function sourceRefFor(source: SeInfoValueSource, raw: string): string {` to `function sourceRefFor(source: string, raw: string): string {` and its first line to `if (source === REVIEWER_SOURCE) return "";`.
3. Replace the validator's signature and the two membership checks:

```ts
export function validateSeInfoFieldValue(
  input: SeInfoFieldValueInput,
  registry: SeInfoFieldVocabulary,
): SeInfoFieldValueDraft {
  const companyId = input.companyId.trim();
  if (!COMPANY_ID_PATTERN.test(companyId)) {
    fail("Company must be a 10-digit or 12-digit Swedish company id.");
  }
  if (!registry.fields.includes(input.field)) fail("Unknown field.");
  if (!registry.sources.includes(input.source)) fail("Unknown source.");
```

The rest of the function body (value, note, sourceAt, the returned draft) is unchanged.

- [ ] **Step 7: Make the form builder take the field list from its context**

In `app/lib/se-info-field-value-form.ts`:

1. Replace the import block at lines 33–37 with:

```ts
import type { SeInfoFieldValueInput } from "~/lib/se-info-field-values";
```

2. Replace the `SeInfoFieldValueContext` interface (lines 47–50) with:

```ts
/** What the page knows that the form alone cannot carry: whose company this is,
 * which suggestions are actually on it (a suggestion id names text the
 * reviewer never typed, so it is read from the row rather than the post), and
 * which fields the registry declares (`fieldVocabulary(registry).fields`), so
 * a posted field is checked against the same list the store validates with. */
export interface SeInfoFieldValueContext {
  companyId: string;
  suggestions: SeCompanyInfoSuggestionRow[];
  fields: string[];
}
```

3. Delete the `isField` type guard (lines 61–63).

4. `useSource`: change the signature to `function useSource(form: FormData, context: SeInfoFieldValueContext): SeInfoFieldValueRequest {`, the first check to `if (!context.fields.includes(field)) return refuse("Unknown field.");`, and `companyId,` inside the returned input to `companyId: context.companyId,`. (The `ARTIFACT_SOURCES` check stays: the phase-A page copies text from the three artifact cards only.)

5. `edit`: change the signature to `function edit(form: FormData, context: SeInfoFieldValueContext): SeInfoFieldValueRequest {`, the loop header to `for (const field of context.fields) {`, and both `companyId,` occurrences inside the pushed inputs to `companyId: context.companyId,`.

6. `release`: change the signature to `function release(form: FormData, context: SeInfoFieldValueContext): SeInfoFieldValueRequest {`, the check to `if (!context.fields.includes(field)) return refuse("Unknown field.");`, and the input to `{ companyId: context.companyId, field, value: null, source: "reviewer" }`.

7. The dispatcher's four cases become `return useSource(form, context);`, `return useSuggestion(form, context);`, `return edit(form, context);`, `return release(form, context);`.

- [ ] **Step 8: Thread the registry through the store**

In `app/lib/se-company-info.server.ts`:

1. Replace the import block at lines 6–10 with:

```ts
import {
  fieldVocabulary,
  SeInfoFieldValueValidationError,
  validateSeInfoFieldValue,
  type SeInfoFieldValueInput,
} from "~/lib/se-info-field-values";
import {
  loadFieldRegistry,
  type FieldRegistry,
} from "~/lib/se-company-field-registry.server";
```

2. Replace the signature and the first line of `appendSeCompanyInfoFieldValues` (lines 393–396):

```ts
export async function appendSeCompanyInfoFieldValues(
  inputs: SeInfoFieldValueInput[],
  opts: { registry?: FieldRegistry } = {},
): Promise<{ valueIds: string[] }> {
  // The registry export says which fields and sources exist (spec 11); the
  // action loads it once per post and hands it in, so a post validates and
  // resolves against ONE registry version even if an export lands mid-way.
  const registry = opts.registry ?? (await loadFieldRegistry());
  const vocabulary = fieldVocabulary(registry);
  const drafts = inputs.map((input) =>
    validateSeInfoFieldValue(input, vocabulary),
  );
```

- [ ] **Step 9: Load the registry once in the action**

In `app/routes/admin-se-company-info.tsx`:

1. Replace line 11 (`import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";`) with:

```ts
import { loadFieldRegistry } from "~/lib/se-company-field-registry.server";
import {
  fieldVocabulary,
  SeInfoFieldValueValidationError,
} from "~/lib/se-info-field-values";
```

2. Replace the action body from `const detail = await loadSeCompanyInfoDetail(params.companyId);` through the `appendSeCompanyInfoFieldValues` call:

```ts
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
    fields: fieldVocabulary(registry).fields,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  try {
    const { valueIds } = await appendSeCompanyInfoFieldValues(built.inputs, {
      registry,
    });
    return { ok: true as const, valueIds };
  } catch (error) {
```

(the `catch` body is unchanged.)

- [ ] **Step 10: Give the workspace its own release-button list**

In `app/components/admin/se-company-info-review-workspace.tsx`, replace line 64 (`import { SE_INFO_FIELDS } from "~/lib/se-info-field-values";`) with:

```ts
/** The fields this phase-A page lets a reviewer release: the two description
 * columns its editor renders. Phase B renders every registry field from the
 * export instead (spec 2026-09-02, section 11), and this list goes. */
const RELEASABLE_FIELDS = ["description", "description_sv"] as const;
```

and line 670 `{SE_INFO_FIELDS.map((field) => (` with `{RELEASABLE_FIELDS.map((field) => (`.

- [ ] **Step 11: Run the tests and the typecheck**

Run: `npx vitest run tests/se-info-field-values.test.ts tests/se-info-field-value-form.test.ts tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx tests/company-description-card.test.tsx && npm run typecheck`
Expected: PASS; typecheck clean. If `tsc` reports another importer of `SE_INFO_FIELDS`/`SeInfoField`, that file must switch to `context.fields`/`string` the same way (`rg -n "SE_INFO_FIELDS|SE_INFO_VALUE_SOURCES|SeInfoField\b|SeInfoValueSource\b" app tests` must return nothing).

- [ ] **Step 12: Commit**

```bash
git add app/lib/se-info-field-values.ts app/lib/se-info-field-value-form.ts app/lib/se-company-info.server.ts app/routes/admin-se-company-info.tsx app/components/admin/se-company-info-review-workspace.tsx tests/se-info-field-values.test.ts tests/se-info-field-value-form.test.ts tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx
git commit -m "refactor(backoffice): validate SE info decisions against the field registry

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 5: Resolve the company right after the decision; result and banner

**Files:**
- Modify: `app/lib/se-company-info.server.ts:1-5` (imports), `:370-374` (delete `valueTimestamp`), `:393-434` (append: `now`, resolve, return shape)
- Modify: `app/routes/admin-se-company-info.tsx` (action result and the resolve-failure branch)
- Modify: `app/components/admin/se-company-info-review-workspace.tsx:67-70` (result type), `:795-813` (banner)
- Test: `tests/se-company-info.server.test.ts`, `tests/admin-se-company-info.test.tsx`

**Interfaces:**
- Consumes: `resolveCompanyFields`, `formatClickHouseDateTime64`, `SeCompanyFieldResolveError`, `SkippedField`, `ResolveCompanyFieldsResult` (Task 3); `FieldRegistry` (Task 2).
- Produces (fixed):
  ```ts
  export async function appendSeCompanyInfoFieldValues(inputs: SeInfoFieldValueInput[], opts?: { registry?: FieldRegistry; now?: Date }): Promise<{ valueIds: string[]; resolved: string[]; skipped: SkippedField[] }>
  // se-company-info-review-workspace.tsx
  export type SeCompanyInfoReviewResult =
    | { ok: true; valueIds: string[]; resolved: string[]; skipped: SkippedField[] }
    | { ok: false; error: string; valueIds?: string[] }
    | null;
  ```
  Banner copy: success title `Saved and resolved.` when `skipped` is empty, else `Saved. <field[, field]> applies|apply on the next run.`; description keeps the row count (`1 value row saved` / `N value rows saved`). Failure after insert: `{ ok: false, error, valueIds }` renders under the title `Saved, not resolved` (a plain refusal keeps `Not saved`).

- [ ] **Step 1: Extend the server test**

In `tests/se-company-info.server.test.ts`:

1. Add `chCommand: vi.fn(),` to the `~/lib/clickhouse.server` mock factory's object (the resolve module imports it; the fake is never called because `resolveCompanyFields` is mocked below), and after the registry mock add:

```ts
const resolveModule = vi.hoisted(() => ({ resolveCompanyFields: vi.fn() }));
vi.mock("~/lib/se-company-field-resolve.server", async (importOriginal) => ({
  ...(await importOriginal<
    typeof import("~/lib/se-company-field-resolve.server")
  >()),
  resolveCompanyFields: resolveModule.resolveCompanyFields,
}));
```

and `import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";` with the other imports.

2. In the append describe's `beforeEach` add:

```ts
    resolveModule.resolveCompanyFields.mockReset();
    resolveModule.resolveCompanyFields.mockResolvedValue({
      resolved: [],
      skipped: [],
    });
```

3. In `"appends every row in one insert, with backoffice provenance"`, change the line `const { valueIds } = await appendSeCompanyInfoFieldValues([` to `const { valueIds, resolved, skipped } = await appendSeCompanyInfoFieldValues([` (the two inputs it passes are unchanged) and append at the end of the test:

```ts
    // One resolve call for the whole decision, in the order decided, against
    // the registry the store validated with, at the same instant as created_at
    // -- so resolved_at and created_at read alike on the two tables.
    expect(resolveModule.resolveCompanyFields).toHaveBeenCalledTimes(1);
    const [companyId, fields, opts] =
      resolveModule.resolveCompanyFields.mock.calls[0];
    expect(companyId).toBe(COMPANY);
    expect(fields).toEqual(["description", "description_sv"]);
    expect(opts.registry).toBe(REGISTRY_FIXTURE);
    expect(opts.now.toISOString().replace("T", " ").replace("Z", "")).toBe(
      rows[0].created_at,
    );
    expect(resolved).toEqual([]);
    expect(skipped).toEqual([]);
```

4. Add three tests to the describe:

```ts
  it("returns what the resolver resolved and skipped", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    resolveModule.resolveCompanyFields.mockResolvedValue({
      resolved: ["description"],
      skipped: [{ field: "website", reason: "python_only" }],
    });

    const result = await appendSeCompanyInfoFieldValues(
      [
        scbValue,
        {
          companyId: COMPANY,
          field: "website",
          value: "https://alpha.example",
          source: "reviewer",
        },
      ],
      { registry: REGISTRY_FIXTURE },
    );

    expect(result.valueIds).toHaveLength(2);
    expect(result.resolved).toEqual(["description"]);
    expect(result.skipped).toEqual([{ field: "website", reason: "python_only" }]);
  });

  it("uses the caller's clock for created_at and resolved_at", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    const now = new Date("2026-09-02T10:15:30.123Z");

    await appendSeCompanyInfoFieldValues([scbValue], {
      registry: REGISTRY_FIXTURE,
      now,
    });

    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0].created_at).toBe("2026-09-02 10:15:30.123");
    expect(resolveModule.resolveCompanyFields.mock.calls[0][2].now).toBe(now);
  });

  // The rows are already in the store when resolving fails; the sensor picks
  // them up. The failure must say so and carry the ids -- not read as "the
  // decision was refused", and not silently succeed.
  it("keeps the decision and raises SeCompanyFieldResolveError when resolving fails after the insert", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    resolveModule.resolveCompanyFields.mockRejectedValue(
      new Error("Code: 241. DB::Exception: Memory limit"),
    );

    const error: unknown = await appendSeCompanyInfoFieldValues([scbValue], {
      registry: REGISTRY_FIXTURE,
    }).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(SeCompanyFieldResolveError);
    const resolveError = error as SeCompanyFieldResolveError;
    expect(resolveError.message).toBe(
      "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
    );
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(resolveError.valueIds).toEqual([rows[0].value_id]);
  });
```

- [ ] **Step 2: Extend the route test**

In `tests/admin-se-company-info.test.tsx`:

1. Add `import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";` with the other imports.

2. In the action describe's `beforeEach`, change the default to `server.appendSeCompanyInfoFieldValues.mockResolvedValue({ valueIds: [], resolved: [], skipped: [] });`.

3. In `"writes one artifact's text and returns the ids"`, the mock becomes:

```ts
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: ["66666666-6666-4666-8666-666666666666"],
      resolved: ["description"],
      skipped: [],
    });
```

and the expectation:

```ts
    expect(result).toEqual({
      ok: true,
      valueIds: ["66666666-6666-4666-8666-666666666666"],
      resolved: ["description"],
      skipped: [],
    });
```

In `"builds both languages from the suggestion the page is showing"`, the mock becomes:

```ts
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: [
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });
```

and the expectation:

```ts
    expect(result).toEqual({
      ok: true,
      valueIds: [
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });
```

4. In `"checks the posted field against the registry, not a built-in list"` (Task 4), the mock gains `resolved: ["legal_name"], skipped: []`.

5. Add one action test:

```ts
  it("reports a resolve failure as saved-but-not-resolved, with the ids", async () => {
    server.appendSeCompanyInfoFieldValues.mockRejectedValue(
      new SeCompanyFieldResolveError(
        ["66666666-6666-4666-8666-666666666666"],
        new Error("Code: 241. DB::Exception: Memory limit"),
      ),
    );

    const result = await postAction({ intent: "release", field: "description" });

    expect(result).toEqual({
      ok: false,
      error:
        "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
      valueIds: ["66666666-6666-4666-8666-666666666666"],
    });
  });
```

6. Replace the render test `"confirms a save and renders the not-published state"` with:

```tsx
  it("confirms a save that resolved, and renders the not-published state", () => {
    const html = render(detail, {
      ok: true,
      valueIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });
    expect(html).toContain("Saved and resolved.");
    expect(html).toContain("2 value rows saved");
    expect(html).not.toContain("on the next run.");
    expect(
      renderToStaticMarkup(
        <SeCompanyInfoNotPublished companyId="5565200028" />,
      ),
    ).toContain("not published");
  });

  // A python_only field is Dagster's alone (spec 9): the row is saved, the
  // page says when it lands. One field says "applies", several say "apply".
  it("names the fields that apply on the next run", () => {
    const one = render(detail, {
      ok: true,
      valueIds: ["22222222-2222-4222-8222-222222222222"],
      resolved: [],
      skipped: [{ field: "website", reason: "python_only" }],
    });
    expect(one).toContain("Saved. website applies on the next run.");
    expect(one).toContain("1 value row saved");
    expect(one).not.toContain("Saved and resolved.");

    const two = render(detail, {
      ok: true,
      valueIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
      resolved: [],
      skipped: [
        { field: "website", reason: "python_only" },
        { field: "employee_count", reason: "python_only" },
      ],
    });
    expect(two).toContain(
      "Saved. website, employee_count apply on the next run.",
    );
  });

  it("tells a saved-but-unresolved decision apart from a refused one", () => {
    const unresolved = render(detail, {
      ok: false,
      error: "Saved, but not resolved: Memory limit. The decision is kept and applies on the next pipeline run.",
      valueIds: ["22222222-2222-4222-8222-222222222222"],
    });
    expect(unresolved).toContain("Saved, not resolved");
    expect(unresolved).toContain("The decision is kept");
    expect(unresolved).not.toContain("Not saved");

    const refused = render(detail, { ok: false, error: "Nothing changed." });
    expect(refused).toContain("Not saved");
    expect(refused).toContain("Nothing changed.");
  });
```

- [ ] **Step 3: Run both test files to verify they fail**

Run: `npx vitest run tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx`
Expected: FAIL — `resolveCompanyFields` never called; results lack `resolved`/`skipped`; the banner still says "published on the next rebuild"; the `ok: false` render lacks "Saved, not resolved". (`tsc` will also reject the new result shapes until Step 6.)

- [ ] **Step 4: Resolve inside the store**

In `app/lib/se-company-info.server.ts`:

1. Add to the imports:

```ts
import {
  formatClickHouseDateTime64,
  resolveCompanyFields,
  SeCompanyFieldResolveError,
  type ResolveCompanyFieldsResult,
  type SkippedField,
} from "~/lib/se-company-field-resolve.server";
```

2. Delete the private `valueTimestamp` function and its doc comment (lines 370–374).

3. Replace the function's doc comment, signature and body from `const createdAt = valueTimestamp();` to the end:

```ts
/**
 * Appends one decision -- which may be several rows, e.g. both languages of an
 * About-card choice -- to the field-value store, resolves the decided fields
 * for this company with the registry's SQL and re-pivots its wide row (spec
 * 2026-09-02, section 9), and returns the ids written plus what the resolver
 * did. The loader's next read of se_company_info FINAL shows the outcome.
 *
 * The whole batch is validated before ClickHouse is touched at all, so a bad
 * row cannot leave the good half of a decision behind. All rows must name the
 * same company: the published check below is per company, and a mixed batch
 * would leave part of it unchecked.
 *
 * A resolve failure AFTER the insert is raised as SeCompanyFieldResolveError,
 * never swallowed and never confused with a refusal: the rows are in the
 * store, and se_company_info_field_value_sensor re-resolves the company.
 */
export async function appendSeCompanyInfoFieldValues(
  inputs: SeInfoFieldValueInput[],
  opts: { registry?: FieldRegistry; now?: Date } = {},
): Promise<{ valueIds: string[]; resolved: string[]; skipped: SkippedField[] }> {
  // The registry export says which fields and sources exist (spec 11); the
  // action loads it once per post and hands it in, so a post validates and
  // resolves against ONE registry version even if an export lands mid-way.
  const registry = opts.registry ?? (await loadFieldRegistry());
  const vocabulary = fieldVocabulary(registry);
  const drafts = inputs.map((input) =>
    validateSeInfoFieldValue(input, vocabulary),
  );
  const [first] = drafts;
  if (!first) {
    throw new SeInfoFieldValueValidationError("Nothing to write.");
  }
  if (drafts.some((draft) => draft.company_id !== first.company_id)) {
    throw new SeInfoFieldValueValidationError(
      "Every value in one write must belong to the same company.",
    );
  }
  // One field, one row: the whole batch shares a created_at (below), so two
  // rows for the same field would tie there and the live one would fall to the
  // uuid-text tie-break -- a coin flip between two things the reviewer meant
  // in some order. Refuse instead of writing an arbitrary winner.
  if (new Set(drafts.map((draft) => draft.field)).size !== drafts.length) {
    throw new SeInfoFieldValueValidationError(
      "Each field may appear only once per decision.",
    );
  }
  const [published] = await chQuery<Record<string, unknown>>(
    PUBLISHED_CHECK_SQL,
    { companyId: first.company_id },
  );
  if (!published) {
    throw new SeInfoFieldValueValidationError("This company is not published.");
  }
  // One instant for the whole batch AND for the resolve: the rows are one
  // decision, and giving them the same created_at keeps the per-field
  // tie-break (created_at, then the uuid's text) deciding between rows of
  // DIFFERENT decisions only; stamping resolved_at with the same instant makes
  // the two tables read alike for this decision.
  const now = opts.now ?? new Date();
  const createdAt = formatClickHouseDateTime64(now);
  const rows = drafts.map((draft) => ({
    value_id: randomUUID(),
    ...draft,
    decided_by: DECIDED_BY,
    created_at: createdAt,
  }));
  await chInsertSeCompanyInfoFieldValues(rows);
  const valueIds = rows.map((row) => row.value_id);

  let outcome: ResolveCompanyFieldsResult;
  try {
    outcome = await resolveCompanyFields(
      first.company_id,
      drafts.map((draft) => draft.field),
      { registry, now },
    );
  } catch (error) {
    throw new SeCompanyFieldResolveError(valueIds, error);
  }
  return { valueIds, resolved: outcome.resolved, skipped: outcome.skipped };
}
```

- [ ] **Step 5: Return the outcome from the action**

In `app/routes/admin-se-company-info.tsx`, add `import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";` and replace the `try`/`catch`:

```ts
  try {
    const { valueIds, resolved, skipped } =
      await appendSeCompanyInfoFieldValues(built.inputs, { registry });
    return { ok: true as const, valueIds, resolved, skipped };
  } catch (error) {
    // The store's refusals are the reviewer's to read (a company that is not
    // published, an empty value, a field decided twice in one post); anything
    // else is a real failure and must not be dressed up as a form error.
    if (error instanceof SeInfoFieldValueValidationError) {
      return { ok: false as const, error: error.message };
    }
    // The decision IS in the store; only the synchronous resolve failed. The
    // ids mark it as saved so the page does not call it "Not saved".
    if (error instanceof SeCompanyFieldResolveError) {
      return {
        ok: false as const,
        error: error.message,
        valueIds: error.valueIds,
      };
    }
    throw error;
  }
```

- [ ] **Step 6: Result type and banner in the workspace**

In `app/components/admin/se-company-info-review-workspace.tsx`:

1. Add `import type { SkippedField } from "~/lib/se-company-field-resolve.server";` next to the other `import type` from `~/lib/se-company-info.server` (type-only, erased from the client bundle exactly like that one).

2. Replace the result type (lines 67–70):

```ts
export type SeCompanyInfoReviewResult =
  | {
      ok: true;
      valueIds: string[];
      resolved: string[];
      skipped: SkippedField[];
    }
  /** `valueIds` present = the rows were saved and only the synchronous
   * resolve failed (SeCompanyFieldResolveError); absent = a refusal. */
  | { ok: false; error: string; valueIds?: string[] }
  | null;
```

3. Add a helper above `SeCompanyInfoReviewWorkspace`:

```ts
/**
 * "Saved and resolved." when every decided field was resolved on the spot;
 * "Saved. <field> applies on the next run." for the python_only fields the
 * bulk resolver alone handles (spec 2026-09-02, section 9). The row count
 * stays in the description so a two-language decision reads as two rows.
 */
function savedBanner(result: {
  valueIds: string[];
  skipped: SkippedField[];
}): { title: string; detail: string } {
  const detail =
    result.valueIds.length === 1
      ? "1 value row saved"
      : `${result.valueIds.length} value rows saved`;
  if (result.skipped.length === 0) {
    return { title: "Saved and resolved.", detail };
  }
  const names = result.skipped.map((entry) => entry.field).join(", ");
  const verb = result.skipped.length === 1 ? "applies" : "apply";
  return { title: `Saved. ${names} ${verb} on the next run.`, detail };
}
```

4. In `SeCompanyInfoReviewWorkspace`, directly after `const busy = useNavigation().state !== "idle";`, add:

```ts
  const banner = result?.ok ? savedBanner(result) : null;
```

and replace the two `<Alert>` blocks (lines 795–813):

```tsx
      {banner ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>{banner.title}</AlertTitle>
          <AlertDescription>{banner.detail}</AlertDescription>
        </Alert>
      ) : null}
      {result && !result.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>
            {result.valueIds ? "Saved, not resolved" : "Not saved"}
          </AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}
```

- [ ] **Step 7: Run the tests and the typecheck**

Run: `npx vitest run tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx tests/se-company-field-resolve.server.test.ts && npm run typecheck`
Expected: PASS; typecheck clean (the route's return union is assignable to `SeCompanyInfoReviewResult`).

- [ ] **Step 8: Run the whole unit suite once**

Run: `npx vitest run`
Expected: PASS (the `*.live.test.ts` files are excluded without `VITEST_LIVE`). Note that `tests/clickhouse.server.test.ts` hits the real ClickHouse from `.env` and needs it reachable.

- [ ] **Step 9: Commit**

```bash
git add app/lib/se-company-info.server.ts app/routes/admin-se-company-info.tsx app/components/admin/se-company-info-review-workspace.tsx tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx
git commit -m "feat(backoffice): resolve the company right after a decision

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 6: Live single-company resolve against ClickHouse

**Files:**
- Create: `tests/se-company-field-resolve.live.test.ts`
- Modify: `package.json` (`scripts.test:live`, new `scripts.test:live:projection`)

**Interfaces:**
- Consumes: `chCommand`, `chQuery`, `chInsertSeCompanyInfoFieldValues` (Task 1 / existing); `loadFieldRegistry`, `FieldRegistry` (Task 2); `resolveCompanyFields` with `{ project: false }`, `formatClickHouseDateTime64` (Task 3); `appendSeCompanyInfoFieldValues` (Task 5); the deployed tables and grants from parts 1–3 (the registry export materialized at least once).
- Produces: nothing for later tasks. Runs only under `VITEST_LIVE=1` (the vitest config excludes `tests/**/*.live.test.ts` otherwise), against the ClickHouse named in `.env`.

**Ruling (coordinator, 2026-09-02):** the live test must never publish a synthetic company into `corpscout.se_company_info` on the production ClickHouse (host `companycollect`): that row flows into `se_companies_serving` and the public `company_*_current` tables. The test therefore has two branches:

1. **Production branch (always runs).** Inserts candidates and decision rows for the synthetic id `5599999999`, resolves with the projection DISABLED (`resolveCompanyFields(..., { project: false })`), and asserts only `corpscout.se_company_field` rows — the long table, which nothing serves — before and after a reviewer decision; it also asserts that no wide row was written this run. It cannot go through `appendSeCompanyInfoFieldValues`: that path checks the company is published (`PUBLISHED_CHECK_SQL`) and always projects. The decision rows are written through the same `chInsertSeCompanyInfoFieldValues` helper the store uses, with the same row shape.
2. **Scratch branch (`VITEST_LIVE_PROJECTION=1` only).** The full path including the wide projection and `appendSeCompanyInfoFieldValues`. It refuses loudly — a failing assertion on the hostname parsed from `CLICKHOUSE_URL`, not a skip — when that hostname is `companycollect`. Run it only against a scratch ClickHouse that has parts 1–3 applied and the registry materialized. An alias or IP for the production host would defeat the hostname check, so the flag is the real guard: never set `VITEST_LIVE_PROJECTION` in a production `.env`.

Preconditions the executor must confirm before running (spec 12 cutover steps 1–3 for the target instance): migrations for `se_company_field_registry`, `se_company_field_candidate`, `se_company_field`, the widened CHECKs on `se_company_info_field_value` and the writer grants are applied; `se_company_field_registry_clickhouse` has been materialized. Without them the registry read fails with the "materialize" message.

Execution ruling (2026-09-03): at execution time nothing from parts 1–3 was on production and no scratch ClickHouse existed, so Steps 4 and 5 (the live runs) are DEFERRED to the cutover plan, right after its migrations and the registry export; this task ships the test file, the two scripts, the exclusion check (Step 3) and the typecheck. Part 3's clickhouse-local harness already executes the same statement text under ClickHouse's own `SET param_*` binding, which is what clickhouse-js `query_params` sends.

What the test leaves behind (append-only tables, nothing cleaned; the writer role holds INSERT only): per run of the production branch, 3 rows in `se_company_field_candidate` (`extractor_version = 'backoffice-live-test'`, `source_run_id = 'backoffice-live:<uuid>'`), 2 rows in `se_company_info_field_value` (a release and a reviewer value, `note = 'backoffice live test'`) and new versions of the `legal_name` / `description` rows in `se_company_field`; **nothing in `se_company_info`**. The scratch branch adds the same plus one wide row (`legal_name = 'BACKOFFICE LIVE TEST AB'`) on the scratch host. The id is outside every real registry (the guard refuses to run if an SCB artifact exists for it) and every row is recognisable by the legal name, the note and the `backoffice-live` run-id prefix.

Why each branch writes a release row first: the tables are append-only, so a reviewer value from an earlier run would still be the live decision and the "winner before the decision" assertion would fail on the second run. A release (`value = NULL`, spec 6 / 7.4: "use the winner") written first makes every run start from the winner, and exercises the release path as a bonus.

- [ ] **Step 1: Write the live test**

Create `tests/se-company-field-resolve.live.test.ts`:

```ts
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  chCommand,
  chInsertSeCompanyInfoFieldValues,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  loadFieldRegistry,
  type FieldRegistry,
} from "~/lib/se-company-field-registry.server";
import {
  formatClickHouseDateTime64,
  resolveCompanyFields,
} from "~/lib/se-company-field-resolve.server";
import { appendSeCompanyInfoFieldValues } from "~/lib/se-company-info.server";

/**
 * Integration test against the real ClickHouse (VITEST_LIVE=1). The unit tests
 * pin what this app binds; only a live run proves the registry's generated
 * statements accept those bindings (Array(String), DateTime64(3) text).
 *
 * RULING 2026-09-02: never publish a synthetic company into se_company_info on
 * production -- that row flows into se_companies_serving and the public
 * company_*_current tables. So:
 *
 *   - the first describe ALWAYS runs and resolves with `project: false`,
 *     asserting the long table (corpscout.se_company_field, served by nothing)
 *     and that no wide row was written;
 *   - the second describe runs only with VITEST_LIVE_PROJECTION=1 and FAILS
 *     (does not skip) when CLICKHOUSE_URL's hostname is the production host.
 *     It is for a scratch ClickHouse with parts 1-3 applied.
 *
 * SYNTHETIC COMPANY, APPEND-ONLY TABLES, NOTHING CLEANED. Every production
 * run appends, under company_id 5599999999 (CHECK-valid, no real company --
 * the guard refuses to run if an SCB artifact exists for it):
 *   se_company_field_candidate   3 rows  extractor_version 'backoffice-live-test'
 *   se_company_info_field_value  2 rows  a release, then a reviewer value
 *   se_company_field             new versions of legal_name and description
 * All recognisable by legal name 'BACKOFFICE LIVE TEST AB', the note and the
 * 'backoffice-live:' run-id prefix. The writer role holds INSERT only.
 */

const TEST_COMPANY = "5599999999";
const LEGAL_NAME = "BACKOFFICE LIVE TEST AB";
const SCB_DESCRIPTION = "Backoffice live-test company; not a real business.";
const WIKIDATA_DESCRIPTION =
  "A synthetic company the backoffice live test resolves.";
const REVIEWER_DESCRIPTION =
  "The reviewer's own wording, written by the backoffice live test.";
const NOTE = "backoffice live test";

const PRODUCTION_HOST = "companycollect";
const PROJECTION_ENABLED = process.env.VITEST_LIVE_PROJECTION === "1";
/** Parsed from the same variable, with the same default, as the clients in
 * clickhouse.server.ts. */
const CLICKHOUSE_HOST = new URL(
  process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
).hostname;

/** One candidate row, inserted through the same command runner the resolve
 * uses. evidence_hash is MATERIALIZED, so it is not listed. */
const CANDIDATE_INSERT_SQL = `INSERT INTO corpscout.se_company_field_candidate
  (company_id, field, source, source_record_uid, value, value_json,
   observed_at, extracted_at, extractor_version, source_run_id)
SELECT
  {company_id:String}, {field:String}, {source:String}, {source_record_uid:String},
  {value:String}, {value_json:String},
  {observed_at:DateTime64(3)}, {extracted_at:DateTime64(3)},
  'backoffice-live-test', {source_run_id:String}`;

const RESOLVED_ROWS_SQL = `SELECT
  toString(field) AS field,
  value,
  toString(source) AS source,
  source_record_uid,
  toString(decision_id) AS decision_id,
  registry_version,
  source_run_id,
  toString(resolved_at) AS resolved_at
FROM corpscout.se_company_field FINAL
WHERE company_id = {companyId:String}
ORDER BY field`;

const WIDE_ROW_SQL = `SELECT
  legal_name,
  description,
  arrayMap(id -> toString(id), correction_ids) AS correction_ids,
  toString(resolved_at) AS resolved_at
FROM corpscout.se_company_info FINAL
WHERE company_id = {companyId:String}
LIMIT 1`;

// count() is UInt64, which JSONEachRow quotes as a string by default; the
// UInt32 cast makes both guards compare a number.
const COLLISION_SQL = `SELECT toUInt32(count()) AS n
FROM corpscout.se_company_info_scb
WHERE company_id = {companyId:String}`;

/** Was a wide row written for the test id at or after `since`? The
 * blast-radius assertion of the production branch. */
const WIDE_ROWS_SINCE_SQL = `SELECT toUInt32(count()) AS n
FROM corpscout.se_company_info FINAL
WHERE company_id = {companyId:String}
  AND resolved_at >= {since:DateTime64(3)}`;

interface ResolvedRow {
  field: string;
  value: string;
  source: string;
  source_record_uid: string;
  decision_id: string | null;
  registry_version: string;
  source_run_id: string;
  resolved_at: string;
}

interface WideRow {
  legal_name: string;
  description: string | null;
  correction_ids: string[];
  resolved_at: string;
}

/** The id must name no real company. Every real SE company has an SCB
 * artifact; a hit means the id collided and the test must not write. */
async function assertNoCollision(): Promise<void> {
  const [collision] = await chQuery<{ n: number }>(COLLISION_SQL, {
    companyId: TEST_COMPANY,
  });
  expect(collision?.n).toBe(0);
}

async function loadRegistryForTest(): Promise<FieldRegistry> {
  const registry = await loadFieldRegistry();
  expect(registry.fields.map((entry) => entry.field)).toEqual(
    expect.arrayContaining(["legal_name", "description"]),
  );
  expect(registry.projectionSql).toContain("corpscout.se_company_info");
  return registry;
}

async function insertCandidate(
  runId: string,
  field: string,
  source: string,
  value: string,
  observedAt: string,
): Promise<void> {
  await chCommand(CANDIDATE_INSERT_SQL, {
    company_id: TEST_COMPANY,
    field,
    source,
    source_record_uid: `${source}:${TEST_COMPANY}:${runId}`,
    value,
    value_json: JSON.stringify({
      compare_key: value.toLowerCase(),
      language: "en",
    }),
    observed_at: observedAt,
    extracted_at: formatClickHouseDateTime64(new Date()),
    source_run_id: runId,
  });
}

/** legal_name from scb (spec 8.3: a company without one is not published),
 * and two descriptions where wikidata outranks scb (spec 4.2: description =
 * llm, esef, wikidata, scb). */
async function seedCandidates(runId: string): Promise<void> {
  await insertCandidate(runId, "legal_name", "scb", LEGAL_NAME, "2026-08-01 00:00:00.000");
  await insertCandidate(runId, "description", "scb", SCB_DESCRIPTION, "2026-08-01 00:00:00.000");
  await insertCandidate(runId, "description", "wikidata", WIKIDATA_DESCRIPTION, "2026-08-15 00:00:00.000");
}

/** One decision row for `description`, exactly as appendSeCompanyInfoFieldValues
 * writes it (same helper, same columns). null = release. Returns value_id. */
async function insertDecision(value: string | null): Promise<string> {
  const valueId = randomUUID();
  await chInsertSeCompanyInfoFieldValues([
    {
      value_id: valueId,
      company_id: TEST_COMPANY,
      field: "description",
      value,
      source: "reviewer",
      source_ref: "",
      source_at: null,
      decided_by: "backoffice",
      note: NOTE,
      created_at: formatClickHouseDateTime64(new Date()),
    },
  ]);
  return valueId;
}

const readResolved = () =>
  chQuery<ResolvedRow>(RESOLVED_ROWS_SQL, { companyId: TEST_COMPANY });

describe("single-company resolve against ClickHouse (long table only)", () => {
  it("resolves the winner, then the reviewer's decision, into se_company_field without touching the wide row", async () => {
    await assertNoCollision();
    const registry = await loadRegistryForTest();
    const runId = `backoffice-live:${randomUUID()}`;
    await seedCandidates(runId);

    // A release first, so the live decision is "use the winner" whatever an
    // earlier run left behind.
    await insertDecision(null);

    const firstNow = new Date();
    const first = await resolveCompanyFields(
      TEST_COMPANY,
      ["legal_name", "description"],
      { registry, now: firstNow, sourceRunId: runId, project: false },
    );
    expect(first).toEqual({ resolved: ["legal_name", "description"], skipped: [] });

    const resolved = await readResolved();
    expect(resolved.map((row) => [row.field, row.source, row.value])).toEqual([
      ["description", "wikidata", WIKIDATA_DESCRIPTION],
      ["legal_name", "scb", LEGAL_NAME],
    ]);
    for (const row of resolved) {
      // A released decision means "use the winner": a candidate row, no
      // decision stamped on it (spec 7.4).
      expect(row.decision_id).toBeNull();
      expect(row.registry_version).toBe(registry.version);
      expect(row.source_run_id).toBe(runId);
      expect(row.resolved_at).toBe(formatClickHouseDateTime64(firstNow));
    }

    // The reviewer's own wording beats every candidate by construction
    // (spec 7.4). A distinct run id shows which rows this resolve touched.
    const valueId = await insertDecision(REVIEWER_DESCRIPTION);
    const decisionRunId = `${runId}:decision`;
    const second = await resolveCompanyFields(TEST_COMPANY, ["description"], {
      registry,
      now: new Date(),
      sourceRunId: decisionRunId,
      project: false,
    });
    expect(second).toEqual({ resolved: ["description"], skipped: [] });

    const afterDecision = await readResolved();
    expect(afterDecision.find((row) => row.field === "description")).toMatchObject({
      value: REVIEWER_DESCRIPTION,
      source: "reviewer",
      source_record_uid: "",
      decision_id: valueId,
      source_run_id: decisionRunId,
    });
    // legal_name was not decided and not re-resolved: its row is the first run's.
    expect(afterDecision.find((row) => row.field === "legal_name")).toMatchObject({
      source: "scb",
      source_run_id: runId,
    });

    // The ruling, asserted: nothing this run wrote reached the wide table.
    const [wide] = await chQuery<{ n: number }>(WIDE_ROWS_SINCE_SQL, {
      companyId: TEST_COMPANY,
      since: formatClickHouseDateTime64(firstNow),
    });
    expect(wide?.n).toBe(0);
  }, 120000);
});

describe.skipIf(!PROJECTION_ENABLED)(
  "wide projection on a scratch ClickHouse (VITEST_LIVE_PROJECTION=1)",
  () => {
    it("re-pivots the wide row after the resolve and again after a decision through the store", async () => {
      // Loud, not a skip: the flag was set while .env points at production.
      expect(
        CLICKHOUSE_HOST,
        `VITEST_LIVE_PROJECTION=1 against host "${CLICKHOUSE_HOST}": the projection publishes ${TEST_COMPANY} into se_company_info, which feeds se_companies_serving and the public company_*_current tables. Run this branch against a scratch ClickHouse only.`,
      ).not.toBe(PRODUCTION_HOST);

      await assertNoCollision();
      const registry = await loadRegistryForTest();
      const runId = `backoffice-live:${randomUUID()}`;
      await seedCandidates(runId);
      await insertDecision(null);

      const firstNow = new Date();
      const first = await resolveCompanyFields(
        TEST_COMPANY,
        ["legal_name", "description"],
        { registry, now: firstNow, sourceRunId: runId },
      );
      expect(first).toEqual({ resolved: ["legal_name", "description"], skipped: [] });

      const [wide] = await chQuery<WideRow>(WIDE_ROW_SQL, { companyId: TEST_COMPANY });
      expect(wide).toMatchObject({
        legal_name: LEGAL_NAME,
        description: WIKIDATA_DESCRIPTION,
      });

      // The backoffice path end to end: the published check passes now that a
      // wide row exists; the decision is inserted, resolved and projected
      // (spec 9), and the wide row shows it when this returns.
      const decision = await appendSeCompanyInfoFieldValues(
        [
          {
            companyId: TEST_COMPANY,
            field: "description",
            value: REVIEWER_DESCRIPTION,
            source: "reviewer",
            note: NOTE,
          },
        ],
        { registry },
      );
      expect(decision.valueIds).toHaveLength(1);
      expect(decision.resolved).toEqual(["description"]);
      expect(decision.skipped).toEqual([]);

      const afterDecision = await readResolved();
      expect(afterDecision.find((row) => row.field === "description")).toMatchObject({
        value: REVIEWER_DESCRIPTION,
        source: "reviewer",
        source_record_uid: "",
        decision_id: decision.valueIds[0],
      });

      const [wideAfter] = await chQuery<WideRow>(WIDE_ROW_SQL, { companyId: TEST_COMPANY });
      expect(wideAfter.description).toBe(REVIEWER_DESCRIPTION);
      expect(wideAfter.legal_name).toBe(LEGAL_NAME);
      // Spec 8.3: correction_ids = decision ids applied across all fields.
      expect(wideAfter.correction_ids).toContain(decision.valueIds[0]);
      expect(wideAfter.resolved_at > wide.resolved_at).toBe(true);
    }, 120000);
  },
);
```

- [ ] **Step 2: Add both scripts**

In `package.json`, replace the `test:live` line with these two lines (the second carries the projection flag; it is the only place that flag should ever be written down):

```json
    "test:live": "VITEST_LIVE=1 vitest run tests/se-company-address-corrections.live.test.ts tests/se-company-field-resolve.live.test.ts",
    "test:live:projection": "VITEST_LIVE=1 VITEST_LIVE_PROJECTION=1 vitest run tests/se-company-field-resolve.live.test.ts"
```

- [ ] **Step 3: Confirm the unit run still excludes it**

Run: `npx vitest run tests/se-company-field-resolve.live.test.ts`
Expected: "No test files found" (excluded by `vitest.config.ts` without `VITEST_LIVE`).

- [ ] **Step 4: Run the production branch live**

Run: `VITEST_LIVE=1 npx vitest run tests/se-company-field-resolve.live.test.ts`
Expected: 1 passed, 1 skipped (the projection describe). If it fails on the collision guard, the id already exists: pick another `55999999NN` id and update `TEST_COMPANY` and this task's notes. If it fails on `decision_id`, `source` or `value` in `se_company_field`, the generated statement from part 2 does not implement spec 7.4 for that column: report it against part 2; do not weaken the assertion.

Then confirm from outside the test that the wide table has no row for the id (the assertion inside the test is scoped to this run; this one is absolute):

```bash
npx tsx -e 'import("./app/lib/clickhouse.server.ts").then(async (m) => console.log(await m.chQuery("SELECT toUInt32(count()) AS n FROM corpscout.se_company_info WHERE company_id = {id:String}", { id: "5599999999" })))'
```

Expected: `[ { n: 0 } ]`.

- [ ] **Step 5: (Scratch ClickHouse only) run the projection branch**

Only with `CLICKHOUSE_URL` in `.env` pointing at a scratch ClickHouse (not `companycollect`) that has parts 1–3 applied and the registry materialized:

Run: `npm run test:live:projection`
Expected: 2 passed. If the wide row lacks the reviewer's `description` or the decision id in `correction_ids`, the projection from part 3 does not implement spec 8.3 for that column: report it against part 3; do not weaken the assertion. Confirm the guard as well, once, by running the same command with `CLICKHOUSE_URL=http://companycollect:8123` exported in the shell — expected: the second test FAILS on the hostname assertion before writing anything (`assertNoCollision` runs after it).

- [ ] **Step 6: Typecheck and commit**

Run: `npm run typecheck`
Expected: clean.

```bash
git add tests/se-company-field-resolve.live.test.ts package.json
git commit -m "test(backoffice): live single-company resolve against ClickHouse

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

## Self-review

**Spec coverage.** 4.3 as consumer (`argMax(..., version)` read, `field = '*'` projection row, per-version cache) → Task 2. 6 (decisions table unchanged; `reviewer` as a decision source; live = newest row) → unchanged `FIELD_VALUES_SQL`, vocabulary adds `reviewer` in Task 4. 9 step 1 (resolve_sql per decided field, cached per registry version) → Tasks 2, 3; step 2 (`company_ids = [companyId]`) → Task 3; step 3 (one projection statement from the `*` row) → Task 3; step 4 (loader shows the resolved value) → Task 5 (React Router revalidates the loader after the action; `INFO_SQL` reads `FINAL`); python_only skipped with "applies on next run" → Tasks 3, 5; writer role INSERT on `se_company_field`/`se_company_info` → assumed from part 1, exercised in Task 6. 11 last bullet (validator reads the registry export) → Task 4. 12 backoffice tests (single-company resolve under `VITEST_LIVE`; validator reads the export) → Tasks 6, 4 — per the 2026-09-02 ruling the always-on branch resolves with `project: false` and asserts the long table only (plus "no wide row this run"); the wide-row assertion runs only under `VITEST_LIVE_PROJECTION=1` on a host other than `companycollect`, failing loudly otherwise. Not in scope, per the prompt: the page redesign (phase B), the sensor, migrations.

**Placeholder scan.** Every code step carries the code; the only "change X to Y" edits name the exact old and new text with line numbers. The sed commands are literal.

**Type consistency.** `FieldRegistry`/`FieldRegistryEntry` (Task 2) are what Tasks 3–6 and the fixture import; `SkippedField`/`ResolveCompanyFieldsResult`/`SeCompanyFieldResolveError`/`formatClickHouseDateTime64` (Task 3) are what Task 5 imports; `fieldVocabulary`/`SeInfoFieldVocabulary`/`REVIEWER_SOURCE` (Task 4) are what Tasks 4–5 use; `appendSeCompanyInfoFieldValues(inputs, { registry })` in Task 4 grows to `{ registry, now }` and the three-key return in Task 5, and the Task 4 route test's `toHaveBeenCalledWith(..., { registry: REGISTRY_FIXTURE })` still holds because the action never passes `now`. `chCommand` (Task 1) signature is used identically in Tasks 3 and 6; `resolveCompanyFields`'s `project?: boolean` (Task 3 interface, implementation and test) is what Task 6's production branch passes as `project: false`, and Task 5's store never passes it, so the action path always projects.
