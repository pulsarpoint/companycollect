# ESEF Disclosures Join Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-attach the structured disclosure extraction to the ESEF facts reader by replacing the join against the dropped table `corpscout.esef_fact_disclosures` with the live `corpscout.esef_disclosures`.

**Architecture:** The report loader (`getEsefFinancialReport`) probes `system.tables` for optional tables and only emits the disclosure join when the table exists. Since migration `000316` dropped `esef_fact_disclosures` (replaced by `esef_disclosures`), that branch is dead and every narrative fact silently falls back to client-side raw-XHTML parsing, losing the persisted structured blocks and parser evidence. We point the probe and the join at the new table, adapt the three schema differences (join keys, `text_sha256` instead of `raw_value_sha256`, `disclosure_kind` discriminator), and rename the evidence field through the type chain.

**Tech Stack:** TypeScript, React Router v7, @clickhouse/client, vitest 4.

**Spec:** This section + "Background & evidence" below (findings from the 2026-08-31 investigation of company 5020077862 / Svenska Handelsbanken; no separate spec doc).

## Global Constraints

- Run tests from the backoffice dir: `cd corpscout/services/backoffice && npx vitest run <file>`.
- `npm run typecheck` (= `react-router typegen && tsc`) must pass before every commit.
- `tests/esef-financial-reports.server.test.ts` hits the real ClickHouse (it is NOT named `*.live.test.ts` but requires a live DB). Do not break it; run it only when `ssh companycollect` ClickHouse is reachable via the configured env.
- Conventional Commits format.
- Do not commit the pre-existing unrelated working-tree changes (000363 migrations, ratsit log); stage only files this plan touches.

## Background & evidence

- `app/lib/esef-financial-reports.server.ts:92,115,367` reference `esef_fact_disclosures`. Migration `corpscout/clickhouse/migrations/000316_corpscout_esef_disclosures.up.sql:137` does `DROP TABLE corpscout.esef_fact_disclosures`. Verified live (2026-08-31): only `esef_disclosures` and `esef_document_concept_labels` exist.
- New table schema deltas vs old: join keys are `(source_document_id, source_fact_id)`; there is **no** `raw_value_sha256` — the new `text_sha256` hashes the extracted plain text, verified NOT equal to `SHA256(raw_value)` (6/144 match on the Handelsbanken 2023 filing), so the old third join condition must be dropped, not translated. Rows carry `disclosure_kind` (`'tagged_fact'` for fact-anchored rows) and the table is a plain `MergeTree` partitioned by `processed_week` (duplicates possible → keep `LEFT ANY JOIN`).
- `parsePersistedEsefDisclosure(blocksJson, plainText)` in `app/lib/esef-disclosures.ts:351` already matches the new `blocks_json` shape (the Python writer `dagster_v3/.../esef_filings/disclosure_parser.py` is a line-for-line mirror). No parser change needed.
- Fallback behavior to preserve: `NarrativeFactValue` (`app/components/detail/esef-facts-accordion.tsx:157`) does `fact.structuredDisclosure ?? parseEsefDisclosure(fact.rawValue)` — a missing disclosure row must keep yielding `structuredDisclosure: null`, never a crash.

## File Structure

- Modify: `app/lib/esef-financial-reports.server.ts` (probe, join, columns, row type, mapping)
- Modify: `app/lib/xbrl-facts.ts:32-37` (`disclosureEvidence.rawValueSha256` → `textSha256`)
- Modify: `app/components/detail/esef-facts-accordion.tsx:365-384` (evidence label "Raw value SHA-256" → "Text SHA-256", property rename)
- Create: `app/lib/esef-financial-reports.sql.test.ts` (mocked-ClickHouse unit test; the existing `tests/esef-financial-reports.server.test.ts` stays live-hitting)

---

### Task 1: Failing unit test pinning the new SQL and row mapping

**Files:**
- Create: `app/lib/esef-financial-reports.sql.test.ts`

**Interfaces:**
- Consumes: `getEsefFinancialReport(country, companyId, documentId)` from `~/lib/esef-financial-reports.server` (loader makes 3 sequential `chQuery` calls: summary, optional-tables probe, facts).
- Produces: the SQL assertions Task 2 must satisfy.

- [ ] **Step 1: Write the failing test** (mock idiom B from `app/lib/queries-financials.test.ts:1-16`)

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const chQuery = vi.fn();
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: (...args: unknown[]) => chQuery(...args),
}));
const { getEsefFinancialReport } = await import(
  "~/lib/esef-financial-reports.server"
);

const SUMMARY_ROW = {
  lei: "NHBDILHZTYCNBV5UYZ31",
  fxo_id: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
  entity_name: "Svenska Handelsbanken AB",
  fiscal_year: 2023,
  period_end: "2023-12-31",
  currency: "SEK",
  mapped_fact_count: 9,
  source_fact_count: 292,
  filing_version: 0,
  viewer_url: "",
  source_url: "",
  package_url: "",
  error_count: 0,
  warning_count: 0,
  date_added: "2024-04-24",
};

// Align field names with EsefFactRow (esef-financial-reports.server.ts:30-52)
const FACT_ROW = {
  fact_id: "fact-489",
  concept_qname: "ifrs-full:DisclosureOfFinanceCostExplanatory",
  concept_local_name: "DisclosureOfFinanceCostExplanatory",
  value_kind: "text",
  raw_value: "<p>Note 5</p>",
  amount_original: null,
  amount_usd: null,
  fx_rate_date: "",
  fx_source: "",
  decimals: null,
  period_start: "2023-01-01",
  period_instant: "",
  period_duration_end: "2023-12-31",
  unit: "",
  currency: "",
  dimensions: "",
  language: "sv",
  concept_labels_json: "[]",
  concept_documentation_json: "[]",
  disclosure_blocks_json: JSON.stringify([
    { type: "heading", text: "Note 5" },
    { type: "paragraph", text: "Finance costs consist of interest." },
  ]),
  disclosure_plain_text: "Note 5 Finance costs consist of interest.",
  disclosure_source_record_uid: "abc123",
  disclosure_text_sha256: "89bfb744be7ea2cf",
  disclosure_parser_name: "lxml_html_disclosure",
  disclosure_parser_version: "1",
};

beforeEach(() => {
  chQuery.mockReset();
});

describe("getEsefFinancialReport disclosure join", () => {
  it("probes for esef_disclosures and joins it on document + fact id", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([
        { name: "esef_disclosures" },
        { name: "esef_document_concept_labels" },
      ])
      .mockResolvedValueOnce([FACT_ROW]);

    const report = await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const probeSql = String(chQuery.mock.calls[1][0]);
    expect(probeSql).toContain("'esef_disclosures'");
    expect(probeSql).not.toContain("'esef_fact_disclosures'");

    const factsSql = String(chQuery.mock.calls[2][0]);
    expect(factsSql).toContain("FROM corpscout.esef_disclosures");
    expect(factsSql).not.toContain("esef_fact_disclosures");
    expect(factsSql).toContain("disclosure_kind = 'tagged_fact'");
    expect(factsSql).toContain(
      "disclosures.source_document_id = facts.fxo_id",
    );
    expect(factsSql).toContain("disclosures.source_fact_id = facts.fact_id");
    expect(factsSql).not.toContain("raw_value_sha256");
    expect(factsSql).toContain("text_sha256");

    const fact = report!.facts[0];
    expect(fact.structuredDisclosure).toEqual({
      blocks: [
        { type: "heading", text: "Note 5" },
        { type: "paragraph", text: "Finance costs consist of interest." },
      ],
      plainText: "Note 5 Finance costs consist of interest.",
    });
    expect(fact.disclosureEvidence).toEqual({
      sourceRecordUid: "abc123",
      textSha256: "89bfb744be7ea2cf",
      parserName: "lxml_html_disclosure",
      parserVersion: "1",
    });
  });

  it("degrades to empty disclosure columns when the table is absent", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([{ name: "esef_document_concept_labels" }])
      .mockResolvedValueOnce([
        {
          ...FACT_ROW,
          disclosure_blocks_json: "",
          disclosure_plain_text: "",
          disclosure_source_record_uid: "",
          disclosure_text_sha256: "",
          disclosure_parser_name: "",
          disclosure_parser_version: "",
        },
      ]);

    const report = await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const factsSql = String(chQuery.mock.calls[2][0]);
    expect(factsSql).not.toContain("esef_disclosures");
    expect(report!.facts[0].structuredDisclosure).toBeNull();
    expect(report!.facts[0].disclosureEvidence).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run app/lib/esef-financial-reports.sql.test.ts`
Expected: FAIL — probe SQL still contains `'esef_fact_disclosures'`, facts SQL lacks `esef_disclosures`, and `disclosureEvidence` still exposes `rawValueSha256`.

- [ ] **Step 3: Commit the failing test** (skip if your workflow commits test+impl together; otherwise `git add app/lib/esef-financial-reports.sql.test.ts && git commit -m "test(esef): pin disclosure join to esef_disclosures"`)

### Task 2: Rewrite the probe, join, and columns

**Files:**
- Modify: `app/lib/esef-financial-reports.server.ts`

**Interfaces:**
- Consumes: nothing new.
- Produces: `EsefFactRow.disclosure_text_sha256: string` (replacing `disclosure_raw_value_sha256`); facts SQL that Task 1 asserts.

- [ ] **Step 1: Replace OPTIONAL_TABLES_QUERY (lines 88-92)**

```ts
const OPTIONAL_TABLES_QUERY = `
SELECT name
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('esef_disclosures', 'esef_document_concept_labels')`;
```

- [ ] **Step 2: Replace the disclosure column block (lines 98-112)** — rename the sha alias:

```ts
  const disclosureColumns = withDisclosures
    ? `
  coalesce(disclosures.blocks_json, '') AS disclosure_blocks_json,
  coalesce(disclosures.plain_text, '') AS disclosure_plain_text,
  coalesce(toString(disclosures.source_record_uid), '') AS disclosure_source_record_uid,
  coalesce(toString(disclosures.text_sha256), '') AS disclosure_text_sha256,
  coalesce(disclosures.parser_name, '') AS disclosure_parser_name,
  coalesce(toString(disclosures.parser_version), '') AS disclosure_parser_version`
    : `
  '' AS disclosure_blocks_json,
  '' AS disclosure_plain_text,
  '' AS disclosure_source_record_uid,
  '' AS disclosure_text_sha256,
  '' AS disclosure_parser_name,
  '' AS disclosure_parser_version`;
```

- [ ] **Step 3: Replace the join (lines 113-119)** — subquery join so the `disclosure_kind` and document filters push down (the table is partitioned by `processed_week` and holds every filing; an unfiltered join would scan all partitions):

```ts
  const disclosureJoin = withDisclosures
    ? `
LEFT ANY JOIN (
  SELECT
    source_document_id,
    source_fact_id,
    blocks_json,
    plain_text,
    source_record_uid,
    text_sha256,
    parser_name,
    parser_version
  FROM corpscout.esef_disclosures
  WHERE disclosure_kind = 'tagged_fact'
    AND source_document_id = {documentId:String}
) AS disclosures
  ON disclosures.source_document_id = facts.fxo_id
 AND disclosures.source_fact_id = facts.fact_id`
    : "";
```

(`{documentId:String}` is already in the facts query's params — the outer WHERE uses it at the bottom of the builder.)

- [ ] **Step 4: Update `EsefFactRow` (lines ~47-52)**: rename `disclosure_raw_value_sha256` → `disclosure_text_sha256`.

- [ ] **Step 5: Update the probe consumer (lines ~365-369)**: `optionalTables.has("esef_fact_disclosures")` → `optionalTables.has("esef_disclosures")`.

- [ ] **Step 6: Update the row→fact mapping (lines ~399-410)**: `rawValueSha256: row.disclosure_raw_value_sha256` → `textSha256: row.disclosure_text_sha256`.

- [ ] **Step 7: Run the unit test**

Run: `npx vitest run app/lib/esef-financial-reports.sql.test.ts`
Expected: still FAIL on typecheck of `textSha256` (type not yet renamed) — proceed to Task 3.

### Task 3: Rename the evidence field through the type chain

**Files:**
- Modify: `app/lib/xbrl-facts.ts:32-37`
- Modify: `app/components/detail/esef-facts-accordion.tsx:365-384`

**Interfaces:**
- Produces: `XbrlFact.disclosureEvidence: { sourceRecordUid: string; textSha256: string; parserName: string; parserVersion: string } | null`.

- [ ] **Step 1:** In `app/lib/xbrl-facts.ts`, rename `rawValueSha256` → `textSha256` inside the `disclosureEvidence` object type.

- [ ] **Step 2:** In `app/components/detail/esef-facts-accordion.tsx` evidence block (~lines 365-384), change the DetailField label `"Raw value SHA-256"` → `"Text SHA-256"` and the property read `fact.disclosureEvidence.rawValueSha256` → `.textSha256`.

- [ ] **Step 3:** Search for stragglers: `rg -n "rawValueSha256|raw_value_sha256" app/` must return nothing.

- [ ] **Step 4: Run tests + typecheck**

Run: `npx vitest run app/lib/esef-financial-reports.sql.test.ts && npm run typecheck`
Expected: PASS / clean.

- [ ] **Step 5: Commit**

```bash
git add app/lib/esef-financial-reports.server.ts app/lib/esef-financial-reports.sql.test.ts app/lib/xbrl-facts.ts app/components/detail/esef-facts-accordion.tsx
git commit -m "fix(esef): join structured disclosures from esef_disclosures (esef_fact_disclosures was dropped in 000316)"
```

### Task 4: Live verification

- [ ] **Step 1:** With the dev server running (port 5183) and ClickHouse reachable, load `http://localhost:5183/company/se/5020077862/financials/esef/NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0`, expand a narrative fact (e.g. `ifrs-full:DisclosureOfFinanceCostExplanatory`), and confirm the "Structured extraction v1" badge now renders (`esef-facts-accordion.tsx:168-172`) plus parser evidence fields.
- [ ] **Step 2:** Run the live test if the DB is reachable: `npx vitest run tests/esef-financial-reports.server.test.ts` — must still PASS (SAGAX filing, 20s timeouts).
- [ ] **Step 3:** Report the before/after in the session summary; no commit for this task.

## Self-Review

- Spec coverage: probe rename (T2S1), join rewrite with both schema deltas (T2S3), sha rename chain (T2S4/T3), fallback preserved (T1 test 2). ✔
- The old third join condition is deliberately removed, not translated — documented in Background. ✔
- Type names consistent: `disclosure_text_sha256` (row) / `textSha256` (fact) used identically in T1, T2, T3. ✔
