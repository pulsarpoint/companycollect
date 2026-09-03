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
