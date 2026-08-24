import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import {
  ADDRESS_STATUS_INPUTS_SQL,
  CORRECTIONS_SQL,
  loadSeCompanyAddresses,
} from "~/lib/se-company-address.server";
import {
  ADDRESS_CORRECTION_STATUS_EXPR,
  ADDRESS_SCOPED_PUBLISHED_JOIN_SQL,
  ADDRESS_UNDONE_CTE_SQL,
  listSeCompanyAddressCorrectionsPage,
  loadSeCompanyAddressCorrectionFilterOptions,
} from "~/lib/se-company-address-lists.server";
import { correctionStatus, ZERO_EVIDENCE_HASH } from "~/lib/se-address-corrections";

/**
 * Integration tests against the real ClickHouse. The unit tests pin the SQL
 * TEXT, and text that reads well can still fail to compile or answer wrongly
 * (these statements read a FixedString(64) key, an Array(UUID) of applied ids,
 * a Bool flag and a Map(String, String) parameter, none of which a string
 * assertion knows about).
 *
 * The correction ledger is still empty in production -- no reviewer has decided
 * anything yet -- so the status tests synthesise both sides with those exact
 * column types rather than waiting for rows. The tests that run the shipped
 * queries as-is are what prove the statements themselves parse, and the address
 * final is live (4.67M rows), so the Address tab's own reads go against it.
 *
 * ONE fixture set feeds both the tab's per-company statements and the ledger
 * list's unscoped ones, and both assert the SAME eight statuses. That is the
 * point: the two modules answer the same question in different shapes (a
 * Map parameter for one company, a per-key JOIN for every company), and a
 * fixture they no longer agree on is a drift.
 */

/* -------------------------------------------------------------------- */
/* The shared fixture: one company, four published rows, eight decisions */
/* -------------------------------------------------------------------- */

const COMPANY = "5560125220";
const LIVE_KEY = "a".repeat(64);
const LIVE_HASH = "b".repeat(64);
const OTHER_KEY = "c".repeat(64);
const OTHER_HASH = "d".repeat(64);
const REJECTED_KEY = "e".repeat(64);
const REJECTED_HASH = "f".repeat(64);
const VANISHED_KEY = "1".repeat(64);
const VANISHED_HASH = "2".repeat(64);
/** The id Dagster stamped on the row it tombstoned. */
const STAMPED_ID = "22222222-2222-4222-8222-222222222222";
const id = (digit: string) =>
  `${digit.repeat(8)}-${digit.repeat(4)}-4${digit.repeat(3)}-8${digit.repeat(3)}-${digit.repeat(12)}`;

/** One synthesised published row of corpscout.se_company_address, with the
 * column types the reads actually see: FixedString(64) key and hash, Bool
 * is_current, Array(UUID) correction_ids. */
function addressRow(key: string, hash: string, isCurrent: boolean, ids: string[]): string {
  const idArray =
    ids.length === 0
      ? "CAST([], 'Array(UUID)')"
      : `CAST([${ids.map((value) => `CAST('${value}', 'UUID')`).join(", ")}], 'Array(UUID)')`;
  return `SELECT '${COMPANY}' AS company_id, CAST('${key}', 'FixedString(64)') AS address_key,
      CAST('${hash}', 'FixedString(64)') AS evidence_set_hash, ${isCurrent} AS is_current,
      ${idArray} AS correction_ids`;
}

/** One synthesised ledger row, with UUID / FixedString(64) / Nullable(UUID)
 * where the real table has them. */
function ledgerRow(
  correctionId: string,
  kind: string,
  payload: string,
  hash: string,
  supersedes: string | null = null,
): string {
  return `SELECT CAST('${correctionId}', 'UUID') AS correction_id, '${COMPANY}' AS company_id,
      '${kind}' AS correction_kind, '${payload}' AS payload,
      CAST('${hash}', 'FixedString(64)') AS evidence_hash, 'because' AS reason,
      'backoffice' AS decided_by,
      ${supersedes === null ? "CAST(NULL, 'Nullable(UUID)')" : `CAST('${supersedes}', 'Nullable(UUID)')`} AS supersedes_correction_id,
      now64(3, 'UTC') AS created_at`;
}

const override = (key: string) => `{"address_key":"${key}","care_of":"x"}`;
const reject = (key: string) => `{"address_key":"${key}"}`;

/**
 * The published side: two live rows, a reject-tombstone (is_current false but
 * the id that decided it still on the row, so the key is still in the produced
 * set) and a disappearance tombstone (with_set_replacement cleared its ids
 * exactly because the key left the produced set, so its hash must not count).
 */
const PUBLISHED_FIXTURE = [
  addressRow(LIVE_KEY, LIVE_HASH, true, []),
  addressRow(OTHER_KEY, OTHER_HASH, true, []),
  addressRow(REJECTED_KEY, REJECTED_HASH, false, [STAMPED_ID]),
  addressRow(VANISHED_KEY, VANISHED_HASH, false, []),
].join("\n    UNION ALL ");

const LEDGER_FIXTURE = [
  // Superseded by the undo, whatever it once did.
  ledgerRow(id("1"), "override_field", override(LIVE_KEY), LIVE_HASH),
  // Its id is stamped on the published row.
  ledgerRow(STAMPED_ID, "reject_address", reject(REJECTED_KEY), REJECTED_HASH),
  // Names LIVE_KEY, carries OTHER_KEY's hash -> stale.
  ledgerRow(id("3"), "override_field", override(LIVE_KEY), OTHER_HASH),
  // Live evidence of the row it names, nothing applied yet.
  ledgerRow(id("4"), "override_field", override(LIVE_KEY), LIVE_HASH),
  // Ruling A11: a reject naming a key the resolution did not produce has no
  // row to stamp, and is applied all the same.
  ledgerRow(id("5"), "reject_address", reject(VANISHED_KEY), VANISHED_HASH),
  // An undo carries the zero hash, which is never compared.
  ledgerRow(id("6"), "undo", "{}", ZERO_EVIDENCE_HASH, id("1")),
  // The reject-tombstoned row is still in the produced set and still hashes to
  // REJECTED_HASH -> waiting for the next run, not stale.
  ledgerRow(id("7"), "override_field", override(REJECTED_KEY), REJECTED_HASH),
  // The text had nowhere to land.
  ledgerRow(id("8"), "override_field", override(VANISHED_KEY), VANISHED_HASH),
].join("\n    UNION ALL ");

/**
 * What both modules must answer, in correction_id order. Two of these fail if
 * the per-key comparison is regressed to the company-wide sets it replaced:
 *
 * - 3333... names LIVE_KEY while carrying OTHER_KEY's hash, and is stale
 *   because Dagster compares against the row a correction NAMES; a company-wide
 *   `has(<the company's live hashes>, ...)` finds OTHER_HASH and reads pending.
 * - 7777... names the reject-tombstoned key with that row's own hash, and is
 *   pending because apply_address_ledger runs before with_set_replacement, so
 *   the rejected key is still in the produced set; a live-rows-only hash set
 *   misses REJECTED_HASH and reads stale.
 */
const EXPECTED_STATUSES = [
  "undone", // 1111...
  "applied", // 2222... stamped on the row it decided
  "stale", // 3333... another key's hash
  "pending", // 4444... live evidence, not yet applied
  "applied", // 5555... reject of a key nothing produces (ruling A11)
  "pending", // 6666... undo, zero hash, never compared
  "pending", // 7777... reject-tombstoned key, its own hash
  "stale", // 8888... nowhere to land
];

/** Substitutes a synthesised table for a real one, and refuses to run a test
 * that would silently have read production instead. */
function substituted(sql: string, replacements: Array<[string, string]>): string {
  let out = sql;
  for (const [from, to] of replacements) {
    expect(out).toContain(from);
    out = out.replaceAll(from, to);
  }
  expect(out).not.toContain("corpscout.se_company_address");
  return out;
}

/* -------------------------------------------------------------------- */
/* The ledger list (se-company-address-lists.server.ts)                  */
/* -------------------------------------------------------------------- */

describe("se_company_address_correction list against ClickHouse", () => {
  it("runs the shipped page query, filters, sorts and option list", async () => {
    const page = await listSeCompanyAddressCorrectionsPage({ page: 1, pageSize: 5 });
    expect(page.total).toBeGreaterThanOrEqual(0);
    // Every sort that is not a plain column repeats a whole expression in
    // ORDER BY, so each one is a statement ClickHouse has to accept.
    for (const sort of ["created_at", "status", "address_key", "company_id"]) {
      const filtered = await listSeCompanyAddressCorrectionsPage({
        page: 1,
        pageSize: 5,
        sort,
        dir: "asc",
        kind: "reject_address",
        status: "applied",
        companyId: COMPANY,
        decidedBy: "backoffice",
      });
      expect(filtered.total).toBeGreaterThanOrEqual(0);
    }
    expect(await loadSeCompanyAddressCorrectionFilterOptions()).toHaveProperty("decidedBy");
  }, 60000);

  it("answers all four statuses per address key over the real column types", async () => {
    // The published side, read from a synthesised `addr` instead of the final --
    // everything else, including the per-key join condition, is the shipped text.
    const published = substituted(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL, [
      [
        `  FROM corpscout.se_company_address FINAL
  WHERE company_id IN (SELECT company_id FROM corpscout.se_company_address_correction)
`,
        "  FROM addr\n",
      ],
    ]);
    const undone = ADDRESS_UNDONE_CTE_SQL.replace(
      "corpscout.se_company_address_correction",
      "ledger",
    ).replace("WITH undone AS (", "undone AS (");

    const sql = `WITH ledger AS (\n    ${LEDGER_FIXTURE}\n),
addr AS (\n    ${PUBLISHED_FIXTURE}\n),
${undone}
SELECT toString(c.correction_id) AS correction_id, ${ADDRESS_CORRECTION_STATUS_EXPR} AS status
FROM ledger AS c
${published}
ORDER BY correction_id`;

    const rows = await chQuery<{ correction_id: string; status: string }>(sql, {
      zeroHash: ZERO_EVIDENCE_HASH,
    });
    expect(rows.map((row) => row.status)).toEqual(EXPECTED_STATUSES);
  }, 60000);
});

/* -------------------------------------------------------------------- */
/* The Address tab's reads (se-company-address.server.ts)                */
/* -------------------------------------------------------------------- */

describe("the Address tab's reads against ClickHouse", () => {
  it("runs the shipped statements against the real final, for one real company", async () => {
    const [seed] = await chQuery<{ company_id: string }>(
      "SELECT company_id FROM corpscout.se_company_address LIMIT 1",
    );
    expect(seed?.company_id).toMatch(/^([0-9]{10}|[0-9]{12})$/);

    // The whole loader: two paged reads, the unbounded aggregate, and the
    // ledger read that consumes it -- including the Map(String, String)
    // parameter round-trip, which no string assertion can check.
    const detail = await loadSeCompanyAddresses(seed.company_id);
    expect(detail.addresses.length).toBeGreaterThan(0);
    expect(detail.removed).toBeInstanceOf(Array);
    expect(detail.corrections).toBeInstanceOf(Array);
    for (const row of detail.addresses) {
      expect(row.address_key).toMatch(/^[0-9a-f]{64}$/);
      expect(row.evidence_set_hash).toMatch(/^[0-9a-f]{64}$/);
    }

    const [inputs] = await chQuery<{
      applied_correction_ids: string[];
      key_evidence: Record<string, string>;
    }>(ADDRESS_STATUS_INPUTS_SQL, { companyId: seed.company_id });
    // Every live key of the paged list is in the map: the aggregate is over
    // the same final, unbounded.
    for (const row of detail.addresses) {
      expect(inputs.key_evidence[row.address_key]).toBe(row.evidence_set_hash);
    }
  }, 60000);

  /**
   * Review T7-m4 and T7-m5, over the real column types. The aggregate's own
   * output is what feeds the ledger read, so this is the whole derivation and
   * not two statements checked apart -- and the statuses are the list's own
   * EXPECTED_STATUSES, so the two modules cannot drift past each other.
   */
  it("answers each correction against the row it names, not against the company", async () => {
    const statusSql = `WITH addr AS (\n    ${PUBLISHED_FIXTURE}\n)\n${substituted(
      ADDRESS_STATUS_INPUTS_SQL,
      [
        [
          `FROM corpscout.se_company_address AS a FINAL
WHERE a.company_id = {companyId:String}`,
          "FROM addr AS a",
        ],
      ],
    )}`;

    const [inputs] = await chQuery<{
      applied_correction_ids: string[];
      key_evidence: Record<string, string>;
    }>(statusSql);
    expect(inputs.applied_correction_ids).toEqual([STAMPED_ID]);
    // The disappearance tombstone is absent; the reject-tombstone is not.
    expect(inputs.key_evidence).toEqual({
      [LIVE_KEY]: LIVE_HASH,
      [OTHER_KEY]: OTHER_HASH,
      [REJECTED_KEY]: REJECTED_HASH,
    });

    const correctionsSql = substituted(CORRECTIONS_SQL, [
      ["corpscout.se_company_address_correction", "ledger"],
    ]).replace("WITH superseded AS (", `WITH ledger AS (\n    ${LEDGER_FIXTURE}\n),\nsuperseded AS (`);

    const rows = await chQuery<{
      correction_id: string;
      is_current: number;
      is_applied: number;
      is_stale: number;
    }>(correctionsSql, {
      companyId: COMPANY,
      zeroHash: ZERO_EVIDENCE_HASH,
      // The aggregate's own answer, not a hand-written literal.
      keyEvidence: inputs.key_evidence,
      appliedIds: inputs.applied_correction_ids,
    });

    const statuses = rows
      .slice()
      .sort((a, b) => a.correction_id.localeCompare(b.correction_id))
      .map((row) => correctionStatus(row));
    expect(statuses).toEqual(EXPECTED_STATUSES);
  }, 60000);
});
