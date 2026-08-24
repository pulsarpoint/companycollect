import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import {
  ADDRESS_CORRECTION_STATUS_EXPR,
  ADDRESS_SCOPED_PUBLISHED_JOIN_SQL,
  listSeCompanyAddressCorrectionsPage,
  loadSeCompanyAddressCorrectionFilterOptions,
} from "~/lib/se-company-address-lists.server";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-address-corrections";

/**
 * Integration test against the real ClickHouse: tests/se-company-address-lists
 * .server.test.ts pins the SQL TEXT, and text that reads well can still fail to
 * compile or answer wrongly (the aggregate join reads a FixedString(64) key, an
 * Array(UUID) of applied ids and a Bool flag, none of which a string assertion
 * knows about).
 *
 * Both address tables are still empty in production -- the datatype's first
 * resolve has not run -- so the second test synthesises the two sides with those
 * exact column types rather than waiting for rows. The first test runs the
 * shipped query as-is, which is what proves the statement itself parses.
 */
const KEY = "a".repeat(64);
const GONE = "0".repeat(64);
const HASH = "b".repeat(64);
const STALE = "1".repeat(64);
const APPLIED_ID = "55555555-5555-4555-8555-555555555555";

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
        companyId: "5560125220",
        decidedBy: "backoffice",
      });
      expect(filtered.total).toBeGreaterThanOrEqual(0);
    }
    expect(await loadSeCompanyAddressCorrectionFilterOptions()).toHaveProperty("decidedBy");
  }, 60000);

  it("answers all four statuses over the real column types", async () => {
    // The published side, read from a synthesised `addr` instead of the (empty)
    // final -- everything else, including the aggregation, is the shipped text.
    const published = ADDRESS_SCOPED_PUBLISHED_JOIN_SQL.replace(
      `  FROM corpscout.se_company_address FINAL
  WHERE company_id IN (SELECT company_id FROM corpscout.se_company_address_correction)
`,
      "  FROM addr\n",
    );
    const sql = `WITH
addr AS (
  SELECT '5560125220' AS company_id, CAST({key:String}, 'FixedString(64)') AS address_key,
         CAST([CAST({appliedId:String}, 'UUID')], 'Array(UUID)') AS correction_ids,
         true AS is_current, CAST({hash:String}, 'FixedString(64)') AS evidence_set_hash
  UNION ALL
  SELECT '5560125220', CAST({gone:String}, 'FixedString(64)'), CAST([], 'Array(UUID)'),
         false, CAST({stale:String}, 'FixedString(64)')
),
undone AS (SELECT CAST('11111111-1111-4111-8111-111111111111', 'UUID') AS id),
c AS (
  SELECT CAST('11111111-1111-4111-8111-111111111111', 'UUID') AS correction_id,
         '5560125220' AS company_id, 'override_field' AS correction_kind,
         concat('{"address_key":"', {key:String}, '","care_of":"x"}') AS payload,
         CAST({hash:String}, 'FixedString(64)') AS evidence_hash
  UNION ALL SELECT CAST('22222222-2222-4222-8222-222222222222', 'UUID'), '5560125220',
         'reject_address', concat('{"address_key":"', {gone:String}, '"}'),
         CAST({hash:String}, 'FixedString(64)')
  UNION ALL SELECT CAST('33333333-3333-4333-8333-333333333333', 'UUID'), '5560125220',
         'override_field', concat('{"address_key":"', {key:String}, '","care_of":"y"}'),
         CAST({stale:String}, 'FixedString(64)')
  UNION ALL SELECT CAST('44444444-4444-4444-8444-444444444444', 'UUID'), '5560125220',
         'override_field', concat('{"address_key":"', {key:String}, '","care_of":"z"}'),
         CAST({hash:String}, 'FixedString(64)')
  UNION ALL SELECT CAST({appliedId:String}, 'UUID'), '5560125220',
         'override_field', concat('{"address_key":"', {key:String}, '","care_of":"w"}'),
         CAST({hash:String}, 'FixedString(64)')
  UNION ALL SELECT CAST('66666666-6666-4666-8666-666666666666', 'UUID'), '5560125220',
         'undo', '{}', CAST({zeroHash:String}, 'FixedString(64)')
)
SELECT toString(c.correction_id) AS correction_id, ${ADDRESS_CORRECTION_STATUS_EXPR} AS status
FROM c
${published}
ORDER BY correction_id`;

    const rows = await chQuery<{ correction_id: string; status: string }>(sql, {
      zeroHash: ZERO_EVIDENCE_HASH,
      key: KEY,
      gone: GONE,
      hash: HASH,
      stale: STALE,
      appliedId: APPLIED_ID,
    });

    expect(rows.map((row) => row.status)).toEqual([
      // Superseded by the undo, whatever it once did.
      "undone",
      // Ruling A11: a reject naming a key the company does not publish has no
      // row for Dagster to stamp, and is applied all the same.
      "applied",
      // Its evidence hash matches no live row of the company.
      "stale",
      // Live evidence, nothing applied yet.
      "pending",
      // Its id is stamped on the published row.
      "applied",
      // An undo carries the zero hash, which is never compared -- so it is
      // pending until Dagster reads it, never stale.
      "pending",
    ]);
  }, 60000);
});
