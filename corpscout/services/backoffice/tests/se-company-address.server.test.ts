import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyAddressCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  ADDRESSES_SQL,
  appendSeCompanyAddressCorrection,
  CORRECTIONS_SQL,
  loadSeCompanyAddresses,
  REMOVED_SQL,
} from "~/lib/se-company-address.server";
import {
  SeAddressCorrectionValidationError,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";

const COMPANY = "5560125220";
const KEY = "a".repeat(64);
const HASH = "b".repeat(64);

describe("ADDRESSES_SQL", () => {
  it("reads the final, newest version only, live rows only", () => {
    expect(ADDRESSES_SQL).toContain("FROM corpscout.se_company_address AS a FINAL");
    expect(ADDRESSES_SQL).toContain("WHERE a.company_id = {companyId:String}");
    expect(ADDRESSES_SQL).toContain("AND a.is_current");
    // The raw chain is gone: the geocode now travels on the published row.
    expect(ADDRESSES_SQL).not.toContain("se_company_addresses_current");
    expect(ADDRESSES_SQL).not.toContain("se_company_address_display_current");
    expect(ADDRESSES_SQL).not.toContain("JOIN");
  });

  it("exposes the evidence hash the correction form has to echo back", () => {
    expect(ADDRESSES_SQL).toContain("toString(a.evidence_set_hash) AS evidence_set_hash");
    expect(ADDRESSES_SQL).toContain("toString(a.address_key) AS address_key");
  });

  it("collapses genuinely Nullable columns to text rather than mapping null in TypeScript", () => {
    expect(ADDRESSES_SQL).toContain("ifNull(a.care_of, '') AS care_of");
    expect(ADDRESSES_SQL).toContain("ifNull(toString(a.latitude), '') AS latitude");
  });

  it("passes the company as a parameter and bounds the read", () => {
    for (const sql of [ADDRESSES_SQL, REMOVED_SQL, CORRECTIONS_SQL]) {
      expect(sql).not.toContain(COMPANY);
      expect(sql).not.toContain("${");
      expect(sql).toMatch(/LIMIT \d+/);
    }
  });
});

/**
 * Ruling A8: a rejected address must stay reachable, or the correction that
 * rejected it can never be undone. Tombstones are the same projection read
 * with the flag inverted -- one shape, two lists.
 */
describe("REMOVED_SQL", () => {
  it("reads the tombstoned rows of the same table with the same columns", () => {
    expect(REMOVED_SQL).toContain("FROM corpscout.se_company_address AS a FINAL");
    expect(REMOVED_SQL).toContain("AND NOT a.is_current");
    expect(REMOVED_SQL).toContain("toString(a.address_key) AS address_key");
    // Same projection, so a column added to one list can never be missing
    // from the other.
    expect(REMOVED_SQL.split("\nFROM ")[0]).toBe(ADDRESSES_SQL.split("\nFROM ")[0]);
  });
});

describe("CORRECTIONS_SQL", () => {
  it("computes status against the published rows of this company", () => {
    expect(CORRECTIONS_SQL).toContain("FROM corpscout.se_company_address_correction AS c");
    expect(CORRECTIONS_SQL).toContain("{evidenceSetHashes:Array(String)}");
    expect(CORRECTIONS_SQL).toContain("AS is_current");
    expect(CORRECTIONS_SQL).toContain("AS is_stale");
    expect(CORRECTIONS_SQL).toContain("AS is_applied");
    expect(CORRECTIONS_SQL).toContain("supersedes_correction_id IS NOT NULL");
    expect(CORRECTIONS_SQL).toContain("{zeroHash:String}");
    expect(CORRECTIONS_SQL).toContain(
      "JSONExtractString(c.payload, 'address_key') AS address_key",
    );
  });

  /**
   * Ruling A11: Dagster stamps a reject's id on the row it tombstoned, but
   * when the key it names is not in the produced set there is no row to stamp
   * -- address_rules.py skips it without recording it stale. Deriving the
   * status from correction_ids alone would leave such a reject "pending"
   * forever, so absence of the key IS the applied signal.
   */
  it("counts a reject whose address key is gone as applied, never as pending or stale", () => {
    expect(CORRECTIONS_SQL).toContain(
      `toUInt8(
    has({appliedIds:Array(String)}, toString(c.correction_id))
    OR (
      c.correction_kind = 'reject_address'
      AND address_key != ''
      AND NOT has({liveAddressKeys:Array(String)}, address_key)
    )
  ) AS is_applied`,
    );
    // An applied correction is never also stale: staleness is what happens to
    // a decision that has NOT landed.
    expect(CORRECTIONS_SQL).toContain("AND NOT is_applied");
  });
});

describe("loadSeCompanyAddresses", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockResolvedValue([]);
  });

  it("threads the live rows' hashes and keys, and every row's applied ids, into the ledger read", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        { address_key: KEY, evidence_set_hash: HASH, correction_ids: ["11111111-1111-4111-8111-111111111111"] },
      ])
      .mockResolvedValueOnce([
        { address_key: "c".repeat(64), evidence_set_hash: "d".repeat(64), correction_ids: ["22222222-2222-4222-8222-222222222222"] },
      ])
      .mockResolvedValueOnce([]);
    const detail = await loadSeCompanyAddresses(COMPANY);

    expect(detail.addresses).toHaveLength(1);
    expect(detail.removed).toHaveLength(1);
    expect(clickhouse.query).toHaveBeenNthCalledWith(1, ADDRESSES_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, REMOVED_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(3, CORRECTIONS_SQL, {
      companyId: COMPANY,
      zeroHash: ZERO_EVIDENCE_HASH,
      // Live rows only: a tombstone keeps the hash it had when it was live,
      // so counting it would make a correction that has nowhere to land look
      // fresh forever.
      evidenceSetHashes: [HASH],
      liveAddressKeys: [KEY],
      // Both lists: Dagster stamps a reject's id on the row it tombstoned.
      appliedIds: [
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
      ],
    });
  });

  it("is empty, not an error, for a company nothing has published", async () => {
    const detail = await loadSeCompanyAddresses(COMPANY);
    expect(detail).toEqual({ addresses: [], removed: [], corrections: [] });
  });
});

describe("appendSeCompanyAddressCorrection", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("refuses when the named address is not published", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await expect(
      appendSeCompanyAddressCorrection({
        companyId: COMPANY,
        kind: "reject_address",
        payload: { address_key: KEY },
        evidenceHash: HASH,
        reason: "Not this company's address.",
      }),
    ).rejects.toThrow(SeAddressCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("refuses when that row's evidence moved while the reviewer was deciding", async () => {
    clickhouse.query.mockResolvedValueOnce([{ evidence_set_hash: "e".repeat(64) }]);
    await expect(
      appendSeCompanyAddressCorrection({
        companyId: COMPANY,
        kind: "override_field",
        payload: { address_key: KEY, care_of: "c/o Anna" },
        evidenceHash: HASH,
        reason: "Care-of was wrong.",
      }),
    ).rejects.toThrow(/evidence changed/i);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("re-reads the hash of the NAMED row, not of the company", async () => {
    clickhouse.query.mockResolvedValueOnce([{ evidence_set_hash: HASH }]);
    clickhouse.insert.mockResolvedValue(undefined);
    await appendSeCompanyAddressCorrection({
      companyId: COMPANY,
      kind: "override_field",
      payload: { address_key: KEY, care_of: "c/o Anna" },
      evidenceHash: HASH,
      reason: "Care-of was wrong.",
    });
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("a.address_key = {addressKey:String}");
    expect(sql).toContain("AND a.is_current");
    expect(params).toEqual({ companyId: COMPANY, addressKey: KEY });
  });

  it("appends one row with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ evidence_set_hash: HASH }]);
    clickhouse.insert.mockResolvedValue(undefined);
    const { correctionId } = await appendSeCompanyAddressCorrection({
      companyId: COMPANY,
      kind: "reject_address",
      payload: { address_key: KEY },
      evidenceHash: HASH,
      reason: "Belongs to the accountant, not the company.",
    });
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      correction_id: correctionId,
      company_id: COMPANY,
      correction_kind: "reject_address",
      evidence_hash: HASH,
      decided_by: "backoffice",
      supersedes_correction_id: null,
    });
    expect(JSON.parse(rows[0].payload)).toEqual({ address_key: KEY });
    expect(rows[0].created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });

  it("undo skips the re-read, so a rejected address can still be brought back", async () => {
    clickhouse.insert.mockResolvedValue(undefined);
    await appendSeCompanyAddressCorrection({
      companyId: COMPANY,
      kind: "undo",
      evidenceHash: ZERO_EVIDENCE_HASH,
      reason: "The address was right after all.",
      supersedesCorrectionId: "11111111-1111-4111-8111-111111111111",
    });
    expect(clickhouse.query).not.toHaveBeenCalled();
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0]).toMatchObject({
      correction_kind: "undo",
      supersedes_correction_id: "11111111-1111-4111-8111-111111111111",
    });
  });
});
