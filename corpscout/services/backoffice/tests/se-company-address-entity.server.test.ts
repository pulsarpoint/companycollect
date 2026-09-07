import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  query: vi.fn(),
  insertSuggestions: vi.fn(),
  insertRules: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeCompanyAddressSuggestions: clickhouse.insertSuggestions,
  chInsertSeCompanyAddressRules: clickhouse.insertRules,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => null) }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  dagsterRunUrl: dagster.dagsterRunUrl,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET: "se_company_address_fold_companies",
}));

import {
  activateSeAddressDraft,
  ADDRESS_HISTORY_SQL,
  ADDRESS_MAIN_SQL,
  ADDRESS_NORMALIZED_SQL,
  ADDRESS_PRECEDENCE_SQL,
  ADDRESS_RAW_SQL,
  ADDRESS_RULES_SQL,
  discardSeAddressDraft,
  launchSeAddressFold,
  loadSeAddressDetail,
  removeSeAddress,
  resetSeAddress,
  saveSeAddressDraft,
  SeAddressDecisionError,
  type SeAddressNormalizedRow,
  type SeAddressPrecedenceRow,
  type SeAddressRawRow,
  type SeAddressRow,
  type SeAddressRuleRow,
} from "~/lib/se-company-address-entity.server";

const COMPANY = "5560000001";
const MERGED_KEY = "a".repeat(64);
const BOX_KEY = "b".repeat(64);
const DRAFT_KEY = "c".repeat(64);
const REVIEWER_KEY = "d".repeat(64);
const UNKNOWN_KEY = "e".repeat(64);
/** `stampSlot` of NOW: "r" + the 17 digits of `2026-09-07 20:33:55.123`. */
const DRAFT_SLOT = "r20260907203355123";
const NOW = new Date("2026-09-07T20:33:55.123Z");
const STAMP = "2026-09-07 20:33:55.123";
/** sha256("<company>\n<source>\n<slot>\n<stamp>"), computed outside the module
 * so a changed preimage fails here instead of agreeing with itself. */
const DRAFT_SUGGESTION_ID = "58533985d57554e7af8746bcd6cea87bfcc2b0edd09558e939ff381f9cf00dea";
const REVIEWER_SUGGESTION_ID = "8a0b7aac91f256367a733cc07608fa58e871601b95ea95f320f54ee88dd38fea";
const REVIEWER_SLOT_SUGGESTION_ID = "795d6aeb2fecf372ae87afd81ae98bc060d8155c7a95dbd5a82303a5847eeadd";

function main(over: Partial<SeAddressRow>): SeAddressRow {
  return {
    company_id: COMPANY,
    address_key: MERGED_KEY,
    care_of: "",
    box: "",
    street_name: "",
    house_number: "",
    unit: "",
    postal_code: "",
    city: "",
    country_code: "SE",
    normalized_address: "",
    kinds: ["postal"],
    sources: ["scb"],
    slots: ["s1"],
    normalized_ids: ["n1"],
    text_source: "scb",
    active: 1,
    inactive_reason: "",
    latitude: null,
    longitude: null,
    geocode_status: "",
    geocode_method: "",
    geocode_confidence: null,
    geocode_precision: "",
    geocode_policy: "",
    geocode_reference: "",
    geocoded_at: "",
    normalizer_version: "address-normalizer-v1",
    folded_at: "2026-09-07 09:00:00.000",
    fold_version: "fold-v1",
    source_run_id: "run-fold",
    ...over,
  };
}

function normalized(over: Partial<SeAddressNormalizedRow>): SeAddressNormalizedRow {
  return {
    company_id: COMPANY,
    source: "scb",
    slot: "s1",
    normalized_id: "n1",
    suggestion_id: "sid1",
    suggested_at: "2026-09-06 07:00:00.000",
    kind: "postal",
    care_of: "",
    box: "",
    street_name: "",
    house_number: "",
    unit: "",
    postal_code: "",
    city: "",
    country_code: "SE",
    normalized_address: "",
    address_key: MERGED_KEY,
    parse_status: "ok",
    parse_notes: "",
    normalizer_version: "address-normalizer-v1",
    normalized_at: "2026-09-06 08:00:00.000",
    ...over,
  };
}

function raw(over: Partial<SeAddressRawRow>): SeAddressRawRow {
  return {
    company_id: COMPANY,
    source: "scb",
    slot: "s1",
    suggestion_id: "sid1",
    source_record_uid: "uid-1",
    observed_at: "2026-09-06 06:00:00.000",
    kind: "postal",
    raw_address: "",
    care_of: "",
    street_address: "",
    postal_code: "",
    post_town: "",
    county: "",
    country_code: "SE",
    decided_by: "",
    note: "",
    replaces_key: "",
    suggested_at: "2026-09-06 07:00:00.000",
    source_run_id: "run-scb",
    extractor_version: "scb-v1",
    ...over,
  };
}

// The merged street address: SCB carries the unit, Bolagsverket does not, and
// Bolagsverket's CURRENT normalized version (n2b) is newer than the one the
// published row was folded from (n2) -- a re-fold pending member.
const SCB_NORMALIZED = normalized({
  street_name: "Storgatan",
  house_number: "5",
  unit: "1tr",
  postal_code: "11122",
  city: "Stockholm",
  normalized_address: "Storgatan 5 1tr, 111 22 Stockholm",
});
const BV_NORMALIZED = normalized({
  source: "bolagsverket",
  slot: "b1",
  normalized_id: "n2b",
  suggestion_id: "sid2",
  suggested_at: "2026-09-07 07:00:00.000",
  kind: "registered",
  street_name: "Storgatan",
  house_number: "5",
  postal_code: "11122",
  city: "Stockholm",
  normalized_address: "Storgatan 5, 111 22 Stockholm",
  normalized_at: "2026-09-07 10:00:00.000",
});
const RATSIT_NORMALIZED = normalized({
  source: "ratsit",
  slot: "x1",
  normalized_id: "n3",
  suggestion_id: "sid3",
  suggested_at: "2026-09-05 07:00:00.000",
  box: "123",
  postal_code: "11123",
  city: "Stockholm",
  normalized_address: "Box 123, 111 23 Stockholm",
  address_key: BOX_KEY,
  normalized_at: "2026-09-05 08:00:00.000",
});
// The draft's own normalized row: never folded, never counted for fold-pending.
const DRAFT_NORMALIZED = normalized({
  source: "reviewer_draft",
  slot: DRAFT_SLOT,
  normalized_id: "n4",
  suggestion_id: DRAFT_SUGGESTION_ID,
  suggested_at: STAMP,
  kind: "visiting",
  care_of: "Anna Svensson",
  street_name: "Storgatan",
  house_number: "7",
  postal_code: "11122",
  city: "Stockholm",
  normalized_address: "c/o Anna Svensson, Storgatan 7, 111 22 Stockholm",
  address_key: DRAFT_KEY,
  normalized_at: "2026-09-07 21:00:00.000",
});
const NORMALIZED_ROWS = [BV_NORMALIZED, RATSIT_NORMALIZED, DRAFT_NORMALIZED, SCB_NORMALIZED];

const SCB_RAW = raw({
  street_address: "Storgatan 5 1tr",
  postal_code: "11122",
  post_town: "STOCKHOLM",
});
const BV_RAW = raw({
  source: "bolagsverket",
  slot: "b1",
  suggestion_id: "sid2",
  kind: "registered",
  street_address: "Storgatan 5",
  postal_code: "11122",
  post_town: "Stockholm",
  source_run_id: "run-bv",
  extractor_version: "bolagsverket-v1",
});
const RATSIT_RAW = raw({
  source: "ratsit",
  slot: "x1",
  suggestion_id: "sid3",
  street_address: "Box 123",
  postal_code: "11123",
  post_town: "Stockholm",
  source_run_id: "run-ratsit",
  extractor_version: "ratsit-v1",
});
const REVIEWER_RAW = raw({
  source: "reviewer",
  slot: "rev1",
  suggestion_id: "sid5",
  kind: "visiting",
  care_of: "",
  street_address: "Kungsgatan 1",
  postal_code: "11143",
  post_town: "Stockholm",
  decided_by: "backoffice",
  note: "typed by reviewer",
  source_run_id: "backoffice",
  extractor_version: "backoffice-v1",
});
// A Correct in progress: the draft carries the published key it replaces.
const DRAFT_RAW = raw({
  source: "reviewer_draft",
  slot: DRAFT_SLOT,
  suggestion_id: DRAFT_SUGGESTION_ID,
  source_record_uid: "",
  observed_at: STAMP,
  kind: "visiting",
  care_of: "Anna Svensson",
  street_address: "Storgatan 7",
  postal_code: "11122",
  post_town: "Stockholm",
  decided_by: "backoffice",
  note: "moved next door",
  replaces_key: MERGED_KEY,
  suggested_at: STAMP,
  source_run_id: "backoffice",
  extractor_version: "backoffice-v1",
});
const RAW_ROWS = [BV_RAW, RATSIT_RAW, REVIEWER_RAW, DRAFT_RAW, SCB_RAW];

const MERGED_ROW = main({
  address_key: MERGED_KEY,
  street_name: "Storgatan",
  house_number: "5",
  unit: "1tr",
  postal_code: "11122",
  city: "Stockholm",
  normalized_address: "Storgatan 5 1tr, 111 22 Stockholm",
  kinds: ["postal", "registered"],
  sources: ["scb", "bolagsverket"],
  slots: ["s1", "b1"],
  normalized_ids: ["n1", "n2"],
  text_source: "scb",
  latitude: 59.3326,
  longitude: 18.0649,
  geocode_status: "matched_exact",
  geocode_method: "osm_exact",
  geocode_confidence: 0.98,
  geocode_precision: "rooftop",
  geocode_policy: "osm_v1",
  geocode_reference: "osm-2026-08",
  geocoded_at: "2026-09-07 09:00:02.000",
});
const BOX_ROW = main({
  address_key: BOX_KEY,
  box: "123",
  postal_code: "11123",
  city: "Stockholm",
  normalized_address: "Box 123, 111 23 Stockholm",
  kinds: ["postal"],
  sources: ["ratsit"],
  slots: ["x1"],
  normalized_ids: ["n3"],
  text_source: "ratsit",
  active: 0,
  inactive_reason: "hidden",
  geocode_status: "postal_box",
});
const HISTORY_ROW = main({ ...MERGED_ROW, unit: "", folded_at: "2026-09-06 09:00:00.000" });

const BOX_HIDE_RULE: SeAddressRuleRow = {
  company_id: COMPANY,
  address_key: BOX_KEY,
  action: "hide",
  removed: 0,
  decided_by: "backoffice",
  note: "a box is not where they sit",
  decided_at: "2026-09-06 12:00:00.000",
};
const GLOBAL_PRECEDENCE: SeAddressPrecedenceRow = {
  company_id: "",
  field: "address",
  source: "scb",
  precedence: 1000,
  removed: 0,
  decided_by: "",
  note: "",
  decided_at: "2026-01-01 00:00:00.000",
};

function answer(sql: string): unknown[] {
  if (sql.includes("FROM corpscout.se_company_address_v2")) return [MERGED_ROW, BOX_ROW];
  if (sql.includes("FROM corpscout.se_company_address_history")) return [HISTORY_ROW];
  if (sql.includes("FROM corpscout.se_company_address_normalized")) return NORMALIZED_ROWS;
  if (sql.includes("FROM corpscout.se_company_address_suggestion")) return RAW_ROWS;
  if (sql.includes("FROM corpscout.se_company_address_rule")) return [BOX_HIDE_RULE];
  if (sql.includes("FROM corpscout.se_company_address_precedence")) return [GLOBAL_PRECEDENCE];
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}

/** The rows one `chInsertSeCompanyAddressSuggestions` / `...Rules` call got. */
function inserted(mock: { mock: { calls: unknown[][] } }, call = 0): Record<string, unknown>[] {
  return mock.mock.calls[call]?.[0] as Record<string, unknown>[];
}

describe("se-company-address-entity.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => answer(sql));
    clickhouse.insertSuggestions.mockReset();
    clickhouse.insertRules.mockReset();
    dagster.launchRun.mockReset();
    dagster.launchRun.mockResolvedValue({ runId: "run-9", status: "STARTED" });
  });

  it("pins every read to a FINAL current version, the company parameter and string keys", () => {
    expect(ADDRESS_MAIN_SQL).toContain("FROM corpscout.se_company_address_v2 AS m FINAL");
    expect(ADDRESS_MAIN_SQL).toContain("WHERE m.company_id = {companyId:String}");
    expect(ADDRESS_MAIN_SQL).toContain("toString(m.address_key) AS address_key");
    expect(ADDRESS_MAIN_SQL).toContain("arrayMap(x -> toString(x), m.normalized_ids) AS normalized_ids");
    expect(ADDRESS_MAIN_SQL).toContain("toUInt8(m.active) AS active");
    expect(ADDRESS_MAIN_SQL).toContain("ORDER BY m.active DESC");
    // History is append-only: never FINAL, newest first, capped.
    expect(ADDRESS_HISTORY_SQL).toContain("FROM corpscout.se_company_address_history AS h");
    expect(ADDRESS_HISTORY_SQL).not.toContain("FINAL");
    expect(ADDRESS_HISTORY_SQL).toContain("WHERE h.company_id = {companyId:String}");
    expect(ADDRESS_HISTORY_SQL).toContain("ORDER BY h.folded_at DESC");
    expect(ADDRESS_HISTORY_SQL).toContain("LIMIT 200");
    expect(ADDRESS_NORMALIZED_SQL).toContain("FROM corpscout.se_company_address_normalized AS n FINAL");
    expect(ADDRESS_NORMALIZED_SQL).toContain("WHERE n.company_id = {companyId:String}");
    expect(ADDRESS_NORMALIZED_SQL).toContain("toString(n.normalized_id) AS normalized_id");
    expect(ADDRESS_NORMALIZED_SQL).toContain("toString(n.suggestion_id) AS suggestion_id");
    expect(ADDRESS_NORMALIZED_SQL).toContain("toString(n.address_key) AS address_key");
    expect(ADDRESS_RAW_SQL).toContain("FROM corpscout.se_company_address_suggestion AS s FINAL");
    expect(ADDRESS_RAW_SQL).toContain("WHERE s.company_id = {companyId:String}");
    expect(ADDRESS_RAW_SQL).toContain("ifNull(toString(s.replaces_key), '') AS replaces_key");
    expect(ADDRESS_RULES_SQL).toContain("FROM corpscout.se_company_address_rule AS r FINAL");
    expect(ADDRESS_RULES_SQL).toContain("WHERE r.company_id = {companyId:String}");
    expect(ADDRESS_RULES_SQL).toContain("toString(r.address_key) AS address_key");
    expect(ADDRESS_RULES_SQL).toContain("toUInt8(r.removed) AS removed");
    expect(ADDRESS_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_address_precedence AS p FINAL");
    expect(ADDRESS_PRECEDENCE_SQL).toContain("WHERE p.company_id IN ('', {companyId:String})");
    expect(ADDRESS_PRECEDENCE_SQL).toContain("toUInt32(p.precedence) AS precedence");
    // Every nullable component reaches the page as '' -- never null.
    for (const column of ["care_of", "box", "street_name", "house_number", "unit", "postal_code", "city"]) {
      expect(ADDRESS_MAIN_SQL).toContain(`ifNull(m.${column}, '') AS ${column}`);
      expect(ADDRESS_NORMALIZED_SQL).toContain(`ifNull(n.${column}, '') AS ${column}`);
    }
    for (const column of ["raw_address", "care_of", "street_address", "postal_code", "post_town", "county", "country_code", "decided_by", "note"]) {
      expect(ADDRESS_RAW_SQL).toContain(`ifNull(s.${column}, '') AS ${column}`);
    }
  });

  it("assembles published rows, members, the text-source reason, the hide rule and the drafts", async () => {
    const detail = await loadSeAddressDetail(COMPANY);
    expect(detail).not.toBeNull();
    expect(detail?.published.map((entry) => entry.row.address_key)).toEqual([MERGED_KEY, BOX_KEY]);

    const merged = detail?.published[0];
    expect(merged?.members).toEqual([
      {
        source: "scb",
        slot: "s1",
        normalizedId: "n1",
        current: SCB_NORMALIZED,
        raw: SCB_RAW,
        refoldPending: false,
        completeness: 5,
      },
      {
        source: "bolagsverket",
        slot: "b1",
        normalizedId: "n2",
        current: BV_NORMALIZED,
        raw: BV_RAW,
        // The current normalized version is not the one this row was folded from.
        refoldPending: true,
        completeness: 4,
      },
    ]);
    expect(merged?.textSourceReason).toBe("most complete");
    expect(merged?.hideRule).toBeNull();

    const box = detail?.published[1];
    expect(box?.members).toHaveLength(1);
    expect(box?.textSourceReason).toBe("single source");
    expect(box?.hideRule).toEqual(BOX_HIDE_RULE);

    expect(detail?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: DRAFT_NORMALIZED, replacesKey: MERGED_KEY },
    ]);
    expect(detail?.history).toEqual([HISTORY_ROW]);
    expect(detail?.rules).toEqual([BOX_HIDE_RULE]);
    expect(detail?.precedence).toEqual([GLOBAL_PRECEDENCE]);
    // Bolagsverket's normalized_at (10:00) is newer than the fold (09:00).
    expect(detail?.foldPending).toBe(true);
    for (const sql of [ADDRESS_MAIN_SQL, ADDRESS_HISTORY_SQL, ADDRESS_NORMALIZED_SQL, ADDRESS_RAW_SQL, ADDRESS_RULES_SQL, ADDRESS_PRECEDENCE_SQL]) {
      expect(clickhouse.query.mock.calls.find(([text]) => text === sql)?.[1]).toEqual({ companyId: COMPANY });
    }
  });

  it("calls the text source a tie-break when another member is just as complete", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_NORMALIZED_SQL) {
        return [{ ...SCB_NORMALIZED, unit: "" }, BV_NORMALIZED, RATSIT_NORMALIZED];
      }
      return answer(sql);
    });
    const detail = await loadSeAddressDetail(COMPANY);
    expect(detail?.published[0]?.members.map((member) => member.completeness)).toEqual([4, 4]);
    expect(detail?.published[0]?.textSourceReason).toBe("tie-break");
  });

  it("keeps a draft out of fold-pending and reports no fold when every stamp is older", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      // Only the draft (21:00) is newer than the fold now.
      if (sql === ADDRESS_NORMALIZED_SQL) {
        return [SCB_NORMALIZED, { ...BV_NORMALIZED, normalized_at: "2026-09-07 08:00:00.000" }, RATSIT_NORMALIZED, DRAFT_NORMALIZED];
      }
      return answer(sql);
    });
    expect((await loadSeAddressDetail(COMPANY))?.foldPending).toBe(false);
  });

  it("is fold-pending when a rule is newer than the fold, and when nothing has been folded yet", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_NORMALIZED_SQL) {
        return [SCB_NORMALIZED, { ...BV_NORMALIZED, normalized_at: "2026-09-07 08:00:00.000" }, RATSIT_NORMALIZED];
      }
      if (sql === ADDRESS_RULES_SQL) return [{ ...BOX_HIDE_RULE, removed: 1, decided_at: "2026-09-07 12:00:00.000" }];
      return answer(sql);
    });
    // A release (removed = 1) still counts: it is not applied until the fold runs.
    expect((await loadSeAddressDetail(COMPANY))?.foldPending).toBe(true);

    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_MAIN_SQL || sql === ADDRESS_HISTORY_SQL ? [] : answer(sql),
    );
    const unfolded = await loadSeAddressDetail(COMPANY);
    expect(unfolded?.published).toEqual([]);
    expect(unfolded?.foldPending).toBe(true);
  });

  it("returns null only when there is no main row, no normalized row and no draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_PRECEDENCE_SQL ? [GLOBAL_PRECEDENCE] : [],
    );
    expect(await loadSeAddressDetail(COMPANY)).toBeNull();

    // A typed draft alone is enough to open the tab.
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_RAW_SQL) return [DRAFT_RAW];
      if (sql === ADDRESS_PRECEDENCE_SQL) return [GLOBAL_PRECEDENCE];
      return [];
    });
    const draftOnly = await loadSeAddressDetail(COMPANY);
    expect(draftOnly?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: null, replacesKey: MERGED_KEY },
    ]);
    expect(draftOnly?.foldPending).toBe(false);
  });

  it("ignores a cleared reviewer_draft row: it is a tombstone, not a draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_RAW_SQL) {
        return [{ ...DRAFT_RAW, care_of: "", street_address: "", postal_code: "", post_town: "", note: "discarded" }];
      }
      return answer(sql);
    });
    expect((await loadSeAddressDetail(COMPANY))?.drafts).toEqual([]);
  });

  it("saves a draft under a new slot as one raw row with the stamp's id", async () => {
    const result = await saveSeAddressDraft(
      COMPANY,
      {
        intent: "save-draft",
        slot: null,
        replacesKey: MERGED_KEY,
        input: {
          careOf: "Anna Svensson",
          streetLine: "Storgatan 7",
          postalCode: "11122",
          city: "Stockholm",
          country: "SE",
          kind: "visiting",
          note: "moved next door",
        },
      },
      NOW,
    );
    expect(result).toEqual({ decidedAt: STAMP, slot: DRAFT_SLOT });
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        slot: DRAFT_SLOT,
        suggestion_id: DRAFT_SUGGESTION_ID,
        source_record_uid: "",
        observed_at: STAMP,
        kind: "visiting",
        raw_address: null,
        care_of: "Anna Svensson",
        street_address: "Storgatan 7",
        postal_code: "11122",
        post_town: "Stockholm",
        county: null,
        country_code: "SE",
        decided_by: "backoffice",
        note: "moved next door",
        replaces_key: MERGED_KEY,
        suggested_at: STAMP,
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("reuses the slot of an edited draft and writes null, never '', for what was left empty", async () => {
    const result = await saveSeAddressDraft(
      COMPANY,
      {
        intent: "save-draft",
        slot: "r20260901000000000",
        replacesKey: null,
        input: {
          careOf: "",
          streetLine: "Box 123",
          postalCode: "11123",
          city: "Stockholm",
          country: "SE",
          kind: "postal",
          note: "",
        },
      },
      NOW,
    );
    expect(result).toEqual({ decidedAt: STAMP, slot: "r20260901000000000" });
    const [row] = inserted(clickhouse.insertSuggestions);
    expect(row?.slot).toBe("r20260901000000000");
    expect(row?.care_of).toBeNull();
    expect(row?.note).toBeNull();
    expect(row?.replaces_key).toBeNull();
    expect(row?.country_code).toBe("SE");
    expect(row?.kind).toBe("postal");
  });

  it("activates a draft: the reviewer row and the cleared draft in one insert", async () => {
    const result = await activateSeAddressDraft(COMPANY, { intent: "activate", slot: DRAFT_SLOT, note: "checked" }, NOW);
    expect(result).toEqual({ decidedAt: STAMP });
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer",
        slot: DRAFT_SLOT,
        suggestion_id: REVIEWER_SUGGESTION_ID,
        source_record_uid: "",
        observed_at: STAMP,
        kind: "visiting",
        raw_address: null,
        care_of: "Anna Svensson",
        street_address: "Storgatan 7",
        postal_code: "11122",
        post_town: "Stockholm",
        county: null,
        country_code: "SE",
        decided_by: "backoffice",
        note: "checked",
        replaces_key: null,
        suggested_at: STAMP,
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        slot: DRAFT_SLOT,
        suggestion_id: DRAFT_SUGGESTION_ID,
        source_record_uid: "",
        observed_at: STAMP,
        kind: "visiting",
        raw_address: null,
        care_of: null,
        street_address: null,
        postal_code: null,
        post_town: null,
        county: null,
        country_code: null,
        decided_by: "backoffice",
        note: "activated",
        replaces_key: null,
        suggested_at: STAMP,
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
    // A Correct: the key the draft replaces is hidden in the same action.
    expect(clickhouse.insertRules).toHaveBeenCalledTimes(1);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY,
        address_key: MERGED_KEY,
        action: "hide",
        removed: 0,
        decided_by: "backoffice",
        note: "corrected by reviewer",
        decided_at: STAMP,
      },
    ]);
  });

  it("activates a plain draft without a hide rule", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_RAW_SQL ? [{ ...DRAFT_RAW, replaces_key: "" }] : answer(sql),
    );
    await activateSeAddressDraft(COMPANY, { intent: "activate", slot: DRAFT_SLOT, note: "" }, NOW);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)[0]?.note).toBeNull();
  });

  it("refuses to activate an unknown or empty draft", async () => {
    await expect(activateSeAddressDraft(COMPANY, { intent: "activate", slot: "r20260101000000000", note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("No draft to activate."),
    );
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_RAW_SQL ? [{ ...DRAFT_RAW, street_address: "" }] : answer(sql),
    );
    await expect(activateSeAddressDraft(COMPANY, { intent: "activate", slot: DRAFT_SLOT, note: "" }, NOW)).rejects.toThrow(
      SeAddressDecisionError,
    );
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("discards a draft by clearing every address column", async () => {
    const result = await discardSeAddressDraft(COMPANY, { intent: "discard", slot: DRAFT_SLOT }, NOW);
    expect(result).toEqual({ decidedAt: STAMP });
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        slot: DRAFT_SLOT,
        suggestion_id: DRAFT_SUGGESTION_ID,
        source_record_uid: "",
        observed_at: STAMP,
        kind: "visiting",
        raw_address: null,
        care_of: null,
        street_address: null,
        postal_code: null,
        post_town: null,
        county: null,
        country_code: null,
        decided_by: "backoffice",
        note: "discarded",
        replaces_key: null,
        suggested_at: STAMP,
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("refuses to discard a draft that is not there", async () => {
    await expect(discardSeAddressDraft(COMPANY, { intent: "discard", slot: "r20260101000000000" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("No draft to discard."),
    );
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("removes a source-delivered address with a hide rule alone", async () => {
    const result = await removeSeAddress(COMPANY, { intent: "remove", addressKey: MERGED_KEY, note: "they moved" }, NOW);
    expect(result).toEqual({ decidedAt: STAMP });
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY,
        address_key: MERGED_KEY,
        action: "hide",
        removed: 0,
        decided_by: "backoffice",
        note: "they moved",
        decided_at: STAMP,
      },
    ]);
  });

  it("removes a reviewer-typed address by tombstoning its slot, with no rule", async () => {
    const reviewerRow = main({
      address_key: REVIEWER_KEY,
      street_name: "Kungsgatan",
      house_number: "1",
      postal_code: "11143",
      city: "Stockholm",
      normalized_address: "Kungsgatan 1, 111 43 Stockholm",
      kinds: ["visiting"],
      sources: ["reviewer"],
      slots: ["rev1"],
      normalized_ids: ["n5"],
      text_source: "reviewer",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_MAIN_SQL ? [reviewerRow] : answer(sql),
    );
    await removeSeAddress(COMPANY, { intent: "remove", addressKey: REVIEWER_KEY, note: "" }, NOW);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer",
        slot: "rev1",
        suggestion_id: REVIEWER_SLOT_SUGGESTION_ID,
        source_record_uid: "",
        observed_at: STAMP,
        // The slot's own kind carries over: the column is not nullable.
        kind: "visiting",
        raw_address: null,
        care_of: null,
        street_address: null,
        postal_code: null,
        post_town: null,
        county: null,
        country_code: null,
        decided_by: "backoffice",
        note: "removed by reviewer",
        replaces_key: null,
        suggested_at: STAMP,
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("removes a mixed row with both writes: the source members would republish it otherwise", async () => {
    const mixedRow = main({
      ...MERGED_ROW,
      sources: ["scb", "reviewer"],
      slots: ["s1", "rev1"],
      normalized_ids: ["n1", "n5"],
      kinds: ["postal", "visiting"],
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_MAIN_SQL ? [mixedRow] : answer(sql),
    );
    await removeSeAddress(COMPANY, { intent: "remove", addressKey: MERGED_KEY, note: "" }, NOW);
    const tombstones = inserted(clickhouse.insertSuggestions);
    expect(tombstones).toHaveLength(1);
    expect(tombstones[0]?.source).toBe("reviewer");
    expect(tombstones[0]?.slot).toBe("rev1");
    expect(tombstones[0]?.street_address).toBeNull();
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY,
        address_key: MERGED_KEY,
        action: "hide",
        removed: 0,
        decided_by: "backoffice",
        // No note typed: the action says itself why the row is hidden.
        note: "removed by reviewer",
        decided_at: STAMP,
      },
    ]);
  });

  it("refuses to remove an unknown address or one that is already hidden", async () => {
    await expect(removeSeAddress(COMPANY, { intent: "remove", addressKey: UNKNOWN_KEY, note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("Unknown address."),
    );
    await expect(removeSeAddress(COMPANY, { intent: "remove", addressKey: "nope", note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("Unknown address."),
    );
    await expect(removeSeAddress(COMPANY, { intent: "remove", addressKey: BOX_KEY, note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("Already hidden."),
    );
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("resets a hidden address by releasing the rule", async () => {
    const result = await resetSeAddress(COMPANY, { intent: "reset", addressKey: BOX_KEY, note: "still their post box" }, NOW);
    expect(result).toEqual({ decidedAt: STAMP });
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY,
        address_key: BOX_KEY,
        action: "hide",
        removed: 1,
        decided_by: "backoffice",
        note: "reset to default: still their post box",
        decided_at: STAMP,
      },
    ]);
  });

  it("refuses a reset with no rule in force, released rules included", async () => {
    await expect(resetSeAddress(COMPANY, { intent: "reset", addressKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("No rule to reset."),
    );
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_RULES_SQL ? [{ ...BOX_HIDE_RULE, removed: 1 }] : answer(sql),
    );
    await expect(resetSeAddress(COMPANY, { intent: "reset", addressKey: BOX_KEY, note: "" }, NOW)).rejects.toThrow(
      new SeAddressDecisionError("No rule to reset."),
    );
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("names the default reset note when the reviewer typed none", async () => {
    await resetSeAddress(COMPANY, { intent: "reset", addressKey: BOX_KEY, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)[0]?.note).toBe("reset to default");
  });

  it("launches the targeted fold for this company alone", async () => {
    const launched = await launchSeAddressFold(COMPANY);
    expect(launched).toEqual({ runId: "run-9", url: null });
    expect(dagster.launchRun).toHaveBeenCalledWith({
      job: "__ASSET_JOB",
      assetSelection: ["se_company_address_fold_companies"],
      runConfig: { ops: { se_company_address_fold_companies: { config: { company_ids: [COMPANY] } } } },
      tags: { "backoffice/address": "fold-now" },
    });
  });
});
