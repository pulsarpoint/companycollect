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

import { workplaceQueryFromSearch } from "~/lib/se-address-fields";
import {
  activateSeAddressDraft,
  ADDRESS_DRAFT_NORMALIZED_SQL,
  ADDRESS_DRAFT_RAW_SQL,
  ADDRESS_FOLD_STATE_SQL,
  ADDRESS_HISTORY_SQL,
  ADDRESS_LIST_SQL,
  ADDRESS_MAIN_SQL,
  ADDRESS_MEMBER_NORMALIZED_SQL,
  ADDRESS_MEMBER_RAW_SQL,
  ADDRESS_NORMALIZED_SQL,
  ADDRESS_RAW_SQL,
  ADDRESS_RULES_SQL,
  ADDRESS_SELECTED_SQL,
  ADDRESS_WORKPLACES_COUNT_SQL,
  ADDRESS_WORKPLACES_SQL,
  discardSeAddressDraft,
  launchSeAddressFold,
  loadSeAddressDetail,
  removeSeAddress,
  resetSeAddress,
  saveSeAddressDraft,
  SeAddressDecisionError,
  type SeAddressDetailOptions,
  type SeAddressNormalizedRow,
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
/** An establishment row: `kinds` is EXACTLY `['workplace']`, so it belongs to
 * the Workplaces card and not to the Addresses card (migration 000403's split).
 * Its member's current normalized version (n5b) is not the one it was folded
 * from (n5), so ClickHouse marks the row re-fold pending. */
const WORKPLACE_KEY = "f".repeat(64);
const WORKPLACE_ROW = main({
  address_key: WORKPLACE_KEY,
  street_name: "Verkstadsgatan",
  house_number: "3",
  postal_code: "11124",
  city: "Stockholm",
  normalized_address: "Verkstadsgatan 3, 111 24 Stockholm",
  kinds: ["workplace"],
  sources: ["ratsit"],
  slots: ["est:9"],
  normalized_ids: ["n5"],
  text_source: "ratsit",
});
const WORKPLACE_NORMALIZED = normalized({
  source: "ratsit",
  slot: "est:9",
  normalized_id: "n5b",
  suggestion_id: "sid9",
  kind: "workplace",
  street_name: "Verkstadsgatan",
  house_number: "3",
  postal_code: "11124",
  city: "Stockholm",
  normalized_address: "Verkstadsgatan 3, 111 24 Stockholm",
  address_key: WORKPLACE_KEY,
});
const WORKPLACE_RAW = raw({
  source: "ratsit",
  slot: "est:9",
  suggestion_id: "sid9",
  kind: "workplace",
  street_address: "Verkstadsgatan 3",
  postal_code: "11124",
  post_town: "Stockholm",
  source_run_id: "run-ratsit",
  extractor_version: "ratsit-address-v2",
});

/** One row of the list / workplace queries: the published columns plus the
 * `refold_pending` UInt8 ClickHouse computes per row. */
function listRow(row: SeAddressRow, refoldPending: 0 | 1): Record<string, unknown> {
  return { ...row, refold_pending: refoldPending };
}

/** The one row `ADDRESS_FOLD_STATE_SQL` returns: the newest fold, the newest
 * non-draft normalized and raw stamps, whether any non-draft normalized row is
 * publishable, and how many normalized rows the company has at all. */
const FOLD_STATE = {
  folded_at: "2026-09-07 09:00:00.000",
  newest_normalized_at: "2026-09-07 10:00:00.000",
  has_publishable: 1,
  newest_suggested_at: "2026-09-06 07:00:00.000",
  normalized_rows: 4,
};
function foldState(over: Record<string, unknown>): Record<string, unknown> {
  return { ...FOLD_STATE, ...over };
}

function answer(sql: string, params?: Record<string, unknown>): unknown[] {
  // The tab's reads, each dispatched by identity: several of them select from
  // the same table, so a substring match would answer the wrong one.
  if (sql === ADDRESS_LIST_SQL) return [listRow(MERGED_ROW, 0), listRow(BOX_ROW, 0)];
  if (sql === ADDRESS_WORKPLACES_SQL) return [listRow(WORKPLACE_ROW, 1)];
  if (sql === ADDRESS_WORKPLACES_COUNT_SQL) return [{ total: 1 }];
  if (sql === ADDRESS_SELECTED_SQL) {
    const row = [MERGED_ROW, BOX_ROW, WORKPLACE_ROW].find(
      (candidate) => candidate.address_key === params?.addressKey,
    );
    return row === undefined ? [] : [row];
  }
  if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) {
    const wanted = new Set((params?.memberSlots as string[]) ?? []);
    return [SCB_NORMALIZED, BV_NORMALIZED, RATSIT_NORMALIZED, WORKPLACE_NORMALIZED].filter((row) =>
      wanted.has(row.slot),
    );
  }
  if (sql === ADDRESS_MEMBER_RAW_SQL) {
    const wanted = new Set((params?.memberSlots as string[]) ?? []);
    return [SCB_RAW, BV_RAW, RATSIT_RAW, WORKPLACE_RAW].filter((row) => wanted.has(row.slot));
  }
  if (sql === ADDRESS_DRAFT_RAW_SQL) return [DRAFT_RAW];
  if (sql === ADDRESS_DRAFT_NORMALIZED_SQL) return [DRAFT_NORMALIZED];
  if (sql === ADDRESS_FOLD_STATE_SQL) return [FOLD_STATE];
  if (sql === ADDRESS_HISTORY_SQL) return [HISTORY_ROW];
  if (sql === ADDRESS_RULES_SQL) return [BOX_HIDE_RULE];
  // The five reviewer writes still read the company whole.
  if (sql === ADDRESS_MAIN_SQL) return [MERGED_ROW, BOX_ROW, WORKPLACE_ROW];
  if (sql === ADDRESS_NORMALIZED_SQL) return NORMALIZED_ROWS;
  if (sql === ADDRESS_RAW_SQL) return RAW_ROWS;
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}

/** The loader's options, with the tab's defaults. */
const DEFAULT_OPTIONS: SeAddressDetailOptions = {
  selectedKey: null,
  workplacePage: 1,
  workplaceQuery: "",
};
function load(over: Partial<SeAddressDetailOptions> = {}) {
  return loadSeAddressDetail(COMPANY, { ...DEFAULT_OPTIONS, ...over });
}

/** Every `chQuery` call that ran this SQL, with the params it was given. */
function paramsOf(sql: string): Record<string, unknown> | undefined {
  return clickhouse.query.mock.calls.find(([text]) => text === sql)?.[1] as
    | Record<string, unknown>
    | undefined;
}
function ranSql(sql: string): boolean {
  return clickhouse.query.mock.calls.some(([text]) => text === sql);
}

/** The rows one `chInsertSeCompanyAddressSuggestions` / `...Rules` call got. */
function inserted(mock: { mock: { calls: unknown[][] } }, call = 0): Record<string, unknown>[] {
  return mock.mock.calls[call]?.[0] as Record<string, unknown>[];
}

describe("se-company-address-entity.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      answer(sql, params),
    );
    clickhouse.insertSuggestions.mockReset();
    clickhouse.insertRules.mockReset();
    dagster.launchRun.mockReset();
    dagster.launchRun.mockResolvedValue({ runId: "run-9", status: "STARTED" });
  });

  it("pins every read to a FINAL current version, the company parameter and string keys", () => {
    expect(ADDRESS_MAIN_SQL).toContain("FROM corpscout.se_company_address AS m FINAL");
    expect(ADDRESS_MAIN_SQL).not.toContain("se_company_address_v2");
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
    // Every nullable component reaches the page as '' -- never null.
    for (const column of ["care_of", "box", "street_name", "house_number", "unit", "postal_code", "city"]) {
      expect(ADDRESS_MAIN_SQL).toContain(`ifNull(m.${column}, '') AS ${column}`);
      expect(ADDRESS_NORMALIZED_SQL).toContain(`ifNull(n.${column}, '') AS ${column}`);
    }
    for (const column of ["raw_address", "care_of", "street_address", "postal_code", "post_town", "county", "country_code", "decided_by", "note"]) {
      expect(ADDRESS_RAW_SQL).toContain(`ifNull(s.${column}, '') AS ${column}`);
    }
  });

  it("pins the new reads to the workplace split, the filter, the page and one row's pairs", () => {
    // The list is everything EXCEPT an active workplace-only row -- the exact
    // predicate migration 000403 gave the serving view.
    expect(ADDRESS_LIST_SQL).toContain("FROM corpscout.se_company_address AS m FINAL");
    expect(ADDRESS_LIST_SQL).toContain("WHERE m.company_id = {companyId:String}");
    expect(ADDRESS_LIST_SQL).toContain("AND NOT (m.active = 1 AND m.kinds = ['workplace'])");
    expect(ADDRESS_LIST_SQL).toContain("AS refold_pending");
    // The re-fold mark is computed in ClickHouse against the company's current
    // normalized versions: pending only when the slot HAS a current version and
    // it is not the one the row was folded from.
    expect(ADDRESS_LIST_SQL).toContain("groupArray(concat(toString(n.source), '\\n', n.slot))");
    expect(ADDRESS_LIST_SQL).toContain("has(current_pairs,");
    expect(ADDRESS_LIST_SQL).toContain("AND NOT has(current_triples,");

    // The workplaces are the other half of the same split, paged and filtered.
    expect(ADDRESS_WORKPLACES_SQL).toContain("AND m.active = 1");
    expect(ADDRESS_WORKPLACES_SQL).toContain("AND m.kinds = ['workplace']");
    expect(ADDRESS_WORKPLACES_SQL).toContain(
      "positionCaseInsensitiveUTF8(m.normalized_address, {workplaceQuery:String}) > 0",
    );
    expect(ADDRESS_WORKPLACES_SQL).toContain("ORDER BY m.normalized_address, m.address_key");
    expect(ADDRESS_WORKPLACES_SQL).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
    // The count carries the same WHERE, and comes back as a NUMBER: a bare
    // count() is UInt64, which ClickHouse quotes as a string in JSONEachRow.
    expect(ADDRESS_WORKPLACES_COUNT_SQL).toContain("SELECT toUInt32(count()) AS total");
    expect(ADDRESS_WORKPLACES_COUNT_SQL).toContain("AND m.kinds = ['workplace']");
    expect(ADDRESS_WORKPLACES_COUNT_SQL).not.toContain("LIMIT");

    // The members of ONE row, by the pairs that row carries.
    for (const sql of [ADDRESS_MEMBER_NORMALIZED_SQL, ADDRESS_MEMBER_RAW_SQL]) {
      expect(sql).toContain(
        "has(arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)}),",
      );
    }
    expect(ADDRESS_SELECTED_SQL).toContain("toString(m.address_key) = {addressKey:String}");
    expect(ADDRESS_SELECTED_SQL).toContain("LIMIT 1");
    // Drafts stand on their own, filtered in SQL.
    expect(ADDRESS_DRAFT_RAW_SQL).toContain("AND s.source = 'reviewer_draft'");
    expect(ADDRESS_DRAFT_NORMALIZED_SQL).toContain("AND n.source = 'reviewer_draft'");
    // Fold state: five scalar aggregates, drafts excluded from the stamps.
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS folded_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS newest_normalized_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS has_publishable");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS newest_suggested_at");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("AS normalized_rows");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("n.source != 'reviewer_draft'");
    expect(ADDRESS_FOLD_STATE_SQL).toContain("s.source != 'reviewer_draft'");
  });

  it("returns slim list rows -- no members anywhere but the selected row", async () => {
    const detail = await load();
    expect(detail?.published).toEqual([
      { row: MERGED_ROW, refoldPending: false, hideRule: null },
      { row: BOX_ROW, refoldPending: false, hideRule: BOX_HIDE_RULE },
    ]);
    expect(detail?.published[0]).not.toHaveProperty("members");
    expect(detail?.workplaces).toEqual({
      rows: [{ row: WORKPLACE_ROW, refoldPending: true, hideRule: null }],
      total: 1,
      page: 1,
      pageSize: 50,
      query: "",
    });
    expect(detail?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: DRAFT_NORMALIZED, replacesKey: MERGED_KEY },
    ]);
    expect(detail?.history).toEqual([HISTORY_ROW]);
    expect(detail?.rules).toEqual([BOX_HIDE_RULE]);
    // The whole-company reads are what made the kommun 7.8 MB. The tab's
    // loader must not run any of them any more.
    expect(ranSql(ADDRESS_NORMALIZED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_RAW_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MAIN_SQL)).toBe(false);
    expect(paramsOf(ADDRESS_LIST_SQL)).toEqual({ companyId: COMPANY });
    expect(clickhouse.query.mock.calls.some(([text]) =>
      String(text).includes("se_company_address_precedence"),
    )).toBe(false);
  });

  it("resolves the selected row's members from that row's own (source, slot) pairs", async () => {
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(paramsOf(ADDRESS_SELECTED_SQL)).toEqual({
      companyId: COMPANY,
      addressKey: MERGED_KEY,
    });
    // Two parallel Array(String) parameters, zipped back into pairs in
    // ClickHouse: the merged row's two members and nobody else's.
    const memberParams = { companyId: COMPANY, memberSources: ["scb", "bolagsverket"], memberSlots: ["s1", "b1"] };
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual(memberParams);
    expect(paramsOf(ADDRESS_MEMBER_RAW_SQL)).toEqual(memberParams);
    expect(detail?.selected?.row).toEqual(MERGED_ROW);
    expect(detail?.selected?.members).toEqual([
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
    expect(detail?.selected?.textSourceReason).toBe("most complete");
    expect(detail?.selected?.hideRule).toBeNull();
  });

  it("selects a workplace row exactly as it selects a company address", async () => {
    const detail = await load({ selectedKey: WORKPLACE_KEY });
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["ratsit"],
      memberSlots: ["est:9"],
    });
    expect(detail?.selected?.row).toEqual(WORKPLACE_ROW);
    expect(detail?.selected?.members).toEqual([
      {
        source: "ratsit",
        slot: "est:9",
        normalizedId: "n5",
        current: WORKPLACE_NORMALIZED,
        raw: WORKPLACE_RAW,
        refoldPending: true,
        completeness: 4,
      },
    ]);
    expect(detail?.selected?.textSourceReason).toBe("single source");
  });

  it("falls back to the first active company address for no key, a foreign key and a malformed one", async () => {
    // Owner ruling 2026-09-13: the tab keeps today's default. With no
    // `?address=` the panel describes the first active row of the Addresses
    // card -- `ADDRESS_LIST_SQL` sorts `active DESC` first, so that is the
    // first active entry of the list already read -- and its members load
    // exactly as they would for an explicit key. No extra row read: the row is
    // already in hand.
    const none = await load();
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(none?.selected?.row).toEqual(MERGED_ROW);
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "bolagsverket"],
      memberSlots: ["s1", "b1"],
    });
    expect(paramsOf(ADDRESS_MEMBER_RAW_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "bolagsverket"],
      memberSlots: ["s1", "b1"],
    });
    expect(none?.selected?.members).toHaveLength(2);

    // A well-formed key that is not this company's: the row read comes back
    // empty, and the fallback takes over rather than leaving the panel blank.
    clickhouse.query.mockClear();
    const foreign = await load({ selectedKey: UNKNOWN_KEY });
    expect(paramsOf(ADDRESS_SELECTED_SQL)).toEqual({ companyId: COMPANY, addressKey: UNKNOWN_KEY });
    expect(foreign?.selected?.row).toEqual(MERGED_ROW);

    // A malformed key never reaches ClickHouse at all, and falls back too.
    clickhouse.query.mockClear();
    const malformed = await load({ selectedKey: "not-a-key" });
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(malformed?.selected?.row).toEqual(MERGED_ROW);
  });

  it("selects nothing when the company has no active company address", async () => {
    // A workplace-only company (or one whose every company address is
    // withdrawn): there is no first active row to fall back to, so the panel
    // says nothing is published and no member read runs at all.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_LIST_SQL ? [listRow(BOX_ROW, 0)] : answer(sql, params),
    );
    const detail = await load();
    expect(detail?.published).toEqual([{ row: BOX_ROW, refoldPending: false, hideRule: BOX_HIDE_RULE }]);
    expect(detail?.selected).toBeNull();
    expect(ranSql(ADDRESS_SELECTED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MEMBER_NORMALIZED_SQL)).toBe(false);
    expect(ranSql(ADDRESS_MEMBER_RAW_SQL)).toBe(false);
  });

  it("binds the workplace page, its offset and its filter, and echoes them back", async () => {
    const detail = await load({ workplacePage: 3, workplaceQuery: "storgatan" });
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "storgatan",
      limit: 50,
      offset: 100,
    });
    expect(paramsOf(ADDRESS_WORKPLACES_COUNT_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "storgatan",
    });
    expect(detail?.workplaces.page).toBe(3);
    expect(detail?.workplaces.pageSize).toBe(50);
    expect(detail?.workplaces.query).toBe("storgatan");
    expect(detail?.workplaces.total).toBe(1);

    // Page 1 is offset 0, and the count is what `total` reports.
    clickhouse.query.mockClear();
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_WORKPLACES_COUNT_SQL ? [{ total: 1502 }] : answer(sql, params),
    );
    const first = await load();
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "",
      limit: 50,
      offset: 0,
    });
    expect(first?.workplaces.total).toBe(1502);
  });

  it("binds a page past the end without throwing: MAX_WORKPLACE_PAGE's own offset fits UInt32", async () => {
    // workplacePageFromSearch clamps a hand-typed page to MAX_WORKPLACE_PAGE
    // (100,000) before it ever reaches here; this is the loader's own half of
    // that guarantee -- the clamp's own ceiling must not overflow the
    // {offset:UInt32} it binds, and a page with no rows must not throw.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_WORKPLACES_SQL
        ? []
        : sql === ADDRESS_WORKPLACES_COUNT_SQL
          ? [{ total: 1 }]
          : answer(sql, params),
    );
    const detail = await load({ workplacePage: 100_000 });
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)).toEqual({
      companyId: COMPANY,
      workplaceQuery: "",
      limit: 50,
      offset: 4_999_950,
    });
    expect(detail?.workplaces).toEqual({
      rows: [],
      total: 1,
      page: 100_000,
      pageSize: 50,
      query: "",
    });
  });

  it("binds a filter with SQL wildcard characters verbatim, and a whitespace-only one as ''", async () => {
    // positionCaseInsensitiveUTF8 takes its needle as a literal, so `%` and `_`
    // must reach ClickHouse exactly as typed -- there is nothing to escape.
    await load({ workplaceQuery: "10%_off" });
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)?.workplaceQuery).toBe("10%_off");
    expect(paramsOf(ADDRESS_WORKPLACES_COUNT_SQL)?.workplaceQuery).toBe("10%_off");

    // workplaceQueryFromSearch is what trims a whitespace-only ?workplace_q=
    // before the route ever calls the loader; what it produces is what must
    // bind -- '', which is also what turns the filter off in WORKPLACE_WHERE_SQL.
    clickhouse.query.mockClear();
    const trimmed = workplaceQueryFromSearch(new URLSearchParams("workplace_q=+++"));
    expect(trimmed).toBe("");
    await load({ workplaceQuery: trimmed });
    expect(paramsOf(ADDRESS_WORKPLACES_SQL)?.workplaceQuery).toBe("");
    expect(paramsOf(ADDRESS_WORKPLACES_COUNT_SQL)?.workplaceQuery).toBe("");
  });

  it("calls the text source a tie-break when another member is just as complete", async () => {
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) return [{ ...SCB_NORMALIZED, unit: "" }, BV_NORMALIZED];
      return answer(sql, params);
    });
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(detail?.selected?.members.map((member) => member.completeness)).toEqual([4, 4]);
    expect(detail?.selected?.textSourceReason).toBe("tie-break");
  });

  it("attributes the published text to the most complete member of the text source", async () => {
    // SCB and Bolagsverket both write slot '' (ratsit writes 'company'), so one
    // source can hold two members; the text came from the fuller of them.
    const twoScb = main({
      ...MERGED_ROW,
      sources: ["scb", "scb"],
      slots: ["", "x"],
      normalized_ids: ["n6", "n7"],
      text_source: "scb",
    });
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_SELECTED_SQL) return [twoScb];
      if (sql === ADDRESS_MEMBER_NORMALIZED_SQL) {
        return [
          // The first member is the thinner one: picking it would read as a tie.
          normalized({ slot: "", normalized_id: "n6", postal_code: "11122", city: "Stockholm" }),
          normalized({ ...SCB_NORMALIZED, slot: "x", normalized_id: "n7" }),
        ];
      }
      if (sql === ADDRESS_MEMBER_RAW_SQL) return [];
      return answer(sql, params);
    });
    const detail = await load({ selectedKey: MERGED_KEY });
    expect(paramsOf(ADDRESS_MEMBER_NORMALIZED_SQL)).toEqual({
      companyId: COMPANY,
      memberSources: ["scb", "scb"],
      memberSlots: ["", "x"],
    });
    expect(detail?.selected?.members.map((member) => member.completeness)).toEqual([2, 5]);
    expect(detail?.selected?.textSourceReason).toBe("most complete");
  });

  it("reads fold-pending from the fold-state row, with the drafts already excluded", async () => {
    // Newest non-draft normalized stamp (10:00) is newer than the fold (09:00).
    expect((await load())?.foldPending).toBe(true);

    const state = (over: Record<string, unknown>) => async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_FOLD_STATE_SQL ? [foldState(over)] : answer(sql, params);

    // Every non-draft stamp older than the fold: settled. The draft's own
    // 21:00 normalized_at is not in the fold state at all (the SQL excludes
    // `reviewer_draft`), so it cannot raise the flag.
    clickhouse.query.mockImplementation(
      state({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "2026-09-06 07:00:00.000" }),
    );
    expect((await load())?.foldPending).toBe(false);

    // A rule newer than the fold counts, released or not: the release is not
    // applied until the fold runs.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_FOLD_STATE_SQL) {
        return [foldState({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "" })];
      }
      if (sql === ADDRESS_RULES_SQL) {
        return [{ ...BOX_HIDE_RULE, removed: 1, decided_at: "2026-09-07 12:00:00.000" }];
      }
      return answer(sql, params);
    });
    expect((await load())?.foldPending).toBe(true);

    // A reviewer raw row the normalize step has not seen yet -- what an
    // Activate or a Remove tombstone leaves -- speaks through its own stamp.
    clickhouse.query.mockImplementation(
      state({ newest_normalized_at: "2026-09-07 08:00:00.000", newest_suggested_at: "2026-09-07 20:33:55.123" }),
    );
    expect((await load())?.foldPending).toBe(true);

    // Nothing folded yet, but a publishable normalized row exists.
    clickhouse.query.mockImplementation(
      state({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 1 }),
    );
    expect((await load())?.foldPending).toBe(true);

    // Nothing folded and nothing publishable (every row parses `no_address`):
    // no fold is owed.
    clickhouse.query.mockImplementation(
      state({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0 }),
    );
    expect((await load())?.foldPending).toBe(false);
  });

  it("returns null only when there is no list row, no workplace, no normalized row and no draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_WORKPLACES_COUNT_SQL
        ? [{ total: 0 }]
        : sql === ADDRESS_FOLD_STATE_SQL
          ? [foldState({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0, normalized_rows: 0 })]
          : [],
    );
    expect(await load()).toBeNull();

    // A typed draft alone is enough to open the tab.
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === ADDRESS_DRAFT_RAW_SQL) return [DRAFT_RAW];
      if (sql === ADDRESS_WORKPLACES_COUNT_SQL) return [{ total: 0 }];
      if (sql === ADDRESS_FOLD_STATE_SQL) {
        return [foldState({ folded_at: "", newest_normalized_at: "", newest_suggested_at: "", has_publishable: 0, normalized_rows: 0 })];
      }
      return [];
    });
    const draftOnly = await load();
    expect(draftOnly?.drafts).toEqual([
      { slot: DRAFT_SLOT, raw: DRAFT_RAW, normalized: null, replacesKey: MERGED_KEY },
    ]);
    expect(draftOnly?.foldPending).toBe(false);

    // A company whose only rows are workplaces has an empty Addresses card and
    // is still very much a company with addresses.
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) =>
      sql === ADDRESS_LIST_SQL ? [] : answer(sql, params),
    );
    const workplacesOnly = await load();
    expect(workplacesOnly?.published).toEqual([]);
    expect(workplacesOnly?.workplaces.total).toBe(1);
  });

  it("ignores a cleared reviewer_draft row: it is a tombstone, not a draft", async () => {
    clickhouse.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql === ADDRESS_DRAFT_RAW_SQL) {
        return [{ ...DRAFT_RAW, care_of: "", street_address: "", postal_code: "", post_town: "", note: "discarded" }];
      }
      return answer(sql, params);
    });
    expect((await load())?.drafts).toEqual([]);
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

  it("writes the country the reviewer typed, not a constant", async () => {
    // Task 2's validation is what limits the sheet to SE today; the row must
    // carry whatever came through it, so opening a second country needs no
    // change here.
    await saveSeAddressDraft(
      COMPANY,
      {
        intent: "save-draft",
        slot: null,
        replacesKey: null,
        input: {
          careOf: "",
          streetLine: "Karl Johans gate 1",
          postalCode: "01154",
          city: "Oslo",
          country: "NO",
          kind: "postal",
          note: "",
        },
      },
      NOW,
    );
    expect(inserted(clickhouse.insertSuggestions)[0]?.country_code).toBe("NO");
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

  it("writes no hide rule when the draft parses back to the very key it replaces", async () => {
    // The reviewer retyped the address the sources already deliver: the fold
    // publishes the same key, and a rule would hide the reviewer's own row.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_NORMALIZED_SQL
        ? [...NORMALIZED_ROWS.filter((row) => row !== DRAFT_NORMALIZED), { ...DRAFT_NORMALIZED, address_key: MERGED_KEY }]
        : answer(sql),
    );
    await activateSeAddressDraft(COMPANY, { intent: "activate", slot: DRAFT_SLOT, note: "" }, NOW);
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("writes the hide rule for a draft nothing has parsed yet: the fold decides, Reset undoes", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_NORMALIZED_SQL ? NORMALIZED_ROWS.filter((row) => row !== DRAFT_NORMALIZED) : answer(sql),
    );
    await activateSeAddressDraft(COMPANY, { intent: "activate", slot: DRAFT_SLOT, note: "" }, NOW);
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

  it("removes a reviewer-only address by tombstoning every one of its slots, with no rule", async () => {
    const reviewerRow = main({
      address_key: REVIEWER_KEY,
      street_name: "Kungsgatan",
      house_number: "1",
      postal_code: "11143",
      city: "Stockholm",
      normalized_address: "Kungsgatan 1, 111 43 Stockholm",
      kinds: ["visiting"],
      sources: ["reviewer", "reviewer"],
      slots: ["rev1", "rev2"],
      normalized_ids: ["n5", "n6"],
      text_source: "reviewer",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === ADDRESS_MAIN_SQL ? [reviewerRow] : answer(sql),
    );
    await removeSeAddress(COMPANY, { intent: "remove", addressKey: REVIEWER_KEY, note: "" }, NOW);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    const tombstones = inserted(clickhouse.insertSuggestions);
    expect(tombstones).toHaveLength(2);
    // The second slot has no current raw row, so its kind falls back.
    expect(tombstones[1]).toMatchObject({
      source: "reviewer",
      slot: "rev2",
      kind: "unknown",
      street_address: null,
      country_code: null,
      note: "removed by reviewer",
    });
    expect(tombstones.slice(0, 1)).toEqual([
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

  it("removes a mixed row with the rule alone: tombstoning a member would re-key the address", async () => {
    // The published key is the union of the members' components, so retiring
    // the reviewer member would shrink the union, change the key and leave the
    // hide rule pointing at an address nothing publishes any more.
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
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
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
