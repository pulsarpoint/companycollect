import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  query: vi.fn(),
  insertSuggestions: vi.fn(),
  insertRules: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeCompanyPersonSuggestions: clickhouse.insertSuggestions,
  chInsertSeCompanyPersonRules: clickhouse.insertRules,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => null) }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  dagsterRunUrl: dagster.dagsterRunUrl,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET: "se_company_person_fold_companies",
}));
const roles = vi.hoisted(() => ({ getCompanyPersonRoleTypes: vi.fn() }));
vi.mock("~/lib/company-roles.server", () => roles);

import {
  activateSePersonDraft,
  discardSePersonDraft,
  launchSePersonFold,
  loadSePersonDetail,
  loadSePersonRoleOptions,
  mergeSePersons,
  PERSON_HISTORY_SQL,
  PERSON_MAIN_SQL,
  PERSON_MATCH_SQL,
  PERSON_NORMALIZED_SQL,
  PERSON_PRECEDENCE_SQL,
  PERSON_RAW_SQL,
  PERSON_ROLE_SQL,
  PERSON_RULES_SQL,
  removeSePerson,
  resetSePersonRules,
  saveSePersonDraft,
  SePersonDecisionError,
  splitSePersonSlots,
  type SePersonNormalizedRow,
  type SePersonRawRow,
  type SePersonRoleRow,
  type SePersonRow,
  type SePersonRuleRow,
} from "~/lib/se-company-person-entity.server";
import type { SePersonMatch, SePersonMatchRow } from "~/lib/se-person-match";

const COMPANY = "5560000001";
const MERGED_KEY = "a".repeat(64);
const WIKI_KEY = "b".repeat(64);
/** The key the merged person published under before a fuller spelling re-keyed it. */
const OLD_KEY = "c".repeat(64);
const UNKNOWN_KEY = "e".repeat(64);
const NOW = new Date("2026-09-10T12:00:00.123Z");
const STAMP = "2026-09-10 12:00:00.123";
const GROUP = "r20260910120000123";
/** sha256("<company>\n<source>\n<slot>\n<stamp>"), computed outside the module so a
 * changed preimage fails here instead of agreeing with itself. */
const DRAFT_ID_1 = "e5d1712bee6930abe9769b83f408082a074c412204ebfe089fb1211869e223ec";
const DRAFT_ID_2 = "5c806db8df550ccd76e3bdd3a350d87e559ca24218601a723204db1c1ba98bff";
const REVIEWER_ID_1 = "63d6b632d69f4e0eebecbec6b020eaf48360bafc5e5e5d1c03b9aee7aa954e2c";
const REVIEWER_ID_2 = "b1c15bf09aacdf64b49b819950b9f935fad537aeb62608cf1b32d71a22e23cc7";
/** sha256("<company>\n<kind>\n<sorted keys, comma-joined>\n<sorted slots, comma-joined>\n<stamp>"). */
const HIDE_RULE_ID = "06cb08340aba69721e3f4e916b79bf183495e26dc3d745082c35846b34f35da7";
const MERGE_RULE_ID = "e8c13b618f94caab5d684d67021a88b05f89ed5847f545a53f5a0c6c529e5d4e";
const SPLIT_RULE_ID = "ffb849959a8aaaca818fc0c815eeec07f7694162488be51b6bb09137e2591ffc";

function main(over: Partial<SePersonRow>): SePersonRow {
  return {
    company_id: COMPANY, person_key: MERGED_KEY,
    display_name: "Anna Svensson", first_name: "Anna", last_name: "Svensson",
    birth_year: "1975", wikidata_id: "",
    sources: ["bolagsverket"], slots: ["uid-1:sig-1"], normalized_ids: ["n1"],
    member_sources: ["bolagsverket"], member_slots: ["uid-1:sig-1"],
    member_names: ["Anna Svensson"], member_birth_years: ["1975"],
    member_wikidata_ids: [""], member_data: ['{"role_kind":"board_member"}'],
    role_codes: ["board_member"], role_years: [2025], role_sources: [["bolagsverket"]],
    current_roles: ["board_member"], first_year: "2025", last_year: "2025",
    text_source: "bolagsverket", data: '{"role_kind":"board_member"}',
    active: 1, inactive_reason: "",
    folded_at: "2026-09-10 09:00:00.000", fold_version: "se-person-fold-v1",
    source_run_id: "run-fold",
    ...over,
  };
}
function normalized(over: Partial<SePersonNormalizedRow>): SePersonNormalizedRow {
  return {
    company_id: COMPANY, source: "bolagsverket", slot: "uid-1:sig-1",
    suggestion_id: "sid1", normalized_id: "n1", normalizer_version: "se-person-normalizer-v1",
    parse_status: "ok", parse_notes: [],
    first_tokens: ["anna"], middle_tokens: [], last_tokens: ["svensson"],
    display_first: "Anna", display_last: "Svensson", display_name: "Anna Svensson",
    birth_year: "1975", wikidata_id: "", role_code: "board_member", role_key: "board_member",
    role_year: "2025", role_from: "", role_to: "", data: '{"role_kind":"board_member"}',
    normalized_at: "2026-09-10 08:00:00.000",
    ...over,
  };
}
function raw(over: Partial<SePersonRawRow>): SePersonRawRow {
  return {
    company_id: COMPANY, source: "bolagsverket", slot: "uid-1:sig-1", suggestion_id: "sid1",
    suggested_at: "2026-09-10 07:00:00.000", source_record_id: "uid-1",
    full_name: "", first_name: "Anna", last_name: "Svensson", birth_year: "1975",
    wikidata_id: "", role_original: "Styrelseledamot", role_key: "board_member",
    fiscal_year: "2025", role_from: "", role_to: "", document_ref: "doc-1",
    data: '{"role_kind":"board_member"}',
    ...over,
  };
}

// The merged person: Bolagsverket's split spelling plus an ESEF full name whose
// current normalized version (n2b) is newer than the one the row was folded from (n2).
const ESEF_NORMALIZED = normalized({
  source: "esef", slot: "doc-9:cand-1", normalized_id: "n2b", suggestion_id: "sid2",
  first_tokens: ["anna"], middle_tokens: ["maria"], last_tokens: ["svensson"],
  display_first: "Anna Maria", display_last: "Svensson", display_name: "Anna Maria Svensson",
  role_code: "board_chair", role_key: "board_chair", role_year: "2025",
  data: '{"title":"Chair"}', normalized_at: "2026-09-10 10:00:00.000",
});
const WIKI_NORMALIZED = normalized({
  source: "wikidata", slot: "Q1:P169:Q7", normalized_id: "n3", suggestion_id: "sid3",
  first_tokens: ["carl"], middle_tokens: [], last_tokens: ["von", "essen"],
  display_first: "Carl", display_last: "von Essen", display_name: "Carl von Essen",
  birth_year: "", wikidata_id: "Q7", role_code: "chief_executive_officer",
  role_key: "P169", role_year: "", role_from: "2019-05-01", role_to: "",
  data: '{"description":"executive"}', normalized_at: "2026-09-10 08:00:00.000",
});
const DRAFT_NORMALIZED = normalized({
  source: "reviewer_draft", slot: `${GROUP}01`, normalized_id: "n4", suggestion_id: DRAFT_ID_1,
  role_code: "board_member", role_key: "board_member", role_year: "2024",
  data: '{"decided_by":"backoffice","note":"seen in the annual report"}',
  normalized_at: "2026-09-10 11:00:00.000",
});
const NORMALIZED_ROWS = [normalized({}), ESEF_NORMALIZED, WIKI_NORMALIZED, DRAFT_NORMALIZED];

const ESEF_RAW = raw({
  source: "esef", slot: "doc-9:cand-1", suggestion_id: "sid2", source_record_id: "doc-9",
  full_name: "Anna Maria Svensson", first_name: "", last_name: "", birth_year: "",
  role_original: "Chair of the board", role_key: "board_chair", fiscal_year: "2025",
  document_ref: "doc-9", data: '{"title":"Chair"}',
});
const WIKI_RAW = raw({
  source: "wikidata", slot: "Q1:P169:Q7", suggestion_id: "sid3", source_record_id: "Q7",
  full_name: "Carl von Essen", first_name: "", last_name: "", birth_year: "",
  wikidata_id: "Q7", role_original: "chief executive officer", role_key: "P169",
  fiscal_year: "", role_from: "2019-05-01", document_ref: "", data: '{"description":"executive"}',
});
/** A Correct in progress: the draft's data carries Ruling 4's reserved keys. */
const DRAFT_RAW_1 = raw({
  source: "reviewer_draft", slot: `${GROUP}01`, suggestion_id: DRAFT_ID_1,
  suggested_at: STAMP, source_record_id: "", first_name: "Anna", last_name: "Svensson",
  birth_year: "1975", role_original: "board_member", role_key: "board_member",
  fiscal_year: "2024", document_ref: "",
  data: `{"decided_by":"backoffice","note":"seen in the annual report","replaces_key":"${MERGED_KEY}"}`,
});
const DRAFT_RAW_2 = raw({
  ...DRAFT_RAW_1, slot: `${GROUP}02`, suggestion_id: DRAFT_ID_2,
  role_original: "board_chair", role_key: "board_chair", fiscal_year: "", role_from: "2025-01-01",
});
const RAW_ROWS = [raw({}), ESEF_RAW, WIKI_RAW, DRAFT_RAW_1, DRAFT_RAW_2];

const MERGED_ROW = main({
  sources: ["bolagsverket", "esef"], slots: ["uid-1:sig-1", "doc-9:cand-1"],
  normalized_ids: ["n1", "n2"],
  member_sources: ["bolagsverket", "esef"], member_slots: ["uid-1:sig-1", "doc-9:cand-1"],
  member_names: ["Anna Svensson", "Anna Maria Svensson"], member_birth_years: ["1975", ""],
  member_wikidata_ids: ["", ""],
  member_data: ['{"role_kind":"board_member"}', '{"title":"Chair"}'],
  role_codes: ["board_chair", "board_member"], role_years: [2025, 2025],
  role_sources: [["esef"], ["bolagsverket"]], current_roles: ["board_chair", "board_member"],
  data: '{"role_kind":"board_member","title":"Chair"}',
});
const WIKI_ROW = main({
  person_key: WIKI_KEY, display_name: "Carl von Essen", first_name: "Carl", last_name: "von Essen",
  birth_year: "", wikidata_id: "Q7",
  sources: ["wikidata"], slots: ["Q1:P169:Q7"], normalized_ids: ["n3"],
  member_sources: ["wikidata"], member_slots: ["Q1:P169:Q7"], member_names: ["Carl von Essen"],
  member_birth_years: [""], member_wikidata_ids: ["Q7"],
  member_data: ['{"description":"executive"}'],
  role_codes: ["chief_executive_officer"], role_years: [2026],
  role_sources: [["wikidata"]], current_roles: ["chief_executive_officer"],
  first_year: "2019", last_year: "2026", text_source: "wikidata",
  data: '{"description":"executive"}',
});
const HISTORY_ROW = {
  ...MERGED_ROW, folded_at: "2026-09-09 09:00:00.000",
  changed_at: "2026-09-10 09:00:00.000", change_kind: "updated", fold_run_id: "run-fold",
};
const MERGE_RULE: SePersonRuleRow = {
  company_id: COMPANY, rule_id: "f".repeat(64), kind: "merge",
  person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 1,
  note: "same person", created_at: "2026-09-09 12:00:00.000", created_by: "backoffice",
};
const PRECEDENCE_ROWS = [
  { company_id: "", field: "name", source: "reviewer", precedence: 20000, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
  { company_id: "", field: "name", source: "bolagsverket", precedence: 900, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
  { company_id: "", field: "name", source: "esef", precedence: 400, removed: 0, decided_by: "dagster", note: "", decided_at: "2026-09-10 06:00:00.000" },
];
/** The model's answer for this company, as PERSON_MATCH_SQL delivers it. n1 and n2 are
 * the merged person's two observations, n3 is the Wikidata person's one. */
const MATCH_ROWS: SePersonMatchRow[] = [
  {
    company_id: COMPANY, candidate_a: "n1", candidate_b: "n2",
    members_a: ["n1"], members_b: ["n2"],
    name_a: "Anna Svensson", name_b: "Anna Maria Svensson",
    confidence: 0.93, reason: "call name",
  },
  {
    company_id: COMPANY, candidate_a: "n1", candidate_b: "n3",
    members_a: ["n1"], members_b: ["n3"],
    name_a: "Anna Svensson", name_b: "Carl von Essen",
    confidence: 0.72, reason: "same household",
  },
  {
    company_id: COMPANY, candidate_a: "n2", candidate_b: "n3",
    members_a: ["n2"], members_b: ["n3"],
    name_a: "Anna Maria Svensson", name_b: "Carl von Essen",
    confidence: 0.64, reason: "same surname",
  },
  {
    company_id: COMPANY, candidate_a: "n3", candidate_b: "zz",
    members_a: ["n3"], members_b: ["zz"],
    name_a: "Carl von Essen", name_b: "Carla Essen",
    confidence: 0.66, reason: "an observation no published person holds",
  },
];
/** The one pair at or above the threshold, as the loader derives it. */
const CALL_NAME_MATCH: SePersonMatch = {
  nameA: "Anna Svensson", nameB: "Anna Maria Svensson", confidence: 0.93,
  reason: "call name", members: ["n1", "n2"],
};

/** The roles view's rows for this company (spec section 11), in the view's own order:
 * the merged person's two current roles and one she no longer holds, then the Wikidata
 * person's DATELESS role, which the view publishes under year 0. */
const ROLE_ROWS: SePersonRoleRow[] = [
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "board_chair", role_year: 2025,
    role_from: "", role_to: "", source: "esef", slot: "doc-9:cand-1", normalized_id: "n2",
    is_current: 1, folded_at: "2026-09-10 09:00:00.000",
  },
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "board_member", role_year: 2025,
    role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-1",
    normalized_id: "n1", is_current: 1, folded_at: "2026-09-10 09:00:00.000",
  },
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "auditor", role_year: 2019,
    role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-4",
    normalized_id: "n5", is_current: 0, folded_at: "2026-09-10 09:00:00.000",
  },
  {
    company_id: COMPANY, person_key: WIKI_KEY, role_code: "chief_executive_officer",
    role_year: 0, role_from: "2019-05-01", role_to: "", source: "wikidata",
    slot: "Q1:P169:Q7", normalized_id: "n3", is_current: 1,
    folded_at: "2026-09-10 09:00:00.000",
  },
];

function answer(sql: string): unknown[] {
  if (sql.includes("FROM corpscout.se_company_person AS m FINAL")) return [MERGED_ROW, WIKI_ROW];
  if (sql.includes("FROM corpscout.se_company_person_role AS r")) return ROLE_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_history")) return [HISTORY_ROW];
  if (sql.includes("FROM corpscout.se_company_person_normalized")) return NORMALIZED_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_suggestion")) return RAW_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_rule")) return [MERGE_RULE];
  if (sql.includes("FROM corpscout.se_company_person_precedence")) return PRECEDENCE_ROWS;
  if (sql.includes("FROM corpscout.se_company_person_match AS p FINAL")) return MATCH_ROWS;
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}
/** The rows one insert call got. */
function inserted(mock: { mock: { calls: unknown[][] } }, call = 0): Record<string, unknown>[] {
  return mock.mock.calls[call]?.[0] as Record<string, unknown>[];
}

describe("se-company-person-entity.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => answer(sql));
    clickhouse.insertSuggestions.mockReset();
    clickhouse.insertRules.mockReset();
    dagster.launchRun.mockReset().mockResolvedValue({ runId: "run-9", status: "STARTED" });
    roles.getCompanyPersonRoleTypes.mockReset().mockResolvedValue([
      { role_code: "board_member", display_name: "Board member", role_group: "governance", description: "", is_active: 1, created_at: "", updated_at: "" },
      { role_code: "retired_code", display_name: "Retired", role_group: "governance", description: "", is_active: 0, created_at: "", updated_at: "" },
    ]);
  });

  it("pins every read to the entity's tables, FINAL where a current version is needed, the company parameter and string keys", () => {
    expect(PERSON_MAIN_SQL).toContain("FROM corpscout.se_company_person AS m FINAL");
    expect(PERSON_MAIN_SQL).not.toContain("se_company_person_v2");
    expect(PERSON_MAIN_SQL).toContain("WHERE m.company_id = {companyId:String}");
    expect(PERSON_MAIN_SQL).toContain("toString(m.person_key) AS person_key");
    expect(PERSON_MAIN_SQL).toContain("arrayMap(x -> toString(x), m.normalized_ids) AS normalized_ids");
    expect(PERSON_MAIN_SQL).toContain("arrayMap(x -> ifNull(toString(x), ''), m.member_birth_years) AS member_birth_years");
    expect(PERSON_MAIN_SQL).toContain("toUInt8(m.active) AS active");
    expect(PERSON_MAIN_SQL).toContain("ORDER BY m.active DESC, m.inactive_reason, m.display_name");
    for (const column of ["birth_year", "wikidata_id", "first_year", "last_year"]) {
      expect(PERSON_MAIN_SQL).toContain(`AS ${column}`);
    }
    // History is append-only: never FINAL, newest first, capped, and it carries the
    // three columns the main table does not.
    expect(PERSON_HISTORY_SQL).toContain("FROM corpscout.se_company_person_history AS h");
    expect(PERSON_HISTORY_SQL).not.toContain("FINAL");
    expect(PERSON_HISTORY_SQL).toContain("ORDER BY h.changed_at DESC");
    expect(PERSON_HISTORY_SQL).toContain("LIMIT 200");
    expect(PERSON_HISTORY_SQL).toContain("toString(h.changed_at) AS changed_at");
    expect(PERSON_HISTORY_SQL).toContain("toString(h.change_kind) AS change_kind");
    expect(PERSON_NORMALIZED_SQL).toContain("FROM corpscout.se_company_person_normalized AS n FINAL");
    expect(PERSON_NORMALIZED_SQL).toContain("n.first_tokens AS first_tokens");
    expect(PERSON_NORMALIZED_SQL).toContain("ifNull(n.role_code, '') AS role_code");
    expect(PERSON_NORMALIZED_SQL).toContain("ifNull(toString(n.role_from), '') AS role_from");
    expect(PERSON_RAW_SQL).toContain("FROM corpscout.se_company_person_suggestion AS s FINAL");
    expect(PERSON_RAW_SQL).toContain("ifNull(s.full_name, '') AS full_name");
    expect(PERSON_RAW_SQL).toContain("ifNull(toString(s.fiscal_year), '') AS fiscal_year");
    expect(PERSON_RAW_SQL).toContain("s.data AS data");
    expect(PERSON_RULES_SQL).toContain("FROM corpscout.se_company_person_rule AS r FINAL");
    expect(PERSON_RULES_SQL).toContain("arrayMap(x -> toString(x), r.person_keys) AS person_keys");
    expect(PERSON_RULES_SQL).toContain("toUInt8(r.active) AS active");
    expect(PERSON_RULES_SQL).toContain("ORDER BY r.created_at DESC");
    // The global order and the company's own, one read (spec 3.6).
    expect(PERSON_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_person_precedence AS p FINAL");
    expect(PERSON_PRECEDENCE_SQL).toContain("WHERE p.company_id IN ('', {companyId:String}) AND p.field = 'name'");
    for (const sql of [PERSON_MAIN_SQL, PERSON_HISTORY_SQL, PERSON_NORMALIZED_SQL, PERSON_RAW_SQL, PERSON_RULES_SQL, PERSON_PRECEDENCE_SQL, PERSON_MATCH_SQL, PERSON_ROLE_SQL]) {
      expect(sql).toContain("{companyId:String}");
    }
    // The seventh read (spec section 5): the pairs the company's CURRENT input
    // certifies, exactly the join `batch.py::match_pairs_sql` makes.
    expect(PERSON_MATCH_SQL).toContain("FROM corpscout.se_company_person_match AS p FINAL");
    expect(PERSON_MATCH_SQL).toContain("FROM corpscout.se_company_person_match_state FINAL");
    expect(PERSON_MATCH_SQL).toContain("WHERE company_id = {companyId:String} AND error = ''");
    expect(PERSON_MATCH_SQL).toContain(
      "ON s.company_id = p.company_id AND s.input_hash = p.input_hash",
    );
    // The floor, not the threshold: the band is what the reviewer acts on.
    expect(PERSON_MATCH_SQL).toContain("AND p.confidence >= 0.5");
    expect(PERSON_MATCH_SQL).toContain("toString(p.candidate_a) AS candidate_a");
    expect(PERSON_MATCH_SQL).toContain("arrayMap(x -> toString(x), p.members_a) AS members_a");
    expect(PERSON_MATCH_SQL).toContain("arrayMap(x -> toString(x), p.members_b) AS members_b");
    expect(PERSON_MATCH_SQL).toContain("toFloat64(p.confidence) AS confidence");
    // The eighth read (spec section 11): the roles view. NO FINAL -- every refresh
    // rebuilds a plain MergeTree whole, so it holds exactly one version of every row.
    expect(PERSON_ROLE_SQL).toContain("FROM corpscout.se_company_person_role AS r");
    expect(PERSON_ROLE_SQL).not.toContain("FINAL");
    expect(PERSON_ROLE_SQL).toContain("toString(r.person_key) AS person_key");
    expect(PERSON_ROLE_SQL).toContain("toUInt16(r.role_year) AS role_year");
    expect(PERSON_ROLE_SQL).toContain("ifNull(toString(r.role_from), '') AS role_from");
    expect(PERSON_ROLE_SQL).toContain("ifNull(toString(r.role_to), '') AS role_to");
    expect(PERSON_ROLE_SQL).toContain("toUInt8(r.is_current) AS is_current");
    // F2: folded_at travels with every row so the panel can tell a fresh set from a
    // stale one by comparing it against the live person row's own folded_at.
    expect(PERSON_ROLE_SQL).toContain("toString(r.folded_at) AS folded_at");
    expect(PERSON_ROLE_SQL).toContain("WHERE r.company_id = {companyId:String}");
    expect(PERSON_ROLE_SQL).toContain(
      "ORDER BY r.person_key, r.role_year DESC, r.role_code, r.source",
    );
  });

  it("assembles the published persons, their members, roles, rules and the drafts", async () => {
    const detail = await loadSePersonDetail(COMPANY);
    expect(detail?.published.map((entry) => entry.row.person_key)).toEqual([MERGED_KEY, WIKI_KEY]);
    const merged = detail?.published[0];
    expect(merged?.members).toEqual([
      {
        source: "bolagsverket", slot: "uid-1:sig-1", normalizedId: "n1",
        name: "Anna Svensson", birthYear: "1975", wikidataId: "",
        data: '{"role_kind":"board_member"}',
        current: NORMALIZED_ROWS[0], raw: RAW_ROWS[0], refoldPending: false, precedence: 900,
        match: CALL_NAME_MATCH,
      },
      {
        source: "esef", slot: "doc-9:cand-1", normalizedId: "n2",
        name: "Anna Maria Svensson", birthYear: "", wikidataId: "",
        data: '{"title":"Chair"}',
        // The current normalized version is not the one this row was folded from.
        current: ESEF_NORMALIZED, raw: ESEF_RAW, refoldPending: true, precedence: 400,
        match: CALL_NAME_MATCH,
      },
    ]);
    // The roles block, zipped out of the three parallel arrays (spec 5.4).
    expect(merged?.roles).toEqual([
      { code: "board_chair", year: 2025, sources: ["esef"] },
      { code: "board_member", year: 2025, sources: ["bolagsverket"] },
    ]);
    // The view's own rows, grouped by person and left in the view's order. The
    // array-derived `roles` block above is untouched: it stays the fold's summary and
    // the panel's fallback.
    expect(merged?.roleRows).toEqual(ROLE_ROWS.slice(0, 3));
    expect(detail?.published[1]?.roleRows).toEqual([ROLE_ROWS[3]]);
    // Bolagsverket 900 beats ESEF 400 outright, so precedence explains the spelling.
    expect(merged?.spellingReason).toBe("precedence");
    expect(detail?.published[1]?.spellingReason).toBe("single source");
    // The active merge rule names both keys, so it is attached to both persons.
    expect(merged?.rules).toEqual([MERGE_RULE]);
    expect(merged?.matchedBy).toEqual([CALL_NAME_MATCH]);
    expect(detail?.published[1]?.matchedBy).toEqual([]);
    expect(detail?.published[1]?.members[0]?.match).toBeNull();
    expect(detail?.published[1]?.rules).toEqual([MERGE_RULE]);
    expect(detail?.history).toEqual([HISTORY_ROW]);
    expect(detail?.rules).toEqual([MERGE_RULE]);
    expect(detail?.precedence).toEqual(PRECEDENCE_ROWS);
    // The draft's two rows are ONE draft, grouped by the slot's first 18 characters,
    // carrying Ruling 4's note and replaced key out of `data`.
    expect(detail?.drafts).toEqual([
      {
        slot: GROUP, rows: [DRAFT_RAW_1, DRAFT_RAW_2], normalized: [DRAFT_NORMALIZED],
        name: "Anna Svensson", note: "seen in the annual report", replacesKey: MERGED_KEY,
      },
    ]);
    for (const sql of [PERSON_MAIN_SQL, PERSON_HISTORY_SQL, PERSON_NORMALIZED_SQL, PERSON_RAW_SQL, PERSON_RULES_SQL, PERSON_PRECEDENCE_SQL, PERSON_MATCH_SQL, PERSON_ROLE_SQL]) {
      expect(clickhouse.query.mock.calls.find(([text]) => text === sql)?.[1]).toEqual({ companyId: COMPANY });
    }
  });

  it("leaves roleRows empty when the view has not rebuilt since the fold", async () => {
    // The view refreshes at :20 and a fold can land at any minute, so a freshly folded
    // person legitimately has no row for up to an hour -- and a brand-new person has
    // none at all. That is a fallback, never an error.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_ROLE_SQL ? [] : answer(sql),
    );

    const detail = await loadSePersonDetail(COMPANY);

    expect(detail?.published.map((entry) => entry.roleRows)).toEqual([[], []]);
    expect(detail?.published[0]?.roles).toEqual([
      { code: "board_chair", year: 2025, sources: ["esef"] },
      { code: "board_member", year: 2025, sources: ["bolagsverket"] },
    ]);
  });

  it("F4: a missing or failing role view degrades to empty roleRows instead of taking the tab down", async () => {
    // Migration 000402 not yet applied, or rolled back: the eighth read alone rejects
    // with UNKNOWN_TABLE. The other seven reads still ran (this test's `answer` throws
    // on any SQL it does not recognise, so the whole detail resolving proves they did),
    // and the promise as a whole must resolve, not reject.
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    const unknownTable = new Error(
      "Code: 60. DB::Exception: Table corpscout.se_company_person_role doesn't exist (UNKNOWN_TABLE)",
    );
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_ROLE_SQL ? Promise.reject(unknownTable) : answer(sql),
    );

    const detail = await loadSePersonDetail(COMPANY);

    expect(detail?.published.map((entry) => entry.roleRows)).toEqual([[], []]);
    expect(detail?.published[0]?.members.length).toBeGreaterThan(0);
    expect(spy).toHaveBeenCalledTimes(1);
    spy.mockRestore();
  });

  it("drops a match pair whose two sides carry conflicting birth years", async () => {
    // Same two members as the merged person's default fixture, but now BOTH carry a
    // non-empty year and they disagree -- a person can hold two years only through a
    // reviewer merge rule, never through the model alone.
    const conflicting: SePersonRow = { ...MERGED_ROW, member_birth_years: ["1975", "1980"] };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [conflicting, WIKI_ROW] : answer(sql),
    );
    const detail = await loadSePersonDetail(COMPANY);
    const person = detail?.published[0];
    expect(person?.matchedBy).toEqual([]);
    expect(person?.members[0]?.match).toBeNull();
    expect(person?.members[1]?.match).toBeNull();
  });

  it("calls the spelling a tie-break, or the most complete, when two members rank the same", async () => {
    // A REAL tie, the one `fold.py::_text_member` actually resolves by something other
    // than completeness: two Bolagsverket members (precedence 900 each) whose spellings
    // have the same token count, so the longer string wins and neither is "more
    // complete" than the other.
    const tied = main({
      ...MERGED_ROW,
      sources: ["bolagsverket"],
      member_sources: ["bolagsverket", "bolagsverket"],
      member_slots: ["uid-1:sig-1", "uid-2:sig-1"],
      member_names: ["Anna Svensson", "Anna Svenson"],
      display_name: "Anna Svensson",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [tied] : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.published[0]?.spellingReason).toBe("tie-break");

    // The other branch of the same tie: same precedence, and the published spelling has
    // strictly more words than the other member -- which is how the fold broke it.
    const fuller = main({
      ...tied,
      member_names: ["Anna Maria Svensson", "Anna Svensson"],
      display_name: "Anna Maria Svensson",
      first_name: "Anna Maria",
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [fuller] : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.published[0]?.spellingReason).toBe("most complete");
  });

  it("is fold-pending on a newer normalized row, raw row or rule, never on a draft or the global precedence", async () => {
    // ESEF's normalized_at (10:00) is newer than the fold (09:00).
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);

    const settled = (sql: string) => {
      if (sql === PERSON_NORMALIZED_SQL) {
        return [normalized({}), { ...ESEF_NORMALIZED, normalized_at: "2026-09-10 08:00:00.000" }, WIKI_NORMALIZED, DRAFT_NORMALIZED];
      }
      if (sql === PERSON_RULES_SQL) return [];
      return answer(sql);
    };
    clickhouse.query.mockImplementation(async (sql: string) => settled(sql));
    // Only the draft (11:00) and the global precedence export (06:00) are left, and
    // neither selects a company for the fold (Ruling 7).
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(false);

    // A reviewer row the normalize step has not seen yet still owes a fold.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [...RAW_ROWS, raw({ source: "reviewer", slot: `${GROUP}01`, suggested_at: STAMP })]
        : settled(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);

    // So does a rule, active or released.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [{ ...MERGE_RULE, active: 0, created_at: "2026-09-10 11:30:00.000" }]
        : settled(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.foldPending).toBe(true);
  });

  it("returns null only when there is no main row, no normalized row and no draft", async () => {
    clickhouse.query.mockImplementation(async () => []);
    expect(await loadSePersonDetail(COMPANY)).toBeNull();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL ? [DRAFT_RAW_1] : [],
    );
    expect((await loadSePersonDetail(COMPANY))?.drafts).toHaveLength(1);
  });

  it("offers the band's cross-person pairs, strongest first, one row per pair, and drops the ones nobody can act on", async () => {
    // No active merge rule this time: the reviewer could act on either band, but the
    // card wants ONE row per person pair -- the higher confidence.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL ? [] : answer(sql),
    );
    const detail = await loadSePersonDetail(COMPANY);
    // 0.72 and 0.64 both join the merged person to the Wikidata person: one row, the
    // higher confidence. 0.93 is already merged (the fold did it), the 0.66 pair
    // names an observation no published person holds.
    expect(detail?.possibleMatches).toEqual([
      {
        personKeyA: MERGED_KEY, personKeyB: WIKI_KEY,
        nameA: "Anna Svensson", nameB: "Carl von Essen",
        confidence: 0.72, reason: "same household",
      },
    ]);
  });

  it("drops a possible match whose two persons are already named by an active merge rule", async () => {
    // MERGE_RULE (the default rules answer) already names both MERGED_KEY and
    // WIKI_KEY: the next fold merges them on its own, so there is nothing left for the
    // reviewer to act on.
    const detail = await loadSePersonDetail(COMPANY);
    expect(detail?.possibleMatches).toEqual([]);
  });

  it("keeps a possible-match side only when every one of its owned members agrees on one person", async () => {
    // A candidate whose members a birth-year split (or a reviewer split) left in TWO
    // persons: n1 is MERGED_KEY's own member, n3 is the Wikidata person's. A side like
    // that names no single Merge target, so the pair is dropped even though n2 alone
    // would resolve cleanly.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MATCH_SQL
        ? [
            {
              company_id: COMPANY, candidate_a: "mixed", candidate_b: "n2",
              members_a: ["n1", "n3"], members_b: ["n2"],
              name_a: "Mixed candidate", name_b: "Anna Maria Svensson",
              confidence: 0.6, reason: "ambiguous split",
            },
          ]
        : sql === PERSON_RULES_SQL
          ? []
          : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.possibleMatches).toEqual([]);
  });

  it("keeps a pair inside one person out of the band, and resolves a side through active persons only", async () => {
    // One person holding all three observations: every pair is internal now, so there
    // is nothing to merge -- and the 0.93 pair is still the record of what joined it.
    const whole = main({
      sources: ["bolagsverket", "esef", "wikidata"],
      slots: ["uid-1:sig-1", "doc-9:cand-1", "Q1:P169:Q7"],
      normalized_ids: ["n1", "n2", "n3"],
      member_sources: ["bolagsverket", "esef", "wikidata"],
      member_slots: ["uid-1:sig-1", "doc-9:cand-1", "Q1:P169:Q7"],
      member_names: ["Anna Svensson", "Anna Maria Svensson", "Carl von Essen"],
      member_birth_years: ["1975", "", ""], member_wikidata_ids: ["", "", "Q7"],
      member_data: ["{}", "{}", "{}"],
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [whole] : answer(sql),
    );
    const one = await loadSePersonDetail(COMPANY);
    expect(one?.possibleMatches).toEqual([]);
    expect(one?.published[0]?.matchedBy).toEqual([CALL_NAME_MATCH]);

    // The Wikidata person withdrawn: n3 has no ACTIVE owner, so a Merge naming it
    // would name a key nothing publishes any more.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL
        ? [MERGED_ROW, { ...WIKI_ROW, active: 0, inactive_reason: "withdrawn" }]
        : answer(sql),
    );
    expect((await loadSePersonDetail(COMPANY))?.possibleMatches).toEqual([]);
  });

  it("offers only the catalog's active roles, as code, label and group", async () => {
    expect(await loadSePersonRoleOptions()).toEqual([
      { code: "board_member", label: "Board member", group: "governance" },
    ]);
  });

  it("saves a draft as one row per role, under a group slot, with Ruling 4's data keys", async () => {
    const result = await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: null, replacesKey: MERGED_KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "",
          roles: [
            { code: "board_member", fromYear: "2024", toYear: "2024" },
            { code: "board_chair", fromYear: "2025", toYear: "" },
          ],
          data: '{"source":"annual report"}', note: "seen in the annual report",
        },
      },
      NOW,
    );
    expect(result).toEqual({ decidedAt: STAMP, slot: GROUP });
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const data = `{"source":"annual report","decided_by":"backoffice","note":"seen in the annual report","replaces_key":"${MERGED_KEY}"}`;
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      {
        company_id: COMPANY, source: "reviewer_draft", slot: `${GROUP}01`,
        suggestion_id: DRAFT_ID_1, suggested_at: STAMP, source_record_id: "",
        full_name: null, first_name: "Anna", last_name: "Svensson", birth_year: 1975,
        wikidata_id: null, role_original: "board_member", role_key: "board_member",
        // Ruling 5: one year is a fiscal year, a span is two dates.
        fiscal_year: 2024, role_from: null, role_to: null, document_ref: null, data,
      },
      {
        company_id: COMPANY, source: "reviewer_draft", slot: `${GROUP}02`,
        suggestion_id: DRAFT_ID_2, suggested_at: STAMP, source_record_id: "",
        full_name: null, first_name: "Anna", last_name: "Svensson", birth_year: 1975,
        wikidata_id: null, role_original: "board_chair", role_key: "board_chair",
        fiscal_year: null, role_from: "2025-01-01", role_to: null, document_ref: null, data,
      },
    ]);
  });

  it("writes one row with no role columns at all when the reviewer names no role", async () => {
    await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: null, replacesKey: null,
        input: { firstName: "Anna", lastName: "Svensson", birthYear: "", wikidataId: "Q7", roles: [], data: "{}", note: "" },
      },
      NOW,
    );
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      expect.objectContaining({
        slot: `${GROUP}01`, birth_year: null, wikidata_id: "Q7",
        role_original: null, role_key: null, fiscal_year: null, role_from: null, role_to: null,
        data: '{"decided_by":"backoffice","note":""}',
      }),
    ]);
  });

  it("tombstones the rows an edited draft no longer uses", async () => {
    // The stored draft has two rows; the edit keeps one, so 02 must be cleared or it
    // would stay a live observation of a role the reviewer just deleted.
    await saveSePersonDraft(
      COMPANY,
      {
        intent: "save-draft", slot: GROUP, replacesKey: MERGED_KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "",
          roles: [{ code: "board_member", fromYear: "2024", toYear: "2024" }],
          data: "{}", note: "",
        },
      },
      NOW,
    );
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows).toHaveLength(2);
    expect(rows[1]).toEqual(
      expect.objectContaining({
        slot: `${GROUP}02`, source: "reviewer_draft", full_name: null, first_name: null,
        last_name: null, birth_year: null, wikidata_id: null, role_original: null,
        role_key: null, fiscal_year: null, role_from: null, role_to: null,
        data: '{"decided_by":"backoffice","note":"role removed"}',
      }),
    );
  });

  it("activates a draft into reviewer rows and clears it, in one insert", async () => {
    await activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "confirmed" }, NOW);
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows).toHaveLength(4);
    // Ruling 4: `replaces_key` never reaches the published row.
    expect(rows[0]).toEqual({
      company_id: COMPANY, source: "reviewer", slot: `${GROUP}01`, suggestion_id: REVIEWER_ID_1,
      suggested_at: STAMP, source_record_id: "", full_name: null, first_name: "Anna",
      last_name: "Svensson", birth_year: 1975, wikidata_id: null,
      role_original: "board_member", role_key: "board_member", fiscal_year: 2024,
      role_from: null, role_to: null, document_ref: null,
      data: '{"decided_by":"backoffice","note":"confirmed"}',
    });
    expect(rows[1]).toEqual(expect.objectContaining({ slot: `${GROUP}02`, suggestion_id: REVIEWER_ID_2, role_key: "board_chair", role_from: "2025-01-01" }));
    expect(rows[2]).toEqual(expect.objectContaining({ source: "reviewer_draft", slot: `${GROUP}01`, first_name: null, data: '{"decided_by":"backoffice","note":"activated"}' }));
    expect(rows[3]).toEqual(expect.objectContaining({ source: "reviewer_draft", slot: `${GROUP}02`, first_name: null }));
    // Ruling 1: this correction folds back into the person it corrects (same tokens,
    // same birth year), so hiding that person would hide the correction with it.
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("hides the corrected person only when the correction does not fold back into it", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [raw({}), ESEF_RAW, WIKI_RAW, { ...DRAFT_RAW_1, last_name: "Svenson" }, { ...DRAFT_RAW_2, last_name: "Svenson" }]
        : answer(sql),
    );
    await activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY],
        slots: [], active: 1, note: "corrected by reviewer",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
  });

  it("refuses to activate or discard a draft that is not there", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL ? [raw({})] : answer(sql),
    );
    await expect(activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW)).rejects.toBeInstanceOf(SePersonDecisionError);
    await expect(discardSePersonDraft(COMPANY, { intent: "discard", slot: GROUP }, NOW)).rejects.toBeInstanceOf(SePersonDecisionError);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("discards a draft by clearing every row of its group", async () => {
    await discardSePersonDraft(COMPANY, { intent: "discard", slot: GROUP }, NOW);
    const rows = inserted(clickhouse.insertSuggestions);
    expect(rows.map((row) => row.slot)).toEqual([`${GROUP}01`, `${GROUP}02`]);
    expect(rows.every((row) => row.first_name === null && row.data === '{"decided_by":"backoffice","note":"discarded"}')).toBe(true);
  });

  it("removes a source-backed person with a hide rule and a reviewer-only one with tombstones", async () => {
    await removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY],
        slots: [], active: 1, note: "removed by reviewer", created_at: STAMP, created_by: "backoffice",
      },
    ]);

    clickhouse.insertRules.mockReset();
    const reviewerOnly = main({
      person_key: MERGED_KEY, sources: ["reviewer"], slots: [`${GROUP}01`, `${GROUP}02`],
      member_sources: ["reviewer", "reviewer"], member_slots: [`${GROUP}01`, `${GROUP}02`],
    });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [reviewerOnly] : sql === PERSON_RULES_SQL ? [] : answer(sql),
    );
    await removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "left" }, NOW);
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
    expect(inserted(clickhouse.insertSuggestions)).toEqual([
      expect.objectContaining({ source: "reviewer", slot: `${GROUP}01`, suggestion_id: REVIEWER_ID_1, first_name: null, data: '{"decided_by":"backoffice","note":"left"}' }),
      expect.objectContaining({ source: "reviewer", slot: `${GROUP}02`, suggestion_id: REVIEWER_ID_2, first_name: null }),
    ]);
  });

  it("refuses Remove on an unknown person, a hidden one, and one a rule already hides", async () => {
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: UNKNOWN_KEY, note: "" }, NOW)).rejects.toThrow("Unknown person.");
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_MAIN_SQL ? [main({ active: 0, inactive_reason: "hidden" })] : answer(sql),
    );
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("Already hidden.");
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [{ ...MERGE_RULE, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [MERGED_KEY] }]
        : answer(sql),
    );
    await expect(removeSePerson(COMPANY, { intent: "remove", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("Already hidden.");
  });

  it("writes one merge rule over the chosen keys and one split rule over the chosen slots", async () => {
    await mergeSePersons(COMPANY, { intent: "merge", personKeys: [MERGED_KEY, WIKI_KEY], note: "same person" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: MERGE_RULE_ID, kind: "merge",
        person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 1, note: "same person",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
    clickhouse.insertRules.mockReset();
    await splitSePersonSlots(COMPANY, { intent: "split", slots: ["doc-9:cand-1"], note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: SPLIT_RULE_ID, kind: "split", person_keys: [],
        slots: ["doc-9:cand-1"], active: 1, note: "split by reviewer",
        created_at: STAMP, created_by: "backoffice",
      },
    ]);
  });

  it("refuses a merge naming a person that is not published, and a split that changes nothing", async () => {
    await expect(mergeSePersons(COMPANY, { intent: "merge", personKeys: [MERGED_KEY, UNKNOWN_KEY], note: "" }, NOW)).rejects.toThrow("Unknown person.");
    await expect(splitSePersonSlots(COMPANY, { intent: "split", slots: ["nope"], note: "" }, NOW)).rejects.toThrow("Unknown observation.");
    // Every observation of one person: the fold would rebuild the same set.
    await expect(
      splitSePersonSlots(COMPANY, { intent: "split", slots: ["uid-1:sig-1", "doc-9:cand-1"], note: "" }, NOW),
    ).rejects.toThrow("That is every observation of one person.");
  });

  it("resets every active rule that names the person, keeping each rule_id", async () => {
    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "not the same" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: MERGE_RULE.rule_id, kind: "merge",
        person_keys: [MERGED_KEY, WIKI_KEY], slots: [], active: 0,
        note: "reset: not the same", created_at: STAMP, created_by: "backoffice",
      },
    ]);
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL ? [{ ...MERGE_RULE, active: 0 }] : answer(sql),
    );
    await expect(resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW)).rejects.toThrow("No rule to reset.");
  });

  it("resets a split rule through the person's own slots, not only its key", async () => {
    const splitRule: SePersonRuleRow = {
      ...MERGE_RULE, rule_id: SPLIT_RULE_ID, kind: "split", person_keys: [], slots: ["doc-9:cand-1"],
    };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL ? [splitRule] : answer(sql),
    );
    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      expect.objectContaining({ rule_id: SPLIT_RULE_ID, kind: "split", active: 0, note: "reset" }),
    ]);
  });

  it("resets a rule naming the key this person published under before it was re-keyed", async () => {
    // Remove hid the person under OLD_KEY. A fuller spelling in a later filing changed
    // the canonical tokens and the fold minted MERGED_KEY -- but the fold keeps applying
    // the rule, because it resolves a hide key through the PREVIOUS published members.
    // The tab has to resolve it the same way, or the person is hidden with no route
    // back: no rule shown and Reset answering "No rule to reset".
    const oldRule: SePersonRuleRow = {
      ...MERGE_RULE, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [OLD_KEY], slots: [],
    };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [oldRule]
        : sql === PERSON_HISTORY_SQL
          ? [{ ...HISTORY_ROW, person_key: OLD_KEY }]
          : answer(sql),
    );
    // The person whose observations do NOT overlap that history row keeps its own keys.
    const detail = await loadSePersonDetail(COMPANY);
    expect(detail?.published[0]?.rules).toEqual([oldRule]);
    expect(detail?.published[1]?.rules).toEqual([]);

    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [OLD_KEY],
        slots: [], active: 0, note: "reset", created_at: STAMP, created_by: "backoffice",
      },
    ]);

    // The SAME rule, resolved with NO history row at all: the main table keeps the old
    // key's fact permanently, as a withdrawn row whose member arrays still overlap the
    // re-keyed person's (`fold.py:754-765`) -- unlike history, which is capped at 200
    // rows and can miss it. `previousKeysOf` must resolve through `mainRows`, not only
    // through history.
    clickhouse.insertRules.mockReset();
    const withdrawnRow = main({ person_key: OLD_KEY, active: 0, inactive_reason: "withdrawn" });
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RULES_SQL
        ? [oldRule]
        : sql === PERSON_HISTORY_SQL
          ? []
          : sql === PERSON_MAIN_SQL
            ? [MERGED_ROW, WIKI_ROW, withdrawnRow]
            : answer(sql),
    );
    const viaMainRow = await loadSePersonDetail(COMPANY);
    expect(viaMainRow?.published[0]?.rules).toEqual([oldRule]);
    expect(viaMainRow?.published[1]?.rules).toEqual([]);

    await resetSePersonRules(COMPANY, { intent: "reset", personKey: MERGED_KEY, note: "" }, NOW);
    expect(inserted(clickhouse.insertRules)).toEqual([
      {
        company_id: COMPANY, rule_id: HIDE_RULE_ID, kind: "hide", person_keys: [OLD_KEY],
        slots: [], active: 0, note: "reset", created_at: STAMP, created_by: "backoffice",
      },
    ]);
  });

  it("refuses to activate a draft whose replaced key is not a key", async () => {
    // `replaces_key` rides inside `data`, an unconstrained String; the rule column is a
    // FixedString(64) that would zero-pad anything shorter into a key naming nothing.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [{ ...DRAFT_RAW_1, data: '{"decided_by":"backoffice","note":"","replaces_key":"nope"}' }]
        : answer(sql),
    );
    await expect(
      activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW),
    ).rejects.toThrow("not a person key");
    // It refuses before anything lands, so the draft is still there to be corrected.
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
    expect(clickhouse.insertRules).not.toHaveBeenCalled();
  });

  it("names what already landed when the hide rule fails after the rows are published", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_RAW_SQL
        ? [raw({}), ESEF_RAW, WIKI_RAW, { ...DRAFT_RAW_1, last_name: "Svenson" }, { ...DRAFT_RAW_2, last_name: "Svenson" }]
        : answer(sql),
    );
    clickhouse.insertRules.mockRejectedValue(new Error("connection reset"));
    const failure = activateSePersonDraft(COMPANY, { intent: "activate", slot: GROUP, note: "" }, NOW);
    await expect(failure).rejects.toBeInstanceOf(SePersonDecisionError);
    // The message names the group whose rows are already in, so the reviewer is not
    // told to try again: a second Activate finds no draft and refuses.
    await expect(failure).rejects.toThrow(GROUP);
    await expect(failure).rejects.toThrow("Remove that person instead");
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
  });

  it("launches the targeted fold for this company alone", async () => {
    expect(await launchSePersonFold(COMPANY)).toEqual({ runId: "run-9", url: null });
    expect(dagster.launchRun).toHaveBeenCalledWith({
      job: "__ASSET_JOB",
      assetSelection: ["se_company_person_fold_companies"],
      runConfig: {
        ops: { se_company_person_fold_companies: { config: { company_ids: [COMPANY] } } },
      },
      tags: { "backoffice/person": "fold-now" },
    });
  });
});
