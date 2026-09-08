import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  query: vi.fn(),
  insertPrecedence: vi.fn(),
  insertSuggestions: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeBasicInfoPrecedence: clickhouse.insertPrecedence,
  chInsertSeBasicInfoSuggestions: clickhouse.insertSuggestions,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => null) }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  dagsterRunUrl: dagster.dagsterRunUrl,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_BASIC_INFO_FOLD_COMPANIES_ASSET: "se_company_basic_info_fold_companies",
}));

import {
  activateSeBasicInfoDraft,
  appendSeBasicInfoDraft,
  appendSeBasicInfoRule,
  BASIC_INFO_ACTIVE_RULES_SQL,
  BASIC_INFO_HISTORY_SQL,
  BASIC_INFO_LEGAL_FORM_LABELS_SQL,
  BASIC_INFO_LEGAL_FORM_OPTIONS_SQL,
  BASIC_INFO_PRECEDENCE_SQL,
  BASIC_INFO_SQL,
  BASIC_INFO_SUGGESTIONS_SQL,
  discardSeBasicInfoDraft,
  launchSeBasicInfoFold,
  loadSeBasicInfoDetail,
  SeBasicInfoDecisionError,
  suggestionRowVersion,
  type SeBasicInfoPrecedenceRow,
  type SeBasicInfoRow,
  type SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

export const MAIN_ROW: SeBasicInfoRow = {
  company_id: COMPANY,
  legal_name: "Fastighetsföreningen Sportstugan nr 1 upa",
  legal_name_source: "bolagsverket",
  legal_form_code: "51",
  legal_form_code_source: "bolagsverket",
  status: "inactive",
  status_source: "bolagsverket",
  economic_activity: "",
  economic_activity_source: "",
  incorporation_date: "1937-05-12",
  incorporation_date_source: "bolagsverket",
  lei: "",
  lei_source: "",
  wikidata_id: "",
  wikidata_id_source: "",
  description: "Föreningen har till ändamål att förvalta fastigheter.",
  description_source: "bolagsverket",
  description_language: "sv",
  description_sv: "Föreningen har till ändamål att förvalta fastigheter.",
  description_sv_source: "bolagsverket",
  folded_at: "2026-09-04 17:04:01.293",
  fold_version: "fold-v1",
  source_run_id: "da0c49db-d285-410e-8bed-cceed86ab82c",
};

export const BOLAGSVERKET_ROW: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  source_record_uid: "abc",
  observed_at: "2026-09-03 18:16:21.117",
  suggested_at: "2026-09-04 17:46:53.852",
  legal_name: "Fastighetsföreningen Sportstugan nr 1 upa",
  legal_form_code: "51",
  status: "inactive",
  economic_activity: "",
  incorporation_date: "1937-05-12",
  lei: "",
  wikidata_id: "",
  description: "Föreningen har till ändamål att förvalta fastigheter.",
  description_language: "sv",
  description_sv: "Föreningen har till ändamål att förvalta fastigheter.",
  decided_by: "",
  note: "",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};

// A reviewer_draft row with only legal_name set; every other value column is
// '' the way a FINAL read reports "no value yet".
export const DRAFT_ROW: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "reviewer_draft",
  source_record_uid: "",
  observed_at: "2026-09-04 20:00:00.000",
  suggested_at: "2026-09-04 20:00:00.000",
  legal_name: "Sportstugan Draft AB",
  legal_form_code: "",
  status: "",
  economic_activity: "",
  incorporation_date: "",
  lei: "",
  wikidata_id: "",
  description: "",
  description_language: "",
  description_sv: "",
  decided_by: "backoffice",
  note: "edited",
  source_run_id: "backoffice",
  extractor_version: "backoffice-v1",
};

// An active reviewer row with only status set.
export const REVIEWER_ROW: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "reviewer",
  source_record_uid: "",
  observed_at: "2026-09-01 00:00:00.000",
  suggested_at: "2026-09-01 00:00:00.000",
  legal_name: "",
  legal_form_code: "",
  status: "active",
  economic_activity: "",
  incorporation_date: "",
  lei: "",
  wikidata_id: "",
  description: "",
  description_language: "",
  description_sv: "",
  decided_by: "backoffice",
  note: "typed by reviewer",
  source_run_id: "backoffice",
  extractor_version: "backoffice-v1",
};

const GLOBAL_PRECEDENCE_ROWS: SeBasicInfoPrecedenceRow[] = [
  { company_id: "", field: "legal_name", source: "reviewer", precedence: 10000, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
  { company_id: "", field: "legal_name", source: "scb", precedence: 1000, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
  { company_id: "", field: "legal_name", source: "bolagsverket", precedence: 900, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
];

const COMPANY_RULE_ROW: SeBasicInfoPrecedenceRow = {
  company_id: COMPANY,
  field: "legal_form_code",
  source: "scb",
  precedence: 10000,
  removed: 0,
  decided_by: "backoffice",
  note: "matches register",
  decided_at: "2026-09-05 08:00:00.000",
};

function answer(sql: string): unknown[] {
  if (sql === BASIC_INFO_SQL) return [MAIN_ROW];
  if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW];
  if (sql === BASIC_INFO_HISTORY_SQL) {
    return [
      {
        folded_at: MAIN_ROW.folded_at,
        fold_version: MAIN_ROW.fold_version,
        source_run_id: MAIN_ROW.source_run_id,
        changed_fields: ["legal_form_code"],
      },
    ];
  }
  if (sql === BASIC_INFO_PRECEDENCE_SQL) return [...GLOBAL_PRECEDENCE_ROWS, COMPANY_RULE_ROW];
  // No other active rule by default; the "one other active rule" test below
  // overrides this to exercise the retirement path.
  if (sql === BASIC_INFO_ACTIVE_RULES_SQL) return [];
  if (sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL) {
    return [{ code: "51", label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" }];
  }
  if (sql === BASIC_INFO_LEGAL_FORM_OPTIONS_SQL) {
    return [{ code: "51", label_sv: "Ekonomisk förening", label_en: "Economic association (ekonomisk förening)" }];
  }
  throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
}

describe("se-basic-info.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => answer(sql));
  });

  it("pins the SQL to FINAL reads keyed on the company parameter", () => {
    expect(BASIC_INFO_SQL).toContain("FROM corpscout.se_company_basic_info AS b FINAL");
    expect(BASIC_INFO_SQL).toContain("WHERE b.company_id = {companyId:String}");
    // Slice 6: the main row carries economic_activity and its source; a suggestion row
    // reads NULL as ''.
    expect(BASIC_INFO_SQL).toContain("toString(b.economic_activity) AS economic_activity");
    expect(BASIC_INFO_SQL).toContain("toString(b.economic_activity_source) AS economic_activity_source");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("ifNull(s.economic_activity, '') AS economic_activity");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_basic_info_suggestion AS s FINAL");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("WHERE s.company_id = {companyId:String}");
    expect(BASIC_INFO_HISTORY_SQL).toContain("FROM corpscout.se_company_basic_info_history AS h");
    expect(BASIC_INFO_HISTORY_SQL).toContain("ORDER BY h.folded_at DESC");
    // The history row is narrowed to the four fields HistoryCard renders.
    expect(BASIC_INFO_HISTORY_SQL).toContain("toString(h.folded_at) AS folded_at");
    expect(BASIC_INFO_HISTORY_SQL).toContain("toString(h.fold_version) AS fold_version");
    expect(BASIC_INFO_HISTORY_SQL).toContain("h.source_run_id AS source_run_id");
    expect(BASIC_INFO_HISTORY_SQL).toContain("h.changed_fields AS changed_fields");
    expect(BASIC_INFO_HISTORY_SQL).not.toContain("legal_name");
    // The suggestion row keeps the Nullable-column adjustments, not the raw
    // shared-column text those replacements start from.
    expect(BASIC_INFO_SUGGESTIONS_SQL).not.toContain("  s.legal_name AS legal_name");
    expect(BASIC_INFO_SUGGESTIONS_SQL).not.toContain("toString(s.status) AS status");
    // The precedence table is scoped to the global rows and this company's own,
    // FINAL so a released rule's newest version wins, bound by name.
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_basic_info_precedence AS p FINAL");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("WHERE p.company_id IN ('', {companyId:String})");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toString(p.field) AS field");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toString(p.source) AS source");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toUInt32(p.precedence) AS precedence");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toUInt8(p.removed) AS removed");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toString(p.decided_by) AS decided_by");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("toString(p.decided_at) AS decided_at");
    // The other-active-rules read (bound by field too) that lets a use-this
    // retire every other source's rule for the field in the same insert.
    expect(BASIC_INFO_ACTIVE_RULES_SQL).toContain("FROM corpscout.se_company_basic_info_precedence AS p FINAL");
    expect(BASIC_INFO_ACTIVE_RULES_SQL).toContain(
      "WHERE p.company_id = {companyId:String} AND p.field = {field:String} AND p.removed = 0",
    );
    expect(BASIC_INFO_ACTIVE_RULES_SQL).toContain("toString(p.source) AS source");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("l.code IN {codes:Array(String)}");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("code_type = 'legal_form'");
    // The legal-form options for the edit sheet's select: every numeric code,
    // unbound (no company parameter), sv label first since the sheet reads
    // Swedish first.
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain("FROM corpscout.se_code_labels AS l");
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain(
      "WHERE l.code_type = 'legal_form' AND match(l.code, '^[0-9]{2}$')",
    );
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain("argMax(l.label_sv, l.version) AS label_sv");
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain("argMax(l.label_en, l.version) AS label_en");
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain("GROUP BY l.code");
    expect(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL).toContain("ORDER BY l.code");
    // Every nullable value column reaches the page as '' (never null).
    for (const column of ["legal_form_code", "lei", "wikidata_id", "description", "description_language", "description_sv"]) {
      expect(BASIC_INFO_SQL).toContain(`ifNull(b.${column}, '') AS ${column}`);
      expect(BASIC_INFO_SUGGESTIONS_SQL).toContain(`ifNull(s.${column}, '') AS ${column}`);
    }
    expect(BASIC_INFO_SQL).toContain("ifNull(toString(b.incorporation_date), '') AS incorporation_date");
    // legal_name and status are Nullable on the suggestion table only, so the
    // shared column snippet is adjusted for the suggestion alias.
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("ifNull(s.legal_name, '') AS legal_name");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("ifNull(s.status, '') AS status");
  });

  it("loads the detail, splits global precedence from this company's rules, and labels every legal-form code it saw", async () => {
    const detail = await loadSeBasicInfoDetail(COMPANY);
    expect(detail).not.toBeNull();
    expect(detail?.info?.legal_form_code).toBe("51");
    expect(detail?.suggestions).toEqual([BOLAGSVERKET_ROW]);
    expect(detail?.history[0]?.changed_fields).toEqual(["legal_form_code"]);
    expect(detail?.precedence).toEqual(GLOBAL_PRECEDENCE_ROWS);
    expect(detail?.rules).toEqual([COMPANY_RULE_ROW]);
    expect(detail?.legalFormLabels["51"]?.label_sv).toBe("Ekonomisk förening");
    expect(detail?.foldPending).toBe(true);
    expect(detail?.legalFormOptions).toEqual([
      { code: "51", label_sv: "Ekonomisk förening", label_en: "Economic association (ekonomisk förening)" },
    ]);
    const labelCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL);
    expect(labelCall?.[1]).toEqual({ codes: ["51"] });
    const precedenceCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_PRECEDENCE_SQL);
    expect(precedenceCall?.[1]).toEqual({ companyId: COMPANY });
    expect(clickhouse.query.mock.calls.some(([sql]) => sql === BASIC_INFO_LEGAL_FORM_OPTIONS_SQL)).toBe(true);
  });

  it("computes foldPending from a company rule newer than the fold, even a released one", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [{ ...BOLAGSVERKET_ROW, suggested_at: "2026-09-01 00:00:00.000" }];
      if (sql === BASIC_INFO_PRECEDENCE_SQL) {
        return [
          { company_id: "", field: "status", source: "scb", precedence: 1000, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
          { company_id: COMPANY, field: "status", source: "bolagsverket", precedence: 10000, removed: 1, decided_by: "backoffice", note: "", decided_at: "2026-09-05 09:00:00.000" },
        ];
      }
      return answer(sql);
    });
    const detail = await loadSeBasicInfoDetail(COMPANY);
    // Released (removed = 1), so it is not an active rule...
    expect(detail?.rules).toEqual([]);
    // ...but it still counts toward fold-pending: a release must also read as
    // pending until the next fold applies it.
    expect(detail?.foldPending).toBe(true);
  });

  it("excludes a reviewer_draft row from fold-pending but counts a reviewer row", async () => {
    // Mirrors Dagster's suggestion_watermarks_sql exclusion: a draft is not yet
    // activated, so it must never raise "Fold pending" on its own -- but once
    // the same value lands on the active `reviewer` row, it must.
    const withSuggestion = (source: string) =>
      clickhouse.query.mockImplementation(async (sql: string) => {
        if (sql === BASIC_INFO_SUGGESTIONS_SQL) {
          return [{ ...BOLAGSVERKET_ROW, source, suggested_at: "2026-09-05 09:00:00.000" }];
        }
        if (sql === BASIC_INFO_PRECEDENCE_SQL) return GLOBAL_PRECEDENCE_ROWS; // no company rule in play
        return answer(sql);
      });
    withSuggestion("reviewer_draft");
    expect((await loadSeBasicInfoDetail(COMPANY))?.foldPending).toBe(false);
    withSuggestion("reviewer");
    expect((await loadSeBasicInfoDetail(COMPANY))?.foldPending).toBe(true);
  });

  it("is null only when neither the main row nor a suggestion exists", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SQL || sql === BASIC_INFO_SUGGESTIONS_SQL ? [] : answer(sql),
    );
    expect(await loadSeBasicInfoDetail(COMPANY)).toBeNull();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SQL ? [] : answer(sql),
    );
    const unfolded = await loadSeBasicInfoDetail(COMPANY);
    expect(unfolded?.info).toBeNull();
    expect(unfolded?.foldPending).toBe(true);
  });

  it("skips the label lookup when no code is in play", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_SQL) return [{ ...MAIN_ROW, legal_form_code: "" }];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [{ ...BOLAGSVERKET_ROW, legal_form_code: "" }];
      return answer(sql);
    });
    const detail = await loadSeBasicInfoDetail(COMPANY);
    expect(detail?.legalFormLabels).toEqual({});
    expect(clickhouse.query.mock.calls.some(([sql]) => sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL)).toBe(false);
  });

  const NOW = new Date("2026-09-04T19:30:00.123Z");

  it("use-this with no other active rule inserts one row", async () => {
    clickhouse.insertPrecedence.mockReset();
    const result = await appendSeBasicInfoRule(
      COMPANY,
      { intent: "use-this", field: "legal_form_code", source: "bolagsverket", note: "register is right" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertPrecedence).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertPrecedence.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        field: "legal_form_code",
        source: "bolagsverket",
        precedence: 10000,
        removed: 0,
        decided_by: "backoffice",
        note: "register is right",
        decided_at: "2026-09-04 19:30:00.123",
      },
    ]);
    const activeRulesCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_ACTIVE_RULES_SQL);
    expect(activeRulesCall?.[1]).toEqual({ companyId: COMPANY, field: "legal_form_code" });
  });

  it("use-this with one other active rule retires it and writes the new rule last", async () => {
    clickhouse.insertPrecedence.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_ACTIVE_RULES_SQL ? [{ source: "scb" }] : answer(sql),
    );
    const result = await appendSeBasicInfoRule(
      COMPANY,
      { intent: "use-this", field: "status", source: "bolagsverket", note: "register is right" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertPrecedence).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertPrecedence.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        field: "status",
        source: "scb",
        precedence: 10000,
        removed: 1,
        decided_by: "backoffice",
        note: "superseded by Bolagsverket",
        decided_at: "2026-09-04 19:30:00.123",
      },
      {
        company_id: COMPANY,
        field: "status",
        source: "bolagsverket",
        precedence: 10000,
        removed: 0,
        decided_by: "backoffice",
        note: "register is right",
        decided_at: "2026-09-04 19:30:00.123",
      },
    ]);
    const activeRulesCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_ACTIVE_RULES_SQL);
    expect(activeRulesCall?.[1]).toEqual({ companyId: COMPANY, field: "status" });
  });

  it("reset retires every active rule of the field in one write and skips the suggestion insert when the reviewer has no value", async () => {
    clickhouse.insertPrecedence.mockReset();
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_ACTIVE_RULES_SQL) return [{ source: "bolagsverket" }, { source: "ratsit" }];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW]; // no reviewer row for `status`
      return answer(sql);
    });
    const result = await appendSeBasicInfoRule(
      COMPANY,
      { intent: "reset", field: "status", note: "back to default" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertPrecedence).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertPrecedence.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        field: "status",
        source: "bolagsverket",
        precedence: 10000,
        removed: 1,
        decided_by: "backoffice",
        note: "reset to default: back to default",
        decided_at: "2026-09-04 19:30:00.123",
      },
      {
        company_id: COMPANY,
        field: "status",
        source: "ratsit",
        precedence: 10000,
        removed: 1,
        decided_by: "backoffice",
        note: "reset to default: back to default",
        decided_at: "2026-09-04 19:30:00.123",
      },
    ]);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("reset also clears an active reviewer value, precedence insert first, in a separate suggestion insert", async () => {
    clickhouse.insertPrecedence.mockReset();
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_ACTIVE_RULES_SQL) return [{ source: "bolagsverket" }];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW, REVIEWER_ROW];
      return answer(sql);
    });
    const result = await appendSeBasicInfoRule(COMPANY, { intent: "reset", field: "status", note: "" }, NOW);
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertPrecedence).toHaveBeenCalledTimes(1);
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    expect(clickhouse.insertPrecedence.mock.invocationCallOrder[0]).toBeLessThan(
      clickhouse.insertSuggestions.mock.invocationCallOrder[0],
    );
    const [precedenceRows] = clickhouse.insertPrecedence.mock.calls[0] as [Record<string, unknown>[]];
    expect(precedenceRows).toEqual([
      {
        company_id: COMPANY,
        field: "status",
        source: "bolagsverket",
        precedence: 10000,
        removed: 1,
        decided_by: "backoffice",
        note: "reset to default",
        decided_at: "2026-09-04 19:30:00.123",
      },
    ]);
    const [suggestionRows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(suggestionRows).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: null,
        legal_form_code: null,
        status: null,
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: null,
        description_language: null,
        description_sv: null,
        decided_by: "backoffice",
        note: "reset to default",
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("reset skips the precedence insert when there is no rule but still clears an active reviewer value", async () => {
    clickhouse.insertPrecedence.mockReset();
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_ACTIVE_RULES_SQL) return [];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [REVIEWER_ROW];
      return answer(sql);
    });
    const result = await appendSeBasicInfoRule(
      COMPANY,
      { intent: "reset", field: "status", note: "cleanup" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertPrecedence).not.toHaveBeenCalled();
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows[0]).toMatchObject({ source: "reviewer", status: null, note: "reset to default: cleanup" });
  });

  it("reset refuses a field that has neither a company rule nor a reviewer value", async () => {
    clickhouse.insertPrecedence.mockReset();
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === BASIC_INFO_ACTIVE_RULES_SQL) return [];
      if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW]; // no reviewer row
      return answer(sql);
    });
    await expect(
      appendSeBasicInfoRule(COMPANY, { intent: "reset", field: "status", note: "" }, NOW),
    ).rejects.toThrow("No company rule or reviewer value to reset for Status.");
    expect(clickhouse.insertPrecedence).not.toHaveBeenCalled();
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("suggestionRowVersion nullifies '' and undefined value columns, applies changes, and nulls an empty note", () => {
    const fromNothing = suggestionRowVersion(
      COMPANY,
      undefined,
      "reviewer_draft",
      { legal_name: "New name" },
      "  ",
      "2026-09-04 19:30:00.123",
    );
    expect(fromNothing).toEqual({
      company_id: COMPANY,
      source: "reviewer_draft",
      source_record_uid: "",
      observed_at: "2026-09-04 19:30:00.123",
      legal_name: "New name",
      legal_form_code: null,
      status: null,
      economic_activity: null,
      incorporation_date: null,
      lei: null,
      wikidata_id: null,
      description: null,
      description_language: null,
      description_sv: null,
      decided_by: "backoffice",
      note: null,
      suggested_at: "2026-09-04 19:30:00.123",
      source_run_id: "backoffice",
      extractor_version: "backoffice-v1",
    });
    const fromCurrent = suggestionRowVersion(
      COMPANY,
      { ...BOLAGSVERKET_ROW, lei: "" },
      "reviewer",
      { status: null },
      "kept",
      "2026-09-04 19:30:00.123",
    );
    expect(fromCurrent.legal_name).toBe(BOLAGSVERKET_ROW.legal_name);
    expect(fromCurrent.lei).toBeNull();
    expect(fromCurrent.status).toBeNull();
    expect(fromCurrent.note).toBe("kept");
  });

  it("edit writes a reviewer_draft version with the field set, carrying forward the rest of the draft", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [BOLAGSVERKET_ROW, DRAFT_ROW] : answer(sql),
    );
    const result = await appendSeBasicInfoDraft(
      COMPANY,
      { intent: "edit", field: "status", value: "active", language: "", note: "looks right" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: DRAFT_ROW.legal_name,
        legal_form_code: null,
        status: "active",
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: null,
        description_language: null,
        description_sv: null,
        decided_by: "backoffice",
        note: "looks right",
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("edit on description also sets description_language, starting from no current draft", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => (sql === BASIC_INFO_SUGGESTIONS_SQL ? [] : answer(sql)));
    const result = await appendSeBasicInfoDraft(
      COMPANY,
      { intent: "edit", field: "description", value: "New description", language: "en", note: "" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: null,
        legal_form_code: null,
        status: null,
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: "New description",
        description_language: "en",
        description_sv: null,
        decided_by: "backoffice",
        note: null,
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("activate copies the draft value into a reviewer version and clears the draft, in one insert", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [BOLAGSVERKET_ROW, DRAFT_ROW, REVIEWER_ROW] : answer(sql),
    );
    const result = await activateSeBasicInfoDraft(
      COMPANY,
      { intent: "activate", field: "legal_name", note: "reviewer confirmed" },
      NOW,
    );
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: DRAFT_ROW.legal_name,
        legal_form_code: null,
        status: REVIEWER_ROW.status,
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: null,
        description_language: null,
        description_sv: null,
        decided_by: "backoffice",
        note: "reviewer confirmed",
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: null,
        legal_form_code: null,
        status: null,
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: null,
        description_language: null,
        description_sv: null,
        decided_by: "backoffice",
        note: "activated",
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("activate on description copies description_language to the reviewer version and clears it on the draft", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    const draftWithDescription = { ...DRAFT_ROW, description: "Ny beskrivning", description_language: "sv" };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [draftWithDescription] : answer(sql),
    );
    await activateSeBasicInfoDraft(COMPANY, { intent: "activate", field: "description", note: "" }, NOW);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows[0]).toMatchObject({
      source: "reviewer",
      description: "Ny beskrivning",
      description_language: "sv",
      note: null,
    });
    expect(rows[1]).toMatchObject({
      source: "reviewer_draft",
      description: null,
      description_language: null,
      note: "activated",
    });
  });

  it("activate refuses when there is no draft row for the field", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [BOLAGSVERKET_ROW] : answer(sql),
    );
    await expect(
      activateSeBasicInfoDraft(COMPANY, { intent: "activate", field: "legal_name", note: "" }, NOW),
    ).rejects.toThrow("No draft value for Legal name to activate.");
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("activate refuses when the draft row exists but is empty for the field", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [DRAFT_ROW] : answer(sql),
    );
    await expect(
      activateSeBasicInfoDraft(COMPANY, { intent: "activate", field: "legal_form_code", note: "" }, NOW),
    ).rejects.toBeInstanceOf(SeBasicInfoDecisionError);
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("discard clears the draft field and notes it discarded", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [DRAFT_ROW] : answer(sql),
    );
    const result = await discardSeBasicInfoDraft(COMPANY, { intent: "discard", field: "legal_name" }, NOW);
    expect(result).toEqual({ decidedAt: "2026-09-04 19:30:00.123" });
    expect(clickhouse.insertSuggestions).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows).toEqual([
      {
        company_id: COMPANY,
        source: "reviewer_draft",
        source_record_uid: "",
        observed_at: "2026-09-04 19:30:00.123",
        legal_name: null,
        legal_form_code: null,
        status: null,
        economic_activity: null,
        incorporation_date: null,
        lei: null,
        wikidata_id: null,
        description: null,
        description_language: null,
        description_sv: null,
        decided_by: "backoffice",
        note: "discarded",
        suggested_at: "2026-09-04 19:30:00.123",
        source_run_id: "backoffice",
        extractor_version: "backoffice-v1",
      },
    ]);
  });

  it("discard on description clears description_language too", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    const draftWithDescription = { ...DRAFT_ROW, description: "Ny beskrivning", description_language: "sv" };
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === BASIC_INFO_SUGGESTIONS_SQL ? [draftWithDescription] : answer(sql),
    );
    await discardSeBasicInfoDraft(COMPANY, { intent: "discard", field: "description" }, NOW);
    const [rows] = clickhouse.insertSuggestions.mock.calls[0] as [Record<string, unknown>[]];
    expect(rows[0]).toMatchObject({ description: null, description_language: null, note: "discarded" });
  });

  it("discard refuses when the draft has no value for the field", async () => {
    clickhouse.insertSuggestions.mockReset();
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) => (sql === BASIC_INFO_SUGGESTIONS_SQL ? [] : answer(sql)));
    await expect(
      discardSeBasicInfoDraft(COMPANY, { intent: "discard", field: "lei" }, NOW),
    ).rejects.toThrow("No draft value for LEI to discard.");
    expect(clickhouse.insertSuggestions).not.toHaveBeenCalled();
  });

  it("refuses a source with no opinion on the field", async () => {
    await expect(
      appendSeBasicInfoRule(COMPANY, { intent: "use-this", field: "lei", source: "bolagsverket", note: "" }, NOW),
    ).rejects.toBeInstanceOf(SeBasicInfoDecisionError);
    await expect(
      appendSeBasicInfoRule(COMPANY, { intent: "use-this", field: "status", source: "scb", note: "" }, NOW),
    ).rejects.toThrow("SCB has no status for this company.");
  });

  it("fold-now launches the per-company fold asset for exactly this company", async () => {
    dagster.launchRun.mockResolvedValue({ runId: "run-9", status: "QUEUED" });
    const launched = await launchSeBasicInfoFold(COMPANY);
    expect(launched.runId).toBe("run-9");
    expect(dagster.launchRun).toHaveBeenCalledWith({
      job: "__ASSET_JOB",
      assetSelection: ["se_company_basic_info_fold_companies"],
      runConfig: { ops: { se_company_basic_info_fold_companies: { config: { company_ids: [COMPANY] } } } },
      tags: { "backoffice/basic-info": "fold-now" },
    });
  });
});
