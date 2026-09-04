import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn(), insert: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  chInsertSeBasicInfoSuggestions: clickhouse.insert,
}));
const dagster = vi.hoisted(() => ({ launchRun: vi.fn() }));
vi.mock("~/lib/dagster.server", () => ({
  launchRun: dagster.launchRun,
  ASSET_JOB_NAME: "__ASSET_JOB",
  SE_BASIC_INFO_FOLD_COMPANIES_ASSET: "se_company_basic_info_fold_companies",
}));

import {
  BASIC_INFO_HISTORY_SQL,
  BASIC_INFO_LEGAL_FORM_LABELS_SQL,
  BASIC_INFO_PRECEDENCE_SQL,
  BASIC_INFO_SQL,
  BASIC_INFO_SUGGESTIONS_SQL,
  loadSeBasicInfoDetail,
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

function answer(sql: string): unknown[] {
  if (sql === BASIC_INFO_SQL) return [MAIN_ROW];
  if (sql === BASIC_INFO_SUGGESTIONS_SQL) return [BOLAGSVERKET_ROW];
  if (sql === BASIC_INFO_HISTORY_SQL) {
    return [{ ...MAIN_ROW, changed_fields: ["legal_form_code"] }];
  }
  if (sql === BASIC_INFO_PRECEDENCE_SQL) {
    return [
      { field: "legal_name", source: "reviewer", precedence: 10000 },
      { field: "legal_name", source: "scb", precedence: 1000 },
      { field: "legal_name", source: "bolagsverket", precedence: 900 },
    ];
  }
  if (sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL) {
    return [{ code: "51", label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" }];
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
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_basic_info_suggestion AS s FINAL");
    expect(BASIC_INFO_SUGGESTIONS_SQL).toContain("WHERE s.company_id = {companyId:String}");
    expect(BASIC_INFO_HISTORY_SQL).toContain("FROM corpscout.se_company_basic_info_history AS h");
    expect(BASIC_INFO_HISTORY_SQL).toContain("ORDER BY h.folded_at DESC");
    expect(BASIC_INFO_PRECEDENCE_SQL).toContain("FROM corpscout.se_company_basic_info_precedence AS p FINAL");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("l.code IN {codes:Array(String)}");
    expect(BASIC_INFO_LEGAL_FORM_LABELS_SQL).toContain("code_type = 'legal_form'");
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

  it("loads the detail and labels every legal-form code it saw", async () => {
    const detail = await loadSeBasicInfoDetail(COMPANY);
    expect(detail).not.toBeNull();
    expect(detail?.info?.legal_form_code).toBe("51");
    expect(detail?.suggestions).toEqual([BOLAGSVERKET_ROW]);
    expect(detail?.history[0]?.changed_fields).toEqual(["legal_form_code"]);
    expect(detail?.precedence).toHaveLength(3);
    expect(detail?.legalFormLabels["51"]?.label_sv).toBe("Ekonomisk förening");
    expect(detail?.foldPending).toBe(true);
    const labelCall = clickhouse.query.mock.calls.find(([sql]) => sql === BASIC_INFO_LEGAL_FORM_LABELS_SQL);
    expect(labelCall?.[1]).toEqual({ codes: ["51"] });
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
});
