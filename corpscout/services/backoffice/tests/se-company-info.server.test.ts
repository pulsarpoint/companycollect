import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyInfoCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  ARTIFACT_ROWS_SQL,
  appendSeCompanyInfoCorrection,
  CORRECTIONS_SQL,
  INFO_SQL,
  loadSeCompanyInfoDetail,
  NACE_LABEL_SQL,
  SUGGESTIONS_SQL,
} from "~/lib/se-company-info.server";
import { SeInfoCorrectionValidationError, ZERO_EVIDENCE_HASH } from "~/lib/se-info-corrections";
import {
  ARTIFACT_PAYLOAD_FIELDS,
  type ArtifactSource,
} from "~/lib/se-company-info-payload";

const COMPANY = "5565200028";

/**
 * The payload columns of each artifact table, straight from the DDL
 * (migration 000297, 000300's activity_description_en, 000306's two
 * legal-form labels and 000365's three ESEF enrichment columns) minus the
 * envelope
 * (company_id, source_record_uid, observed_at, source_run_id) and the
 * MATERIALIZED evidence_hash. Hand-copied here on purpose: it is the
 * independent statement of the schema that both the display list and the
 * query are checked against, so a column the migrations add can never stay
 * invisible on the review page.
 */
const DDL_PAYLOAD_COLUMNS: Record<ArtifactSource, string[]> = {
  scb: [
    "legal_name",
    "legal_name_raw",
    "legal_form_code",
    "legal_form_label_en",
    "legal_form_label_sv",
    "status",
    "incorporation_date",
    "dissolution_date",
    "activity_description",
    "activity_description_en",
    "primary_sni_code",
    "primary_nace_code",
  ],
  esef: [
    "source_document_id",
    "lei",
    "entity_name",
    "fiscal_year",
    "company_description",
    "description_language",
    "description_confidence",
    "products_and_services_json",
    "customer_markets_json",
    "operating_geographies_json",
    "business_segments_json",
    "material_group_relationships_json",
  ],
  wikidata: [
    "wikidata_id",
    "wikidata_url",
    "name",
    "official_name",
    "company_description",
    "inception_date",
    "legal_form_label",
    "industry_wikidata_id",
    "industry_label",
    "headquarters_label",
    "employee_count",
  ],
};

describe("company info queries", () => {
  it("qualify WHERE columns and expose provenance", () => {
    expect(INFO_SQL).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(INFO_SQL).toContain("WHERE i.company_id = {companyId:String}");
    for (const c of [
      "toString(i.evidence_set_hash) AS evidence_set_hash",
      "i.correction_ids",
      "toString(i.suggestion_id) AS suggestion_id",
      // Task 17: the Bool llm_enhanced replaces the description_source label,
      // cast to UInt8 so JSONEachRow yields one predictable 0/1 shape.
      "toUInt8(i.llm_enhanced) AS llm_enhanced",
      // Task 14: the published row holds both languages natively (migration 000301).
      "i.description AS description",
      "i.description_sv AS description_sv",
      // Task 19: what legal_form_code is CALLED, copied from the curated
      // dictionary by Dagster (migration 000306). Plain String columns -- an
      // absent label is '', which is what the artifact's join miss wrote.
      "i.legal_form_label_en AS legal_form_label_en",
      "i.legal_form_label_sv AS legal_form_label_sv",
    ]) {
      expect(INFO_SQL).toContain(c);
    }
    expect(INFO_SQL).not.toContain("description_source AS");
    expect(ARTIFACT_ROWS_SQL).toContain("'scb' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'esef' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'wikidata' AS source");
    // Each artifact leg reads FINAL -- pin the exact FROM clause per leg, not
    // just the source literal, so a leg silently losing FINAL still fails.
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_scb AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_esef AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_wikidata AS a FINAL");
    // Task 16: the review page is the hub, so each leg carries its FULL payload
    // as one JSON map instead of a single pre-picked summary column.
    expect(ARTIFACT_ROWS_SQL).not.toContain("AS summary");
    // A trailing ORDER BY only binds to the last SELECT of a UNION ALL chain
    // in ClickHouse, so the union must be wrapped and the ORDER BY placed
    // outside it -- pin the closing paren immediately followed by the ORDER
    // BY. The source_record_uid tiebreak (bulk-loaded SCB rows share one
    // observed_at) and the LIMIT (ESEF grows per filing; the other three
    // queries are bounded by construction already) are pinned separately.
    expect(ARTIFACT_ROWS_SQL).toContain(")\nORDER BY source, observed_at DESC");
    expect(ARTIFACT_ROWS_SQL).toContain(", source_record_uid");
    expect(ARTIFACT_ROWS_SQL).toContain("LIMIT 500");
    expect(SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_info_enrichment_observation AS s");
    // Pin the full expression including its alias for both suggestion flags,
    // so a swap between is_published and is_newest fails: P16 ruling made
    // them two independent flags, not a single is_current.
    expect(SUGGESTIONS_SQL).toContain(
      "toUInt8(ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)) AS is_published",
    );
    expect(SUGGESTIONS_SQL).toContain(
      `toUInt8(s.suggestion_id = (
    SELECT suggestion_id
    FROM corpscout.se_company_info_enrichment_observation
    WHERE company_id = {companyId:String}
    ORDER BY created_at DESC, suggestion_id DESC
    LIMIT 1
  )) AS is_newest`,
    );
    expect(CORRECTIONS_SQL).toContain("supersedes_correction_id IS NOT NULL");
    expect(CORRECTIONS_SQL).toContain("{zeroHash:String}");
    expect(CORRECTIONS_SQL).toContain("has({appliedIds:Array(String)}, toString(c.correction_id))");
  });

  it("projects every artifact leg with the same alias order, envelope first then payload_json", () => {
    // UNION ALL matches legs positionally, so every leg must project the same
    // five aliases in the same order -- pin the order, not just the presence.
    // Select-list aliases only: each ends the line, unlike the `AS a FINAL`
    // table alias every leg's FROM carries.
    const aliases = [...ARTIFACT_ROWS_SQL.matchAll(/AS (\w+)(?=,|\n)/g)].map((m) => m[1]);
    expect(aliases).toEqual([
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
    ]);
  });

  it("carries EVERY payload column of every artifact table in its toJSONString(map(...))", () => {
    for (const [source, columns] of Object.entries(DDL_PAYLOAD_COLUMNS)) {
      // The display list is the query's own column list, so a column missing
      // from one is missing from both -- check it against the DDL directly.
      const fields = ARTIFACT_PAYLOAD_FIELDS[source as ArtifactSource];
      expect([...fields.map((field) => field.key)].sort()).toEqual([...columns].sort());
      for (const column of columns) {
        // The Dagster build_artifact_rows_sql convention: the cast is INSIDE
        // ifNull (ClickHouse has no common type for a Date/number and '').
        expect(ARTIFACT_ROWS_SQL).toContain(`'${column}', ifNull(toString(a.${column}), '')`);
      }
    }
    expect(ARTIFACT_ROWS_SQL.match(/toJSONString\(map\(/g)).toHaveLength(3);
  });
});

describe("appendSeCompanyInfoCorrection", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("refuses when evidence moved", async () => {
    clickhouse.query.mockResolvedValueOnce([{ evidence_set_hash: "b".repeat(64) }]);
    await expect(
      appendSeCompanyInfoCorrection({
        companyId: COMPANY,
        kind: "override_field",
        payload: { description: "x" },
        evidenceHash: "a".repeat(64),
        reason: "r",
      }),
    ).rejects.toThrow(SeInfoCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("appends one row with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ evidence_set_hash: "a".repeat(64) }]);
    clickhouse.insert.mockResolvedValue(undefined);
    const { correctionId } = await appendSeCompanyInfoCorrection({
      companyId: COMPANY,
      kind: "override_field",
      payload: { description: "x" },
      evidenceHash: "a".repeat(64),
      reason: "r",
    });
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0]).toMatchObject({
      correction_id: correctionId,
      company_id: COMPANY,
      correction_kind: "override_field",
      decided_by: "backoffice",
    });
    expect(rows[0].created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });

  it("undo skips the evidence re-read and inserts exactly one row", async () => {
    clickhouse.insert.mockResolvedValue(undefined);
    const { correctionId } = await appendSeCompanyInfoCorrection({
      companyId: COMPANY,
      kind: "undo",
      evidenceHash: ZERO_EVIDENCE_HASH,
      reason: "r",
      supersedesCorrectionId: "11111111-1111-4111-8111-111111111111",
    });
    expect(clickhouse.query).not.toHaveBeenCalled();
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0]).toMatchObject({
      correction_id: correctionId,
      company_id: COMPANY,
      correction_kind: "undo",
      supersedes_correction_id: "11111111-1111-4111-8111-111111111111",
    });
  });

  it("loadSeCompanyInfoDetail threads ids into the detail queries and returns null when missing", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadSeCompanyInfoDetail(COMPANY)).toBeNull();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
  });

  it("loadSeCompanyInfoDetail resolves the NACE label when the row carries a code", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          evidence_set_hash: "a".repeat(64),
          correction_ids: [],
          primary_nace_code: "62.01",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        { description_en: "62.01 Computer programming activities" },
      ]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.naceLabel).toBe("Computer programming activities");
    expect(clickhouse.query).toHaveBeenCalledTimes(5);
    expect(clickhouse.query).toHaveBeenNthCalledWith(5, NACE_LABEL_SQL, {
      code: "62.01",
    });
    // The published rows carry dot-less codes; the lookup matches
    // normalized_code and the prefix strip handles the dotted description.
    expect(NACE_LABEL_SQL).toContain("normalized_code");
  });

  it("loadSeCompanyInfoDetail strips the dotted code prefix for dot-less lookups", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: null,
          evidence_set_hash: "a".repeat(64),
          correction_ids: [],
          primary_nace_code: "6419",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        { description_en: "64.19 Other monetary intermediation" },
      ]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.naceLabel).toBe("Other monetary intermediation");
  });

  it("loadSeCompanyInfoDetail threads the published suggestion id, evidence hash and applied ids", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          evidence_set_hash: "a".repeat(64),
          correction_ids: ["22222222-2222-4222-8222-222222222222"],
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    const detail = await loadSeCompanyInfoDetail(COMPANY);
    expect(detail).not.toBeNull();
    expect(clickhouse.query).toHaveBeenCalledTimes(4);
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, ARTIFACT_ROWS_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(3, SUGGESTIONS_SQL, {
      companyId: COMPANY,
      publishedSuggestionId: "11111111-1111-4111-8111-111111111111",
    });
    expect(clickhouse.query).toHaveBeenNthCalledWith(4, CORRECTIONS_SQL, {
      companyId: COMPANY,
      zeroHash: "0".repeat(64),
      evidenceSetHash: "a".repeat(64),
      appliedIds: ["22222222-2222-4222-8222-222222222222"],
    });
  });

  it("parses each artifact's payload_json into a name->string map, and never lets a bad one throw", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        { company_id: COMPANY, suggestion_id: null, evidence_set_hash: "a".repeat(64), correction_ids: [] },
      ])
      .mockResolvedValueOnce([
        {
          source: "scb",
          source_record_uid: "scb:1",
          observed_at: "2026-08-01 00:00:00.000",
          evidence_hash: "b".repeat(64),
          payload_json: '{"legal_name":"Alpha AB","incorporation_date":"2001-02-03"}',
        },
        {
          source: "wikidata",
          source_record_uid: "wikidata:Q1",
          observed_at: "2026-08-01 00:00:00.000",
          evidence_hash: "c".repeat(64),
          payload_json: "not json",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.artifacts[0]).toEqual({
      source: "scb",
      source_record_uid: "scb:1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "b".repeat(64),
      payload: { legal_name: "Alpha AB", incorporation_date: "2001-02-03" },
    });
    // A malformed payload is an empty map, not a 500 on the review page.
    expect(detail?.artifacts[1].payload).toEqual({});
  });
});
