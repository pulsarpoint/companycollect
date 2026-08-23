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
  SUGGESTIONS_SQL,
} from "~/lib/se-company-info.server";
import { SeInfoCorrectionValidationError, ZERO_EVIDENCE_HASH } from "~/lib/se-info-corrections";

const COMPANY = "5565200028";

describe("company info queries", () => {
  it("qualify WHERE columns and expose provenance", () => {
    expect(INFO_SQL).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(INFO_SQL).toContain("WHERE i.company_id = {companyId:String}");
    for (const c of [
      "toString(i.evidence_set_hash) AS evidence_set_hash",
      "i.correction_ids",
      "toString(i.suggestion_id) AS suggestion_id",
      "i.description_source",
      // Task 14: the published row holds both languages natively (migration 000301).
      "i.description AS description",
      "i.description_sv AS description_sv",
    ]) {
      expect(INFO_SQL).toContain(c);
    }
    expect(ARTIFACT_ROWS_SQL).toContain("'scb' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'esef' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'wikidata' AS source");
    // Each artifact leg reads FINAL -- pin the exact FROM clause per leg, not
    // just the source literal, so a leg silently losing FINAL still fails.
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_scb AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_esef AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_wikidata AS a FINAL");
    // SCB's verksamhetsbeskrivning is Swedish; the artifact carries the translator's
    // English rendering beside it (migration 000300) and that is what the Sources table
    // shows -- the Swedish original only for a company the translator has not reached.
    // Both legs stay non-null, so summary is still a plain string, never null.
    expect(ARTIFACT_ROWS_SQL).toContain(
      "ifNull(nullIf(a.activity_description_en, ''), ifNull(a.activity_description, '')) AS summary",
    );
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
});
