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
import { SeInfoCorrectionValidationError } from "~/lib/se-info-corrections";

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
    ]) {
      expect(INFO_SQL).toContain(c);
    }
    expect(ARTIFACT_ROWS_SQL).toContain("'scb' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'esef' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'wikidata' AS source");
    expect(SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_info_enrichment_observation AS s");
    expect(SUGGESTIONS_SQL).toContain("ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)");
    // P16 ruling: the two suggestion flags are is_published and is_newest,
    // not a single is_current.
    expect(SUGGESTIONS_SQL).toContain("AS is_newest");
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
