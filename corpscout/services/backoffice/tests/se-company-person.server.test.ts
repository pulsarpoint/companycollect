import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyPersonCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  appendSeCompanyPersonCorrection,
  CORRECTIONS_SQL,
  DRAFTS_SQL,
  getSeCompanyPerson,
  PERSON_SQL,
  ROLES_SQL,
  seCompanyPersonId,
  SUGGESTIONS_SQL,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-company-person.server";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

const COMPANY = "5565200028";
const PERSON = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

describe("seCompanyPersonId", () => {
  it("matches the Dagster person_id_for hash", () => {
    expect(seCompanyPersonId(COMPANY, "David Mindus")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "  david   MINDUS ")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "Anna Karin Svensson")).toBe(
      "6942ffc1-e104-ebea-7aa0-ef7377e8a508",
    );
  });
});

describe("appendSeCompanyPersonCorrection", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("refuses when the published evidence hash moved", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "b".repeat(64) }]);

    await expect(
      appendSeCompanyPersonCorrection({
        companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
        payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
        reason: "spelling", activeRoleCodes: new Set(),
      }),
    ).rejects.toThrow(SePersonCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("appends one row with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "a".repeat(64) }]);
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await appendSeCompanyPersonCorrection({
      companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
      payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
      reason: "spelling", activeRoleCodes: new Set(),
    });

    expect(result.correctionId).toMatch(/^[0-9a-f-]{36}$/);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      correction_id: result.correctionId,
      company_id: COMPANY,
      correction_kind: "override_field",
      subject_person_id: PERSON,
      payload: JSON.stringify({ name: "David G. Mindus" }),
      decided_by: "backoffice",
    });
    expect(rows[0].created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });
});

describe("review query SQL text", () => {
  it("DRAFTS_SQL derives Bolagsverket names from first_name/last_name and falls back to name", () => {
    expect(DRAFTS_SQL).toContain(
      "trim(concat(\n      JSONExtractString(source_value_json, 'first_name'), ' ',\n      JSONExtractString(source_value_json, 'last_name')\n    ))",
    );
    expect(DRAFTS_SQL).toContain("JSONExtractString(source_value_json, 'name')");
    expect(DRAFTS_SQL).toContain("FROM corpscout.se_company_person_draft FINAL");
    expect(DRAFTS_SQL).toContain("{draftIds:Array(UUID)}");
  });

  it("PERSON_SQL selects the review columns from se_company_person", () => {
    expect(PERSON_SQL).toContain("FROM corpscout.se_company_person FINAL");
    expect(PERSON_SQL).toContain("draft_set_hash");
    expect(PERSON_SQL).toContain("correction_ids");
    expect(PERSON_SQL).toContain("suggestion_id");
    expect(PERSON_SQL).toContain("merged_into_person_id");
  });

  it("ROLES_SQL selects correction_ids and is_current from se_company_person_role", () => {
    expect(ROLES_SQL).toContain("FROM corpscout.se_company_person_role FINAL");
    expect(ROLES_SQL).toContain("correction_ids");
    expect(ROLES_SQL).toContain("is_current");
  });

  it("SUGGESTIONS_SQL reads the observation table and null-guards is_published", () => {
    expect(SUGGESTIONS_SQL).toContain(
      "FROM corpscout.se_company_person_enrichment_observation",
    );
    expect(SUGGESTIONS_SQL).toContain(
      "ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)",
    );
  });

  it("CORRECTIONS_SQL derives is_current/is_stale/is_applied from the right params", () => {
    expect(CORRECTIONS_SQL).toContain("supersedes_correction_id AS id");
    expect(CORRECTIONS_SQL).toContain("NOT IN (SELECT id FROM superseded)) AS is_current");
    expect(CORRECTIONS_SQL).toContain("{zeroHash:String}");
    expect(CORRECTIONS_SQL).toContain("{draftSetHash:String}");
    expect(CORRECTIONS_SQL).toContain("AS is_stale");
    expect(CORRECTIONS_SQL).toContain("{appliedIds:Array(String)}");
    expect(CORRECTIONS_SQL).toContain("AS is_applied");
  });
});

describe("getSeCompanyPerson", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("threads draft ids, published suggestion id, and applied correction ids into the detail queries", async () => {
    const personRow = {
      person_id: PERSON,
      company_id: COMPANY,
      name: "David Mindus",
      description: null,
      draft_ids: ["draft-1", "draft-2"],
      draft_set_hash: "a".repeat(64),
      correction_ids: ["corr-1"],
      suggestion_id: "sugg-1",
      merged_into_person_id: null,
      model_provider: "deterministic",
      model_name: "bolagsverket",
      prompt_version: "v1",
      updated_at: "2026-08-22 10:00:00.000",
    };
    clickhouse.query
      .mockResolvedValueOnce([personRow])
      .mockResolvedValueOnce([{ draft_id: "draft-1" }])
      .mockResolvedValueOnce([{ role_id: "role-1" }])
      .mockResolvedValueOnce([{ suggestion_id: "sugg-1" }])
      .mockResolvedValueOnce([{ correction_id: "corr-1" }]);

    const result = await getSeCompanyPerson(COMPANY, PERSON);

    expect(result?.person).toEqual(personRow);
    expect(result?.drafts).toEqual([{ draft_id: "draft-1" }]);
    expect(result?.roles).toEqual([{ role_id: "role-1" }]);
    expect(result?.suggestions).toEqual([{ suggestion_id: "sugg-1" }]);
    expect(result?.corrections).toEqual([{ correction_id: "corr-1" }]);

    expect(clickhouse.query).toHaveBeenCalledTimes(5);
    expect(clickhouse.query.mock.calls[0]).toEqual([
      PERSON_SQL,
      { companyId: COMPANY, personId: PERSON },
    ]);
    expect(clickhouse.query.mock.calls[1]).toEqual([
      DRAFTS_SQL,
      { companyId: COMPANY, draftIds: personRow.draft_ids },
    ]);
    expect(clickhouse.query.mock.calls[2]).toEqual([
      ROLES_SQL,
      { companyId: COMPANY, personId: PERSON },
    ]);
    expect(clickhouse.query.mock.calls[3]).toEqual([
      SUGGESTIONS_SQL,
      { companyId: COMPANY, personId: PERSON, publishedSuggestionId: personRow.suggestion_id },
    ]);
    expect(clickhouse.query.mock.calls[4]).toEqual([
      CORRECTIONS_SQL,
      {
        companyId: COMPANY,
        personId: PERSON,
        zeroHash: ZERO_EVIDENCE_HASH,
        draftSetHash: personRow.draft_set_hash,
        appliedIds: personRow.correction_ids,
      },
    ]);
  });

  it("returns null without querying details when the person is missing", async () => {
    clickhouse.query.mockResolvedValueOnce([]);

    const result = await getSeCompanyPerson(COMPANY, PERSON);

    expect(result).toBeNull();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
  });
});
