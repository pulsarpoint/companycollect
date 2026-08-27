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
  listStaleSeCompanyPersonCorrections,
  PERSON_SQL,
  STALE_CORRECTIONS_SQL,
  ROLES_SQL,
  seCompanyPersonId,
  SUGGESTIONS_SQL,
} from "~/lib/se-company-person.server";
import {
  SePersonCorrectionValidationError,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-person-corrections";

const COMPANY = "5565200028";
const PERSON = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

describe("seCompanyPersonId", () => {
  it("matches the Dagster person_id_for hash (v2 domain, K2 canonical key)", () => {
    // Expected values from dagster_v3's person_id_for(COMPANY, identity_key_k2(name)) --
    // se-people-experiment Task 3 moved the hash off the v1 domain (first|last token, K1)
    // to v2, keyed by an already-canonical K2 key (all tokens, not just first+last).
    const DAVID_MINDUS = "4e2390d6-9bc2-9ca9-7846-2154e5bdfe48";
    expect(seCompanyPersonId(COMPANY, "David Mindus")).toBe(DAVID_MINDUS);
    expect(seCompanyPersonId(COMPANY, "  david   MINDUS ")).toBe(DAVID_MINDUS);
    // M6 (fix round, minor): SHARED_VECTOR -- this exact (company_id, name, uuid) triple is
    // ALSO asserted in dagster_v3's tests/test_se_company_person_normalization.py
    // (test_shared_cross_language_vector_matches_the_typescript_twin), independently
    // computed in Node during the fix round and confirmed to match Python's
    // person_id_for("5565200028", identity_key_k2("Anna Svensson")) before either test was
    // written. A byte-for-byte divergence between the two implementations fails on either
    // side.
    expect(seCompanyPersonId(COMPANY, "Anna Svensson")).toBe(
      "a95ef2f2-b817-c3f7-2ecf-e78d42acfc10",
    );
    // A middle name changes the K2 key (unlike K1, which dropped it): a different hash
    // from "Anna Svensson", proving this is no longer first|last-token grouping.
    expect(seCompanyPersonId(COMPANY, "Anna Karin Svensson")).toBe(
      "8b94d500-5c3a-f2b2-193e-8aef7b16e074",
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
    expect(DRAFTS_SQL).toContain("FROM corpscout.se_company_person_draft AS d FINAL");
    expect(DRAFTS_SQL).toContain("{draftIds:Array(UUID)}");
  });

  it("PERSON_SQL selects the review columns from se_company_person", () => {
    expect(PERSON_SQL).toContain("FROM corpscout.se_company_person AS p FINAL");
    expect(PERSON_SQL).toContain("draft_set_hash");
    expect(PERSON_SQL).toContain("correction_ids");
    expect(PERSON_SQL).toContain("suggestion_id");
    expect(PERSON_SQL).toContain("merged_into_person_id");
  });

  it("filters on the table's own columns, never on the String aliases", () => {
    // `toString(person_id) AS person_id` shadows the UUID column, so an
    // unqualified `person_id = {personId:UUID}` dies with NO_COMMON_TYPE and
    // `draft_id IN {draftIds:Array(UUID)}` silently matches nothing.
    expect(PERSON_SQL).toContain(
      "WHERE p.company_id = {companyId:String} AND p.person_id = {personId:UUID}",
    );
    expect(DRAFTS_SQL).toContain(
      "WHERE d.company_id = {companyId:String} AND d.draft_id IN {draftIds:Array(UUID)}",
    );
    expect(DRAFTS_SQL).toContain("FROM corpscout.se_company_person_draft AS d FINAL");
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

  it("SUGGESTIONS_SQL marks the rows whose evidence still matches the published one", () => {
    expect(SUGGESTIONS_SQL).toContain("AS is_current");
    expect(SUGGESTIONS_SQL).toContain(
      "WHERE suggestion_id = {publishedSuggestionId:Nullable(UUID)}",
    );
  });

  it("CORRECTIONS_SQL derives is_current/is_stale/is_applied from the right params", () => {
    expect(CORRECTIONS_SQL).toContain("supersedes_correction_id AS id");
    expect(CORRECTIONS_SQL).toContain("NOT IN (SELECT id FROM superseded)) AS is_current");
    expect(CORRECTIONS_SQL).toContain("{zeroHash:String}");
    expect(CORRECTIONS_SQL).toContain("AS is_stale");
    expect(CORRECTIONS_SQL).toContain("{appliedIds:Array(String)}");
    expect(CORRECTIONS_SQL).toContain("AS is_applied");
  });

  it("CORRECTIONS_SQL measures staleness against the row's own subject", () => {
    // A merge or reassign shown on the destination's page is about the SUBJECT's
    // evidence; comparing it with the page person's hash marks it stale on sight.
    expect(CORRECTIONS_SQL).toContain(
      "LEFT JOIN corpscout.se_company_person AS subj FINAL",
    );
    expect(CORRECTIONS_SQL).toContain("subj.person_id = c.subject_person_id");
    expect(CORRECTIONS_SQL).toContain("toString(subj.draft_set_hash)");
    expect(CORRECTIONS_SQL).not.toContain("{draftSetHash:String}");
  });
});

describe("listStaleSeCompanyPersonCorrections", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("lists live rows the pipeline cannot apply, with the reason for each", () => {
    expect(STALE_CORRECTIONS_SQL).toContain(
      "LEFT JOIN corpscout.se_company_person AS subj FINAL",
    );
    expect(STALE_CORRECTIONS_SQL).toContain(
      "ON subj.company_id = c.company_id AND subj.person_id = c.subject_person_id",
    );
    expect(STALE_CORRECTIONS_SQL).toContain(
      "c.correction_id NOT IN (SELECT id FROM superseded)",
    );
    expect(STALE_CORRECTIONS_SQL).toContain("c.correction_kind != 'undo'");
    expect(STALE_CORRECTIONS_SQL).toContain(
      "toString(c.evidence_hash) != {zeroHash:String}",
    );
    for (const reason of [
      "AS subject_missing",
      "AS evidence_moved",
      "AS drafts_missing",
    ]) {
      expect(STALE_CORRECTIONS_SQL).toContain(reason);
    }
    expect(STALE_CORRECTIONS_SQL).toContain("hasAll(subj.draft_ids, c.draft_ids)");
    expect(STALE_CORRECTIONS_SQL).toContain("ORDER BY c.created_at DESC\nLIMIT 500");
  });

  it("passes the zero hash as a named parameter", async () => {
    clickhouse.query.mockResolvedValueOnce([{ correction_id: "corr-1" }]);

    const rows = await listStaleSeCompanyPersonCorrections();

    expect(rows).toEqual([{ correction_id: "corr-1" }]);
    expect(clickhouse.query.mock.calls[0]).toEqual([
      STALE_CORRECTIONS_SQL,
      { zeroHash: ZERO_EVIDENCE_HASH },
    ]);
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
