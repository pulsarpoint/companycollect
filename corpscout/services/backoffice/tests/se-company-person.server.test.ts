import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyPersonCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  appendSeCompanyPersonCorrection,
  approveMergeSuggestion,
  COLLISION_CANDIDATES_SQL,
  CORRECTIONS_SQL,
  DECIDED_CANDIDATE_GROUPS_SQL,
  DRAFTS_SQL,
  getSeCompanyPerson,
  keepSeparateMergeSuggestion,
  listStaleSeCompanyPersonCorrections,
  loadSeCompanyPersonCollisionReview,
  MERGE_GROUP_LIVE_SQL,
  MERGE_SUGGESTION_BY_ID_SQL,
  MERGE_SUGGESTIONS_FOR_COMPANY_SQL,
  PERSON_SQL,
  revalidateMergeSuggestion,
  STALE_CORRECTIONS_SQL,
  ROLES_SQL,
  seCompanyPersonId,
  SUGGESTIONS_SQL,
} from "~/lib/se-company-person.server";
import {
  SePersonCorrectionValidationError,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-person-corrections";
import type { SeMergeSuggestionPayload } from "~/lib/se-person-merge-suggestions";

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
    expect(DRAFTS_SQL).toContain("FROM source_observations");
    expect(DRAFTS_SQL).toContain("{draftIds:Array(UUID)}");
  });

  it("DRAFTS_SQL reads the three source views, never se_company_person_draft (Task 5)", () => {
    // The draft table is retired in Task 6; nothing in the backoffice may read
    // it any more (rg se_company_person_draft must come back empty).
    expect(DRAFTS_SQL).not.toContain("se_company_person_draft");
    for (const view of [
      "corpscout.se_company_person_bolagsverket",
      "corpscout.se_company_person_esef",
      "corpscout.se_company_person_wikidata",
    ]) {
      expect(DRAFTS_SQL).toContain(`FROM ${view}`);
    }
    // Blank-name exclusion, mirroring source_views.py's shared CTE.
    expect(DRAFTS_SQL.match(/WHERE trim\(full_name\) != ''/g)).toHaveLength(3);
  });

  it("DRAFTS_SQL computes draft_id with the same v2 hash formula as dagster_v3's shared CTE", () => {
    // Byte-for-byte port of source_views.py's _SOURCE_OBSERVATION_ID_SQL: same
    // hash domain, same field order, same per-branch disambiguator folded in
    // (Task 3's fix round -- without it two rows can collide onto one
    // draft_id). This MUST match, because person.draft_ids (the IN filter
    // below) was populated by exactly that Python-side SQL.
    expect(DRAFTS_SQL).toContain("se-company-person-source-observation-v2");
    expect(DRAFTS_SQL).toContain("reinterpretAsUUID(unhex(substring(hex(SHA256(concat(");
    expect(DRAFTS_SQL).toContain("toString(signatory_uid)");
    expect(DRAFTS_SQL).toContain("toString(candidate_uid)");
    expect(DRAFTS_SQL).toContain("toString(company_wikidata_id)");
  });

  it("DRAFTS_SQL scopes each branch by company_id, not just the outer WHERE", () => {
    // Pushed into every UNION branch so a single person's evidence read does
    // not rescan every SE person in all three source tables.
    expect(DRAFTS_SQL.match(/AND company_id = \{companyId:String\}/g)).toHaveLength(3);
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
      "WHERE company_id = {companyId:String} AND draft_id IN {draftIds:Array(UUID)}",
    );
    expect(DRAFTS_SQL).not.toContain("se_company_person_draft");
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

/* -------------------------------------------------------------------- */
/* Collision-candidate + merge-suggestion review (SE People Experiment   */
/* Task 5)                                                                */
/* -------------------------------------------------------------------- */

const INTO_PERSON = PERSON;
const FROM_PERSON = "11111111-1111-4111-8111-111111111111";
const GROUP_ID = "grp-1";
const MERGE_SUGGESTION_ID = "99999999-9999-4999-8999-999999999999";

function mergeSuggestionPayload(
  overrides: Partial<SeMergeSuggestionPayload> = {},
): SeMergeSuggestionPayload {
  return {
    candidate_group_id: GROUP_ID,
    decision: "merge",
    confidence: 0.9,
    rationale: "Same person, middle name variant.",
    into_person_id: INTO_PERSON,
    from_person_ids: [FROM_PERSON],
    member_person_ids: [INTO_PERSON, FROM_PERSON],
    ...overrides,
  };
}

describe("collision-candidate review SQL text", () => {
  it("scopes collision candidates and decided groups by company_id", () => {
    expect(COLLISION_CANDIDATES_SQL).toContain(
      "FROM corpscout.se_company_person_collision_candidate",
    );
    expect(COLLISION_CANDIDATES_SQL).toContain("WHERE company_id = {companyId:String}");
    expect(DECIDED_CANDIDATE_GROUPS_SQL).toContain(
      "correction_kind IN ('merge_persons', 'keep_separate')",
    );
    // Mirrors merge.py's own decided-marker read: a decision an undo
    // superseded must not keep a group stuck as "decided" forever.
    expect(DECIDED_CANDIDATE_GROUPS_SQL).toContain("supersedes_correction_id");
    expect(MERGE_SUGGESTIONS_FOR_COMPANY_SQL).toContain(
      "JSONExtractString(s.suggestion, 'candidate_group_id') != ''",
    );
  });

  it("MERGE_GROUP_LIVE_SQL reads live, non-tombstoned people fresh from se_company_person", () => {
    expect(MERGE_GROUP_LIVE_SQL).toContain("FROM corpscout.se_company_person FINAL");
    expect(MERGE_GROUP_LIVE_SQL).toContain("merged_into_person_id IS NULL");
    expect(MERGE_GROUP_LIVE_SQL).toContain("{personIds:Array(UUID)}");
  });

  it("MERGE_SUGGESTION_BY_ID_SQL reads draft_ids for the revalidation check", () => {
    expect(MERGE_SUGGESTION_BY_ID_SQL).toContain(
      "arrayMap(id -> toString(id), s.draft_ids) AS draft_ids",
    );
  });
});

describe("loadSeCompanyPersonCollisionReview", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("groups candidate rows by candidate_group_id, attaches the newest merge suggestion, and marks decided groups", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          candidate_group_id: GROUP_ID,
          person_key: "anna svensson",
          full_name: "Anna Svensson",
          source: "bolagsverket",
          source_record_uid: "u1",
        },
        {
          candidate_group_id: GROUP_ID,
          person_key: "anna maria svensson",
          full_name: "Anna Maria Svensson",
          source: "esef",
          source_record_uid: "u2",
        },
        {
          candidate_group_id: "grp-2",
          person_key: "erik eriksson",
          full_name: "Erik Eriksson",
          source: "wikidata",
          source_record_uid: "u3",
        },
      ])
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload()),
          created_at: "2026-08-27 09:00:00.000",
        },
        // A row this reader did not write (an ordinary profile suggestion,
        // sharing the same table) -- must be skipped, not crash.
        {
          suggestion_id: "not-a-merge-suggestion",
          suggestion: JSON.stringify({ name: "Someone" }),
          created_at: "2026-08-27 08:00:00.000",
        },
      ])
      .mockResolvedValueOnce([{ candidate_group_id: "grp-2" }]);

    const groups = await loadSeCompanyPersonCollisionReview(COMPANY);

    expect(groups).toHaveLength(2);
    const grp1 = groups.find((group) => group.candidate_group_id === GROUP_ID);
    expect(grp1?.members).toHaveLength(2);
    expect(grp1?.suggestion?.decision).toBe("merge");
    expect(grp1?.suggestion?.into_person_id).toBe(INTO_PERSON);
    expect(grp1?.is_decided).toBe(false);

    const grp2 = groups.find((group) => group.candidate_group_id === "grp-2");
    expect(grp2?.suggestion).toBeNull();
    expect(grp2?.is_decided).toBe(true);
  });
});

describe("revalidateMergeSuggestion", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("is ok when every group member is live and its drafts are still owned by the group", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
    ]);

    const result = await revalidateMergeSuggestion(COMPANY, mergeSuggestionPayload(), [
      "d1",
      "d2",
    ]);

    expect(result).toEqual({
      ok: true,
      evidenceHashByPersonId: { [INTO_PERSON]: "a".repeat(64), [FROM_PERSON]: "b".repeat(64) },
    });
    expect(clickhouse.query).toHaveBeenCalledWith(MERGE_GROUP_LIVE_SQL, {
      companyId: COMPANY,
      personIds: [INTO_PERSON, FROM_PERSON],
    });
  });

  it("refuses when a group member is no longer published", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      // FROM_PERSON: no row at all -- split/reassigned away since the
      // suggestion was written.
    ]);

    const result = await revalidateMergeSuggestion(COMPANY, mergeSuggestionPayload(), ["d1"]);

    expect(result.ok).toBe(false);
    expect(result.ok === false && result.reason).toMatch(/no longer published/);
  });

  it("refuses when a group member was already merged elsewhere", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 0 },
    ]);

    const result = await revalidateMergeSuggestion(COMPANY, mergeSuggestionPayload(), [
      "d1",
      "d2",
    ]);

    expect(result.ok).toBe(false);
    expect(result.ok === false && result.reason).toMatch(/already merged elsewhere/);
  });

  it("refuses when the suggestion's evidence moved out of the group since it was written", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
    ]);

    const result = await revalidateMergeSuggestion(COMPANY, mergeSuggestionPayload(), [
      "d1",
      "d-moved-away",
    ]);

    expect(result.ok).toBe(false);
    expect(result.ok === false && result.reason).toMatch(/evidence moved/);
  });
});

const SECOND_FROM_PERSON = "22222222-2222-4222-8222-222222222222";

describe("approveMergeSuggestion", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("writes one merge_persons correction per from_person_id after live re-validation", async () => {
    const liveRows = [
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
    ];
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload()),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce(liveRows) // upfront revalidation
      .mockResolvedValueOnce(liveRows); // immediately-before-INSERT recheck
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await approveMergeSuggestion({
      companyId: COMPANY,
      suggestionId: MERGE_SUGGESTION_ID,
      reason: "LLM agreed, matches the register",
    });

    expect(result.correctionIds).toHaveLength(1);
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0]).toMatchObject({
      correction_kind: "merge_persons",
      subject_person_id: FROM_PERSON,
      target_person_id: INTO_PERSON,
      payload: JSON.stringify({ candidate_group_id: GROUP_ID }),
      evidence_hash: "b".repeat(64),
    });
  });

  it("writes every from_person_id's correction in ONE atomic INSERT when the group has more than two members", async () => {
    // Coordinator review item 1: an N-way merge cannot be one correction ROW
    // (apply_person_corrections' merge_persons handler reads only a row's own
    // singular subject_person_id/target_person_id -- never `payload` -- so an
    // array payload would silently merge just one of these and drop the
    // rest). What IS achievable and IS asserted here: every row lands in a
    // SINGLE `chInsertSeCompanyPersonCorrections` call, not N independent
    // ones -- so the group can never be observed half-merged.
    const payload = mergeSuggestionPayload({
      from_person_ids: [FROM_PERSON, SECOND_FROM_PERSON],
      member_person_ids: [INTO_PERSON, FROM_PERSON, SECOND_FROM_PERSON],
    });
    const liveRows = [
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
      { person_id: SECOND_FROM_PERSON, draft_set_hash: "c".repeat(64), draft_ids: ["d3"], is_live: 1 },
    ];
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(payload),
          draft_ids: ["d1", "d2", "d3"],
        },
      ])
      .mockResolvedValueOnce(liveRows)
      .mockResolvedValueOnce(liveRows);
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await approveMergeSuggestion({
      companyId: COMPANY,
      suggestionId: MERGE_SUGGESTION_ID,
      reason: "approve the whole group",
    });

    expect(result.correctionIds).toHaveLength(2);
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0] as [Array<Record<string, unknown>>];
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.subject_person_id).sort()).toEqual(
      [FROM_PERSON, SECOND_FROM_PERSON].sort(),
    );
    for (const row of rows) {
      expect(row.target_person_id).toBe(INTO_PERSON);
      expect(row.correction_kind).toBe("merge_persons");
      expect(JSON.parse(row.payload as string)).toEqual({ candidate_group_id: GROUP_ID });
    }
  });

  it("is all-or-nothing: a failed batch insert leaves zero rows written, never a half-merged group", async () => {
    const payload = mergeSuggestionPayload({
      from_person_ids: [FROM_PERSON, SECOND_FROM_PERSON],
      member_person_ids: [INTO_PERSON, FROM_PERSON, SECOND_FROM_PERSON],
    });
    const liveRows = [
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
      { person_id: SECOND_FROM_PERSON, draft_set_hash: "c".repeat(64), draft_ids: ["d3"], is_live: 1 },
    ];
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(payload),
          draft_ids: ["d1", "d2", "d3"],
        },
      ])
      .mockResolvedValueOnce(liveRows)
      .mockResolvedValueOnce(liveRows);
    clickhouse.insert.mockRejectedValueOnce(new Error("network reset mid-batch"));

    await expect(
      approveMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "approve the whole group",
      }),
    ).rejects.toThrow("network reset mid-batch");
    // The whole group's evidence moves as ONE call; there is no partial
    // state a reload could observe, and DECIDED_CANDIDATE_GROUPS_SQL cannot
    // flip the group to "decided" because no correction row exists at all.
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
  });

  it("REFUSES at write time when a subject was tombstoned in the gap after the upfront revalidation", async () => {
    // Coordinator review item 2: merge-tombstoning sets merged_into_person_id
    // WITHOUT touching draft_set_hash, so this can only be caught by a fresh
    // is_live check immediately before the INSERT -- a hash-only recheck
    // would not see it (draft_set_hash is IDENTICAL in both reads below).
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload()),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce([
        { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
        { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
      ]) // upfront revalidation: both live
      .mockResolvedValueOnce([
        { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
        // FROM_PERSON merged into someone else by a DIFFERENT correction in
        // the gap between the two reads -- same hash, is_live flipped to 0.
        { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 0 },
      ]);

    let caught: unknown;
    try {
      await approveMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "approve",
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(SePersonCorrectionValidationError);
    expect((caught as Error).message).toMatch(/stale while saving/);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("REFUSES with a clear message and writes nothing when the suggestion has gone stale", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload()),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce([
        { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
        // FROM_PERSON already merged into someone else by the time this is
        // reviewed -- the carry-forward requirement this guard exists for.
      ]);

    let caught: unknown;
    try {
      await approveMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "approve",
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(SePersonCorrectionValidationError);
    expect((caught as Error).message).toMatch(/no longer published/);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("refuses a suggestion row that is not a recognizable merge suggestion", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        suggestion_id: MERGE_SUGGESTION_ID,
        suggestion: JSON.stringify({ name: "Someone", description: null }),
        draft_ids: [],
      },
    ]);

    await expect(
      approveMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "approve",
      }),
    ).rejects.toThrow(SePersonCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("refuses a suggestion id that does not exist", async () => {
    clickhouse.query.mockResolvedValueOnce([]);

    await expect(
      approveMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "approve",
      }),
    ).rejects.toThrow(SePersonCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });
});

describe("keepSeparateMergeSuggestion", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("writes one keep_separate correction anchored on into_person_id", async () => {
    const liveRows = [
      { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
      { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
    ];
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload({ decision: "keep_separate" })),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce(liveRows) // upfront revalidation
      .mockResolvedValueOnce([liveRows[0]]); // immediately-before-INSERT recheck (into_person_id only)
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await keepSeparateMergeSuggestion({
      companyId: COMPANY,
      suggestionId: MERGE_SUGGESTION_ID,
      reason: "Different people, same name",
    });

    expect(result.correctionId).toMatch(/^[0-9a-f-]{36}$/);
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0]).toMatchObject({
      correction_kind: "keep_separate",
      subject_person_id: INTO_PERSON,
      target_person_id: null,
      payload: JSON.stringify({ candidate_group_id: GROUP_ID }),
      evidence_hash: "a".repeat(64),
    });
  });

  it("REFUSES at write time when into_person_id was tombstoned in the gap after the upfront revalidation", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload({ decision: "keep_separate" })),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce([
        { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 1 },
        { person_id: FROM_PERSON, draft_set_hash: "b".repeat(64), draft_ids: ["d2"], is_live: 1 },
      ])
      .mockResolvedValueOnce([
        // Same hash, but merged elsewhere by another correction in the gap.
        { person_id: INTO_PERSON, draft_set_hash: "a".repeat(64), draft_ids: ["d1"], is_live: 0 },
      ]);

    let caught: unknown;
    try {
      await keepSeparateMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "keep separate",
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(SePersonCorrectionValidationError);
    expect((caught as Error).message).toMatch(/stale while saving/);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("REFUSES with a clear message and writes nothing when the suggestion has gone stale", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          suggestion_id: MERGE_SUGGESTION_ID,
          suggestion: JSON.stringify(mergeSuggestionPayload()),
          draft_ids: ["d1", "d2"],
        },
      ])
      .mockResolvedValueOnce([]); // neither member is live any more

    let caught: unknown;
    try {
      await keepSeparateMergeSuggestion({
        companyId: COMPANY,
        suggestionId: MERGE_SUGGESTION_ID,
        reason: "keep separate",
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(SePersonCorrectionValidationError);
    expect((caught as Error).message).toMatch(/no longer published/);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });
});
