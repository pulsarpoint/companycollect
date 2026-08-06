import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  insertCorrections: vi.fn(),
  query: vi.fn(),
}));

vi.mock("~/lib/clickhouse.server", () => ({
  chInsertPersonCorrections: clickhouse.insertCorrections,
  chQuery: clickhouse.query,
}));

import {
  applyCountryPersonCorrection,
  PersonCorrectionValidationError,
  searchCountryPersonTargets,
} from "~/lib/people.server";

const SOURCE_PERSON_ID = "11111111-1111-4111-8111-111111111111";
const TARGET_PERSON_ID = "22222222-2222-4222-8222-222222222222";
const OBSERVATION_ONE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const OBSERVATION_TWO = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const REVIEW_ID = "33333333-3333-4333-8333-333333333333";
const CORRECTION_ID = "44444444-4444-4444-8444-444444444444";

function summary(personId: string) {
  return {
    country_iso2: "SE",
    person_id: personId,
    preferred_name:
      personId === SOURCE_PERSON_ID ? "Source Person" : "Target Person",
    preferred_name_normalized:
      personId === SOURCE_PERSON_ID ? "source person" : "target person",
    resolution_status: "provisional",
    resolution_method: "same_company_name",
    merged_into_person_id: null,
    first_observed_year: 2023,
    last_observed_year: 2024,
    observation_count: personId === SOURCE_PERSON_ID ? 2 : 1,
    company_count: 1,
    resolved_at: "2026-08-01 12:00:00.000",
  };
}

function observation(observationId: string, personId: string) {
  return {
    observation_id: observationId,
    source: "test_source",
    source_record_id: `record-${observationId}`,
    source_person_key: `person-${observationId}`,
    company_id:
      personId === SOURCE_PERSON_ID ? "source-company" : "target-company",
    company_name: personId === SOURCE_PERSON_ID ? "Source AB" : "Target AB",
    observed_first_name: "Test",
    observed_last_name: "Person",
    observed_full_name: "Test Person",
    role_original: "Board member",
    role_kind: "board_member",
    signatory_kind: "board",
    fiscal_year: 2024,
    source_statement_key: `statement-${observationId}`,
    match_method: "same_company_name",
    match_status: "provisional",
    confidence: 70,
  };
}

function installIdentityQueries(): void {
  clickhouse.query.mockImplementation(
    (sql: string, params?: Record<string, unknown>) => {
      const personId = String(params?.personId ?? "");
      if (sql.includes("FROM country_person AS p")) {
        return Promise.resolve([summary(personId)]);
      }
      if (sql.includes("FROM country_person_observation AS o")) {
        return Promise.resolve(
          personId === SOURCE_PERSON_ID
            ? [
                observation(OBSERVATION_ONE, SOURCE_PERSON_ID),
                observation(OBSERVATION_TWO, SOURCE_PERSON_ID),
              ]
            : [observation("cccccccc-cccc-4ccc-8ccc-cccccccccccc", personId)],
        );
      }
      if (sql.includes("FROM country_person_identifier")) {
        return Promise.resolve([]);
      }
      if (
        sql.includes("FROM country_person_correction AS c") &&
        sql.includes("c.from_person_id = {personId:UUID}")
      ) {
        return Promise.resolve([]);
      }
      if (
        sql.includes("SELECT count() AS total") &&
        sql.includes("current_target")
      ) {
        return Promise.resolve([{ total: "0" }]);
      }
      if (
        sql.includes("argMax(c.correction_id") &&
        sql.includes("GROUP BY c.observation_id")
      ) {
        return Promise.resolve([]);
      }
      if (
        sql.includes("c.observation_id IS NULL") &&
        sql.includes("GROUP BY c.from_person_id")
      ) {
        return Promise.resolve([]);
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  );
}

describe("reviewed country-person corrections", () => {
  beforeEach(() => {
    clickhouse.insertCorrections.mockReset();
    clickhouse.query.mockReset();
    installIdentityQueries();
  });

  it("rejects malformed country identities before reading or writing", async () => {
    await expect(
      applyCountryPersonCorrection({
        kind: "split",
        countryIso2: "Sweden",
        sourcePersonId: SOURCE_PERSON_ID,
        observationIds: [OBSERVATION_ONE],
        reason: "Namesake",
      }),
    ).rejects.toBeInstanceOf(PersonCorrectionValidationError);

    expect(clickhouse.query).not.toHaveBeenCalled();
    expect(clickhouse.insertCorrections).not.toHaveBeenCalled();
  });

  it("searches correction targets inside the source country", async () => {
    clickhouse.query.mockResolvedValueOnce([summary(TARGET_PERSON_ID)]);

    await expect(
      searchCountryPersonTargets("se", "Target", SOURCE_PERSON_ID),
    ).resolves.toEqual([summary(TARGET_PERSON_ID)]);

    expect(clickhouse.query).toHaveBeenCalledWith(
      expect.stringContaining("p.country_iso2 = {country:String}"),
      expect.objectContaining({
        country: "SE",
        sourcePersonId: SOURCE_PERSON_ID,
        normalized: "target",
        pattern: "%target%",
      }),
    );
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "p.person_id != {sourcePersonId:UUID}",
    );
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "p.resolution_status != 'merged'",
    );
  });

  it("does not query targets for malformed source ids or short terms", async () => {
    await expect(
      searchCountryPersonTargets("SE", "T", SOURCE_PERSON_ID),
    ).resolves.toEqual([]);
    await expect(
      searchCountryPersonTargets("SE", "Target", "not-a-person-id"),
    ).resolves.toEqual([]);

    expect(clickhouse.query).not.toHaveBeenCalled();
  });

  it("appends a split decision without changing the source observation", async () => {
    const result = await applyCountryPersonCorrection({
      kind: "split",
      countryIso2: "se",
      sourcePersonId: SOURCE_PERSON_ID,
      observationIds: [OBSERVATION_ONE],
      reason: "  Two different people  ",
    });

    expect(result.correctionCount).toBe(1);
    expect(result.targetPersonId).not.toBe(SOURCE_PERSON_ID);
    expect(clickhouse.insertCorrections).toHaveBeenCalledOnce();
    expect(clickhouse.insertCorrections.mock.calls[0][0]).toEqual([
      expect.objectContaining({
        country_iso2: "SE",
        observation_id: OBSERVATION_ONE,
        from_person_id: SOURCE_PERSON_ID,
        to_person_id: result.targetPersonId,
        correction_kind: "split",
        reason: "Two different people",
        decided_by: "backoffice",
        supersedes_correction_id: null,
      }),
    ]);
    expect(
      clickhouse.query.mock.calls.some(([sql]) =>
        String(sql).includes(
          "argMax(c.correction_id, (c.created_at, c.correction_id))",
        ),
      ),
    ).toBe(true);
  });

  it("records a country-local merge as one person-level decision", async () => {
    const result = await applyCountryPersonCorrection({
      kind: "merge",
      countryIso2: "SE",
      sourcePersonId: SOURCE_PERSON_ID,
      targetPersonId: TARGET_PERSON_ID,
      reason: "Same officer confirmed from filings",
    });

    expect(result).toEqual({
      reviewId: expect.stringMatching(/^[0-9a-f-]{36}$/),
      targetPersonId: TARGET_PERSON_ID,
      correctionCount: 1,
    });
    expect(clickhouse.insertCorrections.mock.calls[0][0]).toEqual([
      expect.objectContaining({
        country_iso2: "SE",
        observation_id: null,
        from_person_id: SOURCE_PERSON_ID,
        to_person_id: TARGET_PERSON_ID,
        correction_kind: "merge",
      }),
    ]);
    expect(
      clickhouse.query.mock.calls.some(([sql]) =>
        String(sql).includes(
          "argMax(c.correction_id, (c.created_at, c.correction_id))",
        ),
      ),
    ).toBe(true);
  });

  it("undoes by appending a superseding decision", async () => {
    clickhouse.query.mockImplementation(
      (sql: string, params?: Record<string, unknown>) => {
        const personId = String(params?.personId ?? "");
        if (sql.includes("FROM country_person AS p")) {
          return Promise.resolve([summary(personId)]);
        }
        if (sql.includes("FROM country_person_observation AS o")) {
          return Promise.resolve([
            observation(OBSERVATION_ONE, SOURCE_PERSON_ID),
            observation(OBSERVATION_TWO, SOURCE_PERSON_ID),
          ]);
        }
        if (sql.includes("FROM country_person_identifier")) {
          return Promise.resolve([]);
        }
        if (
          sql.includes("FROM country_person_correction AS c") &&
          sql.includes("c.review_id = {reviewId:UUID}")
        ) {
          return Promise.resolve([
            {
              correction_id: CORRECTION_ID,
              review_id: REVIEW_ID,
              observation_id: OBSERVATION_ONE,
              from_person_id: SOURCE_PERSON_ID,
              to_person_id: TARGET_PERSON_ID,
              correction_kind: "reassign",
              reason: "Original review",
              decided_by: "First reviewer",
              supersedes_correction_id: null,
              created_at: "2026-07-31 12:00:00.000",
              is_current: 1,
            },
          ]);
        }
        if (sql.includes("FROM country_person_correction AS c")) {
          return Promise.resolve([]);
        }
        throw new Error(`Unexpected query: ${sql}`);
      },
    );

    const result = await applyCountryPersonCorrection({
      kind: "undo",
      countryIso2: "SE",
      sourcePersonId: SOURCE_PERSON_ID,
      reviewId: REVIEW_ID,
      reason: "Review was based on the wrong filing",
    });

    expect(result.targetPersonId).toBe(SOURCE_PERSON_ID);
    expect(clickhouse.insertCorrections.mock.calls[0][0]).toEqual([
      expect.objectContaining({
        observation_id: OBSERVATION_ONE,
        from_person_id: TARGET_PERSON_ID,
        to_person_id: SOURCE_PERSON_ID,
        correction_kind: "undo",
        supersedes_correction_id: CORRECTION_ID,
      }),
    ]);
  });
});
