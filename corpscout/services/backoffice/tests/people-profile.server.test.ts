import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));

vi.mock("~/lib/clickhouse.server", () => ({
  chInsertPersonCorrections: vi.fn(),
  chQuery: clickhouse.query,
}));

import {
  findPossibleCountryPersonMatches,
  getCountryPerson,
  resolveCountryPersonProfilesForCompany,
  type CountryPersonObservation,
  type CountryPersonSummary,
} from "~/lib/people.server";

const PERSON_ID = "11111111-1111-4111-8111-111111111111";
const POSSIBLE_MATCH_ID = "22222222-2222-4222-8222-222222222222";

function personSummary(
  personId = PERSON_ID,
  name = "Niklas Thorén",
): CountryPersonSummary {
  return {
    country_iso2: "SE",
    person_id: personId,
    preferred_name: name,
    preferred_name_normalized: name.toLowerCase(),
    resolution_status: "provisional",
    resolution_method: "same_company_name",
    merged_into_person_id: null,
    first_observed_year: 2024,
    last_observed_year: 2025,
    observation_count: 2,
    company_count: 1,
    resolved_at: "2026-08-14 10:00:00.000",
  };
}

function observation(
  roleKind: string,
  signatoryKind: string,
): CountryPersonObservation {
  return {
    observation_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    source: "se_xbrl_signatures",
    source_record_id: "statement-1",
    source_person_key: `${signatoryKind}|1`,
    company_id: "5591408504",
    company_name: "2004 Wecall AB",
    observed_first_name: "Johan",
    observed_last_name: "Lindström",
    observed_full_name: "Johan Lindström",
    role_original: roleKind === "auditor" ? "Auktoriserad revisor" : "",
    role_kind: roleKind,
    signatory_kind: signatoryKind,
    fiscal_year: 2025,
    source_statement_key: "statement-1",
    match_method: "same_company_name",
    match_status: "provisional",
    confidence: 70,
  };
}

describe("country person profile data", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
  });

  it("resolves a legacy company name only when it has one person identity", async () => {
    clickhouse.query.mockResolvedValue([
      {
        observed_name_normalized: "niklas thorén",
        person_id: PERSON_ID,
      },
    ]);

    const result = await resolveCountryPersonProfilesForCompany(
      "se",
      "5591408504",
      [" Niklas  Thorén ", "Niklas Thorén"],
    );

    expect(result).toEqual(new Map([["niklas thorén", PERSON_ID]]));
    expect(clickhouse.query).toHaveBeenCalledWith(
      expect.stringContaining("HAVING uniqExact(m.person_id) = 1"),
      {
        country: "SE",
        companyId: "5591408504",
        names: ["niklas thorén"],
      },
    );
  });

  it("keeps public person contacts separate from identity identifiers", async () => {
    clickhouse.query.mockImplementation((sql?: string) => {
      const statement = String(sql ?? "");
      if (statement.includes("FROM country_person AS p")) {
        return Promise.resolve([personSummary()]);
      }
      if (statement.includes("FROM country_person_observation AS o")) {
        return Promise.resolve([]);
      }
      if (statement.includes("FROM country_person_identifier")) {
        return Promise.resolve([
          {
            identifier_id: "email-id",
            source: "official_bio",
            identifier_kind: "email",
            identifier_value: "niklas@example.test",
            observation_id: "observation-1",
            is_public: 1,
          },
          {
            identifier_id: "registry-id",
            source: "official_registry",
            identifier_kind: "director_id",
            identifier_value: "director-123",
            observation_id: "observation-1",
            is_public: 1,
          },
          {
            identifier_id: "private-email-id",
            source: "restricted_source",
            identifier_kind: "email",
            identifier_value: "private@example.test",
            observation_id: "observation-1",
            is_public: 0,
          },
        ]);
      }
      if (statement.includes("FROM country_person_correction AS c")) {
        return Promise.resolve([]);
      }
      throw new Error(`Unexpected query: ${statement || "<empty>"}`);
    });

    const detail = await getCountryPerson("se", PERSON_ID);

    expect(detail?.contacts).toEqual([
      {
        contact_kind: "email",
        contact_value: "niklas@example.test",
        source: "official_bio",
        observation_id: "observation-1",
      },
    ]);
    expect(
      detail?.identifiers.map((identifier) => identifier.identifier_kind),
    ).toEqual(["director_id"]);
  });

  it("groups exact-name identities only for compatible relationship types", async () => {
    clickhouse.query.mockResolvedValue([
      {
        ...personSummary(POSSIBLE_MATCH_ID),
        company_id: "5560000001",
        company_name: "First AB",
        role_kind: "auditor",
        role_original: "Auktoriserad revisor",
        relationship_kind: "external_audit",
        first_year: 2022,
        last_year: 2024,
        company_observation_count: 3,
      },
      {
        ...personSummary(POSSIBLE_MATCH_ID),
        company_id: "5560000002",
        company_name: "Second AB",
        role_kind: "auditor",
        role_original: "Auktoriserad revisor",
        relationship_kind: "external_audit",
        first_year: 2025,
        last_year: 2025,
        company_observation_count: 1,
      },
    ]);

    const result = await findPossibleCountryPersonMatches(personSummary(), [
      observation("unknown", "board_signature"),
      observation("auditor", "auditor"),
    ]);

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      person: { person_id: POSSIBLE_MATCH_ID },
      reason: "compatible_relationship_and_name",
    });
    expect(result[0].connections).toHaveLength(2);
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "p.preferred_name_normalized = {normalizedName:String}",
    );
    expect(clickhouse.query.mock.calls[0][1]).toMatchObject({
      relationshipKinds: ["external_audit"],
    });
  });
});
