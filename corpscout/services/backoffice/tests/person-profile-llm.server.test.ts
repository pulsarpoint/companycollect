import { describe, expect, it } from "vitest";
import {
  buildPersonProfileEvidenceMap,
  buildPersonProfileLlmInput,
  parsePersonProfileSuggestion,
} from "~/lib/person-profile-llm.server";
import type {
  SwedenPeopleDraftSourceObservation,
  SwedenPeopleDraftTwoRow,
} from "~/lib/sweden-people-draft-two.server";

function observation({
  id,
  source,
  year,
  payload,
}: {
  id: string;
  source: "bolagsverket" | "esef" | "wikidata";
  year: number | null;
  payload: Record<string, unknown>;
}): SwedenPeopleDraftSourceObservation {
  return {
    observation_id: id,
    company_id: "5565200028",
    name: "David Mindus",
    source,
    role_original:
      source === "wikidata" ? "chief executive officer" : "VD",
    fiscal_year: year,
    description:
      source === "wikidata" ? "Swedish business executive" : null,
    source_entity_id: `${source}-entity-${id}`,
    source_record_uid: `${source}-record-${id}`,
    source_profile_hash: `${source}-profile-${id}`,
    source_role_hash: `${source}-role-${id}`,
    source_payload_json: JSON.stringify(payload),
    source_observed_at: `202${year ? year % 10 : 4}-08-20T12:00:00Z`,
  };
}

function candidate(): SwedenPeopleDraftTwoRow {
  return {
    draft_2_id: "draft-two-person",
    company_id: "5565200028",
    name: "David Mindus",
    position: "chief_executive_officer",
    start_year: 2022,
    end_year: null,
    source_count: 3,
    observation_count: 5,
    bolagsverket_source_ids: ["bolags-2022", "bolags-2023"],
    bolagsverket_descriptions: [],
    esef_source_ids: ["esef-2023", "esef-2024"],
    esef_descriptions: [],
    wikidata_source_ids: ["wikidata-current"],
    wikidata_descriptions: ["Swedish business executive"],
    source_observations: [
      observation({
        id: "bolags-2022",
        source: "bolagsverket",
        year: 2022,
        payload: {
          first_name: "David",
          last_name: "Mindus",
          signatory_kind: "officer",
          role_kind: "chief_executive",
          statement_key: "2022-report",
        },
      }),
      observation({
        id: "bolags-2023",
        source: "bolagsverket",
        year: 2023,
        payload: {
          first_name: "David",
          last_name: "Mindus",
          signatory_kind: "officer",
          role_kind: "chief_executive",
          statement_key: "2023-report",
        },
      }),
      observation({
        id: "esef-2023",
        source: "esef",
        year: 2023,
        payload: {
          organization: "AB Sagax",
          role_category: "chief_executive",
          status: "current",
          effective_from: "2017-01-01",
          effective_to: null,
          confidence: 0.91,
          source_document_id: "esef-2023-document",
        },
      }),
      observation({
        id: "esef-2024",
        source: "esef",
        year: 2024,
        payload: {
          organization: "AB Sagax",
          role_category: "chief_executive",
          status: "current",
          effective_from: "2017-01-01",
          effective_to: null,
          confidence: 0.97,
          source_document_id: "esef-2024-document",
        },
      }),
      observation({
        id: "wikidata-current",
        source: "wikidata",
        year: null,
        payload: {
          company_wikidata_id: "Q123",
          person_wikidata_id: "Q456",
          role_property: "P169",
          role_label: "chief executive officer",
          start_date: "2017-01-01",
          end_date: null,
          is_current: 1,
          name: "David Mindus",
          description: "Swedish business executive",
          birth_year: 1972,
          image_url: "https://example.test/david.jpg",
          wikidata_url: "https://www.wikidata.org/wiki/Q456",
        },
      }),
    ],
  };
}

describe("person profile LLM input", () => {
  it("parses each source and compacts identical yearly observations", () => {
    const input = buildPersonProfileLlmInput(candidate());

    expect(input.personContext).toEqual({
      countryCode: "SE",
      companyId: "5565200028",
      currentName: "David Mindus",
      role: {
        code: "chief_executive_officer",
        startYear: 2022,
        endYear: null,
      },
    });
    expect(input.sourceRecords).toHaveLength(3);
    expect(input.sourceRecords).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          evidenceId: "bolagsverket:1",
          source: "bolagsverket",
          observedFiscalYears: [2022, 2023],
          firstFiscalYear: 2022,
          lastFiscalYear: 2023,
          facts: {
            firstName: "David",
            lastName: "Mindus",
            signatoryKind: "officer",
          },
        }),
        expect.objectContaining({
          evidenceId: "esef:1",
          source: "esef",
          observedFiscalYears: [2023, 2024],
          facts: {
            organization: "AB Sagax",
            status: "current",
            effectiveFrom: "2017-01-01",
            effectiveTo: null,
          },
        }),
        expect.objectContaining({
          evidenceId: "wikidata:1",
          source: "wikidata",
          facts: expect.objectContaining({
            birthYear: 1972,
            imageUrl: "https://example.test/david.jpg",
            wikidataUrl: "https://www.wikidata.org/wiki/Q456",
          }),
        }),
      ]),
    );
    expect(input).not.toHaveProperty("draft_candidate");
    for (const record of input.sourceRecords) {
      expect(record).not.toHaveProperty("sourceEntityIds");
      expect(record).not.toHaveProperty("sourceRecordUids");
      expect(record).not.toHaveProperty("sourceObservationIds");
      expect(record).not.toHaveProperty("latestSourceObservedAt");
    }
    expect(buildPersonProfileEvidenceMap(candidate())).toEqual({
      "bolagsverket:1": ["bolags-2022", "bolags-2023"],
      "esef:1": ["esef-2023", "esef-2024"],
      "wikidata:1": ["wikidata-current"],
    });
  });

  it("keeps only citations that belong to the supplied observations", () => {
    const value = parsePersonProfileSuggestion(
      JSON.stringify({
        displayName: "David Mindus",
        alternativeNames: [],
        description: "Swedish business executive",
        birthDate: null,
        birthYear: 1972,
        deathYear: null,
        nationalities: ["Swedish"],
        occupations: ["Business executive"],
        imageUrl: "https://example.test/david.jpg",
        referenceUrls: ["https://www.wikidata.org/wiki/Q456", "javascript:x"],
        companyRoles: [
          {
            companyId: "5565200028",
            role: "chief_executive_officer",
            roleLabel: "VD",
            startYear: 2017,
            endYear: null,
            isCurrent: true,
            evidenceIds: ["wikidata:1", "invented-id"],
          },
        ],
        additionalFacts: [],
        evidenceSummary: "Sources agree on the current role.",
        fieldEvidence: [
          {
            field: "birthYear",
            evidenceIds: ["wikidata:1", "invented-id"],
          },
        ],
        evidenceIds: ["wikidata:1", "invented-id"],
      }),
      candidate(),
    );

    expect(value.birthYear).toBe(1972);
    expect(value.referenceUrls).toEqual([
      "https://www.wikidata.org/wiki/Q456",
    ]);
    expect(value.companyRoles[0].evidenceIds).toEqual(["wikidata:1"]);
    expect(value.fieldEvidence[0].evidenceIds).toEqual(["wikidata:1"]);
    expect(value.evidenceIds).toEqual(["wikidata:1"]);
  });
});
