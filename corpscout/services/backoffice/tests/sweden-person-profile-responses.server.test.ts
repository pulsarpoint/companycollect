import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import type { PersonProfileSuggestion } from "~/lib/person-profile-llm.server";
import {
  findPersonProfileResponseDraftTwoIds,
  getLatestPersonProfileResponse,
  listPersonProfileResponseDraftTwoIds,
  personProfileInputHash,
  savePersonProfileResponse,
} from "~/lib/sweden-person-profile-responses.server";

const temporaryDirectories: string[] = [];

function temporaryDatabasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "people-curation-"));
  temporaryDirectories.push(directory);
  return join(directory, "sweden", "people-curation.sqlite");
}

function suggestion(description: string): PersonProfileSuggestion {
  return {
    displayName: "David Mindus",
    alternativeNames: ["David Gustaf Mindus"],
    description,
    birthDate: null,
    birthYear: null,
    deathYear: null,
    nationalities: [],
    occupations: ["Business executive"],
    imageUrl: null,
    referenceUrls: [],
    companyRoles: [],
    additionalFacts: [],
    evidenceSummary: "Two sources identify the same executive.",
    fieldEvidence: [],
    evidenceIds: ["bolagsverket:1", "esef:1"],
  };
}

const generation = {
  provider: "DeepSeek",
  model: "deepseek-v4-flash",
  promptTokens: 120,
  completionTokens: 80,
  totalTokens: 200,
};

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("Sweden person-profile response SQLite store", () => {
  it("appends retries and returns the latest response for the exact input", () => {
    const databasePath = temporaryDatabasePath();
    const input = {
      task: "Create a profile",
      sourceRecords: [{ evidenceId: "bolagsverket:1" }],
    };

    savePersonProfileResponse(
      {
        draftTwoId: "draft-two-person",
        input,
        rawResponse: '{"description":"First response"}',
        suggestion: suggestion("First response"),
        generation,
      },
      databasePath,
    );
    const latest = savePersonProfileResponse(
      {
        draftTwoId: "draft-two-person",
        input,
        rawResponse: "```json\n{\"description\":\"Retried response\"}\n```",
        suggestion: suggestion("Retried response"),
        generation: { ...generation, totalTokens: 220 },
      },
      databasePath,
    );

    expect(latest.attemptCount).toBe(2);
    expect(latest.suggestion.description).toBe("Retried response");
    expect(latest.reviewStatus).toBe("pending");
    expect(latest.inputHash).toBe(personProfileInputHash(input));
    expect(listPersonProfileResponseDraftTwoIds(databasePath)).toEqual([
      "draft-two-person",
    ]);
    expect(
      findPersonProfileResponseDraftTwoIds(
        ["missing", "draft-two-person"],
        databasePath,
      ),
    ).toEqual(new Set(["draft-two-person"]));
    expect(
      getLatestPersonProfileResponse(
        { draftTwoId: "draft-two-person", input },
        databasePath,
      ),
    ).toMatchObject({
      attemptId: latest.attemptId,
      attemptCount: 2,
      suggestion: { description: "Retried response" },
      generation: { totalTokens: 220 },
    });

    const database = new DatabaseSync(databasePath, { readOnly: true });
    try {
      const rows = database
        .prepare(
          `SELECT raw_response_json
           FROM person_profile_llm_response
           ORDER BY rowid`,
        )
        .all() as unknown as Array<{ raw_response_json: string }>;
      expect(rows).toHaveLength(2);
      expect(rows[1].raw_response_json).toContain("```json");
    } finally {
      database.close();
    }
  });

  it("does not reuse a response when the prepared LLM input changes", () => {
    const databasePath = temporaryDatabasePath();
    const originalInput = { sourceRecords: [{ evidenceId: "esef:1" }] };
    savePersonProfileResponse(
      {
        draftTwoId: "draft-two-person",
        input: originalInput,
        rawResponse: '{"displayName":"David Mindus"}',
        suggestion: suggestion("Stored response"),
        generation,
      },
      databasePath,
    );

    expect(
      getLatestPersonProfileResponse(
        {
          draftTwoId: "draft-two-person",
          input: { sourceRecords: [{ evidenceId: "wikidata:1" }] },
        },
        databasePath,
      ),
    ).toBeNull();
    expect(
      getLatestPersonProfileResponse(
        { draftTwoId: "draft-two-person" },
        databasePath,
      )?.suggestion.description,
    ).toBe("Stored response");
  });
});
