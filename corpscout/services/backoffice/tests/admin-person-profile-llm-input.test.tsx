import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { PersonProfileLlmInputWorkspace } from "~/routes/admin-se-people-llm-input";

const person = {
  draftTwoId: "draft-two-person",
  companyId: "5565200028",
  name: "David Mindus",
  position: "chief_executive_officer",
  rawObservationCount: 6,
  compactedObservationCount: 3,
};

const input = {
  task: "Create one comprehensive person profile.",
  personContext: {
    countryCode: "SE",
    companyId: "5565200028",
    currentName: "David Mindus",
  },
  sourceRecords: [{ source: "wikidata", facts: { birthYear: 1972 } }],
};

function renderWorkspace(
  llmAvailability: Parameters<
    typeof PersonProfileLlmInputWorkspace
  >[0]["llmAvailability"],
  storedResponse: Parameters<
    typeof PersonProfileLlmInputWorkspace
  >[0]["storedResponse"] = null,
  returnTo = "/admin/se/people?step=draft-2",
) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <PersonProfileLlmInputWorkspace
            person={person}
            input={input}
            llmAvailability={llmAvailability}
            storedResponse={storedResponse}
            storedResponseIsCurrent={storedResponse !== null}
            returnTo={returnTo}
            result={null}
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: ["/admin/se/people/llm-input/draft-two-person"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("person profile LLM input page", () => {
  it("shows the prepared request but disables sending without an active LLM", () => {
    const html = renderWorkspace({
      ready: false,
      warning:
        "No active LLM is configured. Configure and activate an LLM before sending this request.",
      profile: null,
    });

    expect(html).toContain("Exact JSON sent to the LLM");
    expect(html).toContain("Create one comprehensive person profile.");
    expect(html).toContain("LLM request is disabled");
    expect(html).toContain("No active LLM is configured");
    expect(html).toContain('href="/admin/settings/llms"');
    expect(html).toMatch(
      /<button[^>]*\sdisabled=""[^>]*>[\s\S]*?Send LLM request<\/button>/,
    );
  });

  it("enables sending when the active LLM and its API key are available", () => {
    const html = renderWorkspace({
      ready: true,
      warning: null,
      profile: {
        name: "People normalization",
        provider: "DeepSeek",
        model: "DeepSeek Flash V4",
        apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
      },
    });

    expect(html).toContain("Ready to send");
    expect(html).toContain("People normalization");
    expect(html).toContain("DeepSeek Flash V4");
    expect(html).toContain("Send LLM request");
    expect(html).not.toContain("LLM request is disabled");
    expect(html).not.toMatch(
      /<button[^>]*\sdisabled=""[^>]*>[\s\S]*?Send LLM request<\/button>/,
    );
  });

  it("returns to the Draft 2 filters that opened the preview", () => {
    const html = renderWorkspace(
      {
        ready: false,
        warning: "No active LLM is configured.",
        profile: null,
      },
      null,
      "/admin/se/people?step=draft-2&draft_2=multiple-sources&draft_2_page=3",
    );

    expect(html).toContain(
      'href="/admin/se/people?step=draft-2&amp;draft_2=multiple-sources&amp;draft_2_page=3"',
    );
  });

  it("loads a saved response and offers a retry", () => {
    const html = renderWorkspace(
      {
        ready: true,
        warning: null,
        profile: {
          name: "People normalization",
          provider: "DeepSeek",
          model: "DeepSeek Flash V4",
          apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
        },
      },
      {
        attemptId: "attempt-2",
        draftTwoId: "draft-two-person",
        inputHash: "a".repeat(64),
        suggestion: {
          displayName: "David Mindus",
          alternativeNames: [],
          description: "Swedish business executive.",
          birthDate: null,
          birthYear: null,
          deathYear: null,
          nationalities: ["Swedish"],
          occupations: ["Business executive"],
          imageUrl: null,
          referenceUrls: [],
          companyRoles: [],
          additionalFacts: [],
          evidenceSummary: "The supplied sources agree.",
          fieldEvidence: [],
          evidenceIds: ["bolagsverket:1", "esef:1"],
        },
        generation: {
          provider: "DeepSeek",
          model: "DeepSeek Flash V4",
          promptTokens: 120,
          completionTokens: 80,
          totalTokens: 200,
        },
        reviewStatus: "pending",
        attemptCount: 2,
        createdAt: "2026-08-21T12:00:00.000Z",
      },
    );

    expect(html).toContain("Saved LLM response");
    expect(html).toContain("local people-curation SQLite database");
    expect(html).toContain("attempt 2");
    expect(html).toContain("Swedish business executive.");
    expect(html).toContain("Retry LLM request");
    expect(html).toContain("Ready to retry");
  });
});
