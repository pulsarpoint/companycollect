import { describe, expect, it } from "vitest";
import {
  AGENT_TURN_OUTPUT_SCHEMA,
  AgentOutputError,
  MAX_MEMORY_CONTENT_LENGTH,
  MAX_QUERIES_PER_TURN,
  MAX_SUGGESTION_DESCRIPTION_LENGTH,
  MAX_SUGGESTION_PATTERN_LENGTH,
  MAX_SUGGESTION_YIELD_BASIS_LENGTH,
  MAX_SUGGESTIONS_PER_RUN,
  parseAgentTurnOutput,
} from "~/agents/geocode-analysis-contract";

function finalPayload(overrides: Record<string, unknown> = {}) {
  return JSON.stringify({
    action: "final",
    rationale: "done",
    queries: [],
    report_md: "# Report\n\nOne class found.",
    converged: false,
    suggestions: [],
    memory: [],
    ...overrides,
  });
}

describe("AGENT_TURN_OUTPUT_SCHEMA", () => {
  it("is a strict object schema listing every property as required", () => {
    // OpenAI-style strict structured output: additionalProperties false and a
    // complete `required` list, or the model is free to invent fields.
    expect(AGENT_TURN_OUTPUT_SCHEMA.additionalProperties).toBe(false);
    expect([...AGENT_TURN_OUTPUT_SCHEMA.required].sort()).toEqual(
      Object.keys(AGENT_TURN_OUTPUT_SCHEMA.properties).sort(),
    );
  });
});

describe("parseAgentTurnOutput: query turns", () => {
  it("accepts a query turn and trims its parts", () => {
    const output = parseAgentTurnOutput(
      JSON.stringify({
        action: "query",
        rationale: " cluster the pool ",
        queries: [{ purpose: " counts ", sql: " SELECT 1 " }],
        report_md: "",
        converged: false,
        suggestions: [],
        memory: [],
      }),
    );
    expect(output).toEqual({
      action: "query",
      rationale: "cluster the pool",
      queries: [{ purpose: "counts", sql: "SELECT 1" }],
    });
  });

  it("tolerates a markdown fence around the JSON", () => {
    const output = parseAgentTurnOutput("```json\n" + finalPayload() + "\n```");
    expect(output.action).toBe("final");
  });

  it("refuses a query turn with no queries, an empty statement, or too many", () => {
    const base = {
      action: "query",
      rationale: "",
      report_md: "",
      converged: false,
      suggestions: [],
      memory: [],
    };
    expect(() =>
      parseAgentTurnOutput(JSON.stringify({ ...base, queries: [] })),
    ).toThrow(AgentOutputError);
    expect(() =>
      parseAgentTurnOutput(
        JSON.stringify({ ...base, queries: [{ purpose: "x", sql: "  " }] }),
      ),
    ).toThrow(/must not be empty/);
    expect(() =>
      parseAgentTurnOutput(
        JSON.stringify({
          ...base,
          queries: Array.from({ length: MAX_QUERIES_PER_TURN + 1 }, () => ({
            purpose: "x",
            sql: "SELECT 1",
          })),
        }),
      ),
    ).toThrow(new RegExp(`At most ${MAX_QUERIES_PER_TURN} queries`));
  });
});

describe("parseAgentTurnOutput: final turns", () => {
  it("maps suggestions and memory into the stored shape", () => {
    const output = parseAgentTurnOutput(
      finalPayload({
        converged: true,
        suggestions: [
          {
            pattern: "glued street suffix",
            description: "Split 'Storgatan12' into street and number.",
            expected_yield: "1234",
            yield_basis: "count of unmatched rows matching the regex",
            confidence: "HIGH",
            examples: [
              {
                address: "Storgatan12, 111 22 Stockholm",
                geocode_status: "unmatched",
                count: 7,
                note: "matched exemplar exists as 'Storgatan 12'",
              },
            ],
          },
        ],
        memory: [{ key: "tested", content: "x".repeat(MAX_MEMORY_CONTENT_LENGTH + 50) }],
      }),
    );
    if (output.action !== "final") throw new Error("expected a final turn");
    expect(output.converged).toBe(true);
    expect(output.suggestions[0]).toMatchObject({
      pattern: "glued street suffix",
      expectedYield: 1234,
      confidence: "high",
    });
    expect(output.suggestions[0]?.examples[0]).toEqual({
      address: "Storgatan12, 111 22 Stockholm",
      geocodeStatus: "unmatched",
      count: 7,
      note: "matched exemplar exists as 'Storgatan 12'",
    });
    // Memory content is clipped rather than rejected: the note is still useful.
    expect(output.memory[0]?.content).toHaveLength(MAX_MEMORY_CONTENT_LENGTH);
  });

  it("drops an unrecognised confidence instead of writing it", () => {
    const output = parseAgentTurnOutput(
      finalPayload({
        suggestions: [
          {
            pattern: "p",
            description: "d",
            expected_yield: 1,
            yield_basis: "",
            confidence: "certain",
            examples: [],
          },
        ],
      }),
    );
    if (output.action !== "final") throw new Error("expected a final turn");
    expect(output.suggestions[0]?.confidence).toBe("");
  });

  it("refuses a final turn with no report", () => {
    expect(() => parseAgentTurnOutput(finalPayload({ report_md: "  " }))).toThrow(
      /non-empty "report_md"/,
    );
  });

  it("refuses a suggestion flood", () => {
    expect(() =>
      parseAgentTurnOutput(
        finalPayload({
          suggestions: Array.from({ length: MAX_SUGGESTIONS_PER_RUN + 1 }, () => ({
            pattern: "p",
            description: "d",
            expected_yield: 0,
            yield_basis: "",
            confidence: "low",
            examples: [],
          })),
        }),
      ),
    ).toThrow(new RegExp(`At most ${MAX_SUGGESTIONS_PER_RUN} suggestions`));
  });

  it("clips suggestion free text, which the next run's prompt has to carry", () => {
    const output = parseAgentTurnOutput(
      finalPayload({
        suggestions: [
          {
            pattern: "p".repeat(MAX_SUGGESTION_PATTERN_LENGTH + 50),
            description: "d".repeat(MAX_SUGGESTION_DESCRIPTION_LENGTH + 50),
            expected_yield: 1,
            yield_basis: "y".repeat(MAX_SUGGESTION_YIELD_BASIS_LENGTH + 50),
            confidence: "low",
            examples: [],
          },
        ],
      }),
    );
    if (output.action !== "final") throw new Error("expected a final turn");
    expect(output.suggestions[0]?.pattern).toHaveLength(MAX_SUGGESTION_PATTERN_LENGTH);
    expect(output.suggestions[0]?.description).toHaveLength(
      MAX_SUGGESTION_DESCRIPTION_LENGTH,
    );
    expect(output.suggestions[0]?.yieldBasis).toHaveLength(
      MAX_SUGGESTION_YIELD_BASIS_LENGTH,
    );
  });

  it("clamps an implausible yield instead of failing the run at the INSERT", () => {
    // expected_yield is a bigint column at the end of a minutes-long run; a
    // hallucinated 1e30 must not be what discards the report.
    const output = parseAgentTurnOutput(
      finalPayload({
        suggestions: [
          {
            pattern: "p",
            description: "d",
            expected_yield: 1e30,
            yield_basis: "",
            confidence: "low",
            examples: [{ address: "a", geocode_status: "", count: 1e30, note: "" }],
          },
        ],
      }),
    );
    if (output.action !== "final") throw new Error("expected a final turn");
    expect(output.suggestions[0]?.expectedYield).toBe(Number.MAX_SAFE_INTEGER);
    expect(output.suggestions[0]?.examples[0]?.count).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("refuses a suggestion with a negative yield or a missing pattern", () => {
    expect(() =>
      parseAgentTurnOutput(
        finalPayload({
          suggestions: [
            {
              pattern: "p",
              description: "d",
              expected_yield: -5,
              yield_basis: "",
              confidence: "low",
              examples: [],
            },
          ],
        }),
      ),
    ).toThrow(/non-negative/);
    expect(() =>
      parseAgentTurnOutput(
        finalPayload({
          suggestions: [
            {
              pattern: "",
              description: "d",
              expected_yield: 1,
              yield_basis: "",
              confidence: "low",
              examples: [],
            },
          ],
        }),
      ),
    ).toThrow(/non-empty pattern/);
  });
});

describe("parseAgentTurnOutput: unusable answers", () => {
  it("names the problem so the loop can hand it back to the agent", () => {
    expect(() => parseAgentTurnOutput("not json at all")).toThrow(/not valid JSON/);
    expect(() => parseAgentTurnOutput("   ")).toThrow(/empty/);
    expect(() =>
      parseAgentTurnOutput(JSON.stringify({ action: "delete_everything" })),
    ).toThrow(/"action" must be "query" or "final"/);
    expect(() => parseAgentTurnOutput(JSON.stringify([1, 2, 3]))).toThrow(
      /must be an object/,
    );
  });
});
