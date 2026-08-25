/**
 * The client-safe contract of the geocode analysis agent: the row shapes the
 * UI renders, the JSON schema the agent must answer with, and the validator
 * that stands between the agent's answer and any database write.
 *
 * No `.server` import lives here on purpose (CLAUDE.md: a route component may
 * not pull a server module into the client bundle). The route component, the
 * store, and the agent loop all read their types from this one file, so a
 * field can never mean two different things on the two sides of the wire.
 */

/* -------------------------------------------------------------------- */
/* Stored rows (as the loader serialises them: ISO strings, never Dates)  */
/* -------------------------------------------------------------------- */

export const GEOCODE_AGENT_RUN_STATUSES = [
  "queued",
  "running",
  "done",
  "failed",
] as const;
export type GeocodeAgentRunStatus = (typeof GEOCODE_AGENT_RUN_STATUSES)[number];

export const GEOCODE_AGENT_SUGGESTION_STATUSES = [
  "new",
  "accepted",
  "implemented",
  "rejected",
] as const;
export type GeocodeAgentSuggestionStatus =
  (typeof GEOCODE_AGENT_SUGGESTION_STATUSES)[number];

/** A run is live while it is queued or running: the UI polls exactly then. */
export function isActiveRunStatus(status: GeocodeAgentRunStatus): boolean {
  return status === "queued" || status === "running";
}

export interface GeocodeAgentRun {
  id: string;
  countryCode: string;
  params: GeocodeAgentRunParams;
  status: GeocodeAgentRunStatus;
  model: string;
  threadId: string;
  iterations: number;
  inputTokens: number;
  outputTokens: number;
  converged: boolean;
  reportMd: string;
  errorMessage: string;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface GeocodeAgentRunParams {
  /** Free-text directive from the trigger form ("go deeper on box addresses"). */
  focus: string;
  maxIterations: number;
  maxRowsPerQuery: number;
}

export interface GeocodeAgentSuggestionExample {
  address: string;
  geocodeStatus: string;
  count: number;
  note: string;
}

export interface GeocodeAgentSuggestion {
  id: string;
  runId: string;
  countryCode: string;
  pattern: string;
  description: string;
  expectedYield: number;
  yieldBasis: string;
  confidence: "" | "low" | "medium" | "high";
  examples: GeocodeAgentSuggestionExample[];
  status: GeocodeAgentSuggestionStatus;
  policyVersion: string;
  decidedBy: string;
  decidedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface GeocodeAgentMemoryEntry {
  countryCode: string;
  key: string;
  content: string;
  runId: string | null;
  updatedAt: string;
}

/**
 * Everything the Geocoding tab renders about the agent, in one payload. The
 * page loader and the poll endpoint both answer with this shape so the polled
 * update can replace the loaded one field for field.
 *
 * `available: false` is a normal state, not an error: a machine without
 * `BACKOFFICE_POSTGRES_URL` still renders the tab's list, with the agent panel
 * explaining what is missing.
 */
export interface GeocodeAgentPanel {
  countryCode: string;
  available: boolean;
  unavailableReason: string;
  runs: GeocodeAgentRun[];
  suggestions: GeocodeAgentSuggestion[];
}

/** The newest run, which is the one the UI polls and reports on. */
export function latestRun(panel: GeocodeAgentPanel): GeocodeAgentRun | null {
  return panel.runs[0] ?? null;
}

/* -------------------------------------------------------------------- */
/* The agent's structured output                                         */
/* -------------------------------------------------------------------- */

/** How many queries the agent may ask for in one turn. */
export const MAX_QUERIES_PER_TURN = 4;
/** Caps on a final answer, so one confused run cannot flood the tables. */
export const MAX_SUGGESTIONS_PER_RUN = 25;
export const MAX_MEMORY_ENTRIES_PER_RUN = 20;
export const MAX_MEMORY_KEY_LENGTH = 120;
export const MAX_MEMORY_CONTENT_LENGTH = 8_000;
export const MAX_REPORT_LENGTH = 200_000;
export const MAX_EXAMPLES_PER_SUGGESTION = 20;

export interface AgentQueryRequest {
  purpose: string;
  sql: string;
}

export interface AgentSuggestionDraft {
  pattern: string;
  description: string;
  expectedYield: number;
  yieldBasis: string;
  confidence: "" | "low" | "medium" | "high";
  examples: GeocodeAgentSuggestionExample[];
}

export interface AgentMemoryDraft {
  key: string;
  content: string;
}

export type AgentTurnOutput =
  | { action: "query"; rationale: string; queries: AgentQueryRequest[] }
  | {
      action: "final";
      rationale: string;
      reportMd: string;
      converged: boolean;
      suggestions: AgentSuggestionDraft[];
      memory: AgentMemoryDraft[];
    };

/**
 * The JSON schema handed to the Codex SDK as `outputSchema`.
 *
 * One flat object with every field required rather than a `oneOf` of two
 * shapes: OpenAI-style strict structured outputs want `additionalProperties:
 * false` and a complete `required` list, and a discriminated union at the root
 * is the one shape that is not reliably supported. `action` is the
 * discriminator; the fields the chosen action does not use are sent empty.
 */
export const AGENT_TURN_OUTPUT_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: [
    "action",
    "rationale",
    "queries",
    "report_md",
    "converged",
    "suggestions",
    "memory",
  ],
  properties: {
    action: {
      type: "string",
      enum: ["query", "final"],
      description:
        "'query' to run more read-only ClickHouse SQL, 'final' to deliver the report.",
    },
    rationale: {
      type: "string",
      description: "One or two sentences: what this turn is testing and why.",
    },
    queries: {
      type: "array",
      description: `Read-only SELECT/WITH statements to run (max ${MAX_QUERIES_PER_TURN}). Empty when action is 'final'.`,
      items: {
        type: "object",
        additionalProperties: false,
        required: ["purpose", "sql"],
        properties: {
          purpose: { type: "string" },
          sql: { type: "string" },
        },
      },
    },
    report_md: {
      type: "string",
      description:
        "Markdown report for the reviewer. Required when action is 'final', empty otherwise.",
    },
    converged: {
      type: "boolean",
      description:
        "True only when the remaining unmatched pool holds no further actionable class.",
    },
    suggestions: {
      type: "array",
      description:
        "Concrete Dagster augmentation rules, each with evidence and an expected yield. Empty unless action is 'final'.",
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "pattern",
          "description",
          "expected_yield",
          "yield_basis",
          "confidence",
          "examples",
        ],
        properties: {
          pattern: { type: "string" },
          description: { type: "string" },
          expected_yield: { type: "integer" },
          yield_basis: { type: "string" },
          confidence: { type: "string", enum: ["low", "medium", "high"] },
          examples: {
            type: "array",
            items: {
              type: "object",
              additionalProperties: false,
              required: ["address", "geocode_status", "count", "note"],
              properties: {
                address: { type: "string" },
                geocode_status: { type: "string" },
                count: { type: "integer" },
                note: { type: "string" },
              },
            },
          },
        },
      },
    },
    memory: {
      type: "array",
      description:
        "Durable notes for the next run: hypotheses already tested, classes already converged, register quirks.",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["key", "content"],
        properties: {
          key: { type: "string" },
          content: { type: "string" },
        },
      },
    },
  },
} as const;

export class AgentOutputError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentOutputError";
  }
}

function asString(value: unknown, field: string): string {
  if (typeof value !== "string") {
    throw new AgentOutputError(`"${field}" must be a string.`);
  }
  return value.trim();
}

function asArray(value: unknown, field: string): unknown[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new AgentOutputError(`"${field}" must be an array.`);
  }
  return value;
}

function asCount(value: unknown, field: string): number {
  const numeric = typeof value === "string" ? Number(value) : value;
  if (typeof numeric !== "number" || !Number.isFinite(numeric) || numeric < 0) {
    throw new AgentOutputError(`"${field}" must be a non-negative number.`);
  }
  return Math.round(numeric);
}

function asRecord(value: unknown, field: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AgentOutputError(`"${field}" must be an object.`);
  }
  return value as Record<string, unknown>;
}

/**
 * Parses one agent turn. Throws `AgentOutputError` with a message the loop
 * feeds straight back to the agent, so a malformed answer costs one turn
 * rather than the whole run.
 *
 * The Codex SDK returns `finalResponse` as text even under `outputSchema`;
 * some models still wrap it in a ```json fence, which is tolerated here
 * because the alternative is discarding an otherwise valid answer.
 */
export function parseAgentTurnOutput(rawText: string): AgentTurnOutput {
  const text = rawText.trim().replace(/^```(?:json)?\s*/i, "").replace(/```$/, "").trim();
  if (text === "") {
    throw new AgentOutputError("The answer was empty; reply with the JSON object.");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new AgentOutputError(
      `The answer was not valid JSON (${(error as Error).message}). Reply with the JSON object only.`,
    );
  }

  const root = asRecord(parsed, "output");
  const action = asString(root.action, "action");
  const rationale = asString(root.rationale ?? "", "rationale");

  if (action === "query") {
    const queries = asArray(root.queries, "queries").map((entry, index) => {
      const record = asRecord(entry, `queries[${index}]`);
      const sql = asString(record.sql, `queries[${index}].sql`);
      if (sql === "") {
        throw new AgentOutputError(`"queries[${index}].sql" must not be empty.`);
      }
      return {
        purpose: asString(record.purpose ?? "", `queries[${index}].purpose`),
        sql,
      };
    });
    if (queries.length === 0) {
      throw new AgentOutputError(
        'action "query" needs at least one entry in "queries"; use action "final" when the analysis is done.',
      );
    }
    if (queries.length > MAX_QUERIES_PER_TURN) {
      throw new AgentOutputError(
        `At most ${MAX_QUERIES_PER_TURN} queries per turn; you asked for ${queries.length}.`,
      );
    }
    return { action: "query", rationale, queries };
  }

  if (action !== "final") {
    throw new AgentOutputError(`"action" must be "query" or "final"; got "${action}".`);
  }

  const reportMd = asString(root.report_md ?? "", "report_md");
  if (reportMd === "") {
    throw new AgentOutputError('action "final" requires a non-empty "report_md".');
  }

  const suggestions = asArray(root.suggestions, "suggestions").map((entry, index) => {
    const record = asRecord(entry, `suggestions[${index}]`);
    const pattern = asString(record.pattern, `suggestions[${index}].pattern`);
    const description = asString(
      record.description,
      `suggestions[${index}].description`,
    );
    if (pattern === "" || description === "") {
      throw new AgentOutputError(
        `"suggestions[${index}]" needs a non-empty pattern and description.`,
      );
    }
    const confidenceRaw = asString(
      record.confidence ?? "",
      `suggestions[${index}].confidence`,
    ).toLowerCase();
    const confidence: AgentSuggestionDraft["confidence"] =
      confidenceRaw === "low" || confidenceRaw === "medium" || confidenceRaw === "high"
        ? confidenceRaw
        : "";
    const examples = asArray(record.examples, `suggestions[${index}].examples`)
      .slice(0, MAX_EXAMPLES_PER_SUGGESTION)
      .map((example, exampleIndex) => {
        const exampleRecord = asRecord(
          example,
          `suggestions[${index}].examples[${exampleIndex}]`,
        );
        return {
          address: asString(exampleRecord.address ?? "", "address"),
          geocodeStatus: asString(exampleRecord.geocode_status ?? "", "geocode_status"),
          count: asCount(exampleRecord.count ?? 0, "count"),
          note: asString(exampleRecord.note ?? "", "note"),
        };
      });
    return {
      pattern,
      description,
      expectedYield: asCount(
        record.expected_yield ?? 0,
        `suggestions[${index}].expected_yield`,
      ),
      yieldBasis: asString(record.yield_basis ?? "", `suggestions[${index}].yield_basis`),
      confidence,
      examples,
    };
  });
  if (suggestions.length > MAX_SUGGESTIONS_PER_RUN) {
    throw new AgentOutputError(
      `At most ${MAX_SUGGESTIONS_PER_RUN} suggestions per run; you sent ${suggestions.length}.`,
    );
  }

  const memory = asArray(root.memory, "memory").map((entry, index) => {
    const record = asRecord(entry, `memory[${index}]`);
    const key = asString(record.key, `memory[${index}].key`);
    if (key === "") {
      throw new AgentOutputError(`"memory[${index}].key" must not be empty.`);
    }
    return {
      key: key.slice(0, MAX_MEMORY_KEY_LENGTH),
      content: asString(record.content ?? "", `memory[${index}].content`).slice(
        0,
        MAX_MEMORY_CONTENT_LENGTH,
      ),
    };
  });
  if (memory.length > MAX_MEMORY_ENTRIES_PER_RUN) {
    throw new AgentOutputError(
      `At most ${MAX_MEMORY_ENTRIES_PER_RUN} memory entries per run; you sent ${memory.length}.`,
    );
  }

  return {
    action: "final",
    rationale,
    reportMd: reportMd.slice(0, MAX_REPORT_LENGTH),
    converged: root.converged === true,
    suggestions,
    memory,
  };
}
