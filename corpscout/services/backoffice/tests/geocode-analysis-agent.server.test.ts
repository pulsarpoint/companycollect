import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { describe, expect, it, vi } from "vitest";
import type {
  AgentMemoryDraft,
  AgentSuggestionDraft,
  GeocodeAgentMemoryEntry,
  GeocodeAgentRun,
  GeocodeAgentSuggestion,
} from "~/agents/geocode-analysis-contract";
import type {
  GeocodeAgentRunOutcome,
  GeocodeAgentStore,
} from "~/lib/geocode-agent-store.server";
import type { AgentQueryOutcome } from "~/agents/geocode-agent-clickhouse.server";
import {
  DEFAULT_MAX_ITERATIONS,
  executeGeocodeAnalysisRun,
  agentProcessEnv,
  geocodeAgentCredentialProblem,
  geocodeAgentCountry,
  readGeocodeAgentConfig,
  type AgentThreadFactory,
  type GeocodeAgentDeps,
} from "~/agents/geocode-analysis.server";
import { assertReadOnlyQuery } from "~/agents/read-only-sql";

/* -------------------------------------------------------------------- */
/* Fakes: no PostgreSQL, no Codex process, no ClickHouse                 */
/* -------------------------------------------------------------------- */

interface RecordedStore extends GeocodeAgentStore {
  calls: string[];
  running: { model: string; threadId: string } | null;
  finished: (GeocodeAgentRunOutcome & { reportMd: string; converged: boolean }) | null;
  failed: (GeocodeAgentRunOutcome & { errorMessage: string }) | null;
  suggestions: AgentSuggestionDraft[];
  memory: AgentMemoryDraft[];
}

function fakeStore(
  seed: {
    memory?: GeocodeAgentMemoryEntry[];
    suggestions?: GeocodeAgentSuggestion[];
  } = {},
): RecordedStore {
  const store: RecordedStore = {
    calls: [],
    running: null,
    finished: null,
    failed: null,
    suggestions: [],
    memory: [],
    async markRunning(_id, update) {
      store.calls.push("markRunning");
      store.running = update;
    },
    async finish(_id, outcome) {
      store.calls.push("finish");
      store.finished = outcome;
    },
    async fail(_id, outcome) {
      store.calls.push("fail");
      store.failed = outcome;
    },
    async saveSuggestions(_runId, _country, drafts) {
      store.calls.push("saveSuggestions");
      store.suggestions = drafts;
    },
    async saveMemory(_country, _runId, entries) {
      store.calls.push("saveMemory");
      store.memory = entries;
    },
    async readMemory() {
      store.calls.push("readMemory");
      return seed.memory ?? [];
    },
    async readSuggestions() {
      store.calls.push("readSuggestions");
      return seed.suggestions ?? [];
    },
  };
  return store;
}

/** A scripted model: each entry is one answer, and every prompt it was given
 * is recorded so the tests can assert what the agent actually saw. */
function fakeThreads(answers: string[]): AgentThreadFactory & {
  prompts: string[];
  signals: Array<AbortSignal | undefined>;
  closed: boolean;
} {
  const state = {
    prompts: [] as string[],
    signals: [] as Array<AbortSignal | undefined>,
    closed: false,
    model: "fake-model",
    async start() {
      let turn = 0;
      return {
        thread: {
          id: "thread-1",
          async run(input: string, options: { signal?: AbortSignal } = {}) {
            state.prompts.push(input);
            state.signals.push(options.signal);
            const text = answers[turn] ?? answers[answers.length - 1] ?? "";
            turn += 1;
            return { text, inputTokens: 10, outputTokens: 5 };
          },
        },
        close: async () => {
          state.closed = true;
        },
      };
    },
  };
  return state;
}

function queryAnswer(rows: unknown[]): AgentQueryOutcome {
  return {
    purpose: "",
    sql: "",
    rows,
    rowCount: rows.length,
    truncated: false,
    elapsedMs: 3,
  };
}

/** The production capability minus the network: the REAL guard decides, so a
 * write request fails here exactly as it would against ClickHouse. */
function guardedQueryRunner(rows: unknown[] = [{ n: 1 }]) {
  return async (request: { purpose: string; sql: string }) => {
    const sql = assertReadOnlyQuery(request.sql);
    return { ...queryAnswer(rows), purpose: request.purpose, sql };
  };
}

function run(overrides: Partial<GeocodeAgentRun> = {}): GeocodeAgentRun {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    countryCode: "SE",
    params: { focus: "", maxIterations: 4, maxRowsPerQuery: 200, maxRunMinutes: 30 },
    status: "queued",
    model: "",
    threadId: "",
    iterations: 0,
    inputTokens: 0,
    outputTokens: 0,
    converged: false,
    reportMd: "",
    errorMessage: "",
    createdAt: "2026-08-25T10:00:00.000Z",
    startedAt: null,
    finishedAt: null,
    ...overrides,
  };
}

const QUERY_TURN = JSON.stringify({
  action: "query",
  rationale: "count the unmatched pool",
  queries: [
    { purpose: "pool size", sql: "SELECT count() FROM corpscout.se_company_address" },
  ],
  report_md: "",
  converged: false,
  suggestions: [],
  memory: [],
});

const FINAL_TURN = JSON.stringify({
  action: "final",
  rationale: "evidence is in",
  queries: [],
  report_md: "# Geocoding analysis\n\nOne actionable class.",
  converged: true,
  suggestions: [
    {
      pattern: "glued street suffix",
      description: "Split 'Storgatan12'.",
      expected_yield: 4200,
      yield_basis: "countIf(match(street_address, ...))",
      confidence: "medium",
      examples: [
        { address: "Storgatan12", geocode_status: "unmatched", count: 12, note: "" },
      ],
    },
  ],
  memory: [{ key: "glued-suffix", content: "Tested and quantified in run 1." }],
});

function deps(
  store: GeocodeAgentStore,
  threads: AgentThreadFactory,
  runQuery = guardedQueryRunner(),
): GeocodeAgentDeps {
  return { store, threads, runQuery };
}

/* -------------------------------------------------------------------- */

describe("executeGeocodeAnalysisRun: the happy path", () => {
  it("runs the loop, then persists suggestions, memory and the report", async () => {
    const store = fakeStore({
      memory: [
        {
          countryCode: "SE",
          key: "box-addresses",
          content: "Box addresses converged in run 0.",
          runId: null,
          updatedAt: "2026-08-24T00:00:00.000Z",
        },
      ],
    });
    const threads = fakeThreads([QUERY_TURN, FINAL_TURN]);

    const summary = await executeGeocodeAnalysisRun(run(), deps(store, threads));

    expect(summary).toEqual({
      status: "done",
      iterations: 2,
      suggestions: 1,
      errorMessage: "",
    });
    // Context first, then the run is marked running, and the writes happen
    // only after the agent's final answer validated.
    expect(store.calls).toEqual([
      "readMemory",
      "readSuggestions",
      "markRunning",
      "saveSuggestions",
      "saveMemory",
      "finish",
    ]);
    expect(store.running).toEqual({ model: "fake-model", threadId: "thread-1" });
    expect(store.finished).toMatchObject({
      converged: true,
      iterations: 2,
      threadId: "thread-1",
      // Usage is summed across turns, not taken from the last one.
      inputTokens: 20,
      outputTokens: 10,
    });
    expect(store.finished?.reportMd).toContain("# Geocoding analysis");
    expect(store.suggestions[0]?.expectedYield).toBe(4200);
    expect(store.memory[0]?.key).toBe("glued-suffix");
    expect(threads.closed).toBe(true);
  });

  it("injects memory, prior suggestions and the focus directive into the opening prompt", async () => {
    const store = fakeStore({
      memory: [
        {
          countryCode: "SE",
          key: "box-addresses",
          content: "Box addresses converged in run 0.",
          runId: null,
          updatedAt: "2026-08-24T00:00:00.000Z",
        },
      ],
      suggestions: [
        {
          id: "s1",
          runId: "r0",
          countryCode: "SE",
          pattern: "c/o prefix",
          description: "Strip the c/o line before matching.",
          expectedYield: 900,
          yieldBasis: "",
          confidence: "high",
          examples: [],
          status: "accepted",
          policyVersion: "",
          decidedBy: "",
          decidedAt: null,
          createdAt: "2026-08-24T00:00:00.000Z",
          updatedAt: "2026-08-24T00:00:00.000Z",
        },
      ],
    });
    const threads = fakeThreads([FINAL_TURN]);

    await executeGeocodeAnalysisRun(
      run({
        params: {
          focus: "ignore box addresses",
          maxIterations: 4,
          maxRowsPerQuery: 200,
          maxRunMinutes: 30,
        },
      }),
      deps(store, threads),
    );

    const opening = threads.prompts[0] ?? "";
    expect(opening).toContain("Box addresses converged in run 0.");
    expect(opening).toContain("[accepted] c/o prefix");
    expect(opening).toContain("ignore box addresses");
    // The country profile, not a hard-coded Sweden string in the loop.
    expect(opening).toContain("corpscout.se_address_geocodes_current");
  });

  it("feeds query results back to the agent", async () => {
    const store = fakeStore();
    const threads = fakeThreads([QUERY_TURN, FINAL_TURN]);
    await executeGeocodeAnalysisRun(
      run(),
      deps(store, threads, guardedQueryRunner([{ status: "unmatched", c: 70896 }])),
    );
    const second = threads.prompts[1] ?? "";
    expect(second).toContain('"c": 70896');
    expect(second).toContain("Turns left after this one: 2");
  });
});

describe("executeGeocodeAnalysisRun: guardrails", () => {
  it("hands a refused write back to the agent instead of failing the run", async () => {
    const store = fakeStore();
    const threads = fakeThreads([
      JSON.stringify({
        action: "query",
        rationale: "clean up",
        queries: [{ purpose: "fix it", sql: "ALTER TABLE corpscout.se_company_address DELETE WHERE 1" }],
        report_md: "",
        converged: false,
        suggestions: [],
        memory: [],
      }),
      FINAL_TURN,
    ]);

    const summary = await executeGeocodeAnalysisRun(run(), deps(store, threads));

    expect(summary.status).toBe("done");
    const feedback = threads.prompts[1] ?? "";
    expect(feedback).toContain("ERROR:");
    expect(feedback).toMatch(/Query must start with one of SELECT/);
    // Nothing was executed, and the run still produced its report.
    expect(store.finished?.reportMd).toContain("# Geocoding analysis");
  });

  it("asks the agent to correct a malformed answer, then gives up after three", async () => {
    const store = fakeStore();
    const threads = fakeThreads(["not json"]);

    const summary = await executeGeocodeAnalysisRun(run(), deps(store, threads));

    expect(summary.status).toBe("failed");
    expect(store.failed?.errorMessage).toMatch(/3 unusable answers/);
    expect(threads.prompts[1]).toContain("Your last answer could not be used");
    expect(store.calls).not.toContain("finish");
  });

  it("recovers when the agent fixes its answer", async () => {
    const store = fakeStore();
    const threads = fakeThreads(["not json", FINAL_TURN]);
    const summary = await executeGeocodeAnalysisRun(run(), deps(store, threads));
    expect(summary.status).toBe("done");
    expect(summary.iterations).toBe(2);
  });

  it("fails the run when the turn budget runs out without a report", async () => {
    const store = fakeStore();
    const threads = fakeThreads([QUERY_TURN]);

    const summary = await executeGeocodeAnalysisRun(
      run({
        params: { focus: "", maxIterations: 2, maxRowsPerQuery: 200, maxRunMinutes: 30 },
      }),
      deps(store, threads),
    );

    expect(summary.status).toBe("failed");
    expect(summary.iterations).toBe(2);
    expect(store.failed?.errorMessage).toMatch(/used all 2 turns/);
    // The last turn is announced as the last one.
    expect(threads.prompts[1]).toContain("This is your LAST turn");
  });

  it("hands each turn the run's remaining wall-clock budget", async () => {
    const store = fakeStore();
    const threads = fakeThreads([FINAL_TURN]);
    await executeGeocodeAnalysisRun(run(), deps(store, threads));
    expect(threads.signals[0]).toBeInstanceOf(AbortSignal);
    expect(threads.signals[0]?.aborted).toBe(false);
  });

  it("fails the run when the wall-clock budget runs out between turns", async () => {
    // Only Date is faked: AbortSignal.timeout keeps its real timer, so the
    // turn itself is unaffected -- what is being tested is the loop noticing
    // that the budget is gone before it spends another one.
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      const store = fakeStore();
      let turns = 0;
      const threads: AgentThreadFactory = {
        model: "fake-model",
        async start() {
          return {
            thread: {
              id: "thread-slow",
              async run() {
                turns += 1;
                vi.setSystemTime(Date.now() + 2 * 60_000);
                return { text: QUERY_TURN, inputTokens: 1, outputTokens: 1 };
              },
            },
            close: async () => undefined,
          };
        },
      };

      const summary = await executeGeocodeAnalysisRun(
        run({
          params: { focus: "", maxIterations: 6, maxRowsPerQuery: 200, maxRunMinutes: 1 },
        }),
        deps(store, threads),
      );

      expect(turns).toBe(1);
      expect(summary.status).toBe("failed");
      expect(store.failed?.errorMessage).toMatch(/1-minute budget/);
    } finally {
      vi.useRealTimers();
    }
  });

  it("fails a country that has no profile without ever starting a thread", async () => {
    const store = fakeStore();
    const threads = fakeThreads([FINAL_TURN]);
    const summary = await executeGeocodeAnalysisRun(
      run({ countryCode: "ZZ" }),
      deps(store, threads),
    );
    expect(summary.status).toBe("failed");
    expect(store.failed?.errorMessage).toMatch(/No geocode analysis profile for country ZZ/);
    expect(threads.prompts).toHaveLength(0);
  });

  it("turns a thrown model error into a failed run, not an unhandled rejection", async () => {
    const store = fakeStore();
    const threads: AgentThreadFactory = {
      model: "fake-model",
      async start() {
        return {
          thread: {
            id: "thread-x",
            async run() {
              throw new Error("codex exited with status 1");
            },
          },
          close: async () => undefined,
        };
      },
    };
    const summary = await executeGeocodeAnalysisRun(run(), deps(store, threads));
    expect(summary.status).toBe("failed");
    expect(store.failed?.errorMessage).toBe("codex exited with status 1");
  });
});

describe("configuration", () => {
  it("falls back to the Codex-native key names and sane caps", () => {
    expect(readGeocodeAgentConfig({} as NodeJS.ProcessEnv)).toEqual({
      apiKey: "",
      codexHome: "",
      model: "",
      baseUrl: "",
      reasoningEffort: "",
      maxIterations: DEFAULT_MAX_ITERATIONS,
      maxRunMinutes: 30,
    });
    expect(
      readGeocodeAgentConfig({ CODEX_API_KEY: " k " } as NodeJS.ProcessEnv).apiKey,
    ).toBe("k");
    expect(
      readGeocodeAgentConfig({ OPENAI_API_KEY: "k2" } as NodeJS.ProcessEnv).apiKey,
    ).toBe("k2");
    expect(
      readGeocodeAgentConfig({
        GEOCODE_AGENT_API_KEY: "k1",
        OPENAI_API_KEY: "k2",
        GEOCODE_AGENT_MAX_ITERATIONS: "not a number",
      } as NodeJS.ProcessEnv),
    ).toMatchObject({ apiKey: "k1", maxIterations: DEFAULT_MAX_ITERATIONS });
  });

  it("reports a missing credential as a message, never as an exception", () => {
    const emptyCodexHome = mkdtempSync(`${tmpdir()}/codex-home-`);
    const env = { CODEX_HOME: emptyCodexHome } as NodeJS.ProcessEnv;
    const problem = geocodeAgentCredentialProblem(readGeocodeAgentConfig(env));
    expect(problem).toMatch(/GEOCODE_AGENT_API_KEY/);
    expect(
      geocodeAgentCredentialProblem(
        readGeocodeAgentConfig({
          ...env,
          GEOCODE_AGENT_API_KEY: "k",
        } as NodeJS.ProcessEnv),
      ),
    ).toBe("");
  });

  it("hands the Codex process an allowlisted environment, not the backoffice's", () => {
    const config = readGeocodeAgentConfig({
      GEOCODE_AGENT_CODEX_HOME: "/srv/agent-codex",
    } as NodeJS.ProcessEnv);
    const passed = agentProcessEnv(config, {
      PATH: "/usr/bin",
      HOME: "/home/app",
      HTTPS_PROXY: "http://proxy:3128",
      // None of these may reach a model process.
      CLICKHOUSE_PASSWORD: "secret",
      BACKOFFICE_POSTGRES_URL: "postgres://user:pw@host/db",
      OPENROUTER_API: "sk-secret",
      CORPSCOUT_S3_SECRET_KEY: "secret",
    } as NodeJS.ProcessEnv);

    expect(passed).toEqual({
      PATH: "/usr/bin",
      HOME: "/home/app",
      HTTPS_PROXY: "http://proxy:3128",
      CODEX_HOME: "/srv/agent-codex",
    });
  });

  it("wires Sweden and nothing else, by country code", () => {
    expect(geocodeAgentCountry("se")?.countryCode).toBe("SE");
    expect(geocodeAgentCountry("NO")).toBeNull();
  });
});
