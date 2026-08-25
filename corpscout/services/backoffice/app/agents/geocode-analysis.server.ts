/**
 * The geocode analysis agent: one UI-triggered run that clusters a country's
 * unmatched addresses, tests each hypothesis against matched exemplars, and
 * emits concrete Dagster augmentation suggestions with examples and counts.
 *
 * Shape of the integration (design spec
 * dagster_v3/docs/superpowers/specs/2026-08-25-se-geocode-lab-design.md §2):
 *
 * - The agent is an OpenAI **Codex** thread (@openai/codex-sdk). Owner
 *   decision: Codex for provider flexibility, not an Anthropic SDK.
 * - Its ONLY capability is read-only ClickHouse SQL, and it does not hold that
 *   capability either: it *asks* for statements in its structured output and
 *   this loop decides whether to run them (read-only-sql.ts) before sending
 *   them on a `readonly=1` connection (geocode-agent-clickhouse.server.ts).
 * - It has NO PostgreSQL access. Suggestions, memory and the report come back
 *   as structured output, are validated by geocode-analysis-contract.ts, and
 *   are written here through the store. A hallucinated table name cannot
 *   become a row, and the agent cannot rewrite its own history.
 * - It never writes the geocode store, never deploys, never triggers Dagster.
 *
 * Runs take minutes, so `startGeocodeAnalysisRun` inserts a queued row and
 * returns; the loop continues in the background and the UI polls the row.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { Codex, type ModelReasoningEffort } from "@openai/codex-sdk";
import {
  AGENT_TURN_OUTPUT_SCHEMA,
  AgentOutputError,
  MAX_QUERIES_PER_TURN,
  parseAgentTurnOutput,
  type AgentTurnOutput,
  type GeocodeAgentMemoryEntry,
  type GeocodeAgentPanel,
  type GeocodeAgentRun,
  type GeocodeAgentRunParams,
  type GeocodeAgentSuggestion,
} from "~/agents/geocode-analysis-contract";
import {
  AGENT_MAX_ROWS_PER_QUERY,
  formatAgentQueryOutcome,
  runAgentClickHouseQuery,
  type AgentQueryOutcome,
} from "~/agents/geocode-agent-clickhouse.server";
import {
  createGeocodeAgentRun,
  expireStaleGeocodeAgentRuns,
  listGeocodeAgentRuns,
  listGeocodeAgentSuggestions,
  postgresGeocodeAgentStore,
  type GeocodeAgentStore,
} from "~/lib/geocode-agent-store.server";
import { isBackofficePostgresConfigured } from "~/lib/postgres.server";

/* -------------------------------------------------------------------- */
/* Country profiles -- country is a parameter, SE is the first wired one  */
/* -------------------------------------------------------------------- */

export interface GeocodeAgentCountryProfile {
  countryCode: string;
  label: string;
  /** Pasted into the prompt: the tables worth starting from and what each is
   * for. Deliberately short -- the agent can DESCRIBE and SHOW TABLES for the
   * rest, which keeps this from rotting into a stale schema copy. */
  briefing: string;
}

export const GEOCODE_AGENT_COUNTRIES: Record<string, GeocodeAgentCountryProfile> = {
  SE: {
    countryCode: "SE",
    label: "Sweden",
    briefing: `Database: corpscout (ClickHouse). Start from these tables:

- corpscout.se_company_address — one published row per company+address_key.
  Read it as \`FROM corpscout.se_company_address FINAL WHERE is_current\`
  (ReplacingMergeTree on resolved_at; without FINAL a re-resolved address is
  read twice). Columns include company_id, address_key, address_type
  ('postal' | 'visiting_or_postal'), care_of, street_address,
  normalized_address, postal_code, city, country_code, address_id,
  latitude, longitude, geocode_status, sources.
- corpscout.se_addresses_current — the canonical address identity behind
  address_id: canonical_display_address, street_name, house_number, unit,
  postal_code, post_town, address_kind, normalized_street,
  normalized_postal_code, normalized_post_town, company_count.
- corpscout.se_address_geocodes_current — one row per address_id with the
  geocoder's own verdict: match_status, candidate_count, match_method,
  match_confidence, geocode_precision, latitude, longitude, matched_at.

Join key: se_company_address.address_id = se_addresses_current.address_id =
se_address_geocodes_current.address_id.

geocode_status / match_status values, and how the backoffice classes them:
- geocoded: matched_exact, matched_corrected, matched_site, matched_area,
  matched_street
- ambiguous: ambiguous (several OSM candidates, no coordinate chosen)
- unmatched (no usable coordinate): unmatched, invalid_address,
  foreign_address, postal_box, property_identifier
- no_outcome: '' (never reached the geocoder)

The pipeline you are advising is Dagster's Swedish address resolution
(dagster_v3 sweden_company address_resolution): a versioned, golden-corpus
gated policy of normalisation and augmentation rules — expanding glued street
suffixes, abbreviations, c/o handling, box addresses, unit stripping, postal
town fixes. Your suggestions become candidate rules for its next version.`,
  },
};

export function geocodeAgentCountry(
  countryCode: string,
): GeocodeAgentCountryProfile | null {
  return GEOCODE_AGENT_COUNTRIES[countryCode.toUpperCase()] ?? null;
}

/* -------------------------------------------------------------------- */
/* Configuration                                                         */
/* -------------------------------------------------------------------- */

export const DEFAULT_MAX_ITERATIONS = 12;
export const DEFAULT_MAX_RUN_MINUTES = 30;

export interface GeocodeAgentConfig {
  /** Empty when the host authenticates the Codex CLI itself (~/.codex/auth.json). */
  apiKey: string;
  /** CODEX_HOME for the agent's process. Point it at a directory of its own to
   * keep the host operator's ~/.codex/config.toml -- MCP servers, plugins,
   * skills -- out of the agent's session entirely. */
  codexHome: string;
  /** Empty means "whatever the Codex CLI defaults to". */
  model: string;
  baseUrl: string;
  reasoningEffort: string;
  maxIterations: number;
  maxRunMinutes: number;
}

function positiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function readGeocodeAgentConfig(
  env: NodeJS.ProcessEnv = process.env,
): GeocodeAgentConfig {
  return {
    apiKey:
      env.GEOCODE_AGENT_API_KEY?.trim() ||
      env.CODEX_API_KEY?.trim() ||
      env.OPENAI_API_KEY?.trim() ||
      "",
    codexHome: env.GEOCODE_AGENT_CODEX_HOME?.trim() || env.CODEX_HOME?.trim() || "",
    model: env.GEOCODE_AGENT_MODEL?.trim() ?? "",
    baseUrl: env.GEOCODE_AGENT_BASE_URL?.trim() ?? "",
    reasoningEffort: env.GEOCODE_AGENT_REASONING_EFFORT?.trim() ?? "",
    maxIterations: positiveInt(env.GEOCODE_AGENT_MAX_ITERATIONS, DEFAULT_MAX_ITERATIONS),
    maxRunMinutes: positiveInt(env.GEOCODE_AGENT_MAX_RUN_MINUTES, DEFAULT_MAX_RUN_MINUTES),
  };
}

/**
 * Whether this host can talk to the model at all: either an API key is
 * configured, or the Codex CLI is already signed in on this machine
 * (~/.codex/auth.json, or $CODEX_HOME/auth.json).
 *
 * A missing key is NOT an exception at trigger time: the run row is created
 * and immediately failed with this message, so the tab shows why.
 */
export function geocodeAgentCredentialProblem(config: GeocodeAgentConfig): string {
  if (config.apiKey !== "") return "";
  const codexHome = config.codexHome || join(homedir(), ".codex");
  if (existsSync(join(codexHome, "auth.json"))) return "";
  return `No Codex credentials. Set GEOCODE_AGENT_API_KEY in the backoffice environment (see .env.example), or sign the Codex CLI in on this host so ${codexHome}/auth.json exists.`;
}

/* -------------------------------------------------------------------- */
/* The model boundary (fake-able in tests)                               */
/* -------------------------------------------------------------------- */

export interface AgentTurnResult {
  text: string;
  inputTokens: number;
  outputTokens: number;
}

/** One conversation with the model. `run` is called repeatedly: the Codex
 * thread carries its own history, so each call sends only what is new.
 * `signal` is the run's remaining wall-clock budget; aborting it kills the
 * model process rather than leaving the turn to hang. */
export interface AgentThread {
  readonly id: string;
  run(input: string, options?: { signal?: AbortSignal }): Promise<AgentTurnResult>;
}

/** The seam the unit tests replace: no Codex process, no network. */
export interface AgentThreadFactory {
  readonly model: string;
  start(): Promise<{ thread: AgentThread; close: () => Promise<void> }>;
}

/** The Codex CLI's own reasoning-effort ladder. An unrecognised value in the
 * environment is ignored rather than passed through to the CLI, which would
 * fail the run on a typo. */
const MODEL_REASONING_EFFORTS: ModelReasoningEffort[] = [
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
  "ultra",
];

function modelReasoningEffort(value: string): ModelReasoningEffort | undefined {
  return MODEL_REASONING_EFFORTS.find((effort) => effort === value);
}

/**
 * The variables the Codex process is allowed to see. Everything else --
 * CLICKHOUSE_PASSWORD, the S3 keys, BACKOFFICE_POSTGRES_URL, every other API
 * key in the backoffice's environment -- is withheld: a model process that
 * cannot read a credential cannot leak one, whatever it is persuaded to do.
 *
 * The Codex CLI needs PATH and a home to resolve its own binary, config and
 * credentials, and proxy variables when the host reaches the API through one.
 */
const AGENT_ENV_ALLOWLIST = [
  "PATH",
  "HOME",
  "USER",
  "LOGNAME",
  "SHELL",
  "TMPDIR",
  "LANG",
  "LC_ALL",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "NO_PROXY",
  "http_proxy",
  "https_proxy",
  "no_proxy",
];

export function agentProcessEnv(
  config: GeocodeAgentConfig,
  env: NodeJS.ProcessEnv = process.env,
): Record<string, string> {
  const allowed: Record<string, string> = {};
  for (const key of AGENT_ENV_ALLOWLIST) {
    const value = env[key];
    if (typeof value === "string" && value !== "") allowed[key] = value;
  }
  if (config.codexHome !== "") allowed.CODEX_HOME = config.codexHome;
  return allowed;
}

/**
 * Config overrides applied on top of whatever `config.toml` the CODEX_HOME in
 * use happens to carry.
 *
 * `mcp_servers={}` is the important one: an operator's own ~/.codex/config.toml
 * may register MCP servers (browsers, REPLs, remote control), and those would
 * become tools of THIS thread too -- a capability this design does not grant.
 * The clean separation is a dedicated GEOCODE_AGENT_CODEX_HOME; this override
 * is the belt for the host that shares one.
 */
const AGENT_CONFIG_OVERRIDES = ["mcp_servers={}"];

/**
 * The real factory: a Codex thread locked down to nothing but thinking.
 *
 * - `sandboxMode: "read-only"` and a fresh empty working directory: the agent
 *   cannot see this repository, let alone edit it.
 * - `networkAccessEnabled: false`, `webSearchEnabled: false`: no egress.
 * - `approvalPolicy: "never"`: no interactive escape hatch on a server.
 *
 * Every fact it uses therefore arrives through this loop's query results.
 */
export function codexThreadFactory(config: GeocodeAgentConfig): AgentThreadFactory {
  return {
    model: config.model || "codex-default",
    async start() {
      const effort = modelReasoningEffort(config.reasoningEffort);
      const workingDirectory = await mkdtemp(join(tmpdir(), "geocode-agent-"));
      const codex = new Codex({
        ...(config.apiKey ? { apiKey: config.apiKey } : {}),
        ...(config.baseUrl ? { baseUrl: config.baseUrl } : {}),
        env: agentProcessEnv(config),
        configOverrides: AGENT_CONFIG_OVERRIDES,
      });
      const thread = codex.startThread({
        ...(config.model ? { model: config.model } : {}),
        ...(effort ? { modelReasoningEffort: effort } : {}),
        sandboxMode: "read-only",
        workingDirectory,
        skipGitRepoCheck: true,
        networkAccessEnabled: false,
        webSearchEnabled: false,
        approvalPolicy: "never",
      });
      return {
        thread: {
          get id() {
            return thread.id ?? "";
          },
          async run(
            input: string,
            runOptions: { signal?: AbortSignal } = {},
          ): Promise<AgentTurnResult> {
            const turn = await thread.run(input, {
              outputSchema: AGENT_TURN_OUTPUT_SCHEMA,
              ...(runOptions.signal ? { signal: runOptions.signal } : {}),
            });
            return {
              text: turn.finalResponse,
              inputTokens: turn.usage?.input_tokens ?? 0,
              outputTokens: turn.usage?.output_tokens ?? 0,
            };
          },
        },
        close: async () => {
          await rm(workingDirectory, { recursive: true, force: true });
        },
      };
    },
  };
}

/* -------------------------------------------------------------------- */
/* Prompts                                                               */
/* -------------------------------------------------------------------- */

function memoryBlock(memory: GeocodeAgentMemoryEntry[]): string {
  if (memory.length === 0) {
    return "(none — this is the first run for this country)";
  }
  return memory
    .map((entry) => `- [${entry.key}] (updated ${entry.updatedAt})\n  ${entry.content}`)
    .join("\n");
}

function suggestionsBlock(suggestions: GeocodeAgentSuggestion[]): string {
  if (suggestions.length === 0) return "(none yet)";
  return suggestions
    .map(
      (suggestion) =>
        `- [${suggestion.status}] ${suggestion.pattern} — expected yield ${suggestion.expectedYield}\n  ${suggestion.description}`,
    )
    .join("\n");
}

export function buildOpeningPrompt(input: {
  profile: GeocodeAgentCountryProfile;
  params: GeocodeAgentRunParams;
  memory: GeocodeAgentMemoryEntry[];
  suggestions: GeocodeAgentSuggestion[];
}): string {
  const { profile, params, memory, suggestions } = input;
  return `You are an address-data analyst for the ${profile.label} (${profile.countryCode}) geocoding pipeline.

YOUR OBJECTIVE, in order:
1. Cluster the unmatched/ambiguous address pool into concrete, countable patterns
   (shape of the street field, tokens, abbreviations, c/o and box forms, unit
   suffixes, postal-town mismatches, foreign rows, register-specific quirks).
   Every cluster must carry a real count from a query, never an estimate.
2. Form one hypothesis per cluster about WHY those addresses fail to match.
3. Test each hypothesis against MATCHED exemplars: if the rewrite you propose
   produces a string that matched addresses actually use, that is evidence; if
   no matched address has that shape, say so and drop the hypothesis.
4. Quantify the expected yield of each surviving rule: how many currently
   unmatched addresses it would newly match, counted by a query, and say in
   'yield_basis' exactly which query produced the number.
5. Emit each surviving rule as a suggestion with verbatim examples and counts.
6. Write memory notes for the next run: what you tested, what converged, what
   is left. Set converged=true only when the remaining pool holds no further
   actionable class.

HOW YOU WORK — read this carefully:
- You have NO tools, NO shell, NO network, NO filesystem. You cannot run code.
  The ONLY way you learn anything is by returning JSON with action "query";
  this program runs those statements for you and sends the rows back.
- Each answer MUST be a single JSON object matching the schema you were given.
  No prose outside it, no markdown fence.
- Queries are read-only: one statement each, starting with SELECT, WITH,
  DESCRIBE, SHOW or EXPLAIN. Writes, DDL, and the url()/file()/s3()/remote()
  table functions are refused. At most ${MAX_QUERIES_PER_TURN} queries per turn,
  at most ${AGENT_MAX_ROWS_PER_QUERY} rows returned per query — aggregate
  server-side (count(), groupArray, topK) instead of asking for raw dumps.
- You get at most ${params.maxIterations} turns in total. Spend them on
  counting and testing, not on browsing.

${profile.briefing}

WHAT PREVIOUS RUNS LEARNED (your memory — do not re-derive it, go past it):
${memoryBlock(memory)}

SUGGESTIONS ALREADY ON THE BOARD (do not repeat these; deepen or supersede them):
${suggestionsBlock(suggestions)}

${params.focus ? `FOCUS DIRECTIVE FOR THIS RUN (from the operator):\n${params.focus}\n` : ""}
Begin: answer with the JSON object for your first turn.`;
}

function turnFooter(turnsLeft: number): string {
  if (turnsLeft <= 0) {
    return `This is your LAST turn: answer with action "final", the report, your suggestions and your memory notes. A "query" answer now ends the run without a report.`;
  }
  return `Turns left after this one: ${turnsLeft}. Answer with action "query" to keep testing, or "final" when the evidence is in.`;
}

export function buildQueryResultsPrompt(
  results: Array<{ ok: true; outcome: AgentQueryOutcome } | { ok: false; purpose: string; sql: string; error: string }>,
  turnsLeft: number,
): string {
  const blocks = results.map((result, index) => {
    const header = `--- result ${index + 1} ---`;
    if (result.ok) return `${header}\n${formatAgentQueryOutcome(result.outcome)}`;
    return `${header}\npurpose: ${result.purpose || "(none given)"}\nsql: ${result.sql}\nERROR: ${result.error}`;
  });
  return `${blocks.join("\n\n")}\n\n${turnFooter(turnsLeft)}`;
}

/* -------------------------------------------------------------------- */
/* The loop                                                              */
/* -------------------------------------------------------------------- */

export interface GeocodeAgentDeps {
  store: GeocodeAgentStore;
  threads: AgentThreadFactory;
  runQuery(request: { purpose: string; sql: string }): Promise<AgentQueryOutcome>;
}

/** How many malformed answers in a row end the run. Two corrections are a
 * model having a bad moment; three is a model that cannot hold the contract. */
const MAX_INVALID_ANSWERS = 3;

export interface GeocodeAgentRunSummary {
  status: "done" | "failed";
  iterations: number;
  suggestions: number;
  errorMessage: string;
}

/**
 * Drives one run to completion and writes its outcome. Never throws: every
 * failure path ends as a `failed` run row carrying a message a reviewer can
 * read, because the caller is a detached background task with nobody to catch
 * it.
 */
export async function executeGeocodeAnalysisRun(
  run: GeocodeAgentRun,
  deps: GeocodeAgentDeps,
): Promise<GeocodeAgentRunSummary> {
  const profile = geocodeAgentCountry(run.countryCode);
  let iterations = 0;
  let inputTokens = 0;
  let outputTokens = 0;
  let threadId = "";
  let session: { thread: AgentThread; close: () => Promise<void> } | null = null;

  const fail = async (message: string): Promise<GeocodeAgentRunSummary> => {
    await deps.store.fail(run.id, {
      errorMessage: message,
      threadId,
      iterations,
      inputTokens,
      outputTokens,
    });
    return { status: "failed", iterations, suggestions: 0, errorMessage: message };
  };

  if (!profile) {
    return fail(
      `No geocode analysis profile for country ${run.countryCode}. Wire it into GEOCODE_AGENT_COUNTRIES first.`,
    );
  }

  try {
    const [memory, suggestions] = await Promise.all([
      deps.store.readMemory(run.countryCode),
      deps.store.readSuggestions(run.countryCode, 50),
    ]);

    session = await deps.threads.start();
    threadId = session.thread.id;
    await deps.store.markRunning(run.id, {
      model: deps.threads.model,
      threadId,
    });

    let input = buildOpeningPrompt({
      profile,
      params: run.params,
      memory,
      suggestions,
    });
    let invalidAnswers = 0;
    const maxIterations = run.params.maxIterations || DEFAULT_MAX_ITERATIONS;
    // One deadline for the whole run, spent turn by turn. `maxRunMinutes` is 0
    // only for a row written before this budget existed; those runs keep the
    // old behaviour of waiting on the model indefinitely.
    const deadline =
      run.params.maxRunMinutes > 0
        ? Date.now() + run.params.maxRunMinutes * 60_000
        : 0;

    while (iterations < maxIterations) {
      iterations += 1;
      const remainingMs = deadline === 0 ? 0 : deadline - Date.now();
      if (deadline !== 0 && remainingMs <= 0) {
        return fail(
          `The run passed its ${run.params.maxRunMinutes}-minute budget after ${iterations - 1} turns. Raise GEOCODE_AGENT_MAX_RUN_MINUTES or narrow the focus.`,
        );
      }
      const turn = await session.thread.run(
        input,
        deadline === 0 ? {} : { signal: AbortSignal.timeout(remainingMs) },
      );
      inputTokens += turn.inputTokens;
      outputTokens += turn.outputTokens;
      threadId = session.thread.id || threadId;

      let output: AgentTurnOutput;
      try {
        output = parseAgentTurnOutput(turn.text);
      } catch (error) {
        if (!(error instanceof AgentOutputError)) throw error;
        invalidAnswers += 1;
        if (invalidAnswers >= MAX_INVALID_ANSWERS) {
          return fail(
            `The agent produced ${invalidAnswers} unusable answers in a row. Last problem: ${error.message}`,
          );
        }
        input = `Your last answer could not be used: ${error.message}\nReply again with ONLY the JSON object required by the schema.`;
        continue;
      }
      invalidAnswers = 0;

      if (output.action === "final") {
        await deps.store.saveSuggestions(run.id, run.countryCode, output.suggestions);
        await deps.store.saveMemory(run.countryCode, run.id, output.memory);
        await deps.store.finish(run.id, {
          reportMd: output.reportMd,
          converged: output.converged,
          threadId,
          iterations,
          inputTokens,
          outputTokens,
        });
        return {
          status: "done",
          iterations,
          suggestions: output.suggestions.length,
          errorMessage: "",
        };
      }

      const results = [];
      for (const request of output.queries) {
        try {
          results.push({ ok: true as const, outcome: await deps.runQuery(request) });
        } catch (error) {
          results.push({
            ok: false as const,
            purpose: request.purpose,
            sql: request.sql,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      input = buildQueryResultsPrompt(results, maxIterations - iterations - 1);
    }

    return fail(
      `The agent used all ${maxIterations} turns without delivering a report. Re-run with a narrower focus, or raise GEOCODE_AGENT_MAX_ITERATIONS.`,
    );
  } catch (error) {
    return fail(error instanceof Error ? error.message : String(error));
  } finally {
    await session?.close().catch(() => undefined);
  }
}

/* -------------------------------------------------------------------- */
/* What the tab renders                                                  */
/* -------------------------------------------------------------------- */

/** How many runs the history shows, and how many suggestions the board holds. */
export const RUN_HISTORY_LIMIT = 10;
export const SUGGESTION_BOARD_LIMIT = 100;

/**
 * Reads the agent's state for one country, and never throws: an unconfigured
 * or unreachable review-queue database leaves the Geocoding tab's list intact
 * and turns the agent panel into an explanation.
 *
 * Reaps abandoned runs on the way past. A backoffice restart mid-run leaves a
 * 'running' row nobody will ever finish, and the partial unique index would
 * then block every new run for that country; anything older than the run
 * budget (plus slack) is failed here instead.
 */
export async function loadGeocodeAgentPanel(
  countryCode: string,
): Promise<GeocodeAgentPanel> {
  const country = countryCode.toUpperCase();
  const empty: GeocodeAgentPanel = {
    countryCode: country,
    available: false,
    unavailableReason: "",
    runs: [],
    suggestions: [],
  };

  if (!geocodeAgentCountry(country)) {
    return {
      ...empty,
      unavailableReason: `No geocode analysis profile is wired for ${country}.`,
    };
  }
  if (!isBackofficePostgresConfigured()) {
    return {
      ...empty,
      unavailableReason:
        "BACKOFFICE_POSTGRES_URL is not set, so agent runs cannot be recorded. See .env.example.",
    };
  }

  try {
    const config = readGeocodeAgentConfig();
    await expireStaleGeocodeAgentRuns(country, config.maxRunMinutes + 10);
    const [runs, suggestions] = await Promise.all([
      listGeocodeAgentRuns(country, RUN_HISTORY_LIMIT),
      listGeocodeAgentSuggestions(country, SUGGESTION_BOARD_LIMIT),
    ]);
    return { ...empty, available: true, runs, suggestions };
  } catch (error) {
    return {
      ...empty,
      unavailableReason: `The review-queue database did not answer: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

/* -------------------------------------------------------------------- */
/* Trigger                                                               */
/* -------------------------------------------------------------------- */

export interface StartGeocodeAnalysisInput {
  countryCode: string;
  focus?: string;
}

/**
 * Inserts the queued run row and starts the loop in the background.
 *
 * Returns as soon as the row exists — the HTTP action that called this must
 * not wait minutes for a model. The UI polls the row; a process that dies
 * mid-run leaves a stale 'running' row that `expireStaleGeocodeAgentRuns`
 * reaps on the next page load.
 */
export async function startGeocodeAnalysisRun(
  input: StartGeocodeAnalysisInput,
): Promise<GeocodeAgentRun> {
  const countryCode = input.countryCode.toUpperCase();
  const config = readGeocodeAgentConfig();
  const params: GeocodeAgentRunParams = {
    focus: (input.focus ?? "").trim().slice(0, 2_000),
    maxIterations: config.maxIterations,
    maxRowsPerQuery: AGENT_MAX_ROWS_PER_QUERY,
    maxRunMinutes: config.maxRunMinutes,
  };
  const run = await createGeocodeAgentRun({ countryCode, params });

  const credentialProblem = geocodeAgentCredentialProblem(config);
  if (credentialProblem !== "") {
    await postgresGeocodeAgentStore.fail(run.id, {
      errorMessage: credentialProblem,
      threadId: "",
      iterations: 0,
      inputTokens: 0,
      outputTokens: 0,
    });
    return { ...run, status: "failed", errorMessage: credentialProblem };
  }

  // Detached on purpose: this is the fire half of fire-and-poll.
  void executeGeocodeAnalysisRun(run, {
    store: postgresGeocodeAgentStore,
    threads: codexThreadFactory(config),
    runQuery: (request) => runAgentClickHouseQuery(request),
  }).catch((error: unknown) => {
    console.error("geocode analysis run crashed", run.id, error);
  });

  return run;
}
