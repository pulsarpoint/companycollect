/**
 * Every read and write of the geocode analysis agent's three PostgreSQL
 * tables (`geocode_agent_runs`, `geocode_agent_suggestions`,
 * `geocode_agent_memory`; migration database/migrations/000001_geocode_agent).
 *
 * The agent NEVER reaches PostgreSQL. It returns structured output, the loop
 * in app/agents/geocode-analysis.server.ts validates it against
 * geocode-analysis-contract.ts, and only then does this module write -- so a
 * hallucinated column or an oversized report cannot become a row.
 *
 * `GeocodeAgentStore` is the narrow interface the loop depends on, which is
 * what lets the orchestration be unit-tested against a fake without a
 * database.
 */
import { randomUUID } from "node:crypto";
import type { QueryResultRow } from "pg";
import { pgQuery, pgQueryOne } from "~/lib/postgres.server";
import type {
  AgentMemoryDraft,
  AgentSuggestionDraft,
  GeocodeAgentMemoryEntry,
  GeocodeAgentRun,
  GeocodeAgentRunParams,
  GeocodeAgentRunStatus,
  GeocodeAgentSuggestion,
  GeocodeAgentSuggestionExample,
  GeocodeAgentSuggestionStatus,
} from "~/agents/geocode-analysis-contract";

/** Raised when a country already has a queued or running analysis: the
 * partial unique index `geocode_agent_runs_one_active_per_country` is the
 * authority, not a read-then-write check in application code. */
export class GeocodeAgentRunActiveError extends Error {
  constructor(countryCode: string) {
    super(`A geocode analysis run is already active for ${countryCode}.`);
    this.name = "GeocodeAgentRunActiveError";
  }
}

function isUniqueViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: string }).code === "23505"
  );
}

function iso(value: Date | string | null | undefined): string | null {
  if (!value) return null;
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

/** `bigint`/`numeric` come back from `pg` as strings; every count this module
 * exposes is a JS number the UI can format. */
function count(value: unknown): number {
  const numeric = Number(value ?? 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

/* -------------------------------------------------------------------- */
/* Runs                                                                  */
/* -------------------------------------------------------------------- */

interface RunRow extends QueryResultRow {
  id: string;
  country_code: string;
  params: unknown;
  status: string;
  model: string;
  thread_id: string;
  iterations: number | string;
  input_tokens: number | string;
  output_tokens: number | string;
  converged: boolean;
  report_md: string;
  error_message: string;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
}

const RUN_COLUMNS = `id, country_code, params, status, model, thread_id, iterations,
    input_tokens, output_tokens, converged, report_md, error_message,
    created_at, started_at, finished_at`;

function runParams(value: unknown): GeocodeAgentRunParams {
  const record = (typeof value === "object" && value !== null ? value : {}) as Record<
    string,
    unknown
  >;
  return {
    focus: typeof record.focus === "string" ? record.focus : "",
    maxIterations: count(record.maxIterations),
    maxRowsPerQuery: count(record.maxRowsPerQuery),
  };
}

function toRun(row: RunRow): GeocodeAgentRun {
  return {
    id: row.id,
    countryCode: row.country_code,
    params: runParams(row.params),
    status: row.status as GeocodeAgentRunStatus,
    model: row.model,
    threadId: row.thread_id,
    iterations: count(row.iterations),
    inputTokens: count(row.input_tokens),
    outputTokens: count(row.output_tokens),
    converged: row.converged,
    reportMd: row.report_md,
    errorMessage: row.error_message,
    createdAt: iso(row.created_at) ?? new Date(0).toISOString(),
    startedAt: iso(row.started_at),
    finishedAt: iso(row.finished_at),
  };
}

export async function createGeocodeAgentRun(input: {
  countryCode: string;
  params: GeocodeAgentRunParams;
}): Promise<GeocodeAgentRun> {
  try {
    const row = await pgQueryOne<RunRow>(
      `INSERT INTO geocode_agent_runs (id, country_code, params, status)
       VALUES ($1, $2, $3::jsonb, 'queued')
       RETURNING ${RUN_COLUMNS}`,
      [randomUUID(), input.countryCode, JSON.stringify(input.params)],
    );
    if (!row) throw new Error("INSERT ... RETURNING produced no row.");
    return toRun(row);
  } catch (error) {
    if (isUniqueViolation(error)) throw new GeocodeAgentRunActiveError(input.countryCode);
    throw error;
  }
}

export async function markGeocodeAgentRunRunning(
  id: string,
  update: { model: string; threadId: string },
): Promise<void> {
  await pgQuery(
    `UPDATE geocode_agent_runs
     SET status = 'running', model = $2, thread_id = $3, started_at = now()
     WHERE id = $1`,
    [id, update.model, update.threadId],
  );
}

export interface GeocodeAgentRunOutcome {
  threadId: string;
  iterations: number;
  inputTokens: number;
  outputTokens: number;
}

export async function finishGeocodeAgentRun(
  id: string,
  outcome: GeocodeAgentRunOutcome & { reportMd: string; converged: boolean },
): Promise<void> {
  await pgQuery(
    `UPDATE geocode_agent_runs
     SET status = 'done',
         report_md = $2,
         converged = $3,
         thread_id = $4,
         iterations = $5,
         input_tokens = $6,
         output_tokens = $7,
         error_message = '',
         finished_at = now()
     WHERE id = $1`,
    [
      id,
      outcome.reportMd,
      outcome.converged,
      outcome.threadId,
      outcome.iterations,
      outcome.inputTokens,
      outcome.outputTokens,
    ],
  );
}

export async function failGeocodeAgentRun(
  id: string,
  outcome: GeocodeAgentRunOutcome & { errorMessage: string },
): Promise<void> {
  await pgQuery(
    `UPDATE geocode_agent_runs
     SET status = 'failed',
         error_message = $2,
         thread_id = $3,
         iterations = $4,
         input_tokens = $5,
         output_tokens = $6,
         finished_at = now()
     WHERE id = $1`,
    [
      id,
      outcome.errorMessage,
      outcome.threadId,
      outcome.iterations,
      outcome.inputTokens,
      outcome.outputTokens,
    ],
  );
}

export async function listGeocodeAgentRuns(
  countryCode: string,
  limit = 10,
): Promise<GeocodeAgentRun[]> {
  const rows = await pgQuery<RunRow>(
    `SELECT ${RUN_COLUMNS}
     FROM geocode_agent_runs
     WHERE country_code = $1
     ORDER BY created_at DESC
     LIMIT $2`,
    [countryCode, limit],
  );
  return rows.map(toRun);
}

export async function getGeocodeAgentRun(id: string): Promise<GeocodeAgentRun | null> {
  const row = await pgQueryOne<RunRow>(
    `SELECT ${RUN_COLUMNS} FROM geocode_agent_runs WHERE id = $1`,
    [id],
  );
  return row ? toRun(row) : null;
}

/**
 * Marks runs abandoned by a process that died mid-run. Called when the tab
 * loads: without it a crashed run would hold the one-active-run index forever
 * and the trigger would stay disabled.
 */
export async function expireStaleGeocodeAgentRuns(
  countryCode: string,
  maxAgeMinutes: number,
): Promise<number> {
  const rows = await pgQuery<{ id: string }>(
    `UPDATE geocode_agent_runs
     SET status = 'failed',
         error_message = 'The backoffice process ended before this run finished.',
         finished_at = now()
     WHERE country_code = $1
       AND status IN ('queued', 'running')
       AND created_at < now() - make_interval(mins => $2::int)
     RETURNING id`,
    [countryCode, maxAgeMinutes],
  );
  return rows.length;
}

/* -------------------------------------------------------------------- */
/* Suggestions                                                           */
/* -------------------------------------------------------------------- */

interface SuggestionRow extends QueryResultRow {
  id: string;
  run_id: string;
  country_code: string;
  pattern: string;
  description: string;
  expected_yield: number | string;
  yield_basis: string;
  confidence: string;
  examples: unknown;
  status: string;
  policy_version: string;
  decided_by: string;
  decided_at: Date | null;
  created_at: Date;
  updated_at: Date;
}

const SUGGESTION_COLUMNS = `id, run_id, country_code, pattern, description, expected_yield,
    yield_basis, confidence, examples, status, policy_version, decided_by,
    decided_at, created_at, updated_at`;

function toExamples(value: unknown): GeocodeAgentSuggestionExample[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => {
    const record = (typeof entry === "object" && entry !== null ? entry : {}) as Record<
      string,
      unknown
    >;
    return {
      address: String(record.address ?? ""),
      geocodeStatus: String(record.geocodeStatus ?? record.geocode_status ?? ""),
      count: count(record.count),
      note: String(record.note ?? ""),
    };
  });
}

function toSuggestion(row: SuggestionRow): GeocodeAgentSuggestion {
  return {
    id: row.id,
    runId: row.run_id,
    countryCode: row.country_code,
    pattern: row.pattern,
    description: row.description,
    expectedYield: count(row.expected_yield),
    yieldBasis: row.yield_basis,
    confidence: row.confidence as GeocodeAgentSuggestion["confidence"],
    examples: toExamples(row.examples),
    status: row.status as GeocodeAgentSuggestionStatus,
    policyVersion: row.policy_version,
    decidedBy: row.decided_by,
    decidedAt: iso(row.decided_at),
    createdAt: iso(row.created_at) ?? new Date(0).toISOString(),
    updatedAt: iso(row.updated_at) ?? new Date(0).toISOString(),
  };
}

export async function insertGeocodeAgentSuggestions(
  runId: string,
  countryCode: string,
  drafts: AgentSuggestionDraft[],
): Promise<GeocodeAgentSuggestion[]> {
  if (drafts.length === 0) return [];
  const columns = 9;
  const values: unknown[] = [];
  const tuples = drafts.map((draft, index) => {
    values.push(
      randomUUID(),
      runId,
      countryCode,
      draft.pattern,
      draft.description,
      draft.expectedYield,
      draft.yieldBasis,
      draft.confidence,
      JSON.stringify(draft.examples),
    );
    const base = index * columns;
    const placeholders = Array.from({ length: columns }, (_, offset) =>
      offset === columns - 1 ? `$${base + offset + 1}::jsonb` : `$${base + offset + 1}`,
    );
    return `(${placeholders.join(", ")})`;
  });
  const rows = await pgQuery<SuggestionRow>(
    `INSERT INTO geocode_agent_suggestions
       (id, run_id, country_code, pattern, description, expected_yield, yield_basis, confidence, examples)
     VALUES ${tuples.join(", ")}
     RETURNING ${SUGGESTION_COLUMNS}`,
    values,
  );
  return rows.map(toSuggestion);
}

export async function listGeocodeAgentSuggestions(
  countryCode: string,
  limit = 100,
): Promise<GeocodeAgentSuggestion[]> {
  const rows = await pgQuery<SuggestionRow>(
    `SELECT ${SUGGESTION_COLUMNS}
     FROM geocode_agent_suggestions
     WHERE country_code = $1
     ORDER BY created_at DESC, pattern ASC
     LIMIT $2`,
    [countryCode, limit],
  );
  return rows.map(toSuggestion);
}

export async function setGeocodeAgentSuggestionStatus(
  id: string,
  status: GeocodeAgentSuggestionStatus,
  decision: { decidedBy?: string; policyVersion?: string } = {},
): Promise<GeocodeAgentSuggestion | null> {
  const row = await pgQueryOne<SuggestionRow>(
    `UPDATE geocode_agent_suggestions
     SET status = $2,
         decided_by = $3,
         policy_version = CASE WHEN $4 = '' THEN policy_version ELSE $4 END,
         decided_at = now(),
         updated_at = now()
     WHERE id = $1
     RETURNING ${SUGGESTION_COLUMNS}`,
    [id, status, decision.decidedBy ?? "", decision.policyVersion ?? ""],
  );
  return row ? toSuggestion(row) : null;
}

/* -------------------------------------------------------------------- */
/* Memory                                                                */
/* -------------------------------------------------------------------- */

interface MemoryRow extends QueryResultRow {
  country_code: string;
  key: string;
  content: string;
  run_id: string | null;
  updated_at: Date;
}

export async function listGeocodeAgentMemory(
  countryCode: string,
): Promise<GeocodeAgentMemoryEntry[]> {
  const rows = await pgQuery<MemoryRow>(
    `SELECT country_code, key, content, run_id, updated_at
     FROM geocode_agent_memory
     WHERE country_code = $1
     ORDER BY updated_at DESC`,
    [countryCode],
  );
  return rows.map((row) => ({
    countryCode: row.country_code,
    key: row.key,
    content: row.content,
    runId: row.run_id,
    updatedAt: iso(row.updated_at) ?? new Date(0).toISOString(),
  }));
}

/** Upsert on `(country_code, key)`: a later run refines a note it wrote
 * before rather than appending a near-duplicate the next prompt would carry
 * twice. */
export async function upsertGeocodeAgentMemory(
  countryCode: string,
  runId: string,
  entries: AgentMemoryDraft[],
): Promise<number> {
  if (entries.length === 0) return 0;
  for (const entry of entries) {
    await pgQuery(
      `INSERT INTO geocode_agent_memory (country_code, key, content, run_id)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (country_code, key) DO UPDATE
       SET content = EXCLUDED.content, run_id = EXCLUDED.run_id, updated_at = now()`,
      [countryCode, entry.key, entry.content, runId],
    );
  }
  return entries.length;
}

/* -------------------------------------------------------------------- */
/* The narrow interface the agent loop depends on                        */
/* -------------------------------------------------------------------- */

export interface GeocodeAgentStore {
  markRunning(id: string, update: { model: string; threadId: string }): Promise<void>;
  finish(
    id: string,
    outcome: GeocodeAgentRunOutcome & { reportMd: string; converged: boolean },
  ): Promise<void>;
  fail(id: string, outcome: GeocodeAgentRunOutcome & { errorMessage: string }): Promise<void>;
  saveSuggestions(
    runId: string,
    countryCode: string,
    drafts: AgentSuggestionDraft[],
  ): Promise<unknown>;
  saveMemory(
    countryCode: string,
    runId: string,
    entries: AgentMemoryDraft[],
  ): Promise<unknown>;
  readMemory(countryCode: string): Promise<GeocodeAgentMemoryEntry[]>;
  readSuggestions(countryCode: string, limit?: number): Promise<GeocodeAgentSuggestion[]>;
}

export const postgresGeocodeAgentStore: GeocodeAgentStore = {
  markRunning: markGeocodeAgentRunRunning,
  finish: finishGeocodeAgentRun,
  fail: failGeocodeAgentRun,
  saveSuggestions: insertGeocodeAgentSuggestions,
  saveMemory: upsertGeocodeAgentMemory,
  readMemory: listGeocodeAgentMemory,
  readSuggestions: listGeocodeAgentSuggestions,
};
