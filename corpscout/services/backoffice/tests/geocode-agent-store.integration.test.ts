/**
 * The claim guard on the terminal writes, exercised against a REAL PostgreSQL.
 *
 * The resurrection unit test runs through the fake store, so deleting the SQL
 * `WHERE status IN ('queued','running')` from completeGeocodeAgentRun /
 * failGeocodeAgentRun would not fail it. This test runs the actual statements:
 * it reaps a run (freeing the country's active slot) and then proves the
 * reaped run cannot write terminal state, suggestions or memory back in.
 *
 * Gated on GEOCODE_AGENT_PG_TEST_URL, the same shape the ClickHouse-hitting
 * tests use to stay out of a plain `vitest run`: point it at a throwaway
 * `postgres:17.10` and this suite runs; leave it unset and it skips cleanly.
 *
 *   docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 55433:5432 postgres:17.10-bookworm
 *   GEOCODE_AGENT_PG_TEST_URL=postgres://postgres:pw@127.0.0.1:55433/postgres?sslmode=disable \
 *     npx vitest run tests/geocode-agent-store.integration.test.ts
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const PG_URL = process.env.GEOCODE_AGENT_PG_TEST_URL;

// The store reads BACKOFFICE_POSTGRES_URL lazily (per call, not at import), so
// setting it here before the dynamic import is enough to point the pool at the
// throwaway instance.
if (PG_URL) process.env.BACKOFFICE_POSTGRES_URL = PG_URL;

const MIGRATION = fileURLToPath(
  new URL("../migrations/000001_geocode_agent.up.sql", import.meta.url),
);

const params = {
  focus: "",
  maxIterations: 12,
  maxRowsPerQuery: 200,
  maxRunMinutes: 30,
};

const draft = (pattern: string) => ({
  pattern,
  description: "d",
  expectedYield: 10,
  yieldBasis: "",
  confidence: "low" as const,
  examples: [],
});

describe.skipIf(!PG_URL)("completeGeocodeAgentRun / failGeocodeAgentRun claim guard", () => {
  let store: typeof import("~/lib/geocode-agent-store.server");
  let pg: typeof import("~/lib/postgres.server");

  beforeAll(async () => {
    store = await import("~/lib/geocode-agent-store.server");
    pg = await import("~/lib/postgres.server");
    // Apply the real migration (idempotent: every statement is IF NOT EXISTS).
    await pg.pgQuery(readFileSync(MIGRATION, "utf8"));
  });

  afterAll(async () => {
    await pg.closeBackofficePostgresPool();
  });

  beforeEach(async () => {
    await pg.pgQuery(
      "TRUNCATE geocode_agent_suggestions, geocode_agent_memory, geocode_agent_runs RESTART IDENTITY CASCADE",
    );
  });

  it("a reaped run cannot resurrect itself or write its output", async () => {
    const run = await store.createGeocodeAgentRun({ countryCode: "SE", params });

    // Make it look like a process that started and then died.
    await pg.pgQuery(
      "UPDATE geocode_agent_runs SET status = 'running', created_at = now() - interval '2 hours' WHERE id = $1",
      [run.id],
    );
    const reaped = await store.expireStaleGeocodeAgentRuns("SE", {
      fallbackMinutes: 30,
      slackMinutes: 10,
    });
    expect(reaped).toBe(1);

    // A fresh run now legitimately owns the country's single active slot.
    const fresh = await store.createGeocodeAgentRun({ countryCode: "SE", params });
    expect(fresh.id).not.toBe(run.id);

    // The reaped executor finishes late and tries to commit. The SQL guard
    // must refuse the claim.
    const committed = await store.completeGeocodeAgentRun(run.id, "SE", {
      outcome: { threadId: "zombie", iterations: 9, inputTokens: 1, outputTokens: 1 },
      reportMd: "# ZOMBIE REPORT",
      converged: true,
      suggestions: [draft("zombie-rule")],
      memory: [{ key: "zombie", content: "should not persist" }],
    });

    expect(committed).toBe(false);

    // Nothing the zombie carried reached the database.
    const suggestions = await store.listGeocodeAgentSuggestions("SE");
    expect(suggestions).toEqual([]);
    const memory = await store.listGeocodeAgentMemory("SE");
    expect(memory).toEqual([]);

    // The reaped run stays failed; the fresh run stays live.
    const runs = await store.listGeocodeAgentRuns("SE", 5);
    const byId = new Map(runs.map((r) => [r.id, r]));
    expect(byId.get(run.id)?.status).toBe("failed");
    expect(byId.get(run.id)?.reportMd).toBe("");
    expect(byId.get(fresh.id)?.status).toBe("queued");
  });

  it("failGeocodeAgentRun will not overwrite a run that already finished", async () => {
    const run = await store.createGeocodeAgentRun({ countryCode: "SE", params });
    const first = await store.completeGeocodeAgentRun(run.id, "SE", {
      outcome: { threadId: "t", iterations: 1, inputTokens: 0, outputTokens: 0 },
      reportMd: "# DONE",
      converged: false,
      suggestions: [],
      memory: [],
    });
    expect(first).toBe(true);

    const late = await store.failGeocodeAgentRun(run.id, {
      errorMessage: "late failure",
      threadId: "",
      iterations: 0,
      inputTokens: 0,
      outputTokens: 0,
    });
    expect(late).toBe(false);

    const [reloaded] = await store.listGeocodeAgentRuns("SE", 1);
    expect(reloaded?.status).toBe("done");
    expect(reloaded?.reportMd).toBe("# DONE");
    expect(reloaded?.errorMessage).toBe("");
  });

  it("a healthy run committing normally still succeeds", async () => {
    const run = await store.createGeocodeAgentRun({ countryCode: "SE", params });
    await store.markGeocodeAgentRunRunning(run.id, { model: "m", threadId: "t" });
    const committed = await store.completeGeocodeAgentRun(run.id, "SE", {
      outcome: { threadId: "t", iterations: 3, inputTokens: 5, outputTokens: 2 },
      reportMd: "# OK",
      converged: true,
      suggestions: [draft("real-rule")],
      memory: [{ key: "note", content: "kept" }],
    });
    expect(committed).toBe(true);
    expect((await store.listGeocodeAgentSuggestions("SE")).map((s) => s.pattern)).toEqual([
      "real-rule",
    ]);
    expect((await store.listGeocodeAgentMemory("SE")).map((m) => m.key)).toEqual(["note"]);
  });
});
