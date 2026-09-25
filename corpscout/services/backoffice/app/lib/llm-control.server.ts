import "dotenv/config";
import { Pool, type PoolClient } from "pg";

let pool: Pool | undefined;
export function llmControl() {
  if (!pool) {
    const connectionString = process.env.LLM_CONTROL_PG_URL;
    if (!connectionString) throw new Error("Configure LLM_CONTROL_PG_URL before using saved models.");
    pool = new Pool({connectionString, max: 5, connectionTimeoutMillis: 5000, idleTimeoutMillis: 30000, statement_timeout: 10000});
    pool.on("error", () => console.error("LLM control database connection unavailable"));
  }
  return pool;
}

export async function llmTransaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await llmControl().connect();
  try {
    await client.query("BEGIN");
    const result = await work(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally { client.release(); }
}

/** Call with the profile locked, using the same lock order as run admission. */
export async function stopLlmRuns(client: PoolClient, profileId: string, reason: string, revision?: number) {
  return client.query(`UPDATE processing.run_requests r SET stop_requested_at=coalesce(stop_requested_at,now()),
      stop_reason=$2,updated_at=now() WHERE finished_at IS NULL AND EXISTS (
      SELECT 1 FROM processing.run_llm_dependencies d WHERE d.request_id=r.request_id
      AND d.profile_id=$1 AND ($3::integer IS NULL OR d.revision=$3))`, [profileId, reason, revision ?? null]);
}
