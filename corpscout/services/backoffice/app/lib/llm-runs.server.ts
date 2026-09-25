import { randomUUID } from "node:crypto";
import { llmControl, llmTransaction } from "./llm-control.server";
import { LlmSettingsValidationError } from "./llm-settings.server";
import { verifySelectedLlm } from "./crawl-llm.server";

type Dependency = { profileId: string; revision: number; purpose: string; provider: string; model: string; baseUrl: string };
export function llmDependencies(config: unknown, path = 'config'): Dependency[] {
  if (!config || typeof config !== 'object') return [];
  const value = config as Record<string, unknown>;
  if (typeof value.api_key_encrypted === 'string') {
    if (typeof value.profile_id !== 'string' || !Number.isInteger(value.profile_revision))
      throw new LlmSettingsValidationError("This launch has no saved LLM revision. Select a model again before starting or resuming.");
    return [{profileId:value.profile_id,revision:Number(value.profile_revision),purpose:path,
      provider:String(value.provider),model:String(value.model),baseUrl:String(value.base_url)}];
  }
  return Object.entries(value).flatMap(([key,child]) => llmDependencies(child, `${path}.${key}`));
}

/** Persist before submission: a lost HTTP acknowledgement never loses cancellation ownership. */
export async function admitLlmRun(job: string, config: unknown, tags: Record<string,string> = {}) {
  const dependencies = llmDependencies(config);
  if (!dependencies.length) return null;
  // Every launch path gets a live preflight. Verify outside the transaction, then
  // fence the result against edits/removal under the same profile locks as disable.
  const checked = new Set<string>();
  for (const d of dependencies) {
    const identity = `${d.profileId}:${d.revision}`;
    if (checked.has(identity)) continue;
    const verified = await verifySelectedLlm(d.profileId, job.includes('brave') || job.includes('company_domains') ? 'brave' : 'crawler');
    if (verified.profile_revision !== d.revision) throw new LlmSettingsValidationError("The selected model changed. Reload before launching.");
    checked.add(identity);
  }
  const requestId = randomUUID();
  await llmTransaction(async client => {
    for (const id of [...new Set(dependencies.map(d => d.profileId))].sort()) {
      const {rows:[p]} = await client.query("SELECT * FROM processing.llm_profiles WHERE profile_id=$1 FOR UPDATE", [id]);
      if (p?.state !== 'enabled') throw new LlmSettingsValidationError("The selected model is disabled or removed.");
      for (const d of dependencies.filter(d => d.profileId === id)) {
        const {rows:[revision]} = await client.query(`SELECT * FROM processing.llm_profile_revisions WHERE profile_id=$1 AND revision=$2`, [id,d.revision]);
        if (!revision || revision.invalidated_at || p.current_revision !== d.revision || revision.provider !== d.provider
          || revision.base_url !== d.baseUrl || revision.model !== d.model) throw new LlmSettingsValidationError("The model configuration changed or failed validation. Start a new execution with a working model.");
      }
    }
    await client.query(`INSERT INTO processing.run_requests (request_id,task_id,job_name)
      VALUES ($1,$2,$3)`, [requestId,tags['processing/task_id'] ?? null,job]);
    for (const d of dependencies) await client.query(`INSERT INTO processing.run_llm_dependencies
      (request_id,profile_id,revision,purpose) VALUES ($1,$2,$3,$4)`, [requestId,d.profileId,d.revision,d.purpose]);
  });
  return {requestId};
}

export async function acknowledgeLlmRun(requestId: string, status: 'queued' | 'launch_failed', runId?: string) {
  await llmControl().query(`UPDATE processing.run_requests SET status=$2,dagster_run_id=coalesce($3,dagster_run_id),updated_at=now(),
    finished_at=CASE WHEN $2='launch_failed' THEN now() ELSE NULL END
    WHERE request_id=$1 AND status='launching'`, [requestId,status,runId ?? null]);
}

export async function recentLlmRuns() {
  const {rows} = await llmControl().query(`SELECT r.request_id AS "requestId",r.dagster_run_id AS "runId",
    r.job_name AS job,r.status,r.stop_reason AS "stopReason",r.stop_requested_at::text AS "stopRequestedAt",
    r.created_at::text AS "createdAt",r.last_error AS "lastError",
    (SELECT count(*)::integer FROM processing.llm_external_requests e WHERE e.request_id=r.request_id AND e.state='submitted') AS "pendingExternal",
    (SELECT string_agg(DISTINCT p.name || ' r' || d.revision, ', ') FROM processing.run_llm_dependencies d
      JOIN processing.llm_profiles p USING (profile_id) WHERE d.request_id=r.request_id) AS models
    FROM processing.run_requests r ORDER BY r.created_at DESC LIMIT 30`);
  return rows as {requestId:string;runId:string|null;job:string;status:string;stopReason:string|null;stopRequestedAt:string|null;createdAt:string;lastError:string|null;pendingExternal:number;models:string}[];
}
