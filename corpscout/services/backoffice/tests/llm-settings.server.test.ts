import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { admitLlmRun, acknowledgeLlmRun, llmDependencies } from "~/lib/llm-runs.server";
import { verifySelectedLlm } from "~/lib/crawl-llm.server";
vi.mock("~/lib/crawl-llm.server", () => ({verifySelectedLlm: vi.fn()}));
import { llmControl } from "~/lib/llm-control.server";
import { activateLlmProfile, getLlmProfile, getLlmProfileApiKey, isLocalCodexEnabled,
  listLlmProfiles, saveAndActivateLlmProfile, setLocalCodexEnabled, setLlmProfileState, recordLlmCheck } from "~/lib/llm-settings.server";

// Run with an isolated database initialized by migrations 127/128. Never use the live catalog.
const suite = process.env.LLM_TEST_PG_URL ? describe : describe.skip;
const secret = 'test-only-key';
const input = {name:'Production',provider:'Provider',baseUrl:'https://provider.example/v1/',model:'model',apiKey:secret};
suite('PostgreSQL LLM lifecycle', () => {
  beforeEach(async () => {
    vi.stubEnv('LLM_CONTROL_PG_URL', process.env.LLM_TEST_PG_URL!);
    vi.stubEnv('CRAWLER_LLM_ENCRYPTION_KEY','ab'.repeat(32));
    const db = llmControl();
    const {rows:[row]} = await db.query('SELECT current_database() AS name');
    if (row.name !== 'llm_lifecycle_test') throw new Error('An isolated llm_lifecycle_test database is required');
    await db.query('TRUNCATE processing.run_requests,processing.llm_profiles,processing.llm_catalog_imports CASCADE');
  });
  afterEach(() => vi.unstubAllEnvs());
  afterAll(async () => { await llmControl().end(); });
  async function run(profileId: string, revision = 1) {
    const id = randomUUID();
    await llmControl().query("INSERT INTO processing.run_requests(request_id,job_name) VALUES ($1,'test')",[id]);
    await llmControl().query("INSERT INTO processing.run_llm_dependencies VALUES ($1,$2,$3,'test')",[id,profileId,revision]);
    return id;
  }
  async function stopped(id: string) {
    return (await llmControl().query('SELECT stop_requested_at IS NOT NULL AS stopped FROM processing.run_requests WHERE request_id=$1',[id])).rows[0].stopped;
  }
  it('persists exact dependencies before launch, retains uncertain launches, and fences removal during verification', async () => {
    const id = await saveAndActivateLlmProfile(input);
    const envelope = {profile_id:id,profile_revision:1,provider:input.provider,base_url:'https://provider.example/v1',model:input.model,api_key_encrypted:'test-envelope'};
    vi.mocked(verifySelectedLlm).mockResolvedValue(envelope);
    const admitted = await admitLlmRun('test_job',{ops:{crawl:{config:{llm:envelope}}}});
    expect(admitted).not.toBeNull();
    const rows = (await llmControl().query('SELECT * FROM processing.run_requests')).rows;
    expect(rows[0].status).toBe('launching');
    expect(rows[0].dagster_run_id).toBeNull();
    const runId = randomUUID();
    await acknowledgeLlmRun(admitted!.requestId,'queued',runId);
    expect((await llmControl().query('SELECT dagster_run_id FROM processing.run_requests')).rows[0].dagster_run_id).toBe(runId);
    vi.mocked(verifySelectedLlm).mockImplementationOnce(async () => {await setLlmProfileState(id,'archived'); return envelope;});
    await expect(admitLlmRun('test_job',{llm:envelope})).rejects.toThrow('disabled or removed');
    expect((await llmControl().query('SELECT count(*) FROM processing.run_requests')).rows[0].count).toBe('1');
    expect(await stopped(admitted!.requestId)).toBe(true);
    expect(() => llmDependencies({api_key_encrypted:'old-untracked-key'})).toThrow('no saved LLM revision');
  });
  it('keeps immutable encrypted revisions and never returns key material in public metadata', async () => {
    const id = await saveAndActivateLlmProfile(input);
    await saveAndActivateLlmProfile({...input,profileId:id,model:'replacement',apiKey:''});
    expect(await getLlmProfileApiKey(id,1)).toBe(secret);
    expect(await getLlmProfileApiKey(id,2)).toBe(secret);
    const rows = (await llmControl().query('SELECT * FROM processing.llm_profile_revisions ORDER BY revision')).rows;
    expect(rows.map(r=>r.model)).toEqual(['model','replacement']);
    expect(JSON.stringify(rows)).not.toContain(secret);
    expect(rows[0].api_key_encrypted).toMatch(/^v1\./);
    expect(await listLlmProfiles()).toEqual([expect.objectContaining({revision:2,apiKeyAvailable:true})]);
    expect(JSON.stringify(await listLlmProfiles())).not.toContain(rows[0].api_key_encrypted);
    await expect(llmControl().query("UPDATE processing.llm_profile_revisions SET model='tampered'")).rejects.toThrow('immutable');
    vi.stubEnv('CRAWLER_LLM_ENCRYPTION_KEY','cd'.repeat(32));
    await expect(getLlmProfileApiKey(id)).rejects.toThrow('could not be decrypted');
  });
  it('archives a model, removes it from all selections, and stops only its unfinished tasks', async () => {
    const id = await saveAndActivateLlmProfile(input);
    const other = await saveAndActivateLlmProfile({...input,name:'Other'});
    const active = await run(id), completed = await run(id), unrelated = await run(other);
    await llmControl().query("UPDATE processing.run_requests SET status='succeeded',finished_at=now() WHERE request_id=$1",[completed]);
    await setLlmProfileState(id,'archived');
    expect(await getLlmProfile(id)).toBeNull();
    expect((await listLlmProfiles(true)).map(p=>p.profileId)).toEqual([other]);
    expect(await stopped(active)).toBe(true);
    expect(await stopped(completed)).toBe(false);
    expect(await stopped(unrelated)).toBe(false);
    expect((await llmControl().query('SELECT count(*) FROM processing.llm_profile_revisions')).rows[0].count).toBe('2');
    await expect(activateLlmProfile(id)).rejects.toThrow('Test and enable');
  });
  it('records transient failures without disabling or canceling', async () => {
    const id = await saveAndActivateLlmProfile(input), active = await run(id);
    await recordLlmCheck((await getLlmProfile(id))!,'crawler',new Date().toISOString(),false,'Rate limited','transient');
    expect((await getLlmProfile(id))?.state).toBe('enabled');
    expect(await stopped(active)).toBe(false);
  });
  it('invalidates only the tested revision when a newer configuration already exists', async () => {
    const id = await saveAndActivateLlmProfile(input), oldProfile = (await getLlmProfile(id))!;
    const oldRun = await run(id);
    await saveAndActivateLlmProfile({...input,profileId:id,model:'new'});
    const newRun = await run(id,2);
    await recordLlmCheck(oldProfile,'crawler',new Date().toISOString(),false,'Retired model','configuration');
    expect((await getLlmProfile(id))?.state).toBe('enabled');
    expect(await stopped(oldRun)).toBe(true);
    expect(await stopped(newRun)).toBe(false);
  });
  it('ignores stale checks and requires a successful explicit test to re-enable', async () => {
    const id = await saveAndActivateLlmProfile(input), p = (await getLlmProfile(id))!;
    await recordLlmCheck(p,'crawler','2030-01-02',false,'Invalid credential','configuration');
    expect((await getLlmProfile(id))?.state).toBe('disabled');
    await recordLlmCheck(p,'crawler','2030-01-01',true,'Old pass',null,true);
    expect((await getLlmProfile(id))?.state).toBe('disabled');
    await recordLlmCheck(p,'crawler','2030-01-03',true,'Fixed',null,true);
    expect((await getLlmProfile(id))?.state).toBe('enabled');
  });
  it('keeps one default and rolls back an invalid save without changing it', async () => {
    const first = await saveAndActivateLlmProfile(input);
    const second = await saveAndActivateLlmProfile({...input,name:'Second'});
    await expect(saveAndActivateLlmProfile(input)).rejects.toThrow('already exists');
    expect((await listLlmProfiles()).find(p=>p.isActive)?.profileId).toBe(second);
    await activateLlmProfile(first);
    expect((await listLlmProfiles()).find(p=>p.isActive)?.profileId).toBe(first);
  });
  it.each(['','key\nsecret','x'.repeat(8193)])('rejects invalid credentials',async apiKey => {
    await expect(saveAndActivateLlmProfile({...input,apiKey})).rejects.toThrow('API key');
  });
  it.each(['file:///tmp/model','https://user:pass@example.org/v1','https://example.org?key=secret'])('rejects unsafe endpoints',async baseUrl => {
    await expect(saveAndActivateLlmProfile({...input,baseUrl})).rejects.toThrow('Base URL');
  });
});

it('keeps local Codex settings independent of the remote catalog', () => {
  const directory = mkdtempSync(join(tmpdir(),'llm-local-'));
  try {
    const path = join(directory,'settings.sqlite');
    expect(isLocalCodexEnabled(path)).toBe(false);
    setLocalCodexEnabled(true,path);
    expect(isLocalCodexEnabled(path)).toBe(true);
  } finally { rmSync(directory,{recursive:true,force:true}); }
});
