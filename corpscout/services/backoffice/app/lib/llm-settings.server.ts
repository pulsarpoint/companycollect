import { createCipheriv, createDecipheriv, randomBytes, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { llmControl, llmTransaction, stopLlmRuns } from "./llm-control.server";

export const SETTINGS_DATABASE_PATH =
  process.env.BACKOFFICE_SETTINGS_DATABASE_PATH?.trim() ||
  join(process.cwd(), "data", "settings", "settings.sqlite");

export interface LlmProfile {
  profileId: string;
  name: string;
  provider: string;
  baseUrl: string;
  model: string;
  isActive: boolean;
  revision: number;
  state: "enabled" | "disabled" | "archived";
  disabledReason: string | null;
  lastCheck: {ok: boolean; message: string; target: string; checkedAt: string; failureKind: string | null} | null;
  apiKeyAvailable: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface SaveLlmProfileInput {
  profileId?: string;
  name: string;
  provider: string;
  baseUrl: string;
  model: string;
  apiKey?: string;
}

export class LlmSettingsValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LlmSettingsValidationError";
  }
}

function connectSettingsDatabase(databasePath: string): DatabaseSync {
  const absolutePath = resolve(databasePath);
  mkdirSync(dirname(absolutePath), { recursive: true });
  const database = new DatabaseSync(absolutePath);
  database.exec("PRAGMA busy_timeout = 5000");
  database.exec("PRAGMA journal_mode = WAL");
  database.exec(`CREATE TABLE IF NOT EXISTS local_llm_setting (
    setting_key TEXT PRIMARY KEY, setting_value TEXT NOT NULL, updated_at TEXT NOT NULL
  )`);
  return database;
}

function encryptionKey(): Buffer {
  const key = process.env.CRAWLER_LLM_ENCRYPTION_KEY ?? "";
  if (!/^[a-fA-F0-9]{64}$/.test(key)) {
    throw new LlmSettingsValidationError("Configure the same 64-character hexadecimal CRAWLER_LLM_ENCRYPTION_KEY in Backoffice and the crawler before saving or using API keys.");
  }
  return Buffer.from(key, "hex");
}

function encryptStoredApiKey(profileId: string, apiKey: string): string {
  if (!apiKey || Buffer.byteLength(apiKey, "utf8") > 8192 || /[\x00-\x1f\x7f]/.test(apiKey)) {
    throw new LlmSettingsValidationError("API key is required and must contain at most 8192 bytes without control characters.");
  }
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", encryptionKey(), nonce);
  cipher.setAAD(Buffer.from(`corpscout-llm-profile:v1\0${profileId}`, "utf8"));
  const encrypted = Buffer.concat([cipher.update(apiKey, "utf8"), cipher.final(), cipher.getAuthTag()]);
  return `v1.${nonce.toString("base64url")}.${encrypted.toString("base64url")}`;
}

function decryptStoredApiKey(profileId: string, encrypted: string): string {
  const key = encryptionKey();
  try {
    const parts = /^v1\.([A-Za-z0-9_-]{16})\.([A-Za-z0-9_-]+)$/.exec(encrypted);
    if (!parts) throw new Error("Invalid envelope");
    const data = Buffer.from(parts[2], "base64url");
    if (data.length <= 16) throw new Error("Invalid ciphertext");
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(parts[1], "base64url"));
    decipher.setAAD(Buffer.from(`corpscout-llm-profile:v1\0${profileId}`, "utf8"));
    decipher.setAuthTag(data.subarray(-16));
    return Buffer.concat([decipher.update(data.subarray(0, -16)), decipher.final()]).toString("utf8");
  } catch {
    throw new LlmSettingsValidationError("The saved API key could not be decrypted. Restore the original encryption key or enter a replacement API key in LLM settings.");
  }
}

/** Server-only access to the exact immutable revision selected by the caller. */
export async function getLlmProfileApiKey(profileId: string, revision?: number): Promise<string> {
  const {rows} = await llmControl().query(`SELECT r.api_key_encrypted FROM processing.llm_profiles p
    JOIN processing.llm_profile_revisions r ON r.profile_id=p.profile_id AND r.revision=coalesce($2,p.current_revision)
    WHERE p.profile_id=$1 AND p.state <> 'archived'`, [profileId, revision ?? null]);
  if (!rows[0]?.api_key_encrypted) throw new LlmSettingsValidationError("The selected LLM API key is missing or the profile was removed.");
  return decryptStoredApiKey(profileId, rows[0].api_key_encrypted);
}

const LOCAL_CODEX_SETTING_KEY = "local_codex_enabled";

/** Whether launches may offer the local codex agent as an LLM option. */
export function isLocalCodexEnabled(
  databasePath = SETTINGS_DATABASE_PATH,
): boolean {
  const database = connectSettingsDatabase(databasePath);
  try {
    const row = database
      .prepare(
        "SELECT setting_value FROM local_llm_setting WHERE setting_key = ?",
      )
      .get(LOCAL_CODEX_SETTING_KEY) as
      | unknown
      | { setting_value: string }
      | undefined;
    return (row as { setting_value: string } | undefined)?.setting_value === "1";
  } finally {
    database.close();
  }
}

export function setLocalCodexEnabled(
  enabled: boolean,
  databasePath = SETTINGS_DATABASE_PATH,
): void {
  const database = connectSettingsDatabase(databasePath);
  try {
    database
      .prepare(
        `INSERT INTO local_llm_setting (setting_key, setting_value, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT (setting_key) DO UPDATE SET
           setting_value = excluded.setting_value,
           updated_at = excluded.updated_at`,
      )
      .run(
        LOCAL_CODEX_SETTING_KEY,
        enabled ? "1" : "0",
        new Date().toISOString(),
      );
  } finally {
    database.close();
  }
}

function requiredValue(value: string, label: string, maximumLength: number): string {
  const cleanValue = value.trim();
  if (cleanValue === "") {
    throw new LlmSettingsValidationError(`${label} is required.`);
  }
  if (/[\x00-\x1f\x7f]/.test(cleanValue)) {
    throw new LlmSettingsValidationError(`${label} must not contain control characters.`);
  }
  if (cleanValue.length > maximumLength) {
    throw new LlmSettingsValidationError(
      `${label} must contain at most ${maximumLength} characters.`,
    );
  }
  return cleanValue;
}

function validatedBaseUrl(value: string): string {
  const cleanValue = requiredValue(value, "Base URL", 2_048);
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(cleanValue);
  } catch {
    throw new LlmSettingsValidationError("Base URL must be a valid URL.");
  }
  if (!["http:", "https:"].includes(parsedUrl.protocol) || parsedUrl.username || parsedUrl.password || cleanValue.includes("?") || cleanValue.includes("#") || /\s/.test(cleanValue)) {
    throw new LlmSettingsValidationError(
      "Base URL must use HTTP(S) without credentials, query parameters, or fragments.",
    );
  }
  return cleanValue.replace(/\/+$/, "");
}

const PROFILE_QUERY = `SELECT p.profile_id AS "profileId", p.name, p.current_revision AS revision,
  p.state, p.disabled_reason AS "disabledReason", p.is_default AS "isActive",
  r.provider, r.base_url AS "baseUrl", r.model,
  (r.api_key_encrypted IS NOT NULL) AS "apiKeyAvailable", p.created_at::text AS "createdAt", p.updated_at::text AS "updatedAt",
  (SELECT jsonb_build_object('ok',c.ok,'message',c.message,'target',c.target,'checkedAt',c.finished_at,
    'failureKind',c.failure_kind) FROM processing.llm_checks c WHERE c.profile_id=p.profile_id
    AND c.revision=p.current_revision ORDER BY c.started_at DESC LIMIT 1) AS "lastCheck"
  FROM processing.llm_profiles p JOIN processing.llm_profile_revisions r
    ON r.profile_id=p.profile_id AND r.revision=p.current_revision`;

export async function listLlmProfiles(includeDisabled = false): Promise<LlmProfile[]> {
  const {rows} = await llmControl().query(PROFILE_QUERY + ` WHERE p.state <> 'archived'
    AND ($1 OR p.state='enabled') ORDER BY p.is_default DESC, lower(p.name),p.profile_id`, [includeDisabled]);
  return rows;
}

export async function getLlmProfile(profileId: string): Promise<LlmProfile | null> {
  if (!/^[0-9a-f-]{36}$/i.test(profileId)) return null;
  const {rows} = await llmControl().query(PROFILE_QUERY + " WHERE p.profile_id=$1 AND p.state <> 'archived'", [profileId]);
  return rows[0] ?? null;
}

export async function saveAndActivateLlmProfile(input: SaveLlmProfileInput): Promise<string> {
  const profileId = input.profileId?.trim() || randomUUID();
  const name = requiredValue(input.name, "Name", 120);
  const provider = requiredValue(input.provider, "Provider", 100);
  const baseUrl = validatedBaseUrl(input.baseUrl);
  const model = requiredValue(input.model, "Model", 200);
  const apiKey = input.apiKey?.trim() ?? "";
  try {
    return await llmTransaction(async client => {
      // Serialize changes to the singleton default and profile admission locks.
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('llm_catalog',0))");
      const {rows: [existing]} = await client.query(`SELECT p.*,r.api_key_encrypted FROM processing.llm_profiles p
        JOIN processing.llm_profile_revisions r ON r.profile_id=p.profile_id AND r.revision=p.current_revision
        WHERE p.profile_id=$1 FOR UPDATE OF p`, [profileId]);
      if (input.profileId && (!existing || existing.state === 'archived')) throw new LlmSettingsValidationError("LLM profile was not found.");
      if (!apiKey && !existing?.api_key_encrypted) throw new LlmSettingsValidationError("API key is required.");
      const encrypted = apiKey ? encryptStoredApiKey(profileId, apiKey) : existing.api_key_encrypted;
      const revision = (existing?.current_revision ?? 0) + 1;
      await client.query("UPDATE processing.llm_profiles SET is_default=false WHERE is_default");
      await client.query(`INSERT INTO processing.llm_profiles (profile_id,name,current_revision,is_default)
        VALUES ($1,$2,$3,true) ON CONFLICT (profile_id) DO UPDATE SET name=$2,current_revision=$3,
        state='enabled',is_default=true,disabled_reason=NULL,updated_at=now()`, [profileId,name,revision]);
      await client.query(`INSERT INTO processing.llm_profile_revisions
        (profile_id,revision,provider,base_url,model,api_key_encrypted) VALUES ($1,$2,$3,$4,$5,$6)`,
        [profileId,revision,provider,baseUrl,model,encrypted]);
      return profileId;
    });
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === '23505')
      throw new LlmSettingsValidationError("An LLM profile with this name already exists.");
    throw error;
  }
}

export async function activateLlmProfile(profileId: string): Promise<void> {
  await llmTransaction(async client => {
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended('llm_catalog',0))");
    const {rows: [profile]} = await client.query("SELECT state FROM processing.llm_profiles WHERE profile_id=$1 FOR UPDATE", [profileId]);
    if (profile?.state !== 'enabled') throw new LlmSettingsValidationError("Test and enable this model before making it the default.");
    await client.query("UPDATE processing.llm_profiles SET is_default=false WHERE is_default");
    await client.query("UPDATE processing.llm_profiles SET is_default=true,updated_at=now() WHERE profile_id=$1", [profileId]);
  });
}

export async function setLlmProfileState(profileId: string, state: 'disabled' | 'archived'): Promise<void> {
  await llmTransaction(async client => {
    await client.query("SELECT profile_id FROM processing.llm_profiles WHERE profile_id=$1 FOR UPDATE", [profileId]);
    const reason = state === 'archived' ? 'Model removed from configuration.' : 'Model disabled by an operator.';
    await client.query(`UPDATE processing.llm_profiles SET state=$2,is_default=false,disabled_reason=$3,updated_at=now()
      WHERE profile_id=$1 AND state <> 'archived'`, [profileId,state,reason]);
    await stopLlmRuns(client, profileId, reason);
  });
}

export type LlmFailureKind = 'configuration' | 'transient' | 'capability' | 'service';
export async function recordLlmCheck(profile: LlmProfile, target: 'crawler' | 'brave', startedAt: string,
  ok: boolean, message: string, failureKind: LlmFailureKind | null, enable = false) {
  await llmTransaction(async client => {
    const {rows: [current]} = await client.query("SELECT * FROM processing.llm_profiles WHERE profile_id=$1 FOR UPDATE", [profile.profileId]);
    if (!current || current.state === 'archived') return;
    const check = await client.query(`INSERT INTO processing.llm_checks (profile_id,revision,target,started_at,ok,message,failure_kind)
      VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (profile_id,revision,target) DO UPDATE SET
      started_at=excluded.started_at,finished_at=now(),ok=excluded.ok,message=excluded.message,failure_kind=excluded.failure_kind
      WHERE processing.llm_checks.started_at < excluded.started_at RETURNING profile_id`,
      [profile.profileId,profile.revision,target,startedAt,ok,message,failureKind]);
    if (!check.rowCount) return;
    // Tests for different capabilities share one credential health decision.
    const newer = await client.query(`SELECT 1 FROM processing.llm_checks WHERE profile_id=$1 AND revision=$2
      AND started_at > $3 AND (ok OR failure_kind='configuration') LIMIT 1`, [profile.profileId,profile.revision,startedAt]);
    if (newer.rowCount) return;
    // An obsolete configuration can still have active runs, but cannot disable its replacement.
    if (!ok && failureKind === 'configuration') {
      await client.query(`UPDATE processing.llm_profile_revisions SET invalidated_at=now(),invalid_reason=$3
        WHERE profile_id=$1 AND revision=$2`, [profile.profileId,profile.revision,message]);
      if (current.current_revision === profile.revision) await client.query(`UPDATE processing.llm_profiles SET
        state='disabled',is_default=false,disabled_reason=$2,updated_at=now() WHERE profile_id=$1`, [profile.profileId,message]);
      await stopLlmRuns(client,profile.profileId,message,profile.revision);
    } else if (ok && enable && current.current_revision === profile.revision && new Date(current.updated_at) <= new Date(startedAt)) {
      await client.query(`UPDATE processing.llm_profile_revisions SET invalidated_at=NULL,invalid_reason=NULL
        WHERE profile_id=$1 AND revision=$2`, [profile.profileId,profile.revision]);
      await client.query(`UPDATE processing.llm_profiles SET state='enabled',disabled_reason=NULL,updated_at=now()
        WHERE profile_id=$1`, [profile.profileId]);
    }
  });
}

/** Explicit, idempotent upgrade. SQLite remains only for local settings and prompts. */
export async function importLegacyLlmProfiles(databasePath = SETTINGS_DATABASE_PATH): Promise<number> {
  const database = new DatabaseSync(resolve(databasePath), {readOnly: true});
  try {
    const rows = database.prepare('SELECT * FROM llm_profile').all();
    return await llmTransaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('llm_catalog',0))");
      if ((await client.query('SELECT source FROM processing.llm_catalog_imports WHERE source=$1', [resolve(databasePath)])).rowCount) return 0;
      for (const row of rows) {
        if (row.api_key_encrypted) decryptStoredApiKey(String(row.profile_id), String(row.api_key_encrypted));
        await client.query(`INSERT INTO processing.llm_profiles (profile_id,name,current_revision,is_default,created_at,updated_at)
          VALUES ($1,$2,1,$3,$4,$5)`, [row.profile_id,row.name,row.is_active === 1,row.created_at,row.updated_at]);
        await client.query(`INSERT INTO processing.llm_profile_revisions (profile_id,revision,provider,base_url,model,api_key_encrypted,created_at)
          VALUES ($1,1,$2,$3,$4,$5,$6)`, [row.profile_id,row.provider,row.base_url,row.model,row.api_key_encrypted,row.created_at]);
      }
      await client.query('INSERT INTO processing.llm_catalog_imports (source,profile_count) VALUES ($1,$2)', [resolve(databasePath),rows.length]);
      return rows.length;
    });
  } finally { database.close(); }
}
