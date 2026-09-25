import { createCipheriv, createDecipheriv, randomBytes, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

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

interface StoredLlmProfile {
  profile_id: string;
  name: string;
  provider: string;
  base_url: string;
  model: string;
  api_key_encrypted: string | null;
  is_active: number;
  created_at: string;
  updated_at: string;
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
  database.exec(`
    CREATE TABLE IF NOT EXISTS llm_profile (
      profile_id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE CHECK (trim(name) != ''),
      provider TEXT NOT NULL CHECK (trim(provider) != ''),
      base_url TEXT NOT NULL CHECK (trim(base_url) != ''),
      model TEXT NOT NULL CHECK (trim(model) != ''),
      api_key_encrypted TEXT,
      is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0, 1)),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS llm_profile_single_active
      ON llm_profile(is_active)
      WHERE is_active = 1;
    CREATE TABLE IF NOT EXISTS local_llm_setting (
      setting_key TEXT PRIMARY KEY,
      setting_value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  try {
    // Existing profiles referenced process variables. Import each available key once,
    // then remove the reference so all future reads use encrypted database storage.
    const columns = database.prepare("PRAGMA table_info(llm_profile)").all();
    if (columns.some(column => column.name === "api_key_environment_variable")) {
      database.exec("BEGIN IMMEDIATE");
      const currentColumns = database.prepare("PRAGMA table_info(llm_profile)").all();
      if (currentColumns.some(column => column.name === "api_key_environment_variable")) {
        database.exec("ALTER TABLE llm_profile ADD COLUMN api_key_encrypted TEXT");
        const rows = database.prepare("SELECT profile_id, api_key_environment_variable FROM llm_profile").all();
        const update = database.prepare("UPDATE llm_profile SET api_key_encrypted = ? WHERE profile_id = ?");
        for (const row of rows) {
          const apiKey = process.env[String(row.api_key_environment_variable)]?.trim();
          update.run(apiKey ? encryptStoredApiKey(String(row.profile_id), apiKey) : null, row.profile_id);
        }
        database.exec("ALTER TABLE llm_profile DROP COLUMN api_key_environment_variable");
      }
      database.exec("COMMIT");
    }
    return database;
  } catch (error) {
    if (database.isTransaction) database.exec("ROLLBACK");
    database.close();
    throw error;
  }
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

/** Server-only credential access. Public profile responses never include key material. */
export function getLlmProfileApiKey(profileId: string, databasePath = SETTINGS_DATABASE_PATH): string {
  const database = connectSettingsDatabase(databasePath);
  try {
    const row = database.prepare("SELECT api_key_encrypted FROM llm_profile WHERE profile_id = ?").get(profileId) as {api_key_encrypted: string | null} | undefined;
    if (!row) throw new LlmSettingsValidationError("LLM profile was not found.");
    if (!row.api_key_encrypted) throw new LlmSettingsValidationError("The selected LLM API key is missing. Add it in LLM settings.");
    return decryptStoredApiKey(profileId, row.api_key_encrypted);
  } finally {
    database.close();
  }
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

function mapStoredProfile(row: StoredLlmProfile): LlmProfile {
  return {
    profileId: row.profile_id,
    name: row.name,
    provider: row.provider,
    baseUrl: row.base_url,
    model: row.model,
    isActive: row.is_active === 1,
    apiKeyAvailable: Boolean(row.api_key_encrypted),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function listLlmProfiles(
  databasePath = SETTINGS_DATABASE_PATH,
): LlmProfile[] {
  const database = connectSettingsDatabase(databasePath);
  try {
    const rows = database
      .prepare(
        `SELECT *
         FROM llm_profile
         ORDER BY is_active DESC, name COLLATE NOCASE, profile_id`,
      )
      .all() as unknown as StoredLlmProfile[];
    return rows.map(mapStoredProfile);
  } finally {
    database.close();
  }
}

export function getLlmProfile(
  profileId: string,
  databasePath = SETTINGS_DATABASE_PATH,
): LlmProfile | null {
  const database = connectSettingsDatabase(databasePath);
  try {
    const row = database
      .prepare("SELECT * FROM llm_profile WHERE profile_id = ?")
      .get(profileId) as unknown as StoredLlmProfile | undefined;
    return row ? mapStoredProfile(row) : null;
  } finally {
    database.close();
  }
}

export function saveAndActivateLlmProfile(
  input: SaveLlmProfileInput,
  databasePath = SETTINGS_DATABASE_PATH,
): string {
  const profileId = input.profileId?.trim() || randomUUID();
  const name = requiredValue(input.name, "Name", 120);
  const provider = requiredValue(input.provider, "Provider", 100);
  const baseUrl = validatedBaseUrl(input.baseUrl);
  const model = requiredValue(input.model, "Model", 200);
  const apiKey = input.apiKey?.trim() ?? "";
  const database = connectSettingsDatabase(databasePath);
  const now = new Date().toISOString();
  try {
    database.exec("BEGIN IMMEDIATE");
    const existing = database
      .prepare("SELECT created_at, api_key_encrypted FROM llm_profile WHERE profile_id = ?")
      .get(profileId) as unknown as { created_at: string; api_key_encrypted: string | null } | undefined;
    if (!apiKey && !existing?.api_key_encrypted) {
      throw new LlmSettingsValidationError("API key is required.");
    }
    const encryptedApiKey = apiKey ? encryptStoredApiKey(profileId, apiKey) : existing!.api_key_encrypted;
    database.prepare("UPDATE llm_profile SET is_active = 0").run();
    database
      .prepare(
        `INSERT INTO llm_profile (
          profile_id,
          name,
          provider,
          base_url,
          model,
          api_key_encrypted,
          is_active,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT (profile_id) DO UPDATE SET
          name = excluded.name,
          provider = excluded.provider,
          base_url = excluded.base_url,
          model = excluded.model,
          api_key_encrypted = excluded.api_key_encrypted,
          is_active = 1,
          updated_at = excluded.updated_at`,
      )
      .run(
        profileId,
        name,
        provider,
        baseUrl,
        model,
        encryptedApiKey,
        existing?.created_at ?? now,
        now,
      );
    database.exec("COMMIT");
    return profileId;
  } catch (error) {
    if (database.isTransaction) database.exec("ROLLBACK");
    if (
      error instanceof Error &&
      error.message.includes("UNIQUE constraint failed: llm_profile.name")
    ) {
      throw new LlmSettingsValidationError(
        "An LLM profile with this name already exists.",
      );
    }
    throw error;
  } finally {
    database.close();
  }
}

export function activateLlmProfile(
  profileId: string,
  databasePath = SETTINGS_DATABASE_PATH,
): void {
  const cleanProfileId = profileId.trim();
  if (cleanProfileId === "") {
    throw new LlmSettingsValidationError("LLM profile is required.");
  }
  const database = connectSettingsDatabase(databasePath);
  try {
    const existing = database
      .prepare("SELECT profile_id FROM llm_profile WHERE profile_id = ?")
      .get(cleanProfileId);
    if (!existing) {
      throw new LlmSettingsValidationError("LLM profile was not found.");
    }
    database.exec("BEGIN IMMEDIATE");
    database.prepare("UPDATE llm_profile SET is_active = 0").run();
    database
      .prepare(
        "UPDATE llm_profile SET is_active = 1, updated_at = ? WHERE profile_id = ?",
      )
      .run(new Date().toISOString(), cleanProfileId);
    database.exec("COMMIT");
  } catch (error) {
    if (database.isTransaction) database.exec("ROLLBACK");
    throw error;
  } finally {
    database.close();
  }
}
