import { randomUUID } from "node:crypto";
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
  apiKeyEnvironmentVariable: string;
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
  apiKeyEnvironmentVariable: string;
}

interface StoredLlmProfile {
  profile_id: string;
  name: string;
  provider: string;
  base_url: string;
  model: string;
  api_key_environment_variable: string;
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
      api_key_environment_variable TEXT NOT NULL CHECK (
        trim(api_key_environment_variable) != ''
      ),
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
  return database;
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
  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    throw new LlmSettingsValidationError(
      "Base URL must use the http or https protocol.",
    );
  }
  return cleanValue.replace(/\/+$/, "");
}

function validatedEnvironmentVariable(value: string): string {
  const cleanValue = requiredValue(value, "API key environment variable", 128);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(cleanValue)) {
    throw new LlmSettingsValidationError(
      "API key environment variable must be a valid environment variable name.",
    );
  }
  return cleanValue;
}

function mapStoredProfile(row: StoredLlmProfile): LlmProfile {
  return {
    profileId: row.profile_id,
    name: row.name,
    provider: row.provider,
    baseUrl: row.base_url,
    model: row.model,
    apiKeyEnvironmentVariable: row.api_key_environment_variable,
    isActive: row.is_active === 1,
    apiKeyAvailable: Boolean(
      process.env[row.api_key_environment_variable]?.trim(),
    ),
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
  const provider = requiredValue(input.provider, "Provider", 120);
  const baseUrl = validatedBaseUrl(input.baseUrl);
  const model = requiredValue(input.model, "Model", 240);
  const apiKeyEnvironmentVariable = validatedEnvironmentVariable(
    input.apiKeyEnvironmentVariable,
  );
  const database = connectSettingsDatabase(databasePath);
  const now = new Date().toISOString();
  try {
    database.exec("BEGIN IMMEDIATE");
    const existing = database
      .prepare("SELECT created_at FROM llm_profile WHERE profile_id = ?")
      .get(profileId) as unknown as { created_at: string } | undefined;
    database.prepare("UPDATE llm_profile SET is_active = 0").run();
    database
      .prepare(
        `INSERT INTO llm_profile (
          profile_id,
          name,
          provider,
          base_url,
          model,
          api_key_environment_variable,
          is_active,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT (profile_id) DO UPDATE SET
          name = excluded.name,
          provider = excluded.provider,
          base_url = excluded.base_url,
          model = excluded.model,
          api_key_environment_variable = excluded.api_key_environment_variable,
          is_active = 1,
          updated_at = excluded.updated_at`,
      )
      .run(
        profileId,
        name,
        provider,
        baseUrl,
        model,
        apiKeyEnvironmentVariable,
        existing?.created_at ?? now,
        now,
      );
    database.exec("COMMIT");
    return profileId;
  } catch (error) {
    database.exec("ROLLBACK");
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
