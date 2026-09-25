import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  activateLlmProfile, getLlmProfileApiKey, isLocalCodexEnabled, listLlmProfiles,
  saveAndActivateLlmProfile, setLocalCodexEnabled,
} from "~/lib/llm-settings.server";

let directory: string;
let databasePath: string;
const secret = 'sk-direct-api-key-with-hyphens';
const input = { name: "Production", provider: "Provider", baseUrl: "https://provider.example/v1/", model: "model", apiKey: secret };
beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "backoffice-settings-"));
  databasePath = join(directory, "settings.sqlite");
  vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "ab".repeat(32));
});
afterEach(() => {
  vi.unstubAllEnvs();
  rmSync(directory, { recursive: true, force: true });
});
function storedRows() {
  const database = new DatabaseSync(databasePath);
  try { return database.prepare("SELECT * FROM llm_profile ORDER BY name").all(); }
  finally { database.close(); }
}
function legacyDatabase() {
  const database = new DatabaseSync(databasePath);
  database.exec(`CREATE TABLE llm_profile (
    profile_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, provider TEXT NOT NULL,
    base_url TEXT NOT NULL, model TEXT NOT NULL,
    api_key_environment_variable TEXT NOT NULL CHECK (trim(api_key_environment_variable) != ''),
    is_active INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  );
  CREATE UNIQUE INDEX llm_profile_single_active ON llm_profile(is_active) WHERE is_active = 1;
  INSERT INTO llm_profile VALUES ('old', 'Existing', 'Provider', 'https://provider.example/v1', 'old-model', 'LEGACY_KEY', 1, '2026-01-01', '2026-01-01');
  INSERT INTO llm_profile VALUES ('missing', 'Missing key', 'Provider', 'https://provider.example/v1', 'model', 'ABSENT_LEGACY_KEY', 0, '2026-01-01', '2026-01-01');`);
  database.close();
}

describe("encrypted LLM settings", () => {
  it("stores authenticated ciphertext and returns only public metadata", () => {
    const id = saveAndActivateLlmProfile(input, databasePath);
    expect(getLlmProfileApiKey(id, databasePath)).toBe(secret);
    expect(storedRows()[0].api_key_encrypted).toMatch(/^v1\./);
    expect(storedRows()[0]).not.toHaveProperty("api_key_environment_variable");
    expect(JSON.stringify(storedRows())).not.toContain(secret);
    const profiles = listLlmProfiles(databasePath);
    expect(profiles).toEqual([expect.objectContaining({ profileId: id, baseUrl: "https://provider.example/v1", apiKeyAvailable: true })]);
    expect(profiles[0]).not.toHaveProperty("api_key_encrypted");
    expect(profiles[0]).not.toHaveProperty("apiKey");
    expect(profiles[0]).not.toHaveProperty("apiKeyEnvironmentVariable");
    for (const suffix of ["", "-wal", "-shm"]) {
      if (existsSync(databasePath + suffix)) expect(readFileSync(databasePath + suffix).includes(Buffer.from(secret))).toBe(false);
    }
  });
  it("retains a saved key on blank edits and replaces it only when supplied", () => {
    const id = saveAndActivateLlmProfile(input, databasePath);
    const encrypted = storedRows()[0].api_key_encrypted;
    saveAndActivateLlmProfile({ ...input, profileId: id, model: "new-model", apiKey: "" }, databasePath);
    expect(getLlmProfileApiKey(id, databasePath)).toBe(secret);
    expect(storedRows()[0].api_key_encrypted).toBe(encrypted);
    saveAndActivateLlmProfile({ ...input, profileId: id, apiKey: "replacement" }, databasePath);
    expect(getLlmProfileApiKey(id, databasePath)).toBe("replacement");
    expect(storedRows()[0].api_key_encrypted).not.toBe(encrypted);
  });
  it("uses fresh nonces and rejects swapping ciphertext across profiles", () => {
    const first = saveAndActivateLlmProfile(input, databasePath);
    const second = saveAndActivateLlmProfile({ ...input, name: "Second" }, databasePath);
    const rows = storedRows();
    expect(rows[0].api_key_encrypted).not.toBe(rows[1].api_key_encrypted);
    const database = new DatabaseSync(databasePath);
    database.prepare("UPDATE llm_profile SET api_key_encrypted = ? WHERE profile_id = ?").run(rows[0].api_key_encrypted, second);
    database.close();
    expect(getLlmProfileApiKey(first, databasePath)).toBe(secret);
    expect(() => getLlmProfileApiKey(second, databasePath)).toThrow("could not be decrypted");
  });
  it("fails safely on wrong encryption keys and modified ciphertext", () => {
    const id = saveAndActivateLlmProfile(input, databasePath);
    vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "cd".repeat(32));
    expect(() => getLlmProfileApiKey(id, databasePath)).toThrow("could not be decrypted");
    vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "ab".repeat(32));
    const database = new DatabaseSync(databasePath);
    database.prepare("UPDATE llm_profile SET api_key_encrypted = ?").run("invalid-envelope");
    database.close();
    expect(() => getLlmProfileApiKey(id, databasePath)).toThrow("could not be decrypted");
  });
  it.each(["", "not-a-master-key"])("blocks saves without a valid master key: %s", key => {
    vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", key);
    expect(() => saveAndActivateLlmProfile(input, databasePath)).toThrow("CRAWLER_LLM_ENCRYPTION_KEY");
    expect(listLlmProfiles(databasePath)).toEqual([]);
  });
  it.each(["", "key\nsecret", "x".repeat(8193)])("requires a valid direct key for new profiles", apiKey => {
    expect(() => saveAndActivateLlmProfile({ ...input, apiKey }, databasePath)).toThrow("API key");
    expect(listLlmProfiles(databasePath)).toEqual([]);
  });
  it.each(["file:///tmp/model", "https://user:password@provider.example/v1", "https://provider.example/v1?api_key=secret"])("rejects credential-bearing or invalid endpoints", baseUrl => {
    expect(() => saveAndActivateLlmProfile({ ...input, baseUrl }, databasePath)).toThrow("Base URL");
  });
  it.each(["provider", "model"])("rejects control characters in %s before saving", field => {
    expect(() => saveAndActivateLlmProfile({ ...input, [field]: "invalid\nmetadata" }, databasePath)).toThrow("control characters");
    expect(listLlmProfiles(databasePath)).toEqual([]);
  });
  it("keeps one active profile and rolls back activation when a save fails", () => {
    const first = saveAndActivateLlmProfile(input, databasePath);
    const second = saveAndActivateLlmProfile({ ...input, name: "Second" }, databasePath);
    expect(() => saveAndActivateLlmProfile(input, databasePath)).toThrow("already exists");
    expect(listLlmProfiles(databasePath).filter(profile => profile.isActive).map(profile => profile.profileId)).toEqual([second]);
    activateLlmProfile(first, databasePath);
    expect(listLlmProfiles(databasePath).filter(profile => profile.isActive).map(profile => profile.profileId)).toEqual([first]);
  });
});

describe("legacy environment reference migration", () => {
  it("imports available keys once, preserving metadata and marking missing keys for replacement", () => {
    vi.stubEnv("LEGACY_KEY", secret);
    vi.stubEnv("ABSENT_LEGACY_KEY", "");
    legacyDatabase();
    expect(listLlmProfiles(databasePath)).toEqual([
      expect.objectContaining({profileId: "old", apiKeyAvailable: true, isActive: true, createdAt: "2026-01-01"}),
      expect.objectContaining({profileId: "missing", apiKeyAvailable: false}),
    ]);
    vi.stubEnv("LEGACY_KEY", "");
    expect(getLlmProfileApiKey("old", databasePath)).toBe(secret);
    expect(() => getLlmProfileApiKey("missing", databasePath)).toThrow("API key is missing");
    expect(storedRows()[0]).not.toHaveProperty("api_key_environment_variable");
    expect(listLlmProfiles(databasePath)).toHaveLength(2);
  });
  it("rolls back migration if encryption is unavailable, so credentials can be migrated later", () => {
    vi.stubEnv("LEGACY_KEY", secret);
    vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "");
    legacyDatabase();
    expect(() => listLlmProfiles(databasePath)).toThrow("CRAWLER_LLM_ENCRYPTION_KEY");
    expect(storedRows()[0]).toHaveProperty("api_key_environment_variable", "LEGACY_KEY");
    expect(storedRows()[0]).not.toHaveProperty("api_key_encrypted");
    vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "ab".repeat(32));
    expect(getLlmProfileApiKey("old", databasePath)).toBe(secret);
  });
});

describe("local Codex setting", () => {
  it("persists independently of remote keys and activation", () => {
    expect(isLocalCodexEnabled(databasePath)).toBe(false);
    setLocalCodexEnabled(true, databasePath);
    saveAndActivateLlmProfile(input, databasePath);
    expect(isLocalCodexEnabled(databasePath)).toBe(true);
    setLocalCodexEnabled(false, databasePath);
    expect(isLocalCodexEnabled(databasePath)).toBe(false);
  });
});
