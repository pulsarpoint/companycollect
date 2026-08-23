import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import {
  activateLlmProfile,
  listLlmProfiles,
  LlmSettingsValidationError,
  saveAndActivateLlmProfile,
} from "~/lib/llm-settings.server";

const temporaryDirectories: string[] = [];

function temporarySettingsPath(): string {
  const directory = mkdtempSync(join(tmpdir(), "backoffice-settings-"));
  temporaryDirectories.push(directory);
  return join(directory, "settings", "settings.sqlite");
}

afterEach(() => {
  delete process.env.TEST_BACKOFFICE_LLM_KEY;
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("LLM settings SQLite store", () => {
  it("creates a separate settings database without a secret-value column", () => {
    const databasePath = temporarySettingsPath();

    expect(listLlmProfiles(databasePath)).toEqual([]);
    expect(existsSync(databasePath)).toBe(true);

    const database = new DatabaseSync(databasePath, { readOnly: true });
    try {
      const columns = database
        .prepare("PRAGMA table_info(llm_profile)")
        .all() as unknown as Array<{ name: string }>;
      expect(columns.map((column) => column.name)).toContain(
        "api_key_environment_variable",
      );
      expect(columns.map((column) => column.name)).not.toContain("api_key");
      expect(columns.map((column) => column.name)).not.toContain("secret");
    } finally {
      database.close();
    }
  });

  it("stores multiple profiles while keeping exactly one active", () => {
    const databasePath = temporarySettingsPath();
    process.env.TEST_BACKOFFICE_LLM_KEY = "available-in-process";

    const firstProfileId = saveAndActivateLlmProfile(
      {
        name: "DeepSeek production",
        provider: "DeepSeek",
        baseUrl: "https://api.deepseek.com/",
        model: "deepseek-v4-flash",
        apiKeyEnvironmentVariable: "TEST_BACKOFFICE_LLM_KEY",
      },
      databasePath,
    );
    const secondProfileId = saveAndActivateLlmProfile(
      {
        name: "Local model",
        provider: "OpenAI compatible",
        baseUrl: "http://localhost:11434/v1",
        model: "local-model",
        apiKeyEnvironmentVariable: "LOCAL_LLM_API_KEY",
      },
      databasePath,
    );

    let profiles = listLlmProfiles(databasePath);
    expect(profiles).toHaveLength(2);
    expect(profiles.filter((profile) => profile.isActive)).toHaveLength(1);
    expect(profiles.find((profile) => profile.profileId === secondProfileId)).toMatchObject({
      isActive: true,
      apiKeyAvailable: false,
    });
    expect(profiles.find((profile) => profile.profileId === firstProfileId)).toMatchObject({
      baseUrl: "https://api.deepseek.com",
      isActive: false,
      apiKeyAvailable: true,
    });

    activateLlmProfile(firstProfileId, databasePath);
    profiles = listLlmProfiles(databasePath);
    expect(profiles.find((profile) => profile.profileId === firstProfileId)?.isActive).toBe(true);
    expect(profiles.find((profile) => profile.profileId === secondProfileId)?.isActive).toBe(false);
  });

  it("rejects invalid endpoints and environment-variable names", () => {
    const databasePath = temporarySettingsPath();

    expect(() =>
      saveAndActivateLlmProfile(
        {
          name: "Invalid",
          provider: "Provider",
          baseUrl: "file:///tmp/model",
          model: "model",
          apiKeyEnvironmentVariable: "NOT VALID",
        },
        databasePath,
      ),
    ).toThrow(LlmSettingsValidationError);
    expect(listLlmProfiles(databasePath)).toEqual([]);
  });
});
