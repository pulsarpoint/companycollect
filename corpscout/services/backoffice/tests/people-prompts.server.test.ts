import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getPeoplePrompt, listPeoplePrompts, savePeoplePrompt } from "~/lib/people-prompts.server";
import { listLlmProfiles, saveAndActivateLlmProfile } from "~/lib/llm-settings.server";

let directory: string;
let databasePath: string;
beforeEach(() => { vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "ab".repeat(32)); directory = mkdtempSync(join(tmpdir(), "people-prompts-")); databasePath = join(directory, "settings.sqlite"); });
afterEach(() => { vi.unstubAllEnvs(); rmSync(directory, { recursive: true, force: true }); });

describe("People prompts in the settings database", () => {
  it("seeds the current matcher prompt once and preserves edits", () => {
    const [seed] = listPeoplePrompts(databasePath);
    expect(seed.systemPrompt).toContain('"pairs"');
    savePeoplePrompt({ ...seed, systemPrompt: "Edited prompt" }, databasePath);
    expect(listPeoplePrompts(databasePath)).toEqual([expect.objectContaining({ revision: 2, systemPrompt: "Edited prompt" })]);
    expect(getPeoplePrompt("missing", databasePath)).toBeNull();
  });
  it("persists multiple named prompts alongside existing LLM settings", () => {
    saveAndActivateLlmProfile({ name: "LLM", provider: "provider", model: "model", baseUrl: "https://example.com", apiKey: "test-key" }, databasePath);
    const id = savePeoplePrompt({ name: " Careful matching ", systemPrompt: "Return pairs conservatively." }, databasePath);
    expect(getPeoplePrompt(id, databasePath)).toMatchObject({ name: "Careful matching", revision: 1 });
    expect(listPeoplePrompts(databasePath)).toHaveLength(2);
    expect(listLlmProfiles(databasePath)).toHaveLength(1);
  });
  it("refuses stale edits without overwriting the saved revision", () => {
    const [seed] = listPeoplePrompts(databasePath);
    savePeoplePrompt({ ...seed, systemPrompt: "New revision" }, databasePath);
    expect(() => savePeoplePrompt({ ...seed, systemPrompt: "Stale revision" }, databasePath)).toThrow("Reload");
    expect(getPeoplePrompt(seed.promptId, databasePath)?.systemPrompt).toBe("New revision");
  });
  it("refuses duplicate names and empty or oversized instructions", () => {
    const [seed] = listPeoplePrompts(databasePath);
    expect(() => savePeoplePrompt({ name: seed.name, systemPrompt: "duplicate" }, databasePath)).toThrow("already exists");
    for (const systemPrompt of [" ", "x".repeat(30_001)]) {
      expect(() => savePeoplePrompt({ name: "Invalid", systemPrompt }, databasePath)).toThrow("Prompt must");
    }
    expect(listPeoplePrompts(databasePath)).toHaveLength(1);
  });
});
