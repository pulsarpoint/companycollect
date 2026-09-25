import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getDomainPrompt, listDomainPrompts, saveDomainPrompt } from "~/lib/domain-prompts.server";

let directory: string;
let databasePath: string;
beforeEach(() => { vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", "ab".repeat(32)); directory = mkdtempSync(join(tmpdir(), "domain-prompts-")); databasePath = join(directory, "settings.sqlite"); });
afterEach(() => { vi.unstubAllEnvs(); rmSync(directory, { recursive: true, force: true }); });

describe("Domain prompts in the settings database", () => {
  it("seeds the domain verification prompt once and preserves edits", () => {
    const [seed] = listDomainPrompts(databasePath);
    expect(seed.systemPrompt).toContain('company');
    saveDomainPrompt({ ...seed, systemPrompt: "Edited prompt" }, databasePath);
    expect(listDomainPrompts(databasePath)).toEqual([expect.objectContaining({ revision: 2, systemPrompt: "Edited prompt" })]);
    expect(getDomainPrompt("missing", databasePath)).toBeNull();
  });
  it("persists multiple named prompts without changing existing prompts", () => {
    const id = saveDomainPrompt({ name: " Careful matching ", systemPrompt: "Return pairs conservatively." }, databasePath);
    expect(getDomainPrompt(id, databasePath)).toMatchObject({ name: "Careful matching", revision: 1 });
    expect(listDomainPrompts(databasePath)).toHaveLength(2);
  });
  it("refuses stale edits without overwriting the saved revision", () => {
    const [seed] = listDomainPrompts(databasePath);
    saveDomainPrompt({ ...seed, systemPrompt: "New revision" }, databasePath);
    expect(() => saveDomainPrompt({ ...seed, systemPrompt: "Stale revision" }, databasePath)).toThrow("Reload");
    expect(getDomainPrompt(seed.promptId, databasePath)?.systemPrompt).toBe("New revision");
  });
  it("refuses duplicate names and empty or oversized instructions", () => {
    const [seed] = listDomainPrompts(databasePath);
    expect(() => saveDomainPrompt({ name: seed.name, systemPrompt: "duplicate" }, databasePath)).toThrow("already exists");
    for (const systemPrompt of [" ", "x".repeat(30_001)]) {
      expect(() => saveDomainPrompt({ name: "Invalid", systemPrompt }, databasePath)).toThrow("Prompt must");
    }
    expect(listDomainPrompts(databasePath)).toHaveLength(1);
  });
});
