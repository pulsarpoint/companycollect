import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { SETTINGS_DATABASE_PATH } from "~/lib/llm-settings.server";
import defaultPrompt from "~/lib/domain-default-prompt.json";

export interface DomainPrompt {
  promptId: string;
  name: string;
  systemPrompt: string;
  revision: number;
  updatedAt: string;
}

export class DomainPromptValidationError extends Error {}

function connect(databasePath: string): DatabaseSync {
  const path = resolve(databasePath);
  mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL");
  db.exec(`CREATE TABLE IF NOT EXISTS domain_prompt (
    prompt_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (trim(name) != ''),
    system_prompt TEXT NOT NULL CHECK (trim(system_prompt) != ''),
    revision INTEGER NOT NULL CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  const now = new Date().toISOString();
  db.prepare(`INSERT OR IGNORE INTO domain_prompt VALUES (?, ?, ?, 1, ?, ?)`).run(
    "swedish-domain-default", defaultPrompt.name, defaultPrompt.systemPrompt, now, now,
  );
  return db;
}

const COLUMNS = `prompt_id AS promptId, name, system_prompt AS systemPrompt,
  revision, updated_at AS updatedAt`;

export function listDomainPrompts(databasePath = SETTINGS_DATABASE_PATH): DomainPrompt[] {
  const db = connect(databasePath);
  try {
    return db.prepare(`SELECT ${COLUMNS} FROM domain_prompt ORDER BY name COLLATE NOCASE`).all() as unknown as DomainPrompt[];
  } finally { db.close(); }
}

export function getDomainPrompt(promptId: string, databasePath = SETTINGS_DATABASE_PATH): DomainPrompt | null {
  const db = connect(databasePath);
  try {
    return (db.prepare(`SELECT ${COLUMNS} FROM domain_prompt WHERE prompt_id = ?`).get(promptId) as unknown as DomainPrompt | undefined) ?? null;
  } finally { db.close(); }
}

export function saveDomainPrompt(
  input: { promptId?: string; name: string; systemPrompt: string; revision?: number },
  databasePath = SETTINGS_DATABASE_PATH,
): string {
  const name = input.name.trim();
  const systemPrompt = input.systemPrompt.trim();
  if (!name || name.length > 120) throw new DomainPromptValidationError("Name must contain 1–120 characters.");
  if (!systemPrompt || systemPrompt.length > 30_000) throw new DomainPromptValidationError("Prompt must contain 1–30,000 characters.");
  const promptId = input.promptId || randomUUID();
  const db = connect(databasePath);
  const now = new Date().toISOString();
  try {
    if (input.promptId) {
      const result = db.prepare(`UPDATE domain_prompt SET name = ?, system_prompt = ?,
        revision = revision + 1, updated_at = ? WHERE prompt_id = ? AND revision = ?`)
        .run(name, systemPrompt, now, promptId, input.revision ?? 0);
      if (result.changes !== 1) throw new DomainPromptValidationError("This prompt changed or no longer exists. Reload before saving.");
    } else {
      db.prepare("INSERT INTO domain_prompt VALUES (?, ?, ?, 1, ?, ?)").run(promptId, name, systemPrompt, now, now);
    }
    return promptId;
  } catch (error) {
    if (error instanceof Error && error.message.includes("UNIQUE constraint failed: domain_prompt.name")) {
      throw new DomainPromptValidationError("A Domain prompt with this name already exists.");
    }
    throw error;
  } finally { db.close(); }
}
