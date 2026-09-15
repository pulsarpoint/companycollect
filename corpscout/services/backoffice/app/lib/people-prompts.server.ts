import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { SETTINGS_DATABASE_PATH } from "~/lib/llm-settings.server";
import defaultPrompt from "~/lib/people-default-prompt.json";

export interface PeoplePrompt {
  promptId: string;
  name: string;
  systemPrompt: string;
  revision: number;
  updatedAt: string;
}

export class PeoplePromptValidationError extends Error {}

function connect(databasePath: string): DatabaseSync {
  const path = resolve(databasePath);
  mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL");
  db.exec(`CREATE TABLE IF NOT EXISTS people_prompt (
    prompt_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (trim(name) != ''),
    system_prompt TEXT NOT NULL CHECK (trim(system_prompt) != ''),
    revision INTEGER NOT NULL CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  const now = new Date().toISOString();
  db.prepare(`INSERT OR IGNORE INTO people_prompt VALUES (?, ?, ?, 1, ?, ?)`).run(
    "swedish-people-default", defaultPrompt.name, defaultPrompt.systemPrompt, now, now,
  );
  return db;
}

const COLUMNS = `prompt_id AS promptId, name, system_prompt AS systemPrompt,
  revision, updated_at AS updatedAt`;

export function listPeoplePrompts(databasePath = SETTINGS_DATABASE_PATH): PeoplePrompt[] {
  const db = connect(databasePath);
  try {
    return db.prepare(`SELECT ${COLUMNS} FROM people_prompt ORDER BY name COLLATE NOCASE`).all() as unknown as PeoplePrompt[];
  } finally { db.close(); }
}

export function getPeoplePrompt(promptId: string, databasePath = SETTINGS_DATABASE_PATH): PeoplePrompt | null {
  const db = connect(databasePath);
  try {
    return (db.prepare(`SELECT ${COLUMNS} FROM people_prompt WHERE prompt_id = ?`).get(promptId) as unknown as PeoplePrompt | undefined) ?? null;
  } finally { db.close(); }
}

export function savePeoplePrompt(
  input: { promptId?: string; name: string; systemPrompt: string; revision?: number },
  databasePath = SETTINGS_DATABASE_PATH,
): string {
  const name = input.name.trim();
  const systemPrompt = input.systemPrompt.trim();
  if (!name || name.length > 120) throw new PeoplePromptValidationError("Name must contain 1–120 characters.");
  if (!systemPrompt || systemPrompt.length > 30_000) throw new PeoplePromptValidationError("Prompt must contain 1–30,000 characters.");
  const promptId = input.promptId || randomUUID();
  const db = connect(databasePath);
  const now = new Date().toISOString();
  try {
    if (input.promptId) {
      const result = db.prepare(`UPDATE people_prompt SET name = ?, system_prompt = ?,
        revision = revision + 1, updated_at = ? WHERE prompt_id = ? AND revision = ?`)
        .run(name, systemPrompt, now, promptId, input.revision ?? 0);
      if (result.changes !== 1) throw new PeoplePromptValidationError("This prompt changed or no longer exists. Reload before saving.");
    } else {
      db.prepare("INSERT INTO people_prompt VALUES (?, ?, ?, 1, ?, ?)").run(promptId, name, systemPrompt, now, now);
    }
    return promptId;
  } catch (error) {
    if (error instanceof Error && error.message.includes("UNIQUE constraint failed: people_prompt.name")) {
      throw new PeoplePromptValidationError("A People prompt with this name already exists.");
    }
    throw error;
  } finally { db.close(); }
}
