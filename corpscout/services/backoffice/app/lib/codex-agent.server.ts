import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { Codex, type Thread } from "@openai/codex-sdk";

import { SETTINGS_DATABASE_PATH } from "~/lib/llm-settings.server";

/**
 * Codex threads triggered from inside the backoffice. The SDK drives the
 * locally installed, ChatGPT-authenticated `codex` CLI; every thread runs
 * read-only in a dedicated scratch workspace so a web-page-triggered agent
 * can never modify the repo or the machine. History lives in our SQLite
 * mirror because the SDK resumes by id but exposes no history read-back.
 */

const CODEX_WORKSPACE_DIR =
  process.env.BACKOFFICE_CODEX_WORKSPACE_DIR?.trim() ||
  join(process.cwd(), "data", "codex-workspace");

const THREAD_TITLE_MAX_LENGTH = 120;

export class CodexAgentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CodexAgentError";
  }
}

export interface CodexThreadSummary {
  threadId: string;
  page: string;
  title: string;
  createdAt: string;
  updatedAt: string;
}

export interface CodexMessage {
  messageId: string;
  threadId: string;
  role: "user" | "agent";
  content: string;
  createdAt: string;
}

interface StoredThread {
  thread_id: string;
  page: string;
  title: string;
  created_at: string;
  updated_at: string;
}

interface StoredMessage {
  message_id: string;
  thread_id: string;
  role: "user" | "agent";
  content: string;
  created_at: string;
}

function connectCodexDatabase(databasePath: string): DatabaseSync {
  const absolutePath = resolve(databasePath);
  mkdirSync(dirname(absolutePath), { recursive: true });
  const database = new DatabaseSync(absolutePath);
  database.exec("PRAGMA busy_timeout = 5000");
  database.exec("PRAGMA journal_mode = WAL");
  database.exec("PRAGMA foreign_keys = ON");
  database.exec(`
    CREATE TABLE IF NOT EXISTS codex_thread (
      thread_id  TEXT PRIMARY KEY,
      page       TEXT NOT NULL CHECK (trim(page) != ''),
      title      TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS codex_message (
      message_id TEXT PRIMARY KEY,
      thread_id  TEXT NOT NULL REFERENCES codex_thread(thread_id)
        ON DELETE CASCADE,
      role       TEXT NOT NULL CHECK (role IN ('user', 'agent')),
      content    TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);
  return database;
}

function mapThread(row: StoredThread): CodexThreadSummary {
  return {
    threadId: row.thread_id,
    page: row.page,
    title: row.title,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapMessage(row: StoredMessage): CodexMessage {
  return {
    messageId: row.message_id,
    threadId: row.thread_id,
    role: row.role,
    content: row.content,
    createdAt: row.created_at,
  };
}

function threadOptions() {
  mkdirSync(CODEX_WORKSPACE_DIR, { recursive: true });
  return {
    workingDirectory: CODEX_WORKSPACE_DIR,
    sandboxMode: "read-only" as const,
    skipGitRepoCheck: true,
  };
}

// One in-flight turn per thread (and one pending new-thread turn per page):
// codex turns take a while, and a double-send would interleave the same
// conversation. Module-level because the dev server is a single process.
const inFlight = new Set<string>();

function acquireTurnSlot(key: string): void {
  if (inFlight.has(key)) {
    throw new CodexAgentError(
      "A codex turn is already running for this conversation. Wait for it to finish.",
    );
  }
  inFlight.add(key);
}

export async function runCodexTurn(
  request: { page: string; input: string; threadId?: string },
  databasePath = SETTINGS_DATABASE_PATH,
): Promise<{ threadId: string; response: string }> {
  const page = request.page.trim();
  const input = request.input.trim();
  if (page === "") throw new CodexAgentError("A page is required.");
  if (input === "") throw new CodexAgentError("A message is required.");
  const requestedThreadId = request.threadId?.trim() || null;

  if (requestedThreadId) {
    const existing = getCodexThreadHistory(requestedThreadId, databasePath);
    if (!existing) {
      throw new CodexAgentError(
        "That codex thread is not in the local store. Start a new conversation instead.",
      );
    }
  }

  const slotKey = requestedThreadId ?? `new:${page}`;
  acquireTurnSlot(slotKey);
  try {
    const codex = new Codex();
    const thread: Thread = requestedThreadId
      ? codex.resumeThread(requestedThreadId, threadOptions())
      : codex.startThread(threadOptions());

    let turn: Awaited<ReturnType<Thread["run"]>>;
    try {
      turn = await thread.run(input);
    } catch (error) {
      throw new CodexAgentError(
        `The codex CLI could not complete the turn: ${
          error instanceof Error ? error.message : String(error)
        }. Is codex installed and signed in on this machine?`,
      );
    }

    const threadId = thread.id ?? requestedThreadId;
    if (!threadId) {
      throw new CodexAgentError(
        "The codex CLI did not report a thread id for this conversation.",
      );
    }

    const now = new Date().toISOString();
    const database = connectCodexDatabase(databasePath);
    try {
      database.exec("BEGIN IMMEDIATE");
      database
        .prepare(
          `INSERT INTO codex_thread (thread_id, page, title, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (thread_id) DO UPDATE SET updated_at = excluded.updated_at`,
        )
        .run(
          threadId,
          page,
          input.slice(0, THREAD_TITLE_MAX_LENGTH),
          now,
          now,
        );
      const insertMessage = database.prepare(
        `INSERT INTO codex_message (message_id, thread_id, role, content, created_at)
         VALUES (?, ?, ?, ?, ?)`,
      );
      insertMessage.run(randomUUID(), threadId, "user", input, now);
      insertMessage.run(
        randomUUID(),
        threadId,
        "agent",
        turn.finalResponse,
        now,
      );
      database.exec("COMMIT");
    } catch (error) {
      if (database.isTransaction) database.exec("ROLLBACK");
      throw error;
    } finally {
      database.close();
    }

    return { threadId, response: turn.finalResponse };
  } finally {
    inFlight.delete(slotKey);
  }
}

export function listCodexThreads(
  page: string,
  databasePath = SETTINGS_DATABASE_PATH,
): CodexThreadSummary[] {
  const database = connectCodexDatabase(databasePath);
  try {
    const rows = database
      .prepare(
        `SELECT * FROM codex_thread
         WHERE page = ?
         ORDER BY updated_at DESC, rowid DESC`,
      )
      .all(page) as unknown as StoredThread[];
    return rows.map(mapThread);
  } finally {
    database.close();
  }
}

export function getCodexThreadHistory(
  threadId: string,
  databasePath = SETTINGS_DATABASE_PATH,
): { thread: CodexThreadSummary; messages: CodexMessage[] } | null {
  const database = connectCodexDatabase(databasePath);
  try {
    const thread = database
      .prepare("SELECT * FROM codex_thread WHERE thread_id = ?")
      .get(threadId) as unknown as StoredThread | undefined;
    if (!thread) return null;
    const messages = database
      .prepare(
        `SELECT * FROM codex_message
         WHERE thread_id = ?
         ORDER BY created_at, rowid`,
      )
      .all(threadId) as unknown as StoredMessage[];
    return { thread: mapThread(thread), messages: messages.map(mapMessage) };
  } finally {
    database.close();
  }
}

export function deleteCodexThread(
  threadId: string,
  databasePath = SETTINGS_DATABASE_PATH,
): void {
  const database = connectCodexDatabase(databasePath);
  try {
    database
      .prepare("DELETE FROM codex_thread WHERE thread_id = ?")
      .run(threadId);
  } finally {
    database.close();
  }
}
