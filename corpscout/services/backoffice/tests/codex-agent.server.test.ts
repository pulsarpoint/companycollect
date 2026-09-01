import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sdk = vi.hoisted(() => {
  const state = {
    nextThreadId: "thread-1",
    finalResponse: "agent says hi",
    runDelayMs: 0,
    resumedIds: [] as string[],
    startOptions: [] as unknown[],
    runs: [] as string[],
  };

  class FakeThread {
    id: string | null = null;
    private readonly assignedId: string;
    constructor(assignedId: string) {
      this.assignedId = assignedId;
    }
    async run(input: string) {
      state.runs.push(input);
      if (state.runDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, state.runDelayMs));
      }
      this.id = this.assignedId;
      return { items: [], finalResponse: state.finalResponse, usage: null };
    }
  }

  class FakeCodex {
    startThread(options: unknown) {
      state.startOptions.push(options);
      return new FakeThread(state.nextThreadId);
    }
    resumeThread(id: string, options: unknown) {
      state.resumedIds.push(id);
      state.startOptions.push(options);
      const thread = new FakeThread(id);
      thread.id = id;
      return thread;
    }
  }

  return { state, FakeCodex };
});

vi.mock("@openai/codex-sdk", () => ({ Codex: sdk.FakeCodex }));

import {
  CodexAgentError,
  deleteCodexThread,
  getCodexThreadHistory,
  listCodexThreads,
  runCodexTurn,
} from "~/lib/codex-agent.server";

const temporaryDirectories: string[] = [];

function temporaryDatabasePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "backoffice-codex-"));
  temporaryDirectories.push(directory);
  return join(directory, "settings.sqlite");
}

beforeEach(() => {
  sdk.state.nextThreadId = "thread-1";
  sdk.state.finalResponse = "agent says hi";
  sdk.state.runDelayMs = 0;
  sdk.state.resumedIds = [];
  sdk.state.startOptions = [];
  sdk.state.runs = [];
});

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("runCodexTurn", () => {
  it("starts a new sandboxed thread and persists the conversation", async () => {
    const databasePath = temporaryDatabasePath();

    const result = await runCodexTurn(
      { page: "/admin/settings/llms/local", input: "hello codex" },
      databasePath,
    );

    expect(result).toEqual({ threadId: "thread-1", response: "agent says hi" });
    expect(sdk.state.startOptions[0]).toMatchObject({
      sandboxMode: "read-only",
      skipGitRepoCheck: true,
    });
    expect(
      (sdk.state.startOptions[0] as { workingDirectory: string })
        .workingDirectory,
    ).toContain("codex-workspace");

    const threads = listCodexThreads("/admin/settings/llms/local", databasePath);
    expect(threads).toHaveLength(1);
    expect(threads[0]).toMatchObject({
      threadId: "thread-1",
      page: "/admin/settings/llms/local",
      title: "hello codex",
    });

    const history = getCodexThreadHistory("thread-1", databasePath);
    expect(history?.messages.map((m) => [m.role, m.content])).toEqual([
      ["user", "hello codex"],
      ["agent", "agent says hi"],
    ]);
  });

  it("resumes an existing thread and appends to its history", async () => {
    const databasePath = temporaryDatabasePath();
    await runCodexTurn({ page: "/p", input: "first" }, databasePath);

    sdk.state.finalResponse = "second answer";
    const result = await runCodexTurn(
      { page: "/p", input: "second", threadId: "thread-1" },
      databasePath,
    );

    expect(result.response).toBe("second answer");
    expect(sdk.state.resumedIds).toEqual(["thread-1"]);
    const history = getCodexThreadHistory("thread-1", databasePath);
    expect(history?.messages).toHaveLength(4);
    expect(listCodexThreads("/p", databasePath)).toHaveLength(1);
  });

  it("rejects a resume of a thread id that is not in the store", async () => {
    const databasePath = temporaryDatabasePath();

    await expect(
      runCodexTurn(
        { page: "/p", input: "hi", threadId: "ghost" },
        databasePath,
      ),
    ).rejects.toThrow(CodexAgentError);
    expect(sdk.state.resumedIds).toEqual([]);
  });

  it("rejects a concurrent second send to the same thread", async () => {
    const databasePath = temporaryDatabasePath();
    await runCodexTurn({ page: "/p", input: "first" }, databasePath);

    sdk.state.runDelayMs = 50;
    const first = runCodexTurn(
      { page: "/p", input: "slow", threadId: "thread-1" },
      databasePath,
    );
    await expect(
      runCodexTurn(
        { page: "/p", input: "eager", threadId: "thread-1" },
        databasePath,
      ),
    ).rejects.toThrow(/already running/i);
    await first;
  });

  it("truncates long first prompts into the thread title", async () => {
    const databasePath = temporaryDatabasePath();
    const longInput = "x".repeat(500);

    await runCodexTurn({ page: "/p", input: longInput }, databasePath);

    const [thread] = listCodexThreads("/p", databasePath);
    expect(thread.title).toHaveLength(120);
  });
});

describe("thread listing and removal", () => {
  it("lists only the requested page, newest-updated first", async () => {
    const databasePath = temporaryDatabasePath();
    await runCodexTurn({ page: "/a", input: "one" }, databasePath);
    sdk.state.nextThreadId = "thread-2";
    await runCodexTurn({ page: "/b", input: "two" }, databasePath);
    sdk.state.nextThreadId = "thread-3";
    await runCodexTurn({ page: "/a", input: "three" }, databasePath);

    const pageA = listCodexThreads("/a", databasePath);
    expect(pageA.map((thread) => thread.threadId)).toEqual([
      "thread-3",
      "thread-1",
    ]);
  });

  it("deletes a thread together with its messages", async () => {
    const databasePath = temporaryDatabasePath();
    await runCodexTurn({ page: "/p", input: "one" }, databasePath);

    deleteCodexThread("thread-1", databasePath);

    expect(listCodexThreads("/p", databasePath)).toEqual([]);
    expect(getCodexThreadHistory("thread-1", databasePath)).toBeNull();
  });
});
