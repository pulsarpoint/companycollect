import { beforeEach, describe, expect, it, vi } from "vitest";

const service = vi.hoisted(() => ({
  runCodexTurn: vi.fn(),
  listCodexThreads: vi.fn(),
  getCodexThreadHistory: vi.fn(),
  deleteCodexThread: vi.fn(),
}));

vi.mock("~/lib/codex-agent.server", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  ...service,
}));

const threadsRoute = await import("~/routes/admin-api-codex-threads");
const threadRoute = await import("~/routes/admin-api-codex-thread");
const messagesRoute = await import("~/routes/admin-api-codex-thread-messages");

function jsonRequest(url: string, method: string, body?: unknown): Request {
  return new Request(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

beforeEach(() => {
  service.runCodexTurn.mockReset();
  service.listCodexThreads.mockReset();
  service.getCodexThreadHistory.mockReset();
  service.deleteCodexThread.mockReset();
});

describe("GET /admin/api/codex/threads", () => {
  it("requires a page parameter", async () => {
    const response = await threadsRoute.loader({
      request: new Request("http://backoffice/admin/api/codex/threads"),
    } as never);
    expect(response.status).toBe(400);
  });

  it("returns the threads for a page", async () => {
    service.listCodexThreads.mockReturnValue([{ threadId: "t1" }]);
    const response = await threadsRoute.loader({
      request: new Request(
        "http://backoffice/admin/api/codex/threads?page=%2Fdemo",
      ),
    } as never);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ threads: [{ threadId: "t1" }] });
    expect(service.listCodexThreads).toHaveBeenCalledWith("/demo");
  });
});

describe("POST /admin/api/codex/threads", () => {
  it("starts a new thread with the first turn", async () => {
    service.runCodexTurn.mockResolvedValue({
      threadId: "t1",
      response: "hi",
    });
    const response = await threadsRoute.action({
      request: jsonRequest("http://backoffice/admin/api/codex/threads", "POST", {
        page: "/demo",
        input: "hello",
      }),
    } as never);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ threadId: "t1", response: "hi" });
    expect(service.runCodexTurn).toHaveBeenCalledWith({
      page: "/demo",
      input: "hello",
    });
  });

  it("rejects a missing input with 400", async () => {
    const response = await threadsRoute.action({
      request: jsonRequest("http://backoffice/admin/api/codex/threads", "POST", {
        page: "/demo",
      }),
    } as never);
    expect(response.status).toBe(400);
    expect(service.runCodexTurn).not.toHaveBeenCalled();
  });
});

describe("GET/DELETE /admin/api/codex/threads/:threadId", () => {
  it("returns history or 404", async () => {
    service.getCodexThreadHistory.mockReturnValue(null);
    const missing = await threadRoute.loader({
      request: new Request("http://backoffice/admin/api/codex/threads/ghost"),
      params: { threadId: "ghost" },
    } as never);
    expect(missing.status).toBe(404);

    service.getCodexThreadHistory.mockReturnValue({
      thread: { threadId: "t1" },
      messages: [],
    });
    const found = await threadRoute.loader({
      request: new Request("http://backoffice/admin/api/codex/threads/t1"),
      params: { threadId: "t1" },
    } as never);
    expect(await found.json()).toEqual({
      thread: { threadId: "t1" },
      messages: [],
    });
  });

  it("deletes a thread", async () => {
    const response = await threadRoute.action({
      request: new Request("http://backoffice/admin/api/codex/threads/t1", {
        method: "DELETE",
      }),
      params: { threadId: "t1" },
    } as never);
    expect(await response.json()).toEqual({ ok: true });
    expect(service.deleteCodexThread).toHaveBeenCalledWith("t1");
  });
});

describe("POST /admin/api/codex/threads/:threadId/messages", () => {
  it("sends a message to an existing thread", async () => {
    service.getCodexThreadHistory.mockReturnValue({
      thread: { threadId: "t1", page: "/demo" },
      messages: [],
    });
    service.runCodexTurn.mockResolvedValue({
      threadId: "t1",
      response: "again",
    });
    const response = await messagesRoute.action({
      request: jsonRequest(
        "http://backoffice/admin/api/codex/threads/t1/messages",
        "POST",
        { input: "more" },
      ),
      params: { threadId: "t1" },
    } as never);
    expect(await response.json()).toEqual({ threadId: "t1", response: "again" });
    expect(service.runCodexTurn).toHaveBeenCalledWith({
      page: "/demo",
      input: "more",
      threadId: "t1",
    });
  });

  it("maps CodexAgentError to a 400 with the message", async () => {
    const { CodexAgentError } = await import("~/lib/codex-agent.server");
    service.getCodexThreadHistory.mockReturnValue({
      thread: { threadId: "t1", page: "/demo" },
      messages: [],
    });
    service.runCodexTurn.mockRejectedValue(
      new CodexAgentError("already running"),
    );
    const response = await messagesRoute.action({
      request: jsonRequest(
        "http://backoffice/admin/api/codex/threads/t1/messages",
        "POST",
        { input: "more" },
      ),
      params: { threadId: "t1" },
    } as never);
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: "already running" });
  });
});
