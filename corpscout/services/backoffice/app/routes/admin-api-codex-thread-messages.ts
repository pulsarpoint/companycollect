import {
  CodexAgentError,
  getCodexThreadHistory,
  runCodexTurn,
} from "~/lib/codex-agent.server";
import type { Route } from "./+types/admin-api-codex-thread-messages";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export async function action({ request, params }: Route.ActionArgs) {
  if (request.method !== "POST") {
    return json({ error: "Method not allowed." }, 405);
  }
  const history = getCodexThreadHistory(params.threadId);
  if (!history) return json({ error: "Thread not found." }, 404);
  const body = (await request.json().catch(() => null)) as {
    input?: string;
  } | null;
  const input = body?.input?.trim() ?? "";
  if (input === "") return json({ error: "An input is required." }, 400);
  try {
    return json(
      await runCodexTurn({
        page: history.thread.page,
        input,
        threadId: params.threadId,
      }),
    );
  } catch (error) {
    if (error instanceof CodexAgentError) {
      return json({ error: error.message }, 400);
    }
    throw error;
  }
}
