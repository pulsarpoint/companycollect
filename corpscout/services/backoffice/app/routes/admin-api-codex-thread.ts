import {
  deleteCodexThread,
  getCodexThreadHistory,
} from "~/lib/codex-agent.server";
import type { Route } from "./+types/admin-api-codex-thread";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export async function loader({ params }: Route.LoaderArgs) {
  const history = getCodexThreadHistory(params.threadId);
  if (!history) return json({ error: "Thread not found." }, 404);
  return json(history);
}

export async function action({ request, params }: Route.ActionArgs) {
  if (request.method !== "DELETE") {
    return json({ error: "Method not allowed." }, 405);
  }
  deleteCodexThread(params.threadId);
  return json({ ok: true });
}
