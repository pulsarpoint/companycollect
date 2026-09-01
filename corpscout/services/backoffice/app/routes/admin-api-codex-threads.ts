import {
  CodexAgentError,
  listCodexThreads,
  runCodexTurn,
} from "~/lib/codex-agent.server";
import type { Route } from "./+types/admin-api-codex-threads";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export async function loader({ request }: Route.LoaderArgs) {
  const page = new URL(request.url).searchParams.get("page")?.trim() ?? "";
  if (page === "") {
    return json({ error: "A page query parameter is required." }, 400);
  }
  return json({ threads: listCodexThreads(page) });
}

export async function action({ request }: Route.ActionArgs) {
  if (request.method !== "POST") {
    return json({ error: "Method not allowed." }, 405);
  }
  const body = (await request.json().catch(() => null)) as {
    page?: string;
    input?: string;
  } | null;
  const page = body?.page?.trim() ?? "";
  const input = body?.input?.trim() ?? "";
  if (page === "" || input === "") {
    return json({ error: "Both page and input are required." }, 400);
  }
  try {
    return json(await runCodexTurn({ page, input }));
  } catch (error) {
    if (error instanceof CodexAgentError) {
      return json({ error: error.message }, 400);
    }
    throw error;
  }
}
