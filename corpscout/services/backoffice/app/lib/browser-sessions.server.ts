import { browserFetch, browserWebsocketUrl } from "~/lib/browser-service.server";
import type { SavedBrowserSession, BrowserInspection } from "~/lib/browser-sessions";

export async function loadBrowserSessions(): Promise<{ sessions: SavedBrowserSession[] }> {
  return (await browserFetch("/v1/browser-sessions")).json();
}

export async function browserSessionAction(sessionId: string, intent: string, values: { url?: string; tabId?: string; pinned?: boolean; headless?: boolean; executionId?: string }) {
  const base = `/v1/browser-sessions/${encodeURIComponent(sessionId)}`;
  const path = intent === "open-tab" ? `${base}/tabs`
    : intent === "focus" || intent === "inspect" ? `${base}/tabs/${encodeURIComponent(values.tabId || "")}/${intent}`
    : `${base}/${intent}`;
  const response = await browserFetch(path, {
    method: intent === "inspect" ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
    body: intent === "open-tab" ? JSON.stringify({ url: values.url })
      : intent === "settings" ? JSON.stringify({ pinned: values.pinned })
      : intent === "start" || intent === "stop" ? JSON.stringify({ headless: values.headless, executionId: values.executionId }) : undefined,
    signal: AbortSignal.timeout(90_000),
  });
  if (intent === "browser-ticket") {
    const ticket = await response.json() as { websocket_path: string };
    return { browserUrl: browserWebsocketUrl(ticket.websocket_path) };
  }
  if (intent === "inspect") return { inspection: await response.json() as BrowserInspection };
  return {};
}
