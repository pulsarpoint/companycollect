import { browserFetch, browserWebsocketUrl } from "~/lib/browser-service.server";

export interface BrowserDesktopInfo {
  id: string;
  kind: "saved" | "brave" | "interactive";
  name: string;
  request_id: string | null;
  url: string | null;
  state: string;
  started_at: string;
}

export interface BrowserServer {
  hostname: string;
  version: string;
  healthy: boolean;
  settings?: BrowserRuntimeConfiguration;
  sessions: BrowserDesktopInfo[];
  idle_timeout_seconds: number;
  leases: { id: string; session_id: string; request_id: string; domain: string; profile_id: string | null; state: string; last_request_at: number; expires_at: number; operation?: { kind: string; stage: string; agent_runs: number } | null }[];
}

export async function loadBrowserServer(): Promise<BrowserServer> {
  return (await browserFetch("/v1/server")).json();
}

export async function connectBrowserDesktop(id: string) {
  const response = await browserFetch(`/v1/desktops/${encodeURIComponent(id)}/browser-ticket`, { method: "POST" });
  const ticket = await response.json() as { websocket_path: string };
  return browserWebsocketUrl(ticket.websocket_path);
}

export interface BrowserRuntimeConfiguration {
  source: "sqlite" | "startup";
  startup: { max_browsers: number; idle_timeout_seconds: number; session_retention_days: number };
  current: { max_browsers: number; idle_timeout_seconds: number; session_retention_days: number };
  capacity: { occupied: number; available: number };
}
