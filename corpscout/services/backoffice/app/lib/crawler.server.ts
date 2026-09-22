import { browserWebsocketUrl } from "~/lib/browser-service.server";
import type { CrawlAttempt, CrawlSnapshot } from "~/lib/crawler";

function crawlerUrl(path: string) {
  const base = process.env.CRAWLER_API_URL;
  if (!base) throw new Error("Configure CRAWLER_API_URL on Backoffice.");
  return new URL(path, base);
}

export async function crawlerFetch(path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  const token = process.env.CRAWLER_API_TOKEN;
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(crawlerUrl(path), {
    ...init, headers, redirect: "error", signal: init.signal ?? AbortSignal.timeout(10_000),
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    if (response.status === 422 && Array.isArray(body?.detail)) {
      throw new Error(body.detail.map((issue: {loc?: unknown[]; msg?: string}) =>
        `${issue.loc?.join(".") || "request"}: ${issue.msg || "Invalid value"}`).join("; "));
    }
    throw new Error(typeof body?.detail === "string" ? body.detail : `Crawler returned HTTP ${response.status}.`);
  }
  return response;
}

export async function loadCrawls(search: URLSearchParams): Promise<CrawlSnapshot> {
  const query = new URLSearchParams();
  for (const key of ["domain", "state", "source", "offset"]) {
    const value = search.get(key);
    if (value) query.set(key, value);
  }
  return (await crawlerFetch(`/v1/crawls/status?${query}`)).json();
}

export async function crawlAction(id: string, action: "retry" | "verify-search" | "resume" | "cancel" | "browser-ticket", body?: object) {
  const response = await crawlerFetch(`/v1/crawls/${encodeURIComponent(id)}/${action}`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (action === "browser-ticket") {
    const ticket = await response.json() as {websocket_path: string};
    return {browserUrl: browserWebsocketUrl(ticket.websocket_path)};
  }
  return response.json() as Promise<CrawlAttempt | {state: string}>;
}
