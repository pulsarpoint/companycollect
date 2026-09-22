/** Authenticated transport; callers inspecting raw API errors keep the Response. */
export async function browserRequest(path: string, init: RequestInit = {}) {
  const base = process.env.BROWSER_API_URL;
  if (!base) throw new Error("Configure BROWSER_API_URL on Backoffice.");
  const headers = new Headers(init.headers);
  const token = process.env.BROWSER_API_TOKEN;
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(new URL(path, base), {
    ...init, headers, signal: init.signal ?? AbortSignal.timeout(10_000),
  });
}

/** Server-side access for management operations that require a successful response. */
export async function browserFetch(path: string, init: RequestInit = {}) {
  const response = await browserRequest(path, init);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(typeof body?.detail === "string" ? body.detail : typeof body?.detail?.message === "string" ? body.detail.message : `Browser service returned HTTP ${response.status}.`);
  }
  return response;
}

export function browserWebsocketUrl(path: string) {
  const base = process.env.BROWSER_PUBLIC_URL || process.env.BROWSER_API_URL;
  if (!base) throw new Error("Configure BROWSER_API_URL on Backoffice.");
  const url = new URL(path, base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}
