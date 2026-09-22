export type BrowserTestRequest = { method: string; path: string; body: string };

export const browserTestPresets = {
  navigate: "Navigate and get HTML",
  capture: "Capture current page",
  screenshot: "Capture screenshot",
  status: "Session status",
  heartbeat: "Heartbeat",
  release: "Release browser",
  open: "Open named tab",
  focus: "Focus named tab",
  close: "Close named tab",
  recover: "Recover named tab",
};

export type BrowserTestPreset = keyof typeof browserTestPresets;

/** Presets only prepare editable requests; the caller must explicitly send them. */
export function browserTestRequest(preset: BrowserTestPreset, sessionId: string, url: string, tab: string, mode = "headless", executionId = ""): BrowserTestRequest {
  const sessionPath = `/v1/browser/sessions/${sessionId}`;
  let method = "POST";
  let path = "/v1/browser/extract";
  let body: unknown;
  if (preset === "status" || preset === "release" || preset === "heartbeat") {
    method = preset === "status" ? "GET" : preset === "release" ? "DELETE" : "POST";
    path = sessionPath + (preset === "heartbeat" ? "/heartbeat" : "");
  } else if (["open", "focus", "close", "recover"].includes(preset)) {
    path = `${sessionPath}/tabs/${tab}`;
    body = preset === "recover" ? { action: preset, url, reopenClosedTab: false } : { action: preset };
  } else {
    body = {
      session: { id: sessionId, ...(executionId ? { executionId } : {}) }, tab,
      headless: mode !== "headed",
      ...(preset === "navigate" ? { url } : {}),
      browserHtml: preset !== "screenshot", screenshot: preset === "screenshot",
      timeoutSeconds: 60, checkRobotsTxt: true,
    };
  }
  return { method, path, body: body === undefined ? "" : JSON.stringify(body, null, 2) };
}

export function parseBrowserTestJson(body: string): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(body);
    return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
  } catch { return null; }
}
