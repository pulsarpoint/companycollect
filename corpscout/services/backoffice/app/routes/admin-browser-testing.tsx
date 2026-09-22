import { useEffect, useMemo, useRef, useState } from "react";
import { useFetcher } from "react-router";
import { CopyIcon, MonitorIcon, SendIcon } from "lucide-react";
import type { Route } from "./+types/admin-browser-testing";
import { browserRequest, browserWebsocketUrl } from "~/lib/browser-service.server";
import { browserTestPresets, browserTestRequest, parseBrowserTestJson, type BrowserTestPreset, type BrowserTestRequest } from "~/lib/browser-testing";
import { BrowserDesktop } from "~/components/admin/browser-desktop";
import { BrowserTabs } from "~/components/admin/browser-tabs";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { Textarea } from "~/components/ui/textarea";

type TestResponse = {
  request: BrowserTestRequest;
  status: number;
  statusText: string;
  contentType: string;
  body: string;
  durationMs: number;
};
type ActionResult = { error: string | null; response?: TestResponse; browserUrl?: string };


export async function action({ request }: Route.ActionArgs): Promise<ActionResult> {
  if (request.headers.get("Origin") !== new URL(request.url).origin) return { error: "Cross-origin browser requests are not allowed." };
  if (!process.env.BROWSER_API_URL) return { error: "Configure BROWSER_API_URL on Backoffice." };
  const form = await request.formData();
  const intent = String(form.get("intent") || "");
  const sessionId = String(form.get("session_id") || "");
  const method = String(form.get("method") || "");
  const path = String(form.get("path") || "");
  const body = String(form.get("body") || "");
  if (intent === "connect") {
    if (!/^[a-f0-9]{32}$/.test(sessionId)) return { error: "Enter a valid session ID to connect." };
  } else if (intent === "send") {
    // Limit forwarding to the browser data API. Never accept another host, a
    // management endpoint, encoded traversal, or redirects with our credential.
    if (!/^(GET|POST|DELETE)$/.test(method)
      || !/^\/v1\/browser\/(extract|sessions(?:\/[A-Za-z0-9_-]{1,128}(?:\/(heartbeat|tabs\/[A-Za-z0-9_-]{1,64}))?)?)$/.test(path)) {
      return { error: "Use a browser extract, session, heartbeat, or named-tab endpoint with GET, POST, or DELETE." };
    }
    if (new TextEncoder().encode(body).byteLength > 131_072) return { error: "The request body must be 128 KiB or smaller." };
    if (method !== "POST" && body !== "") return { error: "Clear the body before sending GET or DELETE." };
  } else return { error: "Unknown testing action." };

  try {
    const started = performance.now();
    const response = await browserRequest(intent === "connect" ? `/v1/browser/sessions/${sessionId}/browser-ticket` : path, {
      method: intent === "connect" ? "POST" : method,
      headers: { "Content-Type": "application/json", ...(form.get("execution_id") ? { "X-Browser-Execution-Id": String(form.get("execution_id")) } : {}) },
      body: intent === "send" && method === "POST" ? body : undefined,
      redirect: "error",
      // Browser operations are bounded at 310 seconds by the service. Preserve
      // its response instead of cutting them off with the management timeout.
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(330_000)]),
    });
    const text = await response.text();
    if (intent === "connect") {
      const payload = parseBrowserTestJson(text);
      if (!response.ok || typeof payload?.websocket_path !== "string") {
        return { error: typeof payload?.detail === "string" ? payload.detail : `Desktop connection returned HTTP ${response.status}.` };
      }
      return { error: null, browserUrl: browserWebsocketUrl(payload.websocket_path) };
    }
    return { error: null, response: {
      request: { method, path, body }, status: response.status, statusText: response.statusText,
      contentType: response.headers.get("Content-Type") || "", body: text,
      durationMs: Math.round(performance.now() - started),
    } };
  } catch {
    return { error: "No response received from the browser service. Check its connection and session status before retrying; the request may already have run." };
  }
}

export function meta() { return [{ title: "Browser testing | CompanyCollect admin" }]; }

export default function AdminBrowserTesting() {
  const fetcher = useFetcher<typeof action>();
  const desktop = useFetcher<typeof action>();
  const [sessionId, setSessionId] = useState("");
  const [url, setUrl] = useState("https://example.com/");
  const [tab, setTab] = useState("site");
  const [browserMode, setBrowserMode] = useState("headless");
  const [assignment, setAssignment] = useState<{ id: string; profileId: string; headless: boolean; executionId: string } | null>(null);
  const [preset, setPreset] = useState<BrowserTestPreset>("navigate");
  const [draft, setDraft] = useState<BrowserTestRequest>({ method: "POST", path: "/v1/browser/extract", body: "" });
  const [history, setHistory] = useState<TestResponse[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [responseView, setResponseView] = useState("raw");
  const [browserUrl, setBrowserUrl] = useState<string | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [copyStatus, setCopyStatus] = useState("");
  const handledResponse = useRef<unknown>(null);
  const handledDesktop = useRef<unknown>(null);
  const busy = fetcher.state !== "idle" || desktop.state !== "idle";
  const pinnedBrowser = assignment?.id === sessionId ? assignment.profileId : null;
  const selected = history[selectedIndex];
  const payload = useMemo(() => selected ? parseBrowserTestJson(selected.body) : null, [selected]);
  const formattedResponse = useMemo(() => {
    if (!selected) return "";
    try { return JSON.stringify(JSON.parse(selected.body), null, 2); } catch { return selected.body; }
  }, [selected]);
  const responseSession = payload?.session !== null && typeof payload?.session === "object"
    ? payload.session as Record<string, unknown> : payload;

  function loadPreset(value: BrowserTestPreset, id = sessionId) {
    setPreset(value);
    setDraft(browserTestRequest(value, id, url, tab, browserMode, assignment?.id === id ? assignment.executionId : ""));
    setLocalError(null);
  }
  function newSession() {
    const id = crypto.randomUUID().replaceAll("-", "");
    setSessionId(id);
    loadPreset("navigate", id);
  }

  function changeInput(key: "sessionId" | "url" | "tab" | "browserMode", value: string) {
    if (key === "sessionId") setSessionId(value);
    if (key === "url") setUrl(value);
    if (key === "tab") setTab(value);
    if (key === "browserMode") setBrowserMode(value);
    let path = draft.path;
    if (key === "sessionId") path = path.replace(/^\/v1\/browser\/sessions\/[^/]+/, `/v1/browser/sessions/${value}`);
    if (key === "tab") path = path.replace(/\/tabs\/[^/]+$/, `/tabs/${value}`);
    if (!draft.body) { setDraft({ ...draft, path }); return; }
    const body = parseBrowserTestJson(draft.body);
    if (!body) {
      setLocalError("The raw JSON is invalid. Load a preset to apply the updated inputs, or edit the body directly.");
      setDraft({ ...draft, path });
      return;
    }
    // Update only context fields so custom options in the raw JSON survive.
    if (key === "sessionId" && body.session && typeof body.session === "object") body.session = { id: value };
    if (key === "url" && "url" in body) body.url = value;
    if (key === "tab" && "tab" in body) body.tab = value;
    if (key === "browserMode" && draft.path === "/v1/browser/extract") {
      body.headless = value !== "headed";
    }
    setDraft({ ...draft, path, body: JSON.stringify(body, null, 2) });
  }
  useEffect(() => { newSession(); }, []);
  useEffect(() => { setBrowserUrl(null); }, [sessionId]);
  useEffect(() => {
    const data = fetcher.data;
    if (!data || data === handledResponse.current) return;
    handledResponse.current = data;
    setLocalError(data.error);
    if (data.response) {
      setHistory(previous => [data.response!, ...previous].slice(0, 10));
      setSelectedIndex(0);
      setResponseView("raw");
      setCopyStatus("");
      const parsed = parseBrowserTestJson(data.response.body);
      const returnedSession = parsed?.session && typeof parsed.session === "object" ? parsed.session as Record<string, unknown> : parsed;
      if (data.response.status < 300 && typeof returnedSession?.id === "string" && typeof returnedSession.profileId === "string" && typeof returnedSession.executionId === "string") {
        setSessionId(returnedSession.id);
        setBrowserMode(returnedSession.headless === true ? "headless" : "headed");
        setAssignment({ id: returnedSession.id, profileId: returnedSession.profileId, headless: returnedSession.headless === true, executionId: String(returnedSession.executionId || "") });
        setDraft(previous => {
          const body = parseBrowserTestJson(previous.body);
          if (body?.session && typeof body.session === "object") {
            body.session = { ...body.session, executionId: returnedSession.executionId };
            body.headless = returnedSession.headless === true;
          }
          return body ? { ...previous, body: JSON.stringify(body, null, 2) } : previous;
        });
      }
      if ((data.response.request.method === "DELETE" && data.response.request.path === `/v1/browser/sessions/${sessionId}` && data.response.status < 300) || data.response.status === 410 || returnedSession?.state === "closed" || returnedSession?.state === "expired") {
        setBrowserUrl(null);
        setAssignment(null);
        setDraft(previous => {
          const body = parseBrowserTestJson(previous.body);
          if (body?.session && typeof body.session === "object") {
            delete (body.session as Record<string, unknown>).executionId;
          }
          return body ? { ...previous, body: JSON.stringify(body, null, 2) } : previous;
        });
      }
    }
  }, [fetcher.data, sessionId]);
  useEffect(() => {
    if (!desktop.data || desktop.data === handledDesktop.current) return;
    handledDesktop.current = desktop.data;
    setLocalError(desktop.data.error);
    if (desktop.data.browserUrl) setBrowserUrl(desktop.data.browserUrl);
  }, [desktop.data]);

  async function copy(text: string) {
    try { await navigator.clipboard.writeText(text); setCopyStatus("Copied."); }
    catch { setCopyStatus("Copy failed. Select and copy the text directly."); }
  }
  function formatBody() {
    try { setDraft({ ...draft, body: JSON.stringify(JSON.parse(draft.body), null, 2) }); setLocalError(null); }
    catch { setLocalError("The body is not valid JSON. You can still send it to test the service’s validation response."); }
  }

  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <div><h1 className="text-2xl font-semibold">Browser testing</h1><p className="mt-1 text-sm text-muted-foreground">Send raw requests to the browser service and inspect what comes back.</p></div>
    <BrowserTabs value="testing" />

    <section className="flex flex-col gap-4" aria-label="Test session">
      <FieldGroup className="grid gap-4 lg:grid-cols-2">
        <Field><FieldLabel htmlFor="test-session-id">Session ID</FieldLabel><div className="flex gap-2">
          <Input id="test-session-id" value={sessionId} onChange={event => changeInput("sessionId", event.target.value)} disabled={busy} className="font-mono text-xs" autoComplete="off" />
          <Button variant="outline" onClick={newSession} disabled={busy}>New session</Button>
        </div></Field>
        <Field><FieldLabel htmlFor="test-url">Target URL</FieldLabel><Input id="test-url" value={url} onChange={event => changeInput("url", event.target.value)} disabled={busy} autoComplete="off" /></Field>
        <Field><FieldLabel htmlFor="test-browser">Browser mode</FieldLabel><NativeSelect id="test-browser" value={browserMode} onChange={event => changeInput("browserMode", event.target.value)} disabled={busy || Boolean(pinnedBrowser)}><NativeSelectOption value="headless">Headless</NativeSelectOption><NativeSelectOption value="headed">Headed · remote desktop</NativeSelectOption></NativeSelect><FieldDescription>Reopen a closed session in either mode.</FieldDescription></Field>
        <Field><FieldLabel htmlFor="test-tab">Tab name</FieldLabel><Input id="test-tab" value={tab} onChange={event => changeInput("tab", event.target.value)} disabled={busy} autoComplete="off" /></Field>
      </FieldGroup>
      <p className="text-sm text-muted-foreground">Enter a URL and send. The first request opens a browser. Further requests use its execution ID. Closing the browser keeps the session profile for later use. Inputs update the JSON below while preserving custom options.</p>
    </section>

    {localError && <Alert variant="destructive"><AlertTitle>Request needs attention</AlertTitle><AlertDescription>{localError}</AlertDescription></Alert>}

    <div className="grid items-start gap-6 xl:grid-cols-2">
      <section className="flex min-w-0 flex-col gap-4" aria-labelledby="test-request-heading">
        <h2 id="test-request-heading" className="text-lg font-semibold">Request</h2>
        <Field><FieldLabel htmlFor="test-preset">Request preset</FieldLabel><div className="flex gap-2">
          <NativeSelect id="test-preset" value={preset} onChange={event => loadPreset(event.target.value as BrowserTestPreset)} disabled={busy} className="flex-1">
            {Object.entries(browserTestPresets).map(([value, label]) => <NativeSelectOption key={value} value={value}>{label}</NativeSelectOption>)}
          </NativeSelect>
          <Button variant="outline" onClick={() => loadPreset(preset)} disabled={busy}>Reset request</Button>
        </div><FieldDescription>Choosing a preset fills the editor. Send uses the exact method, path and JSON shown below.</FieldDescription></Field>

        <fetcher.Form method="post" className="flex flex-col gap-4" onSubmit={() => { setLocalError(null); setCopyStatus(""); }}>
          <input type="hidden" name="intent" value="send" /><input type="hidden" name="execution_id" value={assignment?.executionId || ""} />
          <FieldGroup className="grid grid-cols-[6.5rem_minmax(0,1fr)] gap-3">
            <Field><FieldLabel htmlFor="test-method">Method</FieldLabel><NativeSelect id="test-method" name="method" value={draft.method} onChange={event => setDraft({ ...draft, method: event.target.value, body: event.target.value === "POST" ? draft.body : "" })} disabled={busy} className="w-full">
              {["GET", "POST", "DELETE"].map(method => <NativeSelectOption key={method} value={method}>{method}</NativeSelectOption>)}
            </NativeSelect></Field>
            <Field><FieldLabel htmlFor="test-path">API path</FieldLabel><Input id="test-path" name="path" value={draft.path} onChange={event => setDraft({ ...draft, path: event.target.value })} required disabled={busy} className="font-mono text-xs" spellCheck={false} /></Field>
          </FieldGroup>
          <Field><div className="flex items-center justify-between gap-2"><FieldLabel htmlFor="test-body">Raw JSON body</FieldLabel><Button type="button" variant="ghost" size="sm" disabled={busy || !draft.body} onClick={formatBody}>Format JSON</Button></div>
            <Textarea id="test-body" name="body" value={draft.body} onChange={event => setDraft({ ...draft, body: event.target.value })} readOnly={draft.method !== "POST"} disabled={busy} className="min-h-72 resize-y font-mono text-xs leading-relaxed" spellCheck={false} autoComplete="off" placeholder={draft.method === "POST" ? "Optional JSON body" : "This request has no body."} />
          </Field>
          <div className="flex flex-wrap items-center gap-2">
            <Button type="submit" disabled={busy || !sessionId}><SendIcon data-icon="inline-start" />{fetcher.state !== "idle" ? "Sending…" : "Send request"}</Button>
            <Button type="button" variant="outline" disabled={busy || !pinnedBrowser || assignment?.headless} onClick={() => { setLocalError(null); setBrowserUrl(null); desktop.submit({ intent: "connect", session_id: sessionId }, { method: "post" }); }}><MonitorIcon data-icon="inline-start" />{desktop.state !== "idle" ? "Connecting…" : "Connect browser"}</Button>
          </div>
          <p className="text-xs text-muted-foreground">Browsers launch on demand. Idle timeout closes the browser; the saved session can be reopened. No automatic retries or heartbeats run here.</p>
        </fetcher.Form>
      </section>

      <section className="flex min-w-0 flex-col gap-4" aria-labelledby="test-response-heading" aria-busy={fetcher.state !== "idle"}>
        <div className="flex items-center justify-between gap-2"><h2 id="test-response-heading" className="text-lg font-semibold">Response</h2>
          {selected && <Button variant="outline" size="sm" onClick={() => void copy(selected.body)}><CopyIcon data-icon="inline-start" />Copy response</Button>}
        </div>
        {fetcher.state !== "idle" && <p role="status" className="text-sm text-muted-foreground">Waiting for the browser service… Navigation may take several minutes.</p>}
        {selected ? <>
          <Field><FieldLabel htmlFor="test-history">Recent requests</FieldLabel><div className="flex gap-2">
            <NativeSelect id="test-history" value={String(selectedIndex)} onChange={event => { setSelectedIndex(Number(event.target.value)); setResponseView("raw"); setCopyStatus(""); }} className="min-w-0 flex-1">
              {history.map((entry, index) => <NativeSelectOption key={index} value={String(index)}>{index === 0 ? "Latest" : `Previous ${index}`} · {entry.status} · {entry.request.method} {entry.request.path}</NativeSelectOption>)}
            </NativeSelect><Button variant="outline" disabled={busy} onClick={() => { setDraft(selected.request); setLocalError(null); }}>Load request</Button>
          </div><FieldDescription>The last 10 responses stay here until you leave this page.</FieldDescription></Field>
          <div className="flex flex-wrap items-center gap-2" role="status">
            <Badge variant={selected.status >= 400 ? "destructive" : "secondary"}>API HTTP {selected.status} {selected.statusText}</Badge>
            {typeof payload?.statusCode === "number" && <Badge variant={payload.statusCode >= 400 ? "destructive" : "outline"}>Page HTTP {payload.statusCode}</Badge>}
            <span className="text-xs text-muted-foreground">{selected.durationMs.toLocaleString()} ms · {selected.contentType || "No content type"}</span>
          </div>
          {(typeof responseSession?.profileId === "string" || typeof responseSession?.state === "string") && <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-4 gap-y-1 text-xs">
            {([['id', 'Session'], ['profileId', 'Browser'], ['state', 'State'], ['generation', 'Generation']] as const).map(([key, label]) => typeof responseSession?.[key] === "string" && <div key={key} className="contents"><dt className="text-muted-foreground">{label}</dt><dd className="break-all font-mono">{responseSession[key]}</dd></div>)}
          </dl>}
          <Tabs value={responseView} onValueChange={value => setResponseView(String(value))}>
            <TabsList aria-label="Response format"><TabsTrigger value="raw">Raw response</TabsTrigger><TabsTrigger value="html" disabled={typeof payload?.browserHtml !== "string"}>HTML</TabsTrigger><TabsTrigger value="screenshot" disabled={typeof payload?.screenshot !== "string"}>Screenshot</TabsTrigger><TabsTrigger value="request">Sent request</TabsTrigger></TabsList>
            <TabsContent value="raw"><pre className="max-h-[32rem] overflow-auto rounded-lg border bg-muted/30 p-3 text-xs leading-relaxed whitespace-pre-wrap break-all">{formattedResponse}</pre></TabsContent>
            <TabsContent value="html"><pre className="max-h-[32rem] overflow-auto rounded-lg border bg-muted/30 p-3 text-xs leading-relaxed whitespace-pre-wrap break-all">{typeof payload?.browserHtml === "string" ? payload.browserHtml : ""}</pre></TabsContent>
            <TabsContent value="screenshot">{typeof payload?.screenshot === "string" && <img src={`data:image/png;base64,${payload.screenshot}`} alt="Screenshot returned by the browser service" className="w-full rounded-lg border" />}</TabsContent>
            <TabsContent value="request"><pre className="max-h-[32rem] overflow-auto rounded-lg border bg-muted/30 p-3 text-xs leading-relaxed whitespace-pre-wrap break-all">{selected.request.method} {selected.request.path}{"\n\n"}{selected.request.body}</pre></TabsContent>
          </Tabs>
          {copyStatus && <p role="status" className="text-xs text-muted-foreground">{copyStatus}</p>}
        </> : <Empty className="min-h-80 border"><EmptyHeader><EmptyTitle>Ready for your first request</EmptyTitle><EmptyDescription>Enter a target URL and send. The response will show the assigned browser, page status and captured content.</EmptyDescription></EmptyHeader></Empty>}
      </section>
    </div>
    {browserUrl && <section className="flex flex-col gap-3 border-t pt-6" aria-label="Test browser desktop">
      <div className="flex items-center justify-between gap-3"><h2 className="text-lg font-semibold">Live browser</h2><Button variant="outline" onClick={() => setBrowserUrl(null)}>Disconnect viewer</Button></div>
      <p className="text-sm text-muted-foreground">Session <code>{sessionId}</code>. Disconnecting the viewer keeps the reservation; use the Release browser preset to release it.</p>
      <BrowserDesktop url={browserUrl} />
    </section>}
  </div>;
}
