import { BrowserTabs } from "~/components/admin/browser-tabs";
import { useEffect, useRef, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import { MonitorIcon, RefreshCwIcon } from "lucide-react";
import type { Route } from "./+types/admin-browser-sessions";
import { browserFetch } from "~/lib/browser-service.server";
import { browserSessionAction, loadBrowserSessions } from "~/lib/browser-sessions.server";
import { BrowserDesktop } from "~/components/admin/browser-desktop";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Switch } from "~/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export async function loader() {
  try { return { sessions: (await loadBrowserSessions()).sessions, error: null }; }
  catch (error) { return { sessions: [], error: error instanceof Error ? error.message : "Browsers unavailable" }; }
}

export async function action({ request }: Route.ActionArgs) {
  if (request.headers.get("Origin") !== new URL(request.url).origin) return { error: "Cross-origin browser controls are not allowed." };
  const form = await request.formData();
  const intent = String(form.get("intent") || "");
  const sessionId = String(form.get("session_id") || "");
  const tabId = String(form.get("tab_id") || "");
  if (intent === "create") {
    try {
      await browserFetch("/v1/browser-sessions", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ headless: false }), signal: AbortSignal.timeout(90_000) });
      return { error: null };
    } catch (error) { return { error: error instanceof Error ? error.message : "Session could not start." }; }
  }
  if (!/^[a-f0-9]{32}$/.test(sessionId) || !["start", "stop", "open-tab", "browser-ticket", "focus", "inspect", "settings"].includes(intent)
      || (["focus", "inspect"].includes(intent) && !/^[a-f0-9]{32}$/.test(tabId))) return { error: "Invalid browser action." };
  try {
    if (intent === "settings" && !["true", "false"].includes(String(form.get("pinned")))) return { error: "Invalid pin setting." };
    const result = await browserSessionAction(sessionId, intent, intent === "settings"
      ? { pinned: form.get("pinned") === "true" }
      : { url: String(form.get("url") || ""), tabId, executionId: String(form.get("execution_id") || "") || undefined, ...(intent === "start" ? { headless: form.get("headless") === "true" } : {}) });
    return { error: null, sessionId, result };
  } catch (error) { return { error: error instanceof Error ? error.message : "Browser action failed." }; }
}

export function meta() { return [{ title: "Browser sessions | CompanyCollect admin" }]; }

export default function AdminBrowserSessions({ loaderData }: Route.ComponentProps) {
  const { sessions, error } = loaderData;
  const fetcher = useFetcher<typeof action>();
  const { revalidate } = useRevalidator();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [browserUrl, setBrowserUrl] = useState<string | null>(null);
  const generation = useRef<string | null>(null);
  const handled = useRef<unknown>(null);
  const selected = sessions.find(session => session.id === selectedId);
  const busy = fetcher.state !== "idle";
  function command(intent: string, sessionId: string, tabId?: string) {
    fetcher.submit({ intent, session_id: sessionId, tab_id: tabId || "", execution_id: sessions.find(s => s.id === sessionId)?.execution_id || "" }, { method: "post" });
  }
  useEffect(() => {
    const timer = setInterval(() => { if (!document.hidden) void revalidate(); }, 5_000);
    return () => clearInterval(timer);
  }, [revalidate]);
  useEffect(() => {
    const data = fetcher.data;
    if (!data || data === handled.current) return;
    handled.current = data;
    if ("result" in data && data.result?.browserUrl) {
      setSelectedId(data.sessionId);
      generation.current = sessions.find(session => session.id === data.sessionId)?.generation || null;
      setBrowserUrl(data.result.browserUrl);
    }
  }, [fetcher.data, sessions]);
  useEffect(() => {
    if (!selected || selected.headless) return;
    if (selected.state !== "running") { setBrowserUrl(null); return; }
    if (selected.generation !== generation.current && !busy) {
      generation.current = selected.generation;
      setBrowserUrl(null);
      command("browser-ticket", selected.id);
    }
  }, [selected, busy]);
  const inspection = fetcher.data && "result" in fetcher.data ? fetcher.data.result?.inspection : undefined;

  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <div className="flex items-start justify-between gap-4">
      <div><h1 className="text-2xl font-semibold">Browser sessions</h1><p className="mt-1 text-sm text-muted-foreground">Each session keeps its own profile. Open it when needed; closing the browser releases capacity and preserves login state.</p></div>
      <div className="flex gap-2"><Button disabled={busy} onClick={() => fetcher.submit({ intent: "create" }, { method: "post" })}>New headed session</Button><Button variant="outline" onClick={() => void revalidate()}><RefreshCwIcon data-icon="inline-start" />Refresh</Button></div>
    </div>
    <BrowserTabs value="profiles" />
    {(error || fetcher.data?.error) && <Alert variant="destructive"><AlertTitle>Browser unavailable</AlertTitle><AlertDescription>{fetcher.data?.error || error}</AlertDescription></Alert>}
    {sessions.length === 0 ? <Empty><EmptyHeader><EmptyTitle>No saved sessions yet</EmptyTitle><EmptyDescription>Sessions are created by crawl and Brave requests, or with New headed session.</EmptyDescription></EmptyHeader></Empty> : <Table>
      <TableHeader><TableRow><TableHead>Session</TableHead><TableHead>Status</TableHead><TableHead>Assigned domain</TableHead><TableHead>Tabs</TableHead><TableHead>Retained until</TableHead><TableHead>Keep session</TableHead><TableHead>Controls</TableHead></TableRow></TableHeader>
      <TableBody>{sessions.map(session => <TableRow key={session.id}>
        <TableCell className="font-medium">{session.label || session.id}{session.label && <div className="font-mono text-xs font-normal">{session.id}</div>}<div className="text-xs font-normal text-muted-foreground">{session.headless ? "Headless" : "Headed · Xvfb"}</div></TableCell><TableCell><Badge variant={session.state === "error" ? "destructive" : "secondary"}>{session.request_id && !session.request_id.startsWith("manual-") ? "in use" : session.state}</Badge>{session.error && <p className="text-sm">{session.error}</p>}</TableCell>
        <TableCell>{session.domain || (session.state === "running" ? "Manual session" : "Closed")}{session.request_id && <p className="text-xs text-muted-foreground">{session.request_id}</p>}{session.lease_id && <p className="text-xs text-muted-foreground">Session ID: <code>{session.lease_id}</code></p>}</TableCell>
        <TableCell>{session.tabs.length}</TableCell><TableCell>{session.pinned ? "Pinned" : new Date(session.retained_until * 1000).toLocaleString()}</TableCell>
        <TableCell><Switch aria-label={`Keep session ${session.id}`} checked={session.pinned} disabled={busy} onCheckedChange={enabled => fetcher.submit({ intent: "settings", session_id: session.id, pinned: String(enabled) }, { method: "post" })} /></TableCell>
        <TableCell><div className="flex gap-2"><Button size="sm" disabled={busy || session.headless || session.state !== "running"} onClick={() => command("browser-ticket", session.id)}><MonitorIcon data-icon="inline-start" />Open browser</Button>
          <Button size="sm" variant="outline" disabled={busy || Boolean(session.request_id && !session.request_id.startsWith("manual-"))} onClick={() => command(Boolean(session.execution_id) ? "stop" : "start", session.id)}>{Boolean(session.execution_id) ? "Stop and save" : "Open headed"}</Button></div></TableCell>
      </TableRow>)}</TableBody>
    </Table>}
    <p className="text-sm text-muted-foreground">Closing a browser preserves the session ID and profile until retention expires. Pin sessions you want to keep. Open headed reuses the same profile and enables the remote desktop; active requests must finish first.</p>
    {selected && <section className="flex flex-col gap-4" aria-label={`${selected.id} browser desktop`}>
      <div><h2 className="text-lg font-semibold">{selected.id}</h2><p className="text-sm text-muted-foreground">Log in directly in this browser. Tabs share this session’s login state. Stopping the browser preserves its profile.</p></div>
      <fetcher.Form method="post"><input type="hidden" name="intent" value="open-tab" /><input type="hidden" name="session_id" value={selected.id} />
        <FieldGroup className="flex-row items-end"><Field><FieldLabel htmlFor="browser-tab-url">Open a website in a new tab</FieldLabel><Input id="browser-tab-url" name="url" type="url" required placeholder="https://melexis.com/" /></Field>
          <Button type="submit" disabled={busy || selected.state !== "running" || Boolean(selected.request_id && !selected.request_id.startsWith("manual-"))}>Open tab</Button></FieldGroup>
      </fetcher.Form>
      <BrowserDesktop url={browserUrl} />
      <Table><TableHeader><TableRow><TableHead>Page</TableHead><TableHead>URL</TableHead><TableHead>Controls</TableHead></TableRow></TableHeader>
        <TableBody>{selected.tabs.map(tab => <TableRow key={tab.id}><TableCell>{tab.title || "New tab"}</TableCell><TableCell className="max-w-xl truncate" title={tab.url}>{tab.url}</TableCell><TableCell><div className="flex gap-2"><Button size="sm" variant="outline" disabled={busy} onClick={() => command("focus", selected.id, tab.id)}>Show tab</Button><Button size="sm" variant="outline" disabled={busy} onClick={() => command("inspect", selected.id, tab.id)}>Check access</Button></div></TableCell></TableRow>)}</TableBody>
      </Table>
      {inspection && <Alert><AlertTitle>{inspection.access_problem ? "Page access blocked" : "No challenge detected"}</AlertTitle><AlertDescription>{inspection.title} · {inspection.status_code === null ? "Response status not observed" : `HTTP ${inspection.status_code}`} · {inspection.url}</AlertDescription></Alert>}
    </section>}
  </div>;
}
