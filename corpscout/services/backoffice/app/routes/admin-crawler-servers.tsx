import { useEffect, useRef, useState } from "react";
import { Link, useFetcher, useRevalidator } from "react-router";
import { MonitorIcon, RefreshCwIcon } from "lucide-react";
import type { Route } from "./+types/admin-crawler-servers";
import { connectBrowserDesktop, loadBrowserServer } from "~/lib/browser-servers.server";
import { browserFetch } from "~/lib/browser-service.server";
import { BrowserDesktop } from "~/components/admin/browser-desktop";
import { BrowserTabs } from "~/components/admin/browser-tabs";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export async function loader() {
  try { return { server: await loadBrowserServer(), error: null }; }
  catch (error) { return { server: null, error: error instanceof Error ? error.message : "Server unavailable" }; }
}

export async function action({ request }: Route.ActionArgs) {
  if (request.headers.get("Origin") !== new URL(request.url).origin) return { error: "Cross-origin browser controls are not allowed." };
  const form = await request.formData();
  const intent = String(form.get("intent") || "connect");
  const id = String(form.get(intent === "terminate" ? "session_id" : "desktop_id") || "");
  if (!["connect", "terminate"].includes(intent) || !/^[a-f0-9]{32}$/.test(id)) return { error: "Invalid browser session action." };
  try {
    if (intent === "terminate") {
      await browserFetch(`/v1/browser/sessions/${id}`, {
        method: "DELETE",
        headers: { "X-Browser-Execution-Id": String(form.get("execution_id") || "") },
        // Release waits for any in-flight operation and browser shutdown.
        signal: AbortSignal.timeout(360_000),
      });
      return { error: null, terminatedId: id };
    }
    return { error: null, id, browserUrl: await connectBrowserDesktop(id) };
  }
  catch (error) { return { error: error instanceof Error ? error.message : "Browser action failed" }; }
}

export function meta() { return [{ title: "Browser servers | CompanyCollect admin" }]; }
const kinds = { saved: "Saved profile", brave: "Brave search", interactive: "Interactive crawl" };

export default function AdminCrawlerServers({ loaderData }: Route.ComponentProps) {
  const { server, error } = loaderData;
  const fetcher = useFetcher<typeof action>();
  const { revalidate } = useRevalidator();
  const [connection, setConnection] = useState<{ id: string; browserUrl: string; savedProfile: string | null } | null>(null);
  const handled = useRef<unknown>(null);
  const connecting = useRef<string | null>(null);
  const selected = server?.sessions.find(session => session.id === connection?.id);
  const terminatingId = fetcher.state !== "idle" && fetcher.formData?.get("intent") === "terminate"
    ? String(fetcher.formData.get("session_id")) : null;
  const terminatedId = fetcher.state === "idle" && fetcher.data && "terminatedId" in fetcher.data ? fetcher.data.terminatedId : null;
  useEffect(() => {
    const timer = setInterval(() => { if (!document.hidden) void revalidate(); }, 5_000);
    return () => clearInterval(timer);
  }, [revalidate]);
  useEffect(() => {
    const data = fetcher.data;
    if (!data || data === handled.current) return;
    handled.current = data;
    if ("browserUrl" in data && data.browserUrl && data.id) {
      const desktop = server?.sessions.find(session => session.id === data.id);
      setConnection({ id: data.id, browserUrl: data.browserUrl, savedProfile: desktop?.kind === "saved" ? desktop.name : null });
    }
  }, [fetcher.data, server]);
  useEffect(() => {
    if (!connection?.savedProfile || selected || fetcher.state !== "idle") return;
    const replacement = server?.sessions.find(session => session.kind === "saved" && session.name === connection.savedProfile && session.state === "running");
    if (replacement && connecting.current !== replacement.id) {
      connecting.current = replacement.id;
      fetcher.submit({ desktop_id: replacement.id }, { method: "post" });
    }
  }, [server, connection, selected, fetcher.state]);
  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <div className="flex items-start justify-between gap-4">
      <div><h1 className="text-2xl font-semibold">Browsers</h1><p className="mt-1 text-sm text-muted-foreground">Independent browser servers, active assignments, and remote desktops.</p></div>
      <Button variant="outline" onClick={() => void revalidate()}><RefreshCwIcon data-icon="inline-start" />Refresh</Button>
    </div>
    <BrowserTabs value="servers" />
    {(error || fetcher.data?.error) && <Alert variant="destructive"><AlertTitle>{fetcher.data?.error ? "Browser action failed" : "Server unavailable"}</AlertTitle><AlertDescription>{fetcher.data?.error || error}</AlertDescription></Alert>}
    {terminatedId && <Alert><AlertTitle>Session terminated</AlertTitle><AlertDescription>Session <code className="break-all">{terminatedId}</code> has been released.</AlertDescription></Alert>}
    {terminatingId && <p role="status" className="text-sm text-muted-foreground">Terminating session {terminatingId}. Waiting for any current request and browser cleanup to finish.</p>}
    {server && <>
      {server.settings && <p className="text-sm text-muted-foreground">Browsers: {server.settings.capacity.occupied} active / {server.settings.current.max_browsers} maximum. <Link to="/admin/browsers/settings" className="underline">Configure browsers</Link></p>}
      <Table><TableHeader><TableRow><TableHead>Server</TableHead><TableHead>Status</TableHead><TableHead>Version</TableHead><TableHead>Open Xvfb sessions</TableHead></TableRow></TableHeader>
        <TableBody><TableRow><TableCell className="font-medium">{server.hostname}</TableCell><TableCell><Badge variant={server.healthy ? "secondary" : "destructive"}>{server.healthy ? "Online" : "Not ready"}</Badge></TableCell><TableCell>{server.version}</TableCell><TableCell>{server.sessions.length}</TableCell></TableRow></TableBody>
      </Table>
      <section className="flex flex-col gap-3" aria-label="Browser assignments">
        <div><h2 className="text-lg font-semibold">Assignments</h2><p className="text-sm text-muted-foreground">Idle sessions expire after {server.idle_timeout_seconds} seconds. Terminate ends an assignment and saves its profile and closes its browser. Viewing this page does not extend sessions.</p></div>
        <Table><TableHeader><TableRow><TableHead>Domain / crawl</TableHead><TableHead>Execution ID</TableHead><TableHead>Saved session</TableHead><TableHead>Status</TableHead><TableHead>Last request</TableHead><TableHead>Idle deadline</TableHead><TableHead>Controls</TableHead></TableRow></TableHeader>
          <TableBody>{server.leases?.map(lease => <TableRow key={lease.id}>
            <TableCell>{lease.domain}<div className="text-xs text-muted-foreground">{lease.request_id}</div></TableCell>
            <TableCell className="font-mono text-xs">{lease.id}</TableCell><TableCell>{lease.profile_id || "Waiting"}</TableCell>
            <TableCell><Badge variant="secondary">{lease.operation ? `${lease.operation.kind}: ${lease.operation.stage.replaceAll("_", " ")}` : lease.state}</Badge>{lease.operation && lease.operation.agent_runs > 0 && <p className="text-xs text-muted-foreground">{lease.operation.agent_runs} CAPTCHA agent runs completed</p>}</TableCell>
            <TableCell>{new Date(lease.last_request_at * 1000).toLocaleTimeString()}</TableCell>
            <TableCell>{["starting", "ready"].includes(lease.state) ? new Date(lease.expires_at * 1000).toLocaleTimeString() : "—"}</TableCell>
            <TableCell>{["starting", "ready", "stopping"].includes(lease.state) && <Button size="sm" variant="destructive" aria-label={`Terminate session ${lease.id}`} disabled={fetcher.state !== "idle" || lease.state === "stopping"} onClick={() => {
              if (connection?.savedProfile === lease.profile_id || selected?.request_id === lease.request_id) setConnection(null);
              fetcher.submit({ intent: "terminate", session_id: lease.session_id, execution_id: lease.id }, { method: "post" });
            }}>{terminatingId === lease.session_id || lease.state === "stopping" ? "Terminating…" : "Terminate"}</Button>}</TableCell>
          </TableRow>)}</TableBody>
        </Table>
      </section>
      <section className="flex flex-col gap-3" aria-label="Open Xvfb sessions">
        <div><h2 className="text-lg font-semibold">Open sessions</h2><p className="text-sm text-muted-foreground">All browser desktops managed by the browser service. Connect to open the live browser.</p></div>
        {server.sessions.length === 0 ? <p className="text-sm text-muted-foreground">No Xvfb sessions are open.</p> : <Table>
          <TableHeader><TableRow><TableHead>Session</TableHead><TableHead>Type</TableHead><TableHead>Status</TableHead><TableHead>Page / crawl</TableHead><TableHead>Started</TableHead><TableHead>Connect</TableHead></TableRow></TableHeader>
          <TableBody>{server.sessions.map(session => <TableRow key={session.id}>
            <TableCell><div className="font-medium">{session.name}</div><div className="font-mono text-xs text-muted-foreground">{session.id.slice(0, 8)}</div></TableCell>
            <TableCell>{kinds[session.kind]}</TableCell><TableCell><Badge variant="secondary">{session.state.replaceAll("_", " ")}</Badge></TableCell>
            <TableCell><div className="max-w-md truncate" title={session.url || undefined}>{session.url || "New tab"}</div>{session.request_id && <div className="text-xs text-muted-foreground">{session.request_id}</div>}</TableCell>
            <TableCell className="whitespace-nowrap text-xs">{new Date(session.started_at).toLocaleTimeString()}</TableCell>
            <TableCell><Button size="sm" disabled={fetcher.state !== "idle"} onClick={() => fetcher.submit({ desktop_id: session.id }, { method: "post" })}><MonitorIcon data-icon="inline-start" />{connection?.id === session.id ? "Reconnect" : "Connect"}</Button></TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </section>
    </>}
    {connection && <section className="flex flex-col gap-3" aria-label="Selected browser session">
      <div className="flex items-center justify-between"><h2 className="text-lg font-semibold">{selected?.name || "Session closed"}</h2><Button variant="outline" onClick={() => setConnection(null)}>Disconnect</Button></div>
      {selected ? <>
        {selected.kind !== "saved" && <p className="text-sm text-muted-foreground">The crawler may navigate this browser while it is active. <Link className="underline" to="/admin/crawls">Manage the crawl</Link> to resume or cancel it.</p>}
        <BrowserDesktop url={connection.browserUrl} />
      </> : <p className="text-sm text-muted-foreground">This desktop is no longer open. Choose another session above.</p>}
    </section>}
  </div>;
}
