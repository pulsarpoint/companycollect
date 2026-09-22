import { useEffect, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import type { Route } from "./+types/admin-browser-settings";
import { loadBrowserServer } from "~/lib/browser-servers.server";
import type { BrowserRuntimeConfiguration } from "~/lib/browser-servers.server";
import { browserFetch } from "~/lib/browser-service.server";
import { BrowserTabs } from "~/components/admin/browser-tabs";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";

export async function loader() {
  try { return { server: await loadBrowserServer(), error: null }; }
  catch (error) { return { server: null, error: error instanceof Error ? error.message : "Settings unavailable" }; }
}

export async function action({ request }: Route.ActionArgs) {
  if (request.headers.get("Origin") !== new URL(request.url).origin) return { error: "Cross-origin browser controls are not allowed." };
  const form = await request.formData();
  const intent = String(form.get("intent") || "");
  if (!["settings", "reset"].includes(intent)) return { error: "Invalid browser settings action." };
  const maximum = Number(form.get("max_browsers"));
  const idle = Number(form.get("idle_timeout_seconds"));
  const retention = Number(form.get("session_retention_days"));
  if (intent === "settings" && (!/^\d+$/.test(String(form.get("max_browsers"))) || maximum > 64 || !Number.isFinite(idle) || idle <= 0 || !Number.isFinite(retention) || retention <= 0 || retention > 3650)) {
    return { error: "Use 0–64 browsers, a positive idle timeout, and retention between 0 and 3650 days." };
  }
  try {
    await browserFetch("/v1/browser/settings", intent === "reset" ? { method: "DELETE" } : {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ max_browsers: maximum, idle_timeout_seconds: idle, session_retention_days: retention }),
    });
    return { error: null, message: intent === "reset" ? "Startup settings restored." : "Browser settings saved in SQLite." };
  } catch (error) { return { error: error instanceof Error ? error.message : "Settings could not be saved." }; }
}

export function meta() { return [{ title: "Browser settings | CompanyCollect admin" }]; }

function SettingsForm({ settings }: { settings: BrowserRuntimeConfiguration }) {
  const fetcher = useFetcher<typeof action>();
  const [maximum, setMaximum] = useState(String(settings.current.max_browsers));
  const [idle, setIdle] = useState(String(settings.current.idle_timeout_seconds));
  const [retention, setRetention] = useState(String(settings.current.session_retention_days));
  useEffect(() => {
    setMaximum(String(settings.current.max_browsers));
    setIdle(String(settings.current.idle_timeout_seconds));
    setRetention(String(settings.current.session_retention_days));
  }, [settings.current.max_browsers, settings.current.idle_timeout_seconds, settings.current.session_retention_days]);
  const busy = fetcher.state !== "idle";
  return <section className="flex max-w-2xl flex-col gap-4" aria-label="Browser configuration">
    <p className="text-sm">{settings.capacity.occupied} browsers active · {settings.capacity.available} places available</p>
    <fetcher.Form method="post" className="flex flex-col gap-4">
      <input type="hidden" name="intent" value="settings" />
      <FieldGroup>
        <Field><FieldLabel htmlFor="maximum-browsers">Maximum browsers</FieldLabel><Input id="maximum-browsers" name="max_browsers" type="number" min={0} max={64} step={1} required value={maximum} onChange={e => setMaximum(e.target.value)} /><FieldDescription>One limit for headless and headed browsers. Zero pauses new executions. Reducing the limit lets active work finish.</FieldDescription></Field>
        <Field><FieldLabel htmlFor="browser-idle">Idle timeout (seconds)</FieldLabel><Input id="browser-idle" name="idle_timeout_seconds" type="number" min={0.1} step="any" required value={idle} onChange={e => setIdle(e.target.value)} /><FieldDescription>Closes idle browsers and releases capacity. The saved session remains available. Crawler heartbeats arrive every 20 seconds.</FieldDescription></Field>
        <Field><FieldLabel htmlFor="browser-retention">Session retention (days)</FieldLabel><Input id="browser-retention" name="session_retention_days" type="number" min={0.01} max={3650} step="any" required value={retention} onChange={e => setRetention(e.target.value)} /><FieldDescription>Retains profiles after last use. Pin a saved session to keep it beyond this period. Changes apply when sessions are next used or closed.</FieldDescription></Field>
      </FieldGroup>
      <div className="flex gap-2"><Button type="submit" disabled={busy}>Save settings</Button><Button type="button" variant="outline" disabled={busy || settings.source !== "sqlite"} onClick={() => fetcher.submit({ intent: "reset" }, { method: "post" })}>Use startup settings</Button></div>
    </fetcher.Form>
    <p className="text-sm text-muted-foreground">Source: {settings.source === "sqlite" ? "SQLite override" : "Startup settings"}. Startup: {settings.startup.max_browsers} browsers, {settings.startup.idle_timeout_seconds}s idle timeout, {settings.startup.session_retention_days} days retention.</p>
    {fetcher.data?.error && <p role="alert" className="text-sm text-destructive">{fetcher.data.error}</p>}
    {fetcher.data && "message" in fetcher.data && <p role="status" className="text-sm">{fetcher.data.message}</p>}
  </section>;
}

export default function AdminBrowserSettings({ loaderData }: Route.ComponentProps) {
  const { revalidate } = useRevalidator();
  useEffect(() => { const timer = setInterval(() => { if (!document.hidden) void revalidate(); }, 5000); return () => clearInterval(timer); }, [revalidate]);
  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <div><h1 className="text-2xl font-semibold">Browser settings</h1><p className="mt-1 text-sm text-muted-foreground">Browsers open on request. Saved settings override startup configuration and survive restarts.</p></div>
    <BrowserTabs value="settings" />
    {loaderData.error && <Alert variant="destructive"><AlertTitle>Settings unavailable</AlertTitle><AlertDescription>{loaderData.error}</AlertDescription></Alert>}
    {loaderData.server?.settings && <SettingsForm settings={loaderData.server.settings} />}
  </div>;
}
