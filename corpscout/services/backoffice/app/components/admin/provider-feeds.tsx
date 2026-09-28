import { Form, isRouteErrorResponse, Link } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { feedProtocol, flagFeed, rangeStatus, safeHttpUrl, type FeedRun, type IndicatorRules, type IPRange } from "~/lib/provider-recon";
import type { ProviderReconDagster } from "~/lib/provider-recon-ops.server";

const STATUS_VARIANT: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
  ok: "secondary",
  active: "secondary",
  stale: "outline",
  missing: "outline",
  failed: "destructive",
  removed: "destructive",
};

export function StatusBadge({ status }: { status: string }) {
  return <Badge variant={STATUS_VARIANT[status] ?? "outline"}>{status}</Badge>;
}

/** One row per feed; rows the warning rules flag are highlighted with their reasons. */
export function FeedTable({ feeds, rules }: { feeds: FeedRun[]; rules: IndicatorRules }) {
  if (feeds.length === 0) return <p className="text-sm text-muted-foreground">No feeds in this run.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Provider</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Source</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Ranges</TableHead>
          <TableHead className="text-right">Added</TableHead>
          <TableHead className="text-right">Missing</TableHead>
          <TableHead className="text-right">Reappeared</TableHead>
          <TableHead className="text-right">Removed</TableHead>
          <TableHead className="text-right">Purged</TableHead>
          <TableHead>Warnings</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {feeds.map((feed) => {
          const flags = flagFeed(feed, rules);
          const flagged = flags.length > 0;
          return (
            <TableRow key={`${feed.slug}/${feed.collector}`} data-flagged={flagged ? "true" : "false"} className={flagged ? "bg-destructive/5" : undefined}>
              <TableCell>
                <Link className="font-medium underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${feed.slug}`}>
                  {feed.slug}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-xs">{feed.collector}</TableCell>
              <TableCell className="max-w-72">
                <FeedSource url={feed.source_url} format={feed.format} version={feed.source_version} />
              </TableCell>
              <TableCell><StatusBadge status={feed.status} /></TableCell>
              <TableCell className="text-right tabular-nums">{feed.items}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.added}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.missing}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.reappeared}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.removed}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.purged}</TableCell>
              <TableCell>
                {flagged ? (
                  <ul className="flex flex-col gap-1 text-sm text-destructive">
                    {flags.map((f) => <li key={f.kind}>{f.message}</li>)}
                  </ul>
                ) : (
                  <span className="text-sm text-muted-foreground">—</span>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

/** Protocol (scheme · format), the full URL as a link when it is http(s), and the source version. */
export function FeedSource({ url, format, version }: { url?: string; format?: string; version?: string }) {
  const href = safeHttpUrl(url);
  return (
    <>
      <div className="text-xs font-medium">{feedProtocol(url, format)}</div>
      {href ? (
        <a className="block truncate font-mono text-xs underline-offset-4 hover:underline" href={href} target="_blank" rel="noreferrer" title={url}>
          {url}
        </a>
      ) : null}
      {version ? (
        <div className="truncate font-mono text-xs text-muted-foreground" title={version}>
          {version}
        </div>
      ) : null}
    </>
  );
}

function when(seconds: number | null): string {
  return seconds === null ? "—" : `${new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 16)} UTC`;
}

const LINK = "underline-offset-4 hover:underline";

/** provider-recon in Dagster: assets, schedule, recent runs, and the Run now / schedule controls. */
const ACTIVE_RUN = new Set(["QUEUED", "NOT_STARTED", "MANAGED", "STARTING", "STARTED"]);

export function DagsterPanel({ dagster, busy = false }: { dagster: ProviderReconDagster; busy?: boolean }) {
  if (dagster.error) {
    return <p className="text-sm text-destructive">Dagster unavailable: {dagster.error}</p>;
  }
  const running = dagster.schedule?.status === "RUNNING";
  // A second launch would only collide with the first one's collect (409) in the service.
  const active = dagster.runs.some((r) => ACTIVE_RUN.has(r.status));
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-2">
        <Form method="post">
          <Button type="submit" name="intent" value="run-now" disabled={busy || active}>
            {active ? "Run in progress" : "Run now"}
          </Button>
        </Form>
        <Form method="post">
          <Button type="submit" variant="outline" name="intent" value={running ? "schedule-stop" : "schedule-start"} disabled={busy}>
            {running ? "Stop schedule" : "Start schedule"}
          </Button>
        </Form>
      </div>
      {dagster.schedule && (
        <p className="text-sm">
          Schedule <span className="font-mono">{dagster.schedule.name}</span> <StatusBadge status={dagster.schedule.status} />{" "}
          <span className="font-mono">{dagster.schedule.cronSchedule}</span> {dagster.schedule.timezone ?? ""} · next {when(dagster.schedule.nextTick)}
        </p>
      )}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Asset</TableHead>
            <TableHead>Last materialised</TableHead>
            <TableHead>Numbers</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {dagster.assets.map((a) => (
            <TableRow key={a.asset}>
              <TableCell>
                {a.url ? (
                  <a className={`font-mono text-xs ${LINK}`} href={a.url} target="_blank" rel="noreferrer">{a.asset}</a>
                ) : (
                  <span className="font-mono text-xs">{a.asset}</span>
                )}
              </TableCell>
              <TableCell className="text-sm">
                {a.runUrl ? <a className={LINK} href={a.runUrl} target="_blank" rel="noreferrer">{when(a.materializedAt)}</a> : when(a.materializedAt)}
              </TableCell>
              <TableCell className="text-xs">{Object.entries(a.numbers).map(([k, v]) => `${k}: ${v}`).join(", ") || "—"}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {dagster.runs.length > 0 && (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Run</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Started</TableHead>
              <TableHead className="text-right">Took</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {dagster.runs.map((r) => (
              <TableRow key={r.runId}>
                <TableCell>
                  {r.url ? (
                    <a className={`font-mono text-xs ${LINK}`} href={r.url} target="_blank" rel="noreferrer">{r.runId.slice(0, 8)}</a>
                  ) : (
                    <span className="font-mono text-xs">{r.runId.slice(0, 8)}</span>
                  )}
                </TableCell>
                <TableCell><StatusBadge status={r.status === "SUCCESS" ? "ok" : r.status.toLowerCase()} /></TableCell>
                <TableCell className="text-sm">{when(r.startTime)}</TableCell>
                <TableCell className="text-right text-sm tabular-nums">
                  {r.startTime && r.endTime ? `${Math.round(r.endTime - r.startTime)} s` : "—"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

/** Route error panel: keeps the admin shell and sidebar instead of a full-page crash. */
export function ProviderFeedsError({ error }: { error: unknown }) {
  const message = isRouteErrorResponse(error) ? String(error.data) : error instanceof Error ? error.message : "Unknown error";
  return (
    <div className="p-4 md:p-6">
      <Alert variant="destructive">
        <AlertTitle>Provider feeds unavailable</AlertTitle>
        <AlertDescription>{message}</AlertDescription>
      </Alert>
    </div>
  );
}

/** Missing and removed ranges with their lifecycle dates. */
export function RangeTable({ rows }: { rows: { serviceKey: string; range: IPRange }[] }) {
  if (rows.length === 0) return <p className="text-sm text-muted-foreground">Every range is active.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Range</TableHead>
          <TableHead>Service</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>First seen</TableHead>
          <TableHead>Last seen</TableHead>
          <TableHead>Missing since</TableHead>
          <TableHead>Removed</TableHead>
          <TableHead>Action</TableHead>
          <TableHead>Restored</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map(({ serviceKey, range }) => (
          <TableRow key={`${serviceKey}/${range.cidr}/${range.collector ?? "curated"}/${range.first_seen ?? ""}`}>
            <TableCell className="font-mono text-xs">{range.cidr}</TableCell>
            <TableCell className="text-sm">{serviceKey}</TableCell>
            <TableCell className="font-mono text-xs">{range.collector ?? "curated"}</TableCell>
            <TableCell><StatusBadge status={rangeStatus(range)} /></TableCell>
            <TableCell className="tabular-nums">{range.first_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.last_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.missing_since ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.removed_at ?? "—"}</TableCell>
            <TableCell className="text-sm">{range.removal_action ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.restored_at ?? "—"}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
