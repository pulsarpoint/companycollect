import type { ReactNode } from "react";
import {
  CircleAlertIcon,
  CircleCheckIcon,
  LoaderCircleIcon,
  RefreshCwIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  formatAge,
  formatBytes,
  formatCount,
  formatNanos,
  streamBacklog,
  usagePercent,
  type NatsConsumer,
  type NatsJetStreamInfo,
  type NatsMonitorResult,
  type NatsServerInfo,
  type NatsStream,
} from "~/lib/nats-monitor";
import { cn } from "~/lib/utils";

const RETENTION_LABELS: Record<string, string> = {
  limits: "Limits",
  interest: "Interest",
  workqueue: "Work queue",
};
const STORAGE_LABELS: Record<string, string> = { file: "File", memory: "Memory" };
const DISCARD_LABELS: Record<string, string> = { old: "Drop oldest", new: "Reject new" };

const clock = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/Stockholm",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
const dateTime = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/Stockholm",
  dateStyle: "medium",
  timeStyle: "medium",
});

function label(labels: Record<string, string>, value: string): string {
  return labels[value] ?? value;
}

/**
 * A headline number. Proportional figures: tabular ones look loose this large.
 * Deliberately no warning state: the server's counters are cumulative since
 * start and never return to zero, so a flag on one would be permanently on.
 * Warnings are kept for current states (pending redeliveries, a missing cap).
 */
function StatTile({ name, value, hint }: { name: string; value: string; hint?: ReactNode }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border p-4">
      <dt className="text-xs text-muted-foreground">{name}</dt>
      <dd className="text-xl font-semibold">{value}</dd>
      {hint && <dd className="text-xs text-muted-foreground">{hint}</dd>}
    </div>
  );
}

/**
 * Used-of-limit. The fill carries severity and the track is a lighter step of
 * the same colour, so the state reads across the whole bar; the percentage and
 * wording carry it too, never the colour alone.
 */
function UsageMeter({
  name,
  used,
  limit,
  note,
}: {
  name: string;
  used: number;
  limit: number;
  note?: string;
}) {
  const percent = usagePercent(used, limit) ?? 0;
  const severity = percent >= 90 ? "critical" : percent >= 75 ? "warning" : "normal";
  return (
    <div className="flex flex-col gap-2 rounded-lg border p-4">
      <div className="flex items-baseline justify-between gap-3">
        <span className="text-xs text-muted-foreground">{name}</span>
        <span className="text-xs text-muted-foreground tabular-nums">
          {percent}%{severity === "critical" ? " · nearly full" : severity === "warning" ? " · filling up" : ""}
        </span>
      </div>
      <div className="text-xl font-semibold">
        {formatBytes(used)} <span className="text-sm font-normal text-muted-foreground">of {formatBytes(limit)}</span>
      </div>
      <div
        role="meter"
        aria-label={`${name} used`}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={percent}
        aria-valuetext={`${formatBytes(used)} of ${formatBytes(limit)}`}
        className={cn(
          "h-1.5 w-full overflow-hidden rounded-full",
          severity === "critical" ? "bg-destructive/20" : severity === "warning" ? "bg-amber-500/20" : "bg-primary/15",
        )}
      >
        <div
          className={cn(
            "h-full rounded-full",
            severity === "critical" ? "bg-destructive" : severity === "warning" ? "bg-amber-500" : "bg-primary",
          )}
          // A sliver stays visible for any non-zero use, which a rounded 0% would hide.
          style={{ width: used > 0 ? `max(${percent}%, 2px)` : "0%" }}
        />
      </div>
      {note && <span className="text-xs text-muted-foreground">{note}</span>}
    </div>
  );
}

function ServerSection({ server }: { server: NatsServerInfo }) {
  return (
    <section aria-labelledby="nats-server-title" className="space-y-3">
      <h2 id="nats-server-title" className="text-base font-semibold">Server</h2>
      <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatTile name="Uptime" value={server.uptime} hint={<span title={server.startedAt}>Started {dateTime.format(new Date(server.startedAt))}</span>} />
        <StatTile name="Connections" value={formatCount(server.connections)} hint={`${formatCount(server.totalConnections)} since start · max ${formatCount(server.maxConnections)}`} />
        <StatTile name="Subscriptions" value={formatCount(server.subscriptions)} />
        <StatTile name="Slow consumers" value={formatCount(server.slowConsumers)} hint={`Clients dropped since start · ${formatCount(server.staleConnections)} stale`} />
        <StatTile name="CPU" value={`${server.cpuPercent}%`} hint={`${server.cores} cores`} />
        <StatTile name="Memory" value={formatBytes(server.memoryBytes)} hint="Server process" />
        <StatTile name="Messages in / out" value={`${formatCount(server.inMessages)} / ${formatCount(server.outMessages)}`} hint={`${formatBytes(server.inBytes)} in · ${formatBytes(server.outBytes)} out`} />
        <StatTile name="Max payload" value={formatBytes(server.maxPayloadBytes)} hint="Largest single message" />
      </dl>
      <p className="text-xs text-muted-foreground">
        {server.goVersion} · client port {server.clientPort} · {server.authRequired ? "authentication required" : "no authentication"}
        {server.configLoadedAt && <> · config loaded {dateTime.format(new Date(server.configLoadedAt))}</>}
      </p>
    </section>
  );
}

function JetStreamSection({ jetstream }: { jetstream: NatsJetStreamInfo }) {
  return (
    <section aria-labelledby="nats-jetstream-title" className="space-y-3">
      <h2 id="nats-jetstream-title" className="text-base font-semibold">JetStream</h2>
      <div className="grid gap-3 md:grid-cols-2">
        <UsageMeter
          name="File store"
          used={jetstream.fileUsedBytes}
          limit={jetstream.fileLimitBytes}
          note={`${formatBytes(jetstream.fileReservedBytes)} reserved by stream size caps · ${jetstream.storeDir}`}
        />
        <UsageMeter
          name="Memory store"
          used={jetstream.memoryUsedBytes}
          limit={jetstream.memoryLimitBytes}
          note={`${formatBytes(jetstream.memoryReservedBytes)} reserved by stream size caps`}
        />
      </div>
      <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatTile name="Streams" value={formatCount(jetstream.streams)} hint={`${formatCount(jetstream.accounts)} ${jetstream.accounts === 1 ? "account" : "accounts"}`} />
        <StatTile name="Consumers" value={formatCount(jetstream.consumers)} />
        <StatTile name="Messages stored" value={formatCount(jetstream.messages)} hint={formatBytes(jetstream.bytes)} />
        <StatTile name="API requests" value={formatCount(jetstream.apiRequests)} hint={`${formatCount(jetstream.apiErrors)} ${jetstream.apiErrors === 1 ? "error" : "errors"} since start`} />
      </dl>
    </section>
  );
}

function Moment({ at, checkedAt }: { at: string | null; checkedAt: number }) {
  if (at === null) return <span className="text-muted-foreground">Never</span>;
  return <span title={dateTime.format(new Date(at))}>{formatAge(at, checkedAt)}</span>;
}

function ConsumerTable({ consumers, checkedAt }: { consumers: NatsConsumer[]; checkedAt: number }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Consumer</TableHead>
          <TableHead>Mode</TableHead>
          <TableHead className="text-right" title="Matching messages not yet delivered to anyone: the backlog">Unprocessed</TableHead>
          <TableHead className="text-right" title="Delivered, acknowledgement still outstanding">In flight</TableHead>
          <TableHead className="text-right" title="Delivered again after a worker failed or timed out">Redelivered</TableHead>
          <TableHead className="text-right" title="Pull requests parked on the server: idle workers waiting for messages">Waiting pulls</TableHead>
          <TableHead className="text-right" title="Every stream sequence up to this one is acknowledged">Ack floor</TableHead>
          <TableHead className="text-right">Ack wait</TableHead>
          <TableHead>Last delivery</TableHead>
          <TableHead>Last ack</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {consumers.map((consumer) => (
          <TableRow key={consumer.name}>
            <TableCell className="font-medium">
              {consumer.name}
              {!consumer.durable && <span className="ml-2 text-xs font-normal text-muted-foreground">ephemeral</span>}
              {consumer.filterSubjects.length > 0 && <div className="font-mono text-xs font-normal text-muted-foreground">{consumer.filterSubjects.join(", ")}</div>}
            </TableCell>
            <TableCell>{consumer.mode === "pull" ? "Pull" : "Push"} · {consumer.ackPolicy} ack</TableCell>
            <TableCell className={cn("text-right tabular-nums", consumer.unprocessed > 0 && "font-semibold")}>{formatCount(consumer.unprocessed)}</TableCell>
            <TableCell className="text-right tabular-nums">{formatCount(consumer.inFlight)}</TableCell>
            <TableCell className="text-right tabular-nums">
              {consumer.redelivered > 0 ? (
                <span className="inline-flex items-center justify-end gap-1 font-semibold">
                  <TriangleAlertIcon className="size-3.5 text-amber-600 dark:text-amber-400" aria-label="Redeliveries" />
                  {formatCount(consumer.redelivered)}
                </span>
              ) : formatCount(consumer.redelivered)}
            </TableCell>
            <TableCell className="text-right tabular-nums">{formatCount(consumer.waitingPulls)}</TableCell>
            <TableCell className="text-right tabular-nums">{formatCount(consumer.ackFloorSequence)}</TableCell>
            <TableCell className="text-right tabular-nums">{consumer.ackWaitNanos === null ? "—" : formatNanos(consumer.ackWaitNanos)}</TableCell>
            <TableCell><Moment at={consumer.lastDeliveredAt} checkedAt={checkedAt} /></TableCell>
            <TableCell><Moment at={consumer.lastAckedAt} checkedAt={checkedAt} /></TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function Fact({ name, children }: { name: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <dt className="text-xs text-muted-foreground">{name}</dt>
      <dd className="text-sm font-medium tabular-nums">{children}</dd>
    </div>
  );
}

function StreamBlock({ stream, checkedAt }: { stream: NatsStream; checkedAt: number }) {
  const backlog = streamBacklog(stream.consumers);
  const sizePercent = usagePercent(stream.bytes, stream.maxBytes);
  const unlimited = <span className="font-normal text-muted-foreground">unlimited</span>;
  return (
    <article id={`stream-${stream.name}`} aria-labelledby={`stream-${stream.name}-title`} className="space-y-4 rounded-lg border p-4">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <h3 id={`stream-${stream.name}-title`} className="font-mono text-sm font-semibold">{stream.name}</h3>
          <Badge variant="secondary">{label(STORAGE_LABELS, stream.storage)}</Badge>
          <Badge variant="secondary">{label(RETENTION_LABELS, stream.retention)}</Badge>
          <Badge variant="outline">{stream.account}</Badge>
          {stream.maxBytes === null && (
            <Badge variant="outline" className="border-amber-500/50" title="Without max_bytes this stream can grow until the whole JetStream store is full">
              <TriangleAlertIcon className="text-amber-600 dark:text-amber-400" /> No size cap
            </Badge>
          )}
        </div>
        <p className="flex flex-wrap gap-1.5">
          {stream.subjects.map((subject) => (
            <code key={subject} className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">{subject}</code>
          ))}
        </p>
      </header>

      <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3 lg:grid-cols-6">
        <Fact name="Messages">{formatCount(stream.messages)}</Fact>
        <Fact name="Size">
          {formatBytes(stream.bytes)}
          {stream.maxBytes !== null && <span className="font-normal text-muted-foreground"> of {formatBytes(stream.maxBytes)} · {sizePercent}%</span>}
        </Fact>
        <Fact name="Backlog">{backlog === null ? <span className="font-normal text-muted-foreground">No consumers</span> : formatCount(backlog)}</Fact>
        <Fact name="Sequence">{formatCount(stream.firstSequence)} – {formatCount(stream.lastSequence)}</Fact>
        <Fact name="Distinct subjects">{formatCount(stream.subjectCount)}</Fact>
        <Fact name="Last message"><Moment at={stream.lastMessageAt} checkedAt={checkedAt} /></Fact>
      </dl>

      <dl className="grid grid-cols-2 gap-x-6 gap-y-3 border-t pt-4 sm:grid-cols-3 lg:grid-cols-6">
        <Fact name="Max age">{stream.maxAgeNanos === null ? unlimited : formatNanos(stream.maxAgeNanos)}</Fact>
        <Fact name="Max messages">{stream.maxMessages === null ? unlimited : formatCount(stream.maxMessages)}</Fact>
        <Fact name="Max per subject">{stream.maxMessagesPerSubject === null ? unlimited : formatCount(stream.maxMessagesPerSubject)}</Fact>
        <Fact name="Max message size">{stream.maxMessageBytes === null ? unlimited : formatBytes(stream.maxMessageBytes)}</Fact>
        <Fact name="When full">{label(DISCARD_LABELS, stream.discard)}</Fact>
        <Fact name="Duplicate window">{stream.duplicateWindowNanos === null ? "off" : formatNanos(stream.duplicateWindowNanos)}</Fact>
        <Fact name="Compression">{stream.compression}</Fact>
        <Fact name="Replicas">{stream.replicas}</Fact>
        <Fact name="Max consumers">{stream.maxConsumers === null ? unlimited : formatCount(stream.maxConsumers)}</Fact>
        <Fact name="Oldest message"><Moment at={stream.firstMessageAt} checkedAt={checkedAt} /></Fact>
        <Fact name="Created"><span title={stream.createdAt}>{dateTime.format(new Date(stream.createdAt))}</span></Fact>
      </dl>

      {stream.consumers.length > 0 && <ConsumerTable consumers={stream.consumers} checkedAt={checkedAt} />}
    </article>
  );
}

export function NatsMonitorView({
  result,
  refreshing,
  onRefresh,
}: {
  result: NatsMonitorResult;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  const { snapshot, error } = result;
  return (
    <div className="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-8 p-4 md:p-6">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="space-y-2">
          <h1 className="text-2xl font-semibold tracking-tight">NATS</h1>
          <p className="text-sm text-muted-foreground">JetStream server status, streams, and how far each consumer has processed.</p>
          {snapshot && (
            <p className="flex flex-wrap items-center gap-2 text-sm">
              <Badge variant="secondary" className="bg-emerald-500/10 text-emerald-700 dark:text-emerald-400"><CircleCheckIcon /> Online</Badge>
              <span className="font-medium">{snapshot.server.name}</span>
              <span className="text-muted-foreground">nats-server {snapshot.server.version}</span>
              <span className="font-mono text-xs text-muted-foreground">{snapshot.monitorUrl}</span>
            </p>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          <Button variant="outline" size="sm" disabled={refreshing} onClick={onRefresh}>
            {refreshing ? <LoaderCircleIcon className="motion-safe:animate-spin" /> : <RefreshCwIcon />} Refresh
          </Button>
          <span className="text-xs text-muted-foreground">
            {snapshot ? `Checked ${clock.format(snapshot.checkedAt * 1000)} · ` : ""}auto-refresh
          </span>
        </div>
      </header>

      {error && (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertTitle>{error.kind === "not_configured" ? "NATS monitoring is not configured" : "NATS server unreachable"}</AlertTitle>
          <AlertDescription className="break-words">{error.message}</AlertDescription>
        </Alert>
      )}

      {snapshot && <ServerSection server={snapshot.server} />}

      {snapshot && (snapshot.jetstream ? (
        <>
          <JetStreamSection jetstream={snapshot.jetstream} />
          <section aria-labelledby="nats-streams-title" className="space-y-3">
            <h2 id="nats-streams-title" className="flex items-center gap-2 text-base font-semibold">
              Streams <Badge variant="secondary">{snapshot.streams.length}</Badge>
            </h2>
            {snapshot.streams.length > 0 ? (
              snapshot.streams.map((stream) => <StreamBlock key={`${stream.account}/${stream.name}`} stream={stream} checkedAt={snapshot.checkedAt} />)
            ) : (
              <Empty className="border">
                <EmptyHeader>
                  <EmptyTitle>No streams yet</EmptyTitle>
                  <EmptyDescription>Streams appear here as soon as a client creates one. Messages published before a stream covers their subject are not stored.</EmptyDescription>
                </EmptyHeader>
              </Empty>
            )}
          </section>
        </>
      ) : (
        <Alert>
          <CircleAlertIcon />
          <AlertTitle>JetStream is not enabled</AlertTitle>
          <AlertDescription>This server answers, but runs as core NATS only: nothing published to it is stored.</AlertDescription>
        </Alert>
      ))}
    </div>
  );
}
