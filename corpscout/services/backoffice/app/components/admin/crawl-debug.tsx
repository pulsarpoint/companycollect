import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { DownloadIcon, XIcon } from "lucide-react";
import { debugTime, type CrawlDebugEvent, type CrawlDebugSnapshot } from "~/lib/crawl-debug";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export function CrawlDebugDetails({event, details}: {event: CrawlDebugEvent | undefined; details: unknown}) {
  return <div className="flex min-w-0 flex-col gap-2">
    <h3 className="font-medium">{event ? `#${event.id} · ${event.message}` : "Event details"}</h3>
    {event && <p className="text-xs text-muted-foreground">{event.timestamp} · +{debugTime(event.elapsed_ms)}{event.duration_ms !== null && ` · Duration ${debugTime(event.duration_ms)}`}{event.operation && ` · ${event.operation}`}</p>}
    <pre className="max-h-[36rem] overflow-auto rounded-md border bg-muted/30 p-3 text-xs whitespace-pre-wrap break-words">{details === undefined ? "Select an event to inspect its complete saved data." : JSON.stringify(details, null, 2)}</pre>
  </div>;
}

export function CrawlDebug({requestId, attempt}: {requestId: string; attempt: string}) {
  const [search, setSearch] = useSearchParams();
  const selectedId = Number(search.get("debug_event")) || null;
  const [snapshot, setSnapshot] = useState<CrawlDebugSnapshot | null>(null);
  const [events, setEvents] = useState<CrawlDebugEvent[]>([]);
  const [error, setError] = useState("");
  const [details, setDetails] = useState<unknown>(undefined);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [filter, setFilter] = useState("");
  const [level, setLevel] = useState("");
  const [stage, setStage] = useState("");
  const [follow, setFollow] = useState(selectedId === null);
  const [visibleCount, setVisibleCount] = useState(200);
  const [now, setNow] = useState(Date.now());
  const viewport = useRef<HTMLDivElement>(null);
  const base = `/admin/crawls/debug?${new URLSearchParams({request: requestId, ...(attempt ? {attempt} : {})})}`;

  useEffect(() => {
    let currentAttempt = Number(attempt) || 0;
    const stream = new EventSource(`${base}&stream=1`);
    function receive(event: MessageEvent) {
      const next = JSON.parse(event.data) as CrawlDebugSnapshot;
      if (currentAttempt !== next.attempt) setEvents(next.events);
      else setEvents(previous => [...previous, ...next.events.filter(item => !previous.some(saved => saved.id === item.id))]);
      currentAttempt = next.attempt;
      setSnapshot(next); setError(""); setNow(Date.now());
      if (event.type === "crawl-debug-complete") stream.close();
    }
    stream.addEventListener("crawl-debug", receive as EventListener);
    stream.addEventListener("crawl-debug-complete", receive as EventListener);
    stream.onerror = () => setError("Live connection interrupted. Reconnecting from the last saved event.");
    return () => stream.close();
  }, [base, attempt]);

  useEffect(() => {
    if (selectedId === null) {setDetails(undefined); setDetailError(""); setDetailLoading(false); return;}
    const controller = new AbortController();
    setDetailLoading(true); setDetails(undefined); setDetailError("");
    fetch(`${base}&event=${selectedId}`, {signal: controller.signal, cache: "no-store"})
      .then(async response => {const data = await response.json(); if (!response.ok) throw new Error(data.error || "Could not load event details."); return data;})
      .then(data => {if (!controller.signal.aborted) setDetails(data);})
      .catch(failure => {if (!controller.signal.aborted) setDetailError(failure instanceof Error ? failure.message : "Could not load event details.");})
      .finally(() => {if (!controller.signal.aborted) setDetailLoading(false);});
    return () => controller.abort();
  }, [base, selectedId]);

  useEffect(() => {if (follow && viewport.current) viewport.current.scrollTop = viewport.current.scrollHeight;}, [events, follow]);
  const matching = events.filter(event => (!level || event.level === level) && (!stage || event.stage === stage) && `${event.message} ${event.stage}`.toLowerCase().includes(filter.toLowerCase()));
  const selected = events.find(event => event.id === selectedId);
  const job = snapshot?.job;
  const active = job && !["completed", "failed", "cancelled"].includes(job.state);
  const elapsed = job?.started_at ? (job.finished_at ? Date.parse(job.finished_at) : now) - Date.parse(job.started_at) : 0;
  const latestProgress = [...events].reverse().find(event => event.stage === "progress");
  return <section className="flex min-w-0 flex-col gap-4 rounded-lg border p-4" aria-label="Crawl debug trace">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div className="flex min-w-0 flex-col gap-1"><h2 className="text-lg font-semibold">Crawl debug trace {job?.domain && `· ${job.domain}`}</h2>
        <p className="break-all text-xs text-muted-foreground">{requestId} · Attempt {snapshot?.attempt || "waiting"}</p>
        <div className="flex flex-wrap items-center gap-2"><Badge variant={job?.state === "failed" ? "destructive" : "secondary"}>{job?.state || "Loading"}</Badge><span className="text-sm">{debugTime(Math.max(0, elapsed, events.at(-1)?.elapsed_ms || 0))} elapsed · {events.length} events{active ? " · Live stream" : ""}</span></div>
        {latestProgress && <p className="text-sm">{latestProgress.message}</p>}
      </div>
      <div className="flex items-center gap-2"><a className={buttonVariants({variant: "outline", size: "sm"})} href={`${base}&download=1`}><DownloadIcon data-icon="inline-start" />Download full trace</a>
        <Button size="icon-sm" variant="ghost" aria-label="Close debug trace" onClick={() => {const next = new URLSearchParams(search); for (const key of ["debug_request", "debug_attempt", "debug_event"]) next.delete(key); setSearch(next);}}><XIcon /></Button></div>
    </div>
    {error && <Alert variant="destructive"><AlertTitle>Debug trace unavailable</AlertTitle><AlertDescription>{error} Retrying automatically.</AlertDescription></Alert>}
    {snapshot && !snapshot.enabled && <Alert><AlertTitle>No debug trace recorded</AlertTitle><AlertDescription>This attempt predates debug recording or was submitted without it. New crawls from the Crawl tab record a trace automatically.</AlertDescription></Alert>}
    <FieldGroup className="flex-row flex-wrap items-end gap-3">
      <Field className="w-64"><FieldLabel htmlFor="trace-search">Search events</FieldLabel><Input id="trace-search" value={filter} onChange={event => setFilter(event.target.value)} placeholder="URL, model call, decision…" /></Field>
      <Field className="w-40"><FieldLabel htmlFor="trace-stage">Stage</FieldLabel><NativeSelect id="trace-stage" value={stage} onChange={event => setStage(event.target.value)}><NativeSelectOption value="">All stages</NativeSelectOption>{[...new Set(events.map(event => event.stage))].sort().map(value => <NativeSelectOption key={value} value={value}>{value.replaceAll("_", " ")}</NativeSelectOption>)}</NativeSelect></Field>
      <Field className="w-36"><FieldLabel htmlFor="trace-level">Level</FieldLabel><NativeSelect id="trace-level" value={level} onChange={event => setLevel(event.target.value)}><NativeSelectOption value="">All levels</NativeSelectOption>{["debug", "info", "warning", "error"].map(value => <NativeSelectOption key={value} value={value}>{value}</NativeSelectOption>)}</NativeSelect></Field>
      <Button variant="outline" size="sm" aria-pressed={follow} onClick={() => setFollow(!follow)}>{follow ? "Following latest" : "Follow latest"}</Button>
    </FieldGroup>
    <div className="grid min-w-0 gap-4 xl:grid-cols-2">
      <div className="flex min-w-0 flex-col gap-2">
        {matching.length > visibleCount && <Button variant="outline" size="sm" onClick={() => {setVisibleCount(visibleCount + 200); setFollow(false);}}>Show 200 earlier events ({matching.length - visibleCount} hidden)</Button>}
        <div ref={viewport} className="max-h-[36rem] overflow-auto rounded-md border">
          <Table><TableHeader><TableRow><TableHead>Time / elapsed</TableHead><TableHead>Stage</TableHead><TableHead>Event</TableHead></TableRow></TableHeader>
            <TableBody>{matching.slice(-visibleCount).map(event => <TableRow key={event.id} data-state={event.id === selectedId ? "selected" : undefined}>
              <TableCell className="align-top text-xs whitespace-nowrap"><time dateTime={event.timestamp}>{event.timestamp.slice(11, 23)} UTC</time><div className="text-muted-foreground">+{debugTime(event.elapsed_ms)}</div>{event.duration_ms !== null && <div>{debugTime(event.duration_ms)} duration</div>}</TableCell>
              <TableCell className="align-top"><Badge variant={event.level === "error" ? "destructive" : "outline"}>{event.level}</Badge><div className="mt-1 text-xs text-muted-foreground">{event.stage.replaceAll("_", " ")}</div></TableCell>
              <TableCell className="whitespace-normal align-top"><Button variant="link" size="sm" className="h-auto max-w-full justify-start whitespace-normal text-left" onClick={() => {setFollow(false); const next = new URLSearchParams(search); next.set("debug_event", String(event.id)); if (snapshot?.attempt) next.set("debug_attempt", String(snapshot.attempt)); setSearch(next, {preventScrollReset: true});}}>#{event.id} {event.message}</Button></TableCell>
            </TableRow>)}</TableBody></Table>
          {!matching.length && <p className="p-4 text-sm text-muted-foreground">{events.length ? "No events match these filters." : active ? "Waiting for the crawler to start producing events…" : "No events recorded."}</p>}
        </div><p className="text-xs text-muted-foreground">{matching.length} matching events. Request headers are omitted; credentials are redacted before storage. Download includes every event and its full details.</p>
      </div>
      <div className="min-w-0" aria-live="polite">{detailLoading ? <p className="text-sm text-muted-foreground">Loading event details…</p> : detailError ? <Alert variant="destructive"><AlertTitle>Could not load event</AlertTitle><AlertDescription>{detailError}</AlertDescription></Alert> : <CrawlDebugDetails event={selected} details={details} />}</div>
    </div>
  </section>;
}
