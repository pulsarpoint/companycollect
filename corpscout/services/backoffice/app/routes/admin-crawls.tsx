import { useEffect, useRef, useState } from "react";
import { Form, Link, useFetcher, useRevalidator, useSearchParams } from "react-router";
import { MonitorIcon, RefreshCwIcon } from "lucide-react";
import { toast } from "sonner";
import type { Route } from "./+types/admin-crawls";
import { crawlAction, loadCrawls } from "~/lib/crawler.server";
import { publishTestCrawl } from "~/lib/crawl-submit.server";
import { runChallengeAgent } from "~/lib/challenge-agent.server";
import { CRAWL_STATES, isCrawlWaiting, type CrawlAttempt, type CrawlPublishReceipt } from "~/lib/crawler";
import { CrawlBrowser } from "~/components/admin/crawl-browser";
import { CrawlSubmit } from "~/components/admin/crawl-submit";
import { CrawlProgress } from "~/components/admin/crawl-progress";
import { loadCrawlProgress } from "~/lib/crawl-progress.server";
import { CrawlInputs } from "~/components/admin/crawl-inputs";
import { loadCrawlInputs, startSavedCrawls } from "~/lib/crawl-inputs.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Toaster } from "~/components/ui/sonner";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export async function loader({request}: Route.LoaderArgs) {
  const submissionEnabled = process.env.CRAWLER_TEST_SUBMIT_ENABLED === "true";
  const search = new URL(request.url).searchParams;
  const inputsPromise = loadCrawlInputs(search);
  const [crawls, inputs, progress] = await Promise.allSettled([
    loadCrawls(search), inputsPromise,
    inputsPromise.then(inputs => loadCrawlProgress(inputs.type)),
  ]);
  return {
    snapshot: crawls.status === "fulfilled" ? crawls.value : null,
    error: crawls.status === "rejected" ? (crawls.reason instanceof Error ? crawls.reason.message : "Crawler unavailable") : null,
    inputs: inputs.status === "fulfilled" ? inputs.value : null,
    inputsError: inputs.status === "rejected" ? "Could not load crawl inputs from ClickHouse." : null,
    progress: progress.status === "fulfilled" ? progress.value : null,
    progressError: progress.status === "rejected" ? "Could not load processing status. Refresh to try again." : null,
    submissionEnabled,
  };
}

export async function action({request}: Route.ActionArgs) {
  if (request.headers.get("Origin") !== new URL(request.url).origin) {
    return {error: "Cross-origin crawler controls are not allowed."};
  }
  const form = await request.formData();
  const intent = String(form.get("intent") || "");
  if (intent === "start-inputs") {
    try { return {error: null, intent, batch: await startSavedCrawls(form)}; }
    catch (error) { return {error: error instanceof Error ? error.message : "Starting saved crawls failed."}; }
  }
  if (intent === "submit") {
    try { return {error: null, intent, receipt: await publishTestCrawl(String(form.get("body") || ""))}; }
    catch (error) { return {error: error instanceof Error ? error.message : "Crawl submission failed."}; }
  }
  const id = String(form.get("request_id") || "");
  if (!["retry", "verify-search", "resume", "cancel", "browser-ticket", "challenge-agent"].includes(intent) || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(id)) {
    return {error: "Invalid crawler action."};
  }
  try {
    if (intent === "challenge-agent") return {error: null, intent, requestId: id, agent: await runChallengeAgent(id)};
    const result = await crawlAction(id, intent as "retry" | "verify-search" | "resume" | "cancel" | "browser-ticket",
      intent === "retry" ? {attempt: Number(form.get("attempt")), request_id: String(form.get("retry_id")),
        ...(form.has("interactive") ? {interactive: form.get("interactive") !== "false"} : {}),
        ...(form.get("challenge_agent_max_runs") ? {challenge_agent_max_runs: Number(form.get("challenge_agent_max_runs"))} : {}),
        ...(form.get("challenge_agent_model") ? {challenge_agent_model: String(form.get("challenge_agent_model"))} : {}),
      } : undefined);
    return {error: null, intent, result};
  } catch (error) { return {error: error instanceof Error ? error.message : "Crawler action failed."}; }
}

export function meta() { return [{title: "Crawler | CompanyCollect admin"}]; }

export default function AdminCrawls({loaderData}: Route.ComponentProps) {
  const {snapshot, error, submissionEnabled} = loaderData;
  const [search, setSearch] = useSearchParams();
  const {revalidate, state: refreshState} = useRevalidator();
  const fetcher = useFetcher<typeof action>();
  const agentFetcher = useFetcher<typeof action>();
  const [selected, setSelected] = useState<CrawlAttempt | null>(null);
  const [browserUrl, setBrowserUrl] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [submission, setSubmission] = useState<CrawlPublishReceipt | null>(null);
  const initialRevision = useRef(snapshot?.revision ?? 0);
  const openedSession = useRef<string | null>(null);
  const handled = useRef<unknown>(null);
  const busy = fetcher.state !== "idle";
  const agentBusy = agentFetcher.state !== "idle";
  const agentData = agentFetcher.data;
  const agentResult = agentData && "agent" in agentData && agentData.requestId === selected?.request_id ? agentData.agent : null;

  useEffect(() => {
    const timer = setInterval(() => {
      if (document.visibilityState === "visible" && refreshState === "idle") void revalidate();
    }, 5000);
    return () => clearInterval(timer);
  }, [revalidate, refreshState]);

  function command(intent: string, attempt: CrawlAttempt, options: Record<string, string> = {}) {
    fetcher.submit({intent, request_id: attempt.request_id, attempt: String(attempt.attempt), retry_id: `manual-${crypto.randomUUID()}`, ...options}, {method: "post"});
  }

  useEffect(() => {
    const stream = new EventSource(`/admin/crawls/events?after=${initialRevision.current}`);
    let refreshTimer: ReturnType<typeof setTimeout> | undefined;
    stream.onopen = () => {setLive(true); void revalidate();};
    stream.onerror = () => setLive(false);
    stream.addEventListener("crawl-status", (event) => {
      const job = JSON.parse((event as MessageEvent).data) as CrawlAttempt;
      setSelected((current) => current?.request_id === job.request_id ? job : current);
      if (job.challenge_agent_running) toast.info(`${job.domain}: CAPTCHA agent running`, {id: `${job.request_id}-attention`, description: job.reason || undefined});
      else if (isCrawlWaiting(job)) toast.warning(`${job.domain}: ${job.state.replaceAll("_", " ")}`, {id: `${job.request_id}-attention`, description: job.reason || "Human assistance is needed."});
      else toast.dismiss(`${job.request_id}-attention`);
      if (!refreshTimer) refreshTimer = setTimeout(() => {refreshTimer = undefined; void revalidate();}, 300);
    });
    return () => {stream.close(); clearTimeout(refreshTimer);};
  }, [revalidate]);

  useEffect(() => {
    if (!fetcher.data || fetcher.data === handled.current) return;
    handled.current = fetcher.data;
    if (fetcher.data.error) {toast.error(fetcher.data.error); return;}
    if (!("result" in fetcher.data) || !fetcher.data.result) return;
    if ("browserUrl" in fetcher.data.result) setBrowserUrl(fetcher.data.result.browserUrl);
    if (fetcher.data.intent === "retry" && "request_id" in fetcher.data.result) {
      setSelected(fetcher.data.result as CrawlAttempt);
      setBrowserUrl(null);
      openedSession.current = null;
      const retry = fetcher.data.result as CrawlAttempt;
      toast.success(retry.interactive === false ? `Agent retry queued with ${retry.challenge_agent_max_runs} runs.` : "Interactive retry started in the manual queue.");
    }
  }, [fetcher.data]);

  useEffect(() => {
    if (selected?.browser_available && isCrawlWaiting(selected) && openedSession.current !== selected.browser_session_id && !busy) {
      openedSession.current = selected.browser_session_id;
      command("browser-ticket", selected);
    }
  }, [selected, busy]);

  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <Toaster />
    <div className="flex items-start justify-between gap-4">
      <div><h1 className="text-2xl font-semibold">Crawler</h1>
        <p className="mt-1 text-sm text-muted-foreground">Choose a crawl type, activate saved inputs, and follow processing.</p></div>
      <div className="flex items-center gap-2"><Badge variant={live ? "secondary" : "outline"}>{live ? "Live" : "Reconnecting"}</Badge>
        <CrawlSubmit enabled={submissionEnabled} onPublished={receipt => {
          setSubmission(receipt);
          setSearch({domain: new URL(receipt.url).hostname.replace(/^www\./, ""), source: "rest"});
          void revalidate();
        }} />
        <Button variant="outline" onClick={() => void revalidate()}><RefreshCwIcon data-icon="inline-start" />Refresh</Button></div>
    </div>
    {loaderData.inputsError && <Alert variant="destructive"><AlertTitle>Crawl inputs unavailable</AlertTitle><AlertDescription>{loaderData.inputsError}</AlertDescription></Alert>}
    {loaderData.inputs && <CrawlInputs key={`${loaderData.inputs.type}:${loaderData.inputs.domain}:${loaderData.inputs.offset}`} snapshot={loaderData.inputs}><CrawlProgress snapshot={loaderData.progress} error={loaderData.progressError} /></CrawlInputs>}
    <div className="border-t pt-6" id="crawl-attempts"><h2 className="text-lg font-semibold">Crawl attempts</h2><p className="text-sm text-muted-foreground">All crawl types and sources · Live requests, saved results, and failures available for retry.</p></div>
    {submission && <Alert><AlertTitle>Crawler accepted the request</AlertTitle>
      <AlertDescription><p>{submission.request_id} · {submission.state.replaceAll("_", " ")}</p>
        <p>The request is stored in the crawler queue. Its live status appears below.</p></AlertDescription>
    </Alert>}
    {error && <Alert variant="destructive"><AlertTitle>Crawler unavailable</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
    <Form method="get" key={search.toString()}>
      {[...search.entries()].filter(([key]) => key.startsWith("input_")).map(([key,value]) => <input key={key} type="hidden" name={key} value={value} />)}
      <FieldGroup className="flex-row flex-wrap items-end">
        <Field className="w-64"><FieldLabel htmlFor="crawl-domain">Domain</FieldLabel><Input id="crawl-domain" name="domain" defaultValue={search.get("domain") || ""} placeholder="e.g. melexis.com" /></Field>
        <Field className="w-44"><FieldLabel htmlFor="crawl-state">Status</FieldLabel><NativeSelect id="crawl-state" name="state" defaultValue={search.get("state") || ""}><NativeSelectOption value="">All statuses</NativeSelectOption>{CRAWL_STATES.map(state => <NativeSelectOption key={state} value={state}>{state.replaceAll("_", " ")}</NativeSelectOption>)}</NativeSelect></Field>
        <Field className="w-40"><FieldLabel htmlFor="crawl-source">Input</FieldLabel><NativeSelect id="crawl-source" name="source" defaultValue={search.get("source") || ""}><NativeSelectOption value="">All inputs</NativeSelectOption><NativeSelectOption value="jetstream">JetStream (history)</NativeSelectOption><NativeSelectOption value="rest">REST</NativeSelectOption><NativeSelectOption value="manual">Manual retry</NativeSelectOption></NativeSelect></Field>
        <Button type="submit" variant="outline">Apply filters</Button>
      </FieldGroup>
    </Form>
    {selected && <CrawlBrowser attempt={selected} browserUrl={browserUrl} busy={busy}
      agentBusy={agentBusy} agentResult={agentResult} agentError={agentData?.error ?? null}
      onAgent={() => agentFetcher.submit({intent: "challenge-agent", request_id: selected.request_id}, {method: "post"})}
      onVerify={() => command("verify-search", selected)} onResume={() => command("resume", selected)} onCancel={() => command("cancel", selected)}
      onReconnect={() => command("browser-ticket", selected)} />}
    {snapshot && snapshot.attempts.length === 0 ? <Empty><EmptyHeader><EmptyTitle>No matching crawl attempts</EmptyTitle><EmptyDescription>Failed requests remain here for later review and retry.</EmptyDescription></EmptyHeader></Empty> : snapshot && <>
      <Table><TableHeader><TableRow><TableHead>Website / attempt</TableHead><TableHead>Status</TableHead><TableHead>Input</TableHead><TableHead>Reason</TableHead><TableHead>Archive</TableHead><TableHead>Updated</TableHead><TableHead>Action</TableHead></TableRow></TableHeader>
        <TableBody>{snapshot.attempts.map(job => {
          const collectedPages = job.collected_pages || job.s3_event?.page_count || 0;
          return <TableRow key={`${job.request_id}:${job.attempt}`}>
          <TableCell><div className="font-medium">{job.domain}</div><div className="max-w-56 truncate text-xs text-muted-foreground" title={job.request_id}>{job.request_id} · #{job.attempt}</div>{job.retry_of && <div className="text-xs text-muted-foreground">Retry of {job.retry_of} · #{job.retry_of_attempt}</div>}</TableCell>
          <TableCell><Badge variant={!job.challenge_agent_running && ["failed", "blocked", "captcha"].includes(job.state) ? "destructive" : "secondary"}>{job.challenge_agent_running ? "CAPTCHA agent running" : job.state === "failed" && collectedPages > 0 ? "partial · stopped" : job.state.replaceAll("_", " ")}</Badge>{job.challenge_agent_result && <Button size="sm" variant="link" onClick={() => {setSelected(job); setBrowserUrl(null); openedSession.current = null;}}>Agent result</Button>}</TableCell>
          <TableCell>{job.source}</TableCell><TableCell className="max-w-80 whitespace-normal text-sm">{job.reason || job.error || job.blocked_reason || "—"}{collectedPages > 0 && <div>{collectedPages} pages saved</div>}{job.state === "failed" && job.blocked_reason && job.current_url && <div className="break-all">Stopped at {job.current_url}</div>}</TableCell>
          <TableCell title={job.s3_event?.result ? `s3://${job.s3_event.result.bucket}/${job.s3_event.result.key}` : job.s3_error || ""}>{job.s3_state === "uploaded" && job.s3_event?.result ? <Button size="sm" variant="link" render={<Link to={`/admin/crawls/results?${new URLSearchParams({path: `${job.s3_event.result.bucket}/${job.s3_event.result.key}`})}`} />}>S3 saved · View</Button> : job.s3_state === "uploaded" ? "S3 saved" : job.s3_state === "pending" ? "Upload pending" : "Local only"}</TableCell>
          <TableCell className="whitespace-nowrap text-xs">{new Date(job.updated_at).toLocaleString()}</TableCell>
          <TableCell>{job.state === "failed" ? <div className="flex flex-col items-start gap-2">
            <fetcher.Form method="post" className="flex flex-col gap-2" onSubmit={event => {
              event.preventDefault();
              const form = new FormData(event.currentTarget);
              command("retry", job, {interactive: "false", challenge_agent_max_runs: String(form.get("challenge_agent_max_runs") || ""), challenge_agent_model: String(form.get("challenge_agent_model") || "")});
            }}>
              <Button type="submit" size="sm" disabled={busy || !snapshot.human_enabled || snapshot.challenge_agent_enabled === false}>Retry with agent</Button>
              <details className="text-xs"><summary>Retry options</summary><FieldGroup className="mt-2 w-52 gap-2">
                <Field><FieldLabel htmlFor={`budget-${job.request_id}-${job.attempt}`}>Agent run budget</FieldLabel>
                  <Input id={`budget-${job.request_id}-${job.attempt}`} name="challenge_agent_max_runs" type="number" min={3} max={1000} step={1} placeholder="Automatic" disabled={busy} />
                  <p className="whitespace-normal text-muted-foreground">Leave blank to double after exhaustion, up to 1000 runs.</p>
                </Field>
                <Field><FieldLabel htmlFor={`model-${job.request_id}-${job.attempt}`}>Agent model</FieldLabel>
                  <NativeSelect id={`model-${job.request_id}-${job.attempt}`} name="challenge_agent_model" defaultValue="" disabled={busy}>
                    <NativeSelectOption value="">Same as previous attempt</NativeSelectOption>
                    <NativeSelectOption value="deepseek-flash">DeepSeek V4.1 Flash</NativeSelectOption>
                    <NativeSelectOption value="z-ai/glm-5.3-flash">GLM-5.3 Flash</NativeSelectOption>
                  </NativeSelect>
                </Field>
              </FieldGroup></details>
            </fetcher.Form>
            <Button size="sm" variant="outline" disabled={busy || !snapshot.human_enabled} onClick={() => command("retry", job)}>Retry interactively</Button>
          </div> : job.verification_available ? <Button size="sm" variant="outline" disabled={busy} onClick={() => {setSelected(job); setBrowserUrl(null); openedSession.current = null; command("verify-search", job);}}>Start verification</Button> : job.browser_available ? <Button size="sm" variant="outline" onClick={() => {setSelected(job); setBrowserUrl(null); openedSession.current = null;}}><MonitorIcon data-icon="inline-start" />Open browser</Button> : "—"}</TableCell>
        </TableRow>;})}</TableBody></Table>
      <div className="flex items-center justify-between"><p className="text-sm text-muted-foreground">{snapshot.total} matching attempts · Failures are retained in the crawler’s SQLite history.</p><div className="flex gap-2">
        <Button variant="outline" disabled={snapshot.offset === 0} onClick={() => {const next = new URLSearchParams(search); next.set("offset", String(Math.max(0, snapshot.offset - snapshot.limit))); setSearch(next);}}>Previous</Button>
        <Button variant="outline" disabled={snapshot.offset + snapshot.limit >= snapshot.total} onClick={() => {const next = new URLSearchParams(search); next.set("offset", String(snapshot.offset + snapshot.limit)); setSearch(next);}}>Next</Button>
      </div></div>
    </>}
  </div>;
}
