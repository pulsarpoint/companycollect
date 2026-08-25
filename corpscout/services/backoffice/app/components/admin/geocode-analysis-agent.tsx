/**
 * The Geocoding tab's agent panel: trigger, run history, the latest report,
 * and the suggestion board with its accept/reject lifecycle.
 *
 * Client-safe by construction -- every type here comes from
 * ~/agents/geocode-analysis-contract, never from a `.server` module (CLAUDE.md:
 * a value imported from a server module would be bundled for the browser and
 * break hydration).
 *
 * Fire-and-poll, the shape people-draft-initializer.tsx already uses on this
 * codebase: the form POSTs to the tab's own action, which returns as soon as
 * the run row exists, and a second fetcher polls the resource route while the
 * run is live. Turns are model-latency apart, so the poll is seconds, not
 * milliseconds.
 */
import { useEffect, useRef, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import {
  CheckIcon,
  LoaderCircleIcon,
  RocketIcon,
  SparklesIcon,
  TriangleAlertIcon,
  XIcon,
} from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Textarea } from "~/components/ui/textarea";
import {
  isActiveRunStatus,
  latestRun,
  type GeocodeAgentPanel,
  type GeocodeAgentRun,
  type GeocodeAgentRunStatus,
  type GeocodeAgentSuggestion,
  type GeocodeAgentSuggestionStatus,
} from "~/agents/geocode-analysis-contract";

/** The resource route the poll fetcher loads. Same file the tab's own loader
 * calls, so a polled panel and a revalidated one are the same payload. */
export const GEOCODE_AGENT_POLL_PATH = "/admin/se/company-info/geocoding/agent";
const POLL_INTERVAL_MS = 4_000;

const nf = new Intl.NumberFormat("en-US");

export type GeocodeAgentActionData =
  | { ok: true; intent: "start"; run: GeocodeAgentRun }
  | { ok: true; intent: "decide"; suggestion: GeocodeAgentSuggestion }
  | { ok: false; error: string };

const RUN_STATUS_VARIANT: Record<
  GeocodeAgentRunStatus,
  "secondary" | "outline" | "destructive"
> = {
  queued: "outline",
  running: "outline",
  done: "secondary",
  failed: "destructive",
};

const SUGGESTION_STATUS_VARIANT: Record<
  GeocodeAgentSuggestionStatus,
  "secondary" | "outline" | "destructive"
> = {
  new: "outline",
  accepted: "secondary",
  implemented: "secondary",
  rejected: "destructive",
};

function shortTime(value: string | null): string {
  if (!value) return "—";
  return value.replace("T", " ").slice(0, 19);
}

function duration(run: GeocodeAgentRun): string {
  if (!run.startedAt) return "—";
  const end = run.finishedAt ? Date.parse(run.finishedAt) : Date.now();
  const seconds = Math.max(0, Math.round((end - Date.parse(run.startedAt)) / 1000));
  return seconds < 90 ? `${seconds}s` : `${Math.round(seconds / 60)}m`;
}

function RunSummary({ run }: { run: GeocodeAgentRun }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
      <Badge variant={RUN_STATUS_VARIANT[run.status]}>{run.status}</Badge>
      <span>{run.model || "codex default"}</span>
      <span>·</span>
      <span>
        {run.iterations} {run.iterations === 1 ? "turn" : "turns"}
      </span>
      <span>·</span>
      <span>{duration(run)}</span>
      {run.inputTokens + run.outputTokens > 0 ? (
        <>
          <span>·</span>
          <span>
            {nf.format(run.inputTokens)} in / {nf.format(run.outputTokens)} out tokens
          </span>
        </>
      ) : null}
      {run.converged ? <Badge variant="secondary">converged</Badge> : null}
      {run.params.focus ? (
        <span title={run.params.focus}>· focus: {run.params.focus.slice(0, 60)}</span>
      ) : null}
    </div>
  );
}

function SuggestionCard({
  suggestion,
  onDecide,
  busy,
}: {
  suggestion: GeocodeAgentSuggestion;
  onDecide: (status: GeocodeAgentSuggestionStatus) => void;
  busy: boolean;
}) {
  return (
    <div className="flex flex-col gap-2 rounded-lg border p-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">{suggestion.pattern}</span>
        <Badge variant={SUGGESTION_STATUS_VARIANT[suggestion.status]}>
          {suggestion.status}
        </Badge>
        <Badge variant="outline">
          expected yield {nf.format(suggestion.expectedYield)}
        </Badge>
        {suggestion.confidence ? (
          <Badge variant="ghost">{suggestion.confidence} confidence</Badge>
        ) : null}
        {suggestion.policyVersion ? (
          <Badge variant="secondary">policy {suggestion.policyVersion}</Badge>
        ) : null}
      </div>
      <p className="text-sm whitespace-pre-wrap">{suggestion.description}</p>
      {suggestion.yieldBasis ? (
        <p className="text-xs text-muted-foreground">
          Yield basis: {suggestion.yieldBasis}
        </p>
      ) : null}
      {suggestion.examples.length > 0 ? (
        <ul className="flex flex-col gap-1 text-xs text-muted-foreground">
          {suggestion.examples.slice(0, 6).map((example, index) => (
            <li key={`${suggestion.id}-${index}`} className="font-mono">
              {example.address || "(no address)"}
              {example.geocodeStatus ? ` · ${example.geocodeStatus}` : ""}
              {example.count > 0 ? ` · ${nf.format(example.count)} rows` : ""}
              {example.note ? ` — ${example.note}` : ""}
            </li>
          ))}
        </ul>
      ) : null}
      <div className="flex flex-wrap items-center gap-2 pt-1">
        {suggestion.status !== "accepted" && suggestion.status !== "implemented" ? (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={busy}
            onClick={() => onDecide("accepted")}
          >
            <CheckIcon data-icon="inline-start" />
            Accept
          </Button>
        ) : null}
        {suggestion.status === "accepted" ? (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={busy}
            onClick={() => onDecide("implemented")}
          >
            <RocketIcon data-icon="inline-start" />
            Mark implemented
          </Button>
        ) : null}
        {suggestion.status !== "rejected" ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            disabled={busy}
            onClick={() => onDecide("rejected")}
          >
            <XIcon data-icon="inline-start" />
            Reject
          </Button>
        ) : null}
        {suggestion.decidedBy ? (
          <span className="text-xs text-muted-foreground">
            decided by {suggestion.decidedBy}
          </span>
        ) : null}
      </div>
    </div>
  );
}

export function GeocodeAnalysisAgent({ panel }: { panel: GeocodeAgentPanel }) {
  const startFetcher = useFetcher<GeocodeAgentActionData>();
  const decideFetcher = useFetcher<GeocodeAgentActionData>();
  const pollFetcher = useFetcher<{ panel: GeocodeAgentPanel }>();
  const revalidator = useRevalidator();
  const [sheetOpen, setSheetOpen] = useState(false);
  const refreshedTerminalRun = useRef("");

  // The polled panel wins once it exists: it is strictly newer than the one
  // this page was rendered with.
  const current = pollFetcher.data?.panel ?? panel;
  const run = latestRun(current);
  const active = run ? isActiveRunStatus(run.status) : false;
  const starting = startFetcher.state !== "idle";
  const deciding = decideFetcher.state !== "idle";

  useEffect(() => {
    if (startFetcher.state === "idle" && startFetcher.data?.ok) setSheetOpen(false);
  }, [startFetcher.data, startFetcher.state]);

  useEffect(() => {
    if (!active || pollFetcher.state !== "idle") return;
    const timer = window.setTimeout(() => {
      pollFetcher.load(GEOCODE_AGENT_POLL_PATH);
    }, POLL_INTERVAL_MS);
    return () => window.clearTimeout(timer);
  }, [active, pollFetcher, run?.id, run?.status]);

  // When a polled run reaches a terminal state, refresh the page's own loader
  // once so the suggestion board and history come from the source of truth.
  useEffect(() => {
    const polled = latestRun(pollFetcher.data?.panel ?? { ...panel, runs: [] });
    if (!polled || isActiveRunStatus(polled.status)) return;
    const key = `${polled.id}:${polled.status}`;
    if (refreshedTerminalRun.current === key) return;
    refreshedTerminalRun.current = key;
    revalidator.revalidate();
  }, [panel, pollFetcher.data, revalidator]);

  if (!current.available) {
    return (
      <Alert>
        <TriangleAlertIcon />
        <AlertTitle>Analysis agent unavailable</AlertTitle>
        <AlertDescription>{current.unavailableReason}</AlertDescription>
      </Alert>
    );
  }

  const decide = (id: string, status: GeocodeAgentSuggestionStatus) => {
    decideFetcher.submit(
      { intent: "set_geocode_suggestion_status", suggestion_id: id, status },
      { method: "post" },
    );
  };

  return (
    <Card>
      <CardHeader className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-col gap-1">
          <CardTitle className="flex items-center gap-2">
            <SparklesIcon className="size-4" />
            Analysis agent
          </CardTitle>
          <p className="text-sm text-muted-foreground">
            Clusters the unmatched pool, tests each hypothesis against matched
            exemplars, and proposes Dagster augmentation rules with counts. Read-only:
            it never writes the geocode store.
          </p>
        </div>
        <Sheet open={sheetOpen} onOpenChange={setSheetOpen}>
          <SheetTrigger
            render={<Button variant="outline" disabled={active || starting} />}
          >
            {active || starting ? (
              <LoaderCircleIcon data-icon="inline-start" className="animate-spin" />
            ) : (
              <SparklesIcon data-icon="inline-start" />
            )}
            {active ? "Analysis running" : "Run analysis"}
          </SheetTrigger>
          <SheetContent side="right" className="w-full data-[side=right]:sm:max-w-lg">
            <SheetHeader>
              <SheetTitle>Run the geocode analysis agent</SheetTitle>
              <SheetDescription>
                Country {current.countryCode}. The run takes minutes; this page polls
                it. Memory from earlier runs is injected automatically, so a re-run
                goes deeper instead of repeating itself.
              </SheetDescription>
            </SheetHeader>
            <startFetcher.Form method="post" className="flex flex-col gap-4 px-4">
              <input type="hidden" name="intent" value="start_geocode_analysis" />
              <input type="hidden" name="country" value={current.countryCode} />
              <Field>
                <FieldLabel htmlFor="geocode-agent-focus">
                  Focus directive (optional)
                </FieldLabel>
                <Textarea
                  id="geocode-agent-focus"
                  name="focus"
                  rows={4}
                  maxLength={2000}
                  placeholder="e.g. concentrate on ambiguous addresses in Stockholm County; box addresses are already covered"
                />
                <FieldDescription>
                  Left empty, the agent picks the largest unexplained classes itself.
                </FieldDescription>
              </Field>
              <SheetFooter>
                <Button type="submit" disabled={starting || active}>
                  {starting ? (
                    <LoaderCircleIcon data-icon="inline-start" className="animate-spin" />
                  ) : null}
                  Start analysis
                </Button>
              </SheetFooter>
            </startFetcher.Form>
          </SheetContent>
        </Sheet>
      </CardHeader>

      <CardContent className="flex flex-col gap-6">
        {startFetcher.data && !startFetcher.data.ok ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>The run was not started</AlertTitle>
            <AlertDescription>{startFetcher.data.error}</AlertDescription>
          </Alert>
        ) : null}
        {decideFetcher.data && !decideFetcher.data.ok ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>The decision was not saved</AlertTitle>
            <AlertDescription>{decideFetcher.data.error}</AlertDescription>
          </Alert>
        ) : null}

        {run ? (
          <section className="flex flex-col gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-sm font-medium">Latest run</h2>
              <span className="text-xs text-muted-foreground">
                started {shortTime(run.startedAt ?? run.createdAt)}
              </span>
            </div>
            <RunSummary run={run} />
            {run.errorMessage ? (
              <Alert variant="destructive">
                <TriangleAlertIcon />
                <AlertTitle>Run failed</AlertTitle>
                <AlertDescription>{run.errorMessage}</AlertDescription>
              </Alert>
            ) : null}
            {run.reportMd ? (
              <div className="max-h-96 overflow-auto rounded-lg border bg-muted/30 p-4">
                <pre className="font-sans text-sm whitespace-pre-wrap">
                  {run.reportMd}
                </pre>
              </div>
            ) : active ? (
              <p className="text-sm text-muted-foreground" role="status">
                The agent is querying ClickHouse and testing hypotheses. The report
                appears here when the run finishes.
              </p>
            ) : null}
          </section>
        ) : (
          <p className="text-sm text-muted-foreground">
            No analysis has run for {current.countryCode} yet.
          </p>
        )}

        {current.suggestions.length > 0 ? (
          <section className="flex flex-col gap-3">
            <h2 className="text-sm font-medium">
              Suggestions ({current.suggestions.length})
            </h2>
            {current.suggestions.map((suggestion) => (
              <SuggestionCard
                key={suggestion.id}
                suggestion={suggestion}
                busy={deciding}
                onDecide={(status) => decide(suggestion.id, status)}
              />
            ))}
          </section>
        ) : null}

        {current.runs.length > 1 ? (
          <section className="flex flex-col gap-2">
            <h2 className="text-sm font-medium">Run history</h2>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Started</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Turns</TableHead>
                  <TableHead>Duration</TableHead>
                  <TableHead>Focus</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {current.runs.map((historic) => (
                  <TableRow key={historic.id}>
                    <TableCell className="font-mono text-xs">
                      {shortTime(historic.startedAt ?? historic.createdAt)}
                    </TableCell>
                    <TableCell>
                      <Badge variant={RUN_STATUS_VARIANT[historic.status]}>
                        {historic.status}
                      </Badge>
                    </TableCell>
                    <TableCell>{historic.iterations}</TableCell>
                    <TableCell>{duration(historic)}</TableCell>
                    <TableCell className="max-w-[20rem] truncate">
                      {historic.params.focus || "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </section>
        ) : null}
      </CardContent>
    </Card>
  );
}
