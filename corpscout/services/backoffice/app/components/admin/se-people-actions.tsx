import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import { CheckIcon, ChevronDownIcon, CopyIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent } from "~/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "~/components/ui/collapsible";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "~/components/ui/dropdown-menu";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

/**
 * The Actions menu + "Simple sync" sheet on `/admin/se/people`
 * (`admin-se-people.tsx`). ONE fetcher posts to the page's own route:
 * `intent=confirm-simple-sync` is submitted automatically the moment the
 * sheet opens (there is no user-editable scope to fill in first, unlike the
 * Pipeline page's launch forms -- Simple sync always previews and syncs
 * EVERY single-source-pending company), and Confirm posts
 * `intent=launch-simple-sync` -- which launches the COMBINED
 * role_draft+person+role cascade (`SE_COMPANY_PERSON_JOB` +
 * `buildSimpleSyncRunConfig`, `se-company-person-pipeline.server.ts`,
 * `admin-se-people.tsx`'s action), publishing BOTH `se_company_person` and
 * its role assignments in one run -- deliberately NOT the Pipeline page's own
 * clean-copy launch (`SE_COMPANY_PERSON_PUBLISH_JOB`, person table only,
 * left untouched), since a one-click sync should not leave role assignments
 * behind.
 *
 * Layout: this sheet is deliberately wide (`sm:max-w-6xl`, 3x the shadcn
 * Sheet default of `sm:max-w-sm`) with its own internal `max-w-4xl` content
 * column, matching the widest existing sheets in this app
 * (esef-operations-workspace.tsx, se-company-info-pipeline.tsx use
 * `sm:max-w-3xl`) scaled up further -- the preview's headline stats,
 * per-source breakdown and 20-row sample table need real horizontal room, and
 * a 5.5M-row ClickHouse scan backs all of it, hence the skeleton states below
 * rather than a bare "Loading…" line.
 */

const nf = new Intl.NumberFormat("en-US");

export interface SimpleSyncSourceBreakdownView {
  source: string;
  companyCount: number;
  personCount: number;
}

export interface SimpleSyncSamplePersonView {
  name: string;
  companyId: string;
  source: string;
}

export interface SimpleSyncPreviewView {
  companyCount: number;
  personCount: number;
  bySource: SimpleSyncSourceBreakdownView[];
  sample: SimpleSyncSamplePersonView[];
  sampleSize: number;
}

export type SimpleSyncActionResult =
  | { kind: "preview"; preview: SimpleSyncPreviewView }
  | { kind: "launched"; runId: string; url: string | null; job: string }
  | { kind: "error"; error: string };

const SOURCE_LABELS: Record<string, string> = {
  bolagsverket: "Bolagsverket",
  esef: "ESEF",
  wikidata: "Wikidata",
};

export function SourceLabel({ source }: { source: string }) {
  return <>{SOURCE_LABELS[source] ?? source}</>;
}

/** One of the two headline stat cards -- big number, muted label. Exported so
 * its matching skeleton (`HeadlineStatSkeleton`) can mirror its box size. */
export function HeadlineStatCard({ label, value }: { label: string; value: number }) {
  return (
    <Card>
      <CardContent className="flex flex-col gap-1">
        <span className="text-muted-foreground text-sm">{label}</span>
        <span className="text-4xl font-semibold tabular-nums">{nf.format(value)}</span>
      </CardContent>
    </Card>
  );
}

function HeadlineStatSkeleton() {
  return (
    <Card aria-busy="true">
      <CardContent className="flex flex-col gap-2">
        <Skeleton className="h-4 w-28" />
        <Skeleton className="h-9 w-20" />
      </CardContent>
    </Card>
  );
}

/** The Bolagsverket/ESEF/Wikidata breakdown -- always three rows in a fixed
 * order; a zero-count source is rendered muted (outline badge, muted text)
 * rather than dropped, so a reviewer can tell "this source contributed
 * nothing" apart from "this source was left out". */
export function SourceBreakdownList({
  bySource,
}: {
  bySource: SimpleSyncSourceBreakdownView[];
}) {
  return (
    <Card>
      <CardContent className="flex flex-col divide-y divide-border">
        {bySource.map((entry) => {
          const empty = entry.companyCount === 0;
          return (
            <div
              key={entry.source}
              className={`flex flex-wrap items-center justify-between gap-x-6 gap-y-1 py-2.5 first:pt-0 last:pb-0 ${empty ? "text-muted-foreground" : ""}`}
            >
              <Badge variant={empty ? "outline" : "secondary"}>
                <SourceLabel source={entry.source} />
              </Badge>
              <div className="flex items-center gap-6 text-sm tabular-nums">
                <span>
                  {nf.format(entry.companyCount)}{" "}
                  <span className="text-muted-foreground text-xs">companies</span>
                </span>
                <span>
                  {nf.format(entry.personCount)}{" "}
                  <span className="text-muted-foreground text-xs">people</span>
                </span>
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}

function SourceBreakdownSkeleton() {
  return (
    <Card aria-busy="true">
      <CardContent className="flex flex-col divide-y divide-border">
        {[0, 1, 2].map((row) => (
          <div key={row} className="flex items-center justify-between gap-4 py-2.5 first:pt-0 last:pb-0">
            <Skeleton className="h-5 w-24" />
            <Skeleton className="h-4 w-40" />
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

/** The sample of people this run would sync -- a real Table, mono company
 * ids linking to the public company page (`/company/SE/{id}`, matching
 * se-people-sources-table.tsx's own `CompanyCell` for the three source tabs
 * on this same route), capped height with an internal scroll so a wide sheet
 * does not also become a tall one. */
export function SampleTable({
  sample,
  sampleSize,
  personCount,
}: {
  sample: SimpleSyncSamplePersonView[];
  sampleSize: number;
  personCount: number;
}) {
  return (
    <div className="rounded-md border">
      <div className="max-h-72 overflow-y-auto">
        <Table>
          <TableHeader>
            <TableRow className="sticky top-0 z-10 bg-popover">
              <TableHead>Name</TableHead>
              <TableHead>Company</TableHead>
              <TableHead>Source</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sample.length === 0 ? (
              <TableRow>
                <TableCell colSpan={3} className="whitespace-normal text-muted-foreground text-sm">
                  Nothing pending -- every single-source company is already in
                  sync.
                </TableCell>
              </TableRow>
            ) : (
              sample.map((person, index) => (
                <TableRow key={`${person.companyId}-${person.source}-${index}`}>
                  <TableCell className="text-sm">{person.name}</TableCell>
                  <TableCell className="font-mono text-xs">
                    <Link
                      to={`/company/SE/${encodeURIComponent(person.companyId)}`}
                      className="underline underline-offset-2"
                    >
                      {person.companyId}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <Badge variant="secondary" className="text-xs">
                      <SourceLabel source={person.source} />
                    </Badge>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
          {sample.length > 0 ? (
            <TableCaption>
              Sample of {nf.format(sampleSize)} -- the run covers all{" "}
              {nf.format(personCount)} people.
            </TableCaption>
          ) : null}
        </Table>
      </div>
    </div>
  );
}

function SampleTableSkeleton() {
  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Company</TableHead>
            <TableHead>Source</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {[0, 1, 2, 3, 4, 5].map((row) => (
            <TableRow key={row}>
              <TableCell>
                <Skeleton className="h-4 w-36" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-20" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-16" />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

/** The full preview: headline stats, per-source breakdown, sample table --
 * shown once `loadSimpleSyncPreview` answers. An internal `max-w-4xl` column
 * keeps this from feeling stretched inside the sheet's much wider
 * `sm:max-w-6xl` frame. */
export function PreviewBody({ preview }: { preview: SimpleSyncPreviewView }) {
  return (
    <div className="mx-auto flex w-full max-w-4xl flex-col gap-6">
      <div className="grid gap-4 sm:grid-cols-2">
        <HeadlineStatCard label="Companies to sync" value={preview.companyCount} />
        <HeadlineStatCard label="People to sync" value={preview.personCount} />
      </div>
      <div className="flex flex-col gap-2">
        <h3 className="text-sm font-medium text-foreground">Per-source breakdown</h3>
        <SourceBreakdownList bySource={preview.bySource} />
      </div>
      <div className="flex flex-col gap-2">
        <h3 className="text-sm font-medium text-foreground">Sample</h3>
        <SampleTable
          sample={preview.sample}
          sampleSize={preview.sampleSize}
          personCount={preview.personCount}
        />
      </div>
    </div>
  );
}

/** Shown while the preview query -- a 5.5M-row ClickHouse scan across the
 * three source tables -- is still running, mirroring the exact box sizes of
 * the content it stands in for. */
function PreviewSkeleton() {
  return (
    <div className="mx-auto flex w-full max-w-4xl flex-col gap-6">
      <div className="grid gap-4 sm:grid-cols-2">
        <HeadlineStatSkeleton />
        <HeadlineStatSkeleton />
      </div>
      <div className="flex flex-col gap-2">
        <h3 className="text-sm font-medium text-foreground">Per-source breakdown</h3>
        <SourceBreakdownSkeleton />
      </div>
      <div className="flex flex-col gap-2">
        <h3 className="text-sm font-medium text-foreground">Sample</h3>
        <SampleTableSkeleton />
      </div>
      <p className="text-muted-foreground text-xs">
        Scanning the full company/person source tables -- this can take a
        moment.
      </p>
    </div>
  );
}

/** The run id + copy-to-clipboard control of the launched state. */
function CopyableRunId({ runId }: { runId: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="font-mono text-xs">{runId}</span>
      <Button
        type="button"
        variant="ghost"
        size="icon-xs"
        aria-label="Copy run id"
        onClick={() => {
          void navigator.clipboard.writeText(runId).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          });
        }}
      >
        {copied ? <CheckIcon /> : <CopyIcon />}
      </Button>
    </span>
  );
}

/** Inline success state after Confirm -- run id, Tasks tab link, optional
 * direct Dagster link. Inline in the sheet body, not a toast: the sheet
 * stays open so the reviewer can see what launched before deciding what to
 * do next. */
export function LaunchedPanel({
  result,
  tasksHref,
}: {
  result: Extract<SimpleSyncActionResult, { kind: "launched" }>;
  tasksHref: string;
}) {
  return (
    <div className="mx-auto flex w-full max-w-4xl flex-col gap-3">
      <Alert>
        <AlertTitle>Run launched on {result.job}</AlertTitle>
        <AlertDescription>
          <CopyableRunId runId={result.runId} />
        </AlertDescription>
      </Alert>
      <p className="text-muted-foreground text-sm">
        Publishes both people and their role assignments for the full
        single-source scope -- deterministic, no LLM.
      </p>
      <p className="text-muted-foreground text-sm">
        Track progress on the{" "}
        <Link to={tasksHref} className="underline underline-offset-2">
          Tasks tab
        </Link>
        {result.url ? (
          <>
            {" "}
            or{" "}
            <a
              href={result.url}
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2"
            >
              open the run in Dagster
            </a>
          </>
        ) : null}
        .
      </p>
    </div>
  );
}

const FULL_EXPLANATION =
  "Publishes se_company_person AND se_company_person_role for every company backed by EXACTLY ONE source (Bolagsverket, ESEF or Wikidata) whose evidence has changed since the last publish -- the same clean-copy scope se_company_person_clickhouse resolves deterministically, extended to also draft and publish that company's role assignments (se_company_person_role_draft_clickhouse, then se_company_person_role_clickhouse). No LLM involved in any of the three steps.";

export function SePeopleSimpleSyncSheet({ tasksHref }: { tasksHref: string }) {
  const [open, setOpen] = useState(false);
  const fetcher = useFetcher<SimpleSyncActionResult>();
  const requested = useRef(false);

  const onOpenChange = (next: boolean) => {
    if (next) {
      fetcher.reset();
      requested.current = false;
    }
    setOpen(next);
  };

  useEffect(() => {
    if (!open) {
      requested.current = false;
      return;
    }
    if (requested.current) return;
    requested.current = true;
    fetcher.submit({ intent: "confirm-simple-sync" }, { method: "post" });
    // fetcher is stable across renders (useFetcher's own contract); including
    // it would refire this effect on every fetcher state change instead of
    // once per opening.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const result = fetcher.data;
  const busy = fetcher.state !== "idle";
  // Distinct from `busy`: the preview fetch (result undefined) also makes
  // fetcher busy, but only a launch-in-flight (a preview already in hand)
  // should disable/relabel the Confirm button.
  const launching = busy && result?.kind === "preview";

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <DropdownMenu>
        <DropdownMenuTrigger render={<Button variant="outline" size="sm" />}>
          Actions
        </DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuItem onClick={() => onOpenChange(true)}>Simple sync</DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      <SheetContent
        side="right"
        className="flex w-full flex-col sm:max-w-6xl data-[side=right]:sm:max-w-6xl"
      >
        <SheetHeader>
          <SheetTitle>Simple sync</SheetTitle>
          <SheetDescription>
            Publishes people and their role assignments for every
            single-source company with changed evidence -- deterministic, no
            LLM.
          </SheetDescription>
          <Collapsible className="group/explain">
            <CollapsibleTrigger className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
              What exactly does this publish?
              <ChevronDownIcon className="size-3.5 transition-transform duration-200 group-data-open/explain:rotate-180" />
            </CollapsibleTrigger>
            <CollapsibleContent>
              <p className="pt-1.5 text-xs text-muted-foreground">{FULL_EXPLANATION}</p>
            </CollapsibleContent>
          </Collapsible>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
          {!result ? (
            <PreviewSkeleton />
          ) : result.kind === "error" ? (
            <div className="mx-auto w-full max-w-4xl">
              <Alert variant="destructive">
                <AlertTitle>Dagster error</AlertTitle>
                <AlertDescription>{result.error}</AlertDescription>
              </Alert>
            </div>
          ) : result.kind === "preview" ? (
            <PreviewBody preview={result.preview} />
          ) : (
            <LaunchedPanel result={result} tasksHref={tasksHref} />
          )}
        </div>
        <SheetFooter className="flex-row justify-end gap-2">
          <SheetClose render={<Button type="button" variant="outline" />}>
            {result?.kind === "launched" ? "Close" : "Cancel"}
          </SheetClose>
          {result?.kind === "preview" ? (
            <fetcher.Form method="post">
              <input type="hidden" name="intent" value="launch-simple-sync" />
              <Button type="submit" disabled={launching}>
                {launching ? "Launching…" : "Launch sync"}
              </Button>
            </fetcher.Form>
          ) : null}
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
