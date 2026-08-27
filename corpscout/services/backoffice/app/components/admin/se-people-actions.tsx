import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "~/components/ui/dropdown-menu";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
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
 * `intent=launch-simple-sync` -- reusing the People pipeline page's own
 * clean-copy launch path (`buildCleanCopyRunConfig` +
 * `SE_COMPANY_PERSON_PUBLISH_JOB`, `admin-se-people.tsx`'s action), not a
 * second copy of it.
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

function SourceLabel({ source }: { source: string }) {
  if (source === "bolagsverket") return <>Bolagsverket</>;
  if (source === "esef") return <>ESEF</>;
  if (source === "wikidata") return <>Wikidata</>;
  return <>{source}</>;
}

function PreviewBody({ preview }: { preview: SimpleSyncPreviewView }) {
  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="flex flex-col gap-0.5">
          <span className="text-muted-foreground text-xs">Companies to sync</span>
          <span className="text-2xl font-semibold tabular-nums">
            {nf.format(preview.companyCount)}
          </span>
        </div>
        <div className="flex flex-col gap-0.5">
          <span className="text-muted-foreground text-xs">People</span>
          <span className="text-2xl font-semibold tabular-nums">
            {nf.format(preview.personCount)}
          </span>
        </div>
      </div>
      <div className="overflow-x-auto">
        <Table className="min-w-[24rem]">
          <TableHeader>
            <TableRow>
              <TableHead>Source</TableHead>
              <TableHead className="text-right">Companies</TableHead>
              <TableHead className="text-right">People</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {preview.bySource.map((entry) => (
              <TableRow key={entry.source}>
                <TableCell>
                  <SourceLabel source={entry.source} />
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {nf.format(entry.companyCount)}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {nf.format(entry.personCount)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-col gap-2">
        <p className="text-muted-foreground text-xs">
          Sample of the first {preview.sampleSize} people this run would sync
          (out of {nf.format(preview.personCount)} total -- never the full
          list here).
        </p>
        <div className="overflow-x-auto">
          <Table className="min-w-[32rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Company</TableHead>
                <TableHead>Source</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {preview.sample.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={3} className="text-muted-foreground text-sm">
                    Nothing pending -- every single-source company is already
                    in sync.
                  </TableCell>
                </TableRow>
              ) : (
                preview.sample.map((person, index) => (
                  <TableRow key={`${person.companyId}-${index}`}>
                    <TableCell className="text-sm">{person.name}</TableCell>
                    <TableCell className="font-mono text-xs">{person.companyId}</TableCell>
                    <TableCell className="text-xs">
                      <SourceLabel source={person.source} />
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}

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
  const loading = fetcher.state !== "idle";

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
      <SheetContent side="right" className="flex w-full flex-col sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>Simple sync</SheetTitle>
          <SheetDescription>
            Publishes se_company_person for every company backed by EXACTLY
            ONE source (Bolagsverket, ESEF or Wikidata) whose evidence has
            changed since the last publish -- the same clean-copy scope
            se_company_person_clickhouse resolves deterministically, no LLM
            involved.
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
          {!result ? (
            <p className="text-muted-foreground text-sm">Loading preview…</p>
          ) : result.kind === "error" ? (
            <Alert variant="destructive">
              <AlertTitle>Dagster error</AlertTitle>
              <AlertDescription>{result.error}</AlertDescription>
            </Alert>
          ) : result.kind === "preview" ? (
            <PreviewBody preview={result.preview} />
          ) : (
            <div className="flex flex-col gap-3">
              <Alert>
                <AlertTitle>Run launched</AlertTitle>
                <AlertDescription>
                  <span className="font-mono text-xs">{result.runId}</span>
                </AlertDescription>
              </Alert>
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
          )}
        </div>
        <SheetFooter>
          {result?.kind === "preview" ? (
            <fetcher.Form method="post">
              <input type="hidden" name="intent" value="launch-simple-sync" />
              <Button type="submit" disabled={loading}>
                {loading ? "Launching…" : "Confirm"}
              </Button>
            </fetcher.Form>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              {loading ? "Working…" : result?.kind === "launched" ? "Done" : ""}
            </Badge>
          )}
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
