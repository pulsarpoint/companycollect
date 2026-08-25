import { Link } from "react-router";
import {
  AlertTriangleIcon,
  ArrowUpRightIcon,
  CheckCircle2Icon,
  Clock3Icon,
  DatabaseIcon,
  RefreshCwIcon,
} from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { EsefOperationsStatus } from "~/lib/esef-operations.server";

interface EsefOperationsWorkspaceProps {
  status: EsefOperationsStatus | null;
  error: string;
  runUrls: Record<string, string>;
}

const SYNC_LABELS = {
  synced: "In sync",
  out_of_sync: "Update available",
  never_materialized: "Not materialized",
  inputs_updating: "Inputs updating",
  materializing: "Materializing",
} as const;

function syncVariant(
  state: EsefOperationsStatus["syncState"],
): "default" | "secondary" | "destructive" | "outline" {
  if (state === "synced") return "default";
  if (state === "never_materialized") return "destructive";
  if (state === "out_of_sync") return "outline";
  return "secondary";
}

function runVariant(
  status: string,
): "default" | "secondary" | "destructive" | "outline" {
  if (status === "SUCCESS") return "default";
  if (status === "FAILURE" || status === "CANCELED") return "destructive";
  if (status === "QUEUED" || status === "STARTED" || status === "STARTING") {
    return "secondary";
  }
  return "outline";
}

function formatRunTime(seconds: number | null): string {
  if (seconds === null) return "Not started";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Belgrade",
  }).format(new Date(seconds * 1_000));
}

function formatMaterializationTime(milliseconds: number | null): string {
  if (milliseconds === null) return "Never";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Belgrade",
  }).format(new Date(milliseconds));
}

function runLink(
  runId: string,
  runUrls: Record<string, string>,
  className = "font-mono text-xs underline-offset-4 hover:underline",
) {
  const url = runUrls[runId];
  if (!url) return <span className={className}>{runId.slice(0, 8)}</span>;
  return (
    <a className={className} href={url} rel="noreferrer" target="_blank">
      {runId.slice(0, 8)}
      <ArrowUpRightIcon className="ml-1 inline size-3" />
    </a>
  );
}

export function EsefOperationsWorkspace({
  status,
  error,
  runUrls,
}: EsefOperationsWorkspaceProps) {
  const latestRun = status?.latestEnrichmentRun ?? null;

  return (
    <main className="flex flex-1 flex-col gap-8 p-6 lg:p-8">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-8">
        <header className="flex flex-col justify-between gap-4 sm:flex-row sm:items-end">
          <div className="space-y-1.5">
            <p className="text-sm font-medium text-muted-foreground">
              Data operations
            </p>
            <h1 className="text-2xl font-semibold tracking-tight">
              ESEF processing
            </h1>
            <p className="max-w-2xl text-sm text-muted-foreground">
              Company-information enrichment and the ClickHouse inputs it is
              allowed to consume.
            </p>
          </div>
          <Button
            nativeButton={false}
            render={<Link to="/admin/esef" />}
            variant="outline"
          >
            <RefreshCwIcon />
            Refresh status
          </Button>
        </header>

        {error ? (
          <Alert variant="destructive">
            <AlertTriangleIcon />
            <AlertTitle>Dagster status is unavailable</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        {status ? (
          <>
            <section className="grid gap-6 border-y py-6 md:grid-cols-[minmax(0,1fr)_minmax(16rem,0.55fr)] md:items-center">
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-3">
                  <Badge variant={syncVariant(status.syncState)}>
                    {SYNC_LABELS[status.syncState]}
                  </Badge>
                  <span className="text-sm text-muted-foreground">
                    esef_document_company_information_clickhouse
                  </span>
                </div>
                <p className="max-w-2xl text-xl font-medium tracking-tight">
                  {status.syncState === "synced"
                    ? "The company-information asset includes the latest ESEF inputs."
                    : status.syncState === "out_of_sync"
                      ? "One or more ESEF inputs are newer than the company-information asset."
                      : status.syncState === "inputs_updating"
                        ? "Required ESEF inputs are still being produced."
                        : status.syncState === "materializing"
                          ? "Company-information enrichment is currently running."
                          : "The company-information asset has not been produced yet."}
                </p>
              </div>
              <div className="flex items-start gap-3 md:border-l md:pl-6">
                {status.canLaunch ? (
                  <CheckCircle2Icon className="mt-0.5 size-5 text-primary" />
                ) : (
                  <Clock3Icon className="mt-0.5 size-5 text-muted-foreground" />
                )}
                <div>
                  <p className="font-medium">
                    {status.canLaunch ? "Launch guard open" : "Launch blocked"}
                  </p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {status.canLaunch
                      ? "No input or enrichment job is unfinished."
                      : "The Backoffice adapter will refuse a new run until every blocker clears."}
                  </p>
                </div>
              </div>
            </section>

            {status.blockingReasons.length > 0 ? (
              <Alert>
                <Clock3Icon />
                <AlertTitle>Waiting for Dagster</AlertTitle>
                <AlertDescription>
                  <ul className="list-disc space-y-1 pl-4">
                    {status.blockingReasons.map((reason) => (
                      <li key={reason}>{reason}</li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            ) : null}

            <section
              className="space-y-4"
              aria-labelledby="enrichment-job-heading"
            >
              <div className="flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
                <div>
                  <h2 id="enrichment-job-heading" className="font-semibold">
                    Enrichment job
                  </h2>
                  <p className="mt-1 text-sm text-muted-foreground">
                    esef_document_company_information_job
                  </p>
                </div>
                {latestRun ? (
                  <Badge variant={runVariant(latestRun.status)}>
                    {latestRun.status.replaceAll("_", " ")}
                  </Badge>
                ) : (
                  <Badge variant="outline">No runs</Badge>
                )}
              </div>
              <Separator />
              {latestRun ? (
                <dl className="grid gap-5 text-sm sm:grid-cols-2 lg:grid-cols-4">
                  <div>
                    <dt className="text-muted-foreground">Latest run</dt>
                    <dd className="mt-1 font-medium">
                      {runLink(latestRun.runId, runUrls)}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Started</dt>
                    <dd className="mt-1 font-medium">
                      {formatRunTime(latestRun.startTime)}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Requested by</dt>
                    <dd className="mt-1 font-medium">
                      {latestRun.tags["corpscout/requested_by"] ?? "Dagster"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Request ID</dt>
                    <dd className="mt-1 truncate font-mono text-xs">
                      {latestRun.tags["corpscout/request_id"] ?? "—"}
                    </dd>
                  </div>
                </dl>
              ) : (
                <p className="text-sm text-muted-foreground">
                  Dagster has no recorded run for this job.
                </p>
              )}
            </section>

            <section
              className="space-y-4"
              aria-labelledby="asset-state-heading"
            >
              <div>
                <h2 id="asset-state-heading" className="font-semibold">
                  Asset state
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  The output is out of sync whenever a required input has a
                  newer materialization.
                </p>
              </div>
              <div className="rounded-lg border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Role</TableHead>
                      <TableHead>Asset</TableHead>
                      <TableHead>Latest materialization</TableHead>
                      <TableHead>State</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {status.assets.map((asset) => (
                      <TableRow key={asset.asset}>
                        <TableCell className="capitalize text-muted-foreground">
                          {asset.role}
                        </TableCell>
                        <TableCell>
                          <span className="inline-flex items-center gap-2 font-mono text-xs">
                            <DatabaseIcon className="size-3.5 text-muted-foreground" />
                            {asset.asset}
                          </span>
                        </TableCell>
                        <TableCell>
                          {formatMaterializationTime(
                            asset.materialization?.timestamp ?? null,
                          )}
                        </TableCell>
                        <TableCell>
                          {asset.materialization === null ? (
                            <Badge variant="destructive">Missing</Badge>
                          ) : asset.newerThanOutput ? (
                            <Badge variant="outline">Newer than output</Badge>
                          ) : asset.role === "output" ? (
                            <Badge variant={syncVariant(status.syncState)}>
                              {SYNC_LABELS[status.syncState]}
                            </Badge>
                          ) : (
                            <Badge variant="secondary">Included</Badge>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </section>

            <section
              className="space-y-4"
              aria-labelledby="run-history-heading"
            >
              <div>
                <h2 id="run-history-heading" className="font-semibold">
                  Recent runs
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  The eight latest company-information runs known to Dagster.
                </p>
              </div>
              <div className="rounded-lg border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Run</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Started</TableHead>
                      <TableHead>Requested by</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {status.recentEnrichmentRuns.length === 0 ? (
                      <TableRow>
                        <TableCell
                          className="h-20 text-center text-muted-foreground"
                          colSpan={4}
                        >
                          No enrichment runs recorded.
                        </TableCell>
                      </TableRow>
                    ) : (
                      status.recentEnrichmentRuns.map((run) => (
                        <TableRow key={run.runId}>
                          <TableCell>{runLink(run.runId, runUrls)}</TableCell>
                          <TableCell>
                            <Badge variant={runVariant(run.status)}>
                              {run.status.replaceAll("_", " ")}
                            </Badge>
                          </TableCell>
                          <TableCell>{formatRunTime(run.startTime)}</TableCell>
                          <TableCell>
                            {run.tags["corpscout/requested_by"] ?? "Dagster"}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </main>
  );
}
