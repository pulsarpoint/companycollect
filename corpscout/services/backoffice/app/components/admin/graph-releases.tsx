import { Alert, AlertDescription } from "~/components/ui/alert";
import { useState } from "react";
import { Form } from "react-router";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { graphDashboard } from "~/lib/commoncrawl-graph.server";

export function GraphReleases({
  dashboard,
  busy,
}: {
  dashboard: Awaited<ReturnType<typeof graphDashboard>>;
  busy: boolean;
}) {
  const [showAll, setShowAll] = useState(false);
  const { state, automation, releases, latestRanking } = dashboard;
  const running =
    automation?.sensors.find(
      (s) => s.name === "commoncrawl_graph_import_sensor",
    )?.status === "RUNNING";
  const discoveryRunning =
    automation?.schedules.find(
      (s) => s.name === "commoncrawl_graph_discovery_schedule",
    )?.status === "RUNNING";
  return (
    <section className="flex flex-col gap-4" aria-label="Common Crawl releases">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-lg font-semibold">Common Crawl releases</h2>
        <Form method="post" className="flex flex-wrap gap-2">
          <Button
            type="submit"
            name="intent"
            value="discover"
            variant="outline"
            size="sm"
            disabled={busy}
          >
            Check for releases
          </Button>
          <Button
            type="submit"
            name="intent"
            value="backfill"
            variant="outline"
            size="sm"
            disabled={busy || !running}
          >
            Import missing rankings
          </Button>
          <Button
            type="submit"
            name="intent"
            value={state.automatic_imports_enabled ? "pause" : "resume"}
            variant="outline"
            size="sm"
            disabled={busy}
          >
            {state.automatic_imports_enabled
              ? "Pause automatic imports"
              : "Enable automatic imports"}
          </Button>
        </Form>
      </div>
      <dl className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
        <div>
          <dt className="text-muted-foreground">Active full graph</dt>
          <dd>{state.active_graph_release ?? "None activated"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Latest rankings</dt>
          <dd>{latestRanking ?? "None imported"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Last catalog refresh</dt>
          <dd>{state.discovery_succeeded_at ?? "Not yet refreshed"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Automation</dt>
          <dd>
            {state.automatic_imports_enabled
              ? "Imports enabled"
              : "Imports paused"}{" "}
            ·{" "}
            {running
              ? "Dispatcher running"
              : "Dispatcher stopped or unavailable"}
          </dd>
          <dd>
            {discoveryRunning
              ? "Daily discovery running"
              : "Daily discovery stopped or unavailable"}
          </dd>
        </div>
      </dl>
      {state.discovery_error && (
        <p role="status" className="text-sm text-destructive">
          Last discovery failed ({state.discovery_error}). Showing the previous
          catalog.
        </p>
      )}
      {latestRanking &&
        state.active_graph_release &&
        latestRanking !== state.active_graph_release && (
          <Alert>
            <AlertDescription>
              The active graph and latest rankings cover different releases.
              Domain history identifies each ranking release separately.
            </AlertDescription>
          </Alert>
        )}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Release / coverage</TableHead>
            <TableHead>Rankings</TableHead>
            <TableHead>Full graph</TableHead>
            <TableHead>Files cached</TableHead>
            <TableHead>Import</TableHead>
            <TableHead>Action</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {(showAll ? releases : releases.slice(0, 8)).map((release) => {
            const active = ["queued", "launching", "running"].includes(
              release.status ?? "",
            );
            return (
              <TableRow key={release.graph_release}>
                <TableCell>
                  <a
                    href={release.source_index_url}
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-4"
                  >
                    {release.graph_release}
                  </a>
                  <div className="text-xs text-muted-foreground">
                    {release.coverage_start && release.coverage_end
                      ? `${release.coverage_start} – ${release.coverage_end}`
                      : "Coverage dates unavailable"}
                  </div>
                </TableCell>
                <TableCell>
                  {release.rankRows
                    ? `${Number(release.rankRows).toLocaleString()} domains`
                    : release.ranks_available
                      ? "Not imported"
                      : "Unavailable"}
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">
                    {release.graph_release === state.active_graph_release
                      ? "Active"
                      : release.graph_status === "retired"
                        ? release.graphStored
                          ? "Retired · cleanup pending"
                          : "Retired · removed"
                        : "Not active"}
                  </Badge>
                </TableCell>
                <TableCell>
                  {release.cached_files} / {release.available_files}
                </TableCell>
                <TableCell>
                  {release.status ?? "—"}
                  {release.runUrl && (
                    <>
                      {" "}
                      ·{" "}
                      <a
                        href={release.runUrl}
                        className="underline underline-offset-4"
                        target="_blank"
                        rel="noreferrer"
                      >
                        Dagster run
                      </a>
                    </>
                  )}
                  {release.last_error && (
                    <div className="max-w-64 whitespace-normal text-xs text-destructive">
                      {release.last_error}
                    </div>
                  )}
                </TableCell>
                <TableCell>
                  <Form method="post" className="flex gap-2">
                    <input type="hidden" name="intent" value="import" />
                    <input
                      type="hidden"
                      name="release"
                      value={release.graph_release}
                    />
                    <input
                      type="hidden"
                      name="requestId"
                      value={release.submissionId}
                    />
                    {!release.rankRows && release.ranks_available && (
                      <Button
                        type="submit"
                        name="selection"
                        value="ranks"
                        variant="outline"
                        size="sm"
                        disabled={busy || active || !running}
                      >
                        Import rankings
                      </Button>
                    )}
                    {release.canImportFull && (
                      <Button
                        type="submit"
                        name="selection"
                        value="full"
                        variant="outline"
                        size="sm"
                        disabled={busy || active || !running}
                      >
                        {release.selection === "full" &&
                        ["failed", "canceled"].includes(release.status ?? "")
                          ? "Resume full import"
                          : "Import full graph"}
                      </Button>
                    )}
                  </Form>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
      {releases.length > 8 && (
        <Button variant="ghost" size="sm" onClick={() => setShowAll(!showAll)}>
          {showAll
            ? "Show recent releases"
            : `Show all ${releases.length} releases`}
        </Button>
      )}
      <p className="text-xs text-muted-foreground">
        Ranking history is retained. Previous full graphs are retired after a
        validated replacement. Detailed progress and logs are available in
        Dagster.
      </p>
    </section>
  );
}
