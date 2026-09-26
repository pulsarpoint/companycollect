import { useEffect } from "react";
import { data, Form, redirect, useNavigation, useRevalidator } from "react-router";
import type { Route } from "./+types/admin-settings-geolite2";
import { DagsterError } from "~/lib/dagster.server";
import { ObjectStoreError } from "~/lib/object-store.server";
import { GEOLITE2_MAX_AGE_DAYS } from "~/lib/geolite2";
import {
  GeoLite2UploadError,
  loadGeolite2Status,
  uploadGeolite2,
  type GeoLite2Status,
} from "~/lib/geolite2.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

/** Run statuses after which nothing more happens; anything else is polled. */
const FINISHED = new Set(["SUCCESS", "FAILURE", "CANCELED"]);
const POLL_INTERVAL_MS = 3000;

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const launched = url.searchParams.get("run");
  try {
    return { status: await loadGeolite2Status(), error: null, launched };
  } catch (error) {
    if (!(error instanceof DagsterError)) throw error;
    return { status: { installed: null, lastRun: null } as GeoLite2Status, error: error.message, launched };
  }
}

export async function action({ request }: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ error: "Invalid request origin." }, { status: 403 });
  const form = await request.formData();
  const files = form
    .getAll("files")
    .filter((value): value is File => typeof value !== "string" && value.name !== "");
  try {
    const { runId } = await uploadGeolite2(files);
    return redirect(`/admin/settings/geolite2?run=${encodeURIComponent(runId)}`);
  } catch (error) {
    if (error instanceof GeoLite2UploadError) return data({ error: error.message }, { status: 400 });
    if (error instanceof ObjectStoreError || error instanceof DagsterError) {
      return data({ error: error.message }, { status: 502 });
    }
    throw error;
  }
}

export function meta() {
  return [{ title: "GeoLite2 | CompanyCollect" }];
}

function formatTime(ms: number | null): string {
  return ms === null ? "—" : new Date(ms).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

/** Reload the page every few seconds while the latest install run is unfinished. */
function useRunPolling(status: string | undefined) {
  const revalidator = useRevalidator();
  const running = status !== undefined && !FINISHED.has(status);
  useEffect(() => {
    if (!running) return;
    const timer = setInterval(() => {
      if (document.visibilityState === "visible" && revalidator.state === "idle") revalidator.revalidate();
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
    // revalidator is stable; re-arm only when the run starts or stops.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [running]);
  return running;
}

export default function AdminGeolite2Settings({ loaderData, actionData }: Route.ComponentProps) {
  const busy = useNavigation().state === "submitting";
  const { status, error, launched } = loaderData;
  const lastRun = status.lastRun;
  const running = useRunPolling(lastRun?.status);
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">GeoLite2</h1>
        <p className="text-sm text-muted-foreground">
          Upload the GeoLite2 City and ASN files downloaded from MaxMind. Dagster checks them and installs them for the next IP enrichment run.
        </p>
      </header>
      {error && (
        <Alert variant="destructive">
          <AlertTitle>Dagster is unavailable</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {actionData?.error && (
        <Alert variant="destructive">
          <AlertTitle>Upload failed</AlertTitle>
          <AlertDescription>{actionData.error}</AlertDescription>
        </Alert>
      )}
      <div className="grid min-w-0 items-start gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(22rem,30rem)]">
        <section className="flex min-w-0 flex-col gap-4" aria-label="Installed databases">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Edition</TableHead>
                <TableHead>Build</TableHead>
                <TableHead>Age</TableHead>
                <TableHead>SHA-256</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {status.installed ? (
                status.installed.editions.map((edition) => (
                  <TableRow key={edition.edition}>
                    <TableCell className="font-medium">{edition.edition}</TableCell>
                    <TableCell>{edition.build ? edition.build.slice(0, 10) : edition.problem === "unreadable" ? "Unreadable" : "Not installed"}</TableCell>
                    <TableCell className={edition.stale ? "font-medium text-destructive" : undefined}>
                      {edition.ageDays === null ? "—" : `${edition.ageDays} days`}
                    </TableCell>
                    <TableCell className="max-w-48 truncate font-mono text-xs" title={edition.sha256 ?? undefined}>
                      {edition.sha256 ?? "—"}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={4}>No install has run yet. Upload both files to install them.</TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
          <p className="text-sm text-muted-foreground">
            {status.installed
              ? `Installed ${formatTime(status.installed.installedAt)}. `
              : ""}
            Files older than {GEOLITE2_MAX_AGE_DAYS} days are shown in red; MaxMind publishes new builds twice a week.
          </p>
          <div className="flex flex-col gap-1 text-sm">
            <h2 className="font-medium">Last install run</h2>
            {lastRun ? (
              <p className="flex flex-wrap items-center gap-2">
                <Badge variant={lastRun.status === "FAILURE" ? "destructive" : "secondary"}>{lastRun.status}</Badge>
                <span className="font-mono text-xs">{lastRun.runId}</span>
                {lastRun.url && (
                  <a className="underline" href={lastRun.url} target="_blank" rel="noreferrer">
                    Open in Dagster
                  </a>
                )}
                {running && <span className="text-muted-foreground">Refreshing until it finishes…</span>}
              </p>
            ) : (
              <p className="text-muted-foreground">None yet.</p>
            )}
            {launched && lastRun?.runId === launched && lastRun.status === "FAILURE" && (
              <p className="text-destructive">The install was refused or failed. The reason is in the Dagster run.</p>
            )}
          </div>
        </section>
        <Card>
          <CardHeader>
            <CardTitle>Upload</CardTitle>
            <CardDescription>One City and/or one ASN file, as MaxMind's .tar.gz or a bare .mmdb, up to 200 MB each.</CardDescription>
          </CardHeader>
          <CardContent>
            <Form method="post" encType="multipart/form-data" className="flex flex-col gap-4">
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="geolite2-files">Files</FieldLabel>
                  <Input id="geolite2-files" name="files" type="file" multiple accept=".gz,.mmdb" required />
                  <FieldDescription>For example GeoLite2-City_20260925.tar.gz and GeoLite2-ASN_20260925.tar.gz.</FieldDescription>
                </Field>
              </FieldGroup>
              <Button type="submit" disabled={busy || running}>
                {busy ? "Uploading…" : "Upload and install"}
              </Button>
            </Form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
