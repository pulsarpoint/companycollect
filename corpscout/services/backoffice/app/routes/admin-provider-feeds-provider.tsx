import { data, Form, Link, redirect, useNavigation, useSearchParams } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-provider";
import { FeedSource, ProviderFeedsError, RangeTable, StatusBadge } from "~/components/admin/provider-feeds";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { attentionRanges, restoreCommand, restoreOptions, serviceRangeCounts } from "~/lib/provider-recon";
import { ProviderReconServiceError, restoreProviderRanges } from "~/lib/provider-recon-ops.server";
import { loadProviderDocument } from "~/lib/provider-recon.server";

export async function loader({ params }: Route.LoaderArgs) {
  const doc = await loadProviderDocument(params.slug);
  if (!doc) throw data(`Provider ${params.slug} not found.`, { status: 404 });
  return { doc };
}

/** Restore: asks the provider-recon service to revive a feed's removed ranges. */
export async function action({ request, params }: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ error: "Invalid request origin." }, { status: 403 });
  const form = await request.formData();
  if (form.get("confirm") !== "yes") return data({ error: "Tick the confirmation first." }, { status: 400 });
  const collector = String(form.get("collector") ?? "");
  const removedSince = String(form.get("removed_since") ?? "");
  try {
    const { runId, restored } = await restoreProviderRanges({ provider: params.slug, collector, removedSince });
    const query = new URLSearchParams({ restored: String(restored), run: runId });
    return redirect(`/admin/provider-feeds/providers/${encodeURIComponent(params.slug)}?${query}`);
  } catch (error) {
    if (error instanceof ProviderReconServiceError) return data({ error: error.message }, { status: 409 });
    throw error;
  }
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  return <ProviderFeedsError error={error} />;
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.slug} | Provider feeds` }];
}

export default function AdminProviderFeedsProvider({ loaderData, actionData }: Route.ComponentProps) {
  const { doc } = loaderData;
  const [search] = useSearchParams();
  const restored = search.get("restored");
  const busy = useNavigation().state === "submitting";
  const attention = attentionRanges(doc);
  const restores = restoreOptions(doc);
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <Link className="text-sm text-muted-foreground underline-offset-4 hover:underline" to="/admin/provider-feeds">Provider feeds</Link>
        <h1 className="text-2xl font-semibold">{doc.display_name}</h1>
        <p className="text-sm text-muted-foreground">
          {doc.slug} · {doc.category}{doc.country ? ` · ${doc.country}` : ""} · collected {doc.collection.collected_at.slice(0, 16).replace("T", " ")} UTC
        </p>
      </header>
      {actionData?.error && (
        <Alert variant="destructive">
          <AlertTitle>Restore failed</AlertTitle>
          <AlertDescription>{actionData.error}</AlertDescription>
        </Alert>
      )}
      {restored !== null && (
        <Alert>
          <AlertTitle>Restored {restored} range{restored === "1" ? "" : "s"}</AlertTitle>
          <AlertDescription>
            Run <Link className="font-mono underline-offset-4 hover:underline" to={`/admin/provider-feeds/runs/${search.get("run") ?? ""}`}>{search.get("run")}</Link>
          </AlertDescription>
        </Alert>
      )}
      <Card>
        <CardHeader><CardTitle>Feeds</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Feed</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Ranges</TableHead>
                <TableHead className="text-right">Skipped</TableHead>
                <TableHead>Last success</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {Object.entries(doc.collection.collectors).map(([id, st]) => (
                <TableRow key={id}>
                  <TableCell className="font-mono text-xs">{id}</TableCell>
                  <TableCell className="max-w-96">
                    <FeedSource url={st.source_url} format={st.format} />
                  </TableCell>
                  <TableCell><StatusBadge status={st.status} /></TableCell>
                  <TableCell className="text-right tabular-nums">{st.items}</TableCell>
                  <TableCell className="text-right tabular-nums">{st.skipped_lines ?? 0}</TableCell>
                  <TableCell className="tabular-nums">{st.last_success_at?.slice(0, 16).replace("T", " ") ?? "—"}</TableCell>
                  <TableCell className="max-w-48 truncate font-mono text-xs" title={st.source_version}>{st.source_version ?? "—"}</TableCell>
                  <TableCell className="text-sm text-destructive">{st.error ?? ""}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader><CardTitle>Services</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Service</TableHead>
                <TableHead>Types</TableHead>
                <TableHead className="text-right">Active</TableHead>
                <TableHead className="text-right">Missing</TableHead>
                <TableHead className="text-right">Removed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {doc.services.map((s) => {
                const c = serviceRangeCounts(s);
                return (
                  <TableRow key={s.service_key}>
                    <TableCell>
                      <span className="font-medium">{s.display_name}</span>{" "}
                      <span className="font-mono text-xs text-muted-foreground">{s.service_key}</span>
                      {s.removed_at && <span className="ml-2 text-xs text-destructive">removed from definition {s.removed_at}</span>}
                    </TableCell>
                    <TableCell className="text-sm">{s.service_types.join(", ")}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.active}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.missing}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.removed}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Missing and removed ranges</CardTitle>
          <CardDescription>Removed ranges stay here for 90 days; history keeps them forever.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {restores.length > 0 && (
            <div className="flex flex-col gap-1">
              <p className="text-sm">
                If a removal was wrong, fix the collector, then restore from its removal date (or run the command). Each restore revives
                everything that feed removed on or after that date, so the newest date is the narrowest undo.
              </p>
              {restores.map(({ collector, since, count }) => (
                <div key={`${collector}/${since}`} className="flex flex-wrap items-center gap-2">
                  <code className="rounded bg-muted px-2 py-1 font-mono text-xs">{restoreCommand(doc.slug, collector, since)}</code>
                  <span className="text-xs text-muted-foreground">up to {count} range{count === 1 ? "" : "s"}</span>
                  <Form method="post" className="flex items-center gap-2">
                    <input type="hidden" name="collector" value={collector} />
                    <input type="hidden" name="removed_since" value={since} />
                    <label className="flex items-center gap-1 text-xs">
                      <input type="checkbox" name="confirm" value="yes" required /> I fixed the collector
                    </label>
                    <Button type="submit" size="sm" variant="outline" disabled={busy}>Restore</Button>
                  </Form>
                </div>
              ))}
            </div>
          )}
          <RangeTable rows={attention} />
        </CardContent>
      </Card>
    </div>
  );
}
