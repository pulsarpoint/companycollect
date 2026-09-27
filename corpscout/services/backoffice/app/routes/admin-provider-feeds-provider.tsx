import { data, Link } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-provider";
import { ProviderFeedsError, RangeTable, StatusBadge } from "~/components/admin/provider-feeds";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { attentionRanges, restoreCommand, restoreOptions, serviceRangeCounts } from "~/lib/provider-recon";
import { loadProviderDocument } from "~/lib/provider-recon.server";

export async function loader({ params }: Route.LoaderArgs) {
  const doc = await loadProviderDocument(params.slug);
  if (!doc) throw data(`Provider ${params.slug} not found.`, { status: 404 });
  return { doc };
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  return <ProviderFeedsError error={error} />;
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.slug} | Provider feeds` }];
}

export default function AdminProviderFeedsProvider({ loaderData }: Route.ComponentProps) {
  const { doc } = loaderData;
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
      <Card>
        <CardHeader><CardTitle>Feeds</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Feed</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Ranges</TableHead>
                <TableHead>Last success</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {Object.entries(doc.collection.collectors).map(([id, st]) => (
                <TableRow key={id}>
                  <TableCell className="font-mono text-xs">{id}</TableCell>
                  <TableCell><StatusBadge status={st.status} /></TableCell>
                  <TableCell className="text-right tabular-nums">{st.items}</TableCell>
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
                If a removal was wrong, fix the collector, then run the command for its removal date. Each command restores everything
                that feed removed on or after that date, so the newest date is the narrowest undo.
              </p>
              {restores.map(({ collector, since, count }) => (
                <div key={`${collector}/${since}`} className="flex flex-wrap items-center gap-2">
                  <code className="rounded bg-muted px-2 py-1 font-mono text-xs">{restoreCommand(doc.slug, collector, since)}</code>
                  <span className="text-xs text-muted-foreground">up to {count} range{count === 1 ? "" : "s"}</span>
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
