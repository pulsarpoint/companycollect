import { data, Link } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-run";
import { FeedTable } from "~/components/admin/provider-feeds";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import type { KindDiff } from "~/lib/provider-recon";
import { loadIndicatorRules, loadManifest } from "~/lib/provider-recon.server";

export async function loader({ params }: Route.LoaderArgs) {
  const [manifest, rules] = await Promise.all([loadManifest(params.runId), loadIndicatorRules()]);
  if (!manifest) throw data(`Run ${params.runId} not found.`, { status: 404 });
  return { manifest, rules };
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.runId} | Provider feeds` }];
}

const TRANSITIONS: [keyof KindDiff, keyof KindDiff, string][] = [
  ["added", "added_count", "Added"],
  ["missing", "missing_count", "Missing"],
  ["reappeared", "reappeared_count", "Reappeared"],
  ["restored", "restored_count", "Restored"],
  ["removed", "removed_count", "Removed"],
  ["purged", "purged_count", "Purged"],
  ["updated", "updated_count", "Updated"],
];

function DiffList({ kind, diff }: { kind: string; diff: KindDiff }) {
  return (
    <div className="flex flex-col gap-1">
      <h4 className="font-mono text-xs text-muted-foreground">{kind}</h4>
      {TRANSITIONS.map(([listKey, countKey, label]) => {
        const count = diff[countKey] as number;
        if (!count) return null;
        const items = (diff[listKey] as string[] | undefined) ?? [];
        return (
          <details key={label} className="text-sm">
            <summary>{label}: {count}</summary>
            {items.length > 0 ? (
              <ul className="ml-4 font-mono text-xs">{items.map((item) => <li key={item}>{item}</li>)}</ul>
            ) : (
              <p className="ml-4 text-xs text-muted-foreground">Counts only (new provider).</p>
            )}
          </details>
        );
      })}
      {diff.truncated && <p className="text-xs text-muted-foreground">Lists are truncated; counts are exact.</p>}
    </div>
  );
}

export default function AdminProviderFeedsRun({ loaderData }: Route.ComponentProps) {
  const { manifest, rules } = loaderData;
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <Link className="text-sm text-muted-foreground underline-offset-4 hover:underline" to="/admin/provider-feeds">Provider feeds</Link>
        <h1 className="font-mono text-xl font-semibold">{manifest.run_id}</h1>
        <p className="text-sm text-muted-foreground">
          {manifest.scope.command} · {manifest.scope.providers?.join(", ") || "all providers"} · {manifest.changed.length} changed, {manifest.unchanged.length} unchanged
        </p>
      </header>
      <Card>
        <CardHeader><CardTitle>Feeds</CardTitle></CardHeader>
        <CardContent><FeedTable feeds={manifest.feeds} rules={rules} /></CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Changed providers</CardTitle>
          <CardDescription>Lifecycle transitions per evidence kind.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          {manifest.changed.length === 0 && <p className="text-sm text-muted-foreground">Nothing changed in this run.</p>}
          {manifest.changed.map((change) => (
            <section key={change.slug} className="flex flex-col gap-3">
              <h3 className="font-medium">
                <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${change.slug}`}>{change.slug}</Link>
                {change.created && <span className="ml-2 text-xs text-muted-foreground">first publication</span>}
              </h3>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                {Object.entries(change.evidence ?? {}).map(([kind, diff]) => <DiffList key={kind} kind={kind} diff={diff} />)}
              </div>
            </section>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
