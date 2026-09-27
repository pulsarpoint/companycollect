import { data, Form, Link, redirect, useNavigation } from "react-router";
import type { Route } from "./+types/admin-provider-feeds";
import { FeedTable } from "~/components/admin/provider-feeds";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { ObjectStoreError } from "~/lib/object-store.server";
import { flagFeed, parseIndicatorRules } from "~/lib/provider-recon";
import { loadIndicatorRules, loadRunIndex, saveIndicatorRules } from "~/lib/provider-recon.server";

const RECENT_RUNS = 60;

export async function loader() {
  const [index, rules] = await Promise.all([loadRunIndex(), loadIndicatorRules()]);
  const latestFull = index.runs.find((r) => r.scope.command === "collect" && !(r.scope.providers?.length)) ?? null;
  return { runs: index.runs.slice(0, RECENT_RUNS), providers: index.providers, rules, latestFull };
}

export async function action({ request }: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ error: "Invalid request origin." }, { status: 403 });
  const parsed = parseIndicatorRules(await request.formData());
  if ("error" in parsed) return data({ error: parsed.error }, { status: 400 });
  try {
    await saveIndicatorRules(parsed.rules);
  } catch (error) {
    if (error instanceof ObjectStoreError) return data({ error: error.message }, { status: 502 });
    throw error;
  }
  return redirect("/admin/provider-feeds");
}

export function meta() {
  return [{ title: "Provider feeds | CompanyCollect" }];
}

export default function AdminProviderFeeds({ loaderData, actionData }: Route.ComponentProps) {
  const { runs, providers, rules, latestFull } = loaderData;
  const busy = useNavigation().state === "submitting";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Provider feeds</h1>
        <p className="text-sm text-muted-foreground">
          Every provider-recon run and what each feed changed. Warnings are display only; nothing is blocked. A wrong removal is undone
          with <code className="font-mono">provider-recon restore</code> (see a provider's page).
        </p>
      </header>
      {actionData?.error && (
        <Alert variant="destructive">
          <AlertTitle>Rules not saved</AlertTitle>
          <AlertDescription>{actionData.error}</AlertDescription>
        </Alert>
      )}
      <div className="grid min-w-0 items-start gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,26rem)]">
        <section className="flex min-w-0 flex-col gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Latest full collect</CardTitle>
              <CardDescription>
                {latestFull ? (
                  <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/runs/${latestFull.run_id}`}>{latestFull.run_id}</Link>
                ) : "No runs yet."}
              </CardDescription>
            </CardHeader>
            <CardContent>{latestFull && <FeedTable feeds={latestFull.feeds} rules={rules} />}</CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Runs</CardTitle>
              <CardDescription>The most recent {runs.length} runs, newest first.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Run</TableHead>
                    <TableHead>Command</TableHead>
                    <TableHead>Providers</TableHead>
                    <TableHead className="text-right">Changed</TableHead>
                    <TableHead className="text-right">Warnings</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {runs.map((run) => {
                    const warned = run.feeds.filter((f) => flagFeed(f, rules).length > 0).length;
                    return (
                      <TableRow key={run.run_id}>
                        <TableCell>
                          <Link className="font-mono text-xs underline-offset-4 hover:underline" to={`/admin/provider-feeds/runs/${run.run_id}`}>{run.run_id}</Link>
                        </TableCell>
                        <TableCell>{run.scope.command}</TableCell>
                        <TableCell className="text-sm">{run.scope.providers?.join(", ") || "all"}</TableCell>
                        <TableCell className="text-right tabular-nums">{run.changed.length}</TableCell>
                        <TableCell className="text-right">
                          {warned > 0 ? <Badge variant="destructive">{warned}</Badge> : <span className="text-muted-foreground">0</span>}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </section>
        <aside className="flex min-w-0 flex-col gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Warning rules</CardTitle>
              <CardDescription>Percent of the ranges a feed had before the run. Stale and failed feeds are always flagged.</CardDescription>
            </CardHeader>
            <CardContent>
              <Form method="post">
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="missingPercent">Missing above (%)</FieldLabel>
                    <Input id="missingPercent" name="missingPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.missingPercent} />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="removedPercent">Removed above (%)</FieldLabel>
                    <Input id="removedPercent" name="removedPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.removedPercent} />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="addedPercent">Added above (%)</FieldLabel>
                    <Input id="addedPercent" name="addedPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.addedPercent} />
                    <FieldDescription>A feed's first run has no baseline and is never flagged.</FieldDescription>
                  </Field>
                  <Field orientation="horizontal">
                    <input id="flagUnmappedTags" name="flagUnmappedTags" type="checkbox" defaultChecked={rules.flagUnmappedTags} />
                    <FieldLabel htmlFor="flagUnmappedTags">Flag new unmapped feed tags</FieldLabel>
                  </Field>
                  <Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save rules"}</Button>
                </FieldGroup>
              </Form>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Providers</CardTitle>
              <CardDescription>{providers.length} published.</CardDescription>
            </CardHeader>
            <CardContent>
              <ul className="grid grid-cols-2 gap-1 text-sm">
                {providers.map((slug) => (
                  <li key={slug}>
                    <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${slug}`}>{slug}</Link>
                  </li>
                ))}
              </ul>
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}
