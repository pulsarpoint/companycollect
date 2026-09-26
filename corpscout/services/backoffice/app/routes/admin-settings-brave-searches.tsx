import { useState } from "react";
import { data, Form, Link, redirect, useNavigation } from "react-router";
import type { Route } from "./+types/admin-settings-brave-searches";
import { BraveSearchError, listBraveSearches, removeBraveSearch, saveBraveSearch } from "~/lib/brave-searches.server";
import { braveSearchPreview } from "~/lib/brave-searches";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export async function loader({request}: Route.LoaderArgs) {
  const url = new URL(request.url);
  const searches = await listBraveSearches();
  const editing = searches.find(search => search.searchId === url.searchParams.get("edit")) ?? null;
  if (url.searchParams.has("edit") && !editing) throw new Response("This Brave search was not found.", {status: 404});
  return {searches, editing, saved: url.searchParams.get("saved") === "yes"};
}

export async function action({request}: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({error: "Invalid request origin.", values: null}, {status: 403});
  const form = await request.formData();
  const values = {searchId: String(form.get("search_id") ?? ""), revision: Number(form.get("revision") ?? 0),
    name: String(form.get("name") ?? ""), queryTemplate: String(form.get("query_template") ?? "")};
  try {
    if (form.get("intent") === "remove") await removeBraveSearch(values.searchId, values.revision);
    else if (form.get("intent") === "save") await saveBraveSearch(values);
    else throw new BraveSearchError("Unknown Brave search action.");
    return redirect("/admin/settings/brave-searches?saved=yes");
  } catch (error) {
    if (error instanceof BraveSearchError) return data({error: error.message, values: form.get("intent") === "save" ? values : null}, {status: 400});
    throw error;
  }
}

export function meta() { return [{title: "Brave searches | CompanyCollect"}]; }

function SearchForm({values, busy}: {values: {searchId: string; revision: number; name: string; queryTemplate: string}; busy: boolean}) {
  const [template, setTemplate] = useState(values.queryTemplate);
  return <Form method="post" className="flex flex-col gap-4">
    <input type="hidden" name="intent" value="save" /><input type="hidden" name="search_id" value={values.searchId} /><input type="hidden" name="revision" value={values.revision} />
    <FieldGroup>
      <Field><FieldLabel htmlFor="search-name">Name</FieldLabel><Input id="search-name" name="name" defaultValue={values.name} placeholder="e.g. Official website" maxLength={120} required /></Field>
      <Field><FieldLabel htmlFor="search-question">Question template</FieldLabel>
        <Textarea id="search-question" name="query_template" value={template} onChange={event => setTemplate(event.target.value)} rows={6} maxLength={8000} required placeholder="Find the official website of {company_name}." />
        <FieldDescription>Use {"{company_name}"}, {"{company_id}"}, and {"{country_code}"}. Include the company name or ID. Literal braces must be doubled.</FieldDescription>
      </Field>
    </FieldGroup>
    <div className="flex flex-col gap-2"><h3 className="text-sm font-medium">Example question</h3><p className="whitespace-pre-wrap break-words text-sm text-muted-foreground">{braveSearchPreview(template) || "Enter a question to see its preview."}</p></div>
    <div className="flex gap-2"><Button type="submit" disabled={busy}>{busy ? "Saving…" : values.searchId ? "Save changes" : "Add search"}</Button>
      {values.searchId && <Button variant="outline" nativeButton={false} render={<Link to="/admin/settings/brave-searches" />}>Cancel</Button>}
    </div>
  </Form>;
}

export default function AdminBraveSearchSettings({loaderData, actionData}: Route.ComponentProps) {
  const busy = useNavigation().state !== "idle";
  const values = actionData?.values ?? loaderData.editing ?? {searchId: "", revision: 0, name: "", queryTemplate: ""};
  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <header className="flex flex-col gap-2"><h1 className="text-2xl font-semibold">Brave searches</h1>
      <p className="text-sm text-muted-foreground">Manage the questions available when processing the Brave queue. Started tasks keep their saved search version.</p></header>
    {actionData?.error && <Alert variant="destructive"><AlertTitle>Could not update searches</AlertTitle><AlertDescription>{actionData.error}</AlertDescription></Alert>}
    {loaderData.saved && !actionData?.error && <Alert><AlertDescription>Brave searches updated.</AlertDescription></Alert>}
    <div className="grid min-w-0 items-start gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(22rem,30rem)]">
      <section className="flex min-w-0 flex-col gap-3" aria-label="Saved Brave searches">
        <Table><TableHeader><TableRow><TableHead>Name</TableHead><TableHead>Question template</TableHead><TableHead>Version</TableHead><TableHead>Actions</TableHead></TableRow></TableHeader>
          <TableBody>{loaderData.searches.map(search => <TableRow key={search.searchId}>
            <TableCell className="whitespace-normal font-medium">{search.name}</TableCell><TableCell className="max-w-xl whitespace-pre-wrap break-words">{search.queryTemplate}</TableCell><TableCell>{search.revision}</TableCell>
            <TableCell><div className="flex gap-2"><Button variant="outline" size="sm" nativeButton={false} render={<Link to={`?edit=${search.searchId}`} />}>Edit</Button>
              <Form method="post"><input type="hidden" name="intent" value="remove" /><input type="hidden" name="search_id" value={search.searchId} /><input type="hidden" name="revision" value={search.revision} />
                <Button type="submit" variant="destructive" size="sm" disabled={busy} aria-label={`Remove ${search.name}`}>Remove</Button>
              </Form></div></TableCell>
          </TableRow>)}{!loaderData.searches.length && <TableRow><TableCell colSpan={4}>No searches yet. Add a search to make it available in the Brave queue.</TableCell></TableRow>}</TableBody>
        </Table>
        <p className="text-sm text-muted-foreground">Removing a search hides it from new tasks. Saved results and started tasks retain their questions.</p>
        <Link className="text-sm underline" to="/admin/queues/brave">Open Brave queue</Link>
      </section>
      <Card><CardHeader><CardTitle>{values.searchId ? "Edit search" : "Add search"}</CardTitle><CardDescription>Changes create a new version for future tasks.</CardDescription></CardHeader>
        <CardContent><SearchForm key={`${JSON.stringify(values)}:${loaderData.searches.length}`} values={values} busy={busy} /></CardContent>
      </Card>
    </div>
  </div>;
}
