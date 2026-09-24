import { Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-webtech-input";
import { WebtechViewTabs } from "~/components/admin/webtech-scans-table";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { WEBTECH_PAGE_SIZE, webtechDomainPath } from "~/lib/webtech";
import { parseWebtechInputFilters, webtechInputPath } from "~/lib/webtech-input";
import { listWebtechInputs } from "~/lib/webtech-input.server";

export async function loader({ request }: Route.LoaderArgs) {
  const filters = parseWebtechInputFilters(new URL(request.url).searchParams);
  if (filters.task && !/^[a-f0-9-]{36}$/i.test(filters.task)) throw new Response("Invalid task ID", { status: 400 });
  return { filters, result: await listWebtechInputs(filters) };
}

export function meta() {
  return [{ title: "Webtech inputs | CompanyCollect" }];
}

export default function AdminWebtechInput({ loaderData }: Route.ComponentProps) {
  const { filters, result } = loaderData;
  const busy = useNavigation().state !== "idle";
  const pages = Math.max(1, Math.ceil(result.total / WEBTECH_PAGE_SIZE));
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Webtech inputs</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">
          Submitted page selections from <code>webtech_scan_input</code>.
          Draft tasks accept more inputs until processing begins. Source information is retained for retries.
        </p>
      </header>
      <WebtechViewTabs basePath="/admin/webtech" view="input" prefix={filters.prefix} showInputs />
      <Button variant="outline" nativeButton={false} render={<Link to={`/admin/queues/webtech${filters.task ? `?task=${encodeURIComponent(filters.task)}` : ""}`} />}>Open Webtech queue and processing</Button>
      <Form method="get" key={JSON.stringify(filters)}>
        <FieldGroup className="sm:flex-row sm:flex-wrap sm:items-end">
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="webtech-input-prefix">Domain starts with</FieldLabel>
            <Input id="webtech-input-prefix" name="prefix" defaultValue={filters.prefix} placeholder="wordpress" maxLength={253} />
          </Field>
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="webtech-input-task">Task ID</FieldLabel>
            <Input id="webtech-input-task" name="task" defaultValue={filters.task} placeholder="All tasks" />
          </Field>
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="webtech-input-source">Source</FieldLabel>
            <Input id="webtech-input-source" name="source" defaultValue={filters.source} placeholder="All sources" />
          </Field>
          <Button type="submit" disabled={busy}>{busy ? "Loading…" : "Apply filters"}</Button>
          <Button variant="ghost" nativeButton={false} render={<Link to="/admin/webtech/input" />}>Reset</Button>
        </FieldGroup>
      </Form>
      <p className="max-w-4xl text-sm text-muted-foreground">
        Load domains or page URLs using the <code>webtech_scan_input</code> asset, then run <code>webtech_scan_results</code> with the same task ID.
        “No indexed result” includes inputs awaiting scanning, currently running, or awaiting result ingestion.
      </p>
      <p className="text-sm text-muted-foreground" role="status">
        {result.total.toLocaleString()} inputs · {result.tasks.toLocaleString()} tasks · {result.domains.toLocaleString()} domains
      </p>
      <Table>
        <TableHeader><TableRow>
          <TableHead>Domain / page</TableHead><TableHead>Task / source</TableHead><TableHead>Submitted (UTC)</TableHead><TableHead>Results</TableHead><TableHead><span className="sr-only">History</span></TableHead>
        </TableRow></TableHeader>
        <TableBody>
          {result.rows.map(row => <TableRow key={`${row.task_id}:${row.input_id}`}>
            <TableCell className="max-w-lg whitespace-normal"><span className="font-mono">{row.root_domain}</span><p className="break-all text-xs text-muted-foreground">{row.page_url}</p></TableCell>
            <TableCell className="max-w-sm whitespace-normal"><Link className="break-all font-mono text-xs underline" to={webtechInputPath({ ...filters, task: row.task_id })}>{row.task_id}</Link><p className="text-sm">{row.source_name}</p><p className="break-all text-xs text-muted-foreground">{row.source_record_id}</p></TableCell>
            <TableCell>{row.submitted_at}</TableCell>
            <TableCell><Badge variant={row.scanned_at ? "secondary" : "outline"}>{row.scanned_at ? "Results indexed" : "No indexed result"}</Badge>{row.scanned_at ? <p className="text-xs text-muted-foreground">{row.scanned_at}</p> : null}</TableCell>
            <TableCell><Link className="underline underline-offset-2" to={`${webtechDomainPath(row.root_domain)}?view=history`}>Domain history</Link></TableCell>
          </TableRow>)}
          {!result.rows.length ? <TableRow><TableCell colSpan={5} className="h-32 text-center text-muted-foreground">No submitted inputs match these filters.</TableCell></TableRow> : null}
        </TableBody>
      </Table>
      <div className="flex items-center justify-between gap-3">
        <span className="text-sm text-muted-foreground">Page {filters.page.toLocaleString()} of {pages.toLocaleString()}</span>
        <div className="flex gap-2">
          {filters.page > 1 ? <Button variant="outline" nativeButton={false} render={<Link to={webtechInputPath(filters, filters.page - 1)} />}>Previous</Button> : null}
          {filters.page < pages ? <Button variant="outline" nativeButton={false} render={<Link to={webtechInputPath(filters, filters.page + 1)} />}>Next</Button> : null}
        </div>
      </div>
    </div>
  );
}
