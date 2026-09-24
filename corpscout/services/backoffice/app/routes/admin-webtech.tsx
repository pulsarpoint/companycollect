import { Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-webtech";
import { WebtechScansTable, WebtechViewTabs } from "~/components/admin/webtech-scans-table";
import { Button } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { parseWebtechSearch, webtechListPath } from "~/lib/webtech";
import { listWebtechScans } from "~/lib/webtech.server";

export async function loader({ request }: Route.LoaderArgs) {
  const filters = parseWebtechSearch(new URL(request.url).searchParams);
  return { result: await listWebtechScans(filters), filters };
}

export function meta() {
  return [{ title: "Webtech | CompanyCollect" }];
}

export default function AdminWebtech({ loaderData }: Route.ComponentProps) {
  const { result, filters } = loaderData;
  const busy = useNavigation().state !== "idle";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Webtech</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">Technology detections from Webtech website scans. Inspect the latest attempt for each requested page or browse earlier scans and their detection details.</p>
      </header>
      <WebtechViewTabs basePath="/admin/webtech" view={filters.view} prefix={filters.prefix} showInputs />
      <Form method="get" key={filters.prefix}>
        <input type="hidden" name="view" value={filters.view} />
        <FieldGroup className="sm:flex-row sm:items-end">
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="webtech-prefix">Domain starts with</FieldLabel>
            <Input id="webtech-prefix" name="prefix" defaultValue={filters.prefix} placeholder="wordpress" maxLength={253} />
          </Field>
          <Button type="submit" disabled={busy}>{busy ? "Loading…" : "Search"}</Button>
          <Button variant="ghost" nativeButton={false} render={<Link to={webtechListPath("/admin/webtech", filters.view)} />}>Reset</Button>
        </FieldGroup>
      </Form>
      <p className="text-sm text-muted-foreground">{filters.view === "latest" ? "Latest results include failed and empty scans. Older detections are not carried forward." : "History includes successful, partial, failed and empty attempts. Open a scan to see its recorded detections."}</p>
      <WebtechScansTable result={result} basePath="/admin/webtech" {...filters} />
    </div>
  );
}
