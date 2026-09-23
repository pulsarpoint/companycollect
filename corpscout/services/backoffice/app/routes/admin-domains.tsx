import { Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-domains";
import { WorkspaceDomainsTable } from "~/components/admin/workspace-domains-table";
import { Button } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "~/components/ui/native-select";
import {
  parseWorkspaceDomainFilters,
  workspaceDomainsHref,
} from "~/lib/workspace-domains";
import { listWorkspaceDomains } from "~/lib/workspace-domains.server";

export async function loader({ request }: Route.LoaderArgs) {
  const params = new URL(request.url).searchParams;
  const filters = parseWorkspaceDomainFilters(params);
  const after = params.get("after") ?? "";
  return { ...(await listWorkspaceDomains(filters, after)), filters, after };
}

export function meta() {
  return [{ title: "Domains | CompanyCollect" }];
}

export default function WorkspaceDomains({ loaderData }: Route.ComponentProps) {
  const { rows, filters, after, next, hasMore } = loaderData;
  const busy = useNavigation().state !== "idle";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Domains</h1>
        <p className="text-muted-foreground max-w-4xl text-sm">
          Domains from company records, archived pages, DNS, the domain graph
          and website scans. Expand a domain to inspect its sites and company
          associations.
        </p>
      </header>
      <Form
        method="get"
        key={JSON.stringify(filters)}
        className="flex flex-wrap items-end gap-3"
      >
        <FieldGroup className="sm:flex-row sm:items-end">
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="domain-prefix">Domain starts with</FieldLabel>
            <Input
              id="domain-prefix"
              name="prefix"
              placeholder="example"
              defaultValue={filters.prefix}
            />
          </Field>
          <Field className="sm:max-w-48">
            <FieldLabel htmlFor="domain-companies">Companies</FieldLabel>
            <NativeSelect
              id="domain-companies"
              name="companies"
              defaultValue={filters.companies}
            >
              <NativeSelectOption value="any">All domains</NativeSelectOption>
              <NativeSelectOption value="with">
                With companies
              </NativeSelectOption>
              <NativeSelectOption value="without">
                Without companies
              </NativeSelectOption>
            </NativeSelect>
          </Field>
          <Field className="sm:max-w-52">
            <FieldLabel htmlFor="domain-webtech">Webtech detections</FieldLabel>
            <NativeSelect
              id="domain-webtech"
              name="webtech"
              defaultValue={filters.webtech}
            >
              <NativeSelectOption value="any">All domains</NativeSelectOption>
              <NativeSelectOption value="with">
                With detections
              </NativeSelectOption>
              <NativeSelectOption value="without">
                Without detections
              </NativeSelectOption>
            </NativeSelect>
          </Field>
          <Button type="submit" disabled={busy}>
            {busy ? "Loading…" : "Apply filters"}
          </Button>
          <Button
            variant="ghost"
            nativeButton={false}
            render={<Link to="/admin/domains" />}
          >
            Reset
          </Button>
        </FieldGroup>
      </Form>
      <p className="text-muted-foreground text-xs">
        Technology counts are distinct names across recorded evidence. Webtech
        counts require stored detections, including partial scans. Archived and
        DNS evidence are shown separately.
      </p>
      <WorkspaceDomainsTable rows={rows} />
      <div className="flex items-center justify-between gap-3">
        <span className="text-muted-foreground text-sm">
          {rows.length} domains shown · alphabetical order
        </span>
        <div className="flex gap-2">
          {after ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceDomainsHref(filters)} />}
            >
              First page
            </Button>
          ) : null}
          {hasMore ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceDomainsHref(filters, next)} />}
            >
              Next page
            </Button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
