import { Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-domains";
import { WorkspaceDomainsTable } from "~/components/admin/workspace-domains-table";
import { Button } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel, FieldSet, FieldLegend } from "~/components/ui/field";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "~/components/ui/native-select";
import {
  DOMAIN_SOURCES,
  DOMAIN_SOURCE_LABELS,
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
  const { rows, filters, after, next, hasMore, total, refreshedAt, websiteInventoryTotal } = loaderData;
  const busy = useNavigation().state !== "idle";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Domains</h1>
        <p className="text-muted-foreground max-w-4xl text-sm">
          Canonical domain inventory with recorded DNS, website and company associations.
          Expand a domain to inspect its websites.
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
          <Field className="sm:max-w-48">
            <FieldLabel htmlFor="domain-dns">DNS records</FieldLabel>
            <NativeSelect id="domain-dns" name="dns" defaultValue={filters.dns}>
              <NativeSelectOption value="any">All domains</NativeSelectOption>
              <NativeSelectOption value="with">With records</NativeSelectOption>
              <NativeSelectOption value="without">Without records</NativeSelectOption>
            </NativeSelect>
          </Field>
          <Field className="sm:max-w-52">
            <FieldLabel htmlFor="domain-websites">Websites</FieldLabel>
            <NativeSelect id="domain-websites" name="websites" defaultValue={filters.websites}>
              <NativeSelectOption value="any">All domains</NativeSelectOption>
              <NativeSelectOption value="with">With inventory websites</NativeSelectOption>
              <NativeSelectOption value="without">Without inventory websites</NativeSelectOption>
              <NativeSelectOption value="observed">With observed websites</NativeSelectOption>
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
        <FieldSet className="w-full">
          <FieldLegend variant="label">Sources</FieldLegend>
          <div className="flex flex-wrap items-center gap-4">
            {DOMAIN_SOURCES.map((source) => <Field key={source} orientation="horizontal" className="w-auto">
              <Checkbox aria-label={DOMAIN_SOURCE_LABELS[source]} id={`source-${source}`} name="source" value={source} defaultChecked={filters.sources.includes(source)} />
              <FieldLabel htmlFor={`source-${source}`}>{DOMAIN_SOURCE_LABELS[source]}</FieldLabel>
            </Field>)}
            <Field className="w-auto">
              <FieldLabel htmlFor="source-match" className="sr-only">Source matching</FieldLabel>
              <NativeSelect id="source-match" name="sourceMatch" defaultValue={filters.sourceMatch}>
                <NativeSelectOption value="any">Match any selected source</NativeSelectOption>
                <NativeSelectOption value="all">Match all selected sources</NativeSelectOption>
              </NativeSelect>
            </Field>
          </div>
        </FieldSet>
      </Form>
      <div className="text-muted-foreground text-sm" role="status">
        <p>{Number(total).toLocaleString("en-US")} domains in the inventory snapshot</p>
        <p className="text-xs">{refreshedAt ? `Filters refreshed ${refreshedAt} UTC` : "Domain search index has not been materialized yet."}</p>
        <p className="mt-1 text-xs">Website filters reflect the published website inventory; observed evidence does not confirm current reachability.</p>
      </div>
      {websiteInventoryTotal === "0" ? <p className="text-sm text-muted-foreground">The website inventory is empty. Website counts and filters will become useful after its first materialization and a refresh of this list.</p> : null}
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
