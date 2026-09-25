import {
  graphDashboard,
  graphAction,
  GraphRequestError,
} from "~/lib/commoncrawl-graph.server";
import { GraphReleases } from "~/components/admin/graph-releases";
import type { Route } from "./+types/admin-graph";
import { useEffect, useState } from "react";
import { LoaderCircleIcon, NetworkIcon, SearchIcon } from "lucide-react";
import {
  Form,
  useNavigation,
  useRevalidator,
  useActionData,
} from "react-router";
import { DomainConnections } from "~/components/admin/domain-connections";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "~/components/ui/native-select";
import {
  domainGraphPath,
  graphDomainError,
  parseDomainGraphSearch,
} from "~/lib/domain-graph";
import {
  getDomainGraphReleases,
  getDomainGraphImports,
  searchDomainGraph,
} from "~/lib/domain-graph.server";
import type {
  DomainGraphRelease,
  DomainGraphImport,
  DomainGraphResult,
} from "~/lib/domain-graph.server";

const nf = new Intl.NumberFormat("en-US");

export async function loader({ request }: Route.LoaderArgs) {
  const dashboard = await graphDashboard().catch(() => null);
  const search = parseDomainGraphSearch(new URL(request.url));
  let releases: DomainGraphRelease[] = [];
  let imports: DomainGraphImport[] = [];
  let importStatusError = false;
  let result: DomainGraphResult | null = null;
  let error = graphDomainError(search.domain);
  let failed = false;
  try {
    const [snapshots, runs] = await Promise.allSettled([
      getDomainGraphReleases(),
      getDomainGraphImports(),
    ]);
    if (snapshots.status === "rejected") throw snapshots.reason;
    releases = snapshots.value;
    if (runs.status === "fulfilled") {
      imports = runs.value.filter(
        (run) =>
          !releases.some((item) => item.graph_release === run.graph_release),
      );
    } else {
      importStatusError = true;
    }
    search.release ||=
      releases[0]?.graph_release ?? imports[0]?.graph_release ?? "";
    const published = releases.some(
      (item) => item.graph_release === search.release,
    );
    if (
      search.release &&
      !published &&
      !imports.some((item) => item.graph_release === search.release)
    ) {
      error =
        dashboard?.releases.find((r) => r.graph_release === search.release)
          ?.graph_status === "retired"
          ? "This full graph was retired after a newer release became active. Its rankings remain available in domain history."
          : "This graph release is not available. Choose the active release.";
    }
    if (!error && search.domain && published) {
      result = await searchDomainGraph(search);
    }
  } catch {
    failed = true;
    error =
      "The graph could not be loaded. Please retry. Large domains may take longer to query.";
  }
  return {
    search: {
      ...search,
      page: result?.page ?? search.page,
      pageSize: result?.pageSize ?? search.pageSize,
    },
    releases,
    imports,
    importStatusError,
    dashboard,
    result,
    error,
    failed,
  };
}

export async function action({ request }: Route.ActionArgs) {
  if (
    request.method !== "POST" ||
    request.headers.get("origin") !== new URL(request.url).origin
  )
    throw new Response("Forbidden", { status: 403 });
  try {
    return {
      message: await graphAction(await request.formData()),
      error: false,
    };
  } catch (error) {
    return {
      message:
        error instanceof GraphRequestError
          ? error.message
          : "The request could not be completed. Refresh the page to check its status before retrying.",
      error: true,
    };
  }
}

export function meta() {
  return [{ title: "Graph | CompanyCollect" }];
}

export default function AdminGraph({ loaderData }: Route.ComponentProps) {
  const {
    search,
    releases,
    imports,
    importStatusError,
    dashboard,
    result,
    error,
    failed,
  } = loaderData;
  const feedback = useActionData<typeof action>();
  const [selectedRelease, setSelectedRelease] = useState(search.release);
  useEffect(() => setSelectedRelease(search.release), [search.release]);
  const navigation = useNavigation();
  const revalidator = useRevalidator();
  const loading = navigation.state !== "idle" || revalidator.state !== "idle";
  const release = releases.find(
    (item) => item.graph_release === selectedRelease,
  );
  const resultRelease = releases.find(
    (item) => item.graph_release === search.release,
  );
  const importing = imports.find(
    (item) => item.graph_release === selectedRelease,
  );
  const { revalidate, state: refreshState } = revalidator;
  useEffect(() => {
    if (
      !importing?.active &&
      !dashboard?.releases.some((r) =>
        ["queued", "launching", "running"].includes(r.status ?? ""),
      )
    )
      return;
    const refresh = () => {
      if (
        document.visibilityState === "visible" &&
        refreshState === "idle" &&
        navigation.state === "idle"
      )
        void revalidate();
    };
    const timer = window.setInterval(refresh, 10_000);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, [
    importing?.active,
    dashboard,
    navigation.state,
    refreshState,
    revalidate,
  ]);

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6" aria-busy={loading}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Graph</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Explore domain connections from Common Crawl. See which websites link
          out, which link back, and where the connection is mutual.
        </p>
      </header>

      {feedback && (
        <Alert variant={feedback.error ? "destructive" : "default"}>
          <AlertDescription>{feedback.message}</AlertDescription>
        </Alert>
      )}
      {dashboard ? (
        <GraphReleases dashboard={dashboard} busy={loading} />
      ) : (
        <Alert>
          <AlertDescription>
            Release management is unavailable. The catalog connection and
            migrations must be configured.
          </AlertDescription>
        </Alert>
      )}

      <Form
        key={`${search.domain}\u0000${search.release}`}
        method="get"
        className="flex flex-col gap-3"
      >
        <input type="hidden" name="pageSize" value={search.pageSize} />
        <FieldGroup className="flex flex-col gap-4 sm:flex-row sm:items-start">
          <Field className="sm:max-w-md">
            <FieldLabel htmlFor="graph-domain">Domain</FieldLabel>
            <Input
              id="graph-domain"
              name="domain"
              defaultValue={search.domain}
              placeholder="example.com"
              autoComplete="off"
              required
              aria-invalid={!!error && !failed}
              aria-describedby="graph-domain-hint"
            />
            <FieldDescription id="graph-domain-hint">
              Use the root domain, such as example.co.uk.
            </FieldDescription>
          </Field>
          <Field className="sm:w-auto">
            <FieldLabel htmlFor="graph-release">Graph release</FieldLabel>
            <NativeSelect
              id="graph-release"
              name="release"
              value={selectedRelease}
              onChange={(event) => setSelectedRelease(event.target.value)}
              disabled={releases.length + imports.length === 0}
              className="w-full sm:w-80"
            >
              {releases.length + imports.length === 0 ? (
                <NativeSelectOption value="">
                  No graph releases
                </NativeSelectOption>
              ) : null}
              {releases.map((item) => (
                <NativeSelectOption
                  key={item.graph_release}
                  value={item.graph_release}
                >
                  {item.graph_release}
                </NativeSelectOption>
              ))}
              {imports.map((item) => (
                <NativeSelectOption
                  key={item.graph_release}
                  value={item.graph_release}
                >
                  {item.graph_release} — {item.label}
                </NativeSelectOption>
              ))}
            </NativeSelect>
          </Field>
          <Button
            type="submit"
            className="sm:mt-6"
            disabled={loading || !release}
          >
            {loading ? (
              <LoaderCircleIcon className="animate-spin" />
            ) : (
              <SearchIcon />
            )}
            {loading ? "Searching…" : "Search domain"}
          </Button>
        </FieldGroup>
      </Form>

      {importStatusError ? (
        <Alert>
          <AlertTitle>Import status unavailable</AlertTitle>
          <AlertDescription>
            Dagster could not be reached. Published releases can still be
            searched.
          </AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>
            {failed ? "Graph unavailable" : "Check your search"}
          </AlertTitle>
          <AlertDescription>
            <p>{error}</p>
            {failed ? (
              <Button
                variant="outline"
                size="sm"
                onClick={() => revalidator.revalidate()}
                disabled={loading}
              >
                Retry
              </Button>
            ) : null}
          </AlertDescription>
        </Alert>
      ) : importing ? (
        <Empty className="min-h-64 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              {importing.active ? (
                <LoaderCircleIcon className="animate-spin" />
              ) : (
                <NetworkIcon />
              )}
            </EmptyMedia>
            <EmptyTitle>{importing.label}</EmptyTitle>
            <EmptyDescription>
              {importing.graph_release}.{" "}
              {importing.active
                ? "Search will become available when the full graph is imported and validated. This page checks automatically every 10 seconds."
                : "This run has not published a searchable graph. Open the Dagster run to inspect its status."}
            </EmptyDescription>
          </EmptyHeader>
          <div className="flex gap-2">
            {importing.run_url ? (
              <Button
                variant="outline"
                nativeButton={false}
                render={
                  <a
                    href={importing.run_url}
                    target="_blank"
                    rel="noreferrer"
                  />
                }
              >
                View import in Dagster
              </Button>
            ) : null}
            <Button
              variant="ghost"
              onClick={() => revalidator.revalidate()}
              disabled={loading}
            >
              Check again
            </Button>
          </div>
        </Empty>
      ) : releases.length === 0 ? (
        <Empty className="min-h-64 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <NetworkIcon />
            </EmptyMedia>
            <EmptyTitle>No graph release is ready yet</EmptyTitle>
            <EmptyDescription>
              A release becomes searchable after its domains and links finish
              importing and are validated.
            </EmptyDescription>
          </EmptyHeader>
          <Button
            variant="outline"
            onClick={() => revalidator.revalidate()}
            disabled={loading}
          >
            Check again
          </Button>
        </Empty>
      ) : !result ? (
        <Empty className="min-h-64 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <NetworkIcon />
            </EmptyMedia>
            <EmptyTitle>Explore a domain’s connections</EmptyTitle>
            <EmptyDescription>
              Search a domain to see its neighbors. Open any connected domain to
              continue exploring.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : !result.found ? (
        <Empty className="min-h-64 border">
          <EmptyHeader>
            <EmptyTitle>Domain not found in this release</EmptyTitle>
            <EmptyDescription>
              {search.domain} is absent from this graph. Check the root domain
              or try another release.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <DomainConnections
          direction={search.direction}
          result={result}
          title={
            <h2 className="break-all text-lg font-semibold">{search.domain}</h2>
          }
          directionHref={(direction) =>
            domainGraphPath({ ...search, direction, page: 1 })
          }
          connectedDomainHref={(domain) =>
            domainGraphPath({
              domain,
              release: search.release,
              pageSize: search.pageSize,
            })
          }
        />
      )}
      {resultRelease ? (
        <p className="text-xs text-muted-foreground">
          {resultRelease.graph_release} ·{" "}
          {nf.format(Number(resultRelease.node_count))} domains ·{" "}
          {nf.format(Number(resultRelease.edge_count))} directed links
        </p>
      ) : null}
    </div>
  );
}
