import type { Route } from "./+types/admin-graph";
import { useEffect, useState } from "react";
import {
  ArrowLeftRightIcon,
  CheckIcon,
  LoaderCircleIcon,
  NetworkIcon,
  SearchIcon,
} from "lucide-react";
import { Form, Link, useNavigation, useRevalidator } from "react-router";
import { DataTablePagination } from "~/components/data-table/pagination";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { commonCrawlDomainPath } from "~/lib/common-crawl";
import {
  GRAPH_DIRECTIONS,
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
const directionLabels = {
  all: "All connections",
  mutual: "Mutual links",
  outgoing: "Outgoing only",
  incoming: "Incoming only",
};

export async function loader({ request }: Route.LoaderArgs) {
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
        "This graph release is not available. Choose a published release.";
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
    result,
    error,
    failed,
  };
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
    result,
    error,
    failed,
  } = loaderData;
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
    if (!importing?.active) return;
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
  }, [importing?.active, navigation.state, refreshState, revalidate]);

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6" aria-busy={loading}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Graph</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Explore domain connections from Common Crawl. See which websites link
          out, which link back, and where the connection is mutual.
        </p>
      </header>

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
        <section
          className="flex flex-col gap-4"
          aria-label="Domain connections"
        >
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 className="break-all text-lg font-semibold">{search.domain}</h2>
            <p className="text-sm text-muted-foreground tabular-nums">
              {nf.format(result.counts.all)} connected domains ·{" "}
              {nf.format(result.counts.mutual)} mutual
            </p>
          </div>
          <nav
            className="flex flex-wrap gap-2"
            aria-label="Connection direction"
          >
            {GRAPH_DIRECTIONS.map((direction) => (
              <Button
                key={direction}
                variant={search.direction === direction ? "secondary" : "ghost"}
                size="sm"
                nativeButton={false}
                render={
                  <Link
                    to={domainGraphPath({ ...search, direction, page: 1 })}
                    aria-current={
                      search.direction === direction ? "page" : undefined
                    }
                    preventScrollReset
                  />
                }
              >
                {directionLabels[direction]}{" "}
                <span className="text-muted-foreground tabular-nums">
                  {nf.format(result.counts[direction])}
                </span>
              </Button>
            ))}
          </nav>
          {result.rows.length === 0 ? (
            <Empty className="min-h-48 border">
              <EmptyHeader>
                <EmptyTitle>
                  {result.counts.all === 0
                    ? "No connections in this release"
                    : "No connections in this direction"}
                </EmptyTitle>
                <EmptyDescription>
                  {result.counts.all === 0
                    ? "The domain exists in this graph, but has no links to other domains."
                    : "Choose another direction to see this domain’s connections."}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <div className="overflow-hidden rounded-lg border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="min-w-64">Connected domain</TableHead>
                    <TableHead>Links from searched domain</TableHead>
                    <TableHead>Links back to searched domain</TableHead>
                    <TableHead>Website evidence</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {result.rows.map((row) => (
                    <TableRow key={row.connected_domain}>
                      <TableCell className="py-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link
                            to={domainGraphPath({
                              domain: row.connected_domain,
                              release: search.release,
                              pageSize: search.pageSize,
                            })}
                            className="font-mono font-medium underline-offset-4 hover:underline"
                          >
                            {row.connected_domain}
                          </Link>
                          {row.reciprocal === 1 ? (
                            <Badge variant="secondary">
                              <ArrowLeftRightIcon />
                              Mutual
                            </Badge>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell>
                        {row.outgoing === 1 ? (
                          <span className="inline-flex items-center gap-1.5">
                            <CheckIcon className="size-3.5" />
                            Yes
                          </span>
                        ) : (
                          <span className="text-muted-foreground">No</span>
                        )}
                      </TableCell>
                      <TableCell>
                        {row.incoming === 1 ? (
                          <span className="inline-flex items-center gap-1.5">
                            <CheckIcon className="size-3.5" />
                            Yes
                          </span>
                        ) : (
                          <span className="text-muted-foreground">No</span>
                        )}
                      </TableCell>
                      <TableCell>
                        <Link
                          to={commonCrawlDomainPath(row.connected_domain)}
                          className="text-muted-foreground underline-offset-4 hover:text-foreground hover:underline"
                          aria-label={`Inspect website evidence for ${row.connected_domain}`}
                        >
                          Inspect
                        </Link>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
          <DataTablePagination
            total={result.total}
            page={result.page}
            pageSize={result.pageSize}
            itemsLabel="connected domains"
          />
          <p className="text-xs text-muted-foreground">
            Links indicate a connection in the crawl, including links to shared
            services. They do not establish common ownership.
          </p>
        </section>
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
