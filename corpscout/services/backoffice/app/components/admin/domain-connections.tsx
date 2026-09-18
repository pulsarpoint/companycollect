import type { ReactNode } from "react";
import { ArrowLeftRightIcon, CheckIcon } from "lucide-react";
import { Link } from "react-router";
import { DataTablePagination } from "~/components/data-table/pagination";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { commonCrawlDomainPath } from "~/lib/common-crawl";
import { GRAPH_DIRECTIONS, type GraphDirection } from "~/lib/domain-graph";
// Type-only: erased at build, so the component never drags ClickHouse into the
// client bundle (see CLAUDE.md, "Route modules and .server files").
import type { DomainGraphResult } from "~/lib/domain-graph.server";

const nf = new Intl.NumberFormat("en-US");

export const GRAPH_DIRECTION_LABELS: Record<GraphDirection, string> = {
  all: "All connections",
  mutual: "Mutual links",
  outgoing: "Outgoing only",
  incoming: "Incoming only",
};

/**
 * One domain's neighbours in a Common Crawl graph release, as the Graph page
 * shows them: the counts line, the direction tabs, the connections table (each
 * neighbour a link, mutual pairs badged, both link directions spelled out, a
 * website-evidence link) and the pagination footer.
 *
 * Shared by `/admin/graph` and the SE domain page, which differ only in where
 * a direction tab or a neighbour leads -- hence the two href builders -- and in
 * whether neighbours are annotated (`companyCounts`: how many SE companies
 * claim each neighbour; absent = not an SE company domain).
 */
export function DomainConnections({
  direction,
  result,
  title,
  directionHref,
  connectedDomainHref,
  companyCounts,
}: {
  direction: GraphDirection;
  /** A found result: callers render their own not-found state. */
  result: DomainGraphResult;
  /** The left side of the header row; the counts line is the right side. */
  title: ReactNode;
  directionHref: (direction: GraphDirection) => string;
  connectedDomainHref: (domain: string) => string;
  companyCounts?: Record<string, number>;
}) {
  return (
    <section className="flex flex-col gap-4" aria-label="Domain connections">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        {title}
        <p className="text-sm text-muted-foreground tabular-nums">
          {nf.format(result.counts.all)} connected domains · {nf.format(result.counts.mutual)}{" "}
          mutual
        </p>
      </div>
      <nav className="flex flex-wrap gap-2" aria-label="Connection direction">
        {GRAPH_DIRECTIONS.map((item) => (
          <Button
            key={item}
            variant={direction === item ? "secondary" : "ghost"}
            size="sm"
            nativeButton={false}
            render={
              <Link
                to={directionHref(item)}
                aria-current={direction === item ? "page" : undefined}
                preventScrollReset
              />
            }
          >
            {GRAPH_DIRECTION_LABELS[item]}{" "}
            <span className="text-muted-foreground tabular-nums">
              {nf.format(result.counts[item])}
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
              {result.rows.map((row) => {
                const companies = companyCounts?.[row.connected_domain] ?? 0;
                return (
                  <TableRow key={row.connected_domain}>
                    <TableCell className="py-3">
                      <div className="flex flex-wrap items-center gap-2">
                        <Link
                          to={connectedDomainHref(row.connected_domain)}
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
                        {companies > 0 ? (
                          <Badge variant="outline">
                            {companies === 1 ? "SE company" : `${nf.format(companies)} SE companies`}
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
                );
              })}
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
        Links indicate a connection in the crawl, including links to shared services. They do
        not establish common ownership.
      </p>
    </section>
  );
}
