import {
  ArrowLeft,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  Network,
  Server,
} from "lucide-react";
import { Link, useLocation } from "react-router";
import {
  formatObservedAt,
  RdapRegistrationDetails,
} from "~/components/detail/technology-infrastructure-section";
import { Badge } from "~/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  TechnologyIpDetail,
  TechnologyIpDomainConnectionPage,
} from "~/lib/queries.server";

const numberFormat = new Intl.NumberFormat("en-US");

function ConnectionPagination({
  connectionPage,
  parameter,
}: {
  connectionPage: TechnologyIpDomainConnectionPage;
  parameter: "exactPage" | "segmentPage";
}) {
  const location = useLocation();

  function href(page: number): string {
    const search = new URLSearchParams(location.search);
    if (page === 1) search.delete(parameter);
    else search.set(parameter, String(page));
    const query = search.toString();
    return `${location.pathname}${query ? `?${query}` : ""}`;
  }

  if (connectionPage.page === 1 && !connectionPage.hasMore) return null;
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-muted-foreground text-sm tabular-nums">
        {connectionPage.total === null
          ? `Page ${numberFormat.format(connectionPage.page)}`
          : `${numberFormat.format(connectionPage.total)} domain connection${connectionPage.total === 1 ? "" : "s"}`}
      </p>
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          disabled={connectionPage.page <= 1}
          render={
            connectionPage.page > 1 ? (
              <Link to={href(connectionPage.page - 1)} preventScrollReset />
            ) : undefined
          }
          nativeButton={connectionPage.page <= 1}
        >
          <ChevronLeft />
          Previous
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={!connectionPage.hasMore}
          render={
            connectionPage.hasMore ? (
              <Link to={href(connectionPage.page + 1)} preventScrollReset />
            ) : undefined
          }
          nativeButton={!connectionPage.hasMore}
        >
          Next
          <ChevronRight />
        </Button>
      </div>
    </div>
  );
}

function ConnectionsTable({
  connectionPage,
  companyDomain,
  showAddress,
}: {
  connectionPage: TechnologyIpDomainConnectionPage;
  companyDomain: string | null;
  showAddress: boolean;
}) {
  return (
    <div className="overflow-hidden rounded-xl border">
      <Table>
        <TableHeader>
          <TableRow>
            {showAddress ? <TableHead>Address</TableHead> : null}
            <TableHead>Domain</TableHead>
            <TableHead>Observed hostnames</TableHead>
            <TableHead>Evidence</TableHead>
            <TableHead>Observation window</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {connectionPage.connections.map((connection) => (
            <TableRow key={`${connection.ip}:${connection.domain}`}>
              {showAddress ? (
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">IPv{connection.version}</Badge>
                    <Link
                      to={`/ip/${encodeURIComponent(connection.ip)}`}
                      className="font-mono text-xs font-medium underline-offset-4 hover:underline"
                    >
                      {connection.ip}
                    </Link>
                  </div>
                </TableCell>
              ) : null}
              <TableCell className="max-w-xs whitespace-normal">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-mono text-xs font-medium">
                    {connection.domain}
                  </span>
                  {companyDomain && connection.domain === companyDomain ? (
                    <Badge variant="secondary">This company</Badge>
                  ) : null}
                </div>
              </TableCell>
              <TableCell className="max-w-sm whitespace-normal">
                <p className="break-all font-mono text-xs">
                  {connection.hostnames.slice(0, 2).join(", ")}
                </p>
                {connection.hostnames.length > 2 ? (
                  <p className="text-muted-foreground mt-1 text-xs">
                    +{connection.hostnames.length - 2} more
                  </p>
                ) : null}
              </TableCell>
              <TableCell className="max-w-xs whitespace-normal">
                <div className="flex flex-wrap gap-1.5">
                  {connection.discoveries.map((discovery) => (
                    <Badge key={discovery} variant="outline">
                      {discovery}
                    </Badge>
                  ))}
                  {connection.sources.map((source) => (
                    <Badge key={source} variant="secondary">
                      {source}
                    </Badge>
                  ))}
                  {connection.discoveries.length === 0 &&
                  connection.sources.length === 0 ? (
                    <span className="text-muted-foreground text-xs">
                      Historical DNS record
                    </span>
                  ) : null}
                </div>
              </TableCell>
              <TableCell className="text-muted-foreground text-xs tabular-nums">
                <p>{formatObservedAt(connection.firstSeen)}</p>
                <p className="mt-1">
                  to {formatObservedAt(connection.lastSeen)}
                </p>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function EmptyConnections({ children }: { children: React.ReactNode }) {
  return (
    <Empty className="border border-dashed">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Server />
        </EmptyMedia>
        <EmptyTitle>No other connections observed</EmptyTitle>
        <EmptyDescription>{children}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

export function TechnologyIpAddressDetail({
  detail,
  companyContext,
  backLink,
}: {
  detail: TechnologyIpDetail;
  companyContext?: {
    domain: string;
    hostnames: string[];
  };
  backLink?: {
    label: string;
    to: string;
    relative?: "route" | "path";
  };
}) {
  const location = [detail.address.cityName, detail.address.countryName]
    .filter(Boolean)
    .join(", ");

  return (
    <div className="flex flex-col gap-5">
      {backLink ? (
        <div>
          <Button
            variant="ghost"
            size="sm"
            render={
              <Link to={backLink.to} relative={backLink.relative ?? "route"} />
            }
            nativeButton={false}
          >
            <ArrowLeft data-icon="inline-start" />
            {backLink.label}
          </Button>
        </div>
      ) : null}

      {detail.historyIndexCoverage.completedPartitions <
      detail.historyIndexCoverage.totalPartitions ? (
        <Alert>
          <CircleAlert />
          <AlertTitle>
            Historical relationship index is still loading
          </AlertTitle>
          <AlertDescription>
            {detail.historyIndexCoverage.completedPartitions} of{" "}
            {detail.historyIndexCoverage.totalPartitions} historical DNS
            partitions have been validated. Exact-IP and network-segment results
            can be incomplete until replay finishes; new DNS observations are
            indexed immediately.
          </AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">IPv{detail.address.version}</Badge>
                <CardTitle className="break-all font-mono">
                  {detail.address.ip}
                </CardTitle>
              </div>
              {companyContext ? (
                <CardDescription className="mt-2">
                  Resolved by {companyContext.hostnames.length} hostname
                  {companyContext.hostnames.length === 1 ? "" : "s"} under{" "}
                  <span className="font-mono text-foreground">
                    {companyContext.domain}
                  </span>
                  .
                </CardDescription>
              ) : (
                <CardDescription className="mt-2">
                  Observed across{" "}
                  {numberFormat.format(detail.exactConnections.total ?? 0)}{" "}
                  domain connection
                  {detail.exactConnections.total === 1 ? "" : "s"} in historical
                  A and AAAA records.
                </CardDescription>
              )}
            </div>
            <Badge variant="secondary">
              Last observed {formatObservedAt(detail.address.lastSeen)}
            </Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-5 lg:grid-cols-3">
            <div>
              <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                Location
              </p>
              <p className="mt-2 font-medium">{location || "Unknown"}</p>
              <p className="text-muted-foreground mt-1 text-xs">
                GeoIP location is approximate.
              </p>
            </div>
            <div>
              <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                Routing network
              </p>
              <p className="mt-2 font-medium">
                {detail.address.asn ? `AS${detail.address.asn}` : "Unknown ASN"}
              </p>
              <p className="text-muted-foreground mt-1 text-xs">
                {detail.address.asnOrganization ?? "Organization unavailable"}
              </p>
              <p className="mt-2 font-mono text-xs">
                Segment {detail.address.networkSegment}
              </p>
            </div>
            <div>
              <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                {companyContext ? "Company observation" : "DNS observation"}
              </p>
              <p className="mt-2 text-sm tabular-nums">
                {formatObservedAt(detail.address.firstSeen)}
              </p>
              <p className="text-muted-foreground mt-1 text-xs tabular-nums">
                through {formatObservedAt(detail.address.lastSeen)}
              </p>
            </div>
          </div>
          <Separator className="my-5" />
          <div>
            <p className="text-muted-foreground mb-3 text-xs font-medium uppercase tracking-wide">
              RDAP address-space registration
            </p>
            <RdapRegistrationDetails address={detail.address} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-start gap-3">
            <Server className="text-muted-foreground mt-0.5 size-5" />
            <div>
              <CardTitle>Domains on this exact IP</CardTitle>
              <CardDescription className="mt-1">
                These domains have historical A or AAAA evidence pointing to{" "}
                <span className="font-mono text-foreground">
                  {detail.address.ip}
                </span>
                . Shared hosting and CDNs can place unrelated domains on one
                address, so this is technical association rather than ownership.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {detail.exactConnections.connections.length ? (
            <ConnectionsTable
              connectionPage={detail.exactConnections}
              companyDomain={companyContext?.domain ?? null}
              showAddress={false}
            />
          ) : (
            <EmptyConnections>
              No domain history is indexed for this exact address.
            </EmptyConnections>
          )}
          <ConnectionPagination
            connectionPage={detail.exactConnections}
            parameter="exactPage"
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-start gap-3">
            <Network className="text-muted-foreground mt-0.5 size-5" />
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <CardTitle>Other domains in the network segment</CardTitle>
                <Badge variant="outline" className="font-mono">
                  {detail.address.networkSegment}
                </Badge>
              </div>
              <CardDescription className="mt-1">
                Domains below resolve to other addresses inside the same stable
                IPv{detail.address.version === 4 ? "4 /24" : "6 /48"} segment.
                This is an infrastructure-neighborhood signal and is weaker than
                sharing the exact IP.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {detail.segmentConnections.connections.length ? (
            <>
              <ConnectionsTable
                connectionPage={detail.segmentConnections}
                companyDomain={companyContext?.domain ?? null}
                showAddress
              />
              <ConnectionPagination
                connectionPage={detail.segmentConnections}
                parameter="segmentPage"
              />
            </>
          ) : (
            <EmptyConnections>
              No other domain-to-address relationships were observed in this
              network segment.
            </EmptyConnections>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
