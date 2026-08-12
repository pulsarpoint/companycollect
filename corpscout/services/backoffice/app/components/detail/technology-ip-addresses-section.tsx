import { Globe2, Network, Router, Server } from "lucide-react";
import { Link, useLocation } from "react-router";
import { DataTablePagination } from "~/components/data-table/pagination";
import { formatObservedAt } from "~/components/detail/technology-infrastructure-section";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { CompanyTechnologyIpInventory } from "~/lib/queries.server";

const numberFormat = new Intl.NumberFormat("en-US");

export function TechnologyIpAddressesSection({
  inventory,
}: {
  inventory: CompanyTechnologyIpInventory;
}) {
  const { summary } = inventory;
  const location = useLocation();

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>IP address inventory</CardTitle>
          <CardDescription>
            Addresses observed in A and AAAA records for hostnames under{" "}
            <span className="font-mono text-foreground">
              {inventory.domain}
            </span>
            . Select an address to inspect exact-IP sharing and its stable
            network segment.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="flex gap-3">
              <Server className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.totalAddresses)} addresses
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  Distinct historical resolutions
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <Router className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.ipv4Addresses)} IPv4
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  A-record addresses
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <Globe2 className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.ipv6Addresses)} IPv6
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  AAAA-record addresses
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <Network className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.rdapRegisteredAddressesOnPage)}{" "}
                  registered
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  Trustworthy RDAP matches on this page
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Observed addresses</CardTitle>
          <CardDescription>
            Historical DNS association does not prove that the company owns or
            exclusively controls an address.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {inventory.addresses.length ? (
            <div className="overflow-hidden rounded-xl border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Address</TableHead>
                    <TableHead>Company hostnames</TableHead>
                    <TableHead>Location</TableHead>
                    <TableHead>Network</TableHead>
                    <TableHead>Network segment</TableHead>
                    <TableHead>Observed</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {inventory.addresses.map((address) => {
                    const registration = address.rdapRegistration;
                    return (
                      <TableRow key={address.ip}>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Badge variant="outline">
                              IPv{address.version}
                            </Badge>
                            <Link
                              to={`${location.pathname}/${encodeURIComponent(address.ip)}?domain=${encodeURIComponent(inventory.domain)}`}
                              className="font-mono text-xs font-medium underline-offset-4 hover:underline"
                            >
                              {address.ip}
                            </Link>
                          </div>
                        </TableCell>
                        <TableCell className="max-w-xs whitespace-normal">
                          <p className="font-mono text-xs">
                            {address.hostnames[0]}
                          </p>
                          {address.hostnames.length > 1 ? (
                            <p className="text-muted-foreground mt-1 text-xs">
                              +{address.hostnames.length - 1} more hostname
                              {address.hostnames.length === 2 ? "" : "s"}
                            </p>
                          ) : null}
                        </TableCell>
                        <TableCell className="text-sm">
                          {[address.cityName, address.countryName]
                            .filter(Boolean)
                            .join(", ") || "Unknown"}
                        </TableCell>
                        <TableCell className="max-w-xs whitespace-normal">
                          {address.asn ? (
                            <>
                              <p className="font-medium">AS{address.asn}</p>
                              <p className="text-muted-foreground text-xs">
                                {address.asnOrganization ??
                                  "Unknown organization"}
                              </p>
                            </>
                          ) : (
                            <span className="text-muted-foreground">
                              Unknown
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="max-w-xs whitespace-normal">
                          <div className="flex flex-col gap-1.5">
                            <span className="font-mono text-xs font-medium">
                              {address.networkSegment}
                            </span>
                            {registration ? (
                              <div className="flex flex-wrap items-center gap-1.5 text-xs">
                                <Badge variant="secondary">
                                  {registration.rir.toUpperCase()}
                                </Badge>
                                <span className="text-muted-foreground font-mono">
                                  {registration.matchedCidr}
                                </span>
                              </div>
                            ) : null}
                          </div>
                        </TableCell>
                        <TableCell className="text-muted-foreground text-xs tabular-nums">
                          <p>{formatObservedAt(address.firstSeen)}</p>
                          <p className="mt-1">
                            to {formatObservedAt(address.lastSeen)}
                          </p>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          ) : (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <Server />
                </EmptyMedia>
                <EmptyTitle>No IP addresses observed</EmptyTitle>
                <EmptyDescription>
                  The selected domain has no historical A or AAAA evidence yet.
                </EmptyDescription>
              </EmptyHeader>
              <EmptyContent />
            </Empty>
          )}

          {summary.totalAddresses > inventory.pageSize ? (
            <DataTablePagination
              total={summary.totalAddresses}
              page={inventory.page}
              pageSize={inventory.pageSize}
              itemsLabel="observed IP addresses"
            />
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
