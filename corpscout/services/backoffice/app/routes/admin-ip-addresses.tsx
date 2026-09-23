import { useEffect } from "react";
import { Form, Link, useFetcher, useNavigation } from "react-router";
import type { Route } from "./+types/admin-ip-addresses";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
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
import {
  parseWorkspaceIpFilters,
  workspaceIpAddressesHref,
} from "~/lib/workspace-ip-addresses";
import {
  listWorkspaceIpAddresses,
  type WorkspaceIpStatistics,
} from "~/lib/workspace-ip-addresses.server";

function IpStatistics() {
  const { load, data, state } = useFetcher<{
    statistics: WorkspaceIpStatistics | null;
  }>();
  useEffect(() => {
    void load("/admin/ip-addresses/statistics");
  }, [load]);
  const statistics = data?.statistics;
  return (
    <section
      aria-label="IP inventory statistics"
      aria-live="polite"
      className="flex flex-col gap-2"
    >
      {statistics ? (
        <>
          <dl className="flex flex-wrap gap-x-12 gap-y-4">
            {[
              ["Total unique IP addresses", statistics.total],
              ["IPv4 addresses", statistics.ipv4],
              ["IPv6 addresses", statistics.ipv6],
            ].map(([label, value]) => (
              <div key={label} className="flex flex-col gap-1">
                <dt className="text-muted-foreground text-sm">{label}</dt>
                <dd className="text-2xl font-semibold tabular-nums">
                  {Number(value).toLocaleString("en-US")}
                </dd>
              </div>
            ))}
          </dl>
          <p className="text-muted-foreground text-xs">
            Entire DNS inventory, independent of filters · Counted{" "}
            {statistics.countedAt.replace("T", " ").slice(0, 19)} UTC · Cached
            for up to 5 minutes.
          </p>
        </>
      ) : data && state === "idle" ? (
        <div className="flex items-center gap-3">
          <p className="text-muted-foreground text-sm">
            Inventory counts are temporarily unavailable.
          </p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => void load("/admin/ip-addresses/statistics")}
          >
            Retry counts
          </Button>
        </div>
      ) : (
        <p className="text-muted-foreground text-sm" role="status">
          Counting unique IP addresses… The table is ready to browse.
        </p>
      )}
    </section>
  );
}

export async function loader({ request }: Route.LoaderArgs) {
  const params = new URL(request.url).searchParams;
  const filters = parseWorkspaceIpFilters(params);
  return {
    ...(await listWorkspaceIpAddresses(filters, params.get("after") ?? "")),
    filters,
  };
}

export function meta() {
  return [{ title: "IP addresses | CompanyCollect" }];
}

export default function WorkspaceIpAddresses({
  loaderData,
}: Route.ComponentProps) {
  const { rows, filters, after, next, hasMore } = loaderData;
  const busy = useNavigation().state !== "idle";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">IP addresses</h1>
        <p className="text-muted-foreground max-w-4xl text-sm">
          All IPv4 and IPv6 addresses observed by our DNS and zone-transfer
          scanners. Each address appears once, including historical records and
          addresses without enrichment.
        </p>
      </header>
      <IpStatistics />
      <Form method="get" key={JSON.stringify(filters)}>
        <FieldGroup className="sm:flex-row sm:items-end">
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="ip-search">IP address or prefix</FieldLabel>
            <Input
              id="ip-search"
              name="search"
              placeholder="116.203. or 2001:db8:"
              defaultValue={filters.search}
              maxLength={128}
            />
          </Field>
          <Field className="sm:max-w-48">
            <FieldLabel htmlFor="ip-version">Address type</FieldLabel>
            <NativeSelect
              id="ip-version"
              name="version"
              defaultValue={filters.version}
            >
              <NativeSelectOption value="any">All addresses</NativeSelectOption>
              <NativeSelectOption value="4">IPv4 · A</NativeSelectOption>
              <NativeSelectOption value="6">IPv6 · AAAA</NativeSelectOption>
            </NativeSelect>
          </Field>
          <Button type="submit" disabled={busy}>
            {busy ? "Loading…" : "Apply filters"}
          </Button>
          <Button
            variant="ghost"
            nativeButton={false}
            render={<Link to="/admin/ip-addresses" />}
          >
            Reset
          </Button>
        </FieldGroup>
      </Form>
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>IP address</TableHead>
              <TableHead>DNS type</TableHead>
              <TableHead>Location</TableHead>
              <TableHead>ASN / organization</TableHead>
              <TableHead>RDAP network</TableHead>
              <TableHead>First seen (UTC)</TableHead>
              <TableHead>Last seen (UTC)</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.ip}>
                <TableCell>
                  <span className="font-mono">{row.ip}</span>
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">
                    {row.ip_version === 4 ? "A · IPv4" : "AAAA · IPv6"}
                  </Badge>
                </TableCell>
                <TableCell>
                  {[row.city_name, row.country_iso_code]
                    .filter(Boolean)
                    .join(", ") || "—"}
                </TableCell>
                <TableCell>
                  <div className="flex flex-col gap-1">
                    <span>{row.asn === null ? "—" : `AS${row.asn}`}</span>
                    {row.asn_organization ? (
                      <span
                        className="text-muted-foreground max-w-60 truncate"
                        title={row.asn_organization}
                      >
                        {row.asn_organization}
                      </span>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-col gap-1">
                    <span className="font-mono">
                      {row.rdap_matched_cidr ?? "—"}
                    </span>
                    {row.rdap_name ? (
                      <span
                        className="text-muted-foreground max-w-52 truncate"
                        title={row.rdap_name}
                      >
                        {row.rdap_name}
                      </span>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell>
                  <time dateTime={`${row.first_seen.replace(" ", "T")}Z`}>
                    {row.first_seen.slice(0, 19)}
                  </time>
                </TableCell>
                <TableCell>
                  <time dateTime={`${row.last_seen.replace(" ", "T")}Z`}>
                    {row.last_seen.slice(0, 19)}
                  </time>
                </TableCell>
              </TableRow>
            ))}
            {!rows.length ? (
              <TableRow>
                <TableCell colSpan={7}>
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No matching IP addresses</EmptyTitle>
                      <EmptyDescription>
                        Try a different address or prefix, or reset the filters.
                      </EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-muted-foreground text-sm">
          {rows.length} {rows.length === 1 ? "address" : "addresses"} shown ·
          Dates reflect recorded DNS observations.
        </p>
        <div className="flex gap-2">
          {after ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceIpAddressesHref(filters)} />}
            >
              First page
            </Button>
          ) : null}
          {hasMore ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceIpAddressesHref(filters, next)} />}
            >
              Next page
            </Button>
          ) : null}
        </div>
      </div>
      <p className="text-muted-foreground text-xs">
        Address inventory: <code>corpscout.commoncrawl_ip_addresses</code>.{" "}
        Location and ASN include earlier GeoIP lookups. RDAP networks appear as
        addresses are processed by IP enrichment. A dash means no data is
        available.
      </p>
    </div>
  );
}

export function ErrorBoundary() {
  return (
    <div className="p-4 md:p-6">
      <Alert variant="destructive">
        <AlertTitle>Unable to load IP addresses</AlertTitle>
        <AlertDescription>
          Try again, or <Link to="/admin/ip-addresses">reset the filters</Link>.
        </AlertDescription>
      </Alert>
    </div>
  );
}
