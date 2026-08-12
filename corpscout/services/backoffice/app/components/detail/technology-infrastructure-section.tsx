import { Globe2, Network, Server, ShieldCheck } from "lucide-react";
import { Link } from "react-router";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { DataTablePagination } from "~/components/data-table/pagination";
import type {
  CompanyTechnologyInfrastructure,
  TechnologyDnsRecord,
  TechnologyHostname,
  TechnologyIpAddress,
} from "~/lib/queries.server";

const numberFormat = new Intl.NumberFormat("en-US");
const dateFormat = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

function parseClickHouseDate(value: string): Date {
  const normalized = value.includes("T") ? value : value.replace(" ", "T");
  return new Date(
    /[zZ]|[+-]\d\d:\d\d$/.test(normalized) ? normalized : `${normalized}Z`,
  );
}

export function formatObservedAt(value: string | null): string {
  if (!value) return "Not observed";
  const date = parseClickHouseDate(value);
  return Number.isNaN(date.getTime()) ? value : dateFormat.format(date);
}

function latestObservedAt(hostname: TechnologyHostname): string | null {
  const values = [hostname.certificateLastSeen, hostname.dnsLastSeen].filter(
    (value): value is string => Boolean(value),
  );
  return values.sort().at(-1) ?? null;
}

function wasSeenInLatestScan(
  record: TechnologyDnsRecord,
  scanResolvedAt: string | null,
): boolean {
  if (!scanResolvedAt) return false;
  return (
    parseClickHouseDate(record.lastSeen).getTime() ===
    parseClickHouseDate(scanResolvedAt).getTime()
  );
}

function ObservationWindow({
  title,
  source,
  firstSeen,
  lastSeen,
}: {
  title: string;
  source: string;
  firstSeen: string | null;
  lastSeen: string | null;
}) {
  return (
    <div className="rounded-lg border bg-muted/20 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="font-medium">{title}</p>
        <Badge variant="outline">{source}</Badge>
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-3 text-xs">
        <div>
          <dt className="text-muted-foreground">First observed</dt>
          <dd className="mt-1 tabular-nums">{formatObservedAt(firstSeen)}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Last observed</dt>
          <dd className="mt-1 tabular-nums">{formatObservedAt(lastSeen)}</dd>
        </div>
      </dl>
    </div>
  );
}

function DnsRecordsTable({
  hostname,
  scanResolvedAt,
}: {
  hostname: TechnologyHostname;
  scanResolvedAt: string | null;
}) {
  if (hostname.records.length === 0) {
    return (
      <p className="text-muted-foreground rounded-lg border border-dashed p-4 text-sm">
        No DNS records have been observed for this hostname. A certificate-log
        appearance does not mean the hostname currently resolves.
      </p>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Type</TableHead>
          <TableHead>Value</TableHead>
          <TableHead>First observed</TableHead>
          <TableHead>Last observed</TableHead>
          <TableHead>Evidence</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {hostname.records.map((record) => (
          <TableRow key={`${record.type}:${record.priority}:${record.value}`}>
            <TableCell>
              <Badge variant="outline">{record.type}</Badge>
            </TableCell>
            <TableCell className="max-w-md whitespace-normal">
              <span className="break-all font-mono text-xs">
                {record.value}
              </span>
            </TableCell>
            <TableCell className="text-muted-foreground text-xs tabular-nums">
              {formatObservedAt(record.firstSeen)}
            </TableCell>
            <TableCell className="text-muted-foreground text-xs tabular-nums">
              {formatObservedAt(record.lastSeen)}
            </TableCell>
            <TableCell>
              {wasSeenInLatestScan(record, scanResolvedAt) ? (
                <Badge variant="secondary">Latest scan</Badge>
              ) : (
                <Badge variant="outline">Historical</Badge>
              )}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function IpAddressesTable({
  hostname,
  domain,
  ipAddressesPath,
}: {
  hostname: TechnologyHostname;
  domain: string;
  ipAddressesPath: string;
}) {
  if (hostname.ipAddresses.length === 0) return null;

  return (
    <div className="mt-5">
      <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
        <h4 className="text-sm font-medium">Resolved IP addresses</h4>
        <span className="text-muted-foreground text-xs">
          RDAP identifies the registered address-space holder, not necessarily
          the hosting customer.
        </span>
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Address</TableHead>
            <TableHead>Location</TableHead>
            <TableHead>Network</TableHead>
            <TableHead>RDAP registration</TableHead>
            <TableHead>First observed</TableHead>
            <TableHead>Last observed</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {hostname.ipAddresses.map((address) => (
            <TableRow key={address.ip}>
              <TableCell>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">IPv{address.version}</Badge>
                  <Link
                    to={`${ipAddressesPath}/${encodeURIComponent(address.ip)}?domain=${encodeURIComponent(domain)}`}
                    className="font-mono text-xs font-medium underline-offset-4 hover:underline"
                  >
                    {address.ip}
                  </Link>
                </div>
              </TableCell>
              <TableCell className="text-sm">
                {[address.cityName, address.countryName]
                  .filter(Boolean)
                  .join(", ") || "Unknown"}
              </TableCell>
              <TableCell className="max-w-xs whitespace-normal">
                {address.asn ? (
                  <div>
                    <p className="font-medium">AS{address.asn}</p>
                    <p className="text-muted-foreground text-xs">
                      {address.asnOrganization ?? "Unknown organization"}
                    </p>
                  </div>
                ) : (
                  <span className="text-muted-foreground">Unknown</span>
                )}
              </TableCell>
              <TableCell className="max-w-sm whitespace-normal">
                <RdapRegistrationDetails address={address} />
              </TableCell>
              <TableCell className="text-muted-foreground text-xs tabular-nums">
                {formatObservedAt(address.firstSeen)}
              </TableCell>
              <TableCell className="text-muted-foreground text-xs tabular-nums">
                {formatObservedAt(address.lastSeen)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export function RdapRegistrationDetails({
  address,
}: {
  address: TechnologyIpAddress;
}) {
  const registration = address.rdapRegistration;
  if (!registration) {
    return (
      <span className="text-muted-foreground text-xs">
        No trustworthy RDAP match
      </span>
    );
  }

  const registrant = registration.registrantNames[0] ?? registration.name;
  const networkDescription = [
    registration.name !== registrant ? registration.name : null,
    registration.registrationType,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-1.5">
        <Badge variant="secondary">{registration.rir.toUpperCase()}</Badge>
        {registration.statuses.map((status) => (
          <Badge key={status} variant="outline">
            {status}
          </Badge>
        ))}
      </div>
      <div>
        <p className="font-medium">{registrant ?? "Registrant not named"}</p>
        {networkDescription ? (
          <p className="text-muted-foreground text-xs">{networkDescription}</p>
        ) : null}
      </div>
      <p className="font-mono text-xs">{registration.matchedCidr}</p>
      <p className="text-muted-foreground break-all text-xs">
        {registration.startAddress} – {registration.endAddress}
      </p>
      <p className="text-muted-foreground text-xs tabular-nums">
        {registration.registrationDate
          ? `Registered ${formatObservedAt(registration.registrationDate)}`
          : "Registration date unavailable"}
        {registration.lastChangedAt
          ? ` · Updated ${formatObservedAt(registration.lastChangedAt)}`
          : ""}
      </p>
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="text-muted-foreground font-mono">
          {registration.handle}
        </span>
        {registration.sourceUrl ? (
          <a
            href={registration.sourceUrl}
            target="_blank"
            rel="noreferrer"
            className="underline underline-offset-2"
          >
            Registry record
          </a>
        ) : null}
      </div>
    </div>
  );
}

function HostnameDetails({
  hostname,
  scanResolvedAt,
  domain,
  ipAddressesPath,
}: {
  hostname: TechnologyHostname;
  scanResolvedAt: string | null;
  domain: string;
  ipAddressesPath: string;
}) {
  return (
    <div className="border-t bg-muted/10 px-4 py-4 sm:px-5">
      <div className="grid gap-3 md:grid-cols-2">
        {hostname.evidence.includes("certificate") ? (
          <ObservationWindow
            title="Certificate-log observation"
            source={`${hostname.certificateSourceLogs.length} CT log${hostname.certificateSourceLogs.length === 1 ? "" : "s"}`}
            firstSeen={hostname.certificateFirstSeen}
            lastSeen={hostname.certificateLastSeen}
          />
        ) : null}
        {hostname.evidence.includes("dns") ? (
          <ObservationWindow
            title="DNS resolution observation"
            source={hostname.dnsDiscoverySource ?? "unknown discovery"}
            firstSeen={hostname.dnsFirstSeen}
            lastSeen={hostname.dnsLastSeen}
          />
        ) : null}
      </div>

      {hostname.certificateExpiresAt ? (
        <p className="text-muted-foreground mt-3 text-xs">
          Latest observed certificate expiry:{" "}
          {formatObservedAt(hostname.certificateExpiresAt)}
        </p>
      ) : null}

      <div className="mt-5">
        <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
          <h4 className="text-sm font-medium">DNS records</h4>
          <span className="text-muted-foreground text-xs">
            DNSSEC signatures are summarized at domain level; RRSIG rows are
            omitted.
          </span>
        </div>
        <DnsRecordsTable hostname={hostname} scanResolvedAt={scanResolvedAt} />
      </div>
      <IpAddressesTable
        hostname={hostname}
        domain={domain}
        ipAddressesPath={ipAddressesPath}
      />
    </div>
  );
}

function HostnameSummary({ hostname }: { hostname: TechnologyHostname }) {
  return (
    <div className="grid min-w-0 flex-1 grid-cols-1 items-center gap-3 pr-3 sm:grid-cols-[minmax(12rem,1fr)_auto_auto]">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span className="truncate font-mono font-medium">
            {hostname.hostname}
          </span>
          {hostname.isApex ? <Badge>Apex</Badge> : null}
          {hostname.isWildcard ? (
            <Badge variant="secondary">Wildcard</Badge>
          ) : null}
        </div>
        <p className="text-muted-foreground mt-1 text-xs tabular-nums">
          Last observed {formatObservedAt(latestObservedAt(hostname))}
        </p>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {hostname.evidence.includes("certificate") ? (
          <Badge variant="outline">Certificate log</Badge>
        ) : null}
        {hostname.evidence.includes("dns") ? (
          <Badge variant="secondary">DNS-confirmed</Badge>
        ) : null}
      </div>
      <div className="text-muted-foreground flex gap-3 text-xs tabular-nums">
        <span>
          {numberFormat.format(hostname.records.length)} record
          {hostname.records.length === 1 ? "" : "s"}
        </span>
        <span>
          {numberFormat.format(hostname.ipAddresses.length)} IP
          {hostname.ipAddresses.length === 1 ? "" : "s"}
        </span>
      </div>
    </div>
  );
}

export function TechnologyInfrastructureSection({
  infrastructure,
  ipAddressesPath,
}: {
  infrastructure: CompanyTechnologyInfrastructure;
  ipAddressesPath: string;
}) {
  const { summary, scan, hostnames } = infrastructure;

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle className="font-mono">
                {infrastructure.domain}
              </CardTitle>
              <CardDescription className="mt-1">
                Selected domain infrastructure observed in certificate logs and
                DNS scans.
              </CardDescription>
            </div>
            {scan ? (
              <Badge variant={scan.status === "done" ? "secondary" : "outline"}>
                DNS scan {scan.status}
              </Badge>
            ) : null}
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="flex gap-3">
              <Globe2 className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.totalHostnames)} observed
                  hostnames
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  Union of CT and DNS evidence
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <ShieldCheck className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.certificateHostnames)}{" "}
                  certificate names
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  Observed in public CT logs
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <Server className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.dnsHostnames)} DNS-confirmed
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  Answered with A, AAAA, or CNAME
                </p>
              </div>
            </div>
            <div className="flex gap-3">
              <Network className="text-muted-foreground mt-0.5 size-4" />
              <div>
                <p className="font-medium tabular-nums">
                  {numberFormat.format(summary.resolvedIpAddressesOnPage)}{" "}
                  resolved IPs
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  {numberFormat.format(summary.rdapRegisteredIpAddressesOnPage)}{" "}
                  with RDAP registration
                </p>
              </div>
            </div>
          </div>

          {scan ? (
            <div className="mt-4 flex flex-wrap items-center gap-x-4 gap-y-2 border-t pt-4 text-xs">
              <span className="text-muted-foreground">
                Last DNS scan {formatObservedAt(scan.resolvedAt)}
              </span>
              <span className="tabular-nums">
                {scan.queriesOk}/{scan.queriesTotal} queries answered
              </span>
              <Badge variant={scan.dnssecSigned ? "secondary" : "outline"}>
                DNSSEC {scan.dnssecSigned ? "signed" : "not confirmed"}
              </Badge>
              {scan.zoneTransferOpen ? (
                <Badge variant="destructive">Open zone transfer</Badge>
              ) : null}
              {scan.nameservers.length ? (
                <span className="text-muted-foreground">
                  {scan.nameservers.length} authoritative nameserver
                  {scan.nameservers.length === 1 ? "" : "s"}
                </span>
              ) : null}
            </div>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Hostname inventory</CardTitle>
          <CardDescription>
            Expand a hostname to inspect its observation windows, DNS records,
            resolved IP addresses, and RDAP address-space registrations. These
            observations show technical association with the domain, not legal
            ownership by the company.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {hostnames.length ? (
            <Accordion
              multiple
              defaultValue={[hostnames[0].hostname]}
              className="overflow-hidden rounded-xl border bg-background"
              aria-label="Observed hostname infrastructure"
            >
              {hostnames.map((hostname) => (
                <AccordionItem
                  key={hostname.hostname}
                  value={hostname.hostname}
                >
                  <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                    <HostnameSummary hostname={hostname} />
                  </AccordionTrigger>
                  <AccordionContent className="pb-0">
                    <HostnameDetails
                      hostname={hostname}
                      scanResolvedAt={scan?.resolvedAt ?? null}
                      domain={infrastructure.domain}
                      ipAddressesPath={ipAddressesPath}
                    />
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          ) : (
            <p className="text-muted-foreground rounded-lg border border-dashed p-6 text-center text-sm">
              No certificate-log or DNS hostname observations are available yet.
            </p>
          )}

          {summary.totalHostnames > infrastructure.pageSize ? (
            <DataTablePagination
              total={summary.totalHostnames}
              page={infrastructure.page}
              pageSize={infrastructure.pageSize}
              itemsLabel="observed hostnames"
            />
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
