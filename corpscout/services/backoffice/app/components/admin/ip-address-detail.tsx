import { ChevronDown, ChevronRight, ChevronsLeft, CircleAlert } from "lucide-react";
import { Fragment, useState, type ReactNode } from "react";
import { Link, NavLink, useFetcher, useLocation } from "react-router";
import { formatObservedAt } from "~/components/detail/technology-infrastructure-section";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "~/components/ui/collapsible";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
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
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type {
  IpAddressDnsRecords,
  IpAddressDomains,
  IpAddressOverview,
  IpAddressRegistration,
  IpComponentStatus,
  IpDnsDomainGroup,
  IpDnsRecord,
  IpHistoryCoverage,
  LookupStatus,
  RdapNetworkRecord,
} from "~/lib/ip-address-detail.server";
import {
  DNS_HISTORY_MAX_HOSTNAMES,
  formatPageTotal,
  IP_DETAIL_TABS,
  type IpDetailTab,
  ipAddressDetailHref,
  ipDnsHistoryHref,
  ipEnrichmentTaskHref,
  rdapSourceLabel,
} from "~/lib/ip-address-detail";
import { workspaceDomainHref } from "~/lib/workspace-domains";

const numberFormat = new Intl.NumberFormat("en-US");

function display(value: ReactNode): ReactNode {
  if (value === null || value === undefined || value === "") {
    return <span className="text-muted-foreground">—</span>;
  }
  return value;
}

function listDisplay(values: string[]): ReactNode {
  return values.length ? values.join(", ") : display(null);
}

function dateDisplay(value: string | null): ReactNode {
  return value ? formatObservedAt(value) : display(null);
}

function Fields({ rows }: { rows: Array<[string, ReactNode]> }) {
  return (
    <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-[minmax(0,12rem)_minmax(0,1fr)]">
      {rows.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-muted-foreground text-sm">{label}</dt>
          <dd className="min-w-0 break-words text-sm">{display(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

const STATUS_VARIANT: Record<
  LookupStatus,
  "default" | "secondary" | "destructive" | "outline"
> = {
  found: "default",
  not_found: "secondary",
  not_global: "secondary",
  not_attempted: "outline",
  retryable_error: "destructive",
  terminal_error: "destructive",
};

export function LookupStatusBadge({
  label,
  status,
}: {
  label?: string;
  status: LookupStatus | null;
}) {
  const text = status ? status.replaceAll("_", " ") : "not enriched yet";
  return (
    <Badge variant={status ? STATUS_VARIANT[status] ?? "outline" : "outline"}>
      {label ? `${label}: ${text}` : text}
    </Badge>
  );
}

export function IpDetailTabs({ ip, tab }: { ip: string; tab: IpDetailTab }) {
  return (
    <div className="max-w-full overflow-x-auto">
      <Tabs value={tab}>
        <TabsList aria-label="IP address sections">
          {IP_DETAIL_TABS.map((item) => (
            <TabsTrigger
              key={item.value}
              value={item.value}
              render={<NavLink to={ipAddressDetailHref(ip, item.value)} />}
              nativeButton={false}
            >
              {item.label}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>
    </div>
  );
}

function CoverageNotice({ coverage }: { coverage: IpHistoryCoverage }) {
  if (coverage.completedPartitions >= coverage.totalPartitions) return null;
  return (
    <Alert>
      <CircleAlert />
      <AlertTitle>Historical relationship index is still loading</AlertTitle>
      <AlertDescription>
        {coverage.completedPartitions}/{coverage.totalPartitions} historical DNS
        partitions have been validated. Results can be incomplete until replay
        finishes; new DNS observations are indexed immediately.
      </AlertDescription>
    </Alert>
  );
}

function KeysetControls({
  after,
  next,
  summary,
}: {
  after: string;
  next: string | null;
  summary: string;
}) {
  const location = useLocation();
  function href(cursor: string) {
    const search = new URLSearchParams(location.search);
    if (cursor) search.set("after", cursor);
    else search.delete("after");
    const query = search.toString();
    return `${location.pathname}${query ? `?${query}` : ""}`;
  }
  if (!after && !next) {
    return <p className="text-muted-foreground text-sm tabular-nums">{summary}</p>;
  }
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-muted-foreground text-sm tabular-nums">{summary}</p>
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          disabled={!after}
          render={after ? <Link to={href("")} preventScrollReset /> : undefined}
          nativeButton={!after}
        >
          <ChevronsLeft data-icon="inline-start" />
          First page
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={!next}
          render={next ? <Link to={href(next)} preventScrollReset /> : undefined}
          nativeButton={!next}
        >
          Next
          <ChevronRight data-icon="inline-end" />
        </Button>
      </div>
    </div>
  );
}

/** Tab-level error: the header and tabs stay, only the tab body is replaced. */
export function IpTabError({ status, message }: { status: number | null; message: string }) {
  const timedOut = status === null || status >= 500;
  return (
    <Alert variant="destructive">
      <CircleAlert />
      <AlertTitle>
        {status === 404 ? "Not found" : timedOut ? "This tab could not be loaded" : `Error ${status}`}
      </AlertTitle>
      <AlertDescription>
        {timedOut
          ? "The query timed out or failed. This IP may have too many connections, or the database is busy. Try again in a moment."
          : message}
      </AlertDescription>
    </Alert>
  );
}

function EmptyState({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Empty className="border border-dashed">
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{children}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

function DomainLink({ domain }: { domain: string }) {
  return (
    <Link
      to={workspaceDomainHref(domain)}
      className="font-mono text-xs font-medium underline-offset-4 hover:underline"
    >
      {domain}
    </Link>
  );
}

function EvidenceBadges({
  sources,
  discoveries,
}: {
  sources: string[];
  discoveries: string[];
}) {
  if (!sources.length && !discoveries.length) {
    return <span className="text-muted-foreground text-xs">Historical DNS record</span>;
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {discoveries.map((value) => (
        <Badge key={`d:${value}`} variant="outline">
          {value}
        </Badge>
      ))}
      {sources.map((value) => (
        <Badge key={`s:${value}`} variant="secondary">
          {value}
        </Badge>
      ))}
    </div>
  );
}

// ---------- Overview ----------

function ComponentStatusCard({
  title,
  status,
  children,
}: {
  title: string;
  status: IpComponentStatus;
  children: ReactNode;
}) {
  const staleData = status.dataStatus !== status.status;
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <CardTitle>{title}</CardTitle>
          <LookupStatusBadge status={status.status} />
        </div>
        <CardDescription>
          Checked {dateDisplay(status.checkedAt)}
          {staleData
            ? ` · showing data from the last conclusive lookup (${status.dataStatus.replaceAll("_", " ")}, ${status.dataAt ? formatObservedAt(status.dataAt) : "unknown time"})`
            : null}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {status.errorCode || status.retryAfter ? (
          <Fields
            rows={[
              ["Error code", status.errorCode],
              ["Retry after", status.retryAfter ? formatObservedAt(status.retryAfter) : null],
            ]}
          />
        ) : null}
        {children}
      </CardContent>
    </Card>
  );
}

export function IpOverviewView({ data }: { data: IpAddressOverview }) {
  const { enrichment, rdapMarker } = data;
  return (
    <div className="flex flex-col gap-5">
      {enrichment ? (
        <>
          <Card>
            <CardHeader>
              <CardTitle>Last enrichment result</CardTitle>
            </CardHeader>
            <CardContent>
              <Fields
                rows={[
                  [
                    "Task",
                    <Link
                      key="task"
                      to={ipEnrichmentTaskHref(enrichment.taskId)}
                      className="font-mono text-xs underline-offset-4 hover:underline"
                    >
                      {enrichment.taskId}
                    </Link>,
                  ],
                  ["Completed at", dateDisplay(enrichment.completedAt)],
                  ["Result ID", <span key="r" className="font-mono text-xs">{enrichment.resultId}</span>],
                  ["Processor version", enrichment.processorVersion],
                  ["IP scope", enrichment.ipScope],
                ]}
              />
            </CardContent>
          </Card>
          <div className="grid gap-5 xl:grid-cols-3">
            <ComponentStatusCard title="City (GeoIP)" status={enrichment.city}>
              <Fields
                rows={[
                  [
                    "Continent",
                    [enrichment.geo.continentName, enrichment.geo.continentCode && `(${enrichment.geo.continentCode})`]
                      .filter(Boolean)
                      .join(" "),
                  ],
                  [
                    "Country",
                    [enrichment.geo.countryName, enrichment.geo.countryIsoCode && `(${enrichment.geo.countryIsoCode})`]
                      .filter(Boolean)
                      .join(" "),
                  ],
                  [
                    "Registered country",
                    [
                      enrichment.geo.registeredCountryName,
                      enrichment.geo.registeredCountryIsoCode && `(${enrichment.geo.registeredCountryIsoCode})`,
                    ]
                      .filter(Boolean)
                      .join(" "),
                  ],
                  [
                    "Subdivisions",
                    enrichment.geo.subdivisionNames
                      .map((name, index) => {
                        const code = enrichment.geo.subdivisionIsoCodes[index];
                        return code ? `${name} (${code})` : name;
                      })
                      .join(", "),
                  ],
                  ["City", enrichment.geo.cityName],
                  [
                    "Coordinates",
                    enrichment.geo.latitude !== null && enrichment.geo.longitude !== null
                      ? `${enrichment.geo.latitude}, ${enrichment.geo.longitude}`
                      : null,
                  ],
                  [
                    "Accuracy",
                    enrichment.geo.accuracyRadiusKm !== null
                      ? `${enrichment.geo.accuracyRadiusKm} km`
                      : null,
                  ],
                  ["Time zone", enrichment.geo.timezone],
                  ["Network", enrichment.geo.cityNetwork],
                  ["Database build", enrichment.geo.cityDbBuildDate],
                ]}
              />
            </ComponentStatusCard>
            <ComponentStatusCard title="ASN" status={enrichment.asnStatus}>
              <Fields
                rows={[
                  ["ASN", enrichment.asn.asn !== null ? `AS${enrichment.asn.asn}` : null],
                  ["Organization", enrichment.asn.organization],
                  ["Network", enrichment.asn.network],
                  ["Database build", enrichment.asn.dbBuildDate],
                ]}
              />
            </ComponentStatusCard>
            <ComponentStatusCard title="RDAP" status={enrichment.rdapStatus}>
              <Fields
                rows={[
                  ["RIR", enrichment.rdap.rir],
                  ["Network name", enrichment.rdap.name],
                  ["Handle", enrichment.rdap.handle],
                  ["Registrant", listDisplay(enrichment.rdap.registrantNames)],
                  ["Matched CIDR", enrichment.rdap.matchedCidr],
                  ["Registration type", enrichment.rdap.registrationType],
                  ["Statuses", listDisplay(enrichment.rdap.statuses)],
                  ["Country", enrichment.rdap.countryCode],
                  ["Registered", dateDisplay(enrichment.rdap.registrationDate)],
                  ["Last changed", dateDisplay(enrichment.rdap.lastChangedAt)],
                  [
                    "Source URL",
                    enrichment.rdap.selfUrl ? (
                      <a
                        href={enrichment.rdap.selfUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="break-all underline-offset-4 hover:underline"
                      >
                        {enrichment.rdap.selfUrl}
                      </a>
                    ) : null,
                  ],
                ]}
              />
            </ComponentStatusCard>
          </div>
        </>
      ) : (
        <EmptyState title="Not enriched yet">
          No IP enrichment result exists for this address. Add it to the IP
          enrichment queue from the IP address list.
        </EmptyState>
      )}
      <Card>
        <CardHeader>
          <CardTitle>RDAP lookup marker</CardTitle>
          <CardDescription>
            The per-address RDAP lookup record, kept for addresses whose network
            answers only for the queried IP or failed.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {rdapMarker ? (
            <Fields
              rows={[
                ["Status", rdapMarker.status],
                ["Network key", rdapMarker.networkKey],
                ["Error code", rdapMarker.errorCode],
                ["Retry after", rdapMarker.retryAfter ? formatObservedAt(rdapMarker.retryAfter) : null],
                ["Queried at", dateDisplay(rdapMarker.queriedAt)],
              ]}
            />
          ) : (
            <p className="text-muted-foreground text-sm">No per-address RDAP marker.</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

// ---------- Registration ----------

function networkRows(network: RdapNetworkRecord): Array<[string, ReactNode]> {
  return [
    ["Handle", network.handle],
    ["Range", `${network.startAddress} – ${network.endAddress}`],
    ["Name", network.name],
    ["RIR", network.rir],
    ["Registration type", network.registrationType],
    ["Statuses", listDisplay(network.statuses)],
    ["Country", network.countryCode],
    ["Registrant names", listDisplay(network.registrantNames)],
    ["Registrant handles", listDisplay(network.registrantHandles)],
    ["Registered", dateDisplay(network.registrationDate)],
    ["Last changed", dateDisplay(network.lastChangedAt)],
    ["Fetched", dateDisplay(network.fetchedAt)],
    [
      "Self URL",
      network.selfUrl ? (
        <a href={network.selfUrl} target="_blank" rel="noreferrer" className="break-all underline-offset-4 hover:underline">
          {network.selfUrl}
        </a>
      ) : null,
    ],
    [
      "Up URL",
      network.upUrl ? (
        <a href={network.upUrl} target="_blank" rel="noreferrer" className="break-all underline-offset-4 hover:underline">
          {network.upUrl}
        </a>
      ) : null,
    ],
    ["Network key", <span key="k" className="font-mono text-xs">{network.networkKey}</span>],
  ];
}

export function IpRegistrationView({ data }: { data: IpAddressRegistration }) {
  const { network, registryClass, parent, segments } = data;
  if (!network) {
    return (
      <EmptyState title="No registration">
        {data.keySource
          ? "The matched network key has no cached RDAP registration."
          : "No RDAP registration covers this address yet."}
      </EmptyState>
    );
  }
  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <CardTitle>{network.name ?? network.handle}</CardTitle>
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary">{rdapSourceLabel(network.source)}</Badge>
              {data.keySource === "trie" ? (
                <Badge variant="outline">Matched via RDAP trie</Badge>
              ) : null}
            </div>
          </div>
          <CardDescription>
            {data.matchedCidr ? `Matched ${data.matchedCidr}` : "Registration for this address"}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Fields rows={networkRows(network)} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Registry class</CardTitle>
          <CardDescription>
            How the registration compares with the IANA and RIR reference data.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {registryClass ? (
            <Fields
              rows={[
                ["Registry class", <Badge key="c" variant="outline">{registryClass.registryClass}</Badge>],
                ["Covered RIR blocks", numberFormat.format(registryClass.coveredRirBlocks)],
                ["IANA designation", registryClass.ianaDesignation],
                ["IANA RIR", registryClass.ianaRir],
                ["IANA status", registryClass.ianaStatus],
                ["Special registry", registryClass.specialRegistry],
                ["Special status", registryClass.specialStatus],
                ["Classified", dateDisplay(registryClass.classifiedAt)],
              ]}
            />
          ) : (
            <p className="text-muted-foreground text-sm">Not classified yet.</p>
          )}
        </CardContent>
      </Card>

      {network.parentNetworkKey ? (
        <Card>
          <CardHeader>
            <CardTitle>Parent network</CardTitle>
          </CardHeader>
          <CardContent>
            {parent ? (
              <Fields
                rows={[
                  ["Handle", parent.handle],
                  ["Name", parent.name],
                  ["Range", `${parent.startAddress} – ${parent.endAddress}`],
                  ["Registration type", parent.registrationType],
                  ["Country", parent.countryCode],
                  ["Registrant names", listDisplay(parent.registrantNames)],
                ]}
              />
            ) : (
              <Fields
                rows={[
                  ["Handle", network.parentHandle],
                  ["Network key", network.parentNetworkKey],
                  ["Cached", "No cached registration"],
                ]}
              />
            )}
          </CardContent>
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Network segments</CardTitle>
          <CardDescription>
            CIDR fragments derived from the registration range.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {segments.length ? (
            <div className="overflow-hidden rounded-xl border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>CIDR</TableHead>
                    <TableHead>Prefix</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Derived (UTC)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {segments.map((segment) => (
                    <TableRow key={`${segment.segmentRole}:${segment.cidr}`}>
                      <TableCell className="font-mono text-xs">{segment.cidr}</TableCell>
                      <TableCell className="tabular-nums">/{segment.prefixLength}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{segment.segmentRole}</Badge>
                      </TableCell>
                      <TableCell className="text-muted-foreground text-xs">
                        {formatObservedAt(segment.derivedAt)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          ) : (
            <p className="text-muted-foreground text-sm">No derived segments.</p>
          )}
        </CardContent>
      </Card>

      <Collapsible>
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <CardTitle>Raw response</CardTitle>
              <CollapsibleTrigger render={<Button variant="outline" size="sm" />}>
                Show / hide
              </CollapsibleTrigger>
            </div>
          </CardHeader>
          <CollapsibleContent>
            <CardContent>
              <pre className="bg-muted max-h-[40rem] overflow-auto rounded-lg p-4 font-mono text-xs">
                {network.rawResponse}
              </pre>
            </CardContent>
          </CollapsibleContent>
        </Card>
      </Collapsible>
    </div>
  );
}

// ---------- DNS records ----------

function DnsRecordHistory({
  ip,
  group,
  columns,
}: {
  ip: string;
  group: IpDnsDomainGroup;
  columns: number;
}) {
  const fetcher = useFetcher<{ records: IpDnsRecord[] | null }>();
  const [open, setOpen] = useState(false);
  const hostnames =
    group.hostnames.length <= DNS_HISTORY_MAX_HOSTNAMES ? group.hostnames : [];
  function toggle() {
    const next = !open;
    setOpen(next);
    if (next && fetcher.state === "idle" && !fetcher.data?.records) {
      void fetcher.load(ipDnsHistoryHref(ip, group.rootDomain, hostnames));
    }
  }
  return (
    <>
      <TableRow>
        <TableCell colSpan={columns} className="bg-muted/30 py-1.5">
          <Button variant="ghost" size="sm" onClick={toggle} aria-expanded={open}>
            {open ? <ChevronDown data-icon="inline-start" /> : <ChevronRight data-icon="inline-start" />}
            Record history for {group.rootDomain}
          </Button>
        </TableCell>
      </TableRow>
      {open ? (
        <TableRow>
          <TableCell colSpan={columns} className="whitespace-normal">
            {fetcher.state !== "idle" ? (
              <p className="text-muted-foreground text-sm">Loading record history…</p>
            ) : fetcher.data?.records ? (
              fetcher.data.records.length ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Hostname</TableHead>
                      <TableHead>Value</TableHead>
                      <TableHead>First seen (UTC)</TableHead>
                      <TableHead>Last seen (UTC)</TableHead>
                      <TableHead className="text-right">Seen dates</TableHead>
                      <TableHead>Sources / discoveries</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {fetcher.data.records.map((record) => (
                      <TableRow key={`${record.hostname}:${record.value}`}>
                        <TableCell className="break-all font-mono text-xs">{record.hostname}</TableCell>
                        <TableCell className="font-mono text-xs">{record.value}</TableCell>
                        <TableCell className="text-muted-foreground text-xs tabular-nums">
                          {formatObservedAt(record.firstSeen)}
                        </TableCell>
                        <TableCell className="text-muted-foreground text-xs tabular-nums">
                          {formatObservedAt(record.lastSeen)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {numberFormat.format(record.seenDates)}
                        </TableCell>
                        <TableCell>
                          <EvidenceBadges sources={record.sources} discoveries={record.discoveries} />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <p className="text-muted-foreground text-sm">No record-level rows are indexed.</p>
              )
            ) : (
              <p className="text-destructive text-sm">
                Record history could not be loaded. Collapse and expand to retry.
              </p>
            )}
          </TableCell>
        </TableRow>
      ) : null}
    </>
  );
}

export function IpDnsRecordsView({ data }: { data: IpAddressDnsRecords }) {
  const summary = `${formatPageTotal(data.total, data.domains.length)} root domain${data.total === 1 ? "" : "s"}${data.after ? " (later page)" : ""}`;
  const columns = 7;
  return (
    <div className="flex flex-col gap-4">
      <CoverageNotice coverage={data.coverage} />
      <p className="text-muted-foreground text-sm">
        Hostnames whose {data.address.version === 4 ? "A" : "AAAA"} record points to{" "}
        <span className="text-foreground font-mono">{data.address.ip}</span>. Dates, seen
        count and sources are per root domain; expand a root domain for its record-level
        history.
      </p>
      {data.domains.length ? (
        <div className="overflow-hidden rounded-xl border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Hostname</TableHead>
                <TableHead>Root domain</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>First seen (UTC)</TableHead>
                <TableHead>Last seen (UTC)</TableHead>
                <TableHead className="text-right">Seen dates</TableHead>
                <TableHead>Sources / discoveries</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.domains.map((group) => (
                <Fragment key={group.rootDomain}>
                  {group.hostnames.map((hostname) => (
                    <TableRow key={`${group.rootDomain}:${hostname}`}>
                      <TableCell className="max-w-sm whitespace-normal break-all font-mono text-xs">
                        {hostname}
                      </TableCell>
                      <TableCell>
                        <DomainLink domain={group.rootDomain} />
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{group.type}</Badge>
                      </TableCell>
                      <TableCell className="text-muted-foreground text-xs tabular-nums">
                        {formatObservedAt(group.firstSeen)}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-xs tabular-nums">
                        {formatObservedAt(group.lastSeen)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {numberFormat.format(group.seenDates)}
                      </TableCell>
                      <TableCell className="max-w-xs whitespace-normal">
                        <EvidenceBadges sources={group.sources} discoveries={group.discoveries} />
                      </TableCell>
                    </TableRow>
                  ))}
                  <DnsRecordHistory ip={data.address.ip} group={group} columns={columns} />
                </Fragment>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : (
        <EmptyState title="No hostnames">
          No A or AAAA record pointing to this address is indexed
          {data.after ? " after this point" : ""}.
        </EmptyState>
      )}
      <KeysetControls after={data.after} next={data.next} summary={summary} />
    </div>
  );
}

// ---------- Domains ----------

export function IpDomainsView({ data }: { data: IpAddressDomains }) {
  const segmentLabel = data.address.version === 4 ? "/24" : "/48";
  const summary = `${formatPageTotal(data.total, data.connections.length)} ${data.scope === "segment" ? "neighbour connection" : "domain"}${data.total === 1 ? "" : "s"}${data.after ? " (later page)" : ""}`;
  const base = ipAddressDetailHref(data.address.ip, "domains");
  return (
    <div className="flex flex-col gap-4">
      <CoverageNotice coverage={data.coverage} />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Tabs value={data.scope}>
          <TabsList aria-label="Domain scope">
            <TabsTrigger value="exact" render={<Link to={base} />} nativeButton={false}>
              This address
            </TabsTrigger>
            <TabsTrigger
              value="segment"
              render={<Link to={`${base}?scope=segment`} />}
              nativeButton={false}
            >
              Same {segmentLabel} segment
            </TabsTrigger>
          </TabsList>
        </Tabs>
        {data.scope === "segment" ? (
          <p className="text-muted-foreground text-sm">
            Other addresses in{" "}
            <span className="text-foreground font-mono">{data.address.networkSegment}</span>
            {" "}— a weaker, neighbourhood-level signal.
          </p>
        ) : null}
      </div>
      {data.connections.length ? (
        <div className="overflow-hidden rounded-xl border">
          <Table>
            <TableHeader>
              <TableRow>
                {data.scope === "segment" ? <TableHead>Address</TableHead> : null}
                <TableHead>Root domain</TableHead>
                <TableHead>Hostnames</TableHead>
                <TableHead>First seen (UTC)</TableHead>
                <TableHead>Last seen (UTC)</TableHead>
                <TableHead>Sources</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.connections.map((connection) => (
                <TableRow key={`${connection.ip}:${connection.domain}`}>
                  {data.scope === "segment" ? (
                    <TableCell>
                      <Link
                        to={ipAddressDetailHref(connection.ip, "domains")}
                        className="font-mono text-xs underline-offset-4 hover:underline"
                      >
                        {connection.ip}
                      </Link>
                    </TableCell>
                  ) : null}
                  <TableCell>
                    <DomainLink domain={connection.domain} />
                  </TableCell>
                  <TableCell className="max-w-sm whitespace-normal">
                    <p className="tabular-nums text-sm">
                      {numberFormat.format(connection.hostnames.length)}
                    </p>
                    <p className="text-muted-foreground break-all font-mono text-xs">
                      {connection.hostnames.slice(0, 3).join(", ")}
                      {connection.hostnames.length > 3
                        ? ` +${connection.hostnames.length - 3} more`
                        : ""}
                    </p>
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs tabular-nums">
                    {formatObservedAt(connection.firstSeen)}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs tabular-nums">
                    {formatObservedAt(connection.lastSeen)}
                  </TableCell>
                  <TableCell className="max-w-xs whitespace-normal">
                    <EvidenceBadges
                      sources={connection.sources}
                      discoveries={connection.discoveries}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : (
        <EmptyState title="No domains">
          {data.scope === "exact"
            ? "No domain has an indexed A or AAAA record pointing to this address."
            : `No other address in this ${segmentLabel} segment has indexed domains.`}
        </EmptyState>
      )}
      <KeysetControls after={data.after} next={data.next} summary={summary} />
    </div>
  );
}
