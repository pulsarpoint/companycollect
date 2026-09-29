import { Link } from "react-router";
import type { Route } from "./+types/admin-domain-services";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import {
  groupCurrent,
  serviceTypeLabel,
  type DomainServices,
  type ServiceEvidence,
  type ServiceInterval,
} from "~/lib/domain-services";
import { getDomainServices } from "~/lib/domain-services.server";

export function loader({ params }: Route.LoaderArgs) {
  return getDomainServices(params.domain.trim().toLowerCase());
}

function providerDomainsHref(slug: string) {
  return `/admin/provider-feeds/providers/${encodeURIComponent(slug)}/domains`;
}

function Provider({ interval }: { interval: ServiceInterval }) {
  if (interval.providerSlug) {
    return (
      <Link className="font-medium underline-offset-4 hover:underline" to={providerDomainsHref(interval.providerSlug)}>
        {interval.providerSlug}
      </Link>
    );
  }
  return (
    <span className="inline-flex items-center gap-2">
      <span className="font-mono">{interval.providerKey}</span>
      <Badge variant="outline">unmapped</Badge>
    </span>
  );
}

function Evidence({ rows }: { rows: ServiceEvidence[] }) {
  if (rows.length === 0) return null;
  return (
    <details className="mt-1 text-xs">
      <summary className="text-muted-foreground cursor-pointer">{rows.length} records</summary>
      <ul className="mt-1 flex flex-col gap-1">
        {rows.map((r, i) => (
          <li key={i} className="font-mono break-all">
            {r.recordName} {r.recordType} → {r.subject} · {r.ruleId} · {r.validFrom} – {r.validTo}
          </li>
        ))}
      </ul>
    </details>
  );
}

export default function DomainServicesPage({ loaderData }: { loaderData: DomainServices }) {
  const { resolved, intervals, evidence } = loaderData;
  if (!resolved) {
    return <p className="text-muted-foreground text-sm">This domain has not been resolved yet.</p>;
  }
  if (intervals.length === 0) {
    return <p className="text-muted-foreground text-sm">No providers found in this domain's DNS records.</p>;
  }
  const groups = groupCurrent(intervals);
  const evidenceFor = (i: ServiceInterval) =>
    evidence.filter(
      (e) =>
        e.serviceType === i.serviceType &&
        e.providerKey === i.providerKey &&
        e.validTo >= i.firstSeen &&
        e.validFrom <= i.lastSeen,
    );

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Now</CardTitle>
          <CardDescription>Providers in use at the domain's latest DNS scans.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {groups.length === 0 ? (
            <p className="text-muted-foreground text-sm">No provider is in use at the latest scans.</p>
          ) : (
            groups.map((g) => (
              <section key={g.serviceType} className="flex flex-col gap-1">
                <h3 className="text-sm font-semibold">{g.label}</h3>
                <ul className="flex flex-col gap-1 text-sm">
                  {g.intervals.map((i) => (
                    <li key={`${i.providerKey}-${i.firstSeen}`} className="flex flex-wrap items-center gap-2">
                      <Provider interval={i} />
                      {i.serviceKeys.length > 0 && (
                        <span className="text-muted-foreground font-mono text-xs">{i.serviceKeys.join(", ")}</span>
                      )}
                      <span className="text-muted-foreground text-xs">since {i.firstSeen}</span>
                      <span className="text-muted-foreground text-xs">confidence {i.confidence.toFixed(2)}</span>
                    </li>
                  ))}
                </ul>
              </section>
            ))
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>History</CardTitle>
          <CardDescription>Every period a provider was seen, with the DNS records behind it.</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Service</TableHead>
                  <TableHead>Provider</TableHead>
                  <TableHead>First seen</TableHead>
                  <TableHead>Last seen</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Evidence</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {intervals.map((i) => (
                  <TableRow key={`${i.serviceType}-${i.providerKey}-${i.firstSeen}`}>
                    <TableCell>{serviceTypeLabel(i.serviceType)}</TableCell>
                    <TableCell>
                      <Provider interval={i} />
                    </TableCell>
                    <TableCell>{i.firstSeen}</TableCell>
                    <TableCell>{i.lastSeen}</TableCell>
                    <TableCell>
                      <Badge variant={i.isCurrent ? "secondary" : "outline"}>{i.isCurrent ? "current" : "ended"}</Badge>
                    </TableCell>
                    <TableCell>
                      <span className="text-xs">
                        {i.evidence} · {i.recordTypes.join(", ")}
                      </span>
                      <Evidence rows={evidenceFor(i)} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
