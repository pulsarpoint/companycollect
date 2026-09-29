import { data, Link } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-provider-domains";
import { ProviderTabs } from "~/components/admin/provider-tabs";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import {
  parseProviderDomainsFilter,
  serviceTypeLabel,
  type ProviderDomainsFilter,
  type ProviderDomainsPage,
} from "~/lib/domain-services";
import { getProviderDomains } from "~/lib/domain-services.server";
import { loadProviderDocument } from "~/lib/provider-recon.server";

type LoaderData = ProviderDomainsPage & {
  provider: { slug: string; name: string };
  filter: ProviderDomainsFilter;
  error?: string;
};

export async function loader({ params, request }: Route.LoaderArgs): Promise<LoaderData> {
  const doc = await loadProviderDocument(params.slug);
  if (!doc) throw data(`Provider ${params.slug} not found.`, { status: 404 });
  const provider = { slug: doc.slug, name: doc.display_name };
  const filter = parseProviderDomainsFilter(new URL(request.url).searchParams);
  try {
    return { provider, filter, ...(await getProviderDomains(doc.slug, filter)) };
  } catch (error) {
    return {
      provider,
      filter,
      error: error instanceof Error ? error.message : String(error),
      rows: [],
      total: 0,
      page: 1,
      pageSize: 50,
      byType: {},
      byService: [],
    };
  }
}

const fmt = (n: number) => n.toLocaleString("en-US");

function pageHref(filter: ProviderDomainsFilter, page: number) {
  const q = new URLSearchParams();
  if (filter.serviceType) q.set("type", filter.serviceType);
  if (filter.service) q.set("service", filter.service);
  if (!filter.now) q.set("now", "0");
  q.set("page", String(page));
  return `?${q}`;
}

const SELECT = "border-input bg-background h-9 rounded-md border px-2 text-sm";

export default function ProviderDomainsRoute({ loaderData }: { loaderData: LoaderData }) {
  const { provider, filter, rows, total, page, pageSize, byType, byService, error } = loaderData;
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const types = Object.entries(byType).sort(([, a], [, b]) => b.ever - a.ever);

  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <Link className="text-muted-foreground text-sm underline-offset-4 hover:underline" to="/admin/providers">
          Providers
        </Link>
        <h1 className="text-2xl font-semibold">{provider.name}</h1>
      </header>
      <ProviderTabs slug={provider.slug} active="domains" />
      {error && (
        <Alert variant="destructive">
          <AlertTitle>Could not load domains</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Usage</CardTitle>
          <CardDescription>Distinct domains using this provider now and ever, per service type and service.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Service type</TableHead>
                <TableHead className="text-right">Now</TableHead>
                <TableHead className="text-right">Ever</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {types.map(([type, c]) => (
                <TableRow key={type}>
                  <TableCell>{serviceTypeLabel(type)}</TableCell>
                  <TableCell className="text-right">{fmt(c.now)}</TableCell>
                  <TableCell className="text-right">{fmt(c.ever)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Service</TableHead>
                <TableHead className="text-right">Now</TableHead>
                <TableHead className="text-right">Ever</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {byService.map((s) => (
                <TableRow key={s.service}>
                  <TableCell className="font-mono text-xs">{s.service}</TableCell>
                  <TableCell className="text-right">{fmt(s.now)}</TableCell>
                  <TableCell className="text-right">{fmt(s.ever)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Domains</CardTitle>
          <CardDescription>{fmt(total)} domains</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <form method="get" className="flex flex-wrap items-center gap-2">
            <select name="type" defaultValue={filter.serviceType} className={SELECT} aria-label="Service type">
              <option value="">All service types</option>
              {types.map(([type]) => (
                <option key={type} value={type}>
                  {serviceTypeLabel(type)}
                </option>
              ))}
            </select>
            <select name="service" defaultValue={filter.service} className={SELECT} aria-label="Service">
              <option value="">All services</option>
              {byService.map((s) => (
                <option key={s.service} value={s.service}>
                  {s.service}
                </option>
              ))}
            </select>
            <select name="now" defaultValue={filter.now ? "1" : "0"} className={SELECT} aria-label="Period">
              <option value="1">Using it now</option>
              <option value="0">Ever used it</option>
            </select>
            <Button type="submit" variant="outline" size="sm">
              Filter
            </Button>
          </form>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Domain</TableHead>
                  <TableHead>Service types</TableHead>
                  <TableHead>Services</TableHead>
                  <TableHead>First seen</TableHead>
                  <TableHead>Last seen</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((r) => (
                  <TableRow key={r.domain}>
                    <TableCell>
                      <Link
                        className="font-mono underline-offset-4 hover:underline"
                        to={`/admin/domains/${encodeURIComponent(r.domain)}/services`}
                      >
                        {r.domain}
                      </Link>
                    </TableCell>
                    <TableCell>{r.serviceTypes.map(serviceTypeLabel).join(", ")}</TableCell>
                    <TableCell className="font-mono text-xs">{r.serviceKeys.join(", ")}</TableCell>
                    <TableCell>{r.firstSeen}</TableCell>
                    <TableCell>{r.lastSeen}</TableCell>
                    <TableCell>
                      <Badge variant={r.isCurrent ? "secondary" : "outline"}>{r.isCurrent ? "current" : "ended"}</Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          <nav className="flex items-center gap-3 text-sm" aria-label="Pages">
            {page > 1 && (
              <Link className="underline-offset-4 hover:underline" to={pageHref(filter, page - 1)}>
                Previous
              </Link>
            )}
            <span className="text-muted-foreground">
              Page {page} of {lastPage}
            </span>
            {page < lastPage && (
              <Link className="underline-offset-4 hover:underline" to={pageHref(filter, page + 1)}>
                Next
              </Link>
            )}
          </nav>
        </CardContent>
      </Card>
    </div>
  );
}
