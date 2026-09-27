import { Fragment, useEffect, useId, useState } from "react";
import { Link, useFetcher } from "react-router";
import { ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";
import { Checkbox } from "~/components/ui/checkbox";
import { isDomainSelected, selectDomains } from "~/lib/domain-selection";
import type { WorkspaceDomainSelection } from "~/lib/workspace-domains";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import type { DomainEvidence, DomainSite } from "~/lib/workspace-domains.server";
import type { loader as sitesLoader } from "~/routes/admin-domain-sites";
import { DOMAIN_SOURCE_LABELS, workspaceDomainHref } from "~/lib/workspace-domains";

type Selection = { value: WorkspaceDomainSelection; onChange: (selection: WorkspaceDomainSelection) => void };

function DomainRow({ row, selection }: { row: DomainEvidence; selection?: Selection }) {
  const [expanded, setExpanded] = useState(false);
  const [sites, setSites] = useState<DomainSite[]>([]);
  const fetcher = useFetcher<typeof sitesLoader>();
  const id = useId();
  const busy = fetcher.state !== "idle";
  const error = fetcher.data && "error" in fetcher.data ? fetcher.data.error : null;
  useEffect(() => {
    if (fetcher.data && !("error" in fetcher.data)) {
      const incoming = fetcher.data.sites;
      setSites((previous) => [...new Map([...previous, ...incoming].map((site) => [site.website_origin, site])).values()]);
    }
  }, [fetcher.data]);
  function load(after = "") {
    fetcher.load(`/admin/domains/${encodeURIComponent(row.root_domain)}/sites?after=${encodeURIComponent(after)}`);
  }
  return <Fragment>
    <TableRow>
      {selection && <TableCell><Checkbox aria-label={`Select ${row.root_domain}`} checked={isDomainSelected(selection.value, row.root_domain)}
        onCheckedChange={checked => selection.onChange(selectDomains(selection.value, [row.root_domain], checked))} /></TableCell>}
      <TableCell><div className="flex items-center gap-2">
        <Button variant="ghost" size="icon-sm" aria-label={`${expanded ? "Collapse" : "Expand"} websites for ${row.root_domain}`}
          aria-expanded={expanded} aria-controls={id} onClick={() => { setExpanded(!expanded); if (!expanded && !fetcher.data) load(); }}>
          {expanded ? <ChevronDownIcon /> : <ChevronRightIcon />}
        </Button>
        <Link className="font-medium underline underline-offset-2" to={workspaceDomainHref(row.root_domain)}>{row.root_domain}</Link>
      </div></TableCell>
      <TableCell><div className="flex flex-wrap gap-1">{row.sources.map((source) => <Badge key={source} variant="outline">{DOMAIN_SOURCE_LABELS[source] ?? source}</Badge>)}</div></TableCell>
      <TableCell>{row.has_dns_records ? <Link className="underline" to={`${workspaceDomainHref(row.root_domain)}/dns`}>Recorded</Link> : "None recorded"}</TableCell>
      <TableCell className="text-muted-foreground text-xs tabular-nums">{row.dns_last_observed_at ? `${row.dns_last_observed_at.slice(0, 16)} UTC` : "—"}</TableCell>
      <TableCell><Button size="sm" variant="ghost" onClick={() => { setExpanded(true); if (!fetcher.data) load(); }}>{row.website_count} websites</Button>
        {row.website_count > 0 ? <p className="text-muted-foreground text-xs">{row.observed_website_count} observed · {row.website_count - row.observed_website_count} assumed</p> : null}
      </TableCell>
      <TableCell className="tabular-nums">{row.company_count}</TableCell>
      <TableCell>{row.company_matching_status ? <Badge variant="outline">{{ matched: "Matched", not_found: "Not found", already_mapped: "Already associated", failed: "Failed", cancelled: "Cancelled" }[row.company_matching_status] ?? row.company_matching_status}</Badge> : <span className="text-muted-foreground">Not attempted</span>}</TableCell>
      <TableCell><Link className="underline underline-offset-2" aria-label={`View crawls for ${row.root_domain}`} to={`${workspaceDomainHref(row.root_domain)}/crawl`}>Crawl</Link></TableCell>
    </TableRow>
    {expanded ? <TableRow><TableCell colSpan={selection ? 9 : 8} className="whitespace-normal">
      <div id={id} className="flex flex-col gap-3 p-3" aria-busy={busy}>
        <h3 className="font-medium">Websites for {row.root_domain}</h3>
        <p className="text-muted-foreground text-xs">Website inventory entries. Observed means evidence exists; it does not confirm current reachability.</p>
        {error ? <p role="alert">{error} <Button size="sm" variant="outline" onClick={() => load(fetcher.data?.next)}>Retry</Button></p> : null}
        {sites.length ? <Table>
          <TableHeader><TableRow><TableHead>Website</TableHead><TableHead>Evidence</TableHead><TableHead>Sources</TableHead><TableHead>Last observed (UTC)</TableHead></TableRow></TableHeader>
          <TableBody>{sites.map((site) => <TableRow key={site.website_origin}>
            <TableCell className="font-mono text-xs">{site.website_origin}</TableCell>
            <TableCell><Badge variant="outline">{site.evidence_status}</Badge></TableCell>
            <TableCell>{site.sources.join(", ")}</TableCell>
            <TableCell>{site.last_observed_at ?? "Not observed"}</TableCell>
          </TableRow>)}</TableBody>
        </Table> : !busy && !error ? <p className="text-muted-foreground text-sm">No websites in the inventory.</p> : null}
        {busy ? <p role="status" className="text-muted-foreground text-sm">Loading websites…</p> : null}
        {fetcher.data?.hasMore ? <Button className="self-start" variant="outline" disabled={busy} onClick={() => load(fetcher.data?.next)}>Load more websites</Button> : null}
      </div>
    </TableCell></TableRow> : null}
  </Fragment>;
}

export function WorkspaceDomainsTable({ rows, selection }: { rows: DomainEvidence[]; selection?: Selection }) {
  const domains = rows.map(row => row.root_domain);
  const selected = selection ? domains.filter(domain => isDomainSelected(selection.value, domain)).length : 0;
  return <div className="overflow-x-auto rounded-lg border"><Table>
    <TableHeader><TableRow>{selection && <TableHead><Checkbox aria-label="Select every domain on this page" checked={domains.length > 0 && selected === domains.length}
      indeterminate={selected > 0 && selected < domains.length} disabled={!domains.length}
      onCheckedChange={checked => selection.onChange(selectDomains(selection.value, domains, checked))} /></TableHead>}<TableHead>Domain</TableHead><TableHead>Sources</TableHead><TableHead>DNS records</TableHead><TableHead>DNS last observed</TableHead><TableHead>Websites</TableHead><TableHead>Companies</TableHead><TableHead>Company matching</TableHead><TableHead>Crawl</TableHead></TableRow></TableHeader>
    <TableBody>{rows.length ? rows.map((row) => <DomainRow key={row.root_domain} row={row} selection={selection} />) : <TableRow><TableCell colSpan={selection ? 9 : 8}>No domains match these filters.</TableCell></TableRow>}</TableBody>
  </Table></div>;
}
