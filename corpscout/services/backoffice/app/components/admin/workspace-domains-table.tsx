import { Fragment, useEffect, useId, useState } from "react";
import { Link, useFetcher } from "react-router";
import { ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  DomainEvidence,
  DomainSite,
} from "~/lib/workspace-domains.server";
import type { loader as sitesLoader } from "~/routes/admin-domain-sites";
import { workspaceDomainHref, workspaceWebtechHref } from "~/lib/workspace-domains";

function DomainRow({ row }: { row: DomainEvidence }) {
  const [expanded, setExpanded] = useState(false);
  const [sites, setSites] = useState<DomainSite[]>([]);
  const fetcher = useFetcher<typeof sitesLoader>();
  const id = useId();
  const busy = fetcher.state !== "idle";
  const error =
    fetcher.data && "error" in fetcher.data ? fetcher.data.error : null;
  useEffect(() => {
    if (fetcher.data && !("error" in fetcher.data)) {
      setSites((previous) => [
        ...new Map(
          [...previous, ...fetcher.data!.sites].map((site) => [
            site.hostname,
            site,
          ]),
        ).values(),
      ]);
    }
  }, [fetcher.data]);
  function load(after = "") {
    fetcher.load(
      `/admin/domains/${encodeURIComponent(row.root_domain)}/sites?after=${encodeURIComponent(after)}`,
    );
  }
  return (
    <Fragment>
      <TableRow>
        <TableCell>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label={`${expanded ? "Collapse" : "Expand"} sites for ${row.root_domain}`}
              aria-expanded={expanded}
              aria-controls={id}
              onClick={() => {
                setExpanded(!expanded);
                if (!expanded && !fetcher.data) load();
              }}
            >
              {expanded ? (
                <ChevronDownIcon data-icon="inline-start" />
              ) : (
                <ChevronRightIcon data-icon="inline-start" />
              )}
            </Button>
            <Link
              className="font-medium underline underline-offset-2"
              to={workspaceDomainHref(row.root_domain)}
            >
              {row.root_domain}
            </Link>
          </div>
        </TableCell>
        <TableCell className="tabular-nums">{row.companies}</TableCell>
        <TableCell>
          <Link
            className="underline underline-offset-2"
            to={`/admin/common-crawl/${encodeURIComponent(row.root_domain)}`}
          >
            {row.archived}
          </Link>
        </TableCell>
        <TableCell className="tabular-nums">{row.dns}</TableCell>
        <TableCell>
          <Link
            className="underline underline-offset-2"
            to={workspaceWebtechHref(row.root_domain)}
          >
            {row.webtech ? (
              <Badge variant="secondary">{row.webtech} detected</Badge>
            ) : (
              "No detections"
            )}
          </Link>
        </TableCell>
        <TableCell>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              setExpanded(true);
              if (!fetcher.data) load();
            }}
          >
            View sites
          </Button>
        </TableCell>
      </TableRow>
      {expanded ? (
        <TableRow>
          <TableCell colSpan={6} className="whitespace-normal">
            <div id={id} className="flex flex-col gap-4 p-3" aria-busy={busy}>
              <div>
                <h3 className="font-medium">Sites for {row.root_domain}</h3>
                <p className="text-muted-foreground text-xs">
                  Observed hostnames, including redirect destinations.
                  Technology evidence is specific to each hostname; a DNS
                  hostname alone does not confirm a website.
                </p>
              </div>
              {row.companies > 0 ? (
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-sm">Companies:</span>
                  {[
                    ...new Map(
                      row.company_records.map((company) => [
                        `${company[0]}:${company[1]}`,
                        company,
                      ]),
                    ).values(),
                  ].map(([country, company, type]) =>
                    type === "wikidata_id" || !country ? (
                      <Badge key={`${country}:${company}`} variant="outline">
                        {country} {company}
                      </Badge>
                    ) : (
                      <Link
                        key={`${country}:${company}`}
                        className="text-sm underline"
                        to={
                          country === "SE"
                            ? `/admin/se/company/${encodeURIComponent(company)}`
                            : `/company/${country.toLowerCase()}/${encodeURIComponent(company)}`
                        }
                      >
                        {country} {company}
                      </Link>
                    ),
                  )}
                  {row.companies > row.company_records.length ? (
                    <span className="text-muted-foreground text-xs">
                      Showing up to 10 associations
                    </span>
                  ) : null}
                </div>
              ) : null}
              {error ? (
                <p role="alert">
                  {error}{" "}
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => load(fetcher.data?.next)}
                  >
                    Retry
                  </Button>
                </p>
              ) : null}
              {sites.length ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Site / hostname</TableHead>
                      <TableHead>Archived technologies</TableHead>
                      <TableHead>Webtech detections</TableHead>
                      <TableHead>Technology names (up to 20)</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sites.map((site) => (
                      <TableRow key={site.hostname}>
                        <TableCell className="font-mono">
                          {site.hostname}
                        </TableCell>
                        <TableCell>{site.archived}</TableCell>
                        <TableCell>
                          {site.webtech ? (
                            <Link
                              className="underline"
                              to={workspaceWebtechHref(
                                row.root_domain,
                                site.hostname,
                              )}
                            >
                              {site.webtech} detected
                            </Link>
                          ) : (
                            "No detections"
                          )}
                        </TableCell>
                        <TableCell className="max-w-lg whitespace-normal">
                          <div className="flex flex-wrap gap-1">
                            {site.technologies.map((name) => (
                              <Badge key={name} variant="outline">
                                {name}
                              </Badge>
                            ))}
                          </div>
                          <span className="text-muted-foreground text-xs">
                            {site.technologies.length
                              ? site.webtech
                                ? "Webtech"
                                : "Archived pages"
                              : ""}
                          </span>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : !busy && !error ? (
                <p className="text-muted-foreground text-sm">
                  No site hostnames recorded.
                </p>
              ) : null}
              {busy ? (
                <p role="status" className="text-muted-foreground text-sm">
                  Loading sites…
                </p>
              ) : null}
              {fetcher.data?.hasMore ? (
                <Button
                  className="self-start"
                  variant="outline"
                  disabled={busy}
                  onClick={() => load(fetcher.data?.next)}
                >
                  Load more sites
                </Button>
              ) : null}
            </div>
          </TableCell>
        </TableRow>
      ) : null}
    </Fragment>
  );
}

export function WorkspaceDomainsTable({ rows }: { rows: DomainEvidence[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Domain</TableHead>
            <TableHead>Companies</TableHead>
            <TableHead>Archived technologies</TableHead>
            <TableHead>DNS technologies</TableHead>
            <TableHead>Webtech</TableHead>
            <TableHead>Sites</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.length ? (
            rows.map((row) => <DomainRow key={row.root_domain} row={row} />)
          ) : (
            <TableRow>
              <TableCell colSpan={6}>No domains match these filters.</TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );
}
