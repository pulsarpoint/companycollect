import { isRouteErrorResponse, Link, useRevalidator, useRouteError } from "react-router";
import type { Route } from "./+types/admin-se-domain-crawl";
import { CrawlTime, DomainCrawls } from "~/components/admin/domain-crawls";
import { CrawlResultDetails } from "~/components/admin/crawl-result-details";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { loadDomainCrawlDetails } from "~/lib/domain-crawls.server";
import { domainCrawlStatus } from "~/lib/domain-crawl-status";
import { graphDomainError } from "~/lib/domain-graph";

export async function loader({params, request}: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  if (!domain || graphDomainError(domain)) throw new Response("Not found", {status: 404});
  const search = new URL(request.url).searchParams;
  const type = search.get("type");
  const requestId = search.get("request");
  const attempt = search.get("attempt");
  if (type !== null && !["site_info", "jobs", "full"].includes(type)) throw new Response("Unknown crawl type.", {status: 400});
  if ((requestId !== null || attempt !== null) && (type === null || requestId === null || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(requestId)
    || attempt === null || !/^[1-9]\d*$/.test(attempt) || Number(attempt) > 4294967295)) {
    throw new Response("Select a crawl type, request and valid attempt number.", {status: 400});
  }
  try {
    return {domain, details: await loadDomainCrawlDetails(domain, type, requestId ? {requestId, attempt: Number(attempt)} : null, search.get("result") === "latest"), error: null};
  } catch (error) {
    if (error instanceof Response) throw error;
    return {domain, details: null, error: "Crawl results could not be loaded. Refresh crawl status to retry."};
  }
}

export default function DomainCrawl({loaderData: {domain, details, error}}: Route.ComponentProps) {
  const revalidator = useRevalidator();
  const selected = details?.crawls.find(crawl => crawl.type === details.selectedType);
  const result = details?.result;
  const newest = result && result.request_id === details?.latest?.request_id && result.attempt === details.latest.attempt;
  return <div className="flex min-w-0 flex-col gap-5">
    <header className="flex flex-wrap items-center justify-between gap-3">
      <div><h3 className="text-lg font-semibold">Crawl data</h3><p className="text-sm text-muted-foreground">The last good result is shown by default. Choose an attempt to inspect its outcome and data.</p></div>
      <Button variant="outline" size="sm" onClick={() => revalidator.revalidate()} disabled={revalidator.state !== "idle"}>Refresh crawl status</Button>
    </header>
    {error && <Alert variant="destructive"><AlertTitle>Crawl status unavailable</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
    {details && selected && <>
      <Tabs value={details.selectedType}><TabsList aria-label="Crawl types">{details.crawls.map(crawl => <TabsTrigger key={crawl.type} value={crawl.type} nativeButton={false} render={<Link to={`?type=${crawl.type}`} />}>{crawl.label}</TabsTrigger>)}</TabsList></Tabs>
      <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_20rem]">
        <section className="flex min-w-0 flex-col gap-4" aria-label="Crawl result details">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h3 className="text-lg font-semibold">{selected.label} · {details.showingSaved ? "Last good result" : newest ? "Latest attempt details" : "Attempt details"}</h3>
            {!details.showingSaved && selected.saved && <Button variant="outline" size="sm" nativeButton={false} render={<Link to={`?type=${selected.type}`} />}>View last good result</Button>}
          </div>
          {result ? <>
            <div className="flex flex-wrap items-center gap-3"><Badge variant={result.successful ? "secondary" : "destructive"}>{result.successful ? "Successful" : domainCrawlStatus(result)}</Badge><CrawlTime value={result.finished_at} /></div>
            {details.showingSaved && !newest && <p className="text-sm text-muted-foreground">This is the last good result. A newer attempt is listed in the history on the right.</p>}
            <p className="break-all font-mono text-xs text-muted-foreground">{result.request_id} · Attempt {result.attempt}</p>
            <p className="text-xs text-muted-foreground">Recorded outcome: {result.crawl_status} · Processing: {result.state}</p>
            {!result.successful && <Alert variant="destructive"><AlertTitle>{domainCrawlStatus(result)}</AlertTitle><AlertDescription>{result.error || "No failure reason was recorded."}</AlertDescription></Alert>}
          </> : <p className="text-sm text-muted-foreground">Not crawled. No result has been recorded for this type.</p>}
          {details.archiveError && <Alert variant="destructive"><AlertTitle>Saved JSON unavailable</AlertTitle><AlertDescription>{details.archiveError}</AlertDescription></Alert>}
          {details.payload ? <CrawlResultDetails key={`${details.selectedType}:${result?.request_id}:${result?.attempt}`} payload={details.payload} />
            : result && !details.archiveError && <p className="text-sm text-muted-foreground">No uploaded JSON is available for this attempt.</p>}
        </section>
        <DomainCrawls domain={domain} type={details.selectedType} attempts={details.attempts} selected={result ?? null} latest={details.latest} />
      </div>
    </>}
  </div>;
}

export function ErrorBoundary() {
  const error = useRouteError();
  return <Alert variant="destructive"><AlertTitle>Crawl attempt unavailable</AlertTitle><AlertDescription>{isRouteErrorResponse(error) ? String(error.data) : "The selected attempt could not be loaded."}</AlertDescription></Alert>;
}
