import { isRouteErrorResponse, Link, useRevalidator, useRouteError } from "react-router";
import { CrawlTime, DomainCrawls } from "~/components/admin/domain-crawls";
import { CrawlResultDetails } from "~/components/admin/crawl-result-details";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { DomainCrawlResult, loadDomainCrawlPage } from "~/lib/domain-crawls.server";
import { domainCrawlStatus } from "~/lib/domain-crawl-status";

export function DomainCrawlView({domain, details, error, search = ""}: Awaited<ReturnType<typeof loadDomainCrawlPage>> & {search?: string}) {
  function crawlHref(type: string, attempt?: DomainCrawlResult) {
    const next = new URLSearchParams(search);
    next.set("type", type);
    if (attempt) {
      next.set("request", attempt.request_id);
      next.set("attempt", String(attempt.attempt));
    }
    return `?${next}`;
  }
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
      <Tabs value={details.selectedType}><TabsList aria-label="Crawl types">{details.crawls.map(crawl => <TabsTrigger key={crawl.type} value={crawl.type} nativeButton={false} render={<Link to={crawlHref(crawl.type)} />}>{crawl.label}</TabsTrigger>)}</TabsList></Tabs>
      <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_20rem]">
        <section className="flex min-w-0 flex-col gap-4" aria-label="Crawl result details">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h3 className="text-lg font-semibold">{selected.label} · {details.showingSaved ? "Last good result" : newest ? "Latest attempt details" : "Attempt details"}</h3>
            {!details.showingSaved && selected.saved && <Button variant="outline" size="sm" nativeButton={false} render={<Link to={crawlHref(selected.type)} />}>View last good result</Button>}
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
        <DomainCrawls domain={domain} attemptHref={attempt => crawlHref(details.selectedType, attempt)} attempts={details.attempts} selected={result ?? null} latest={details.latest} />
      </div>
    </>}
  </div>;
}

export function DomainCrawlErrorBoundary() {
  const error = useRouteError();
  return <Alert variant="destructive"><AlertTitle>Crawl attempt unavailable</AlertTitle><AlertDescription>{isRouteErrorResponse(error) ? String(error.data) : "The selected attempt could not be loaded."}</AlertDescription></Alert>;
}
