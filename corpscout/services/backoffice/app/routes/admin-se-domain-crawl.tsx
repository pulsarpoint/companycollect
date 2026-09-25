import { Link, useRevalidator } from "react-router";
import type { Route } from "./+types/admin-se-domain-crawl";
import { DomainCrawls } from "~/components/admin/domain-crawls";
import { CrawlResultDetails } from "~/components/admin/crawl-result-details";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { loadDomainCrawlDetails } from "~/lib/domain-crawls.server";
import { graphDomainError } from "~/lib/domain-graph";

export async function loader({params, request}: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  if (!domain || graphDomainError(domain)) throw new Response("Not found", {status: 404});
  const search = new URL(request.url).searchParams;
  try {
    return {domain, details: await loadDomainCrawlDetails(domain, search.get("type"), search.get("result") === "saved"), error: null};
  } catch {
    return {domain, details: null, error: "Crawl results could not be loaded. Refresh crawl status to retry."};
  }
}

export default function DomainCrawl({loaderData: {domain, details, error}}: Route.ComponentProps) {
  const revalidator = useRevalidator();
  const selected = details?.crawls.find(crawl => crawl.type === details.selectedType);
  return <div className="flex min-w-0 flex-col gap-6">
    <DomainCrawls domain={domain} crawls={details?.crawls ?? null} error={error}
      onRefresh={() => revalidator.revalidate()} loading={revalidator.state !== "idle"} />
    {details && selected && <section className="flex min-w-0 flex-col gap-4" aria-label="Crawl result details">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-lg font-semibold">{selected.label} · {details.previous ? "Earlier saved result" : "Latest attempt details"}</h3>
        <div className="flex gap-2">
          {details.previous && <Button variant="outline" size="sm" nativeButton={false} render={<Link to={`?type=${selected.type}`} />}>Latest attempt</Button>}
          {!details.previous && selected.saved && selected.latest && (selected.saved.request_id !== selected.latest.request_id || selected.saved.attempt !== selected.latest.attempt) &&
            <Button variant="outline" size="sm" nativeButton={false} render={<Link to={`?type=${selected.type}&result=saved`} />}>View earlier saved result</Button>}
        </div>
      </div>
      {details.result && <p className="break-all font-mono text-xs text-muted-foreground">{details.result.request_id} · Attempt {details.result.attempt} · {details.result.finished_at}</p>}
      {details.archiveError && <Alert variant="destructive"><AlertTitle>Saved JSON unavailable</AlertTitle><AlertDescription>{details.archiveError}</AlertDescription></Alert>}
      {details.payload ? <CrawlResultDetails key={`${details.selectedType}:${details.result?.request_id}:${details.result?.attempt}`} payload={details.payload} />
        : !details.archiveError && <p className="text-sm text-muted-foreground">{details.result ? "No uploaded JSON is available for this attempt." : "No crawl result has been recorded for this type."}</p>}
    </section>}
  </div>;
}
