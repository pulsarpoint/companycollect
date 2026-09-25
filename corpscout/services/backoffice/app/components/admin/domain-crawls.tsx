import { Link } from "react-router";
import { domainCrawlStatus } from "~/lib/domain-crawl-status";
import type { DomainCrawlResult } from "~/lib/domain-crawls.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { cn } from "~/lib/utils";

export function CrawlTime({value}: {value: string}) {
  return <time dateTime={value}>{new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium", timeStyle: "short", timeZone: "UTC",
  }).format(new Date(value))} UTC</time>;
}

export function DomainCrawls({domain, type, attempts, selected, latest}: {
  domain: string; type: string; attempts: DomainCrawlResult[]; selected: DomainCrawlResult | null; latest: DomainCrawlResult | null;
}) {
  return <aside className="flex min-w-0 flex-col gap-3 lg:border-l lg:pl-5" aria-label="Recent crawl attempts">
    <div><h3 className="text-lg font-semibold">Recent attempts</h3>
      <p className="text-sm text-muted-foreground">Latest 20 attempts, newest first. Select one to view its saved details.</p></div>
    <ol className="flex flex-col gap-2">{attempts.map(attempt => {
      const current = attempt.request_id === selected?.request_id && attempt.attempt === selected.attempt;
      const newest = attempt.request_id === latest?.request_id && attempt.attempt === latest.attempt;
      return <li key={`${attempt.request_id}:${attempt.attempt}`}>
        <Link to={`?${new URLSearchParams({type, request: attempt.request_id, attempt: String(attempt.attempt)})}`}
          aria-current={current ? "true" : undefined}
          className={cn("flex flex-col gap-2 rounded-md border p-3 text-sm hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring", current && "border-primary bg-muted/50")}>
          <div className="flex flex-wrap items-center gap-2"><Badge variant={attempt.successful ? "secondary" : "destructive"}>{attempt.successful ? "Successful" : domainCrawlStatus(attempt)}</Badge>{newest && <span className="text-xs text-muted-foreground">Latest attempt</span>}{current && <span className="text-xs">Viewing</span>}</div>
          <CrawlTime value={attempt.finished_at} />
          <span className="break-all text-xs text-muted-foreground">{attempt.request_id} · Attempt {attempt.attempt}</span>
          {attempt.error && <p className="line-clamp-3 break-words text-xs text-muted-foreground">{attempt.error}</p>}
        </Link>
      </li>;
    })}</ol>
    {!attempts.length && <p className="text-sm text-muted-foreground">No recorded attempts for this crawl type.</p>}
    <Button variant="outline" size="sm" nativeButton={false} render={<Link to={`/admin/crawls?${new URLSearchParams({domain, input_domain: domain})}`} />}>All crawl history</Button>
  </aside>;
}
