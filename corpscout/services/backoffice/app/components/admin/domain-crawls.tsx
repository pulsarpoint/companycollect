import { Link } from "react-router";
import type { DomainCrawlResult, DomainCrawlSummary } from "~/lib/domain-crawls.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

function outcome(result: DomainCrawlResult) {
  if (result.state === "failed" || result.state === "cancelled") return result.state === "failed" ? "Failed" : "Cancelled";
  if (result.crawl_status === "skip_crawling") return "Skipped by classification";
  if (result.crawl_status === "needs_review") return "Needs review";
  if (result.crawl_status === "partial") return "Partial";
  if (result.crawl_status === "finished" && result.successful) return "Crawled";
  return result.crawl_status === "failed" ? "Failed" : "Unsuccessful";
}

function CrawlTime({value}: {value: string}) {
  return <time dateTime={value}>{new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium", timeStyle: "short", timeZone: "UTC",
  }).format(new Date(value))} UTC</time>;
}

function ResultLink({result, children}: {result: DomainCrawlResult; children: React.ReactNode}) {
  return result.s3_state === "uploaded" && result.s3_path
    ? <Link className="underline underline-offset-4" to={`/admin/crawls/results?${new URLSearchParams({path: result.s3_path})}`}>{children}</Link>
    : null;
}

export function DomainCrawls({domain, crawls, error, onRefresh, loading}: {
  domain: string; crawls: DomainCrawlSummary[] | null; error: string | null; onRefresh: () => void; loading: boolean;
}) {
  return <section className="flex flex-col gap-3" aria-labelledby="domain-crawls-heading">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h3 id="domain-crawls-heading" className="text-lg font-semibold">Crawl data</h3>
        <p className="text-sm text-muted-foreground">Saved results and the latest completed attempt for each crawl type.</p></div>
      <div className="flex gap-2">
        <Button variant="outline" size="sm" nativeButton={false} render={<Link to={`/admin/crawls?${new URLSearchParams({domain, input_domain: domain})}`} />}>Crawl history</Button>
        <Button variant="outline" size="sm" onClick={onRefresh} disabled={loading}>Refresh crawl status</Button>
      </div>
    </div>
    {error ? <Alert variant="destructive"><AlertTitle>Crawl status unavailable</AlertTitle><AlertDescription>{error}</AlertDescription></Alert> : crawls && <Table>
      <TableHeader><TableRow><TableHead>Crawl type</TableHead><TableHead>Saved data</TableHead><TableHead>Last crawl status</TableHead><TableHead>Last attempt finished</TableHead><TableHead>Result</TableHead></TableRow></TableHeader>
      <TableBody>{crawls.map(({type, label, latest, saved}) => <TableRow key={type}>
        <TableCell>{label}</TableCell>
        <TableCell><div className="flex flex-col gap-1">
          <span>{saved ? saved.crawl_status === "skip_crawling" ? "Classification only" : "Crawled data available" : latest ? "No successful result" : "No saved data"}</span>
          {saved && <span className="text-xs text-muted-foreground"><CrawlTime value={saved.finished_at} /></span>}
          {saved && latest && (saved.request_id !== latest.request_id || saved.attempt !== latest.attempt) && <span className="text-xs text-muted-foreground">From an earlier attempt</span>}
        </div></TableCell>
        <TableCell><div className="flex max-w-md flex-col gap-1">
          <Badge variant={!latest ? "outline" : latest.successful ? "secondary" : "destructive"}>{latest ? outcome(latest) : "Not crawled"}</Badge>
          {latest?.error && <p className="whitespace-normal break-words text-xs text-muted-foreground">{latest.error}</p>}
        </div></TableCell>
        <TableCell>{latest ? <CrawlTime value={latest.finished_at} /> : "—"}</TableCell>
        <TableCell><div className="flex flex-col gap-1">
          {latest ? <ResultLink result={latest}>Latest attempt</ResultLink> : "—"}
          {saved && latest && (saved.request_id !== latest.request_id || saved.attempt !== latest.attempt) && <ResultLink result={saved}>Saved data</ResultLink>}
        </div></TableCell>
      </TableRow>)}</TableBody>
    </Table>}
    {crawls && !error && <p className="text-xs text-muted-foreground">A classification skip means the first page was inspected and further crawling was skipped. A later failed attempt does not remove an earlier saved result.</p>}
  </section>;
}
