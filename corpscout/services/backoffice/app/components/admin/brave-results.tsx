import { Link } from "react-router";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { braveResultsPath, braveTaskResultsPath } from "~/lib/brave-results";
import type { BraveCaptchaRequest, BraveResultsPage } from "~/lib/brave-results.server";
import { seCompanyTabPath } from "~/lib/se-company-tabs";

function captchaLabel(requests: BraveCaptchaRequest[] = []) {
  if (requests.some(request => request.presented || request.attempts > 0)) return "Presented";
  return requests.length && requests.every(request => request.presented === false) ? "Off" : "Not recorded";
}

function utcTime(value: string | null) {
  return value ? new Date(value).toISOString().replace("T", " ").slice(0, 19) + " UTC" : "Not recorded";
}

export function BraveResults({ results, basePath, showCompany = false }: {
  results: BraveResultsPage; basePath: string; showCompany?: boolean;
}) {
  const { rows, selected, total, succeeded, failed, page, totalPages } = results;
  const captchaDetails = selected?.captcha_requests?.filter(request => request.presented || request.attempts > 0) ?? [];
  return <section className="flex min-w-0 flex-col gap-4" aria-label="Brave search results">
    <div className="flex flex-col gap-1">
      <h2 className="text-lg font-semibold">Brave results</h2>
      <p className="text-sm text-muted-foreground">{total.toLocaleString()} saved attempts · {succeeded.toLocaleString()} successful · {failed.toLocaleString()} failed</p>
      <p className="text-sm text-muted-foreground">Questions and answers from every saved attempt, newest first.</p>
    </div>
    {total === 0 ? <Empty><EmptyHeader><EmptyTitle>No Brave results yet</EmptyTitle>
      <EmptyDescription>There are no saved search attempts for this selection. Skipped inputs do not produce a new answer.</EmptyDescription>
    </EmptyHeader></Empty> : <>
      <Table>
        <TableHeader><TableRow><TableHead>Completed (UTC)</TableHead>{showCompany && <TableHead>Company</TableHead>}<TableHead>Question</TableHead><TableHead>Status</TableHead><TableHead>CAPTCHA</TableHead><TableHead>Proxy route</TableHead><TableHead>Result</TableHead><TableHead><span className="sr-only">Details</span></TableHead></TableRow></TableHeader>
        <TableBody>{rows.map(row => <TableRow key={row.result_id} data-state={row.result_id === selected?.result_id ? "selected" : undefined}>
          <TableCell className="align-top">{row.completed_at.slice(0, 19)}</TableCell>
          {showCompany && <TableCell className="max-w-64 whitespace-normal align-top">
            {row.country_code === "SE" ? <Link className="underline" to={braveResultsPath(seCompanyTabPath(row.company_id, "brave"), 1, row.result_id)}>{row.company_name}</Link> : row.company_name}
            <p className="text-xs text-muted-foreground">{row.country_code}:{row.company_id}</p>
          </TableCell>}
          <TableCell className="max-w-md whitespace-normal align-top"><p className="line-clamp-3">{row.query}</p><p className="text-xs text-muted-foreground">{row.search_name ? `${row.search_name} · version ${row.search_revision}` : row.query_type}</p></TableCell>
          <TableCell className="align-top"><Badge variant={row.status === "success" ? "secondary" : "destructive"}>{row.status === "success" ? "Successful" : "Failed"}</Badge></TableCell>
          <TableCell className="align-top">{captchaLabel(row.captcha_requests)}</TableCell>
          <TableCell className="align-top">{[...new Set(row.captcha_requests?.map(request => request.proxy_route).filter(Boolean))].join(", ") || "Not recorded"}</TableCell>
          <TableCell className="max-w-lg whitespace-normal align-top"><p className="line-clamp-3">{row.status === "error" ? row.error_type || "Search failed without a recorded reason." : row.answer_preview}</p>{row.status === "error" && row.error_stage && <p className="text-xs text-muted-foreground">Stage: {row.error_stage}</p>}</TableCell>
          <TableCell className="align-top"><Button variant="outline" size="sm" nativeButton={false}
            render={<Link to={`${braveResultsPath(basePath, page, row.result_id)}#brave-result`} />} aria-label={`View result for ${row.company_name} at ${row.completed_at}`}>View result</Button></TableCell>
        </TableRow>)}</TableBody>
      </Table>
      <nav className="flex items-center justify-between gap-3" aria-label="Brave result pages">
        <span className="text-sm text-muted-foreground">Page {page.toLocaleString()} of {totalPages.toLocaleString()}</span>
        <div className="flex gap-2">
          {page > 1 && <Button variant="outline" nativeButton={false} render={<Link to={braveResultsPath(basePath, page - 1)} />}>Previous</Button>}
          {page < totalPages && <Button variant="outline" nativeButton={false} render={<Link to={braveResultsPath(basePath, page + 1)} />}>Next</Button>}
        </div>
      </nav>
    </>}
    {selected && <Card id="brave-result" className="scroll-mt-6">
      <CardHeader>
        <CardTitle>{selected.company_name}</CardTitle>
        <CardDescription>{selected.country_code}:{selected.company_id} · {selected.completed_at.slice(0, 19)} UTC · {selected.search_name ? `${selected.search_name} · version ${selected.search_revision}` : selected.query_type}</CardDescription>
      </CardHeader>
      <CardContent className="flex min-w-0 flex-col gap-5">
        <div className="flex flex-wrap items-center gap-3">
          <Badge variant={selected.status === "success" ? "secondary" : "destructive"}>{selected.status === "success" ? "Successful" : "Failed"}</Badge>
          <Link className="text-sm underline" to={`${braveResultsPath(basePath, page, selected.result_id)}#brave-result`}>Link to this result</Link>
          <Link className="text-sm underline" to={braveResultsPath(braveTaskResultsPath(selected.task_id), 1, selected.result_id)}>Task results</Link>
          {selected.runUrl && <a className="text-sm underline" href={selected.runUrl} target="_blank" rel="noreferrer">View in Dagster</a>}
        </div>
        <section className="flex flex-col gap-2" aria-label="Question"><h3 className="font-medium">Question</h3><p className="whitespace-pre-wrap break-words">{selected.query}</p></section>
        {selected.status === "error" && <Alert variant="destructive"><AlertTitle>Search failed</AlertTitle><AlertDescription>
          <p>{selected.error_type || "No failure reason was recorded."}</p>
          {selected.error_stage && <p>Stage: {selected.error_stage}</p>}
        </AlertDescription></Alert>}
        {captchaDetails.length === 0 ? <p className="text-sm text-muted-foreground">CAPTCHA: {captchaLabel(selected.captcha_requests)}</p> : <section className="flex flex-col gap-3" aria-label="CAPTCHA statistics">
          <h3 className="font-medium">CAPTCHA</h3>
          {captchaDetails.map(request =>
            <dl key={request.external_request_id} className="grid gap-2 break-words text-sm sm:grid-cols-[auto_1fr]">
              <dt>Browser request</dt><dd className="break-all">{request.external_request_id}</dd>
              <dt>Presented</dt><dd>{request.presented === null ? "Not recorded" : request.presented ? "Yes" : "No"}</dd>
              <dt>Detected at</dt><dd>{utcTime(request.detected_at)}{request.detection_source === "agent_start" && " (agent start)"}</dd>
              <dt>Clearance</dt><dd>{request.presented ? request.cleared ? "Cleared" : "Not confirmed" : "—"}</dd>
              <dt>Confirmed at</dt><dd>{utcTime(request.confirmed_at)}</dd>
              <dt>Reported input tokens</dt><dd>{request.prompt_tokens?.toLocaleString() ?? "Not recorded"}</dd>
              <dt>Reported output tokens</dt><dd>{request.completion_tokens?.toLocaleString() ?? "Not recorded"}</dd>
              <dt>Assistant attempts</dt><dd>{request.attempts}</dd>
              <dt>Model</dt><dd>{request.models.join(", ") || "Not recorded"}</dd>
              <dt>Proxy route</dt><dd>{request.proxy_route || "Not recorded"}</dd>
            </dl>)}
        </section>}
        <section className="flex min-w-0 flex-col gap-2" aria-label="Answer"><h3 className="font-medium">Answer</h3>
          {selected.answer_text ? <div className="flex min-w-0 max-w-5xl flex-col gap-3 break-words leading-relaxed">
            <Markdown remarkPlugins={[remarkGfm]} skipHtml components={{
              a: ({ children, href }) => <a className="underline" href={href} target="_blank" rel="noreferrer">{children}</a>,
              img: () => null,
              ul: ({ children }) => <ul className="flex list-disc flex-col gap-1 pl-5">{children}</ul>,
              ol: ({ children }) => <ol className="flex list-decimal flex-col gap-1 pl-5">{children}</ol>,
              pre: ({ children }) => <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 text-xs">{children}</pre>,
              table: ({ children }) => <div className="overflow-x-auto"><table>{children}</table></div>,
            }}>{selected.answer_text}</Markdown>
          </div> : <p className="text-muted-foreground">No answer was saved for this attempt.</p>}
        </section>
        <details><summary className="cursor-pointer text-sm">Attempt details</summary>
          <dl className="mt-3 grid gap-2 break-all text-xs sm:grid-cols-[auto_1fr]">
            <dt>Result ID</dt><dd>{selected.result_id}</dd><dt>Task ID</dt><dd>{selected.task_id}</dd>
            <dt>Execution ID</dt><dd>{selected.execution_id}</dd><dt>Run ID</dt><dd>{selected.source_run_id || "Not recorded"}</dd>
            <dt>Attempt</dt><dd>{selected.attempt}</dd><dt>Duration</dt><dd>{(selected.elapsed_ms / 1000).toLocaleString()} seconds</dd>
            <dt>Route</dt><dd>{selected.route || "Not recorded"}</dd><dt>Source URL</dt><dd>{selected.source_url || "Not recorded"}</dd>
          </dl>
        </details>
      </CardContent>
    </Card>}
  </section>;
}
