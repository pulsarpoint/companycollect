import { useState } from "react";
import { isRouteErrorResponse, Link, useRouteError } from "react-router";
import { ArrowLeftIcon, DownloadIcon, ExternalLinkIcon } from "lucide-react";
import type { Route } from "./+types/admin-crawl-result";
import { readCrawlArchive } from "~/lib/crawl-results.server";
import { resultObject, resultObjects, resultText, resultWebUrl } from "~/lib/crawl-results";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";

export async function loader({request}: Route.LoaderArgs) {
  const {result_json, ...archive} = await readCrawlArchive(new URL(request.url).searchParams.get("path"));
  let payload: unknown;
  try { payload = JSON.parse(result_json); }
  catch { throw new Response("The saved result is not valid JSON.", {status: 502}); }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Response("The saved result is not a JSON object.", {status: 502});
  return {archive, payload: resultObject(payload)};
}

export function meta() { return [{title: "Saved crawl result | CompanyCollect admin"}]; }

function JsonContent({value}: {value: unknown}) {
  return <pre className="max-h-[65vh] overflow-auto rounded-md border bg-muted/30 p-4 text-xs whitespace-pre-wrap break-words">{JSON.stringify(value, null, 2) ?? "Not collected"}</pre>;
}

function SourceLink({url}: {url: unknown}) {
  const href = resultWebUrl(url);
  return href ? <a href={href} target="_blank" rel="noreferrer noopener" className="break-all underline underline-offset-4">{resultText(url)}<ExternalLinkIcon className="ml-1 inline size-3" /></a> : <span className="break-all">{resultText(url)}</span>;
}

export default function AdminCrawlResult({loaderData}: Route.ComponentProps) {
  const {archive, payload} = loaderData;
  const crawl = resultObject(payload.crawl);
  const landingPage = resultObjects(crawl.pages).find(page => page.selected_for === "input_url" && page.fetch_status === "fetched");
  const documents = resultObjects(payload.documents);
  const usage = resultObject(crawl.usage ?? payload.model_usage ?? payload.usage);
  const [pageIndex, setPageIndex] = useState(0);
  const [section, setSection] = useState("contacts");
  const document = documents[pageIndex] ?? documents[0];
  const input = resultObject(document?.input);
  const observations = resultObject(input.observations);
  const page = resultObject(input.page);
  const sections = Object.keys(observations);
  const selectedSection = sections.includes(section) ? section : sections[0];
  const links = resultObjects(input.links);
  const summary = Object.fromEntries(Object.entries(payload).filter(([key]) => key !== "documents" && key !== "crawl"));

  return <div className="flex min-w-0 flex-col gap-6 p-4 md:p-6">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div className="flex min-w-0 flex-col gap-2"><Button variant="ghost" size="sm" className="w-fit" render={<Link to={`/admin/crawls?domain=${encodeURIComponent(archive.domain)}`} />}><ArrowLeftIcon data-icon="inline-start" />Crawl history</Button>
        <h1 className="text-2xl font-semibold">{archive.domain}</h1>
        <p className="break-all text-sm text-muted-foreground">{archive.request_id}{archive.attempt !== null ? ` · Attempt ${archive.attempt}` : ""}</p>
        <div className="flex flex-wrap items-center gap-2"><Badge variant={archive.status === "failed" ? "destructive" : "secondary"}>{archive.status}</Badge><Badge variant="outline">Read from S3 via ClickHouse</Badge><span className="text-sm text-muted-foreground">{documents.length} collected pages</span></div>
      </div>
      <Button variant="outline" render={<a href={`/admin/crawls/result.json?${new URLSearchParams({path: archive.source_path})}`} />}><DownloadIcon data-icon="inline-start" />Download JSON</Button>
    </div>
    {Boolean(crawl.stop_reason) && <Alert><AlertTitle>Crawl outcome</AlertTitle><AlertDescription>{crawl.stop_reason === "page_budget" ? "The configured page limit was reached. The saved result contains the pages collected up to that limit." : resultText(crawl.stop_reason)}</AlertDescription></Alert>}
    <Tabs defaultValue="overview">
      <TabsList><TabsTrigger value="overview">Overview</TabsTrigger><TabsTrigger value="pages">Collected pages ({documents.length})</TabsTrigger><TabsTrigger value="json">Full JSON</TabsTrigger></TabsList>
      <TabsContent value="overview" className="flex flex-col gap-5">
        <dl className="grid gap-4 text-sm sm:grid-cols-2">
          <div><dt className="text-muted-foreground">Original website</dt><dd><SourceLink url={crawl.input_url ?? archive.website_url} /></dd></div>
          {landingPage && <div><dt className="text-muted-foreground">Final website</dt><dd><SourceLink url={landingPage.source_url} /></dd></div>}
          <div><dt className="text-muted-foreground">Mode</dt><dd>{resultText(crawl.mode)}</dd></div>
          <div><dt className="text-muted-foreground">Started / finished</dt><dd>{resultText(crawl.started_at ?? payload.started_at)} / {resultText(crawl.finished_at ?? payload.finished_at)}</dd></div>
          <div><dt className="text-muted-foreground">Model usage</dt><dd>{resultText(usage.calls, "0")} calls · {resultText(usage.prompt_tokens, "0")} input / {resultText(usage.completion_tokens, "0")} output tokens</dd></div>
          <div className="sm:col-span-2"><dt className="text-muted-foreground">Saved object</dt><dd className="break-all">s3://{archive.source_path}</dd></div>
        </dl>
        <h2 className="text-base font-medium">Crawl details and extracted sections</h2>
        <JsonContent value={{...summary, ...(payload.crawl ? {crawl} : {})}} />
      </TabsContent>
      <TabsContent value="pages" className="flex min-w-0 flex-col gap-4">
        {!document ? <Empty><EmptyHeader><EmptyTitle>No collected pages</EmptyTitle><EmptyDescription>This saved result contains no page documents. Check Overview or Full JSON for errors, site classification, or analysis sections.</EmptyDescription></EmptyHeader></Empty> : <>
          <FieldGroup><Field><FieldLabel htmlFor="saved-crawl-page">Collected page</FieldLabel><NativeSelect id="saved-crawl-page" value={String(pageIndex)} onChange={event => setPageIndex(Number(event.target.value))}>{documents.map((item, index) => <NativeSelectOption value={String(index)} key={index}>{resultText(item.page_id, String(index + 1))} · {resultText(item.url)}</NativeSelectOption>)}</NativeSelect></Field></FieldGroup>
          <div className="flex flex-col gap-1 text-sm"><h2 className="font-medium">{resultText(resultObject(observations.metadata).title, resultText(document.page_id))}</h2><SourceLink url={document.url} /><p className="text-muted-foreground">HTTP {resultText(page.status_code)} · {resultText(page.fetched_at)} · {links.length} discovered links</p></div>
          <Tabs defaultValue="data" key={pageIndex}>
            <TabsList className="max-w-full flex-wrap"><TabsTrigger value="data">Extracted data</TabsTrigger><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="html">Simplified HTML</TabsTrigger><TabsTrigger value="rendered">Rendered HTML</TabsTrigger><TabsTrigger value="links">Links</TabsTrigger></TabsList>
            <TabsContent value="data" className="flex flex-col gap-3">{sections.length ? <><FieldGroup><Field><FieldLabel htmlFor="saved-crawl-section">Data section</FieldLabel><NativeSelect id="saved-crawl-section" value={selectedSection} onChange={event => setSection(event.target.value)}>{sections.map(key => <NativeSelectOption key={key} value={key}>{key.replaceAll("_", " ")}</NativeSelectOption>)}</NativeSelect></Field></FieldGroup><JsonContent value={observations[selectedSection]} /></> : <JsonContent value={input} />}</TabsContent>
            <TabsContent value="text"><pre className="max-h-[65vh] overflow-auto rounded-md border p-4 text-sm whitespace-pre-wrap break-words">{resultText(resultObject(observations.text).visible, "No extracted text was saved.")}</pre></TabsContent>
            <TabsContent value="html"><pre className="max-h-[65vh] overflow-auto rounded-md border p-4 text-xs whitespace-pre-wrap break-words">{resultText(document.html, "No simplified HTML was saved.")}</pre></TabsContent>
            <TabsContent value="rendered"><pre className="max-h-[65vh] overflow-auto rounded-md border p-4 text-xs whitespace-pre-wrap break-words">{resultText(document.rendered_html, "No original rendered HTML was saved.")}</pre></TabsContent>
            <TabsContent value="links"><Table><TableHeader><TableRow><TableHead>Label / context</TableHead><TableHead>URL</TableHead></TableRow></TableHeader><TableBody>{links.map((link, index) => <TableRow key={index}><TableCell className="max-w-md whitespace-normal">{resultText(link.anchor_text)}<p className="text-xs text-muted-foreground">{resultText(resultObject(link.context).section_heading, "")}</p></TableCell><TableCell className="max-w-xl whitespace-normal"><SourceLink url={link.url} /></TableCell></TableRow>)}</TableBody></Table></TabsContent>
          </Tabs>
        </>}
      </TabsContent>
      <TabsContent value="json"><JsonContent value={payload} /></TabsContent>
    </Tabs>
  </div>;
}

export function ErrorBoundary() {
  const error = useRouteError();
  return <div className="flex flex-col gap-4 p-6"><Button variant="outline" className="w-fit" render={<Link to="/admin/crawls" />}>Crawl history</Button><Alert variant="destructive"><AlertTitle>Saved result unavailable</AlertTitle><AlertDescription>{isRouteErrorResponse(error) ? String(error.data) : "Unable to display this saved crawl result."}</AlertDescription></Alert></div>;
}
