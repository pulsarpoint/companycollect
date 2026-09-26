import { useState } from "react";
import { resultObject, resultObjects, resultText, resultWebUrl } from "~/lib/crawl-results";
import { Field, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

/** Render source values as text, fields and lists; never execute archived HTML. */
function ParsedValue({value}: {value: unknown}) {
  if (value == null || value === "") return <span className="text-muted-foreground">Not recorded</span>;
  if (Array.isArray(value)) return value.length ? <ul className="flex list-disc flex-col gap-2 pl-5">{value.map((item, index) => <li key={index}><ParsedValue value={item} /></li>)}</ul> : <span className="text-muted-foreground">None recorded</span>;
  if (typeof value === "object") return <dl className="flex flex-col gap-3">{Object.entries(value).map(([key, item]) => <div key={key} className="grid gap-1 sm:grid-cols-[minmax(8rem,1fr)_3fr]">
    <dt className="text-muted-foreground">{key.replaceAll("_", " ")}</dt><dd className="min-w-0 break-words"><ParsedValue value={item} /></dd>
  </div>)}</dl>;
  const url = resultWebUrl(value);
  return url ? <a className="break-all underline underline-offset-4" href={url} target="_blank" rel="noreferrer">{String(value)}</a> : <span className="whitespace-pre-wrap break-words">{typeof value === "boolean" ? value ? "Yes" : "No" : String(value)}</span>;
}

export function CrawlResultDetails({payload}: {payload: Record<string, unknown>}) {
  const crawl = resultObject(payload.crawl);
  const documents = resultObjects(payload.documents);
  const pages = resultObjects(crawl.pages);
  const [pageIndex, setPageIndex] = useState(0);
  const document = documents[pageIndex];
  const observations = resultObject(resultObject(document?.input).observations);
  const usage = resultObject(crawl.usage ?? payload.model_usage ?? payload.usage);
  const sections = Object.entries(observations).filter(([key]) => !["schema_version", "html_sha256", "text"].includes(key));
  const summary = {website: crawl.site_url ?? crawl.input_url, mode: crawl.mode,
    started_at: crawl.started_at ?? payload.started_at, finished_at: crawl.finished_at ?? payload.finished_at,
    ...(typeof crawl.full_crawl_all === "boolean" ? {full_crawl_all: crawl.full_crawl_all, classification_override: resultObject(crawl.site_gate).overridden ?? false} : {}),
    stop_reason: crawl.stop_reason, collected_pages: documents.length || pages.length,
    model_calls: usage.calls, input_tokens: usage.prompt_tokens, output_tokens: usage.completion_tokens};
  const classification = crawl.site_info ?? resultObject(crawl.site_gate).profile;
  const extra = Object.fromEntries(Object.entries(payload).filter(([key]) => !["schema_version", "crawl", "documents"].includes(key)));
  return <Tabs defaultValue="details" className="min-w-0">
    <TabsList className="max-w-full flex-wrap"><TabsTrigger value="details">Parsed details</TabsTrigger><TabsTrigger value="pages">Collected pages ({documents.length || pages.length})</TabsTrigger><TabsTrigger value="json">Full JSON</TabsTrigger></TabsList>
    <TabsContent value="details" className="flex flex-col gap-6 text-sm">
      <ParsedValue value={summary} />
      {classification != null && <section className="flex flex-col gap-3"><h4 className="font-semibold">Site classification</h4><ParsedValue value={classification} /></section>}
      {Object.keys(extra).length > 0 && <section className="flex flex-col gap-3"><h4 className="font-semibold">Extracted information</h4><ParsedValue value={extra} /></section>}
      {resultObjects(crawl.errors).length > 0 && <section className="flex flex-col gap-3"><h4 className="font-semibold">Recorded errors</h4><ParsedValue value={crawl.errors} /></section>}
      {pages.length > 0 && <section className="flex flex-col gap-3"><h4 className="font-semibold">Fetched pages</h4><Table><TableHeader><TableRow><TableHead>Page</TableHead><TableHead>HTTP status</TableHead><TableHead>Fetch status</TableHead><TableHead>Errors</TableHead></TableRow></TableHeader><TableBody>
        {pages.map((page, index) => <TableRow key={index}><TableCell className="max-w-lg whitespace-normal"><ParsedValue value={page.source_url ?? page.requested_url} /></TableCell><TableCell>{resultText(page.status_code)}</TableCell><TableCell>{resultText(page.fetch_status)}</TableCell><TableCell className="max-w-md whitespace-normal"><ParsedValue value={page.errors} /></TableCell></TableRow>)}
      </TableBody></Table></section>}
    </TabsContent>
    <TabsContent value="pages" className="flex min-w-0 flex-col gap-5 text-sm">
      {!documents.length ? <p>No page documents were saved in this result.</p> : <>
        <Field><FieldLabel htmlFor="domain-crawl-page">Collected page</FieldLabel><NativeSelect id="domain-crawl-page" value={String(pageIndex)} onChange={event => setPageIndex(Number(event.target.value))}>
          {documents.map((page, index) => <NativeSelectOption key={index} value={String(index)}>{resultText(page.page_id, String(index + 1))} · {resultText(page.url)}</NativeSelectOption>)}
        </NativeSelect></Field>
        <ParsedValue value={resultObject(document?.input).page} />
        {sections.map(([name, value]) => <section key={name} className="flex flex-col gap-3"><h4 className="font-semibold capitalize">{name.replaceAll("_", " ")}</h4><ParsedValue value={value} /></section>)}
        <section className="flex flex-col gap-3"><h4 className="font-semibold">Extracted text</h4><div className="max-h-[32rem] overflow-auto"><ParsedValue value={resultObject(observations.text).visible} /></div></section>
        <section className="flex flex-col gap-3"><h4 className="font-semibold">Discovered links</h4><ParsedValue value={resultObject(document?.input).links} /></section>
      </>}
    </TabsContent>
    <TabsContent value="json"><pre className="max-h-[65vh] overflow-auto rounded-md border p-4 text-xs whitespace-pre-wrap break-words">{JSON.stringify(payload, null, 2)}</pre></TabsContent>
  </Tabs>;
}
