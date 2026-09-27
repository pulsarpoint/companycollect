import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
const server = vi.hoisted(() => ({crawlerFetch: vi.fn()}));
vi.mock("~/lib/crawler.server", () => ({...server, CrawlerApiError: class extends Error {constructor(message: string, public status: number) {super(message);}}}));
import { loader } from "~/routes/admin-crawls-debug";
import { CrawlDebugDetails } from "~/components/admin/crawl-debug";

describe("crawl debug API", () => {
  it("loads the lookup output for the selected attempt", async () => {
    server.crawlerFetch.mockResolvedValue(Response.json({status: "not_found", site_type: "online_store"}));
    const response = await loader({request: new Request("http://backoffice/admin/crawls/debug?request=test&attempt=2&result=1")} as Parameters<typeof loader>[0]);
    expect(server.crawlerFetch).toHaveBeenLastCalledWith("/v1/crawls/test/debug?attempt=2&result=true", expect.anything());
    expect(await response.json()).toEqual({status: "not_found", site_type: "online_store"});
  });
  it("relays saved-file events without buffering and forwards reconnect cursors", async () => {
    let output!: ReadableStreamDefaultController<Uint8Array>;
    server.crawlerFetch.mockResolvedValue(new Response(new ReadableStream({start(controller) {output = controller;}}), {headers: {"Content-Type": "text/event-stream"}}));
    const response = await loader({request: new Request("http://backoffice/admin/crawls/debug?request=test-1&attempt=1&stream=1", {headers: {"Last-Event-ID": "1:237"}})} as Parameters<typeof loader>[0]);
    expect(server.crawlerFetch).toHaveBeenLastCalledWith("/v1/crawls/test-1/debug?attempt=1&stream=true", expect.objectContaining({headers: {"Last-Event-ID": "1:237"}}));
    const reader = response.body!.getReader();
    output.enqueue(new TextEncoder().encode("id: 1:400\nevent: crawl-debug\ndata: {}\n\n"));
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("id: 1:400");
    expect(response.headers.get("Cache-Control")).toBe("no-store");
    expect(response.headers.get("X-Accel-Buffering")).toBe("no");
    output.close();
  });
  it.each(["request=../other", "request=test&after=-1", "request=test&event=0", "request=test&attempt=1/../../x"])("rejects invalid paths/cursors: %s", async query => {
    const response = await loader({request: new Request(`http://backoffice/admin/crawls/debug?${query}`)} as Parameters<typeof loader>[0]);
    expect(response.status).toBe(400);
  });
  it("preserves the downloadable full trace", async () => {
    server.crawlerFetch.mockResolvedValue(new Response('{"details":"full"}\n', {headers: {"Content-Type": "application/x-ndjson", "Content-Disposition": 'attachment; filename="debug.jsonl"'}}));
    const response = await loader({request: new Request("http://backoffice/admin/crawls/debug?request=test&download=1")} as Parameters<typeof loader>[0]);
    expect(response.headers.get("Content-Disposition")).toContain("attachment");
    expect(await response.text()).toBe('{"details":"full"}\n');
  });
  it("renders complete diagnostic data as text, including untrusted HTML and long model answers", () => {
    const content = "complete response ".repeat(500);
    const html = renderToStaticMarkup(<CrawlDebugDetails event={{id: 3, timestamp: "2026-09-27T09:00:00.123Z", elapsed_ms: 2200, duration_ms: 1200, stage: "model_http", message: "Response", operation: "http-2", level: "error", has_details: true}} details={{content, html: '<script>alert("test")</script>'}} />);
    expect(html).toContain(content);
    expect(html).not.toContain("<script>");
    expect(html).toContain("&lt;script&gt;");
    expect(html).toContain("1.20 s");
  });
});
