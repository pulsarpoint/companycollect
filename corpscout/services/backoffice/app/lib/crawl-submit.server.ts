import { crawlerFetch } from "~/lib/crawler.server";
import type { CrawlPublishReceipt } from "~/lib/crawler";

/** Submit one operator test crawl straight to the crawler's REST queue. */
export async function publishTestCrawl(body: string): Promise<CrawlPublishReceipt> {
  if (process.env.CRAWLER_TEST_SUBMIT_ENABLED !== "true") {
    throw new Error("Test submissions are disabled. Set CRAWLER_TEST_SUBMIT_ENABLED=true on Backoffice.");
  }
  if (Buffer.byteLength(body) > 131_072) throw new Error("The request must be 128 KiB or smaller.");
  let payload: unknown;
  try { payload = JSON.parse(body); }
  catch { throw new Error("Enter a valid JSON request."); }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)
    || !("request_id" in payload) || typeof payload.request_id !== "string" || !payload.request_id) {
    throw new Error("Include a request_id in the JSON. Keep it unchanged when retrying a submission.");
  }
  // The crawler owns validation and persistence. Resubmitting the same request_id with
  // the same request returns the existing job; a different request under it is a 409.
  const response = await crawlerFetch("/v1/crawls", {
    method: "POST", headers: {"Content-Type": "application/json"}, body,
  });
  const job = await response.json() as CrawlPublishReceipt;
  return {request_id: job.request_id, url: job.url, state: job.state};
}
