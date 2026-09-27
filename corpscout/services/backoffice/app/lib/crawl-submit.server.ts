import { createHash } from "node:crypto";
import { CrawlerApiError, crawlerFetch } from "~/lib/crawler.server";
import { verifySelectedLlm } from "~/lib/crawl-llm.server";
import { parseTestCrawl } from "~/lib/crawl-test";
import type { CrawlPublishReceipt } from "~/lib/crawler";

/** Submit one operator test crawl straight to the crawler's REST queue. */
export async function publishTestCrawl(form: FormData): Promise<CrawlPublishReceipt> {
  if (process.env.CRAWLER_TEST_SUBMIT_ENABLED !== "true") {
    throw new Error("Test submissions are disabled. Set CRAWLER_TEST_SUBMIT_ENABLED=true on Backoffice.");
  }
  if ([...form.values()].some(value => typeof value !== "string") || Buffer.byteLength(JSON.stringify([...form])) > 131_072) throw new Error("The request must be 128 KiB or smaller.");
  const submissionId = String(form.get("submission_id") ?? "");
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(submissionId)) throw new Error("Invalid submission ID. Reload the page before crawling.");
  const {profileId, decisionProfileId, payload} = parseTestCrawl(form);
  // Bind an operator submission to its public settings. Read an accepted request
  // before re-encrypting credentials so retries work after lost acknowledgements.
  const fingerprint = createHash("sha256").update(JSON.stringify({profileId, decisionProfileId, payload})).digest("hex").slice(0, 24);
  const requestId = `backoffice-${submissionId}-${fingerprint}`;
  const statusPath = `/v1/crawls/${requestId}/status`;
  try { return receipt(await (await crawlerFetch(statusPath)).json()); }
  catch (error) { if (!(error instanceof CrawlerApiError) || error.status !== 404) throw error; }
  const llm = await verifySelectedLlm(profileId, "crawler");
  const decisionLlm = decisionProfileId ? await verifySelectedLlm(decisionProfileId, "crawler", false, "decision") : null;
  try {
    const response = await crawlerFetch("/v1/crawls", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({...payload, request_id: requestId, llm,
        ...(decisionLlm ? {decision_llm: decisionLlm} : {}),
        api: llm.provider === "deepseek" || new URL(llm.base_url).hostname === "api.deepseek.com" ? "deepseek" : "openrouter",
        config: {...payload.config, provider: null}}),
    });
    return receipt(await response.json());
  } catch (error) {
    // Concurrent retries can generate different ciphertext for the same settings.
    if (error instanceof CrawlerApiError && error.status === 409) return receipt(await (await crawlerFetch(statusPath)).json());
    throw error;
  }
}

function receipt(job: CrawlPublishReceipt): CrawlPublishReceipt {
  return {request_id: job.request_id, url: job.url, state: job.state};
}
