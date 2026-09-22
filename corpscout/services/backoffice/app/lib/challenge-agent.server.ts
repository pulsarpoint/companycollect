import { browserFetch } from "~/lib/browser-service.server";
import { crawlerFetch } from "~/lib/crawler.server";
import { isCrawlWaiting, type ChallengeAgentResult, type CrawlAttempt } from "~/lib/crawler";

export async function runChallengeAgent(requestId: string): Promise<ChallengeAgentResult> {
  const job = await (await crawlerFetch(`/v1/crawls/${encodeURIComponent(requestId)}`)).json() as CrawlAttempt & {browser_lease_id: string | null};
  if (job.challenge_agent_running) {
    throw new Error("The automatic CAPTCHA agent is already running for this crawl.");
  }
  if (!isCrawlWaiting(job) || !job.browser_available || !job.browser_lease_id) {
    throw new Error("Open an interactive retry or search verification before using the agent.");
  }
  const tab = job.blocked_reason?.startsWith("brave_") ? "search" : "site";
  const observed = await (await browserFetch("/v1/browser/extract", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({session: {id: job.browser_lease_id}, tab, browserHtml: false}),
  })).json() as {url: string; session: {generation: string}};
  const response = await browserFetch(`/v1/browser/sessions/${encodeURIComponent(job.browser_lease_id)}/tabs/${tab}/challenge-agent`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({confirm: true, expectedUrl: observed.url, expectedGeneration: observed.session.generation, ...(job.challenge_agent_model ? {model: job.challenge_agent_model} : {})}),
    signal: AbortSignal.timeout(190_000),
  });
  return response.json();
}
