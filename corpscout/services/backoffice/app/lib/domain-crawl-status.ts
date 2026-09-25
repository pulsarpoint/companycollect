import type { DomainCrawlResult } from "~/lib/domain-crawls.server";
import { resultObject, resultObjects, resultText } from "~/lib/crawl-results";

/** Processing completion is not a successful crawl. Keep partial and classification outcomes explicit. */
export function domainCrawlStatus(result: DomainCrawlResult) {
  if (result.state === "cancelled") return "Cancelled";
  if (result.state === "failed" || result.crawl_status === "failed") return "Failed";
  if (result.crawl_status === "partial") return "Partial";
  if (!result.successful) return "Failed";
  if (result.crawl_status === "skip_crawling") return "Skipped by classification";
  return result.crawl_status === "finished" ? "Crawled" : result.crawl_status.replaceAll("_", " ");
}

export function crawlFailureReason(payload: Record<string, unknown>) {
  const crawl = resultObject(payload.crawl);
  const usage = resultObject(crawl.usage ?? payload.model_usage ?? payload.usage);
  const reasons = [
    ...resultObjects(usage.by_call).flatMap(call => [resultText(resultObject(call.provider_error).message, ""), resultText(call.error, "")]),
    ...resultObjects(crawl.errors).map(error => resultText(error.error ?? error.message, "")),
    resultText(crawl.stop_reason, "").replaceAll("_", " "),
  ].filter(Boolean);
  return [...new Set(reasons)].join(" · ");
}
