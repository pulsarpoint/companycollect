import type { Route } from "./+types/admin-crawl-result-json";
import { readCrawlArchive } from "~/lib/crawl-results.server";

export async function loader({request}: Route.LoaderArgs) {
  const row = await readCrawlArchive(new URL(request.url).searchParams.get("path"));
  return new Response(row.result_json, {headers: {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Disposition": 'attachment; filename="crawl-result.json"',
    "Cache-Control": "private, no-store",
    "X-Content-Type-Options": "nosniff",
  }});
}
