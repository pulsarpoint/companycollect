import {data} from "react-router";
import type {Route} from "./+types/admin-crawl-queue-submission";
import {crawlQueueSubmission} from "~/lib/crawl-queue.server";
export async function loader({params}: Route.LoaderArgs) {
  try { return data(await crawlQueueSubmission(params.runId), {headers: {"Cache-Control": "no-store"}}); }
  catch { return data({ok: false as const, error: "Import status is unavailable. Check Dagster or retry the status check."}, {status: 503, headers: {"Cache-Control": "no-store"}}); }
}
