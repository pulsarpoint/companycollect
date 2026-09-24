import { data } from "react-router";
import type { Route } from "./+types/admin-webtech-queue-submission";
import { WebtechQueueRequestError, webtechQueueSubmission } from "~/lib/webtech-queue.server";
export async function loader({params}: Route.LoaderArgs) {
  try { return data(await webtechQueueSubmission(params.runId), {headers: {"Cache-Control": "no-store"}}); }
  catch (error) {
    return data({ok: false as const, error: error instanceof WebtechQueueRequestError ? error.message : "Import status is unavailable. Check the Dagster run or retry the status check."}, {status: error instanceof WebtechQueueRequestError ? 400 : 503, headers: {"Cache-Control": "no-store"}});
  }
}
