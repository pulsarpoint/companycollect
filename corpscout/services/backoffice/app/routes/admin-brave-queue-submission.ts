import { data } from "react-router";
import { braveQueueSubmission } from "~/lib/se-company-brave.server";
export async function loader({params}: {params: {runId?: string}}) {
  try { return data(await braveQueueSubmission(params.runId ?? ""), {headers: {"Cache-Control": "no-store"}}); }
  catch { return data({ok: false as const, error: "Import status is unavailable. Check the Dagster run or retry."}, {status: 503, headers: {"Cache-Control": "no-store"}}); }
}
