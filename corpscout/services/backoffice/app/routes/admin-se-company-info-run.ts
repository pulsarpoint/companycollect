import type { Route } from "./+types/admin-se-company-info-run";
import { runStatus } from "~/lib/dagster.server";

/** Terminal Dagster run states: the poller stops and the page reloads on these. */
const FINISHED = new Set(["SUCCESS", "FAILURE", "CANCELED"]);

/**
 * Resource route (no component): the Info tab's Fold now poller reads one run's
 * status here every few seconds until it finishes, then revalidates the page so
 * the freshly folded row appears. Only `loader` lives here.
 */
export async function loader({ params }: Route.LoaderArgs) {
  try {
    const run = await runStatus(params.runId);
    return { runId: run.runId, status: run.status, finished: FINISHED.has(run.status) };
  } catch {
    // A transient Dagster GraphQL failure should keep the poller showing "Folding"
    // rather than routing to the root error boundary.
    return { runId: params.runId, status: "UNKNOWN", finished: false };
  }
}
