import { data } from "react-router";
import type { Route } from "./+types/admin-ip-enrichment-queue-submission";
import {
  IpEnrichmentSelectionError,
  ipEnrichmentQueueSubmission,
} from "~/lib/ip-enrichment.server";

export async function loader({ params }: Route.LoaderArgs) {
  try {
    return data(await ipEnrichmentQueueSubmission(params.runId), {
      headers: { "Cache-Control": "no-store" },
    });
  } catch (error) {
    return data(
      {
        ok: false as const,
        error:
          error instanceof IpEnrichmentSelectionError
            ? error.message
            : "Import status is unavailable. Check the Dagster run or retry the status check.",
      },
      {
        status: error instanceof IpEnrichmentSelectionError ? 400 : 503,
        headers: { "Cache-Control": "no-store" },
      },
    );
  }
}
