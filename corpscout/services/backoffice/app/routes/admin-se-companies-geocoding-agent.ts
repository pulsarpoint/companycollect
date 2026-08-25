/**
 * The Geocoding tab's poll endpoint: one JSON payload describing the analysis
 * agent's state for a country.
 *
 * A resource route (loader only, no component), the same shape
 * admin-se-people-draft-job.ts uses for the Draft 1 job: the page's action
 * starts a run and returns immediately, and the panel loads this while the run
 * is live. `no-store` because the whole point is freshness.
 */
import { loadGeocodeAgentPanel } from "~/agents/geocode-analysis.server";
import type { Route } from "./+types/admin-se-companies-geocoding-agent";

export async function loader({ request }: Route.LoaderArgs) {
  // Country is a parameter everywhere in the agent; this endpoint sits under
  // the Swedish tab, so SE is only its default.
  const country = new URL(request.url).searchParams.get("country")?.trim() || "SE";
  return Response.json(
    { panel: await loadGeocodeAgentPanel(country) },
    { headers: { "Cache-Control": "no-store" } },
  );
}
