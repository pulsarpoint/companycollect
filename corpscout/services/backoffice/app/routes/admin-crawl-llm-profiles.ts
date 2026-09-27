import type { Route } from "./+types/admin-crawl-llm-profiles";
import { isDecisionModel } from "~/lib/llm-model-options";
import { listLlmProfiles } from "~/lib/llm-settings.server";

export async function loader({request}: Route.LoaderArgs) {
  const decisions = new URL(request.url).searchParams.get("role") === "decision";
  try {
    return {
      profiles: (await listLlmProfiles(false, decisions)).filter(profile => isDecisionModel(profile.model) === decisions).map(({profileId, name, provider, model, reasoningEffort, apiKeyAvailable}) => ({profileId, name, provider, model, reasoningEffort, apiKeyAvailable})),
      error: null,
    };
  } catch {
    return {profiles: [], error: "Could not load saved LLMs. Reload the list to try again."};
  }
}
