import { listLlmProfiles } from "~/lib/llm-settings.server";

export async function loader() {
  try {
    return {
      profiles: (await listLlmProfiles()).map(({profileId, name, provider, model, apiKeyAvailable}) => ({profileId, name, provider, model, apiKeyAvailable})),
      error: null,
    };
  } catch {
    return {profiles: [], error: "Could not load saved LLMs. Reload the list to try again."};
  }
}
