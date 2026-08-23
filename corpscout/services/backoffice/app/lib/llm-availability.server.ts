import { listLlmProfiles } from "~/lib/llm-settings.server";

export interface LlmRequestAvailability {
  ready: boolean;
  warning: string | null;
  profile: {
    name: string;
    provider: string;
    model: string;
    apiKeyEnvironmentVariable: string;
  } | null;
}

export function getLlmRequestAvailability(): LlmRequestAvailability {
  const profile = listLlmProfiles().find((candidate) => candidate.isActive);
  if (!profile) {
    return {
      ready: false,
      warning:
        "No active LLM is configured. Configure and activate an LLM before sending this request.",
      profile: null,
    };
  }

  const visibleProfile = {
    name: profile.name,
    provider: profile.provider,
    model: profile.model,
    apiKeyEnvironmentVariable: profile.apiKeyEnvironmentVariable,
  };
  if (!profile.apiKeyAvailable) {
    return {
      ready: false,
      warning: `The active LLM “${profile.name}” is configured, but ${profile.apiKeyEnvironmentVariable} is not available in the process environment.`,
      profile: visibleProfile,
    };
  }
  return { ready: true, warning: null, profile: visibleProfile };
}
