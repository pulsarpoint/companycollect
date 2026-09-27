/** Saved model choices shared by settings, launch controls, and server validation. */
export type ReasoningEffort = "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";

export const LLM_MODEL_PRESETS = [
  {name: "DeepSeek Flash", provider: "DeepSeek", baseUrl: "https://api.deepseek.com", model: "deepseek-flash"},
  {name: "GLM 5.3 Flash", provider: "OpenRouter", baseUrl: "https://openrouter.ai/api/v1", model: "z-ai/glm-5.3-flash"},
  {name: "Jev 1.13 · decisions", provider: "OpenRouter", baseUrl: "https://openrouter.ai/api/v1", model: "typesafe/jev-1.13"},
] as const;

export function isDecisionModel(model: string): boolean {
  return /^typesafe\/jev-\d/.test(model) || model === "~typesafe/jev-latest";
}

export function reasoningOptions(model: string, baseUrl: string): readonly (ReasoningEffort | "")[] {
  if (isDecisionModel(model)) return [""];
  if (/^deepseek-(?:v4-)?flash/.test(model) && /^https:\/\/api\.deepseek\.com(?:\/|$)/.test(baseUrl))
    return ["", "none", "low", "high", "max"];
  if (model.startsWith("z-ai/glm-5.3-flash")) return ["", "none", "low", "medium", "high"];
  return ["", "none", "minimal", "low", "medium", "high", "xhigh", "max"];
}

export function reasoningLabel(effort: string | null | undefined): string {
  if (!effort) return "Provider default";
  if (effort === "none") return "Off";
  return effort === "xhigh" ? "Extra high" : effort[0].toUpperCase() + effort.slice(1);
}
