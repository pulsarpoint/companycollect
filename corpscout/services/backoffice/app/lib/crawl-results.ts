export function resultObject(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
}

export function resultObjects(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter(item => item !== null && typeof item === "object" && !Array.isArray(item)) : [];
}

export function resultText(value: unknown, fallback = "—"): string {
  return typeof value === "string" || typeof value === "number" ? String(value) : fallback;
}

export function resultWebUrl(value: unknown): string | undefined {
  if (typeof value !== "string") return;
  try {
    const url = new URL(value);
    if (["http:", "https:"].includes(url.protocol) && !url.username && !url.password) return url.href;
  } catch { /* Unusable source URLs remain plain text. */ }
}
