/** DNS suffix, with an optional leading dot; supports multi-label suffixes. */
export const DOMAIN_SUFFIX_PATTERN = String.raw`\.?[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*`;
const SUFFIX = new RegExp(`^(?:${DOMAIN_SUFFIX_PATTERN})$`);

export function normalizeDomainSuffix(value: string | null): string {
  const suffix = (value ?? "").trim().toLowerCase();
  return suffix.length <= 253 && SUFFIX.test(suffix) ? suffix.replace(/^\./, "") : "";
}
