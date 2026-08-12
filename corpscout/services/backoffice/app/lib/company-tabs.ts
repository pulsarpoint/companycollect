export type CompanyTab =
  | "overview"
  | "financials"
  | "suggestions"
  | "technology";
export type TechnologySection =
  "overview" | "web-intelligence" | "infrastructure" | "ip-addresses";

export function companyTabFromPath(pathname: string): CompanyTab {
  if (pathname.includes("/technology")) return "technology";
  if (pathname.includes("/financials")) return "financials";
  if (pathname.includes("/suggestions")) return "suggestions";
  return "overview";
}

export function technologySectionFromPath(pathname: string): TechnologySection {
  if (pathname.includes("/technology/ip-addresses")) return "ip-addresses";
  if (pathname.includes("/technology/web-intelligence")) {
    return "web-intelligence";
  }
  return pathname.includes("/technology/infrastructure")
    ? "infrastructure"
    : "overview";
}

export function technologyTabSupported(countryCode: string): boolean {
  return countryCode.toLowerCase() === "se";
}

export function technologyTabAvailable(
  countryCode: string,
  hasDomain: boolean,
): boolean {
  return technologyTabSupported(countryCode) && hasDomain;
}

export function domainSuggestionsTabSupported(countryCode: string): boolean {
  return countryCode.toLowerCase() === "se";
}

export function domainSuggestionsTabAvailable(
  countryCode: string,
  hasSuggestions: boolean,
): boolean {
  return domainSuggestionsTabSupported(countryCode) && hasSuggestions;
}
