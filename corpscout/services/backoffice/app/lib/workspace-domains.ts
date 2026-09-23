export interface WorkspaceDomainFilters {
  prefix: string;
  companies: "any" | "with" | "without";
  webtech: "any" | "with" | "without";
}

export function parseWorkspaceDomainFilters(
  params: URLSearchParams,
): WorkspaceDomainFilters {
  const presence = (value: string | null) =>
    value === "with" || value === "without" ? value : "any";
  return {
    prefix: (params.get("prefix") ?? "").trim().toLowerCase(),
    companies: presence(params.get("companies")),
    webtech: presence(params.get("webtech")),
  };
}

export function workspaceDomainsHref(
  filters: WorkspaceDomainFilters,
  after = "",
) {
  const params = new URLSearchParams();
  if (filters.prefix) params.set("prefix", filters.prefix);
  if (filters.companies !== "any") params.set("companies", filters.companies);
  if (filters.webtech !== "any") params.set("webtech", filters.webtech);
  if (after) params.set("after", after);
  return `/admin/domains${params.size ? `?${params}` : ""}`;
}

export function workspaceDomainHref(domain: string) {
  return `/admin/domains/${encodeURIComponent(domain)}`;
}

export function workspaceWebtechHref(domain: string, site?: string) {
  return `${workspaceDomainHref(domain)}/web-technologies${site ? `?site=${encodeURIComponent(site)}` : ""}`;
}
