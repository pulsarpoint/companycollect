export interface WorkspaceIpFilters {
  search: string;
  version: "any" | "4" | "6";
}

export function parseWorkspaceIpFilters(
  params: URLSearchParams,
): WorkspaceIpFilters {
  const version = params.get("version");
  return {
    search: (params.get("search") ?? "").trim().toLowerCase(),
    version: version === "4" || version === "6" ? version : "any",
  };
}

export function workspaceIpAddressesHref(
  filters: WorkspaceIpFilters,
  after = "",
) {
  const params = new URLSearchParams();
  if (filters.search) params.set("search", filters.search);
  if (filters.version !== "any") params.set("version", filters.version);
  if (after) params.set("after", after);
  return `/admin/ip-addresses${params.size ? `?${params}` : ""}`;
}
