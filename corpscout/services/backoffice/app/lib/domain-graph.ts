import { normalizeCommonCrawlDomain } from "~/lib/common-crawl";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";

export const GRAPH_DIRECTIONS = [
  "all",
  "mutual",
  "outgoing",
  "incoming",
] as const;
export type GraphDirection = (typeof GRAPH_DIRECTIONS)[number];

export interface DomainGraphSearch {
  domain: string;
  release: string;
  direction: GraphDirection;
  page: number;
  pageSize: number;
}

export function parseDomainGraphSearch(url: URL): DomainGraphSearch {
  const domain = url.searchParams.get("domain")?.trim() ?? "";
  const direction = url.searchParams.get("direction");
  return {
    domain: normalizeCommonCrawlDomain(domain) ?? domain,
    release: url.searchParams.get("release")?.trim() ?? "",
    direction: GRAPH_DIRECTIONS.find((value) => value === direction) ?? "all",
    page: clampPage(Number(url.searchParams.get("page") ?? 1)),
    pageSize: clampPageSize(
      Number(url.searchParams.get("pageSize") ?? DEFAULT_PAGE_SIZE),
    ),
  };
}

export function graphDomainError(domain: string): string | null {
  if (domain === "") return null;
  const valid =
    domain.length <= 253 &&
    domain.includes(".") &&
    !/^\d+(\.\d+){3}$/.test(domain) &&
    domain
      .split(".")
      .every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label));
  return valid
    ? null
    : "Enter a domain such as example.com, or paste a website URL.";
}

export function domainGraphPath(search: Partial<DomainGraphSearch>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(search)) {
    if (value !== "" && value !== undefined) params.set(key, String(value));
  }
  return `/admin/graph${params.size ? `?${params}` : ""}`;
}
