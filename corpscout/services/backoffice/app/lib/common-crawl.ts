import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";

export interface CommonCrawlFilters {
  domain: string;
  address: string;
  industry: string;
}

export interface CommonCrawlListView {
  page: number;
  pageSize: number;
}

function trimmed(value: string | null): string {
  return value?.trim() ?? "";
}

function normalizeDomainSearch(value: string): string {
  const trimmedValue = value.trim().toLowerCase();
  if (trimmedValue === "") return "";

  try {
    const parsed = new URL(
      trimmedValue.includes("://") ? trimmedValue : `https://${trimmedValue}`,
    );
    return parsed.hostname.replace(/^www\./, "").replace(/\.$/, "");
  } catch {
    return trimmedValue
      .replace(/^[a-z]+:\/\//, "")
      .split("/")[0]
      .replace(/^www\./, "")
      .replace(/\.$/, "");
  }
}

export function parseCommonCrawlFilters(url: URL): CommonCrawlFilters {
  return {
    domain: normalizeDomainSearch(trimmed(url.searchParams.get("domain"))),
    address: trimmed(url.searchParams.get("address")),
    industry: trimmed(url.searchParams.get("industry")),
  };
}

export function parseCommonCrawlListView(url: URL): CommonCrawlListView {
  return {
    page: clampPage(Number.parseInt(url.searchParams.get("page") || "1", 10)),
    pageSize: clampPageSize(
      Number.parseInt(
        url.searchParams.get("pageSize") || String(DEFAULT_PAGE_SIZE),
        10,
      ),
    ),
  };
}

export function hasCommonCrawlFilters(filters: CommonCrawlFilters): boolean {
  return filters.domain !== "" || filters.address !== "" || filters.industry !== "";
}

export function commonCrawlFilterError(
  filters: CommonCrawlFilters,
): string | null {
  if (filters.domain !== "" && filters.domain.length < 2) {
    return "Domain search needs at least 2 characters.";
  }
  if (filters.address !== "" && filters.address.length < 3) {
    return "Address search needs at least 3 characters.";
  }
  if (filters.industry !== "" && filters.industry.length < 2) {
    return "Industry search needs at least 2 characters.";
  }
  return null;
}

export function commonCrawlDomainPath(domain: string): string {
  return `/admin/common-crawl/${encodeURIComponent(domain)}`;
}

export function normalizeCommonCrawlDomain(domain: string): string | null {
  const normalized = normalizeDomainSearch(domain);
  if (
    normalized.length === 0 ||
    normalized.length > 253 ||
    !normalized.includes(".") ||
    !/^[a-z0-9.-]+$/.test(normalized)
  ) {
    return null;
  }
  return normalized;
}
