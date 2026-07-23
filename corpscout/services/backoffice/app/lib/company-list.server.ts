import { parseUnifiedFilters, type CompanyFilters } from "~/lib/filters";
import { searchUnifiedCompanies, type UnifiedSearchResult } from "~/lib/unified.server";

export type CompanyListData = {
  q: string;
  filters: CompanyFilters;
  result: UnifiedSearchResult;
};

export function parseCompanyListRequest(request: Request, lockedCountry?: string) {
  const url = new URL(request.url);
  const filters = parseUnifiedFilters(url.searchParams);
  if (lockedCountry) delete filters.country;

  return {
    q: url.searchParams.get("q") ?? "",
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
    filters,
    queryFilters: lockedCountry ? { ...filters, country: [lockedCountry] } : filters,
  };
}

export async function loadCompanyList(
  request: Request,
  lockedCountry?: string,
): Promise<CompanyListData> {
  const parsed = parseCompanyListRequest(request, lockedCountry);
  const result = await searchUnifiedCompanies({
    q: parsed.q,
    page: parsed.page,
    pageSize: parsed.pageSize,
    sort: parsed.sort,
    dir: parsed.dir,
    filters: parsed.queryFilters,
  });
  return { q: parsed.q, filters: parsed.filters, result };
}
