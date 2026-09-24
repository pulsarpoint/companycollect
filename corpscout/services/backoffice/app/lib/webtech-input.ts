import { parseWebtechSearch } from "~/lib/webtech";

export function parseWebtechInputFilters(params: URLSearchParams) {
  const { prefix, page } = parseWebtechSearch(params);
  return {
    prefix,
    page,
    task: (params.get("task") ?? "").trim(),
    source: (params.get("source") ?? "").trim(),
  };
}

export type WebtechInputFilters = ReturnType<typeof parseWebtechInputFilters>;

export function webtechInputPath(filters: WebtechInputFilters, page = 1) {
  const params = new URLSearchParams();
  if (filters.prefix) params.set("prefix", filters.prefix);
  if (filters.task) params.set("task", filters.task);
  if (filters.source) params.set("source", filters.source);
  if (page > 1) params.set("page", String(page));
  return `/admin/webtech/input${params.size ? `?${params}` : ""}`;
}
