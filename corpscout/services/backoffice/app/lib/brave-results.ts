export const BRAVE_RESULTS_PAGE_SIZE = 25;

export function braveTaskResultsPath(taskId: string): string {
  return `/admin/queues/brave/results/${encodeURIComponent(taskId)}`;
}

export function braveResultsPath(basePath: string, page: number, resultId = ""): string {
  const params = new URLSearchParams();
  if (page > 1) params.set("page", String(page));
  if (resultId) params.set("result", resultId);
  return `${basePath}${params.size ? `?${params}` : ""}`;
}
