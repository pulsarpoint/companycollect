import { data } from "react-router";
import { parseQueueFilters } from "~/lib/queues";
import { loadBraveSourcePage } from "~/lib/brave-queue-history.server";
export async function loader({request}: {request: Request}) {
  const filters = parseQueueFilters("brave", new URL(request.url).searchParams);
  if (!filters.task) throw new Response("Select a Brave task", {status: 400});
  try { return {rows: await loadBraveSourcePage(filters.task, filters.page), page: filters.page, error: null}; }
  catch { return data({rows: [], page: filters.page, error: "Source companies are unavailable. Please retry."}, {status: 503}); }
}
