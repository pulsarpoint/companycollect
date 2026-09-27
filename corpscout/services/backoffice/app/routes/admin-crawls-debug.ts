import type { Route } from "./+types/admin-crawls-debug";
import { CrawlerApiError, crawlerFetch } from "~/lib/crawler.server";

/** Authenticated admin proxy; crawler credentials never reach the browser. */
export async function loader({request}: Route.LoaderArgs) {
  const search = new URL(request.url).searchParams;
  const id = search.get("request") ?? "";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(id)) return Response.json({error: "Invalid crawl request."}, {status: 400});
  const query = new URLSearchParams();
  for (const key of ["attempt", "after", "event"] as const) {
    const value = search.get(key);
    if (value === null) continue;
    if (!/^\d{1,15}$/.test(value) || key === "event" && Number(value) < 1) return Response.json({error: `Invalid ${key}.`}, {status: 400});
    query.set(key, value);
  }
  if (search.get("download") === "1") query.set("download", "true");
  const streaming = search.get("stream") === "1";
  if (streaming) query.set("stream", "true");
  const lastEvent = request.headers.get("Last-Event-ID");
  if (lastEvent && !/^\d{1,9}:\d{1,15}$/.test(lastEvent)) return Response.json({error: "Invalid event cursor."}, {status: 400});
  try {
    const response = await crawlerFetch(`/v1/crawls/${id}/debug?${query}`, {signal: request.signal,
      ...(lastEvent ? {headers: {"Last-Event-ID": lastEvent}} : {})});
    const headers = new Headers({"Cache-Control": "no-store", "Content-Type": response.headers.get("Content-Type") || "application/json"});
    const disposition = response.headers.get("Content-Disposition");
    if (disposition) headers.set("Content-Disposition", disposition);
    if (streaming) headers.set("X-Accel-Buffering", "no");
    return new Response(response.body, {headers});
  } catch (error) {
    return Response.json({error: error instanceof CrawlerApiError ? error.message : "Could not load crawl debug trace."}, {
      status: error instanceof CrawlerApiError ? error.status : 503, headers: {"Cache-Control": "no-store"},
    });
  }
}
