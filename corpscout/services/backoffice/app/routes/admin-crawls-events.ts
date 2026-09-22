import type { Route } from "./+types/admin-crawls-events";
import { crawlerFetch } from "~/lib/crawler.server";

export async function loader({request}: Route.LoaderArgs) {
  const query = new URL(request.url).searchParams;
  const after = request.headers.get("Last-Event-ID") || query.get("after") || "0";
  if (!/^\d+$/.test(after)) return new Response("Invalid event cursor", {status: 400});
  try {
    const response = await crawlerFetch(`/v1/crawls/events?after=${after}`, {signal: request.signal});
    const reader = response.body!.getReader();
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const next = await reader.read();
          if (next.done) controller.close();
          else controller.enqueue(next.value);
        } catch {
          // A crawler restart ends this stream; EventSource reconnects with its cursor.
          controller.close();
        }
      },
      async cancel() { await reader.cancel().catch(() => {}); },
    });
    return new Response(body, {headers: {
      "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-store",
      "X-Accel-Buffering": "no",
    }});
  } catch {
    return new Response("Crawler status stream is unavailable", {status: 503});
  }
}
