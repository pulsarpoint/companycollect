import type { Route } from "./+types/company-facts-document";
import { getCountry } from "~/lib/countries";
import { getFactsDocument } from "~/lib/queries.server";
import { fetchObject } from "~/lib/object-store.server";

/** Streams the original filing document (inline-XBRL XHTML) from the
 * corpscout object store so the browser can render it directly. */
export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.factsDocumentQuery) throw new Response("Not found", { status: 404 });
  const year = Number(params.year);
  if (!Number.isInteger(year) || year < 1900 || year > 2200) {
    throw new Response("Not found", { status: 404 });
  }
  const doc = await getFactsDocument(country, params.id, year);
  if (!doc) throw new Response("Document not found", { status: 404 });

  // source_uri is s3://<bucket>/<key>; object_key repeats the key part.
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(doc.source_uri);
  if (!match) throw new Response("Unsupported document location", { status: 502 });
  const [, bucket, key] = match;

  const upstream = await fetchObject(bucket, key);
  if (!upstream.ok) {
    throw new Response(`Object store returned ${upstream.status}`, { status: 502 });
  }
  return new Response(upstream.body, {
    headers: {
      "Content-Type": "application/xhtml+xml; charset=utf-8",
      "Content-Disposition": "inline",
      "Cache-Control": "private, max-age=3600",
    },
  });
}
