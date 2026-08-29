import type { Route } from "./+types/tech-icon";
import { fetchObject } from "~/lib/object-store.server";
import { loadTechnologyIconRef } from "~/lib/technology-catalog.server";

/**
 * Streams a technology icon from the `technology-icons` bucket. The browser
 * only ever sees /icons/tech/:slug — the object-store endpoint and credentials
 * stay server-side. Icons are a few KB and effectively immutable (the ETag is
 * derived from the object key + catalog updated_at, so a re-imported icon gets
 * a new ETag), hence the long public cache and the small in-memory LRU.
 */

const ICON_BUCKET = "technology-icons";
const ICON_CACHE_LIMIT = 500;

interface CachedIcon {
  etag: string;
  contentType: string;
  body: Uint8Array<ArrayBuffer>;
}

/** slug -> icon. Map insertion order doubles as LRU recency. */
const iconCache = new Map<string, CachedIcon>();

function rememberIcon(slug: string, icon: CachedIcon): void {
  iconCache.delete(slug);
  iconCache.set(slug, icon);
  while (iconCache.size > ICON_CACHE_LIMIT) {
    const oldest = iconCache.keys().next().value;
    if (oldest === undefined) break;
    iconCache.delete(oldest);
  }
}

function iconHeaders(etag: string, contentType: string): HeadersInit {
  return {
    "Content-Type": contentType,
    "Cache-Control": "public, max-age=86400",
    ETag: etag,
  };
}

export async function loader({ params, request }: Route.LoaderArgs) {
  const ref = await loadTechnologyIconRef(params.slug);
  if (!ref) throw new Response("Icon not found", { status: 404 });

  const etag = `"${ref.objectKey}@${ref.updatedAt}"`;
  const contentType = ref.contentType || "image/svg+xml";

  if (request.headers.get("If-None-Match") === etag) {
    return new Response(null, {
      status: 304,
      headers: iconHeaders(etag, contentType),
    });
  }

  const cached = iconCache.get(params.slug);
  if (cached && cached.etag === etag) {
    rememberIcon(params.slug, cached); // refresh recency
    return new Response(cached.body, {
      headers: iconHeaders(etag, cached.contentType),
    });
  }

  const upstream = await fetchObject(ICON_BUCKET, ref.objectKey);
  if (!upstream.ok) {
    throw new Response(`Object store returned ${upstream.status}`, {
      status: 502,
    });
  }
  const body = new Uint8Array(await upstream.arrayBuffer());
  rememberIcon(params.slug, { etag, contentType, body });
  return new Response(body, { headers: iconHeaders(etag, contentType) });
}
