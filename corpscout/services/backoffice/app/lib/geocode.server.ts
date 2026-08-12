import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";

export interface GeoPoint {
  lat: number;
  lon: number;
}

export interface GeocodeMatch {
  coords: GeoPoint;
  precision: "address" | "street";
}

interface GeocodeOptions {
  fetcher?: typeof fetch;
  minIntervalMs?: number;
  dbPath?: string;
  countryCode?: string;
}

const DEFAULT_DB_PATH = join(process.cwd(), ".cache", "geocode.sqlite");
const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";
const USER_AGENT = "corpscout-backoffice/1.0 (goran.raovic@gmail.com)";
const DEFAULT_MIN_INTERVAL_MS = 1100;

const databases = new Map<string, DatabaseSync>();

function getDb(dbPath: string): DatabaseSync {
  let db = databases.get(dbPath);
  if (!db) {
    mkdirSync(dirname(dbPath), { recursive: true });
    db = new DatabaseSync(dbPath);
    db.exec(`CREATE TABLE IF NOT EXISTS geocode_cache (
      address TEXT PRIMARY KEY,
      lat REAL,
      lon REAL,
      resolved INTEGER NOT NULL,
      fetched_at TEXT NOT NULL
    )`);
    databases.set(dbPath, db);
  }
  return db;
}

function normalizeAddress(address: string): string {
  return address.trim().replace(/\s+/g, " ").toLowerCase();
}

function cacheKey(address: string, countryCode: string | undefined): string {
  return `${countryCode ?? ""}|${normalizeAddress(address)}`;
}

function structuredCacheKey(
  street: string,
  postalCode: string,
  countryCode: string | undefined,
): string {
  return `structured|${countryCode ?? ""}|${normalizeAddress(street)}|${normalizeAddress(postalCode)}`;
}

// Global 1 req/s politeness throttle (Nominatim usage policy).
let lastRequestAt = 0;
let queue: Promise<unknown> = Promise.resolve();

export function clearGeocodeThrottleForTests(): void {
  lastRequestAt = 0;
  queue = Promise.resolve();
}

async function throttled<T>(
  minIntervalMs: number,
  fn: () => Promise<T>,
): Promise<T> {
  const run = queue.then(async () => {
    const wait = lastRequestAt + minIntervalMs - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastRequestAt = Date.now();
    return fn();
  });
  queue = run.catch(() => {});
  return run;
}

async function resolveNominatimQuery(
  key: string,
  requestUrl: string,
  opts?: GeocodeOptions,
): Promise<GeoPoint | null> {
  const db = getDb(opts?.dbPath ?? DEFAULT_DB_PATH);

  const cached = db
    .prepare("SELECT lat, lon, resolved FROM geocode_cache WHERE address = ?")
    .get(key) as
    { lat: number | null; lon: number | null; resolved: number } | undefined;
  if (cached) {
    return cached.resolved === 1
      ? { lat: cached.lat!, lon: cached.lon! }
      : null;
  }

  const fetcher = opts?.fetcher ?? fetch;
  const minIntervalMs = opts?.minIntervalMs ?? DEFAULT_MIN_INTERVAL_MS;
  let results: Array<{ lat: string; lon: string }>;
  try {
    const response = await throttled(minIntervalMs, () =>
      fetcher(requestUrl, {
        headers: { "User-Agent": USER_AGENT },
        signal: AbortSignal.timeout(10_000),
      }),
    );
    if (!response.ok) return null; // transient upstream failure: no cache entry
    results = (await response.json()) as Array<{ lat: string; lon: string }>;
  } catch {
    return null; // network failure: no cache entry, retry next time
  }
  if (!Array.isArray(results)) return null; // malformed upstream payload: no cache entry

  const hit = results[0];
  const point =
    hit &&
    (typeof hit.lat === "string" || typeof hit.lat === "number") &&
    hit.lat !== null &&
    (typeof hit.lon === "string" || typeof hit.lon === "number") &&
    hit.lon !== null &&
    Number.isFinite(Number(hit.lat)) &&
    Number.isFinite(Number(hit.lon))
      ? { lat: Number(hit.lat), lon: Number(hit.lon) }
      : null;

  db.prepare(
    "INSERT OR REPLACE INTO geocode_cache (address, lat, lon, resolved, fetched_at) VALUES (?, ?, ?, ?, ?)",
  ).run(
    key,
    point?.lat ?? null,
    point?.lon ?? null,
    point ? 1 : 0,
    new Date().toISOString(),
  );

  return point;
}

export async function geocodeAddress(
  address: string,
  opts?: GeocodeOptions,
): Promise<GeoPoint | null> {
  if (normalizeAddress(address) === "") return null;
  const searchParams = new URLSearchParams({
    format: "jsonv2",
    limit: "1",
    q: address,
  });
  if (opts?.countryCode) searchParams.set("countrycodes", opts.countryCode);
  return resolveNominatimQuery(
    cacheKey(address, opts?.countryCode),
    `${NOMINATIM_URL}?${searchParams.toString()}`,
    opts,
  );
}

async function geocodeStructuredStreet(
  street: string,
  postalCode: string,
  opts?: GeocodeOptions,
): Promise<GeoPoint | null> {
  if (normalizeAddress(street) === "") return null;
  const searchParams = new URLSearchParams({
    format: "jsonv2",
    limit: "1",
    street,
  });
  if (normalizeAddress(postalCode) !== "")
    searchParams.set("postalcode", postalCode);
  if (opts?.countryCode) searchParams.set("countrycodes", opts.countryCode);
  return resolveNominatimQuery(
    structuredCacheKey(street, postalCode, opts?.countryCode),
    `${NOMINATIM_URL}?${searchParams.toString()}`,
    opts,
  );
}

export async function geocodeAddressWithStreetFallback(
  address: string,
  fallback: { street: string; postalCode: string } | null,
  opts?: GeocodeOptions,
): Promise<GeocodeMatch | null> {
  const exact = await geocodeAddress(address, opts);
  if (exact) return { coords: exact, precision: "address" };
  if (!fallback) return null;
  const street = await geocodeStructuredStreet(
    fallback.street,
    fallback.postalCode,
    opts,
  );
  return street ? { coords: street, precision: "street" } : null;
}
