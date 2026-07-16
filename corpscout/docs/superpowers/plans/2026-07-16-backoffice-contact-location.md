# Backoffice Contact & Location Card Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A compact "Contact & location" card on the company detail page: contact rows with icons, the company's registered address(es), and a small map — coordinates from LV's stored lat/long where present, otherwise geocoded from the address via Nominatim with a persistent local cache.

**Architecture:** Registry gains `detail.addressQuery` per country (canonical rows `{address_type, full_address}`): Norway reads the NEW `no_company_addresses` table (schema live, **0 rows today — dagster asset pending; the card simply shows addresses the moment data lands**), Sweden reads `se_company_addresses` (4.3M rows), and seven countries compose the address from their companies-row columns in SQL. Geocoding is server-side (`geocode.server.ts`): normalized-address key → `node:sqlite` persistent cache (positive AND negative entries) → throttled Nominatim lookup (~1 req/s, proper User-Agent), exposed via a `/:country/geocode` resource route the card calls with a fetcher; Latvia short-circuits using `address_latitude/longitude` already on its record. The map is leaflet via react-leaflet, client-only (lazy import behind a mounted gate — leaflet touches `window`), OSM tiles with attribution, a `CircleMarker` pin (no icon-asset bundling issues).

**Tech Stack:** existing stack + `leaflet`/`react-leaflet` (+`@types/leaflet` dev) + Node's built-in `node:sqlite` (NO new backend dependency — the durable cache case genuinely warrants sqlite, unlike the facet cache).

## Global Constraints

- App: `corpscout/services/backoffice`. RR8 SSR, Base UI shadcn, `chQuery`, `{id:String}` binding, registry-only SQL, read-only ClickHouse.
- **Nominatim etiquette (binding):** max 1 request/second (enforce ≥1100ms between calls via a module-level throttle), `User-Agent: corpscout-backoffice/1.0 (goran.raovic@gmail.com)`, `format=jsonv2&limit=1`, results cached FOREVER including misses (negative cache) — the same address must never be re-queried.
- Geocode cache: `node:sqlite` `DatabaseSync` at `<app>/.cache/geocode.sqlite` (`.cache/` gitignored). Cache key = trimmed, whitespace-collapsed, lowercased address. If `node:sqlite` is unavailable in the runtime Node, STOP and report (NEEDS_CONTEXT) — do not silently add a dependency.
- Map renders client-only (leaflet requires `window`): lazy import + mounted gate; SSR HTML contains the card's contacts/address, never the map itself. OSM tile layer MUST carry the attribution string `&copy; OpenStreetMap contributors`.
- LV uses stored coordinates from the record (`address_latitude`/`address_longitude`, 213,134 companies) — no geocode call. All other countries geocode `addresses[0].full_address + ", " + country.name` on demand.
- Address strings are built with `arrayStringConcat(arrayFilter(x -> x != '', [...]), ', ')` so missing parts never produce dangling separators.
- Fidelity note: the card is additive; the record card keeps showing raw address columns. The card REPLACES the old `ContactsSection` (same data, better presentation) — delete the old section.
- Integration tests: real ClickHouse; geocoder tests use an INJECTED fake fetcher (never hit Nominatim in tests); ids picked dynamically.
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; SHARED git tree — stage only named backoffice paths. Dev server on 5183 is USER-OWNED: never kill it; verify via HMR-reloaded curls against it (or your own server on another port).

## Ground truth (verified live, 2026-07-16)

- `no_company_addresses` (migration 000133, NEW): `registry_id, address_type, address_lines, postal_code, city, municipality, country, is_current, ...` — **0 rows** (dagster `norway_brreg` asset exists; materialization pending). Build against the schema; the card renders addresses automatically once loaded.
- `se_company_addresses`: 4,341,192 rows — `company_id, address_type, source, raw_address, street_address, care_of, postal_code, post_town, country_code`.
- `fi_addresses`: 0 rows and an old schema — FI gets NO addressQuery this pass (log).
- Embedded address columns: ee(`address, postal_code, location`), lv(`address, postal_code` + `address_latitude/longitude` 213k/485k), gb(`address, address_line_2, city, county, postal_code, country`), fr(`address, address_supplement, postal_code, city`), br(`street_type, street_name, street_number, address_complement, district, postal_code, municipality_name, state`), cz(`address, postal_code, city`), sk(`address, postal_code, city`).
- Denmark CVR is being ingested in dagster (recent commits) but has NO ClickHouse tables yet — out of scope.

---

### Task 1: Registry `addressQuery` + addresses in `getCompanyDetail`

**Files:**
- Modify: `app/lib/countries.ts`
- Modify: `app/lib/countries.test.ts`
- Modify: `app/lib/queries.server.ts`
- Modify: `tests/queries.server.test.ts`

**Interfaces:**
- Produces:
  - `CountryDetailConfig.addressQuery?: string` — `{id:String}` → rows `address_type` (String), `full_address` (String, never dangling separators). Declared for no/se/ee/lv/gb/fr/br/cz/sk; NOT fi.
  - `interface AddressRow { address_type: string; full_address: string }`; `CompanyDetail.addresses: AddressRow[]` (`[]` when no query), fetched in the same guarded parallel batch.

- [ ] **Step 1: Failing tests**

`app/lib/countries.test.ts` (inside `describe("detail config")`):

```ts
it("every country except fi declares addressQuery with canonical aliases", () => {
  for (const c of COUNTRIES) {
    if (c.code === "fi") {
      expect(c.detail?.addressQuery).toBeUndefined();
      continue;
    }
    expect(c.detail?.addressQuery, c.code).toContain("AS address_type");
    expect(c.detail?.addressQuery, c.code).toContain("AS full_address");
    expect(c.detail?.addressQuery, c.code).toContain("{id:String}");
  }
});
```

`tests/queries.server.test.ts`:

```ts
describe("addresses", () => {
  it("estonia composes a full address from embedded columns", async () => {
    const [row] = await chQuery<{ id: string }>(
      `SELECT reg_code AS id FROM ee_companies
       WHERE address != '' AND postal_code != ''
       ORDER BY reg_code LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.addresses.length).toBeGreaterThan(0);
    expect(detail!.addresses[0].full_address).toContain(",");
    expect(detail!.addresses[0].full_address).not.toMatch(/, ,|^,|,$/);
  });

  it("sweden reads its addresses table", async () => {
    const se = getCountry("se")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT registration_number AS id FROM se_companies
       WHERE company_id IN (SELECT company_id FROM se_company_addresses WHERE street_address != '')
       ORDER BY registration_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(se, row.id);
    expect(detail!.addresses.length).toBeGreaterThan(0);
    expect(detail!.addresses[0].full_address).toBeTruthy();
  });

  it("finland returns an empty addresses array", async () => {
    const fi = getCountry("fi")!;
    const page = await searchCompanies(fi, { pageSize: 1 });
    const detail = await getCompanyDetail(fi, String(page.rows[0].id));
    expect(detail!.addresses).toEqual([]);
  });
});

// inside the existing all-countries describe:
it.each(
  COUNTRIES.filter((c) => c.detail?.addressQuery).map((c) => [c.code, c] as const),
)(
  "%s: addressQuery SQL is valid against live schema",
  async (_code, country) => {
    const rows = await chQuery(country.detail!.addressQuery!, { id: "0" });
    expect(Array.isArray(rows)).toBe(true);
  },
  60_000,
);
```

Run — FAIL.

- [ ] **Step 2: Registry SQL**

Add to `CountryDetailConfig`:

```ts
  /** {id:String} → address rows: address_type, full_address (clean comma-joined). */
  addressQuery?: string;
```

Norway (the NEW table; empty today, renders when dagster lands):

```ts
addressQuery: `SELECT address_type AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address_lines, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, ''))),
    coalesce(country, '')
  ]), ', ') AS full_address
FROM no_company_addresses
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY address_type
LIMIT 10`,
```

Sweden:

```ts
addressQuery: `SELECT address_type AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(care_of, ''),
    if(coalesce(street_address, '') != '', street_address, coalesce(raw_address, '')),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(post_town, '')))
  ]), ', ') AS full_address
FROM se_company_addresses
WHERE company_id IN (SELECT company_id FROM se_companies WHERE registration_number = {id:String})
ORDER BY address_type
LIMIT 10`,
```

Embedded-column countries — one row, `'registered' AS address_type` (Estonia shown; per-country part lists below):

```ts
addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(location, '')))
  ]), ', ') AS full_address
FROM ee_companies
WHERE reg_code = {id:String}
LIMIT 1`,
```

Part lists (same skeleton, substitute table/key/parts — **wrap EVERY column ref in `coalesce(col, '')`** exactly as the EE example does; `arrayStringConcat` errors on a NULL element, and coalesce is a no-op on non-null Strings, so wrapping everything is strictly safe):
- lv (`lv_companies`/`regcode`): `[address, postal_code]`
- gb (`gb_companies`/`company_number`): `[address, address_line_2, trim(concat(postal_code, ' ', city)), county, country]`
- fr (`fr_companies`/`siren`): `[address, address_supplement, trim(concat(postal_code, ' ', city))]`
- cz (`cz_companies`/`ico`): `[address, trim(concat(postal_code, ' ', city))]`
- sk (`sk_companies`/`ico`): `[address, trim(concat(postal_code, ' ', city))]`
- br (`br_companies`/`cnpj_basico`, `'headquarters' AS address_type`): `[trim(concat(street_type, ' ', street_name, ' ', street_number)), address_complement, district, trim(concat(postal_code, ' ', municipality_name)), state]`

Finland: nothing (fi_addresses is empty; log).

- [ ] **Step 3: Query layer**

`app/lib/queries.server.ts`:

```ts
export interface AddressRow {
  address_type: string;
  full_address: string;
}
```

`CompanyDetail` gains `addresses: AddressRow[];`; add `addressesPromise` following the sibling pattern (constructed with the batch, `.catch(() => {})` at construction, `Promise.resolve([])` when no query, folded into the final `Promise.all`, returned).

- [ ] **Step 4: Verify + commit**

`pnpm test countries && pnpm test queries` → PASS; full `pnpm typecheck && pnpm test` green.

```bash
git add app/lib/countries.ts app/lib/countries.test.ts app/lib/queries.server.ts tests/queries.server.test.ts
git commit -m "feat(backoffice): per-country address queries for company detail"
```

---

### Task 2: Geocoder with persistent sqlite cache + resource route

**Files:**
- Create: `app/lib/geocode.server.ts`
- Create: `tests/geocode.server.test.ts`
- Create: `app/routes/country-geocode.ts`
- Modify: `app/routes.ts` (add `route("geocode", "routes/country-geocode.ts")` inside the `:country` children)
- Modify: `.gitignore` (add `.cache/`)

**Interfaces:**
- Produces:
  - `interface GeoPoint { lat: number; lon: number }`
  - `geocodeAddress(address: string, opts?: { fetcher?: typeof fetch; minIntervalMs?: number; dbPath?: string }): Promise<GeoPoint | null>` — normalized-key cache (positive + negative) in sqlite; throttled Nominatim call on miss; `null` on no-result AND on fetch failure (failures are NOT negative-cached — retry next time).
  - `clearGeocodeThrottleForTests(): void`
  - Route: `GET /:country/geocode?address=...` → `{ coords: GeoPoint | null }`; 404 unknown country; 400 missing/blank or >300-char address.

- [ ] **Step 1: Availability check, then failing tests**

First verify the runtime has the built-in sqlite: `node -e "const {DatabaseSync} = require('node:sqlite'); console.log(typeof DatabaseSync)"` → must print `function`. If it throws, STOP (NEEDS_CONTEXT).

`tests/geocode.server.test.ts`:

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { clearGeocodeThrottleForTests, geocodeAddress } from "~/lib/geocode.server";

function fakeFetch(results: unknown) {
  return vi.fn(async () =>
    new Response(JSON.stringify(results), { status: 200, headers: { "content-type": "application/json" } }),
  ) as unknown as typeof fetch & ReturnType<typeof vi.fn>;
}

function tempDb() {
  return join(mkdtempSync(join(tmpdir(), "geocode-test-")), "cache.sqlite");
}

describe("geocodeAddress", () => {
  it("returns coordinates from nominatim-shaped results and caches them", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch([{ lat: "59.911", lon: "10.752" }]);
    const first = await geocodeAddress("Karl Johans gate 1, 0154 Oslo, Norway", { fetcher, dbPath, minIntervalMs: 0 });
    expect(first).toEqual({ lat: 59.911, lon: 10.752 });
    const second = await geocodeAddress("  karl johans GATE 1,   0154 Oslo, Norway ", { fetcher, dbPath, minIntervalMs: 0 });
    expect(second).toEqual(first); // normalized key → cache hit
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("negative-caches empty results", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch([]);
    expect(await geocodeAddress("Nowhere 1, Atlantis", { fetcher, dbPath, minIntervalMs: 0 })).toBeNull();
    expect(await geocodeAddress("Nowhere 1, Atlantis", { fetcher, dbPath, minIntervalMs: 0 })).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("does not cache fetch failures", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const failing = vi.fn(async () => { throw new Error("network down"); }) as unknown as typeof fetch & ReturnType<typeof vi.fn>;
    expect(await geocodeAddress("Retry St 1, Oslo", { fetcher: failing, dbPath, minIntervalMs: 0 })).toBeNull();
    expect(await geocodeAddress("Retry St 1, Oslo", { fetcher: failing, dbPath, minIntervalMs: 0 })).toBeNull();
    expect(failing).toHaveBeenCalledTimes(2); // second call retried, not negative-cached
  });

  it("throttles consecutive misses", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch([]);
    const start = Date.now();
    await geocodeAddress("A 1, X", { fetcher, dbPath, minIntervalMs: 120 });
    await geocodeAddress("B 2, Y", { fetcher, dbPath, minIntervalMs: 120 });
    expect(Date.now() - start).toBeGreaterThanOrEqual(120);
  });
});
```

Run: `pnpm test geocode` — FAIL (module not found).

- [ ] **Step 2: Implement `app/lib/geocode.server.ts`**

```ts
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";

export interface GeoPoint {
  lat: number;
  lon: number;
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

// Global 1 req/s politeness throttle (Nominatim usage policy).
let lastRequestAt = 0;
let queue: Promise<unknown> = Promise.resolve();

export function clearGeocodeThrottleForTests(): void {
  lastRequestAt = 0;
  queue = Promise.resolve();
}

async function throttled<T>(minIntervalMs: number, fn: () => Promise<T>): Promise<T> {
  const run = queue.then(async () => {
    const wait = lastRequestAt + minIntervalMs - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastRequestAt = Date.now();
    return fn();
  });
  queue = run.catch(() => {});
  return run;
}

export async function geocodeAddress(
  address: string,
  opts?: { fetcher?: typeof fetch; minIntervalMs?: number; dbPath?: string },
): Promise<GeoPoint | null> {
  const key = normalizeAddress(address);
  if (key === "") return null;
  const db = getDb(opts?.dbPath ?? DEFAULT_DB_PATH);

  const cached = db
    .prepare("SELECT lat, lon, resolved FROM geocode_cache WHERE address = ?")
    .get(key) as { lat: number | null; lon: number | null; resolved: number } | undefined;
  if (cached) {
    return cached.resolved === 1 ? { lat: cached.lat!, lon: cached.lon! } : null;
  }

  const fetcher = opts?.fetcher ?? fetch;
  const minIntervalMs = opts?.minIntervalMs ?? DEFAULT_MIN_INTERVAL_MS;
  let results: Array<{ lat: string; lon: string }>;
  try {
    const response = await throttled(minIntervalMs, () =>
      fetcher(`${NOMINATIM_URL}?format=jsonv2&limit=1&q=${encodeURIComponent(address)}`, {
        headers: { "User-Agent": USER_AGENT },
      }),
    );
    if (!response.ok) return null; // transient upstream failure: no cache entry
    results = (await response.json()) as Array<{ lat: string; lon: string }>;
  } catch {
    return null; // network failure: no cache entry, retry next time
  }

  const hit = results[0];
  const point =
    hit && Number.isFinite(Number(hit.lat)) && Number.isFinite(Number(hit.lon))
      ? { lat: Number(hit.lat), lon: Number(hit.lon) }
      : null;

  db.prepare(
    "INSERT OR REPLACE INTO geocode_cache (address, lat, lon, resolved, fetched_at) VALUES (?, ?, ?, ?, ?)",
  ).run(key, point?.lat ?? null, point?.lon ?? null, point ? 1 : 0, new Date().toISOString());

  return point;
}
```

- [ ] **Step 3: Resource route**

`app/routes/country-geocode.ts`:

```ts
import type { Route } from "./+types/country-geocode";
import { getCountry } from "~/lib/countries";
import { geocodeAddress } from "~/lib/geocode.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const address = (new URL(request.url).searchParams.get("address") ?? "").trim();
  if (address === "" || address.length > 300) {
    throw new Response("Invalid address", { status: 400 });
  }
  return { coords: await geocodeAddress(address) };
}
```

Register in `app/routes.ts` inside the `:country` children: `route("geocode", "routes/country-geocode.ts"),`. Append `.cache/` to the app's `.gitignore`.

- [ ] **Step 4: Verify + commit**

`pnpm test geocode` → PASS (4 tests, no real network); full `pnpm typecheck && pnpm test` green. One MANUAL live check (single request, counts against Nominatim politely): `curl -s 'http://localhost:5183/no/geocode?address=Karl%20Johans%20gate%201%2C%20Oslo%2C%20Norway'` → `{"coords":{"lat":...,"lon":...}}`; run it twice — the second is instant (cache).

```bash
git add app/lib/geocode.server.ts tests/geocode.server.test.ts app/routes/country-geocode.ts app/routes.ts .gitignore
git commit -m "feat(backoffice): nominatim geocoder with persistent sqlite cache"
```

---

### Task 3: Mini map + Contact & location card

**Files:**
- Create: `app/components/detail/mini-map.tsx` (mount gate + lazy)
- Create: `app/components/detail/mini-map-inner.tsx` (actual leaflet imports)
- Create: `app/components/detail/contact-location-card.tsx`
- Modify: `app/routes/country-company-detail.tsx` (render the card; drop ContactsSection)
- Modify: `app/components/detail/detail-sections.tsx` (delete ContactsSection)
- Modify: `package.json` (deps)

**Interfaces:**
- Consumes: `ContactRow`/`AddressRow`/`CompanyDetail` (Tasks 1 + existing), `GeoPoint` type shape, the `/:country/geocode` route (Task 2).
- Produces: `<ContactLocationCard country contacts addresses record />` — contacts with per-type icons, addresses, and the mini map (LV stored coords direct; otherwise fetcher-geocoded first address). Card renders null ONLY when there are no contacts AND no addresses.

- [ ] **Step 1: Install deps**

```bash
pnpm add leaflet react-leaflet
pnpm add -D @types/leaflet
```

(react-leaflet v5 targets React 19 — matches this app. If peer resolution complains, report the actual versions rather than forcing.)

- [ ] **Step 2: Map components**

`app/components/detail/mini-map-inner.tsx` (the ONLY file importing leaflet):

```tsx
import { CircleMarker, MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

export default function MiniMapInner({ lat, lon }: { lat: number; lon: number }) {
  return (
    <MapContainer
      center={[lat, lon]}
      zoom={14}
      scrollWheelZoom={false}
      className="h-48 w-full rounded-md"
    >
      <TileLayer
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />
      <CircleMarker center={[lat, lon]} radius={8} pathOptions={{ fillOpacity: 0.7 }} />
    </MapContainer>
  );
}
```

`app/components/detail/mini-map.tsx`:

```tsx
import { Suspense, lazy, useEffect, useState } from "react";

const MiniMapInner = lazy(() => import("./mini-map-inner"));

export function MiniMap({ lat, lon }: { lat: number; lon: number }) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return <div className="bg-muted h-48 w-full rounded-md" />;
  return (
    <Suspense fallback={<div className="bg-muted h-48 w-full rounded-md" />}>
      <MiniMapInner lat={lat} lon={lon} />
    </Suspense>
  );
}
```

- [ ] **Step 3: The card**

`app/components/detail/contact-location-card.tsx`:

```tsx
import { useEffect, useRef } from "react";
import { useFetcher } from "react-router";
import { Globe, Mail, Phone, Printer, Smartphone } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { AddressRow, CompanyListRow, ContactRow } from "~/lib/queries.server";
import { humanizeFieldKey } from "~/components/detail/fields";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { MiniMap } from "~/components/detail/mini-map";

const CONTACT_ICONS: Record<string, typeof Mail> = {
  email: Mail,
  phone: Phone,
  mobile: Smartphone,
  fax: Printer,
  website: Globe,
};

function storedCoords(record: Record<string, unknown>): { lat: number; lon: number } | null {
  const lat = record.address_latitude;
  const lon = record.address_longitude;
  return typeof lat === "number" && typeof lon === "number" ? { lat, lon } : null;
}

export function ContactLocationCard({
  country,
  contacts,
  addresses,
  record,
}: {
  country: CountryConfig;
  contacts: ContactRow[];
  addresses: AddressRow[];
  record: Record<string, unknown>;
}) {
  const fetcher = useFetcher<{ coords: { lat: number; lon: number } | null }>();
  const requested = useRef(false);
  const stored = storedCoords(record);
  const geocodeTarget =
    !stored && addresses.length > 0
      ? `${addresses[0].full_address}, ${country.name}`
      : null;

  useEffect(() => {
    if (geocodeTarget && !requested.current && fetcher.state === "idle" && fetcher.data === undefined) {
      requested.current = true;
      fetcher.load(
        `/${country.code}/geocode?address=${encodeURIComponent(geocodeTarget)}`,
      );
    }
  }, [geocodeTarget, fetcher, country.code]);

  if (contacts.length === 0 && addresses.length === 0) return null;
  const coords = stored ?? fetcher.data?.coords ?? null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contact &amp; location</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {contacts.length > 0 ? (
          <ul className="space-y-1.5">
            {contacts.map((c, i) => {
              const Icon = CONTACT_ICONS[c.contact_type];
              const isLink = c.contact_type === "website" || c.contact_value.startsWith("http");
              return (
                <li key={`${c.contact_type}-${c.contact_value}-${i}`} className="flex items-baseline gap-2 text-sm">
                  {Icon ? (
                    <Icon className="text-muted-foreground size-3.5 shrink-0 self-center" />
                  ) : (
                    <Badge variant="outline">{c.contact_type}</Badge>
                  )}
                  {isLink ? (
                    <a
                      href={c.contact_value.startsWith("http") ? c.contact_value : `https://${c.contact_value}`}
                      target="_blank"
                      rel="noreferrer"
                      className="break-all underline underline-offset-2"
                    >
                      {c.contact_value}
                    </a>
                  ) : (
                    <span className="break-all">{c.contact_value}</span>
                  )}
                </li>
              );
            })}
          </ul>
        ) : null}

        {addresses.map((a, i) => (
          <div key={`${a.address_type}-${i}`} className="text-sm">
            <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              {humanizeFieldKey(a.address_type)}
            </p>
            <p>{a.full_address}</p>
          </div>
        ))}

        {coords ? (
          <MiniMap lat={coords.lat} lon={coords.lon} />
        ) : geocodeTarget && fetcher.state !== "idle" ? (
          <div className="bg-muted text-muted-foreground flex h-48 w-full items-center justify-center rounded-md text-xs">
            Locating…
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 4: Wire the route, remove the old section**

In `app/routes/country-company-detail.tsx`: replace `<ContactsSection contacts={detail.contacts} />` with

```tsx
<ContactLocationCard
  country={country}
  contacts={detail.contacts}
  addresses={detail.addresses}
  record={detail.record}
/>
```

(imports updated). In `app/components/detail/detail-sections.tsx`: DELETE `ContactsSection` (DomainsSection stays); remove any now-unused imports.

- [ ] **Step 5: Verify**

`pnpm typecheck && pnpm test` → green. Against the running dev server (user-owned, hot-reloaded — do not restart it):

```bash
# EE company with contacts + address: card SSR-renders contacts and address text
ID=$(curl -s "http://companycollect:8123/?user=default&password=password123" --data "SELECT registry_id FROM corpscout.ee_company_contacts WHERE is_current=1 AND registry_id IN (SELECT reg_code FROM ee_companies WHERE address != '') ORDER BY registry_id LIMIT 1")
curl -s "http://localhost:5183/ee/companies/$ID" | grep -c 'Contact &amp; location\|Contact & location'   # >= 1
# LV company with stored coords: map container present without geocode
LVID=$(curl -s "http://companycollect:8123/?user=default&password=password123" --data "SELECT regcode FROM corpscout.lv_companies WHERE address_latitude IS NOT NULL ORDER BY regcode LIMIT 1")
curl -s "http://localhost:5183/lv/companies/$LVID" | grep -c 'Contact'  # >= 1
```

Browser: open the EE company — icons per contact type, address block, map appears after a moment (first visit geocodes ≈1s, second visit instant from sqlite cache); LV company with stored coords shows the map immediately; a company with no contacts/address shows no card. Verify the map tiles load and attribution shows.

- [ ] **Step 6: Commit**

```bash
git add app/components/detail/mini-map.tsx app/components/detail/mini-map-inner.tsx app/components/detail/contact-location-card.tsx app/routes/country-company-detail.tsx app/components/detail/detail-sections.tsx package.json pnpm-lock.yaml
git commit -m "feat(backoffice): contact and location card with mini map"
```

---

### Task 4: Gate + README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README**

Add under `### Company detail`:

```markdown
The Contact & location card shows contacts, addresses (per-country
`addressQuery` in `countries.ts` — Norway's `no_company_addresses` is wired
but awaits its first dagster materialization; Finland has no address data
yet), and a leaflet mini map. Coordinates come from Latvia's stored
lat/long where present; otherwise the address is geocoded server-side via
Nominatim (1 req/s, results — including misses — cached permanently in
`.cache/geocode.sqlite` via node:sqlite).
```

- [ ] **Step 2: Full gate**

`pnpm typecheck && pnpm test && pnpm build` (build must succeed with the client-only leaflet split — verify the SSR bundle does NOT bundle leaflet: `rg -l leaflet build/server` must return no matches; a match means the lazy split leaked into the server build). Then `pnpm start` (port 3000, YOUR server — kill after; never touch 5183) and curl an EE detail page for `Contact` presence.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(backoffice): document contact location card and geocoding"
```

---

## Out of scope (logged)

- FI addresses (fi_addresses is empty — pipeline gap list) and NO address data arrival (wired, waiting on the dagster materialization).
- Batch/upstream geocoding in dagster (the sqlite cache is the app-side bridge; a `company_coordinates` table is the eventual proper home).
- Denmark CVR (dagster ingestion in progress; no ClickHouse tables yet).
- Map clustering/multi-address pins; only the first address is geocoded in v1.
