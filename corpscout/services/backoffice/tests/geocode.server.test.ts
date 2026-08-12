import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  clearGeocodeThrottleForTests,
  geocodeAddress,
  geocodeAddressWithStreetFallback,
} from "~/lib/geocode.server";

function fakeFetch(results: unknown) {
  return vi.fn(
    async () =>
      new Response(JSON.stringify(results), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
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
    const first = await geocodeAddress(
      "Karl Johans gate 1, 0154 Oslo, Norway",
      { fetcher, dbPath, minIntervalMs: 0 },
    );
    expect(first).toEqual({ lat: 59.911, lon: 10.752 });
    const second = await geocodeAddress(
      "  karl johans GATE 1,   0154 Oslo, Norway ",
      { fetcher, dbPath, minIntervalMs: 0 },
    );
    expect(second).toEqual(first); // normalized key → cache hit
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("negative-caches empty results", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch([]);
    expect(
      await geocodeAddress("Nowhere 1, Atlantis", {
        fetcher,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(
      await geocodeAddress("Nowhere 1, Atlantis", {
        fetcher,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("does not cache a non-array response body", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch({ error: "malformed upstream payload" });
    expect(
      await geocodeAddress("Bad Payload 1, X", {
        fetcher,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(
      await geocodeAddress("Bad Payload 1, X", {
        fetcher,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(2); // not negative-cached: retried on second call
  });

  it("does not cache fetch failures", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const failing = vi.fn(async () => {
      throw new Error("network down");
    }) as unknown as typeof fetch & ReturnType<typeof vi.fn>;
    expect(
      await geocodeAddress("Retry St 1, Oslo", {
        fetcher: failing,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(
      await geocodeAddress("Retry St 1, Oslo", {
        fetcher: failing,
        dbPath,
        minIntervalMs: 0,
      }),
    ).toBeNull();
    expect(failing).toHaveBeenCalledTimes(2); // second call retried, not negative-cached
  });

  it("scopes the cache and Nominatim query by country code", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = fakeFetch([{ lat: "59.911", lon: "10.752" }]);
    const address = "Bekkeliveien 5, 3470 SLEMMESTAD, Norge";

    const first = await geocodeAddress(address, {
      fetcher,
      dbPath,
      minIntervalMs: 0,
      countryCode: "no",
    });
    expect(first).toEqual({ lat: 59.911, lon: 10.752 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    const firstUrl = fetcher.mock.calls[0][0] as string;
    expect(firstUrl).toContain("countrycodes=no");

    // Same address, different country code → different cache key → second fetch.
    const second = await geocodeAddress(address, {
      fetcher,
      dbPath,
      minIntervalMs: 0,
      countryCode: "se",
    });
    expect(second).toEqual({ lat: 59.911, lon: 10.752 });
    expect(fetcher).toHaveBeenCalledTimes(2);
    const secondUrl = fetcher.mock.calls[1][0] as string;
    expect(secondUrl).toContain("countrycodes=se");
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

  it("falls back to a structured street query and reports street precision", async () => {
    clearGeocodeThrottleForTests();
    const dbPath = tempDb();
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify([]), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify([{ lat: "59.406966", lon: "17.927918" }]), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      ) as unknown as typeof fetch & ReturnType<typeof vi.fn>;

    const first = await geocodeAddressWithStreetFallback(
      "BERGENGATAN 13, 164 37 KISTA",
      { street: "BERGENGATAN 13", postalCode: "16437" },
      { fetcher, dbPath, minIntervalMs: 0, countryCode: "se" },
    );

    expect(first).toEqual({
      coords: { lat: 59.406966, lon: 17.927918 },
      precision: "street",
    });
    expect(fetcher).toHaveBeenCalledTimes(2);
    const fallbackUrl = fetcher.mock.calls[1][0] as string;
    expect(fallbackUrl).toContain("street=BERGENGATAN+13");
    expect(fallbackUrl).toContain("postalcode=16437");
    expect(fallbackUrl).toContain("countrycodes=se");

    const second = await geocodeAddressWithStreetFallback(
      "BERGENGATAN 13, 164 37 KISTA",
      { street: "BERGENGATAN 13", postalCode: "16437" },
      { fetcher, dbPath, minIntervalMs: 0, countryCode: "se" },
    );
    expect(second).toEqual(first);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});
