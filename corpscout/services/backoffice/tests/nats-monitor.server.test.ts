import { afterEach, describe, expect, it, vi } from "vitest";
import { loadNatsMonitor } from "~/lib/nats-monitor.server";
// Captured from the production server (nats-server 2.14.7) with a temporary
// work-queue stream, one consumer mid-flight, and an uncapped stream.
import jszFixture from "./fixtures/nats-jsz.json";
import varzFixture from "./fixtures/nats-varz.json";

const MONITOR_URL = "http://nats.example:8222";
const CHECKED_AT = 1_789_416_292;

function respondWith(payloads: { varz: unknown; jsz: unknown }) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const path = new URL(String(input)).pathname;
    return Response.json(path === "/varz" ? payloads.varz : payloads.jsz);
  });
}

function load(fetchImpl: typeof fetch, url: string | undefined = MONITOR_URL) {
  return loadNatsMonitor({ url, fetchImpl, now: () => CHECKED_AT * 1000 });
}

describe("loadNatsMonitor", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("asks the monitoring endpoint for the server and for every stream with its consumers", async () => {
    const fetchImpl = respondWith({ varz: varzFixture, jsz: jszFixture });
    await load(fetchImpl);
    const requested = fetchImpl.mock.calls.map(([input]) => String(input)).sort();
    expect(requested).toEqual([
      `${MONITOR_URL}/jsz?consumers=true&config=true`,
      `${MONITOR_URL}/varz`,
    ]);
    for (const [, init] of fetchImpl.mock.calls as unknown as [unknown, RequestInit][]) {
      expect(init.signal).toBeInstanceOf(AbortSignal);
    }
  });

  it("describes the server", async () => {
    const { snapshot, error } = await load(respondWith({ varz: varzFixture, jsz: jszFixture }));
    expect(error).toBeNull();
    expect(snapshot?.checkedAt).toBe(CHECKED_AT);
    expect(snapshot?.monitorUrl).toBe(MONITOR_URL);
    expect(snapshot?.server).toEqual({
      name: "nats",
      version: "2.14.7",
      goVersion: "go1.26.8",
      startedAt: "2026-09-17T16:10:49.989040663Z",
      uptime: "3h54m2s",
      configLoadedAt: "2026-09-17T16:11:42.896187849Z",
      clientPort: 4222,
      authRequired: true,
      cores: 8,
      cpuPercent: 0,
      memoryBytes: 21_868_544,
      connections: 0,
      totalConnections: 56,
      maxConnections: 65_536,
      subscriptions: 80,
      slowConsumers: 0,
      staleConnections: 0,
      inMessages: 120,
      outMessages: 87,
      inBytes: 7_527,
      outBytes: 47_796,
      maxPayloadBytes: 1_048_576,
    });
  });

  it("reports JetStream usage against the configured limits", async () => {
    const { snapshot } = await load(respondWith({ varz: varzFixture, jsz: jszFixture }));
    expect(snapshot?.jetstream).toEqual({
      storeDir: "/var/lib/nats/jetstream",
      fileUsedBytes: 531,
      fileLimitBytes: 96_636_764_160,
      fileReservedBytes: 1_073_741_824,
      memoryUsedBytes: 0,
      memoryLimitBytes: 4_294_967_296,
      memoryReservedBytes: 0,
      accounts: 1,
      streams: 2,
      consumers: 1,
      messages: 9,
      bytes: 531,
      apiRequests: 69,
      apiErrors: 7,
    });
  });

  it("keeps every stream setting, with unlimited limits as null", async () => {
    const { snapshot } = await load(respondWith({ varz: varzFixture, jsz: jszFixture }));
    expect(snapshot?.streams.map((stream) => stream.name)).toEqual(["CRAWL_PAGES", "EVENTS"]);
    const [crawl, events] = snapshot!.streams;
    expect(crawl).toMatchObject({
      account: "CORPSCOUT",
      name: "CRAWL_PAGES",
      createdAt: "2026-09-17T20:04:49.886209559Z",
      subjects: ["crawl.pages.>"],
      storage: "file",
      retention: "workqueue",
      discard: "old",
      compression: "none",
      replicas: 1,
      maxBytes: 1_073_741_824,
      maxMessages: null,
      maxMessagesPerSubject: null,
      maxMessageBytes: null,
      maxConsumers: null,
      maxAgeNanos: 604_800_000_000_000,
      duplicateWindowNanos: 120_000_000_000,
      messages: 6,
      bytes: 354,
      firstSequence: 5,
      lastSequence: 10,
      firstMessageAt: "2026-09-17T20:04:49.94538939Z",
      lastMessageAt: "2026-09-17T20:04:49.964098897Z",
      subjectCount: 2,
    });
    expect(events).toMatchObject({
      name: "EVENTS",
      retention: "limits",
      maxBytes: null,
      maxAgeNanos: null,
      messages: 3,
      subjectCount: 1,
      consumers: [],
    });
  });

  it("reports how far each consumer has processed its stream", async () => {
    const { snapshot } = await load(respondWith({ varz: varzFixture, jsz: jszFixture }));
    expect(snapshot?.streams[0].consumers).toEqual([
      {
        name: "analyzer",
        createdAt: "2026-09-17T20:04:50.00462682Z",
        durable: true,
        mode: "pull",
        ackPolicy: "explicit",
        deliverPolicy: "all",
        ackWaitNanos: 1_000_000_000,
        maxDeliver: null,
        maxAckPending: 1000,
        filterSubjects: [],
        unprocessed: 4,
        inFlight: 2,
        redelivered: 1,
        waitingPulls: 0,
        deliveredSequence: 6,
        ackFloorSequence: 4,
        lastDeliveredAt: "2026-09-17T20:04:52.092041636Z",
        lastAckedAt: "2026-09-17T20:04:50.035462578Z",
      },
    ]);
  });

  it("reads an empty server as no streams, not as a failure", async () => {
    const empty = {
      ...jszFixture,
      streams: 0,
      consumers: 0,
      messages: 0,
      bytes: 0,
      // What the server really sends before any stream exists: the account
      // entry carries no stream_detail at all.
      account_details: [{ name: "CORPSCOUT", id: "CORPSCOUT" }],
    };
    const { snapshot, error } = await load(respondWith({ varz: varzFixture, jsz: empty }));
    expect(error).toBeNull();
    expect(snapshot?.streams).toEqual([]);
    expect(snapshot?.jetstream?.streams).toBe(0);
  });

  it("reports a server running without JetStream", async () => {
    const { snapshot, error } = await load(
      respondWith({ varz: varzFixture, jsz: { server_id: "X", now: "2026-09-17T20:04:52Z", disabled: true } }),
    );
    expect(error).toBeNull();
    expect(snapshot?.server.name).toBe("nats");
    expect(snapshot?.jetstream).toBeNull();
    expect(snapshot?.streams).toEqual([]);
  });

  it("reads the endpoint from NATS_MONITOR_URL, ignoring a trailing slash", async () => {
    vi.stubEnv("NATS_MONITOR_URL", " http://from-env:8222/ ");
    const fetchImpl = respondWith({ varz: varzFixture, jsz: jszFixture });
    const { snapshot } = await loadNatsMonitor({ fetchImpl });
    expect(snapshot?.monitorUrl).toBe("http://from-env:8222");
    expect(String(fetchImpl.mock.calls[0][0])).toMatch(/^http:\/\/from-env:8222\/(varz|jsz)/);
  });

  it("says how to configure the endpoint instead of calling anything", async () => {
    vi.stubEnv("NATS_MONITOR_URL", "");
    const fetchImpl = respondWith({ varz: varzFixture, jsz: jszFixture });
    const result = await loadNatsMonitor({ fetchImpl });
    expect(result.snapshot).toBeNull();
    expect(result.error?.kind).toBe("not_configured");
    expect(result.error?.message).toContain("NATS_MONITOR_URL");
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("returns an unreachable server as a result, never as a thrown error", async () => {
    const refused = vi.fn(async () => {
      throw new TypeError("fetch failed");
    });
    const result = await load(refused as unknown as typeof fetch);
    expect(result.snapshot).toBeNull();
    expect(result.error?.kind).toBe("unreachable");
    expect(result.error?.message).toContain(MONITOR_URL);
    expect(result.error?.message).toContain("fetch failed");
  });

  it("treats an HTTP error from the endpoint as unreachable", async () => {
    const failing = vi.fn(async () => new Response("nope", { status: 503 }));
    const result = await load(failing as unknown as typeof fetch);
    expect(result.error?.kind).toBe("unreachable");
    expect(result.error?.message).toContain("HTTP 503");
  });
});
