import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { dagsterAssetUrl, scheduleDetails, startSchedule, stopSchedule } from "~/lib/dagster.server";

function answering(...bodies: unknown[]) {
  const calls: { query: string; variables: Record<string, unknown> }[] = [];
  let i = 0;
  const fetchImpl = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
    calls.push(JSON.parse(String(init?.body)));
    return Response.json({ data: bodies[Math.min(i++, bodies.length - 1)] });
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

const schedule = (status: string) => ({
  scheduleOrError: {
    __typename: "Schedule", name: "provider_recon_daily", cronSchedule: "12 3 * * *", executionTimezone: "UTC",
    scheduleState: { id: "state-1", status }, futureTicks: { results: [{ timestamp: 1790565120 }] },
  },
});

beforeEach(() => {
  vi.stubEnv("DAGSTER_GRAPHQL_URL", "http://dagster.test/graphql");
  vi.stubEnv("DAGSTER_UI_URL", "");
});
afterEach(() => vi.unstubAllEnvs());

describe("schedule details and control", () => {
  it("reads status, cron, timezone and the next tick", async () => {
    const { calls, fetchImpl } = answering(schedule("RUNNING"));
    expect(await scheduleDetails("provider_recon_daily", { fetchImpl })).toEqual({
      name: "provider_recon_daily", status: "RUNNING", stateId: "state-1", cronSchedule: "12 3 * * *", timezone: "UTC", nextTick: 1790565120,
    });
    expect(calls[0].variables.scheduleSelector).toEqual({
      repositoryLocationName: "dagster_v3", repositoryName: "__repository__", scheduleName: "provider_recon_daily",
    });
  });

  it("stops a running schedule by its state id and starts a stopped one by selector", async () => {
    const stop = answering(schedule("RUNNING"), { stopRunningSchedule: { __typename: "ScheduleStateResult", scheduleState: { status: "STOPPED" } } });
    expect(await stopSchedule("provider_recon_daily", { fetchImpl: stop.fetchImpl })).toBe("STOPPED");
    expect(stop.calls[1].variables).toEqual({ id: "state-1" });

    const start = answering(schedule("STOPPED"), { startSchedule: { __typename: "ScheduleStateResult", scheduleState: { status: "RUNNING" } } });
    expect(await startSchedule("provider_recon_daily", { fetchImpl: start.fetchImpl })).toBe("RUNNING");
  });

  it("treats starting a running and stopping a stopped schedule as no-ops", async () => {
    const running = answering(schedule("RUNNING"));
    expect(await startSchedule("provider_recon_daily", { fetchImpl: running.fetchImpl })).toBe("RUNNING");
    expect(running.calls).toHaveLength(1);
    const stopped = answering(schedule("STOPPED"));
    expect(await stopSchedule("provider_recon_daily", { fetchImpl: stopped.fetchImpl })).toBe("STOPPED");
    expect(stopped.calls).toHaveLength(1);
  });

  it("surfaces union errors", async () => {
    const { fetchImpl } = answering({ scheduleOrError: { __typename: "PythonError", message: "boom" } });
    await expect(scheduleDetails("provider_recon_daily", { fetchImpl })).rejects.toThrow("boom");
  });

  it("builds asset links from the UI base", () => {
    expect(dagsterAssetUrl("provider_recon_documents")).toBe("http://dagster.test/assets/provider_recon_documents");
    vi.stubEnv("DAGSTER_UI_URL", "http://dagster.ui:3000/");
    expect(dagsterAssetUrl("provider_recon_clickhouse")).toBe("http://dagster.ui:3000/assets/provider_recon_clickhouse");
  });
});
