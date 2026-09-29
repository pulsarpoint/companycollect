import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
beforeEach(() => {
  vi.resetAllMocks();
  vi.resetModules();
});

function answer(sql: string) {
  return Promise.resolve(
    sql.includes("system.view_refreshes")
      ? [{ last_success_time: "2026-09-29 03:41:12", failed: 0 }]
      : [
          { ip_version: 4, addresses: "10" },
          { ip_version: 6, addresses: "20" },
        ],
  );
}

it("counts the search table once, reports its last refresh and shares cached totals", async () => {
  db.chQuery.mockImplementation(answer);
  const { getWorkspaceIpStatistics } =
    await import("~/lib/workspace-ip-addresses.server");
  const [first, second] = await Promise.all([
    getWorkspaceIpStatistics(),
    getWorkspaceIpStatistics(),
  ]);
  expect(first).toMatchObject({ total: 30, ipv4: 10, ipv6: 20, refreshedAt: "2026-09-29 03:41:12", refreshFailed: false });
  expect(second).toEqual(first);
  expect(db.chQuery).toHaveBeenCalledTimes(2);
  expect(db.chQuery.mock.calls[0][0]).toContain("FROM corpscout.ip_enrichment_search");
  expect(db.chQuery.mock.calls[0][0]).toContain("GROUP BY ip_version");
  expect(db.chQuery.mock.calls[1][0]).toContain("view = 'ip_enrichment_search'");
  const clock = vi.spyOn(Date, "now").mockReturnValue(Date.parse(first.countedAt) + 300001);
  try {
    await getWorkspaceIpStatistics();
    expect(db.chQuery).toHaveBeenCalledTimes(4);
  } finally {
    clock.mockRestore();
  }
});

it("reports a table that was never built and a failed rebuild", async () => {
  db.chQuery.mockImplementation((sql: string) =>
    Promise.resolve(sql.includes("system.view_refreshes") ? [{ last_success_time: null, failed: 1 }] : []),
  );
  const { getWorkspaceIpStatistics } =
    await import("~/lib/workspace-ip-addresses.server");
  expect(await getWorkspaceIpStatistics()).toMatchObject({ total: 0, refreshedAt: null, refreshFailed: true });
});

it("does not cache a failed count and allows a retry", async () => {
  db.chQuery.mockRejectedValueOnce(new Error("Database unavailable")).mockImplementation(answer);
  const { getWorkspaceIpStatistics } =
    await import("~/lib/workspace-ip-addresses.server");
  await expect(getWorkspaceIpStatistics()).rejects.toThrow("Database unavailable");
  expect(await getWorkspaceIpStatistics()).toMatchObject({ total: 30 });
});
