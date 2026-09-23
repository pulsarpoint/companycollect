import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
beforeEach(() => {
  vi.resetAllMocks();
  vi.resetModules();
});

it("counts deduplicated addresses across every bucket and shares cached totals", async () => {
  db.chQuery.mockResolvedValue([
    { ip_version: 4, addresses: "10" },
    { ip_version: 6, addresses: "20" },
  ]);
  const { getWorkspaceIpStatistics } =
    await import("~/lib/workspace-ip-addresses.server");
  const [first, second] = await Promise.all([
    getWorkspaceIpStatistics(),
    getWorkspaceIpStatistics(),
  ]);
  expect(first).toMatchObject({ total: 7680, ipv4: 2560, ipv6: 5120 });
  expect(second).toEqual(first);
  expect(await getWorkspaceIpStatistics()).toEqual(first);
  expect(db.chQuery).toHaveBeenCalledTimes(256);
  expect(
    new Set(db.chQuery.mock.calls.map(([, params]) => params.bucket)).size,
  ).toBe(256);
  for (const [sql] of db.chQuery.mock.calls)
    expect(sql).toContain("commoncrawl_ip_addresses FINAL");
  const clock = vi
    .spyOn(Date, "now")
    .mockReturnValue(Date.parse(first.countedAt) + 300001);
  try {
    await getWorkspaceIpStatistics();
    expect(db.chQuery).toHaveBeenCalledTimes(512);
  } finally {
    clock.mockRestore();
  }
});

it("does not cache partial totals when a bucket fails and allows a retry", async () => {
  db.chQuery
    .mockRejectedValueOnce(new Error("Database unavailable"))
    .mockResolvedValue([]);
  const { getWorkspaceIpStatistics } =
    await import("~/lib/workspace-ip-addresses.server");
  await expect(getWorkspaceIpStatistics()).rejects.toThrow(
    "Database unavailable",
  );
  db.chQuery.mockClear();
  expect(await getWorkspaceIpStatistics()).toMatchObject({
    total: 0,
    ipv4: 0,
    ipv6: 0,
  });
  expect(db.chQuery).toHaveBeenCalledTimes(256);
});
