import { beforeEach, expect, it, vi } from "vitest";
const launch = vi.hoisted(() => vi.fn());
vi.mock("~/lib/ip-enrichment.server", async (original) => ({
  ...(await original<object>()),
  launchIpEnrichment: launch,
}));
import { action } from "~/routes/admin-ip-addresses";

const submit = (body: unknown) =>
  action({
    request: new Request(
      "http://localhost/admin/ip-addresses?version=6&after=cursor",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
    ),
  } as never);
beforeEach(() => {
  launch.mockReset();
});

it("uses the submitted selection independently of page filters", async () => {
  const selection = { mode: "ips", ips: ["8.8.8.8"] };
  launch.mockResolvedValue({ ok: true, runId: "run", taskId: "task" });
  expect(await submit({ action: "enrich", selection })).toMatchObject({
    data: { ok: true, runId: "run" },
  });
  expect(launch).toHaveBeenCalledWith(selection, expect.any(String));
});
it("rejects unsupported actions", async () => {
  expect(await submit({ action: "delete" })).toMatchObject({
    data: { ok: false },
    init: { status: 400 },
  });
  expect(launch).not.toHaveBeenCalled();
});
it("does not claim success or leak internal details after a launch failure", async () => {
  launch.mockRejectedValue(new Error("internal connection details"));
  const result = await submit({
    action: "enrich",
    selection: { mode: "ips", ips: ["8.8.8.8"] },
  });
  expect(result).toMatchObject({ data: { ok: false }, init: { status: 502 } });
  expect(JSON.stringify(result)).not.toContain("internal connection details");
});
