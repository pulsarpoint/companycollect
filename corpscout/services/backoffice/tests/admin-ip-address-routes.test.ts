import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { clearEnrichmentCache } = await import("~/lib/ip-address-detail.server");
const parent = await import("~/routes/admin-ip-address");
const index = await import("~/routes/admin-ip-address-index");
const dns = await import("~/routes/admin-ip-address-dns");
const history = await import("~/routes/admin-ip-address-dns-history");

beforeEach(() => {
  vi.resetAllMocks();
  clearEnrichmentCache();
});

const resolved = (ip: string, segment: string) => [{ ip, bucket: 3, network_segment: segment }];
const args = (address: string, path: string, extra: Record<string, string> = {}) =>
  ({
    params: { address, ...extra },
    request: new Request(`http://localhost${path}`),
  }) as never;

async function thrown(run: () => unknown): Promise<unknown> {
  try {
    await run();
  } catch (error) {
    return error;
  }
  throw new Error("expected the loader to throw");
}

it("parent redirects a non-canonical address to its canonical form, keeping tab and query", async () => {
  db.chQuery.mockResolvedValueOnce(resolved("2001:4860:4860::8888", "2001:4860:4860::/48"));
  const path = "/admin/ip-addresses/2001:4860:4860:0:0:0:0:8888/domains?scope=segment";
  const response = (await thrown(() =>
    parent.loader(args("2001:4860:4860:0:0:0:0:8888", path)),
  )) as Response;
  expect(response.status).toBe(302);
  expect(response.headers.get("Location")).toBe(
    "/admin/ip-addresses/2001%3A4860%3A4860%3A%3A8888/domains?scope=segment",
  );
  expect(db.chQuery).toHaveBeenCalledTimes(1);
});

it("parent, index and tabs answer 404 for an invalid address without querying", async () => {
  for (const run of [
    () => parent.loader(args("not-an-ip", "/admin/ip-addresses/not-an-ip")),
    () => index.loader(args("not-an-ip", "/admin/ip-addresses/not-an-ip")),
    () => dns.loader(args("not-an-ip", "/admin/ip-addresses/not-an-ip/dns")),
  ]) {
    const response = (await thrown(run)) as Response;
    expect(response.status).toBe(404);
  }
  expect(db.chQuery).not.toHaveBeenCalled();
});

it("index redirects a valid address to the canonical overview", async () => {
  db.chQuery.mockResolvedValueOnce(resolved("8.8.8.8", "8.8.8.0/24"));
  const response = (await index.loader(args("8.8.8.8", "/admin/ip-addresses/8.8.8.8?x=1"))) as Response;
  expect(response.status).toBe(302);
  expect(response.headers.get("Location")).toBe("/admin/ip-addresses/8.8.8.8/overview?x=1");
});

it("a tab loader skips its reads when the address is not canonical", async () => {
  db.chQuery.mockResolvedValueOnce(resolved("2001:db8::1", "2001:db8::/48"));
  const result = await dns.loader(args("2001:DB8::1", "/admin/ip-addresses/2001:DB8::1/dns"));
  expect(result).toBeNull();
  expect(db.chQuery).toHaveBeenCalledTimes(1);
});

it("the history route answers a bad root domain and a failed read with records: null", async () => {
  db.chQuery.mockResolvedValue(resolved("8.8.8.8", "8.8.8.0/24"));
  const bad = await history.loader(
    args("8.8.8.8", "/admin/ip-addresses/8.8.8.8/dns/x", { rootDomain: "a b" }),
  );
  expect(bad).toEqual({ records: null });
  expect(db.chQuery).toHaveBeenCalledTimes(1);

  db.chQuery.mockReset();
  db.chQuery
    .mockResolvedValueOnce(resolved("8.8.8.8", "8.8.8.0/24"))
    .mockRejectedValueOnce(new Error("Timeout error."));
  vi.spyOn(console, "error").mockImplementation(() => {});
  const failed = (await history.loader(
    args("8.8.8.8", "/admin/ip-addresses/8.8.8.8/dns/a.example?h=www.a.example", { rootDomain: "a.example" }),
  )) as { data: { records: null } };
  expect(failed.data).toEqual({ records: null });
  const [sql, params] = db.chQuery.mock.calls[1];
  expect(sql).toContain("root_domain = {rootDomain:String}");
  expect(params).toMatchObject({ rootDomain: "a.example", hostnames: ["www.a.example"], code: 1 });
});
