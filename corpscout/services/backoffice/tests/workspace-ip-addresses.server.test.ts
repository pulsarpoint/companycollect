import { beforeEach, expect, it, vi } from "vitest";
import {
  parseWorkspaceIpFilters,
  workspaceIpAddressesHref,
} from "~/lib/workspace-ip-addresses";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { listWorkspaceIpAddresses } =
  await import("~/lib/workspace-ip-addresses.server");
beforeEach(() => vi.resetAllMocks());

it("keeps unenriched DNS addresses and only enriches the displayed page", async () => {
  const keys = Array.from({ length: 51 }, (_, i) => ({
    bucket: 0,
    ip_version: 4,
    ip: `192.0.2.${i + 1}`,
  }));
  db.chQuery.mockResolvedValueOnce(keys).mockResolvedValue([]);
  const result = await listWorkspaceIpAddresses({ search: "", version: "any" });
  expect(result.rows).toHaveLength(50);
  expect(result.rows[0]).toMatchObject({ ip: "192.0.2.1", asn: null });
  expect(result.hasMore).toBe(true);
  expect(JSON.parse(Buffer.from(result.next, "base64url").toString())).toEqual(
    keys[49],
  );
  for (const [, params] of db.chQuery.mock.calls.slice(1)) {
    expect(params.ips).toHaveLength(50);
    expect(params.ips).not.toContain("192.0.2.51");
  }
  expect(db.chQuery.mock.calls[1][0]).toContain("min(first_seen)");
  expect(db.chQuery.mock.calls[1][0]).toContain("max(last_seen)");
});

it("uses the unified current view and retains nulls and unenriched IPs", async () => {
  const keys = [1, 2, 3].map((i) => ({
    bucket: 0,
    ip_version: 4,
    ip: `192.0.2.${i}`,
  }));
  db.chQuery
    .mockResolvedValueOnce(keys)
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([
      {
        ip: keys[0].ip,
        country_iso_code: "RS",
        city_name: null,
        asn: 100,
        asn_organization: "Provider",
        rdap_matched_cidr: "192.0.2.0/24",
        rdap_name: "Network",
      },
      {
        ip: keys[1].ip,
        country_iso_code: null,
        city_name: null,
        asn: null,
        asn_organization: null,
      },
    ]);
  const result = await listWorkspaceIpAddresses({ search: "", version: "any" });
  expect(result.rows[0]).toMatchObject({
    country_iso_code: "RS",
    city_name: null,
    asn: 100,
    rdap_matched_cidr: "192.0.2.0/24",
  });
  expect(result.rows[1]).toMatchObject({
    country_iso_code: null,
    city_name: null,
    asn: null,
  });
  expect(result.rows[2]).toMatchObject({ country_iso_code: null, asn: null });
  expect(db.chQuery.mock.calls[2][0]).toContain(
    "FROM corpscout.ip_enrichment_current",
  );
  expect(db.chQuery.mock.calls).toHaveLength(3);
});

it("binds the full cursor and canonicalizes exact IPv6 searches", async () => {
  db.chQuery.mockResolvedValue([]);
  const after = Buffer.from(
    JSON.stringify({ bucket: 12, ip_version: 6, ip: "2001:db8::1" }),
  ).toString("base64url");
  const filters = parseWorkspaceIpFilters(
    new URLSearchParams("search=2001:0DB8:0:0:0:0:0:2&version=6"),
  );
  const result = await listWorkspaceIpAddresses(filters, after);
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("toString(toIPv6({search:String}))");
  expect(sql).toContain("(bucket, ip_version, ip) >");
  expect(params).toMatchObject({
    bucket: 12,
    cursorVersion: 6,
    afterIp: "2001:db8::1",
    version: 6,
  });
  expect(result.after).toBe(after);
  const href = new URL(
    workspaceIpAddressesHref(filters, after),
    "http://localhost",
  );
  expect(href.searchParams.get("after")).toBe(after);
  expect(href.searchParams.get("version")).toBe("6");
});

it.each([
  "broken",
  Buffer.from("null").toString("base64url"),
  Buffer.from(
    JSON.stringify({ bucket: 256, ip_version: 4, ip: "1.1.1.1" }),
  ).toString("base64url"),
])("ignores invalid cursors safely: %s", async (after) => {
  db.chQuery.mockResolvedValue([]);
  const result = await listWorkspaceIpAddresses(
    { search: "", version: "any" },
    after,
  );
  expect(result.after).toBe("");
  expect(db.chQuery.mock.calls).toHaveLength(1);
  expect(db.chQuery.mock.calls[0][0]).not.toContain("{afterIp:String}");
});

it("rejects invalid address text without scanning the inventory", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceIpAddresses({ search: "x'%_", version: "any" });
  expect(db.chQuery).not.toHaveBeenCalled();
});

it("prunes prefix queries by bucket and binds the prefix", async () => {
  const keys = Array.from({ length: 51 }, (_, i) => ({
    bucket: 0,
    ip_version: 4,
    ip: `192.0.2.${i + 1}`,
  }));
  db.chQuery.mockResolvedValueOnce(keys).mockResolvedValue([]);
  await listWorkspaceIpAddresses({ search: "192.0.2.", version: "any" });
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("bucket = 0");
  expect(sql).toContain("bucket = 7");
  expect(sql).not.toContain("192.0.2.");
  expect(params.search).toBe("192.0.2.");
  expect(params.prefixEnd).toBe("192.0.2/");
});

it("continues sparse prefixes across bucket boundaries before declaring the last page", async () => {
  const keys = Array.from({ length: 51 }, (_, i) => ({
    bucket: 8,
    ip_version: 4,
    ip: `192.0.2.${i + 1}`,
  }));
  db.chQuery
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce(keys)
    .mockResolvedValue([]);
  const result = await listWorkspaceIpAddresses({
    search: "192.0.2.",
    version: "4",
  });
  expect(result.rows).toHaveLength(50);
  expect(result.hasMore).toBe(true);
  expect(db.chQuery.mock.calls[1][0]).toContain("bucket = 8");
  expect(db.chQuery.mock.calls[1][0]).toContain("ip_version = {version:UInt8}");
});
