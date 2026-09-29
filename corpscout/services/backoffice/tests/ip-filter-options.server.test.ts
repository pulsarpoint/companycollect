import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const options = await import("~/lib/ip-filter-options.server");
beforeEach(() => {
  vi.resetAllMocks();
  options.clearIpFilterOptionCache();
});

const ASNS = [
  { value: "15169", label: "Google LLC", count: "900" },
  { value: "3301", label: "Telia Company AB", count: "500" },
  { value: "24940", label: "Hetzner Online GmbH", count: "400" },
];

it("reads ASNs from the asn_counts projection shape once and ranks typed text", async () => {
  db.chQuery.mockResolvedValue(ASNS);
  expect(await options.searchIpFilterOptions("asn", "AS15169")).toEqual([
    { value: "15169", label: "Google LLC", count: 900 },
  ]);
  expect((await options.searchIpFilterOptions("asn", "hetz")).map((o) => o.value)).toEqual(["24940"]);
  expect((await options.searchIpFilterOptions("asn", "company")).map((o) => o.value)).toEqual(["3301"]);
  expect(await options.searchIpFilterOptions("asn", "")).toHaveLength(3);
  expect(db.chQuery).toHaveBeenCalledTimes(1);
  const [sql] = db.chQuery.mock.calls[0];
  expect(sql).toContain("SELECT asn, asn_organization, count() AS addresses FROM corpscout.ip_enrichment_search");
  expect(sql).toContain("GROUP BY asn, asn_organization");
  expect(sql).toContain("WHERE asn != 0");
});

it("scopes regions and cities to the chosen country with bound parameters", async () => {
  db.chQuery.mockResolvedValue([{ value: "AB", label: "Stockholm", count: "10" }]);
  await options.searchIpFilterOptions("region", "", { country: "SE" });
  let [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("WHERE country_iso_code = {country:String} AND subdivision_iso_code != ''");
  expect(sql).toContain("GROUP BY subdivision_iso_code, subdivision_name");
  expect(params).toEqual({ country: "SE" });
  db.chQuery.mockResolvedValue([{ value: "Stockholm", label: "Stockholm", count: "10" }]);
  await options.searchIpFilterOptions("city", "sto", { country: "SE", regions: ["AB", "AB"] });
  [sql, params] = db.chQuery.mock.calls[1];
  expect(sql).toContain("AND subdivision_iso_code IN {regions:Array(String)}");
  expect(params).toEqual({ country: "SE", regions: ["AB"] });
  expect(sql).not.toContain("'SE'");
});

it("answers nothing without a valid country and never caches a failure", async () => {
  expect(await options.searchIpFilterOptions("region", "", { country: "SE' OR 1" })).toEqual([]);
  expect(db.chQuery).not.toHaveBeenCalled();
  db.chQuery.mockRejectedValueOnce(new Error("down")).mockResolvedValue([{ value: "SE", label: "Sweden", count: "5" }]);
  await expect(options.searchIpFilterOptions("country", "")).rejects.toThrow("down");
  await Promise.resolve();
  expect(await options.searchIpFilterOptions("country", "swe")).toEqual([{ value: "SE", label: "Sweden", count: 5 }]);
});

it("labels chosen values for the chips and survives a failed lookup", async () => {
  db.chQuery.mockImplementation((sql: string) =>
    sql.includes("asn_organization")
      ? Promise.resolve(ASNS)
      : Promise.reject(new Error("down")),
  );
  expect(await options.ipFilterLabels({ asn: ["3301", "999"], country: ["SE"], region: [] })).toEqual({
    "asn:3301": "Telia Company AB",
  });
});
