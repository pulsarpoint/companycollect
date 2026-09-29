import { expect, it } from "vitest";
import {
  EMPTY_WORKSPACE_IP_FILTERS,
  normalizeAsn,
  parseWorkspaceIpFilters,
  workspaceIpAddressesHref,
  workspaceIpListOrder,
  withoutFilterValue,
} from "~/lib/workspace-ip-addresses";

it.each([
  ["15169", "15169"],
  ["AS15169", "15169"],
  ["as 15169", "15169"],
  [" 3301 ", "3301"],
  ["0", ""],
  ["4294967296", ""],
  ["AS", ""],
  ["15169; DROP", ""],
])("normalizes ASN %j to %j", (input, expected) => {
  expect(normalizeAsn(input)).toBe(expected);
});

it("parses repeated filters, drops invalid values and deduplicates", () => {
  const filters = parseWorkspaceIpFilters(
    new URLSearchParams("asn=AS15169&asn=15169&asn=x&country=se&country=USA&region=ab&city=Stockholm&search=%2010.0.&version=6"),
  );
  expect(filters).toEqual({
    search: "10.0.",
    version: "6",
    asn: ["15169"],
    country: ["SE"],
    region: ["AB"],
    city: ["Stockholm"],
  });
});

it("drops region and city unless exactly one country is chosen", () => {
  expect(parseWorkspaceIpFilters(new URLSearchParams("region=AB&city=Stockholm"))).toMatchObject({ region: [], city: [] });
  expect(parseWorkspaceIpFilters(new URLSearchParams("country=SE&country=NO&region=AB"))).toMatchObject({ country: ["SE", "NO"], region: [] });
});

it("rejects control characters and overlong city names", () => {
  const params = new URLSearchParams();
  params.append("country", "SE");
  params.append("city", "Bad\u0000City");
  params.append("city", "x".repeat(201));
  params.append("city", "Göteborg");
  expect(parseWorkspaceIpFilters(params).city).toEqual(["Göteborg"]);
});

it("round-trips through the list href", () => {
  const filters = { ...EMPTY_WORKSPACE_IP_FILTERS, asn: ["15169", "3301"], country: ["SE"], region: ["AB"], city: ["Stockholm"], version: "4" as const };
  const href = workspaceIpAddressesHref(filters);
  expect(href).toBe("/admin/ip-addresses?version=4&asn=15169&asn=3301&country=SE&region=AB&city=Stockholm");
  expect(parseWorkspaceIpFilters(new URL(href, "http://x").searchParams)).toEqual(filters);
  expect(workspaceIpAddressesHref(EMPTY_WORKSPACE_IP_FILTERS)).toBe("/admin/ip-addresses");
});

it("removes one chip's value and the location scope with the last country", () => {
  const filters = { ...EMPTY_WORKSPACE_IP_FILTERS, asn: ["15169", "3301"], country: ["SE"], region: ["AB"], city: ["Stockholm"] };
  expect(withoutFilterValue(filters, "asn:3301").asn).toEqual(["15169"]);
  expect(withoutFilterValue(filters, "city:Stockholm")).toMatchObject({ country: ["SE"], region: ["AB"], city: [] });
  expect(withoutFilterValue(filters, "country:SE")).toMatchObject({ country: [], region: [], city: [] });
  expect(withoutFilterValue(filters, "sql:1")).toBe(filters);
});

it("orders by location only when locations are filtered without ASNs", () => {
  expect(workspaceIpListOrder(EMPTY_WORKSPACE_IP_FILTERS)).toBe("asn");
  expect(workspaceIpListOrder({ ...EMPTY_WORKSPACE_IP_FILTERS, country: ["SE"] })).toBe("location");
  expect(workspaceIpListOrder({ ...EMPTY_WORKSPACE_IP_FILTERS, country: ["SE"], asn: ["3301"] })).toBe("asn");
});
