import { expect, it } from "vitest";
import { parseSeDomainsFilters } from "~/lib/se-domains-filters";
import { parseWorkspaceDomainFilters } from "~/lib/workspace-domains";

it.each([
  ["se", "se"], [" .SE ", "se"], [".Co.Uk", "co.uk"], ["xn--p1ai", "xn--p1ai"],
  ["", ""], [".", ""], ["..se", ""], ["https://se", ""], ["se/path", ""],
  ["se%", ""], ["se_", ""], ["se'", ""], ["-se", ""], ["se-", ""],
  ["a".repeat(64), ""], [Array(5).fill("a".repeat(60)).join("."), ""],
])("normalizes the same suffix %s in both domain lists", (input, expected) => {
  const params = new URLSearchParams({ suffix: input });
  expect(parseSeDomainsFilters(params).suffix).toBe(expected);
  expect(parseWorkspaceDomainFilters(params).suffix).toBe(expected);
});
