import { describe, expect, it } from "vitest";
import { loader } from "~/routes/facet-options";

type LoaderArgs = Parameters<typeof loader>[0];

function request(url: string): LoaderArgs {
  return { request: new Request(url) } as LoaderArgs;
}

async function catchThrown(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (e) {
    return e;
  }
  return undefined;
}

describe("facet-options loader", () => {
  it("400s on a column outside UNIFIED_FACET_KEYS", async () => {
    const caught = await catchThrown(loader(request("http://test/facet-options?column=bogus")));
    expect(caught).toBeInstanceOf(Response);
    expect((caught as Response).status).toBe(400);
  });

  it("400s on a real row field that isn't a facet (e.g. name)", async () => {
    const caught = await catchThrown(loader(request("http://test/facet-options?column=name")));
    expect(caught).toBeInstanceOf(Response);
    expect((caught as Response).status).toBe(400);
  });

  it("has_financials is whitelisted but must not 500: resolves with a static yes option", async () => {
    // Regression guard: has_financials passes the UNIFIED_FACET_KEYS
    // whitelist here, but previously fell through to the generic per-column
    // facet lookup and threw "unknown facet: has_financials" (reachable
    // as a real 500 if the endpoint were ever hit directly with this column,
    // e.g. before the FilterSidebar excluded it from the combobox loop).
    const result = await loader(request("http://test/facet-options?column=has_financials"));
    expect(result).toEqual({ options: [{ value: "true", label: "yes", count: 0 }] });
  });
});
