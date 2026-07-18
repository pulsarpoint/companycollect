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

  it("has_financials is whitelisted and resolves with a real live count", async () => {
    // Regression guard: has_financials passes the UNIFIED_FACET_KEYS
    // whitelist here, and now resolves via a real countIf() over
    // companies_all rather than a zero-count stub.
    const result = await loader(request("http://test/facet-options?column=has_financials"));
    const { options } = result as { options: { value: string; label: string; count: number }[] };
    expect(options).toHaveLength(1);
    expect(options[0]).toMatchObject({ value: "true", label: "yes" });
    expect(options[0].count).toBeGreaterThan(1_000_000);
  });
});
