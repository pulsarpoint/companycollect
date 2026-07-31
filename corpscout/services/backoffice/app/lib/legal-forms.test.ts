import { describe, expect, test, vi, beforeEach, afterEach } from "vitest";

const chQuery = vi.fn();
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: (...a: unknown[]) => chQuery(...a) }));

const { getLegalFormLabels, __resetLegalFormCache } = await import("~/lib/legal-forms.server");

const SE = { code: "se" } as never;

const row = (en: string) => [{ legal_form_code: "49", label: "Aktiebolag", label_en: en }];

beforeEach(() => {
  chQuery.mockReset();
  __resetLegalFormCache();
  vi.useFakeTimers();
});
afterEach(() => vi.useRealTimers());

describe("getLegalFormLabels", () => {
  test("serves repeat calls from cache rather than re-querying", async () => {
    chQuery.mockResolvedValue(row("Limited company"));
    await getLegalFormLabels(SE);
    await getLegalFormLabels(SE);
    expect(chQuery).toHaveBeenCalledTimes(1);
  });

  test("picks up a translation loaded after the process cached the map", async () => {
    // The decoding is a dimension a Dagster asset rewrites. Caching it for the
    // life of the process meant Sweden kept rendering "Aktiebolag" after the
    // curated English had already landed in ClickHouse, and only a server
    // restart would have shown it.
    chQuery.mockResolvedValue(row(""));
    expect((await getLegalFormLabels(SE)).get("49")?.en).toBe("");

    chQuery.mockResolvedValue(row("Limited company"));
    await vi.advanceTimersByTimeAsync(6 * 60 * 1000);

    expect((await getLegalFormLabels(SE)).get("49")?.en).toBe("Limited company");
  });

  test("a failed fetch is not cached", async () => {
    chQuery.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(getLegalFormLabels(SE)).rejects.toThrow("clickhouse down");

    chQuery.mockResolvedValue(row("Limited company"));
    expect((await getLegalFormLabels(SE)).get("49")?.en).toBe("Limited company");
  });
});
