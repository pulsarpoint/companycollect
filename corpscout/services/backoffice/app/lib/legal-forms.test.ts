import { describe, expect, test, vi, beforeEach, afterEach } from "vitest";

const chQuery = vi.fn();
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: (...a: unknown[]) => chQuery(...a) }));

const { getLegalFormLabels, __resetLegalFormCache, lookupLegalForm } = await import(
  "~/lib/legal-forms.server"
);

const SE = { code: "se" } as never;
const FR = {
  code: "fr",
  legalFormLookup: {
    table: "fr_legal_forms_translated",
    codeColumn: "code",
    labelColumn: "label_fr",
    enColumn: "label_en",
    paddedParentFallback: true,
  },
} as never;

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

  test("reads a country's own dimension table when it declares one", async () => {
    chQuery.mockResolvedValue([
      { legal_form_code: "5499", label: "Société à responsabilité limitée", label_en: "Limited liability company (SARL)" },
    ]);
    const labels = await getLegalFormLabels(FR);
    const sql = String(chQuery.mock.calls[0][0]);
    expect(sql).toContain("fr_legal_forms_translated");
    // The shared table is country-scoped; a country's own dimension is not,
    // so it must not be filtered by a column it does not have.
    expect(sql).not.toContain("country_code");
    expect(labels.get("5499")?.en).toBe("Limited liability company (SARL)");
  });

  test("qualifies the filtered column so an alias cannot shadow it", async () => {
    // The dimension views name their columns `label` and `label_en`, which are
    // also the aliases this query assigns. Unqualified, ClickHouse reads the
    // WHERE as referring to `any(label) AS label` and rejects the query with
    // "Aggregate function any(label) AS label is found in WHERE".
    chQuery.mockResolvedValue([]);
    const LV = {
      code: "lv",
      legalFormLookup: {
        table: "lv_legal_forms_translated",
        codeColumn: "code",
        labelColumn: "label",
        enColumn: "label_en",
      },
    } as never;
    await getLegalFormLabels(LV);
    const sql = String(chQuery.mock.calls[0][0]);
    expect(sql).toMatch(/WHERE\s+\w+\.label\s*!=/);
    expect(sql).toMatch(/FROM\s+lv_legal_forms_translated\s+AS\s+\w+/);
  });

  test("a failed fetch is not cached", async () => {
    chQuery.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(getLegalFormLabels(SE)).rejects.toThrow("clickhouse down");

    chQuery.mockResolvedValue(row("Limited company"));
    expect((await getLegalFormLabels(SE)).get("49")?.en).toBe("Limited company");
  });
});

describe("lookupLegalForm", () => {
  const labels = new Map([
    ["22", { en: "", original: "Société créée de fait" }],
    ["54", { en: "", original: "SARL" }],
    ["5499", { en: "Limited liability company (SARL)", original: "SARL sans autre indication" }],
  ]);

  test("an exact code wins", () => {
    expect(lookupLegalForm(labels, "5499", true)?.en).toBe("Limited liability company (SARL)");
  });

  test("a padded level-two code falls back to its two digits", () => {
    expect(lookupLegalForm(labels, "2200", true)?.original).toBe("Société créée de fait");
  });

  test("the fallback needs the trailing zeros", () => {
    // '5498' is a missing level-III code, not a padded level-II one. Reporting
    // it as '54' would call the company a plain SARL on no evidence.
    expect(lookupLegalForm(labels, "5498", true)).toBeUndefined();
  });

  test("the fallback is off unless the country asks for it", () => {
    expect(lookupLegalForm(labels, "2200", false)).toBeUndefined();
  });
});
