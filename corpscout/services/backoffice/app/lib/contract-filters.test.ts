import { describe, expect, it } from "vitest";

import {
  contractFilterCount,
  EMPTY_CONTRACT_FILTERS,
  contractFilterSql,
  parseContractFilters,
  serializeContractFilters,
} from "./contract-filters";

const parse = (qs: string) => parseContractFilters(new URLSearchParams(qs));

describe("parsing contract filters", () => {
  it("reads nothing from an empty query", () => {
    expect(parse("")).toEqual({ agreement: [], amountMin: null, amountMax: null, from: null, to: null });
  });

  it("collects repeated agreement values, deduped", () => {
    // Multi-select: the raw register values, because that is what the column
    // holds. The list translates them for display only.
    expect(parse("agreement=Empenho&agreement=Outros&agreement=Empenho").agreement).toEqual([
      "Empenho",
      "Outros",
    ]);
  });

  it("reads an amount range", () => {
    const f = parse("amount_min=1000&amount_max=50000");
    expect(f.amountMin).toBe(1000);
    expect(f.amountMax).toBe(50000);
  });

  it("accepts an open-ended range from either side", () => {
    expect(parse("amount_min=1000").amountMax).toBeNull();
    expect(parse("amount_max=1000").amountMin).toBeNull();
  });

  it("ignores an amount that is not a number", () => {
    // Never throw on a hand-edited URL: an unusable value is no filter.
    expect(parse("amount_min=abc").amountMin).toBeNull();
    expect(parse("amount_min=").amountMin).toBeNull();
    expect(parse("amount_min=-5").amountMin).toBeNull();
  });

  it("swaps a reversed range rather than returning nothing", () => {
    // min above max would match zero rows and read as "the data is empty".
    const f = parse("amount_min=50000&amount_max=1000");
    expect(f.amountMin).toBe(1000);
    expect(f.amountMax).toBe(50000);
  });

  it("expands a year into a date range", () => {
    // "filter a specific year" is the common case, so it gets its own param
    // rather than making a reader type two dates.
    const f = parse("year=2025");
    expect(f.from).toBe("2025-01-01");
    expect(f.to).toBe("2025-12-31");
  });

  it("reads an explicit date range", () => {
    const f = parse("from=2024-03-01&to=2024-06-30");
    expect(f.from).toBe("2024-03-01");
    expect(f.to).toBe("2024-06-30");
  });

  it("ignores a malformed date", () => {
    expect(parse("from=March&to=2024-13-99").from).toBeNull();
    expect(parse("year=nineteen").from).toBeNull();
  });

  it("prefers an explicit range over a year, when both are given", () => {
    const f = parse("year=2025&from=2024-03-01&to=2024-06-30");
    expect(f.from).toBe("2024-03-01");
    expect(f.to).toBe("2024-06-30");
  });
});

describe("building the SQL", () => {
  it("adds nothing when no filter is set", () => {
    const { where, params } = contractFilterSql(parse(""));
    expect(where).toBe("");
    expect(params).toEqual({});
  });

  it("matches the expression the list displays, not the raw column", () => {
    // Brazil's agreement_type holds PNCP's {"id":n,"nome":"..."} object, so
    // filtering the bare column would match nothing while looking correct.
    const { where } = contractFilterSql(parse("agreement=Empenho"), {
      agreementExpr: "JSONExtractString(agreement_type, 'nome')",
    });
    expect(where).toContain("JSONExtractString(agreement_type, 'nome') IN");
  });

  it("parameterises the agreement values rather than interpolating them", () => {
    // These come from a URL, so they never reach SQL as text.
    const { where, params } = contractFilterSql(parse("agreement=Empenho"));
    expect(where).toContain("{agreement:Array(String)}");
    expect(where).not.toContain("Empenho");
    expect(params.agreement).toEqual(["Empenho"]);
  });

  it("filters amount on the USD column, the only comparable axis", () => {
    // A country can mix currencies -- Sweden carries SEK from UHM and EUR from
    // TED -- so a threshold in "the original amount" would compare unlike
    // numbers. The UI labels the unit.
    const { where, params } = contractFilterSql(parse("amount_min=10&amount_max=20"));
    expect(where).toContain("value_amount_usd >= {amount_min:Float64}");
    expect(where).toContain("value_amount_usd <= {amount_max:Float64}");
    expect(params).toMatchObject({ amount_min: 10, amount_max: 20 });
  });

  it("filters the date on publication_date", () => {
    const { where, params } = contractFilterSql(parse("year=2025"));
    expect(where).toContain("publication_date >= {from:String}");
    expect(where).toContain("publication_date <= {to:String}");
    expect(params).toMatchObject({ from: "2025-01-01", to: "2025-12-31" });
  });

  it("joins several filters with AND", () => {
    const { where } = contractFilterSql(parse("agreement=Empenho&amount_min=5&year=2025"));
    expect(where.startsWith(" AND ")).toBe(true);
    expect(where.split(" AND ").length - 1).toBe(4); // agreement, min, from, to
  });
});

describe("round-tripping to the URL", () => {
  it("puts every filter back in the query string", () => {
    // All filter state lives in the URL, so a filtered view is linkable and the
    // back button works.
    const qs = serializeContractFilters(parse("agreement=A&agreement=B&amount_min=5&from=2024-01-01&to=2024-02-01"));
    const back = parseContractFilters(new URLSearchParams(qs));
    expect(back.agreement).toEqual(["A", "B"]);
    expect(back.amountMin).toBe(5);
    expect(back.from).toBe("2024-01-01");
  });

  it("omits what is not set, so a clean view has a clean URL", () => {
    expect(serializeContractFilters(parse(""))).toBe("");
  });

  it("counts active filters for the button badge", () => {
    expect(contractFilterCount(parse(""))).toBe(0);
    // A range counts once however many of its two ends are set.
    expect(contractFilterCount(parse("amount_min=5&amount_max=9"))).toBe(1);
    expect(contractFilterCount(parse("agreement=A&agreement=B"))).toBe(1);
    expect(contractFilterCount(parse("agreement=A&amount_min=5&year=2025"))).toBe(3);
  });
});

describe("clearing", () => {
  it("EMPTY_CONTRACT_FILTERS serializes to nothing, so Clear returns a bare URL", () => {
    // The Clear button beside the trigger applies this: no query string at all,
    // rather than params left behind set to empty values.
    expect(serializeContractFilters(EMPTY_CONTRACT_FILTERS)).toBe("");
    expect(contractFilterCount(EMPTY_CONTRACT_FILTERS)).toBe(0);
  });

  it("clears every parameter, not only the ones the sheet shows", () => {
    // ee and no publish no agreement type, so their sheet renders no agreement
    // section -- Clear must still drop an agreement param a hand-edited URL set.
    const everything = parseContractFilters(
      new URLSearchParams("agreement=A&amount_min=1&amount_max=2&from=2024-01-01&to=2024-02-01"),
    );
    expect(contractFilterCount(everything)).toBe(3);
    expect(serializeContractFilters(EMPTY_CONTRACT_FILTERS)).toBe("");
  });
});
