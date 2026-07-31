import { describe, expect, test } from "vitest";
import {
  availableCompanyColumns,
  defaultCompanyColumns,
  parseCompanyColumns,
  serializeCompanyColumns,
} from "~/lib/company-columns";
import type { CountryConfig } from "~/lib/countries";

/** Brazil's shape: core plus two extras, and a place column of its own. */
const BR = {
  code: "br",
  industryQuery: "SELECT 1",
  columns: [
    { key: "id", label: "ID" },
    { key: "name", label: "Legal name" },
    { key: "trade_name", label: "Trade name" },
    { key: "size", label: "Size" },
    { key: "status", label: "Status" },
    { key: "registered", label: "Activity start" },
    { key: "place", label: "Municipality" },
    { key: "legal_form", label: "Legal form" },
  ],
} as unknown as CountryConfig;

/** Norway's: place comes from placeQuery, and it has a website column. */
const NO = {
  code: "no",
  industryQuery: "SELECT 1",
  placeQuery: "SELECT 1",
  columns: [
    { key: "id", label: "ID" },
    { key: "name", label: "Name" },
    { key: "website", label: "Website" },
    { key: "status", label: "Status" },
    { key: "registered", label: "Registered" },
    { key: "legal_form", label: "Legal form" },
  ],
} as unknown as CountryConfig;

describe("availableCompanyColumns", () => {
  test("offers industry and place even though neither is a declared column", () => {
    // Both are injected by the table rather than selected as expressions, so a
    // picker built from country.columns alone would silently omit them.
    const ids = availableCompanyColumns(NO).map((c) => c.id);
    expect(ids).toContain("industry");
    expect(ids).toContain("place");
  });

  test("does not offer place to a country that has neither column nor query", () => {
    const bare = { code: "xx", columns: [{ key: "name", label: "Name" }] } as unknown as CountryConfig;
    expect(availableCompanyColumns(bare).map((c) => c.id)).not.toContain("place");
  });

  test("offers a country's own extras", () => {
    expect(availableCompanyColumns(BR).map((c) => c.id)).toContain("trade_name");
    expect(availableCompanyColumns(NO).map((c) => c.id)).toContain("website");
  });

  test("orders columns the same way for every country", () => {
    // The core reads identically across countries -- that is the point of the
    // uniform set. Brazil declares legal_form last and place mid-list, and it
    // must still render in the canonical order.
    const core = (c: CountryConfig) =>
      availableCompanyColumns(c)
        .filter((x) => x.core)
        .map((x) => x.id);
    const shared = ["id", "name", "industry", "legal_form", "status", "registered", "place"];
    expect(core(BR)).toEqual(shared);
    // Norway additionally carries the data-availability column, because it is
    // the only country wired to flag sources so far. When the rest follow, the
    // two lists converge again.
    expect(core(NO)).toEqual([...shared, "data"]);
  });

  test("keeps the country's own label for a column", () => {
    // "Municipality" is what Brazil's register calls it, and the label is the
    // country's business even when the key is shared.
    const place = availableCompanyColumns(BR).find((c) => c.id === "place");
    expect(place?.label).toBe("Municipality");
  });
});

describe("defaultCompanyColumns", () => {
  test("is the core only, so every country's default list matches", () => {
    // Compared without the data column: it is offered only where flag sources
    // are configured, which today is Norway alone.
    const withoutData = (ids: string[]) => ids.filter((id) => id !== "data");
    expect(withoutData(defaultCompanyColumns(availableCompanyColumns(BR)))).toEqual(
      withoutData(defaultCompanyColumns(availableCompanyColumns(NO))),
    );
  });

  test("the data column appears only where a flag source exists", () => {
    expect(defaultCompanyColumns(availableCompanyColumns(NO))).toContain("data");
    expect(defaultCompanyColumns(availableCompanyColumns(BR))).not.toContain("data");
  });

  test("leaves a country's extras switched off until asked for", () => {
    expect(defaultCompanyColumns(availableCompanyColumns(BR))).not.toContain("trade_name");
    expect(defaultCompanyColumns(availableCompanyColumns(NO))).not.toContain("website");
  });
});

describe("parseCompanyColumns", () => {
  test("an absent param means the default", () => {
    expect(parseCompanyColumns(new URLSearchParams(), availableCompanyColumns(BR))).toEqual(defaultCompanyColumns(availableCompanyColumns(BR)));
  });

  test("a chosen set is honoured, in canonical order", () => {
    const got = parseCompanyColumns(new URLSearchParams("cols=size,name,trade_name"), availableCompanyColumns(BR));
    expect(got).toEqual(["name", "trade_name", "size"]);
  });

  test("name cannot be dropped, because it is the only way into a company", () => {
    expect(parseCompanyColumns(new URLSearchParams("cols=status"), availableCompanyColumns(BR))).toContain("name");
  });

  test("a column the country does not have is ignored, not rendered empty", () => {
    // A URL shared from Brazil naming trade_name must not add a permanently
    // blank column to Norway.
    expect(parseCompanyColumns(new URLSearchParams("cols=name,trade_name"), availableCompanyColumns(NO))).toEqual(["name"]);
  });

  test("an empty param is a real choice, not a missing one", () => {
    expect(parseCompanyColumns(new URLSearchParams("cols="), availableCompanyColumns(BR))).toEqual(["name"]);
  });
});

describe("serializeCompanyColumns", () => {
  test("the default serializes to null, so an untouched table keeps a clean URL", () => {
    expect(serializeCompanyColumns(defaultCompanyColumns(availableCompanyColumns(BR)), availableCompanyColumns(BR))).toBeNull();
  });

  test("a customised set round-trips", () => {
    const chosen = parseCompanyColumns(new URLSearchParams("cols=name,size"), availableCompanyColumns(BR));
    const raw = serializeCompanyColumns(chosen, availableCompanyColumns(BR))!;
    expect(parseCompanyColumns(new URLSearchParams(`cols=${raw}`), availableCompanyColumns(BR))).toEqual(chosen);
  });
});
