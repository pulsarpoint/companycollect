import { describe, expect, it } from "vitest";

import {
  CONTRACT_COLUMNS,
  contractColumnLabel,
  defaultContractColumns,
  parseContractColumns,
  serializeContractColumns,
  type ContractColumnId,
} from "./contract-columns";

const ALL: ContractColumnId[] = CONTRACT_COLUMNS.map((c) => c.id);
// What a TED-covered country offers: CPV, no agreement type.
const EE: ContractColumnId[] = ALL.filter((id) => id !== "agreement_type");
// What Brazil offers: agreement type, no CPV.
const BR: ContractColumnId[] = ALL.filter((id) => id !== "cpv");

describe("defaultContractColumns", () => {
  it("shows every column the country actually publishes", () => {
    expect(defaultContractColumns(BR)).toContain("agreement_type");
    expect(defaultContractColumns(BR)).not.toContain("cpv");
    expect(defaultContractColumns(EE)).toContain("cpv");
    expect(defaultContractColumns(EE)).not.toContain("agreement_type");
  });

  it("keeps the canonical order rather than the order availability arrived in", () => {
    const shuffled: ContractColumnId[] = ["source", "date", "title"];
    expect(defaultContractColumns(shuffled)).toEqual(["date", "title", "source"]);
  });
});

describe("parseContractColumns", () => {
  it("falls back to the default set when the URL says nothing", () => {
    expect(parseContractColumns(new URLSearchParams(), EE)).toEqual(
      defaultContractColumns(EE),
    );
  });

  it("honours an explicit selection", () => {
    const params = new URLSearchParams("cols=date,cpv");
    expect(parseContractColumns(params, EE)).toEqual(["date", "title", "cpv"]);
  });

  it("always keeps the locked column, which is the only link to the contract", () => {
    // Without `title` a reader can see rows and reach none of them.
    const params = new URLSearchParams("cols=date");
    expect(parseContractColumns(params, EE)).toContain("title");
  });

  it("drops a column the country does not publish", () => {
    // A hand-edited or shared URL must not add an always-empty column.
    const params = new URLSearchParams("cols=date,agreement_type");
    expect(parseContractColumns(params, EE)).not.toContain("agreement_type");
  });

  it("ignores unknown ids instead of throwing on a hand-edited URL", () => {
    const params = new URLSearchParams("cols=date,nonsense,cpv");
    expect(parseContractColumns(params, EE)).toEqual(["date", "title", "cpv"]);
  });

  it("treats an empty cols param as 'everything off', not as absent", () => {
    // Unticking every box is a real choice and must survive a reload.
    expect(parseContractColumns(new URLSearchParams("cols="), EE)).toEqual(["title"]);
  });

  it("returns the canonical order however the URL ordered them", () => {
    const params = new URLSearchParams("cols=source,date,buyer");
    expect(parseContractColumns(params, EE)).toEqual(["date", "buyer", "title", "source"]);
  });
});

describe("serializeContractColumns", () => {
  it("writes nothing when the selection is the country's default", () => {
    // Keeps the URL clean for the case nobody customised anything.
    expect(serializeContractColumns(defaultContractColumns(EE), EE)).toBeNull();
  });

  it("writes the selection when it differs from the default", () => {
    expect(serializeContractColumns(["date", "title", "cpv"], EE)).toBe("date,title,cpv");
  });

  it("round-trips through parse", () => {
    const chosen: ContractColumnId[] = ["date", "title", "amount_usd", "cpv"];
    const serialized = serializeContractColumns(chosen, EE)!;
    expect(parseContractColumns(new URLSearchParams(`cols=${serialized}`), EE)).toEqual(
      chosen,
    );
  });
});

describe("contractColumnLabel", () => {
  it("names every column", () => {
    for (const id of ALL) expect(contractColumnLabel(id)).not.toBe("");
  });
});
