import { describe, expect, it } from "vitest";
import { buildSourceFilter, filterColumns } from "./procurements.server";

const TED_NOTICES = [
  "publication_number", "country_iso2", "publication_date", "notice_type",
  "buyer_name", "buyer_national_id", "total_value_amount_usd", "fx_source",
];
const DOFFIN = [
  "doffin_id", "country_code", "publication_date", "notice_type", "award_result",
  "buyer_name", "buyer_org_number", "winner_name", "winner_org_number",
  "value_amount_usd",
];
const TED_WINNERS = [
  "publication_number", "winner_name", "winner_national_id", "awarded_amount_usd",
];

describe("filterColumns", () => {
  it("discovers per-table filter columns", () => {
    const ted = filterColumns(TED_NOTICES);
    expect(ted).toEqual({
      date: "publication_date",
      country: "country_iso2",
      buyerName: "buyer_name",
      winnerName: null,
      winnerId: null,
      noticeType: "notice_type",
      awardResult: null,
      usdValue: "total_value_amount_usd",
    });
    const winners = filterColumns(TED_WINNERS);
    expect(winners.winnerName).toBe("winner_name");
    expect(winners.winnerId).toBe("winner_national_id");
    expect(winners.usdValue).toBe("awarded_amount_usd");
    expect(winners.buyerName).toBeNull();
  });
});

describe("buildSourceFilter", () => {
  it("builds clauses only for columns the table has, with bound params", () => {
    const { where, params } = buildSourceFilter(DOFFIN, {
      country: "NO",
      from: "2026-01-01",
      buyer: "kommune",
      winner: "consult",
      noticeType: "award",
      awardResult: "won",
      valueMin: 1000,
      valueMax: 500000,
    });
    expect(where).toEqual([
      "upper(country_code) = upper({country:String})",
      "publication_date >= toDate({from:String})",
      "positionCaseInsensitiveUTF8(buyer_name, {buyer:String}) > 0",
      "(positionCaseInsensitiveUTF8(winner_name, {winner:String}) > 0 OR winner_org_number = {winner:String})",
      "notice_type = {noticeType:String}",
      "award_result = {awardResult:String}",
      "value_amount_usd >= {valueMin:Float64}",
      "value_amount_usd <= {valueMax:Float64}",
    ]);
    expect(params).toMatchObject({
      country: "NO",
      from: "2026-01-01",
      buyer: "kommune",
      winner: "consult",
      noticeType: "award",
      awardResult: "won",
      valueMin: 1000,
      valueMax: 500000,
    });
  });

  it("ignores filters whose backing column is absent", () => {
    const { where } = buildSourceFilter(TED_WINNERS, {
      country: "SE",
      buyer: "city",
      noticeType: "award",
    });
    expect(where).toEqual([]);
  });

  it("ignores empty and non-finite values", () => {
    const { where } = buildSourceFilter(DOFFIN, {
      buyer: "",
      winner: "  ",
      valueMin: Number.NaN,
    });
    expect(where).toEqual([]);
  });
});
