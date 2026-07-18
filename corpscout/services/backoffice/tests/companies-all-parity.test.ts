import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry, type CountryConfig } from "~/lib/countries";
import { filterableFacetKeys } from "~/lib/filters";

/**
 * Permanent live parity sweep for `companies_all`.
 *
 * The dagster build's per-country SQL
 * (`dagster_v3/src/dagster_v3/defs/companies_all/sql.py`) DUPLICATES this
 * TS registry's expressions by design (Python can't import the TS module).
 * Every comparison below is derived FROM `~/lib/countries` — never a
 * hand-listed per-country expression — so drift on EITHER side of that
 * duplication (a registry edit here, or a sql.py edit there) fails a test.
 * Runs against the real ClickHouse; keep timeouts generous.
 */

const COMPANIES_ALL = "companies_all";
const FILTER_KEYS = ["status", "legal_form", "place", "size"] as const;
type FilterKey = (typeof FILTER_KEYS)[number];

/** Filterable column keys (of the four this sweep checks) this country's registry actually declares. */
function definedFilterKeys(country: CountryConfig): FilterKey[] {
  const filterable = new Set(filterableFacetKeys(country));
  return FILTER_KEYS.filter((key) => filterable.has(key));
}

function exprFor(country: CountryConfig, key: FilterKey): string {
  return country.columns.find((c) => c.key === key)!.expr;
}

/**
 * Groups rows by `id`, returning each id's SORTED array of `v` values.
 *
 * For every country except sk, each id maps to exactly one row on both
 * sides, so this reduces to a strict single-value equality check. sk has
 * ~53k `ico` values shared by two registers (two source rows per id), and
 * `companies_all` carries the duplication through unchanged (the build has
 * no per-id dedup) — comparing the SORTED MULTISET per id tolerates that
 * known quirk without weakening the check for any other country, whose
 * groups are always singletons.
 */
function groupById(rows: { id: string; v: string }[]): Map<string, string[]> {
  const map = new Map<string, string[]>();
  for (const row of rows) {
    const arr = map.get(row.id) ?? [];
    arr.push(row.v);
    map.set(row.id, arr);
  }
  for (const arr of map.values()) arr.sort();
  return map;
}

describe("companies_all parity: row count", () => {
  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: companies_all count matches the source table count",
    async (_code, country) => {
      const [allRow] = await chQuery<{ total: string }>(
        `SELECT count() AS total FROM ${COMPANIES_ALL} WHERE country_code = {code:String}`,
        { code: country.code },
      );
      const [sourceRow] = await chQuery<{ total: string }>(
        `SELECT count() AS total FROM ${country.companiesTable}`,
      );
      expect(Number(allRow.total), country.code).toBe(Number(sourceRow.total));
    },
    60_000,
  );
});

describe("companies_all parity: status/legal_form/place/size", () => {
  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: sampled values match the registry expr; undefined keys carry ''",
    async (_code, country) => {
      const ids = (
        await chQuery<{ company_id: string }>(
          `SELECT company_id FROM ${COMPANIES_ALL}
           WHERE country_code = {code:String}
           ORDER BY company_id LIMIT 25`,
          { code: country.code },
        )
      ).map((r) => r.company_id);
      expect(ids.length, country.code).toBeGreaterThan(0);

      const allRows = await chQuery<{
        id: string;
        status: string;
        legal_form: string;
        place: string;
        size: string;
      }>(
        `SELECT company_id AS id, status, legal_form, place, size
         FROM ${COMPANIES_ALL}
         WHERE country_code = {code:String} AND company_id IN {ids:Array(String)}`,
        { code: country.code, ids },
      );
      expect(allRows.length, country.code).toBeGreaterThan(0);

      const defined = definedFilterKeys(country);

      // Keys this country's registry does NOT declare: companies_all must
      // carry '' for every sampled row, regardless of the source table.
      for (const key of FILTER_KEYS) {
        if (defined.includes(key)) continue;
        for (const row of allRows) {
          expect(row[key], `${country.code} ${key} (undefined) for id ${row.id}`).toBe("");
        }
      }

      // Keys this country DOES declare: companies_all values must match the
      // registry expr evaluated directly against the source table.
      for (const key of defined) {
        const sourceRows = await chQuery<{ id: string; v: string }>(
          `SELECT toString(${country.idColumn}) AS id, coalesce(toString(${exprFor(country, key)}), '') AS v
           FROM ${country.companiesTable}
           WHERE ${country.idColumn} IN {ids:Array(String)}`,
          { ids },
        );
        const sourceGrouped = groupById(sourceRows);
        const allGrouped = groupById(allRows.map((r) => ({ id: r.id, v: r[key] })));

        for (const id of new Set(ids)) {
          const allValues = allGrouped.get(id) ?? [];
          const sourceValues = sourceGrouped.get(id) ?? [];
          expect(allValues, `${country.code} ${key} for id ${id}`).toEqual(sourceValues);
        }
      }
    },
    60_000,
  );
});

describe("companies_all parity: financials (no)", () => {
  const no = getCountry("no")!;

  it("10 sampled has_financials rows match no_company_financials_latest", async () => {
    const rows = await chQuery<{
      id: string;
      revenue_usd: number | null;
      fiscal_year: number | null;
    }>(
      `SELECT company_id AS id, revenue_usd, fiscal_year
       FROM ${COMPANIES_ALL}
       WHERE country_code = 'no' AND has_financials = 1
       ORDER BY company_id LIMIT 10`,
    );
    expect(rows.length).toBe(10);
    const ids = rows.map((r) => r.id);

    const finTable = no.financialsLatest!.table;
    const finRows = await chQuery<{
      id: string;
      revenue_usd: number | null;
      fiscal_year: number | null;
    }>(
      `SELECT company_id AS id, revenue_amount_usd AS revenue_usd, fiscal_year
       FROM ${finTable}
       WHERE company_id IN {ids:Array(String)}`,
      { ids },
    );
    const finById = new Map(finRows.map((r) => [r.id, r]));

    for (const row of rows) {
      const fin = finById.get(row.id);
      expect(fin, `no company ${row.id} missing from ${finTable}`).toBeDefined();
      expect(row.revenue_usd, `no ${row.id} revenue_usd`).toBe(fin!.revenue_usd);
      expect(row.fiscal_year, `no ${row.id} fiscal_year`).toBe(fin!.fiscal_year);
    }
  }, 30_000);
});

describe("companies_all parity: industry (ee)", () => {
  const ee = getCountry("ee")!;

  it("10 sampled industry_code rows match the registry industryQuery", async () => {
    const rows = await chQuery<{ id: string; industry_code: string; industry_label: string }>(
      `SELECT company_id AS id, industry_code, industry_label
       FROM ${COMPANIES_ALL}
       WHERE country_code = 'ee' AND industry_code != ''
       ORDER BY company_id LIMIT 10`,
    );
    expect(rows.length).toBe(10);
    const ids = rows.map((r) => r.id);

    const industryRows = await chQuery<{
      company_id: string;
      industry_code: string;
      industry_label: string;
    }>(ee.industryQuery!, { ids });
    const byId = new Map(industryRows.map((r) => [r.company_id, r]));

    for (const row of rows) {
      const match = byId.get(row.id);
      expect(match, `ee company ${row.id} missing from industryQuery`).toBeDefined();
      expect(row.industry_code, `ee ${row.id} industry_code`).toBe(match!.industry_code);
      expect(row.industry_label, `ee ${row.id} industry_label`).toBe(match!.industry_label);
    }
  }, 30_000);
});
