import { describe, expect, it, type TestContext } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry, type CountryConfig } from "~/lib/countries";
import { FACET_COLUMN, filterableFacetKeys } from "~/lib/filters";

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
 *
 * FRESHNESS PREFLIGHT: several source registers rebuild on a schedule that
 * can land AFTER the companies_all 07:15 cron (sk's Monday 07:00 swap
 * landing later, se's Monday 06:15 overrunning, fr's 6th 07:15, gb's 7th
 * 07:30, cz's 17th 07:45). When that happens, today's companies_all leg for
 * that country is built from YESTERDAY's source snapshot, so exact
 * count/field parity fails with ZERO spec drift — a benign calendar false
 * alarm, not a bug. `preflightOrSkip` below compares `companies_all`'s
 * per-country `resolved_at` (set to `now64(3)` at INSERT time — i.e. the
 * leg's build timestamp) against the source `companiesTable`'s own
 * `max(resolved_at)`, and SKIPS (with a loud `console.warn` naming both
 * timestamps) when the source is newer. For no/fi/br (the countries whose
 * companies table carries resolved_at) a parity failure WITHOUT that
 * warning is real drift — treat it as a bug. The other seven countries
 * have no freshness signal, so a benign post-build source refresh fails
 * WITHOUT any warning there: check the source's register schedule (see
 * README "companies_all") before treating those as drift.
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

// ---------- Freshness preflight (see module doc comment above) ----------

type Freshness =
  | { status: "fresh" }
  | { status: "no-signal" }
  | { status: "stale"; companiesAllAt: string; sourceAt: string };

/**
 * Compares `companies_all`'s per-country build timestamp against the
 * source `companiesTable`'s own `max(resolved_at)`.
 *
 * Only `no_companies`/`fi_companies`/`br_companies` carry a `resolved_at`
 * column today (verified live via `system.columns`, 2026-07-18) —
 * se/ee/lv/gb/fr/cz/sk's companiesTables do not, so there is no per-table
 * freshness signal available for them: `checkFreshness` returns
 * `"no-signal"` and the caller runs parity normally (unprotected, same as
 * before this preflight existed) for those countries.
 */
async function checkFreshness(country: CountryConfig): Promise<Freshness> {
  let sourceAt: string | null;
  try {
    const [row] = await chQuery<{ m: string | null }>(
      `SELECT max(resolved_at) AS m FROM ${country.companiesTable}`,
    );
    sourceAt = row?.m ?? null;
  } catch {
    // Column doesn't exist on this country's companiesTable.
    return { status: "no-signal" };
  }
  if (!sourceAt) return { status: "no-signal" };

  const [allRow] = await chQuery<{ m: string | null }>(
    `SELECT max(resolved_at) AS m FROM ${COMPANIES_ALL} WHERE country_code = {code:String}`,
    { code: country.code },
  );
  const companiesAllAt = allRow?.m ?? null;
  if (!companiesAllAt) return { status: "no-signal" };

  // Both values come from ClickHouse DateTime64 in "YYYY-MM-DD HH:MM:SS.fff"
  // form, which sorts lexically the same as chronologically -- no Date
  // parsing needed (and safer: that format isn't reliably parsed by `new
  // Date()` across engines).
  if (sourceAt > companiesAllAt) {
    return { status: "stale", companiesAllAt, sourceAt };
  }
  return { status: "fresh" };
}

/**
 * Runs the freshness preflight for `country` and, if the source rebuilt
 * after today's companies_all build, warns loudly (naming both timestamps)
 * and skips the calling test via vitest's `context.skip()` instead of
 * letting count/field parity fail on a benign calendar false alarm. When no
 * freshness signal is available at all, warns which table lacked
 * `resolved_at` and lets the test proceed normally.
 */
async function preflightOrSkip(country: CountryConfig, ctx: TestContext): Promise<void> {
  const freshness = await checkFreshness(country);
  if (freshness.status === "no-signal") {
    console.warn(
      `[companies_all parity] ${country.code}: ${country.companiesTable} has no ` +
        "resolved_at column -- freshness preflight unavailable, running parity normally.",
    );
    return;
  }
  if (freshness.status === "stale") {
    const message =
      `[companies_all parity] ${country.code}: SKIPPING -- source ` +
      `${country.companiesTable} last resolved at ${freshness.sourceAt}, after ` +
      `companies_all's ${country.code} leg was built at ${freshness.companiesAllAt}. ` +
      "This is a benign calendar false alarm (the source register rebuilt after " +
      "today's 07:15 companies_all build); re-run after the next build. A parity " +
      "failure WITHOUT this warning is real drift.";
    console.warn(message);
    ctx.skip(message);
    return;
  }
}

describe("companies_all parity: row count", () => {
  it.for(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: companies_all count matches the source table count",
    { timeout: 60_000 },
    async ([_code, country], ctx) => {
      await preflightOrSkip(country, ctx);

      const [allRow] = await chQuery<{ total: string }>(
        `SELECT count() AS total FROM ${COMPANIES_ALL} WHERE country_code = {code:String}`,
        { code: country.code },
      );
      const [sourceRow] = await chQuery<{ total: string }>(
        `SELECT count() AS total FROM ${country.companiesTable}`,
      );
      expect(Number(allRow.total), country.code).toBe(Number(sourceRow.total));
    },
  );
});

describe("companies_all parity: status/legal_form/place/size", () => {
  it.for(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: sampled values match the registry expr; undefined keys carry ''",
    { timeout: 60_000 },
    async ([_code, country], ctx) => {
      await preflightOrSkip(country, ctx);

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
  );
});

const FINANCIALS_LATEST_COUNTRIES = COUNTRIES.filter((c) => c.financialsLatest);

describe("companies_all parity: financials", () => {
  it.for(FINANCIALS_LATEST_COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: up to 10 sampled has_financials rows match the country's financialsLatest table",
    { timeout: 30_000 },
    async ([_code, country], ctx) => {
      await preflightOrSkip(country, ctx);

      const rows = await chQuery<{
        id: string;
        revenue_usd: number | null;
        fiscal_year: number | null;
      }>(
        `SELECT company_id AS id, revenue_usd, fiscal_year
         FROM ${COMPANIES_ALL}
         WHERE country_code = {code:String} AND has_financials = 1
         ORDER BY company_id LIMIT 10`,
        { code: country.code },
      );
      // Kept as a LIMIT-10 sample (same shape as the original NO-only test)
      // rather than requiring exactly 10: sk_company_financials_latest has
      // just 1 row live today (pipeline still catching up, same class of
      // gap as the se/sk "no financial metrics materialized yet" detail-page
      // caveat in README.md) -- a hard ===10 would permanently fail for any
      // country whose financials pipeline hasn't fully landed yet.
      expect(rows.length, country.code).toBeGreaterThan(0);
      const ids = rows.map((r) => r.id);

      // The sampled `id`s are companies_all's public id (== companiesTable's
      // idColumn), but the financialsLatest table's OWN `company_id` isn't
      // always the same value space -- se_company_financials_latest keys on
      // se_companies' internal `company_id`, distinct from the public
      // `registration_number` (the "SE dual-id-space trap"). Joining through
      // companiesTable via `financialsLatest.companyKeyExpr` (mirroring
      // sql.py's own `fin.company_id = toString({financials_join_key})`
      // join) resolves the correct key for every country generically,
      // instead of assuming idColumn IS the financials join key.
      const { table: finTable, companyKeyExpr } = country.financialsLatest!;
      const finRows = await chQuery<{
        id: string;
        revenue_usd: number | null;
        fiscal_year: number | null;
      }>(
        `SELECT toString(c.${country.idColumn}) AS id,
                f.revenue_amount_usd AS revenue_usd,
                f.fiscal_year AS fiscal_year
         FROM ${country.companiesTable} AS c
         INNER JOIN ${finTable} AS f ON f.company_id = toString(c.${companyKeyExpr})
         WHERE toString(c.${country.idColumn}) IN {ids:Array(String)}`,
        { ids },
      );
      const finById = new Map(finRows.map((r) => [r.id, r]));

      for (const row of rows) {
        const fin = finById.get(row.id);
        expect(
          fin,
          `${country.code} company ${row.id} missing from ${finTable} (join key ${companyKeyExpr})`,
        ).toBeDefined();
        expect(row.revenue_usd, `${country.code} ${row.id} revenue_usd`).toBe(fin!.revenue_usd);
        expect(row.fiscal_year, `${country.code} ${row.id} fiscal_year`).toBe(fin!.fiscal_year);
      }
    },
  );
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

describe("FACET_COLUMN registry invariant", () => {
  it("every filterable column key across all COUNTRIES is a FACET_COLUMN key", () => {
    // A country registry can add a new `filterable: true` column without
    // anyone remembering to also add it to FACET_COLUMN -- since
    // FACET_COLUMN's keys double as the whitelist for the unified WHERE
    // clause (unified.server.ts), a missed key wouldn't error, it would
    // just silently no-op that facet everywhere. This is a pure registry
    // check (no ClickHouse), so it fails fast in typecheck/CI, not just in
    // the live parity sweep.
    const filterableKeys = new Set(
      COUNTRIES.flatMap((c) => c.columns.filter((col) => col.filterable).map((col) => col.key)),
    );
    expect(filterableKeys.size).toBeGreaterThan(0);

    const facetKeys = new Set(Object.keys(FACET_COLUMN));
    for (const key of filterableKeys) {
      expect(facetKeys.has(key), `FACET_COLUMN is missing filterable key "${key}"`).toBe(true);
    }
  });
});
