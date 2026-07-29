import { chQuery } from "~/lib/clickhouse.server";
import { PAGE_SIZES, type CountryConfig, type SortDir } from "~/lib/countries";

/** Every country's contracts live in a view named <cc>_government_contracts
 * with an identical shape, so these queries are built from the country code
 * rather than configured per country.
 *
 * Which countries have one is asked of the database rather than listed here,
 * for the same reason the cross-country view uses merge() over the name pattern
 * instead of a UNION ALL: adding a country should be one CREATE VIEW and
 * nothing else. A hardcoded list here would quietly make that untrue, and the
 * failure mode is a country having contracts that the UI refuses to show. */
const CONTRACTS_VIEW_PATTERN = "^[a-z]{2}_government_contracts$";

let contractCountries: Promise<Set<string>> | null = null;

function loadContractCountries(): Promise<Set<string>> {
  contractCountries ??= chQuery<{ name: string }>(
    `SELECT name FROM system.tables
     WHERE database = currentDatabase()
       AND match(name, {pattern:String})`,
    { pattern: CONTRACTS_VIEW_PATTERN },
  ).then((rows) => new Set(rows.map((r) => r.name.slice(0, 2))));
  return contractCountries;
}

export async function hasContracts(country: CountryConfig): Promise<boolean> {
  return (await loadContractCountries()).has(country.code);
}

/** One contract in the paginated country list. A contract, not a winner row:
 * the view has one row per (source, contract, winner), grouped down to one
 * row per contract here. */
export interface CountryContractListRow {
  /** Grouping key, and the id used in the detail URL. */
  contract_ref: string;
  contract_date: string;
  buyer_name: string;
  title: string;
  agreement_type: string;
  /** The winner shown. Every field on this row (including this one) is read
   * from whichever (contract, source) group carries the largest USD amount —
   * so the winner shown always pairs with the amount shown, never a mix of
   * two sources. */
  winner_name: string;
  /** Additional winners beyond the one shown, from that same source; 0 for a
   * single-winner contract (all of Brazil's PNCP awards, for instance). */
  winner_extra_count: number;
  /** Summed across winners within the winning source — never across sources,
   * which would double count a contract published in both a national
   * register and TED. */
  amount_original: number | null;
  currency: string;
  amount_usd: number | null;
  source_url: string;
}

export type ContractSortKey = "date" | "buyer" | "winner" | "amount_original" | "amount_usd";

export interface CountryContractsPage {
  rows: CountryContractListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: ContractSortKey;
  dir: SortDir;
}

/** One (source, winner) row of a single contract. */
export interface ContractWinnerRow {
  source: string;
  source_notice_id: string;
  source_lot_id: string;
  source_url: string;
  contract_date: string;
  buyer_name: string;
  buyer_id: string;
  title: string;
  agreement_type: string;
  cpv_code: string;
  company_id: string;
  winner_name: string;
  amount_original: number | null;
  amount_usd: number | null;
  currency: string;
  notice_amount_original: number | null;
  notice_amount_usd: number | null;
  notice_currency: string;
  directive_governed: string;
  /** The register field each figure was read from, so a displayed number can
   * be checked against the source. Empty exactly when the figure is null. */
  value_source_field: string;
  notice_value_source_field: string;
}

/** A raw row from a source table, rendered generically -- the columns differ
 * per source and that is the point. */
export type SourceRecord = Record<string, unknown>;

export interface ContractDetail {
  contract_ref: string;
  rows: ContractWinnerRow[];
  sourceRecords: { source: string; notice: string; fields: SourceRecord }[];
}

/** contract_key identifies a contract across sources and is empty when the
 * source publishes nothing to match on, in which case the contract is only
 * itself. */
const REF = "if(contract_key != '', contract_key, contract_id)";

/** Sort keys are an allow-list into SQL column names, never user input passed
 * through — the same pattern `getSortColumn` uses for the company list. */
const CONTRACT_SORT_COLUMNS: Record<ContractSortKey, string> = {
  date: "contract_date",
  buyer: "buyer_name",
  winner: "winner_name",
  amount_original: "amount_original",
  amount_usd: "amount_usd",
};

function isContractSortKey(value: string | null): value is ContractSortKey {
  return value !== null && Object.hasOwn(CONTRACT_SORT_COLUMNS, value);
}

/**
 * One page of a country's government contracts, one row per contract rather
 * than per (source, winner) — mirrors the grouping `getContractDetail` later
 * expands back out, but paginated and sortable server-side instead of capped
 * at a flat LIMIT. Brazil alone has 112,943 contracts; the old LIMIT 200 hid
 * all but the newest sliver of them.
 *
 * Within a contract, every displayed field (buyer, title, winner, amount, ...)
 * is read from the ONE (source) group with the largest USD amount, so the
 * winner shown always pairs with the amount shown — never a winner from one
 * register next to an amount from another. Brazil is single-source and
 * single-winner throughout (verified: 112,943 rows, 112,943 distinct
 * contract_id), so this reduces to "the row" there; TED-covered countries
 * can have several winners on one contract, surfaced as `winner_extra_count`.
 */
export async function getCountryContractsPage(
  country: CountryConfig,
  opts: {
    page?: number;
    pageSize?: number;
    sort?: string | null;
    dir?: string | null;
  } = {},
): Promise<CountryContractsPage> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as (typeof PAGE_SIZES)[number])
    ? (opts.pageSize as number)
    : 50;
  const sort = isContractSortKey(opts.sort ?? null) ? (opts.sort as ContractSortKey) : "date";
  const dir: SortDir = opts.dir === "asc" ? "asc" : "desc";

  if (!(await hasContracts(country))) {
    return { rows: [], total: 0, page: 1, pageSize, sort, dir };
  }

  const sortColumn = CONTRACT_SORT_COLUMNS[sort];

  // Count first (same reason as searchCompanies): the requested page can be
  // stale once contracts are added, so it is clamped to the real range below
  // rather than trusted.
  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM (
       SELECT ${REF} AS contract_ref FROM ${country.code}_government_contracts GROUP BY contract_ref
     )`,
  );
  const total = Number(countRows[0].total);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(Math.max(1, Math.trunc(opts.page ?? 1) || 1), lastPage);

  const rows = await chQuery<
    Omit<CountryContractListRow, "winner_extra_count"> & { winner_extra_count: number | string }
  >(
    `SELECT
       contract_ref,
       contract_date,
       buyer_name,
       title,
       agreement_type,
       winner_name,
       toUInt32(winner_count_primary) - 1 AS winner_extra_count,
       toFloat64(amount_original) AS amount_original,
       currency,
       nullIf(amount_usd, -1.0) AS amount_usd,
       source_url
     FROM (
       SELECT
         contract_ref,
         coalesce(toString(argMax(source_date, priority)), '') AS contract_date,
         argMax(buyer_name_in, priority) AS buyer_name,
         argMax(title_in, priority) AS title,
         argMax(agreement_type_in, priority) AS agreement_type,
         argMax(source_url_in, priority) AS source_url,
         argMax(winner_name_in, priority) AS winner_name,
         argMax(winner_count, priority) AS winner_count_primary,
         argMax(amount_original_in, priority) AS amount_original,
         argMax(currency_in, priority) AS currency,
         max(priority) AS amount_usd
       FROM (
         SELECT
           ${REF} AS contract_ref,
           source_slug AS source,
           max(publication_date) AS source_date,
           any(buyer_name) AS buyer_name_in,
           any(title) AS title_in,
           -- PNCP (Brazil) publishes agreement_type as a raw {"id":N,"nome":"..."}
           -- blob; every other loaded source already publishes plain text.
           any(multiIf(startsWith(agreement_type, '{'),
             JSONExtractString(agreement_type, 'nome'), agreement_type)) AS agreement_type_in,
           any(source_url) AS source_url_in,
           argMin(if(winner_name != '', winner_name, company_id), source_winner_ordinal) AS winner_name_in,
           uniqExact(if(company_id != '', company_id, winner_name)) AS winner_count,
           sum(value_amount_original) AS amount_original_in,
           any(value_currency) AS currency_in,
           -- -1 sentinel distinguishes "no source reported a USD figure" from
           -- a genuine zero once max() below collapses the per-source values.
           coalesce(toFloat64(sum(value_amount_usd)), -1.0) AS priority
         FROM ${country.code}_government_contracts
         GROUP BY contract_ref, source
       )
       GROUP BY contract_ref
     )
     ORDER BY coalesce(toString(${sortColumn}), '') = '' ASC, ${sortColumn} ${dir === "asc" ? "ASC" : "DESC"}, contract_ref
     LIMIT {limit:UInt32} OFFSET {offset:UInt32}`,
    { limit: pageSize, offset: (page - 1) * pageSize },
  );

  return {
    rows: rows.map((r) => ({ ...r, winner_extra_count: Number(r.winner_extra_count) })),
    total,
    page,
    pageSize,
    sort,
    dir,
  };
}

/** SELECT * per source, so a contract shows whatever its register publishes
 * rather than the lowest common denominator. Keyed on the notice id the view
 * already carries. */
const SOURCE_RECORD_QUERIES: Record<string, string> = {
  sweden_uhm_procurement: `SELECT * FROM se_uhm_procurement_awards
     WHERE source_procurement_id = {notice:String} LIMIT 1`,
  ted_procurement: `SELECT * FROM ted_notices
     WHERE publication_number = {notice:String} LIMIT 1`,
  finland_hilma_procurement: `SELECT * FROM fi_hilma_notices
     WHERE notice_number = {notice:String} LIMIT 1`,
};

export async function getContractDetail(
  country: CountryConfig,
  ref: string,
): Promise<ContractDetail | null> {
  if (!(await hasContracts(country))) return null;

  const rows = await chQuery<ContractWinnerRow>(
    `SELECT
       source_slug AS source,
       source_notice_id,
       source_lot_id,
       source_url,
       coalesce(toString(publication_date), '') AS contract_date,
       buyer_name,
       buyer_id,
       title,
       agreement_type,
       cpv_code,
       company_id,
       winner_name,
       toFloat64(value_amount_original) AS amount_original,
       toFloat64(value_amount_usd) AS amount_usd,
       value_currency AS currency,
       toFloat64(notice_value_amount_original) AS notice_amount_original,
       toFloat64(notice_value_amount_usd) AS notice_amount_usd,
       notice_value_currency AS notice_currency,
       directive_governed,
       value_source_field,
       notice_value_source_field
     FROM ${country.code}_government_contracts
     WHERE ${REF} = {ref:String}
     ORDER BY source_slug, source_lot_id, source_winner_ordinal
     LIMIT 500`,
    { ref },
  );
  if (rows.length === 0) return null;

  // One raw record per (source, notice) the contract appears under.
  const wanted = new Map<string, { source: string; notice: string }>();
  for (const row of rows) {
    const key = `${row.source}:${row.source_notice_id}`;
    if (!wanted.has(key) && SOURCE_RECORD_QUERIES[row.source]) {
      wanted.set(key, { source: row.source, notice: row.source_notice_id });
    }
  }

  const sourceRecords = (
    await Promise.all(
      [...wanted.values()].map(async ({ source, notice }) => {
        const found = await chQuery<SourceRecord>(SOURCE_RECORD_QUERIES[source], {
          notice,
        });
        return found.length > 0 ? { source, notice, fields: found[0] } : null;
      }),
    )
  ).filter((r): r is { source: string; notice: string; fields: SourceRecord } => r !== null);

  return { contract_ref: ref, rows, sourceRecords };
}
