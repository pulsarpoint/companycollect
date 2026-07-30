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

/**
 * The awards view is preferred where it exists.
 *
 * `<cc>_government_contracts` is COMPANY-keyed — company_id is its second column
 * and it ends `WHERE company_match_status = 'exact' AND company_id != ''` — which
 * is right for company pages but makes ~15,400 real awards unreachable from a
 * country's contracts list. `<cc>_government_contract_awards` answers the other
 * question: one row per award, matched or not, plus the supplier's published id
 * and why it did not match.
 *
 * Resolved from the database rather than assumed, for the same reason as the
 * pattern above, and so a country still renders from the company-keyed view
 * before its awards view is migrated. Countries are converted one at a time.
 */
const AWARDS_VIEW_PATTERN = "^[a-z]{2}_government_contract_awards$";

let awardsCountries: Promise<Set<string>> | null = null;

function loadAwardsCountries(): Promise<Set<string>> {
  awardsCountries ??= chQuery<{ name: string }>(
    `SELECT name FROM system.tables
     WHERE database = currentDatabase()
       AND match(name, {pattern:String})`,
    { pattern: AWARDS_VIEW_PATTERN },
  ).then((rows) => new Set(rows.map((r) => r.name.slice(0, 2))));
  return awardsCountries;
}

/** Which view to read, and whether it carries the supplier columns. */
async function contractsSource(
  country: CountryConfig,
): Promise<{ table: string; hasSupplierDetail: boolean }> {
  if ((await loadAwardsCountries()).has(country.code)) {
    return {
      table: `${country.code}_government_contract_awards`,
      hasSupplierDetail: true,
    };
  }
  return { table: `${country.code}_government_contracts`, hasSupplierDetail: false };
}

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
  /** The id the register published for the supplier shown, whether or not it
   * resolved to a company. Empty on a country still read from the company-keyed
   * view. Masked for display by `maskPersonalSupplierId`. */
  winner_registered_id: string;
  /** Why that supplier did not resolve to a company — 'exact' when it did.
   * Rendered by `supplierStatusLabel`, which keeps Foreign, Individual,
   * Unmatched and Unverified id apart. */
  winner_match_status: string;
  /** Total suppliers on the contract within the winning source, so the list can
   * say "1/3". 1 for a single-supplier contract (every Brazilian PNCP award). */
  supplier_count: number;
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
  /** The register's published id for this supplier, matched or not. Masked for
   * display by `maskPersonalSupplierId`. */
  winner_registered_id: string;
  /** Why it did not resolve to a company; 'exact' when it did. */
  winner_match_status: string;
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
 * can have several winners on one contract, surfaced as `supplier_count`.
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
  const source = await contractsSource(country);
  const supplierIdExpr = source.hasSupplierDetail ? "winner_registered_id" : "''";
  const supplierStatusExpr = source.hasSupplierDetail
    ? "winner_match_status"
    : "'exact'";
  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM (
       SELECT ${REF} AS contract_ref FROM ${source.table} GROUP BY contract_ref
     )`,
  );
  const total = Number(countRows[0].total);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(Math.max(1, Math.trunc(opts.page ?? 1) || 1), lastPage);

  const rows = await chQuery<
    Omit<CountryContractListRow, "supplier_count"> & { supplier_count: number | string }
  >(
    `SELECT
       contract_ref,
       contract_date,
       buyer_name,
       title,
       agreement_type,
       winner_name,
       winner_registered_id,
       winner_match_status,
       toUInt32(winner_count_primary) AS supplier_count,
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
         argMax(winner_registered_id_in, priority) AS winner_registered_id,
         argMax(winner_match_status_in, priority) AS winner_match_status,
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
           -- Alphabetically first, not source_winner_ordinal: that ordinal is
           -- just the order our parser happened to iterate the notice XML (it
           -- increments per tenderer), so it encodes no rank -- arbitrary but
           -- stable, which looks meaningful and is not. argMin over the same
           -- expression min() uses, so the name, its id and its status all
           -- describe ONE supplier rather than three different ones.
           min(if(winner_name != '', winner_name, company_id)) AS winner_name_in,
           argMin(${supplierIdExpr}, if(winner_name != '', winner_name, company_id))
             AS winner_registered_id_in,
           argMin(${supplierStatusExpr}, if(winner_name != '', winner_name, company_id))
             AS winner_match_status_in,
           uniqExact(if(company_id != '', company_id, winner_name)) AS winner_count,
           sum(value_amount_original) AS amount_original_in,
           any(value_currency) AS currency_in,
           -- -1 sentinel distinguishes "no source reported a USD figure" from
           -- a genuine zero once max() below collapses the per-source values.
           coalesce(toFloat64(sum(value_amount_usd)), -1.0) AS priority
         FROM ${source.table}
         GROUP BY contract_ref, source
       )
       GROUP BY contract_ref
     )
     ORDER BY coalesce(toString(${sortColumn}), '') = '' ASC, ${sortColumn} ${dir === "asc" ? "ASC" : "DESC"}, contract_ref
     LIMIT {limit:UInt32} OFFSET {offset:UInt32}`,
    { limit: pageSize, offset: (page - 1) * pageSize },
  );

  return {
    rows: rows.map((r) => ({ ...r, supplier_count: Number(r.supplier_count) })),
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
  // A source with no entry here renders no record at all, silently. Brazil was
  // missing, so its page showed the handful of unified-view columns while all
  // 42 stored PNCP fields sat unread. estonia_rhr_procurement and
  // norway_doffin_procurement are still missing for the same reason.
  // The *_translated view, not the base table: it adds objeto_contrato_en so the
  // page can lead with English. It reads br_pncp_contracts rather than
  // br_government_contracts, so the 3,283 contracts that view filters out
  // (natural persons, invalid supplier ids) still resolve here.
  // Requires migration 000215.
  brazil_pncp_procurement: `SELECT * FROM br_pncp_contracts_translated
     WHERE numero_controle_pncp = {notice:String} LIMIT 1`,
  sweden_uhm_procurement: `SELECT * FROM se_uhm_procurement_awards
     WHERE source_procurement_id = {notice:String} LIMIT 1`,
  ted_procurement: `SELECT * FROM ted_notices
     WHERE publication_number = {notice:String} LIMIT 1`,
  finland_hilma_procurement: `SELECT * FROM fi_hilma_notices
     WHERE notice_number = {notice:String} LIMIT 1`,
};

/**
 * A source record query may name a view that a target database has not migrated
 * to yet — the migration ledger and this app deploy independently, and
 * br_pncp_contracts_translated (000215) is exactly that case. Falling back to
 * the pre-migration query keeps the page working through the window instead of
 * turning it into a 500, and the richer view is picked up automatically once the
 * migration lands, with no code change to coordinate.
 *
 * Only "table does not exist" is swallowed. Any other ClickHouse error is a real
 * fault and must surface.
 */
const SOURCE_RECORD_FALLBACKS: Record<string, string> = {
  brazil_pncp_procurement: `SELECT * FROM br_pncp_contracts
     WHERE numero_controle_pncp = {notice:String} LIMIT 1`,
};

function isMissingTableError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /UNKNOWN_TABLE|Unknown table|doesn't exist|does not exist/i.test(message);
}

async function fetchSourceRecord(
  source: string,
  notice: string,
): Promise<SourceRecord | null> {
  try {
    const found = await chQuery<SourceRecord>(SOURCE_RECORD_QUERIES[source], { notice });
    return found.length > 0 ? found[0] : null;
  } catch (error) {
    const fallback = SOURCE_RECORD_FALLBACKS[source];
    if (!fallback || !isMissingTableError(error)) throw error;
    console.warn(
      `[contracts] ${source}: primary source-record view missing, using pre-migration query`,
    );
    const found = await chQuery<SourceRecord>(fallback, { notice });
    return found.length > 0 ? found[0] : null;
  }
}

export async function getContractDetail(
  country: CountryConfig,
  ref: string,
): Promise<ContractDetail | null> {
  if (!(await hasContracts(country))) return null;

  // The awards view where it exists, so the winners card shows every awarded
  // supplier including the ones that did not resolve to a company -- which the
  // card already renders correctly (bare name, no company link).
  const source = await contractsSource(country);
  const supplierIdExpr = source.hasSupplierDetail ? "winner_registered_id" : "''";
  const supplierStatusExpr = source.hasSupplierDetail ? "winner_match_status" : "'exact'";

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
       ${supplierIdExpr} AS winner_registered_id,
       ${supplierStatusExpr} AS winner_match_status,
       toFloat64(value_amount_original) AS amount_original,
       toFloat64(value_amount_usd) AS amount_usd,
       value_currency AS currency,
       toFloat64(notice_value_amount_original) AS notice_amount_original,
       toFloat64(notice_value_amount_usd) AS notice_amount_usd,
       notice_value_currency AS notice_currency,
       directive_governed,
       value_source_field,
       notice_value_source_field
     FROM ${source.table}
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
        const found = await fetchSourceRecord(source, notice);
        return found ? { source, notice, fields: found } : null;
      }),
    )
  ).filter((r): r is { source: string; notice: string; fields: SourceRecord } => r !== null);

  return { contract_ref: ref, rows, sourceRecords };
}
