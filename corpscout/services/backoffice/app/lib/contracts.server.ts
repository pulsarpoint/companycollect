import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

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

/** One contract in the country list. A contract, not a winner row: the view has
 * one row per (source, contract, winner), and these are grouped. */
export interface CountryContractRow {
  /** Grouping key, and the id used in the detail URL. */
  contract_ref: string;
  contract_date: string;
  buyer_name: string;
  title: string;
  sources: string[];
  winner_count: number;
  /** Summed across winners within one source, then the largest source taken --
   * never summed across sources, which would double count a contract published
   * in both a national register and TED. */
  amount_usd: number | null;
  notice_amount_usd: number | null;
  /** "yes" when the EU procurement directives govern the contract, which means
   * it is also published in TED and TED carries an award amount. "no" means no
   * value exists in any register. Empty means the source does not say. */
  directive_governed: string;
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

export async function getCountryContracts(
  country: CountryConfig,
  { limit = 200 }: { limit?: number } = {},
): Promise<CountryContractRow[]> {
  if (!(await hasContracts(country))) return [];
  return chQuery<CountryContractRow>(
    `SELECT
       contract_ref,
       coalesce(toString(contract_date), '') AS contract_date,
       buyer_name,
       title,
       sources,
       winner_count,
       amount_usd,
       notice_amount_usd,
       directive_governed
     FROM (
       SELECT
         contract_ref,
         max(source_date) AS contract_date,
         argMax(buyer_name, source_rows) AS buyer_name,
         argMax(title, source_rows) AS title,
         arraySort(groupUniqArrayArray([source])) AS sources,
         max(source_winners) AS winner_count,
         max(source_amount_usd) AS amount_usd,
         max(source_notice_usd) AS notice_amount_usd,
         -- A yes from any source settles it: the contract is above the
         -- threshold, whatever the other sources leave unsaid.
         if(has(groupArray(source_directive), 'yes'), 'yes',
            if(has(groupArray(source_directive), 'no'), 'no', '')) AS directive_governed
       FROM (
         SELECT
           ${REF} AS contract_ref,
           source_slug AS source,
           max(publication_date) AS source_date,
           any(buyer_name) AS buyer_name,
           any(title) AS title,
           count() AS source_rows,
           uniqExact(if(company_id != '', company_id, winner_name)) AS source_winners,
           sum(value_amount_usd) AS source_amount_usd,
           max(notice_value_amount_usd) AS source_notice_usd,
           anyIf(directive_governed, directive_governed != '') AS source_directive
         FROM ${country.code}_government_contracts
         GROUP BY contract_ref, source
       )
       GROUP BY contract_ref
     )
     ORDER BY contract_date DESC, contract_ref
     LIMIT {limit:UInt32}`,
    { limit },
  );
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
