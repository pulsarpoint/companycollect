import { randomUUID } from "node:crypto";
import {
  chInsertSeCompanyAddressCorrections,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  SeAddressCorrectionValidationError,
  validateSeAddressCorrection,
  ZERO_EVIDENCE_HASH,
  type SeAddressCorrectionInput,
} from "~/lib/se-address-corrections";

export { ZERO_EVIDENCE_HASH };
const CORRECTION_ACTOR = "backoffice";

/**
 * One published address of one company: a row of corpscout.se_company_address
 * (migration 000307), which is the whole story -- the register text, the
 * geocode augmentation, the provenance and the applied corrections all travel
 * on it.
 *
 * Every field is text because the query collapses a Nullable miss to "" rather
 * than mapping it in TypeScript, so "absent" is one value ("") rather than
 * null, 0 and undefined.
 */
export interface SeCompanyAddressRow {
  /** sha256 of the normalized (type, care-of, street, postal, city, country)
   * tuple, computed in Dagster's address_rules.py. The subject of every
   * correction on this row. */
  address_key: string;
  address_type: string;
  care_of: string;
  street_address: string;
  normalized_address: string;
  postal_code: string;
  city: string;
  country_code: string;
  /** The shared-identity address this row's geocode came from; "" when the
   * address never reached the chain. */
  address_id: string;
  latitude: string;
  longitude: string;
  /** "" means the address never reached the geocoder at all. */
  geocode_status: string;
  geocoded_at: string;
  /** Every source that carried this address, in the pipeline's precedence order. */
  sources: string[];
  source_record_uids: string[];
  /** What a correction on this row has to echo back; the pipeline compares the
   * two to decide whether the decision still applies. */
  evidence_set_hash: string;
  /** The corrections Dagster actually applied to this row. Cleared on a
   * tombstone that a disappearance wrote, kept on one a reject wrote. */
  correction_ids: string[];
  resolved_at: string;
}

export interface SeCompanyAddressCorrectionRow {
  correction_id: string;
  correction_kind: string;
  payload: string;
  /** The address_key this correction decides, lifted out of the payload so the
   * page can group a correction under its address card. */
  address_key: string;
  evidence_hash: string;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  created_at: string;
  is_current: number;
  is_stale: number;
  is_applied: number;
}

export interface SeCompanyAddressDetail {
  /** The company's live addresses (is_current). */
  addresses: SeCompanyAddressRow[];
  /**
   * Tombstoned rows (is_current = false): rejected by a reviewer, or no longer
   * carried by any source. Ruling A8 -- a rejected address that vanished from
   * the page would take its correction with it, and the undo that brings it
   * back would be unreachable.
   */
  removed: SeCompanyAddressRow[];
  corrections: SeCompanyAddressCorrectionRow[];
}

/**
 * Every column of the final, aliased explicitly, all as text so "absent" is
 * one value. Shared by the live and the tombstone read so a column can never
 * be present in one list and missing from the other.
 *
 * NO JOINS, deliberately. This used to be a six-table LEFT JOIN chain
 * (se_company_addresses_current -> _display_current -> _members_current ->
 * _links_current -> se_addresses_current -> se_address_geocodes_current) with
 * a `has_*` flag per joined side, because ClickHouse fills a LEFT JOIN miss
 * with each column's TYPE DEFAULT and the page rendered a confidence of 0
 * taken on 1970-01-01. The datatype removed the reason for all of it: the
 * geocode is read once at resolve time and stored on the published row. The
 * old statement is in git history; do not re-add it.
 */
const ADDRESS_PROJECTION = `SELECT
  toString(a.address_key) AS address_key,
  toString(a.address_type) AS address_type,
  ifNull(a.care_of, '') AS care_of,
  ifNull(a.street_address, '') AS street_address,
  ifNull(a.normalized_address, '') AS normalized_address,
  ifNull(a.postal_code, '') AS postal_code,
  ifNull(a.city, '') AS city,
  ifNull(a.country_code, '') AS country_code,
  ifNull(toString(a.address_id), '') AS address_id,
  ifNull(toString(a.latitude), '') AS latitude,
  ifNull(toString(a.longitude), '') AS longitude,
  toString(a.geocode_status) AS geocode_status,
  ifNull(toString(a.geocoded_at), '') AS geocoded_at,
  a.sources AS sources,
  a.source_record_uids AS source_record_uids,
  toString(a.evidence_set_hash) AS evidence_set_hash,
  arrayMap(x -> toString(x), a.correction_ids) AS correction_ids,
  toString(a.resolved_at) AS resolved_at`;

/** FINAL is not decoration here: se_company_address is a ReplacingMergeTree on
 * resolved_at, so without it a re-resolved address comes back once per run. */
export const ADDRESSES_SQL = `${ADDRESS_PROJECTION}
FROM corpscout.se_company_address AS a FINAL
WHERE a.company_id = {companyId:String}
  AND a.is_current
ORDER BY a.address_type, a.address_key
LIMIT 100`;

/** The same rows with the flag inverted: what this company no longer has. */
export const REMOVED_SQL = `${ADDRESS_PROJECTION}
FROM corpscout.se_company_address AS a FINAL
WHERE a.company_id = {companyId:String}
  AND NOT a.is_current
ORDER BY a.address_type, a.address_key
LIMIT 100`;

/**
 * The company's correction ledger, with each row's standing against what is
 * published now.
 *
 * `is_applied` has two branches, and the second is not an optimisation. Dagster
 * stamps a correction's id onto the row it decided, but a `reject_address`
 * naming a key the resolution did not produce has NO row to stamp: address_rules.py
 * skips it without recording it stale (ruling A11). Reading membership of
 * `correction_ids` alone would then leave such a reject "pending" for ever, so
 * the absence of its key from the live set IS the applied signal.
 *
 * `is_stale` is a live, NOT-applied correction whose evidence hash matches no
 * live row of this company -- which covers both "the evidence moved on" and
 * "the address it named is gone". The zero hash is undo's own marker and is
 * never stale. Applied is checked first because a decision that landed is not
 * waiting for anything: only tombstoned rows keep the old hash, and counting
 * theirs would instead make a correction with nowhere to land look fresh for ever.
 */
export const CORRECTIONS_SQL = `WITH superseded AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_address_correction
  WHERE company_id = {companyId:String} AND supersedes_correction_id IS NOT NULL
)
SELECT
  toString(c.correction_id) AS correction_id,
  c.correction_kind AS correction_kind,
  c.payload AS payload,
  JSONExtractString(c.payload, 'address_key') AS address_key,
  toString(c.evidence_hash) AS evidence_hash,
  c.reason AS reason,
  c.decided_by AS decided_by,
  toString(c.supersedes_correction_id) AS supersedes_correction_id,
  toString(c.created_at) AS created_at,
  toUInt8(c.correction_id NOT IN (SELECT id FROM superseded)) AS is_current,
  toUInt8(
    has({appliedIds:Array(String)}, toString(c.correction_id))
    OR (
      c.correction_kind = 'reject_address'
      AND address_key != ''
      AND NOT has({liveAddressKeys:Array(String)}, address_key)
    )
  ) AS is_applied,
  toUInt8(
    is_current
    AND NOT is_applied
    AND toString(c.evidence_hash) != {zeroHash:String}
    AND NOT has({evidenceSetHashes:Array(String)}, toString(c.evidence_hash))
  ) AS is_stale
FROM corpscout.se_company_address_correction AS c
WHERE c.company_id = {companyId:String}
ORDER BY c.created_at DESC, c.correction_id DESC
LIMIT 200`;

/**
 * Every published address of one company -- live and tombstoned -- with the
 * correction ledger that decided them.
 *
 * A company with no published address returns empty lists rather than null: it
 * is a normal pipeline state (nothing resolved yet), and its ledger may still
 * carry a reject of an address that has since gone.
 */
export async function loadSeCompanyAddresses(
  companyId: string,
): Promise<SeCompanyAddressDetail> {
  const [addresses, removed] = await Promise.all([
    chQuery<SeCompanyAddressRow>(ADDRESSES_SQL, { companyId }),
    chQuery<SeCompanyAddressRow>(REMOVED_SQL, { companyId }),
  ]);
  const corrections = await chQuery<SeCompanyAddressCorrectionRow>(CORRECTIONS_SQL, {
    companyId,
    zeroHash: ZERO_EVIDENCE_HASH,
    evidenceSetHashes: addresses.map((row) => row.evidence_set_hash),
    liveAddressKeys: addresses.map((row) => row.address_key),
    // Both lists: a reject's id lives on the row it tombstoned, so reading
    // only the live rows would lose it.
    appliedIds: [...addresses, ...removed].flatMap((row) => row.correction_ids),
  });
  return { addresses, removed, corrections };
}

function correctionTimestamp(): string {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

/**
 * Appends one reviewer decision to the ledger. Except for `undo` (which names
 * a correction, not a row), the named address is re-read first: the page may
 * have been open while Dagster republished it, and a decision echoing a hash
 * that has moved would be dropped as stale on the next run without ever
 * telling the reviewer.
 */
export async function appendSeCompanyAddressCorrection(
  input: SeAddressCorrectionInput,
): Promise<{ correctionId: string }> {
  const draft = validateSeAddressCorrection(input);
  if (draft.correction_kind !== "undo") {
    const addressKey = JSON.parse(draft.payload).address_key as string;
    const [current] = await chQuery<{ evidence_set_hash: string }>(
      `SELECT toString(a.evidence_set_hash) AS evidence_set_hash
       FROM corpscout.se_company_address AS a FINAL
       WHERE a.company_id = {companyId:String}
         AND a.address_key = {addressKey:String}
         AND a.is_current
       LIMIT 1`,
      { companyId: draft.company_id, addressKey },
    );
    if (!current) {
      throw new SeAddressCorrectionValidationError("This address is not published.");
    }
    if (current.evidence_set_hash !== draft.evidence_hash) {
      throw new SeAddressCorrectionValidationError(
        "The evidence changed while you were reviewing. Reload and decide again.",
      );
    }
  }
  const correctionId = randomUUID();
  await chInsertSeCompanyAddressCorrections([
    {
      correction_id: correctionId,
      ...draft,
      decided_by: CORRECTION_ACTOR,
      created_at: correctionTimestamp(),
    },
  ]);
  return { correctionId };
}
