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
 * The two SETS every status branch below is answered against, aggregated over
 * EVERY published row of the company. Deliberately its own statement rather
 * than something folded out of the two paged lists above: those carry a display
 * LIMIT, and a company past that many rows would drop a stamped correction's id
 * out of `applied_correction_ids` and silently flip it from applied to stale.
 * Nothing here is paged, and nothing needs to be -- one company publishes a
 * handful of addresses, so this is one row holding two small arrays.
 *
 * `key_evidence` maps each published `address_key` to the `evidence_set_hash`
 * of its row, and its KEY SET is this backoffice's reconstruction of the
 * resolution's produced set (Dagster's `by_key` in `apply_address_ledger`):
 *
 * - live rows are produced rows, trivially;
 * - a tombstoned row with a non-empty `correction_ids` was tombstoned by a
 *   REJECT -- `apply_address_ledger` runs before `with_set_replacement`, so a
 *   rejected key stays in the produced set as `is_current = false` and keeps
 *   the ids that decided it. It is still a row corrections are compared
 *   against, so its hash still counts;
 * - a tombstoned row with an EMPTY `correction_ids` is a disappearance
 *   tombstone: `with_set_replacement` clears the ids exactly because the key
 *   left the produced set. Its hash must not count, or a correction with
 *   nowhere to land would look fresh for ever.
 *
 * One row per key: the final is a ReplacingMergeTree ordered by
 * (company_id, address_key), so FINAL leaves each key its newest version and
 * the CAST to Map can never see a duplicate.
 */
export const ADDRESS_STATUS_INPUTS_SQL = `SELECT
  groupUniqArrayArray(arrayMap(x -> toString(x), a.correction_ids)) AS applied_correction_ids,
  CAST(
    groupArrayIf(
      (toString(a.address_key), toString(a.evidence_set_hash)),
      a.is_current OR notEmpty(a.correction_ids)
    ),
    'Map(String, String)'
  ) AS key_evidence
FROM corpscout.se_company_address AS a FINAL
WHERE a.company_id = {companyId:String}`;

/**
 * The company's correction ledger, with each row's standing against what is
 * published now. Every branch mirrors Dagster's `apply_address_ledger`
 * (se_company/address_rules.py), which is the authority -- this statement only
 * reports what that function will decide on its next run.
 *
 * The comparison is PER ADDRESS KEY. `apply_address_ledger` looks a
 * correction's `address_key` up in the produced set and compares its
 * `evidence_hash` against THAT row's `evidence_set_hash`; a correction naming
 * key A while carrying key B's hash is stale there, and would read "pending"
 * here if the hash were merely looked for somewhere in the company's set.
 * `{keyEvidence}` is that per-key lookup, so `mapContains` answers "did the
 * resolution produce this key" and the map index answers "is this the hash of
 * the row it names".
 *
 * `is_applied` has two branches, and the second is not an optimisation. Dagster
 * stamps a correction's id onto the row it decided, but a `reject_address`
 * naming a key the resolution did not produce has NO row to stamp:
 * address_rules.py skips it without recording it stale (ruling A11). Reading
 * membership of `correction_ids` alone would then leave such a reject "pending"
 * for ever, so the absence of its key from the produced set IS the applied
 * signal.
 *
 * `is_stale` is a live, NOT-applied correction of a kind the ledger knows,
 * naming a key, whose evidence has moved on -- either the key is not in the
 * produced set at all (`apply_address_ledger`: "the text had nowhere to land")
 * or the row it names now hashes to something else. The zero hash is undo's own
 * marker and is never compared. Applied is checked first because a decision
 * that landed is not waiting for anything. The kind list is `ADDRESS_KIND_ORDER`:
 * `effective_ledger` drops every other kind before staleness is ever considered,
 * and the ledger table has no CHECK on `correction_kind`, so a row written
 * outside this backoffice can carry one.
 *
 * Two things this SQL cannot mirror, both of which err toward "pending" and are
 * corrected by the next run's own verdict:
 *
 * - Dagster compares against the hash of the artifacts it is resolving NOW,
 *   while the map holds the hash of the row last PUBLISHED. A correction
 *   written after the artifacts moved but before the next resolve reads pending
 *   here and stale there.
 * - a reject-tombstoned row whose key the sources have since stopped carrying
 *   is never republished (`with_set_replacement` only tombstones rows that were
 *   CURRENT), so it freezes: it keeps both its old hash and every id stamped on
 *   it. Two things follow. A correction naming that key reads pending here
 *   where Dagster, no longer producing the key, calls it stale. And a
 *   correction that HAD been applied to the key before the reject -- an
 *   `override_field` whose id still rides the frozen tombstone next to the
 *   reject's -- reads applied here, where `apply_address_ledger` now returns it
 *   as stale: a stamped id is this page's applied signal, and nothing clears it
 *   from a row that is never rewritten.
 *
 * The first divergence, and the first half of the second, err toward "pending",
 * which only ever means "Dagster has not spoken about this yet". The second
 * half does not: it shows a decision as landed after the pipeline stopped
 * applying it, and is the one shape where this page and the pipeline disagree
 * about a verdict rather than about its timing. It is also inert -- the row is
 * frozen either way, so only the pipeline's own stale log and metric ever see
 * the other answer -- but "pending is always the safe direction" is not true of
 * it, and a reader should not be told that it is.
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
      AND NOT mapContains({keyEvidence:Map(String, String)}, address_key)
    )
  ) AS is_applied,
  toUInt8(
    is_current
    AND NOT is_applied
    AND address_key != ''
    AND c.correction_kind IN ('reject_address', 'override_field')
    AND toString(c.evidence_hash) != {zeroHash:String}
    AND (
      NOT mapContains({keyEvidence:Map(String, String)}, address_key)
      OR {keyEvidence:Map(String, String)}[address_key] != toString(c.evidence_hash)
    )
  ) AS is_stale
FROM corpscout.se_company_address_correction AS c
WHERE c.company_id = {companyId:String}
ORDER BY c.created_at DESC, c.correction_id DESC
LIMIT 200`;

/** One row, whatever the company has published: what ADDRESS_STATUS_INPUTS_SQL
 * returns. `key_evidence` arrives as a plain object because ClickHouse renders
 * a Map as a JSON object, and goes back out as one -- the client formats an
 * object query parameter as ClickHouse's own `{'k':'v'}` map literal. */
interface SeCompanyAddressStatusInputs {
  applied_correction_ids: string[];
  key_evidence: Record<string, string>;
}

/**
 * Every published address of one company -- live and tombstoned -- with the
 * correction ledger that decided them.
 *
 * Three reads go out together and the ledger follows, because its status
 * branches need the aggregates. The aggregates are NOT taken from `addresses`
 * and `removed`: those two carry a display LIMIT, so a company past it would
 * lose a stamped correction's id and report an applied decision as stale.
 *
 * A company with no published address returns empty lists rather than null: it
 * is a normal pipeline state (nothing resolved yet), and its ledger may still
 * carry a reject of an address that has since gone. The aggregate has no
 * GROUP BY, so it answers with one row of empties rather than none -- the
 * fallback below is belt and braces.
 */
export async function loadSeCompanyAddresses(
  companyId: string,
): Promise<SeCompanyAddressDetail> {
  const [addresses, removed, statusInputs] = await Promise.all([
    chQuery<SeCompanyAddressRow>(ADDRESSES_SQL, { companyId }),
    chQuery<SeCompanyAddressRow>(REMOVED_SQL, { companyId }),
    chQuery<SeCompanyAddressStatusInputs>(ADDRESS_STATUS_INPUTS_SQL, { companyId }),
  ]);
  const inputs = statusInputs[0];
  const corrections = await chQuery<SeCompanyAddressCorrectionRow>(CORRECTIONS_SQL, {
    companyId,
    zeroHash: ZERO_EVIDENCE_HASH,
    keyEvidence: inputs?.key_evidence ?? {},
    appliedIds: inputs?.applied_correction_ids ?? [],
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
