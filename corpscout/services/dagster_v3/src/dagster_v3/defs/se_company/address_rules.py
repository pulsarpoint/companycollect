"""Deterministic merge rules for Swedish company addresses.

Pure functions only -- no ClickHouse, no network -- so every rule is a table test.
address.py wires these to the artifacts, the geocode chain and the ledger. There is no
model step anywhere in this datatype, so unlike info_rules there is nothing here that
defers a decision to a suggestion.

The one mechanism info does not have is SET REPLACEMENT: a company has several addresses,
so re-resolving it is not "write one row" but "write this set and tombstone whatever left
it" -- see with_set_replacement. resolve_company_addresses is the whole pipeline in the
one order that is correct, and is what the asset calls.
"""

import hashlib
import re
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.common import LedgerRow, effective_ledger
# _text is info_rules' own trim-to-None helper, imported rather than re-declared for the
# same reason ArtifactRow and evidence_set_hash_for are: a second copy drifts (this module's
# first draft folded 0 to None where info_rules folds it to "0"), and "what counts as empty
# text" has to mean one thing across the whole se_company layer.
from dagster_v3.defs.se_company.info_rules import ArtifactRow, _text, evidence_set_hash_for

# Bolagsverket is the registration authority for a company's postal address, so its text
# wins wherever both sources describe the same address. A source not named here sorts last,
# alphabetically -- a new artifact is never silently promoted above these two.
ADDRESS_SOURCE_PRIORITY = ("bolagsverket", "scb")
# The kinds this ledger knows. MEMBERSHIP is what this map decides: effective_ledger drops
# every kind it does not name, so an unknown correction is ignored instead of applied. The
# relative ranks are inert today -- reject_address writes only is_current and override_field
# only text fields, so the two commute and flipping the ranks changes nothing -- and are kept
# in the order a reader would expect if a future kind ever wrote a column another kind writes.
# Within one kind, later (by created_at) wins.
ADDRESS_KIND_ORDER = {"reject_address": 0, "override_field": 1}
# The text fields a reviewer may decide. address_type is NOT among them: it is part of
# address_key, so overriding it would move the row to a different identity -- reject the
# address and let the corrected one arrive from a source instead. The backoffice validator
# (app/lib/se-address-corrections.ts) keeps the same list; a key this list does not own is
# skipped silently rather than applied, so a drift between the two fails safe.
OVERRIDABLE_FIELDS = ("care_of", "street_address", "normalized_address",
                      "postal_code", "city", "country_code")
ZERO_HASH = "0" * 64
_WHITESPACE = re.compile(r"\s+")
_NON_DIGIT = re.compile(r"[^0-9]")

__all__ = ["ADDRESS_KIND_ORDER", "ADDRESS_SOURCE_PRIORITY", "OVERRIDABLE_FIELDS", "ZERO_HASH",
           "AddressOutcome", "GeocodeFact", "address_components", "address_key_for",
           "apply_address_ledger", "augment_with_geocodes", "evidence_set_hash_for",
           "merge_company_addresses", "resolve_company_addresses", "with_set_replacement"]


@dataclass(frozen=True)
class GeocodeFact:
    """What the shared-identity chain knows about one SOURCE observation's address.

    Keyed by that observation's ``address_fingerprint``: the chain runs
    ``se_company_addresses_current.address_fingerprint`` ->
    ``se_company_address_members_current.address_key`` -> ``canonical_address_key`` ->
    ``se_company_address_links_current.address_id`` -> ``se_address_geocodes``, whose
    current outcome per identity is a read rule rather than a row.
    ``has_geocode`` is the geocoder's hit flag, not "has coordinates": an address can be
    classified (unmatched, foreign, postal-box) without a point.
    """

    address_id: str
    latitude: float | None
    longitude: float | None
    geocode_status: str
    geocoded_at: datetime | None
    has_geocode: bool


@dataclass(frozen=True)
class AddressOutcome:
    """One published row: one address of one company.

    ``address_fingerprints`` is provenance the final table does not store -- it is the
    per-source key the geocode lookup needs, in the same order as ``sources``, and it is
    carried on the outcome only between the merge and the augmentation.
    """

    company_id: str
    address_key: str
    address_type: str
    care_of: str | None
    street_address: str | None
    normalized_address: str | None
    postal_code: str | None
    city: str | None
    country_code: str | None
    sources: tuple[str, ...]
    source_record_uids: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    address_fingerprints: tuple[str, ...] = ()
    address_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geocode_status: str = ""
    geocoded_at: datetime | None = None
    is_current: bool = True
    correction_ids: tuple[uuid.UUID, ...] = ()


def _norm(value: object) -> str:
    return _WHITESPACE.sub(" ", str(value or "").strip()).lower()


def _digits(value: object) -> str:
    return _NON_DIGIT.sub("", str(value or ""))


def address_components(values: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]:
    """The six normalized components an ``address_key`` is built from.

    ``(address_type, care_of, street, postal digits, city, country_code)``, each lowered,
    trimmed and whitespace-collapsed; the postal code keeps its digits only, so "111 22"
    and "111-22" are one address.

    ``care_of`` is INSIDE the key: mail addressed "c/o Anna" does not reach the same
    recipient as mail without it, so two rows that differ only there are two addresses,
    not one address one source spells more fully. (A source that leaves care_of empty
    where the other fills it therefore keeps its own row -- which is the honest outcome:
    the register that named a recipient and the register that did not disagree about the
    address, and neither text should be silently dropped into the other's row.)

    The street component is the pipeline's own ``normalized_address`` when the source has
    one and the raw ``street_address`` otherwise. ``normalized_address`` (migration 000265)
    is a MATERIALIZED fold of street + postal code + post town + country that already
    strips floor suffixes (" 3 tr") and SCB's foreign placeholders, so two sources that
    describe the same address agree on it even when their raw text differs -- which is
    exactly what the key has to see. It repeats the postal code and city that follow it in
    the tuple; that is harmless (the key is an identity, not a display) and worth the
    stability.
    """
    street = _text(values.get("normalized_address")) or _text(values.get("street_address"))
    return (
        _norm(values.get("address_type")),
        _norm(values.get("care_of")),
        _norm(street),
        _digits(values.get("postal_code")),
        _norm(values.get("city")),
        _norm(values.get("country_code")),
    )


def address_key_for(components: Sequence[str]) -> str:
    """sha256 of the components joined by newlines -- the final's ``address_key``.

    Computed here and nowhere else: no SQL expression mirrors it, so there is no second
    definition that can drift (unlike the artifacts' evidence_hash, which is the DDL's).
    """
    return hashlib.sha256("\n".join(components).encode()).hexdigest()


def _priority(source: str) -> tuple[int, str]:
    index = ADDRESS_SOURCE_PRIORITY.index(source) if source in ADDRESS_SOURCE_PRIORITY else len(
        ADDRESS_SOURCE_PRIORITY)
    return (index, source)


def _pick(group: Sequence[ArtifactRow], field: str) -> str | None:
    """The first non-empty value for ``field``, in source precedence order."""
    for row in group:
        value = _text(row.values.get(field))
        if value is not None:
            return value
    return None


def merge_company_addresses(company_id: str, rows: Sequence[ArtifactRow]) -> tuple[AddressOutcome, ...]:
    """Merge one company's artifact rows into one outcome per ``address_key``.

    ONE address per source: the newest row each source offers, by ``observed_at`` and then
    ``source_record_uid``. The artifacts are append-only and their uid is content-derived,
    so a FINAL read hands this function every address the source has EVER carried for the
    company -- a company that moved has an old uid and a new one, both "current" versions
    of different rows. Keeping the newest per (source, key) instead would publish both
    addresses forever and nothing would ever tombstone; keeping the newest per source is
    what the register actually says today. Both of today's sources carry exactly one
    address per company (the normalizer's ``address_rank = 1``), so this drops nothing a
    source is telling us -- it drops history.

    Rows that normalize to the same key are then one address: field values are copied from
    the highest-precedence source that offers each one, and every contributing source, uid
    and evidence hash is recorded. Rows that normalize differently stay separate addresses;
    with today's two sources that is the ordinary case, because Bolagsverket's 'postal' and
    SCB's 'visiting_or_postal' are different address types and the type is part of the key.

    A company with no artifact rows at all returns no outcomes -- and, in address.py, that
    is what tombstones its whole published set.
    """
    newest: dict[str, ArtifactRow] = {}
    for row in rows:
        seen = newest.get(row.source)
        if seen is None or (row.observed_at, row.source_record_uid) > (seen.observed_at, seen.source_record_uid):
            newest[row.source] = row

    grouped: dict[str, list[ArtifactRow]] = defaultdict(list)
    for row in newest.values():
        grouped[address_key_for(address_components(row.values))].append(row)

    outcomes = []
    for key, group in grouped.items():
        group.sort(key=lambda row: _priority(row.source))
        outcomes.append(AddressOutcome(
            company_id=company_id,
            address_key=key,
            # Every row in the group normalizes to the same type; the highest-precedence
            # source's own spelling is published.
            address_type=str(group[0].values.get("address_type") or ""),
            care_of=_pick(group, "care_of"),
            street_address=_pick(group, "street_address"),
            normalized_address=_pick(group, "normalized_address"),
            postal_code=_pick(group, "postal_code"),
            city=_pick(group, "city"),
            country_code=_pick(group, "country_code"),
            sources=tuple(row.source for row in group),
            source_record_uids=tuple(row.source_record_uid for row in group),
            evidence_hashes=tuple(row.evidence_hash for row in group),
            address_fingerprints=tuple(str(row.values.get("address_fingerprint") or "") for row in group),
        ))
    return tuple(sorted(outcomes, key=lambda outcome: outcome.address_key))


def augment_with_geocodes(
    outcomes: Sequence[AddressOutcome], geocodes: Mapping[str, GeocodeFact]
) -> tuple[AddressOutcome, ...]:
    """Attach the shared-identity geocode to each merged address.

    ``geocodes`` is keyed by SOURCE observation fingerprint, so a merged address that folds
    two observations may see two facts: the first (in source precedence order) that carries
    a coordinate wins, failing that the first that the geocoder answered at all, failing
    that the first that merely reached an address identity -- so a linked-but-ungeocoded
    address still publishes its ``address_id`` and an empty status. An address the chain
    has never seen keeps ``address_id = None`` and ``geocode_status = ''``.
    """
    augmented = []
    for outcome in outcomes:
        facts = [geocodes[fingerprint] for fingerprint in outcome.address_fingerprints
                 if fingerprint in geocodes]
        chosen = next((fact for fact in facts if fact.has_geocode and fact.latitude is not None), None)
        if chosen is None:
            chosen = next((fact for fact in facts if fact.has_geocode), None)
        if chosen is None:
            chosen = facts[0] if facts else None
        if chosen is None:
            augmented.append(outcome)
            continue
        augmented.append(replace(
            outcome,
            address_id=chosen.address_id or None,
            latitude=chosen.latitude,
            longitude=chosen.longitude,
            geocode_status=chosen.geocode_status,
            geocoded_at=chosen.geocoded_at,
        ))
    return tuple(augmented)


def with_set_replacement(
    outcomes: Sequence[AddressOutcome], published: Sequence[AddressOutcome]
) -> tuple[AddressOutcome, ...]:
    """The rows to publish for one company: this resolution's set, plus a tombstone for
    every key that WAS current and is no longer produced.

    A tombstone republishes the last published row with ``is_current = False``, keeping
    that row's own provenance: the final's ``has_evidence`` CHECK requires non-empty
    ``source_record_uids``, and a reviewer looking at a disappeared address needs to see
    which source once carried it. Its applied ``correction_ids`` are cleared -- the
    corrections decided a live address, and replaying them onto a tombstone would claim
    they were applied to this resolution.

    A key this resolution produced is never tombstoned from ``published``, whatever its
    ``is_current`` says: a rejected row is already this resolution's own tombstone for that
    key, and a second row for the same key would collide in the final's
    ``ORDER BY (company_id, address_key)`` at the same ``resolved_at``.

    A key already published as a tombstone is not republished: nothing about it changed,
    and re-writing it every week would make the table grow for no reason. A key that comes
    BACK is simply produced again, so it publishes with ``is_current = True`` and the newer
    ``resolved_at`` wins in the ReplacingMergeTree.
    """
    produced = {outcome.address_key for outcome in outcomes}
    tombstones = tuple(
        replace(row, is_current=False, correction_ids=())
        for row in published
        if row.is_current and row.address_key not in produced
    )
    return tuple(outcomes) + tombstones


def _payload_key(payload: Mapping[str, Any]) -> str | None:
    key = payload.get("address_key")
    return key if isinstance(key, str) and key else None


def apply_address_ledger(
    outcomes: Sequence[AddressOutcome], ledger: Sequence[LedgerRow]
) -> tuple[tuple[AddressOutcome, ...], tuple[uuid.UUID, ...]]:
    """Apply live corrections, in step then time order, on top of ``outcomes``.

    Every payload names the ``address_key`` it decides -- a company has several rows, so a
    correction without one has no subject. Staleness is per row: a correction is compared
    against the ``evidence_set_hash`` of the row it names, computed here from that row's own
    evidence hashes exactly as the final table's MATERIALIZED column computes it.

    Never raises on a bad correction:

    - stale (its evidence has moved on, or an ``override_field`` names a key this company
      no longer produces, so the reviewer's text was silently lost) -> its id is returned
      in the second element, not applied.
    - inert (a ``reject_address`` that names a key this company no longer produces) ->
      applied in the only sense available: the address the reviewer did not want published
      is not published. Nothing changed, but nothing went wrong, so it is neither returned
      as stale nor surfaced for re-review -- the mirror of how an undone correction leaves
      no trace.
    - malformed (no ``address_key``, an unknown field, a non-string/non-null value, an
      ``override_field`` that names no field at all) -> silently skipped: neither applied
      nor counted as stale, exactly as ``apply_info_ledger`` treats a malformed payload.

    ``override_field`` rewrites the named text fields of one row; an explicit ``null``
    clears a field and an ABSENT key leaves it as computed. It never touches
    ``address_key`` or ``address_type``: the key is the row's identity, so a corrected
    address is a different address.

    ``reject_address`` publishes the row ``is_current = False``. It ranks BEFORE
    ``override_field``, so a live override still decides the text of a row a reject
    tombstones -- the two answer different questions, and a reviewer who does both means both.
    """
    by_key = {outcome.address_key: outcome for outcome in outcomes}
    applied: dict[str, list[uuid.UUID]] = defaultdict(list)
    stale: list[uuid.UUID] = []
    evidence_by_key = {key: evidence_set_hash_for(outcome.evidence_hashes)
                       for key, outcome in by_key.items()}

    for correction in effective_ledger(ledger, ADDRESS_KIND_ORDER):
        key = _payload_key(correction.payload)
        if key is None:
            continue  # malformed: nothing to decide
        outcome = by_key.get(key)
        if outcome is None:
            if correction.kind != "reject_address":
                stale.append(correction.correction_id)  # the text had nowhere to land
            continue
        if correction.evidence_hash not in (ZERO_HASH, evidence_by_key[key]):
            stale.append(correction.correction_id)
            continue
        if correction.kind == "override_field":
            fields = {name: value for name, value in correction.payload.items() if name != "address_key"}
            if not fields or any(name not in OVERRIDABLE_FIELDS for name in fields):
                continue  # malformed: no field, or a field this ledger does not own
            if any(value is not None and not isinstance(value, str) for value in fields.values()):
                continue  # malformed: every field is a string or null
            by_key[key] = replace(outcome, **{name: _text(value) for name, value in fields.items()})
        else:  # reject_address -- the only other kind effective_ledger lets through
            if set(correction.payload) != {"address_key"}:
                continue  # malformed: a reject decides nothing but the key
            by_key[key] = replace(outcome, is_current=False)
        applied[key].append(correction.correction_id)

    resolved = tuple(
        replace(outcome, correction_ids=tuple(sorted(applied[key], key=str))) if applied[key] else outcome
        for key, outcome in by_key.items()
    )
    return tuple(sorted(resolved, key=lambda outcome: outcome.address_key)), tuple(sorted(stale, key=str))


def resolve_company_addresses(
    company_id: str,
    rows: Sequence[ArtifactRow],
    *,
    geocodes: Mapping[str, GeocodeFact],
    published: Sequence[AddressOutcome],
    ledger: Sequence[LedgerRow],
) -> tuple[tuple[AddressOutcome, ...], tuple[uuid.UUID, ...]]:
    """One company's rows to publish, and the corrections that went stale doing it.

    The whole datatype in the one order that is correct: merge the artifacts, attach the
    geocode augmentation, apply the ledger, and only THEN compute the set replacement.

    The ledger runs BEFORE the set replacement because a correction decides a row this
    resolution produced. Reversed, the tombstones the set replacement writes would be in
    front of the ledger, and a correction naming one of them would be applied to a row this
    resolution never resolved -- writing ``correction_ids`` onto a tombstone that
    ``with_set_replacement`` deliberately clears. This order also makes a reject its own
    tombstone: the rejected key stays in the produced set as ``is_current = False``, so the
    set replacement leaves the published row for that key alone (one row per key per
    resolution) and the ReplacingMergeTree retires it on ``resolved_at``. Undo the reject
    and the key is produced current again.

    The geocode is attached BEFORE the ledger, deliberately: a row whose ``street_address``
    an ``override_field`` rewrites still publishes the coordinate the chain resolved from
    the SOURCE text. The geocode belongs to the source observation -- it is looked up by
    that observation's ``address_fingerprint``, which no correction can change -- so a
    reviewer's wording never silently re-points a coordinate at a place nothing geocoded. A
    reviewer who means a DIFFERENT place rejects the address instead; the corrected one
    arrives from a source, with its own fingerprint and its own geocode.

    address.py calls this and nothing else; the four steps stay public because each is a
    rule with its own table test.
    """
    outcomes = merge_company_addresses(company_id, rows)
    outcomes = augment_with_geocodes(outcomes, geocodes)
    outcomes, stale = apply_address_ledger(outcomes, ledger)
    return with_set_replacement(outcomes, published), stale
