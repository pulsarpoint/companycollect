"""Deterministic merge rules for Swedish company information.

Pure functions only — no ClickHouse, no model calls — so every rule is a table
test. info.py wires these to the artifacts, the ledger and the LLM.
"""

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any

from dagster_v3.defs.se_company.common import LedgerRow, StoredObservation, effective_ledger

INFO_KIND_ORDER = {"approve_suggestion": 0, "reject_suggestion": 0, "override_field": 1}
DESCRIPTION_PRIORITY = ("esef", "wikidata", "scb")
ZERO_HASH = "0" * 64


@dataclass(frozen=True)
class ArtifactRow:
    source: str
    source_record_uid: str
    evidence_hash: str
    observed_at: datetime
    values: Mapping[str, Any]


@dataclass(frozen=True)
class InfoOutcome:
    company_id: str
    legal_name: str
    legal_form_code: str | None
    status: str
    incorporation_date: date | None
    description: str | None
    # The Swedish half of the published pair: SCB's own verksamhetsbeskrivning
    # deterministically, the model's Swedish summary once it has answered, whatever a
    # reviewer decided after that. None means the company has no Swedish text at all
    # (its description came from Wikidata/ESEF only).
    description_sv: str | None
    description_language: str
    # Did the PUBLISHED text come out of the model? True for the model's merged summary
    # and for an approved suggestion; false for anything copied from an input (the
    # deterministic multi-source pick included), for a reviewer's own wording, after a
    # rejection, and when there is no text at all. Where each candidate came from is
    # recorded separately (description_sources / description_source_record_uids), and
    # reviewer involvement in correction_ids -- this flag answers one question only.
    llm_enhanced: bool
    description_sources: tuple[str, ...]
    description_source_record_uids: tuple[str, ...]
    primary_nace_code: str
    primary_sni_code: str
    wikidata_id: str | None
    lei: str | None
    source_record_uids: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    needs_model: bool = False
    description_candidates: tuple[tuple[str, str, str], ...] = ()  # (source, source_record_uid, text)
    description_candidate_languages: tuple[str, ...] = ()  # parallel to description_candidates
    # The deterministic value of description_sv, kept beside it and never mutated by a
    # correction -- reject_suggestion restores the pair from here, exactly as it restores
    # the English text from description_candidates[0].
    description_sv_candidate: str | None = None
    correction_ids: tuple[uuid.UUID, ...] = ()
    stale_correction_ids: tuple[uuid.UUID, ...] = ()
    suggestion_id: uuid.UUID | None = None
    model_provider: str = "deterministic"
    model_name: str = "se-company-info-rules"
    prompt_version: str = "se-company-info-rules-v1"


def evidence_set_hash_for(evidence_hashes: Sequence[str]) -> str:
    """Sha256 hex of the sorted hashes joined by ``\\n``.

    Must equal the final table's MATERIALIZED ``evidence_set_hash`` column:
    ``lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x),
    evidence_hashes)), '\\n'))))``. ``sorted()`` on strings matches ClickHouse's
    default ascending ``arraySort``, and ``hexdigest()`` is already lowercase.
    """
    return hashlib.sha256("\n".join(sorted(evidence_hashes)).encode()).hexdigest()


def _text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _date(value: object) -> date | None:
    """Artifact rows arrive as strings (JSON map); the final stores Date32."""
    if value is None or isinstance(value, date):
        return value
    text = str(value).strip()
    return date.fromisoformat(text) if text else None


def _newest(rows: Sequence[ArtifactRow], source: str, key=None) -> ArtifactRow | None:
    matching = [row for row in rows if row.source == source]
    if not matching:
        return None
    return max(matching, key=key or (lambda row: (row.observed_at, row.source_record_uid)))


def merge_company_info(company_id: str, rows: Sequence[ArtifactRow]) -> InfoOutcome | None:
    """Merge one company's artifact rows into a final outcome.

    Every non-description field is copied as-is from its owning source's
    newest row. ``description_sv`` is SCB's Swedish original (the register's own
    verksamhetsbeskrivning, never the translation) whenever the register has one, and
    None otherwise -- a Wikidata/ESEF-only description has no Swedish half. When two or
    more sources force the model in, it overwrites both languages at once (info.py).

    Description candidates are gathered from every source with a
    non-empty description (newest row per source, SCB's in English when the
    translator has rendered it): zero candidates publish nothing, exactly one
    is copied as-is, two or more always need the model
    (no agreement heuristic) -- the ESEF > Wikidata > SCB pick is only a
    provisional value until the model (or a review correction) replaces it.

    A company without a register row, or whose register row carries no legal
    name at all, is never published: this mirrors the final table's
    ``has_legal_name`` CHECK, so a nameless row never reaches the point of
    aborting the whole publish batch.
    """
    scb = _newest(rows, "scb")
    if scb is None:
        return None
    legal_name = _text(scb.values.get("legal_name")) or _text(scb.values.get("legal_name_raw")) or ""
    if not legal_name:
        return None
    esef = _newest(
        rows,
        "esef",
        key=lambda row: (
            int(str(row.values.get("fiscal_year") or 0) or 0),
            row.observed_at,
            row.source_record_uid,
        ),
    )
    wikidata = _newest(rows, "wikidata")

    # (source, source_record_uid, text, language) for every source that offers a description.
    # Each text is captured once and narrowed by its own truthy check, so the
    # list is genuinely `str` (never `str | None`) in every slot.
    candidates: list[tuple[str, str, str, str]] = []
    if esef is not None:
        esef_description = _text(esef.values.get("company_description"))
        if esef_description:
            candidates.append((
                "esef",
                esef.source_record_uid,
                esef_description,
                str(esef.values.get("description_language") or ""),
            ))
    if wikidata is not None:
        wikidata_description = _text(wikidata.values.get("company_description"))
        if wikidata_description:
            candidates.append(("wikidata", wikidata.source_record_uid, wikidata_description, "en"))
    # SCB's verksamhetsbeskrivning is Swedish; the artifact carries the translator's
    # English rendering beside it (000300) and that is what the pilot publishes and
    # what the model is shown. A company the translator has not reached yet keeps its
    # Swedish text rather than losing its only description.
    scb_description_en = _text(scb.values.get("activity_description_en"))
    scb_description = scb_description_en or _text(scb.values.get("activity_description"))
    if scb_description:
        candidates.append((
            "scb",
            scb.source_record_uid,
            scb_description,
            "en" if scb_description_en else "sv",
        ))
    candidates.sort(key=lambda item: DESCRIPTION_PRIORITY.index(item[0]))

    # SCB is the only Swedish-language source, and it is always present (the merge
    # returns None above without it), so this covers every deterministic case at once:
    # the Swedish original when the register has one, None when it does not -- including
    # the "SCB row exists but carries no description" case, where SCB contributes no
    # candidate either.
    description_sv = _text(scb.values.get("activity_description"))

    description, language = None, ""
    if candidates:
        _, _, description, language = candidates[0]  # copied when single; provisional when several
    needs_model = len(candidates) > 1

    used = [row for row in (scb, esef, wikidata) if row is not None]
    return InfoOutcome(
        company_id=company_id,
        legal_name=legal_name,
        legal_form_code=_text(scb.values.get("legal_form_code")),
        status=str(scb.values.get("status") or ""),
        incorporation_date=_date(scb.values.get("incorporation_date")),
        description=description,
        description_sv=description_sv,
        description_language=language,
        # The merge only ever copies: the model runs later, in info.py.
        llm_enhanced=False,
        description_sources=tuple(c[0] for c in candidates),
        description_source_record_uids=tuple(c[1] for c in candidates),
        primary_nace_code=str(scb.values.get("primary_nace_code") or ""),
        primary_sni_code=str(scb.values.get("primary_sni_code") or ""),
        wikidata_id=_text(wikidata.values.get("wikidata_id")) if wikidata else None,
        lei=_text(esef.values.get("lei")) if esef else None,
        source_record_uids=tuple(row.source_record_uid for row in used),
        evidence_hashes=tuple(row.evidence_hash for row in used),
        needs_model=needs_model,
        description_candidates=tuple((src, uid, text) for src, uid, text, _ in candidates),
        description_candidate_languages=tuple(c[3] for c in candidates),
        description_sv_candidate=description_sv,
    )


def apply_info_ledger(
    outcome: InfoOutcome,
    ledger: Sequence[LedgerRow],
    *,
    evidence_set_hash: str,
    current_input_hash: str | None,
    stored: Sequence[StoredObservation],
) -> InfoOutcome:
    """Apply live corrections, in step then time order, on top of ``outcome``.

    Never raises on a bad correction:

    - stale (its evidence has moved on, or an approve/reject that no longer
      names a suggestion whose ``input_hash`` matches ``current_input_hash``)
      -> its id is collected in ``stale_correction_ids``, not applied.
    - malformed (an ``override_field`` payload naming ``legal_name`` --
      legal name is SCB's -- an unknown field, or a non-string/non-null
      description) -> silently skipped: neither applied nor counted as stale.

    An ``override_field`` payload is ``{"description"}`` or
    ``{"description", "description_sv"}``: the English text is always required, the
    Swedish one is optional and its ABSENCE means "leave the Swedish text as computed"
    (deterministic or model-written), while a present ``None`` is a decision and is
    applied. Anything else is malformed.

    ``approve_suggestion`` publishes both of the stored suggestion's languages with
    ``llm_enhanced = True`` (the text is the model's, whoever approved it) and
    ``suggestion_id`` set -- unless
    the stored suggestion has no non-empty ``description`` string, in which
    case the correction is treated as stale (nothing sensible to approve). A
    suggestion with no Swedish half (one recorded before the bilingual prompt)
    leaves ``description_sv`` as computed, exactly as an absent
    ``description_sv`` in an override payload does.
    ``reject_suggestion`` discards it, falls back to the highest-priority
    deterministic candidate (text and language alike) together with the
    deterministic Swedish text, and clears ``suggestion_id`` and
    ``llm_enhanced``. Both share an
    ``INFO_KIND_ORDER`` rank, so between them "later" (by ``created_at``) wins.

    Both ``reject_suggestion`` and ``override_field`` also reset
    ``model_provider``/``model_name``/``prompt_version`` to fixed
    deterministic values, so a rejected or manually-overridden description is
    never mistaken for a still-live model result: ``model_provider =
    "deterministic"`` and ``prompt_version = ""`` for both, with
    ``model_name`` distinguishing *why* the row is deterministic
    (``"rejected-suggestion"`` vs ``"override"``) rather than reusing
    ``InfoOutcome``'s own class-default ``model_name``, which means "the
    original merge never needed a model" -- a different fact.
    """
    stored_by_id = {row.suggestion_id: row for row in stored}
    applied: list[uuid.UUID] = []
    stale: list[uuid.UUID] = []
    for correction in effective_ledger(ledger, INFO_KIND_ORDER):
        if correction.evidence_hash not in (ZERO_HASH, evidence_set_hash):
            stale.append(correction.correction_id)
            continue
        if correction.kind == "override_field":
            if set(correction.payload) not in ({"description"}, {"description", "description_sv"}):
                continue  # malformed: legal_name (SCB's only), an unknown field, or no description
            values = [correction.payload["description"]]
            if "description_sv" in correction.payload:
                values.append(correction.payload["description_sv"])
            if any(value is not None and not isinstance(value, str) for value in values):
                continue  # malformed: each description must be str or null
            outcome = replace(
                outcome,
                description=_text(values[0]),
                # Absent -> whatever this outcome already carries; present (null included)
                # -> the reviewer's decision.
                description_sv=_text(values[1]) if len(values) > 1 else outcome.description_sv,
                # The reviewer typed this text, so it is not the model's -- however the
                # row got here (a model answer earlier in the same resolution included).
                llm_enhanced=False,
                suggestion_id=None,
                needs_model=False,
                model_provider="deterministic",
                model_name="override",
                prompt_version="",
            )
        elif correction.kind in ("approve_suggestion", "reject_suggestion"):
            try:
                suggestion_id = uuid.UUID(str(correction.payload.get("suggestion_id", "")))
            except ValueError:
                stale.append(correction.correction_id)
                continue
            suggestion = stored_by_id.get(suggestion_id)
            if suggestion is None or current_input_hash is None or suggestion.input_hash != current_input_hash:
                stale.append(correction.correction_id)
                continue
            if correction.kind == "approve_suggestion":
                raw_description = suggestion.suggestion.get("description")
                approved_text = raw_description.strip() if isinstance(raw_description, str) else ""
                if not approved_text:
                    # Nothing sensible to approve -- treat like any other
                    # correction that no longer names something usable.
                    stale.append(correction.correction_id)
                    continue
                # A suggestion recorded before the bilingual prompt has no Swedish half.
                # Absent means "leave it as computed" -- the same rule override_field
                # follows -- so the deterministic Swedish text stays rather than being
                # blanked to NULL by an approval that never spoke about it.
                approved_sv = _text(suggestion.suggestion.get("description_sv"))
                outcome = replace(
                    outcome,
                    description=approved_text,
                    description_sv=outcome.description_sv if approved_sv is None else approved_sv,
                    description_language=str(suggestion.suggestion.get("language") or outcome.description_language),
                    # Approved, but still the model's text.
                    llm_enhanced=True,
                    suggestion_id=suggestion_id,
                    needs_model=False,
                    model_provider=suggestion.model_provider,
                    model_name=suggestion.model_name,
                    prompt_version=suggestion.prompt_version,
                )
            else:
                # Rejected: fall back to the highest-priority deterministic
                # candidate, text and language alike (description_candidates
                # / description_candidate_languages are never mutated by a
                # prior correction, so this is always the original merge's
                # pick, not whatever an intervening approve last set).
                fallback = outcome.description_candidates[0] if outcome.description_candidates else None
                fallback_language = (
                    outcome.description_candidate_languages[0]
                    if outcome.description_candidate_languages
                    else outcome.description_language
                )
                outcome = replace(
                    outcome,
                    suggestion_id=None,
                    needs_model=False,
                    description=fallback[2] if fallback else outcome.description,
                    description_sv=outcome.description_sv_candidate,
                    # Back to a copied candidate, so the flag goes down with the text.
                    llm_enhanced=False,
                    description_language=fallback_language,
                    model_provider="deterministic",
                    model_name="rejected-suggestion",
                    prompt_version="",
                )
        applied.append(correction.correction_id)
    return replace(
        outcome,
        correction_ids=tuple(sorted(applied, key=str)),
        stale_correction_ids=tuple(sorted(stale, key=str)),
    )
