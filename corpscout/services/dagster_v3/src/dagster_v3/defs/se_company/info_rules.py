"""Deterministic merge rules for Swedish company information.

Pure functions only — no ClickHouse, no model calls — so every rule is a table
test. info.py wires these to the artifacts, the field values and the LLM.
"""

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any

from dagster_v3.defs.se_company.common import StoredObservation

DESCRIPTION_PRIORITY = ("esef", "wikidata", "scb")
# corpscout.se_company_info_field_value's two CHECK constraints, mirrored here so a row
# that somehow got past them (a direct INSERT) is dropped rather than published.
INFO_VALUE_FIELDS = frozenset({"description", "description_sv"})
INFO_VALUE_SOURCES = frozenset({"scb", "esef", "wikidata", "llm", "reviewer"})


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
    # What that code is CALLED, in both languages, from the curated corpscout.se_code_labels
    # dictionary the SCB artifact already joined -- copied like every other non-description
    # field, never model-written and never settable by a field value. '' means the
    # dictionary does not name this code (the artifact's join missed), which is a fact about
    # the curation, not about the company.
    legal_form_label_en: str
    legal_form_label_sv: str
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
    # and for a field value whose source is `llm`; false for anything copied from an
    # input (the deterministic multi-source pick included), for a reviewer's own
    # wording, for a field value naming any other source, and when there is no text at
    # all. Where each candidate came from is recorded separately (description_sources /
    # description_source_record_uids), and reviewer involvement in correction_ids --
    # this flag answers one question only.
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
    # field value -- info.py offers it to the reviewer as "what the pipeline computed",
    # exactly as description_candidates[0] carries the English half.
    description_sv_candidate: str | None = None
    # value_ids of the live field values this row applied. The name is the published
    # column's, kept from the correction ledger it replaced.
    correction_ids: tuple[uuid.UUID, ...] = ()
    # Field-value rows dropped for an unknown field or source.
    invalid_value_count: int = 0
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
    newest row -- the legal-form labels included: they are the curated
    dictionary's names for ``legal_form_code``, joined in by the SCB artifact,
    so the merge only carries them across. ``description_sv`` is SCB's Swedish original (the register's own
    verksamhetsbeskrivning, never the translation) whenever the register has one, and
    None otherwise -- a Wikidata/ESEF-only description has no Swedish half. When two or
    more sources force the model in, it overwrites both languages at once (info.py).

    Description candidates are gathered from every source with a
    non-empty description (newest row per source, SCB's in English when the
    translator has rendered it): zero candidates publish nothing, exactly one
    is copied as-is, two or more always need the model
    (no agreement heuristic) -- the ESEF > Wikidata > SCB pick is only a
    provisional value until the model (or a field value) replaces it.

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
        legal_form_label_en=str(scb.values.get("legal_form_label_en") or ""),
        legal_form_label_sv=str(scb.values.get("legal_form_label_sv") or ""),
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


@dataclass(frozen=True)
class FieldValueRow:
    """One row of ``corpscout.se_company_info_field_value``.

    Append-only history: the live value for a ``(company_id, field)`` is simply the row
    with the greatest ``(created_at, value_id)``, and a ``value`` of None releases the
    field back to whatever the pipeline computed. ``source_ref`` is the artifact's
    source_record_uid, the suggestion id for ``llm``, and '' for ``reviewer``.
    """

    value_id: uuid.UUID
    company_id: str
    field: str
    value: str | None
    source: str
    source_ref: str
    created_at: datetime


def _live_field_values(rows: Sequence[FieldValueRow]) -> tuple[dict[str, FieldValueRow], int]:
    """The newest valid row per field, plus how many rows were dropped as invalid.

    ``value_id`` is compared as text so the tie-break is the same total order the
    published table sorts by, and so two rows written in the same millisecond can never
    swap places between runs.
    """
    live: dict[str, FieldValueRow] = {}
    invalid = 0
    for row in rows:
        if row.field not in INFO_VALUE_FIELDS or row.source not in INFO_VALUE_SOURCES:
            invalid += 1  # a direct INSERT past the table's CHECKs; never published
            continue
        current = live.get(row.field)
        if current is None or (row.created_at, str(row.value_id)) > (current.created_at, str(current.value_id)):
            live[row.field] = row
    return live, invalid


def _apply_description_value(
    outcome: InfoOutcome, row: FieldValueRow, stored: Sequence[StoredObservation]
) -> InfoOutcome:
    """Publish one live ``description`` value, with the provenance its source implies."""
    if row.source != "llm":
        # A source's own text, or a reviewer's wording: copied, not model-written --
        # whatever the row carried before (a model answer included) is replaced whole.
        return replace(
            outcome,
            description=_text(row.value),
            llm_enhanced=False,
            suggestion_id=None,
            needs_model=False,
            model_provider="deterministic",
            model_name=f"field-value:{row.source}",
            prompt_version="",
        )
    try:
        suggestion_id: uuid.UUID | None = uuid.UUID(row.source_ref)
    except (ValueError, TypeError, AttributeError):
        suggestion_id = None  # a malformed source_ref names no suggestion, and never raises
    observation = next((row_ for row_ in stored if row_.suggestion_id == suggestion_id), None)
    return replace(
        outcome,
        description=_text(row.value),
        # The observation is what knows which language the model answered in; without
        # one (aged out of the enrichment table) the computed language stands.
        description_language=(
            str(observation.suggestion.get("language") or outcome.description_language)
            if observation is not None
            else outcome.description_language
        ),
        # The text is the model's, whoever chose to publish it.
        llm_enhanced=True,
        suggestion_id=suggestion_id,
        needs_model=False,
        model_provider=observation.model_provider if observation is not None else "llm",
        model_name=observation.model_name if observation is not None else "field-value:llm",
        prompt_version=observation.prompt_version if observation is not None else "",
    )


def apply_field_values(
    outcome: InfoOutcome,
    rows: Sequence[FieldValueRow],
    *,
    stored: Sequence[StoredObservation],
) -> InfoOutcome:
    """Apply the live field values on top of ``outcome``.

    One rule, per field independently: the live row is the newest one written for that
    field, and its value is published. A NULL value is a release -- the pipeline's own
    computed value stands, untouched, and the release is not counted as applied. There
    is no staleness, no kind ranking and no undo chain: undo is writing the previous
    value again, or NULL.

    ``description`` also decides the row's provenance. An ``llm`` value republishes the
    model's text, so ``llm_enhanced`` goes up and ``source_ref`` names the suggestion;
    the model_provider/model_name/prompt_version come from that stored observation when
    it is still around and from fixed ``field-value:llm`` placeholders when it is not.
    Every other source is a copy: ``llm_enhanced`` down, no suggestion, and
    ``deterministic`` / ``field-value:<source>`` provenance. Either way the field is
    decided, so ``needs_model`` is cleared. ``description_sv`` is one string with no
    provenance of its own, so it only ever sets that column.

    Never raises: a row naming an unknown field or source (only reachable by a direct
    INSERT past the table's CHECKs) is skipped, counted in ``invalid_value_count``, and
    cannot shadow a valid row for the same field.
    """
    live, invalid = _live_field_values(rows)
    applied: list[uuid.UUID] = []

    description = live.get("description")
    if description is not None and description.value is not None:
        outcome = _apply_description_value(outcome, description, stored)
        applied.append(description.value_id)

    description_sv = live.get("description_sv")
    if description_sv is not None and description_sv.value is not None:
        outcome = replace(outcome, description_sv=_text(description_sv.value))
        applied.append(description_sv.value_id)

    return replace(
        outcome,
        correction_ids=tuple(sorted(applied, key=str)),
        invalid_value_count=invalid,
    )
