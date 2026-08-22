import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from dagster_v3.defs.se_company.common import LedgerRow, StoredObservation
from dagster_v3.defs.se_company.info_rules import (
    INFO_KIND_ORDER,
    ArtifactRow,
    apply_info_ledger,
    evidence_set_hash_for,
    merge_company_info,
)

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
COMPANY = "5565200028"


def _scb(description=None, **values):
    return ArtifactRow("scb", "scb:1", "a" * 64, NOW, {
        "legal_name": "Alpha AB", "legal_name_raw": "ALPHA AB", "legal_form_code": "AB", "status": "active",
        "incorporation_date": None, "dissolution_date": None, "activity_description": description,
        "primary_sni_code": "62010", "primary_nace_code": "62.01", **values})


def _esef(description, fiscal_year=2024, uid="esef:1", observed_at=None):
    return ArtifactRow("esef", uid, "b" * 64, observed_at if observed_at is not None else NOW + timedelta(days=fiscal_year - 2024), {
        "source_document_id": uid, "lei": "5493001KJTIIGC8Y1R12", "entity_name": "Alpha AB", "fiscal_year": fiscal_year,
        "company_description": description, "description_language": "en", "description_confidence": 0.9,
        "products_and_services_json": "[]", "business_segments_json": "[]"})


def _wikidata(description):
    return ArtifactRow("wikidata", "wikidata:Q1", "c" * 64, NOW, {
        "wikidata_id": "Q1", "wikidata_url": "https://www.wikidata.org/wiki/Q1", "name": "Alpha", "official_name": None,
        "company_description": description, "inception_date": None, "legal_form_label": None,
        "industry_wikidata_id": None, "industry_label": None, "headquarters_label": None, "employee_count": None})


def test_no_register_row_means_no_outcome() -> None:
    assert merge_company_info(COMPANY, [_esef("x")]) is None


def test_single_source_description_is_used_as_is() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="Säljer programvara.")])
    assert outcome is not None
    assert outcome.legal_name == "Alpha AB" and outcome.description == "Säljer programvara."
    assert outcome.description_source == "scb" and not outcome.needs_model
    assert outcome.description_sources == ("scb",) and outcome.description_source_record_uids == ("scb:1",)
    assert outcome.source_record_uids == ("scb:1",) and outcome.evidence_hashes == ("a" * 64,)


def test_two_sources_always_need_the_model_even_when_they_agree() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="Software company."), _wikidata("software company")])
    assert outcome.needs_model
    assert [c[0] for c in outcome.description_candidates] == ["wikidata", "scb"]
    assert outcome.description_sources == ("wikidata", "scb")
    assert outcome.description_source_record_uids == ("wikidata:Q1", "scb:1")
    assert outcome.description == "software company" and outcome.description_source == "wikidata"  # provisional pick


def test_three_sources_keep_every_candidate_and_copy_other_fields_as_is() -> None:
    outcome = merge_company_info(COMPANY, [
        _scb(description="Konsultverksamhet inom IT."), _esef("Alpha builds payment software for retailers."),
        _wikidata("Swedish fintech company"),
    ])
    assert outcome.needs_model and outcome.description_source == "esef"
    assert [c[0] for c in outcome.description_candidates] == ["esef", "wikidata", "scb"]
    assert outcome.lei == "5493001KJTIIGC8Y1R12" and outcome.wikidata_id == "Q1" and outcome.legal_name == "Alpha AB"
    assert set(outcome.source_record_uids) == {"scb:1", "esef:1", "wikidata:Q1"}


def test_newest_esef_filing_wins_among_esef_rows() -> None:
    # The newer observed_at deliberately carries the OLDER fiscal_year, so this
    # isolates fiscal_year -- not observed_at -- as the tie-break key.
    outcome = merge_company_info(COMPANY, [
        _scb(),
        _esef("old", 2023, "esef:0", observed_at=NOW + timedelta(days=10)),
        _esef("new", 2024, "esef:1", observed_at=NOW),
    ])
    assert outcome.description == "new"


def test_zero_description_candidates_publish_nothing() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description=None)])
    assert outcome is not None
    assert outcome.description is None and outcome.description_source == ""
    assert outcome.description_sources == () and outcome.description_source_record_uids == ()
    assert outcome.description_candidates == () and outcome.description_candidate_languages == ()
    assert not outcome.needs_model


def test_empty_legal_name_means_no_outcome() -> None:
    # A nameless SCB row is never published -- the final table's
    # has_legal_name CHECK would otherwise abort the whole publish batch.
    assert merge_company_info(COMPANY, [_scb(legal_name=None, legal_name_raw=None)]) is None
    assert merge_company_info(COMPANY, [_scb(legal_name="   ", legal_name_raw=None)]) is None


def test_override_and_stale_corrections() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    fresh = LedgerRow(uuid.UUID(int=1), COMPANY, "override_field", {"description": "Reviewed text"}, hash_now, None, NOW)
    stale = LedgerRow(uuid.UUID(int=2), COMPANY, "override_field", {"description": "Old"}, "9" * 64, None, NOW + timedelta(seconds=1))
    applied = apply_info_ledger(outcome, [fresh, stale], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.description == "Reviewed text" and applied.description_source == "reviewed"
    assert applied.correction_ids == (uuid.UUID(int=1),) and applied.stale_correction_ids == (uuid.UUID(int=2),)


def test_approve_suggestion_requires_a_current_input_hash() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    stored = StoredObservation(uuid.UUID(int=5), COMPANY, "h" * 64, {"description": "Merged text", "language": "en"}, "deepseek", "m", "v1", NOW)
    approve = LedgerRow(uuid.UUID(int=3), COMPANY, "approve_suggestion", {"suggestion_id": str(uuid.UUID(int=5))}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [approve], evidence_set_hash=hash_now, current_input_hash="h" * 64, stored=[stored])
    assert applied.description == "Merged text" and applied.suggestion_id == uuid.UUID(int=5) and not applied.needs_model
    stale = apply_info_ledger(outcome, [approve], evidence_set_hash=hash_now, current_input_hash="z" * 64, stored=[stored])
    assert stale.stale_correction_ids == (uuid.UUID(int=3),)


def test_kind_order_ranks_approve_and_reject_together_below_override() -> None:
    assert INFO_KIND_ORDER == {"approve_suggestion": 0, "reject_suggestion": 0, "override_field": 1}


# R17a: the brief's combined "later approve beats earlier reject" test never actually
# exercised approve-vs-reject ordering. approve_suggestion and reject_suggestion share
# the same INFO_KIND_ORDER rank (0), so effective_ledger orders them by created_at --
# these two tests apply the same suggestion both ways, fresh, and check which wins.


def test_later_approve_beats_earlier_reject() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    stored = StoredObservation(uuid.UUID(int=5), COMPANY, "h" * 64, {"description": "Merged text", "language": "en"}, "deepseek", "m", "v1", NOW)
    suggestion_id = str(uuid.UUID(int=5))
    reject = LedgerRow(uuid.UUID(int=6), COMPANY, "reject_suggestion", {"suggestion_id": suggestion_id}, hash_now, None, NOW)
    approve = LedgerRow(uuid.UUID(int=7), COMPANY, "approve_suggestion", {"suggestion_id": suggestion_id}, hash_now, None, NOW + timedelta(seconds=1))
    applied = apply_info_ledger(outcome, [reject, approve], evidence_set_hash=hash_now, current_input_hash="h" * 64, stored=[stored])
    assert applied.description == "Merged text"
    assert applied.description_source == "reviewed"
    assert applied.suggestion_id == uuid.UUID(int=5)
    assert set(applied.correction_ids) == {uuid.UUID(int=6), uuid.UUID(int=7)}


def test_later_reject_beats_earlier_approve() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    stored = StoredObservation(uuid.UUID(int=5), COMPANY, "h" * 64, {"description": "Merged text", "language": "en"}, "deepseek", "m", "v1", NOW)
    suggestion_id = str(uuid.UUID(int=5))
    approve = LedgerRow(uuid.UUID(int=8), COMPANY, "approve_suggestion", {"suggestion_id": suggestion_id}, hash_now, None, NOW)
    reject = LedgerRow(uuid.UUID(int=9), COMPANY, "reject_suggestion", {"suggestion_id": suggestion_id}, hash_now, None, NOW + timedelta(seconds=1))
    applied = apply_info_ledger(outcome, [approve, reject], evidence_set_hash=hash_now, current_input_hash="h" * 64, stored=[stored])
    assert applied.suggestion_id is None
    assert applied.description == outcome.description  # deterministic pick kept, not the rejected LLM text
    assert applied.description_source == outcome.description_source
    assert set(applied.correction_ids) == {uuid.UUID(int=8), uuid.UUID(int=9)}


def test_evidence_set_hash_for_is_order_independent() -> None:
    # Must equal the final table's MATERIALIZED expression:
    # lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n')))).
    forward = evidence_set_hash_for(["a" * 64, "b" * 64])
    reverse = evidence_set_hash_for(["b" * 64, "a" * 64])
    assert forward == reverse
    assert len(forward) == 64 and forward == forward.lower()


# apply_info_ledger must never raise on a malformed correction -- these are skipped
# outright (neither applied nor counted as stale).


def test_override_field_rejects_legal_name() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    correction = LedgerRow(uuid.UUID(int=10), COMPANY, "override_field", {"legal_name": "New Name AB"}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [correction], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.legal_name == "Alpha AB"
    assert applied.description == "x"
    assert applied.correction_ids == () and applied.stale_correction_ids == ()


def test_override_field_rejects_unknown_field() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    correction = LedgerRow(uuid.UUID(int=11), COMPANY, "override_field", {"status": "dissolved"}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [correction], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.status == outcome.status
    assert applied.correction_ids == () and applied.stale_correction_ids == ()


def test_override_field_rejects_non_string_description() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    correction = LedgerRow(uuid.UUID(int=12), COMPANY, "override_field", {"description": 12345}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [correction], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.description == "x"
    assert applied.correction_ids == () and applied.stale_correction_ids == ()


def test_override_field_can_clear_description_with_null() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    correction = LedgerRow(uuid.UUID(int=13), COMPANY, "override_field", {"description": None}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [correction], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.description is None
    assert applied.description_source == "reviewed"
    assert applied.correction_ids == (uuid.UUID(int=13),)


def test_approve_suggestion_with_no_description_is_stale() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    stored = StoredObservation(uuid.UUID(int=14), COMPANY, "h" * 64, {"description": "   ", "language": "en"}, "deepseek", "m", "v1", NOW)
    approve = LedgerRow(uuid.UUID(int=15), COMPANY, "approve_suggestion", {"suggestion_id": str(uuid.UUID(int=14))}, hash_now, None, NOW)
    applied = apply_info_ledger(outcome, [approve], evidence_set_hash=hash_now, current_input_hash="h" * 64, stored=[stored])
    assert applied.stale_correction_ids == (uuid.UUID(int=15),)
    assert applied.correction_ids == ()
    assert applied.description == outcome.description
    assert applied.suggestion_id is None


def test_reject_suggestion_resets_model_provenance_and_language() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    # Simulate the final asset having already stamped this outcome with an
    # LLM's provenance before the correction ledger is applied.
    llm_touched = replace(outcome, model_provider="deepseek", model_name="deepseek-v3", prompt_version="v9")
    stored = StoredObservation(uuid.UUID(int=16), COMPANY, "h" * 64, {"description": "Merged text", "language": "en"}, "deepseek", "deepseek-v3", "v9", NOW)
    reject = LedgerRow(uuid.UUID(int=17), COMPANY, "reject_suggestion", {"suggestion_id": str(uuid.UUID(int=16))}, hash_now, None, NOW)
    applied = apply_info_ledger(llm_touched, [reject], evidence_set_hash=hash_now, current_input_hash="h" * 64, stored=[stored])
    assert applied.model_provider == "deterministic"
    assert applied.model_name == "rejected-suggestion"
    assert applied.prompt_version == ""
    assert applied.suggestion_id is None
    # Deterministic fallback pick (wikidata, since esef is absent), text and language alike.
    assert applied.description == outcome.description_candidates[0][2] == "b"
    assert applied.description_language == outcome.description_candidate_languages[0] == "en"


def test_override_field_resets_model_provenance() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    hash_now = evidence_set_hash_for(outcome.evidence_hashes)
    llm_touched = replace(
        outcome, model_provider="deepseek", model_name="deepseek-v3", prompt_version="v9", suggestion_id=uuid.UUID(int=16)
    )
    correction = LedgerRow(uuid.UUID(int=18), COMPANY, "override_field", {"description": "Reviewed text"}, hash_now, None, NOW)
    applied = apply_info_ledger(llm_touched, [correction], evidence_set_hash=hash_now, current_input_hash=None, stored=())
    assert applied.model_provider == "deterministic"
    assert applied.model_name == "override"
    assert applied.prompt_version == ""
    assert applied.suggestion_id is None
    assert applied.description == "Reviewed text"
