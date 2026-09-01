import uuid
from datetime import UTC, datetime, timedelta

from dagster_v3.defs.se_company.common import StoredObservation
from dagster_v3.defs.se_company.info_rules import (
    ArtifactRow,
    FieldValueRow,
    apply_field_values,
    evidence_set_hash_for,
    merge_company_info,
)

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
COMPANY = "5565200028"


def _scb(description=None, **values):
    return ArtifactRow("scb", "scb:1", "a" * 64, NOW, {
        "legal_name": "Alpha AB", "legal_name_raw": "ALPHA AB", "legal_form_code": "AB-ORGFO",
        "legal_form_label_en": "Limited company (aktiebolag)", "legal_form_label_sv": "Aktiebolag",
        "status": "active",
        "incorporation_date": None, "dissolution_date": None, "activity_description": description,
        "activity_description_en": "",
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


def _value(number, field, value, *, source="reviewer", source_ref="", at=NOW):
    """One row of corpscout.se_company_info_field_value, ``number`` its value_id."""
    return FieldValueRow(uuid.UUID(int=number), COMPANY, field, value, source, source_ref, at)


def test_no_register_row_means_no_outcome() -> None:
    assert merge_company_info(COMPANY, [_esef("x")]) is None


def test_single_source_description_is_used_as_is() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="Säljer programvara.")])
    assert outcome is not None
    assert outcome.legal_name == "Alpha AB" and outcome.description == "Säljer programvara."
    assert not outcome.llm_enhanced and not outcome.needs_model
    assert outcome.description_sources == ("scb",) and outcome.description_source_record_uids == ("scb:1",)
    assert outcome.source_record_uids == ("scb:1",) and outcome.evidence_hashes == ("a" * 64,)


def test_scb_description_prefers_the_translators_english_text() -> None:
    outcome = merge_company_info(
        COMPANY, [_scb(description="Säljer programvara.", activity_description_en="Sells software.")]
    )
    assert outcome is not None
    assert outcome.description == "Sells software." and outcome.description_language == "en"
    assert outcome.description_sources == ("scb",) and not outcome.needs_model


def test_scb_description_falls_back_to_swedish_when_untranslated() -> None:
    """~8% of the register has no translation yet (the translator runs outside this
    pipeline), so the Swedish text is still published rather than nothing."""
    outcome = merge_company_info(COMPANY, [_scb(description="Säljer programvara.")])
    assert outcome is not None
    assert outcome.description == "Säljer programvara." and outcome.description_language == "sv"


def test_the_model_is_offered_the_english_scb_text() -> None:
    outcome = merge_company_info(COMPANY, [
        _scb(description="Säljer programvara.", activity_description_en="Sells software."),
        _wikidata("swedish software company"),
    ])
    assert outcome is not None and outcome.needs_model
    assert [c[0] for c in outcome.description_candidates] == ["wikidata", "scb"]
    assert outcome.description_candidates[1] == ("scb", "scb:1", "Sells software.")
    assert outcome.description_candidate_languages == ("en", "en")


def test_two_sources_always_need_the_model_even_when_they_agree() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="Software company."), _wikidata("software company")])
    assert outcome.needs_model
    assert [c[0] for c in outcome.description_candidates] == ["wikidata", "scb"]
    assert outcome.description_sources == ("wikidata", "scb")
    assert outcome.description_source_record_uids == ("wikidata:Q1", "scb:1")
    # Provisional pick: Wikidata, the highest-priority source present -- and not the
    # model's text, so the flag stays down until info.py has an answer.
    assert outcome.description == "software company" and not outcome.llm_enhanced


def test_three_sources_keep_every_candidate_and_copy_other_fields_as_is() -> None:
    outcome = merge_company_info(COMPANY, [
        _scb(description="Konsultverksamhet inom IT."), _esef("Alpha builds payment software for retailers."),
        _wikidata("Swedish fintech company"),
    ])
    assert outcome.needs_model and outcome.description_sources[0] == "esef"
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
    assert outcome.description is None and not outcome.llm_enhanced
    assert outcome.description_sources == () and outcome.description_source_record_uids == ()
    assert outcome.description_candidates == () and outcome.description_candidate_languages == ()
    assert not outcome.needs_model


def test_empty_legal_name_means_no_outcome() -> None:
    # A nameless SCB row is never published -- the final table's
    # has_legal_name CHECK would otherwise abort the whole publish batch.
    assert merge_company_info(COMPANY, [_scb(legal_name=None, legal_name_raw=None)]) is None
    assert merge_company_info(COMPANY, [_scb(legal_name="   ", legal_name_raw=None)]) is None


def test_evidence_set_hash_for_is_order_independent() -> None:
    # Must equal the final table's MATERIALIZED expression:
    # lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n')))).
    forward = evidence_set_hash_for(["a" * 64, "b" * 64])
    reverse = evidence_set_hash_for(["b" * 64, "a" * 64])
    assert forward == reverse
    assert len(forward) == 64 and forward == forward.lower()


# Task 14 (owner decision 2026-08-23): the final holds both languages natively.
# `description` is the published English text, `description_sv` the Swedish one --
# SCB's own verksamhetsbeskrivning deterministically, the model's Swedish summary
# when several sources had to be merged.


def test_scb_only_publishes_the_swedish_original_beside_the_english_text() -> None:
    outcome = merge_company_info(
        COMPANY, [_scb(description="Saeljer programvara.", activity_description_en="Sells software.")]
    )
    assert outcome is not None
    assert outcome.description == "Sells software." and outcome.description_language == "en"
    # The Swedish column is the register's own text, never the translation.
    assert outcome.description_sv == "Saeljer programvara."


def test_an_untranslated_scb_company_publishes_the_same_text_in_both_columns() -> None:
    """Nothing special happens for the ~8% the translator has not reached: `description`
    falls back to the Swedish text, and `description_sv` is that same original."""
    outcome = merge_company_info(COMPANY, [_scb(description="Saeljer programvara.")])
    assert outcome.description == "Saeljer programvara." and outcome.description_language == "sv"
    assert outcome.description_sv == "Saeljer programvara."


def test_a_company_with_no_scb_text_has_no_swedish_description() -> None:
    """Wikidata/ESEF-only descriptions have no Swedish original to publish, and NULL
    (not an empty string) is what "there is none" means in the final."""
    outcome = merge_company_info(COMPANY, [_scb(description=None), _wikidata("Swedish fintech company")])
    assert outcome.description == "Swedish fintech company"
    assert outcome.description_sv is None
    assert merge_company_info(COMPANY, [_scb(description=None)]).description_sv is None


def test_the_deterministic_multi_source_pick_keeps_scbs_swedish_text() -> None:
    """Model off (the initial load): the English pick is ESEF's, but SCB still
    contributed a candidate, so its Swedish original is what description_sv carries."""
    outcome = merge_company_info(COMPANY, [
        _scb(description="Konsultverksamhet inom IT.", activity_description_en="IT consulting."),
        _esef("Alpha builds payment software for retailers."),
    ])
    assert outcome.needs_model and outcome.description_sources[0] == "esef"
    assert outcome.description_sv == "Konsultverksamhet inom IT."
    # Kept unmutated beside it, so info.py can still show what the pipeline computed.
    assert outcome.description_sv_candidate == "Konsultverksamhet inom IT."


# Task 17 (owner decision 2026-08-23): description_source is gone. One boolean --
# llm_enhanced -- says whether the PUBLISHED text came out of the model. Where the
# candidates came from is still recorded (description_sources /
# description_source_record_uids), and reviewer involvement in correction_ids, so the
# flag answers exactly one question and nothing else.


def test_a_copied_description_is_never_llm_enhanced() -> None:
    """One input, copied as-is: the merge alone can never set the flag, whichever source
    the text came from."""
    for rows in (
        [_scb(description="Saeljer programvara.")],
        [_scb(description=None), _wikidata("Swedish fintech company")],
        [_scb(description=None), _esef("Alpha builds payment software.")],
    ):
        outcome = merge_company_info(COMPANY, rows)
        assert outcome.description is not None and not outcome.llm_enhanced


def test_the_deterministic_multi_source_pick_is_not_llm_enhanced() -> None:
    """The initial load runs with the model OFF: several sources offer a description, the
    highest-priority one is published unchanged -- copied, not written, so the flag is
    down even though the row is flagged needs_model."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b"), _esef("c")])
    assert outcome.needs_model and outcome.description == "c" and not outcome.llm_enhanced


def test_a_company_with_no_description_is_not_llm_enhanced() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description=None)])
    assert outcome.description is None and not outcome.llm_enhanced


def test_the_legal_form_labels_are_copied_from_the_register() -> None:
    """Both labels are SCB's, copied like every other non-description field -- the model is
    never asked what a legal form is called, and the reviewer never overrides it (a field
    value naming anything but the two descriptions is skipped as invalid)."""
    outcome = merge_company_info(COMPANY, [_scb(description="Säljer programvara.")])
    assert outcome is not None
    assert outcome.legal_form_code == "AB-ORGFO"
    assert outcome.legal_form_label_en == "Limited company (aktiebolag)"
    assert outcome.legal_form_label_sv == "Aktiebolag"


def test_a_code_with_no_curated_label_publishes_empty_labels() -> None:
    """A legal_form_code the curated dictionary does not name reaches the artifact as ''
    (the SELECT's ifNull on the join miss), and the merge copies that through rather than
    inventing a label or dropping the company."""
    outcome = merge_company_info(
        COMPANY,
        [_scb(description="Säljer programvara.", legal_form_code="ZZZ",
              legal_form_label_en="", legal_form_label_sv="")],
    )
    assert outcome is not None
    assert outcome.legal_form_code == "ZZZ"
    assert (outcome.legal_form_label_en, outcome.legal_form_label_sv) == ("", "")


# Field values (2026-09-01): the correction ledger is gone. A field's live value is
# simply the latest row written for it -- the greatest (created_at, value_id) -- and a
# NULL value releases the field back to whatever the pipeline computed. No kinds, no
# evidence-hash staleness, no undo chain.


def test_the_latest_row_wins_per_field() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    rows = [
        _value(2, "description", "Second", at=NOW + timedelta(seconds=1)),
        _value(1, "description", "First", at=NOW),
    ]
    applied = apply_field_values(outcome, rows, stored=())
    assert applied.description == "Second"
    # Only the live row is applied -- the superseded one is history, not a decision.
    assert applied.correction_ids == (uuid.UUID(int=2),)
    assert applied.invalid_value_count == 0


def test_the_value_id_breaks_a_created_at_tie() -> None:
    """Two rows written in the same millisecond still order deterministically, so the
    published row cannot flip between runs."""
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    rows = [_value(9, "description", "Higher id"), _value(3, "description", "Lower id")]
    applied = apply_field_values(outcome, rows, stored=())
    assert applied.description == "Higher id" and applied.correction_ids == (uuid.UUID(int=9),)
    assert apply_field_values(outcome, list(reversed(rows)), stored=()).description == "Higher id"


def test_a_null_value_releases_the_field_to_the_computed_default() -> None:
    """Undo is a row, not a kind: write NULL and the pipeline's own text is published
    again, with the outcome's provenance left exactly as computed."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    rows = [
        _value(1, "description", "Reviewed text", at=NOW),
        _value(2, "description", None, at=NOW + timedelta(seconds=1)),
    ]
    applied = apply_field_values(outcome, rows, stored=())
    assert applied.description == outcome.description == "b"
    assert applied.description_sv == outcome.description_sv
    assert not applied.llm_enhanced and applied.suggestion_id is None
    assert applied.model_name == outcome.model_name and applied.prompt_version == outcome.prompt_version
    # A released field is not an applied one, and the release hands the row back to the
    # pipeline whole -- needs_model included.
    assert applied.correction_ids == () and applied.needs_model


def test_the_two_description_fields_are_independent() -> None:
    """One field released while the other is set: the live row is found per field, so
    releasing the English text cannot blank the Swedish one."""
    outcome = merge_company_info(COMPANY, [_scb(description="Saeljer programvara.")])
    rows = [
        _value(1, "description_sv", "Granskad text"),
        _value(2, "description", "English text", at=NOW),
        _value(3, "description", None, at=NOW + timedelta(seconds=1)),
    ]
    applied = apply_field_values(outcome, rows, stored=())
    assert applied.description == outcome.description == "Saeljer programvara."
    assert applied.description_sv == "Granskad text"
    assert applied.correction_ids == (uuid.UUID(int=1),)


def test_a_swedish_field_value_changes_nothing_else() -> None:
    """description_sv carries no provenance of its own -- it is one string, and the
    English half's model flags are untouched by it."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    applied = apply_field_values(outcome, [_value(1, "description_sv", "  Granskad text  ")], stored=())
    assert applied.description_sv == "Granskad text"  # trimmed, like every other stored text
    assert applied.description == outcome.description
    assert applied.needs_model  # only a description decides the model is no longer needed
    assert (applied.model_provider, applied.model_name) == (outcome.model_provider, outcome.model_name)


def test_a_reviewer_value_is_deterministic_and_clears_a_model_answer() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    stored = StoredObservation(uuid.UUID(int=60), COMPANY, "h" * 64,
                               {"description": "Merged text", "language": "en"}, "deepseek", "m", "v3", NOW)
    from_model = _value(61, "description", "Merged text", source="llm",
                        source_ref=str(uuid.UUID(int=60)), at=NOW)
    assert apply_field_values(outcome, [from_model], stored=[stored]).llm_enhanced
    reviewed = apply_field_values(
        outcome,
        [from_model, _value(62, "description", "Reviewed text", at=NOW + timedelta(seconds=1))],
        stored=[stored],
    )
    assert reviewed.description == "Reviewed text" and not reviewed.llm_enhanced
    assert reviewed.suggestion_id is None
    assert (reviewed.model_provider, reviewed.model_name, reviewed.prompt_version) == (
        "deterministic", "field-value:reviewer", "")


def test_every_non_llm_source_publishes_deterministic_provenance() -> None:
    """Picking a source's own text off the About card is a copy, not a model answer."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    for number, source in enumerate(("scb", "esef", "wikidata", "reviewer"), start=70):
        applied = apply_field_values(
            outcome, [_value(number, "description", "Chosen text", source=source, source_ref="scb:1")], stored=()
        )
        assert applied.description == "Chosen text", source
        assert not applied.llm_enhanced and applied.suggestion_id is None, source
        assert applied.model_provider == "deterministic", source
        assert applied.model_name == f"field-value:{source}", source
        assert applied.prompt_version == "", source
        assert applied.correction_ids == (uuid.UUID(int=number),), source


def test_an_llm_value_copies_the_matching_observations_provenance() -> None:
    """source_ref is the suggestion id, so the published row still names the model that
    wrote the text -- and takes the observation's language with it."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    stored = StoredObservation(
        uuid.UUID(int=40), COMPANY, "h" * 64,
        {"description": "Merged text", "description_sv": "Sammanslagen text", "language": "sv"},
        "deepseek", "deepseek-v3", "v9", NOW)
    applied = apply_field_values(
        outcome,
        [_value(41, "description", "Merged text", source="llm", source_ref=str(uuid.UUID(int=40)))],
        stored=[stored],
    )
    assert applied.description == "Merged text" and applied.llm_enhanced
    assert applied.suggestion_id == uuid.UUID(int=40)
    assert (applied.model_provider, applied.model_name, applied.prompt_version) == (
        "deepseek", "deepseek-v3", "v9")
    assert applied.description_language == "sv"
    assert not applied.needs_model and applied.correction_ids == (uuid.UUID(int=41),)


def test_an_llm_value_whose_observation_has_no_language_keeps_the_computed_one() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    stored = StoredObservation(uuid.UUID(int=42), COMPANY, "h" * 64, {"description": "Merged text"},
                               "deepseek", "deepseek-v3", "v9", NOW)
    applied = apply_field_values(
        outcome,
        [_value(43, "description", "Merged text", source="llm", source_ref=str(uuid.UUID(int=42)))],
        stored=[stored],
    )
    assert applied.description_language == outcome.description_language == "en"


def test_an_llm_value_with_no_matching_observation_falls_back_to_field_value_provenance() -> None:
    """The observation may have aged out of the enrichment table; the decision still
    stands, flagged as the model's text with a provenance that says where it came from."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    applied = apply_field_values(
        outcome,
        [_value(44, "description", "Merged text", source="llm", source_ref=str(uuid.UUID(int=99)))],
        stored=(),
    )
    assert applied.description == "Merged text" and applied.llm_enhanced
    assert applied.suggestion_id == uuid.UUID(int=99)
    assert (applied.model_provider, applied.model_name, applied.prompt_version) == ("llm", "field-value:llm", "")
    assert applied.description_language == outcome.description_language


def test_an_llm_value_with_an_unparseable_source_ref_publishes_no_suggestion_id() -> None:
    """A bad source_ref never raises -- the text is still the model's, it just names no
    suggestion."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    applied = apply_field_values(
        outcome, [_value(45, "description", "Merged text", source="llm", source_ref="not-a-uuid")], stored=()
    )
    assert applied.description == "Merged text" and applied.llm_enhanced
    assert applied.suggestion_id is None
    assert (applied.model_provider, applied.model_name) == ("llm", "field-value:llm")
    assert applied.correction_ids == (uuid.UUID(int=45),)


def test_setting_a_description_clears_needs_model() -> None:
    """A decided field is decided: the row is not queued for the model again."""
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    assert outcome.needs_model
    applied = apply_field_values(outcome, [_value(1, "description", "Reviewed text")], stored=())
    assert applied.description == "Reviewed text" and not applied.needs_model


def test_unknown_fields_and_sources_are_skipped_and_counted() -> None:
    """Malformed rows can only reach here through a direct INSERT (the table's CHECKs and
    the backoffice validator both refuse them), so they are dropped and counted -- never
    raised on, and never allowed to shadow a valid row for the same field."""
    outcome = merge_company_info(COMPANY, [_scb(description="x")])
    rows = [
        _value(1, "legal_name", "New Name AB", at=NOW + timedelta(seconds=9)),
        _value(2, "status", "dissolved", at=NOW + timedelta(seconds=9)),
        _value(3, "description", "From nowhere", source="mystery", at=NOW + timedelta(seconds=9)),
        _value(4, "description", "Reviewed text", at=NOW),
    ]
    applied = apply_field_values(outcome, rows, stored=())
    assert applied.invalid_value_count == 3
    assert applied.legal_name == "Alpha AB" and applied.status == outcome.status
    assert applied.description == "Reviewed text"
    assert applied.correction_ids == (uuid.UUID(int=4),)


def test_no_rows_leave_the_outcome_as_computed() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    applied = apply_field_values(outcome, [], stored=())
    assert applied == outcome
    assert applied.correction_ids == () and applied.invalid_value_count == 0


def test_apply_field_values_does_not_mutate_the_outcome_it_is_given() -> None:
    outcome = merge_company_info(COMPANY, [_scb(description="a"), _wikidata("b")])
    applied = apply_field_values(
        outcome,
        [_value(1, "description", "Reviewed text"), _value(2, "description_sv", "Granskad text")],
        stored=(),
    )
    assert outcome.description == "b" and outcome.description_sv == "a" and outcome.needs_model
    assert applied.description == "Reviewed text" and applied.description_sv == "Granskad text"
    assert applied.correction_ids == (uuid.UUID(int=1), uuid.UUID(int=2))
