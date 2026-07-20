"""Tests for the OIM xBRL-JSON fact parser (`facts.parse_oim_facts`).

Uses the real (trimmed) fixture captured in Task 2 --
tests/fixtures/esef_filings/facts_sample.json -- for the representative
cases, plus synthetic fact entries built inline for edge cases the fixture
doesn't cover (missing concept, unparseable monetary value, non-monetary
unit, malformed shapes). No network, no DuckDB.
"""

import dataclasses
import decimal
import json
from pathlib import Path
from typing import Any

from dagster_v3.defs.esef_filings import facts
from dagster_v3.defs.esef_filings import tables

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "esef_filings"

LEI = "259400OOMJ31L0SWCY70"
FXO_ID = "259400OOMJ31L0SWCY70-2022-12-31-ESEF-PL-0"
PERIOD_END = "2022-12-31"


def _fixture_payload() -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / "facts_sample.json").read_text())


def _parse_fixture() -> list[facts.EsefFact]:
    return facts.parse_oim_facts(
        _fixture_payload(), lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END
    )


def _by_fact_id(rows: list[facts.EsefFact]) -> dict[str, facts.EsefFact]:
    return {row.fact_id: row for row in rows}


# --------------------------------------------------------------------------
# Alignment contract: EsefFact fields <-> tables.ESEF_FACTS_EXPORT_COLUMNS
# --------------------------------------------------------------------------


def test_esef_fact_fields_align_with_export_columns() -> None:
    field_names = tuple(field.name for field in dataclasses.fields(facts.EsefFact))
    # source_run_id is appended at insert time by the asset, not by the parser.
    assert field_names == tables.ESEF_FACTS_EXPORT_COLUMNS[:-1]
    assert tables.ESEF_FACTS_EXPORT_COLUMNS[-1] == "source_run_id"


# --------------------------------------------------------------------------
# Fixture-driven cases
# --------------------------------------------------------------------------


def test_parses_every_well_formed_fact_in_the_fixture() -> None:
    rows = _parse_fixture()
    assert len(rows) == 6
    assert all(row.lei == LEI for row in rows)
    assert all(row.fxo_id == FXO_ID for row in rows)
    assert all(row.period_end == PERIOD_END for row in rows)


def test_monetary_fact_value_currency_decimals() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID398"]
    assert row.concept_qname == "ifrs-full:Assets"
    assert row.concept_namespace == "ifrs-full"
    assert row.concept_local_name == "Assets"
    assert row.value_kind == "monetary"
    assert row.unit == "iso4217:PLN"
    assert row.currency == "PLN"
    assert row.raw_value == "438395039.49"
    assert row.amount_original == decimal.Decimal("438395039.49")
    assert row.decimals == 2


def test_instant_period_sets_period_instant_not_period_start() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID398"]
    assert row.period_instant == "2022-01-01"
    assert row.period_start is None


def test_instant_fact_has_no_period_duration_end() -> None:
    # Finding 1 regression: an instant fact must never carry a
    # period_duration_end -- only duration facts do.
    row = _by_fact_id(_parse_fixture())["caTagID398"]
    assert row.period_duration_end is None


def test_duration_period_sets_period_start_not_period_instant() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID1463"]
    assert row.concept_qname == "ifrs-full:Revenue"
    assert row.period_start == "2021-01-01"
    assert row.period_instant is None


def test_duration_fact_carries_its_true_period_duration_end() -> None:
    # Finding 1 fix: the duration's own end date ("2021-01-01T00:00:00/
    # 2022-01-01T00:00:00" -> end "2022-01-01") must be parsed and stored,
    # not discarded -- this is what lets metrics.py structurally exclude a
    # prior-year comparative duration fact instead of relying on the
    # filing-level period_end alone.
    row = _by_fact_id(_parse_fixture())["caTagID1463"]
    assert row.period_duration_end == "2022-01-01"


def test_text_fact_has_no_unit_currency_or_amount() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID1647"]
    assert row.concept_qname == "ifrs-full:LegalFormOfEntity"
    assert row.value_kind == "text"
    assert row.unit == ""
    assert row.currency == ""
    assert row.amount_original is None
    assert row.language == "pl"
    assert row.raw_value == "Formą prawną jednostki jest Spółka Akcyjna."


def test_extension_taxonomy_dimension_is_serialized_sorted_json() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID1963"]
    assert row.dimensions == json.dumps(
        {"ifrs-full:ComponentsOfEquityAxis": "ifrs-full:IssuedCapitalMember"},
        sort_keys=True,
    )
    # Core dims (concept/entity/period/unit) must NOT leak into `dimensions`.
    assert "concept" not in row.dimensions
    assert "unit" not in row.dimensions


def test_facts_without_extra_dimensions_get_empty_string() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID398"]
    assert row.dimensions == ""


# --------------------------------------------------------------------------
# Synthetic edge cases the fixture doesn't cover
# --------------------------------------------------------------------------


def _synthetic_payload(*facts_entries: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    return {"facts": dict(facts_entries)}


def test_fact_missing_dimensions_concept_is_skipped() -> None:
    payload = _synthetic_payload(
        (
            "good1",
            {
                "value": "100",
                "dimensions": {
                    "concept": "ifrs-full:Assets",
                    "period": "2022-12-31T00:00:00",
                    "unit": "iso4217:EUR",
                },
            },
        ),
        (
            "bad-no-concept",
            {
                "value": "200",
                "dimensions": {
                    "period": "2022-12-31T00:00:00",
                    "unit": "iso4217:EUR",
                },
            },
        ),
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    assert rows[0].fact_id == "good1"


def test_fact_with_blank_concept_string_is_skipped() -> None:
    payload = _synthetic_payload(
        (
            "bad-blank-concept",
            {"value": "1", "dimensions": {"concept": "", "unit": "iso4217:EUR"}},
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert rows == []


def test_fact_entry_not_a_dict_is_skipped() -> None:
    payload = {"facts": {"weird": "not-a-dict"}}

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert rows == []


def test_fact_dimensions_not_a_dict_is_skipped() -> None:
    payload = {"facts": {"weird": {"value": "1", "dimensions": "not-a-dict"}}}

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert rows == []


def test_payload_missing_facts_key_returns_empty_list() -> None:
    assert (
        facts.parse_oim_facts({}, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END) == []
    )


def test_monetary_unit_with_unparseable_value_keeps_kind_and_raw_value() -> None:
    payload = _synthetic_payload(
        (
            "bad-value",
            {
                "value": "not-a-number",
                "dimensions": {
                    "concept": "ifrs-full:Assets",
                    "period": "2022-12-31T00:00:00",
                    "unit": "iso4217:EUR",
                },
            },
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    row = rows[0]
    assert row.value_kind == "monetary"
    assert row.amount_original is None
    assert row.raw_value == "not-a-number"
    assert row.currency == "EUR"


def test_monetary_unit_with_empty_value_string_keeps_kind_and_amount_none() -> None:
    payload = _synthetic_payload(
        (
            "empty-value",
            {
                "value": "",
                "dimensions": {
                    "concept": "ifrs-full:Assets",
                    "period": "2022-12-31T00:00:00",
                    "unit": "iso4217:EUR",
                },
            },
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    assert rows[0].amount_original is None
    assert rows[0].raw_value == ""


def test_non_monetary_unit_is_classified_numeric_with_empty_currency() -> None:
    payload = _synthetic_payload(
        (
            "shares",
            {
                "value": "1000",
                "dimensions": {
                    "concept": "ifrs-full:NumberOfSharesOutstanding",
                    "period": "2022-12-31T00:00:00",
                    "unit": "shares",
                },
            },
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    row = rows[0]
    assert row.value_kind == "numeric"
    assert row.currency == ""
    assert row.amount_original == decimal.Decimal("1000")


def test_concept_qname_without_colon_has_empty_namespace() -> None:
    payload = _synthetic_payload(
        (
            "no-colon",
            {
                "value": "1",
                "dimensions": {"concept": "JustALocalName", "unit": "iso4217:EUR"},
            },
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    assert rows[0].concept_namespace == ""
    assert rows[0].concept_local_name == "JustALocalName"


def test_fact_missing_period_yields_none_start_and_instant() -> None:
    payload = _synthetic_payload(
        (
            "no-period",
            {
                "value": "1",
                "dimensions": {"concept": "ifrs-full:Assets", "unit": "iso4217:EUR"},
            },
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    assert rows[0].period_start is None
    assert rows[0].period_instant is None
    assert rows[0].period_duration_end is None


def test_fact_with_no_unit_and_no_value_is_text_with_empty_raw_value() -> None:
    payload = _synthetic_payload(
        (
            "no-value-no-unit",
            {"dimensions": {"concept": "ifrs-full:CountryOfIncorporation"}},
        )
    )

    rows = facts.parse_oim_facts(payload, lei=LEI, fxo_id=FXO_ID, period_end=PERIOD_END)

    assert len(rows) == 1
    row = rows[0]
    assert row.value_kind == "text"
    assert row.raw_value == ""
    assert row.amount_original is None
