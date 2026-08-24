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

import pytest

from dagster_v3.defs.esef_filings import facts
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION

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


def test_iter_oim_facts_consumes_fact_entries_lazily() -> None:
    consumed_fact_ids: list[str] = []

    class TrackingFactMap(dict[str, Any]):
        def items(self):
            for fact_id, entry in super().items():
                consumed_fact_ids.append(fact_id)
                yield fact_id, entry

    payload = _fixture_payload()
    payload["facts"] = TrackingFactMap(payload["facts"])
    rows = facts.iter_oim_facts(
        payload,
        lei=LEI,
        fxo_id=FXO_ID,
        period_end=PERIOD_END,
    )

    assert consumed_fact_ids == []
    first_row = next(rows)
    assert consumed_fact_ids == [first_row.fact_id]
    assert len(list(rows)) == 5


@pytest.mark.parametrize("schema_version", [4, ARTIFACT_SCHEMA_VERSION])
def test_iter_artifact_facts_reuses_oim_row_contract_with_deterministic_ids(
    schema_version: int,
) -> None:
    artifact = {
        "schema_version": schema_version,
        "facts": {
            "fact-key-a": {
                "report_member": "reports/report.xhtml",
                "ordinal": 1,
                "source_fact_id": "revenue",
                "canonical_value": "325100000",
                "decimals": -5,
                "oim_dimensions": {
                    "concept": "ifrs-full:Revenue",
                    "entity": "lei:549300SAMPLE000000001",
                    "period": "2024-01-01T00:00:00/2025-01-01T00:00:00",
                    "unit": "iso4217:SEK",
                },
            },
            "fact-key-b": {
                "report_member": "reports/report.xhtml",
                "ordinal": 2,
                "source_fact_id": "revenue",
                "canonical_value": "310000000",
                "decimals": -5,
                "oim_dimensions": {
                    "concept": "ifrs-full:Revenue",
                    "entity": "lei:549300SAMPLE000000001",
                    "period": "2023-01-01T00:00:00/2024-01-01T00:00:00",
                    "unit": "iso4217:SEK",
                },
            },
        },
    }

    rows = list(
        facts.iter_artifact_facts(
            artifact,
            lei=LEI,
            fxo_id=FXO_ID,
            period_end="2024-12-31",
        )
    )

    assert [row.fact_id for row in rows] == ["revenue", "revenue#fact-key-b"]
    assert [row.amount_original for row in rows] == [
        decimal.Decimal("325100000"),
        decimal.Decimal("310000000"),
    ]
    assert [row.period_duration_end for row in rows] == [
        "2024-12-31",
        "2023-12-31",
    ]
    assert all(row.currency == "SEK" for row in rows)
    assert all(row.decimals == -5 for row in rows)


@pytest.mark.parametrize(
    ("base_xsd_type", "expected_value"),
    [
        ("date", "2025-12-31"),
        ("string", "2025-12-31T00:00:00"),
    ],
)
def test_iter_artifact_facts_normalizes_only_date_typed_midnight_values(
    base_xsd_type: str,
    expected_value: str,
) -> None:
    concept_qname = "ifrs-full:DateOfEndOfReportingPeriod2013"
    artifact = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "concepts": {
            concept_qname: {
                "base_xsd_type": base_xsd_type,
            },
        },
        "facts": {
            "date-fact-key": {
                "report_member": "reports/report.xhtml",
                "ordinal": 1,
                "source_fact_id": "reporting-period-end",
                "canonical_value": "2025-12-31T00:00:00",
                "decimals": None,
                "oim_dimensions": {
                    "concept": concept_qname,
                    "entity": "lei:549300SAMPLE000000001",
                    "period": "2025-01-01T00:00:00/2026-01-01T00:00:00",
                },
            },
        },
    }

    row = next(
        facts.iter_artifact_facts(
            artifact,
            lei=LEI,
            fxo_id=FXO_ID,
            period_end="2025-12-31",
        )
    )

    assert row.raw_value == expected_value
    assert row.value_kind == "text"
    assert row.amount_original is None


def test_iter_artifact_facts_rejects_unknown_artifact_schema() -> None:
    with pytest.raises(ValueError, match="unsupported schema version"):
        list(
            facts.iter_artifact_facts(
                {"schema_version": 3, "facts": {}},
                lei=LEI,
                fxo_id=FXO_ID,
                period_end=PERIOD_END,
            )
        )


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
    # Finding C1 fix: caTagID399's raw OIM period is "2023-01-01T00:00:00"
    # -- the midnight-next-day encoding of this FY2022 filing's Dec-31
    # balance date. The parser must subtract the day back off, so the
    # *current-period* instant lands on "2022-12-31", matching
    # filing.period_end exactly (the metrics anchor's positive-match case).
    row = _by_fact_id(_parse_fixture())["caTagID399"]
    assert row.period_instant == "2022-12-31"
    assert row.period_start is None


def test_instant_fact_has_no_period_duration_end() -> None:
    # Finding 1 regression: an instant fact must never carry a
    # period_duration_end -- only duration facts do.
    row = _by_fact_id(_parse_fixture())["caTagID399"]
    assert row.period_duration_end is None


def test_prior_year_comparative_instant_parses_to_its_own_true_date() -> None:
    # Finding C1 fix: caTagID398's raw OIM period is "2022-01-01T00:00:00"
    # -- this filing's FY2021 comparative Assets instant, encoded under the
    # same midnight-next-day convention. Adjusted, it lands on "2021-12-31":
    # the *prior* fiscal year-end, a full year away from filing.period_end
    # ("2022-12-31"), never mistaken for the current period. This is what
    # keeps the metrics anchor's structural comparative exclusion intact.
    row = _by_fact_id(_parse_fixture())["caTagID398"]
    assert row.period_instant == "2021-12-31"
    assert row.period_start is None


def test_duration_period_sets_period_start_not_period_instant() -> None:
    row = _by_fact_id(_parse_fixture())["caTagID1463"]
    assert row.concept_qname == "ifrs-full:Revenue"
    assert row.period_start == "2021-01-01"
    assert row.period_instant is None


def test_duration_fact_carries_its_true_period_duration_end() -> None:
    # Finding 1 fix (end date parsed, not discarded) + Finding C1 fix (the
    # midnight-next-day adjustment applied to that end date): caTagID1463's
    # raw OIM period is "2021-01-01T00:00:00/2022-01-01T00:00:00" -- this
    # filing's FY2021 *comparative* Revenue duration. Adjusted, the end
    # lands on "2021-12-31": a full year before filing.period_end
    # ("2022-12-31"), which is what lets metrics.py structurally exclude it
    # instead of relying on the filing-level period_end alone.
    row = _by_fact_id(_parse_fixture())["caTagID1463"]
    assert row.period_duration_end == "2021-12-31"


def test_current_year_duration_fact_end_matches_filing_period_end() -> None:
    # Finding C1 fix: caTagID1647's raw OIM period is
    # "2022-01-01T00:00:00/2023-01-01T00:00:00" -- this filing's *current*
    # FY2022 duration. period_start needs no adjustment (start-of-day
    # already matches human convention); period_duration_end's midnight-
    # next-day encoding is adjusted back one day, landing exactly on
    # filing.period_end ("2022-12-31") -- the metrics anchor's
    # positive-match case for a duration fact.
    row = _by_fact_id(_parse_fixture())["caTagID1647"]
    assert row.period_start == "2022-01-01"
    assert row.period_duration_end == "2022-12-31"


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
