"""Tests for the canonical Arelle-artifact fact contract."""

import dataclasses
import decimal

import pytest

from dagster_v3.defs.esef_filings import facts
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION

LEI = "259400OOMJ31L0SWCY70"
FXO_ID = "259400OOMJ31L0SWCY70-2022-12-31-ESEF-PL-0"
PERIOD_END = "2022-12-31"


# --------------------------------------------------------------------------
# Alignment contract: EsefFact fields <-> tables.ESEF_FACTS_EXPORT_COLUMNS
# --------------------------------------------------------------------------


def test_esef_fact_fields_align_with_export_columns() -> None:
    field_names = tuple(field.name for field in dataclasses.fields(facts.EsefFact))
    # source_run_id is appended at insert time by the asset, not by the parser.
    assert field_names == tables.ESEF_FACTS_EXPORT_COLUMNS[:-1]
    assert tables.ESEF_FACTS_EXPORT_COLUMNS[-1] == "source_run_id"


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
