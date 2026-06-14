import polars as pl
import pytest

from conformance.schemas import REGISTRATIONS
from conformance.validate import validate_table


def _minimal_registration_row() -> dict:
    return {name: None for name in REGISTRATIONS}


def test_validate_passes_for_correct_columns():
    df = pl.DataFrame([_minimal_registration_row()], schema=REGISTRATIONS)
    validate_table(df, REGISTRATIONS, unique_key="registration_uid")  # no raise


def test_validate_rejects_missing_column():
    df = pl.DataFrame([_minimal_registration_row()], schema=REGISTRATIONS).drop("country")
    with pytest.raises(ValueError, match="missing columns"):
        validate_table(df, REGISTRATIONS)


def test_validate_rejects_duplicate_key():
    row = _minimal_registration_row() | {"registration_uid": "FI:1"}
    df = pl.DataFrame([row, row], schema=REGISTRATIONS)
    with pytest.raises(ValueError, match="duplicate"):
        validate_table(df, REGISTRATIONS, unique_key="registration_uid")
