import pytest

from dagster_v3.defs.xbrl_common.transforms import (
    TransformResult,
    UnknownTransform,
    apply_transform,
)


@pytest.mark.parametrize(
    ("fmt", "raw", "expected"),
    [
        ("ixt:num-dot-decimal", "1,234,567.89", TransformResult("numeric", "1234567.89")),
        ("ixt:numdotdecimal", "1 234 567.89", TransformResult("numeric", "1234567.89")),
        ("ixt:num-dot-decimal", "1 234.5", TransformResult("numeric", "1234.5")),
        ("ixt:num-comma-decimal", "1.234.567,89", TransformResult("numeric", "1234567.89")),
        ("ixt:numcommadecimal", "1 234,5", TransformResult("numeric", "1234.5")),
        ("ixt:num-unit-decimal", "1 234 kr 56", TransformResult("numeric", "1234.56")),
        ("ixt:zerodash", "-", TransformResult("numeric", "0")),
        ("ixt:fixed-zero", "anything", TransformResult("numeric", "0")),
        ("ixt:fixed-empty", "anything", TransformResult("empty", "")),
        ("ixt:fixed-false", "x", TransformResult("boolean", "false")),
        ("ixt:fixed-true", "x", TransformResult("boolean", "true")),
        ("ixt:booleanfalse", "no", TransformResult("boolean", "false")),
        ("ixt:booleantrue", "yes", TransformResult("boolean", "true")),
        ("ixt:date-day-month-year", "31.12.2024", TransformResult("date", "2024-12-31")),
        ("ixt:datedaymonthyear", "31/12/2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-day-month-year", "1.1.2024", TransformResult("date", "2024-01-01")),
        ("ixt:date-year-month-day", "2024-12-31", TransformResult("date", "2024-12-31")),
        ("ixt:dateyearmonthday", "2024.12.31", TransformResult("date", "2024-12-31")),
        ("ixt:date-month-day-year", "12/31/2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-month-year", "12.2024", TransformResult("text", "2024-12")),
        ("ixt4:date-day-monthname-year-en", "31 December 2024", TransformResult("date", "2024-12-31")),
        ("ixt:datedaymonthnameyearen", "1 jan 2024", TransformResult("date", "2024-01-01")),
        ("ixt:date-day-monthname-year-sv", "31 december 2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-day-monthname-year-fi", "31 joulukuuta 2024", TransformResult("date", "2024-12-31")),
    ],
)
def test_apply_transform(fmt, raw, expected):
    assert apply_transform(fmt, raw) == expected


def test_unknown_transform_raises():
    with pytest.raises(UnknownTransform):
        apply_transform("ixt:date-tolkien-calendar", "3019-03-25")


def test_bad_numeric_input_raises_value_error():
    with pytest.raises(ValueError):
        apply_transform("ixt:num-dot-decimal", "not a number")
