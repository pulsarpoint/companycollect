import re

_NON_DIGITS = re.compile(r"[^0-9]")


def normalize_sweden_identity(value: str) -> str:
    """Return the registry identity used by ``se_companies``.

    Swedish legal entities sometimes appear in 12-digit PeOrgNr form with a
    ``16`` prefix. Person-keyed identifiers use birth-century prefixes such
    as ``19`` or ``20`` and deliberately remain 12 digits, so downstream
    company matching can reject them rather than attach them to a company.
    """
    digits = _NON_DIGITS.sub("", value)
    if len(digits) == 12 and digits.startswith("16"):
        return digits[2:]
    return digits


def sweden_identity_sql(raw_column: str) -> str:
    """Return DuckDB SQL equivalent to :func:`normalize_sweden_identity`."""
    digits = f"regexp_replace(coalesce({raw_column}, ''), '[^0-9]', '', 'g')"
    return (
        f"case when length({digits}) = 12 and {digits} like '16%' "
        f"then substring({digits}, 3) else {digits} end"
    )
