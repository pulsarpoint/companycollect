"""Accent-preserving, engine-portable join keys for SE centroid derivation.

`city_key_sql` and `postcode_key_sql` each return a single SQL fragment that
is used VERBATIM on both sides of the centroid join: the DuckDB derivation
side (address points -> centroids) and the ClickHouse serving side (overlay
read). The two engines must therefore compute the identical key string for
the identical input, or the join silently drops rows.

This is a narrow, scoped fix: `city_key_sql` PRESERVES Swedish letters
(å ä ö). It is not a replacement for the resolver's own accent-stripping
normalization (the `ln` column built via `strip_accents(...)` in
`address_canonicalization.py`, and reproduced under ClickHouse's ASCII-only
regex as the ~16%-match diacritic bug the design doc describes) -- that key
intentionally folds accents for a different matching pass and must not be
changed here.

Portability findings (verified empirically 2026-08-26 against local DuckDB
and `clickhouse/clickhouse-server:26.5` via `clickhouse-local`):

- `nfc_normalize` does not exist in ClickHouse (`UNKNOWN_FUNCTION`), so it is
  not usable in a fragment shared by both engines.
- Plain `upper()` is Unicode-aware in DuckDB but ASCII-only in ClickHouse:
  `upper('Göteborg')` -> `'GÖTEBORG'` in DuckDB but `'GöTEBORG'` in
  ClickHouse (lowercase å/ä/ö pass through untouched). The literal
  `upper(trim(coalesce(expr, '')))` fallback is therefore NOT portable on
  its own -- it would desync the two engines' keys for any city name typed
  with a lowercase diacritic (e.g. "Göteborg", "Malmö"). Fixed here by
  chasing `upper()` with explicit `replace()` calls for the three Swedish
  letters; `replace()` is a plain literal-text substitution that behaves
  identically in both engines.
- `regexp_replace(expr, pattern, replacement, 'g')` (4 args) is DuckDB-only:
  ClickHouse's `regexp_replace` (`replaceRegexpAll`) takes exactly 3
  arguments and raises `NUMBER_OF_ARGUMENTS_DOESNT_MATCH` on a 4th. Dropping
  the 4th argument flips the DuckDB semantics instead: ClickHouse's 3-arg
  form already replaces every match, but DuckDB's 3-arg form replaces only
  the first. Fixed by matching whole non-digit RUNS (`[^0-9]+`, so one
  postcode separator = one match) and calling `regexp_replace` a fixed
  number of times: ClickHouse's later passes are harmless no-ops (nothing
  left to match) and DuckDB peels off one run per pass.
  `_POSTCODE_REPLACE_PASSES` is set comfortably above the number of
  separator runs any real postcode string should contain.
"""

_POSTCODE_REPLACE_PASSES = 5

# Swedish letters that must survive city_key_sql's uppercasing unchanged.
_SWEDISH_LOWER_TO_UPPER = (("å", "Å"), ("ä", "Ä"), ("ö", "Ö"))


def city_key_sql(expr: str) -> str:
    """Accent-preserving city join key: upper(trim(...)) with å/ä/ö intact.

    Engine-portable: produces identical output in DuckDB and ClickHouse for
    the same input (see module docstring for the empirical evidence). Do NOT
    reuse the resolver's accent-STRIPPING key here -- this one is
    deliberately the opposite.
    """
    key_sql = f"upper(trim(coalesce({expr}, '')))"
    for lower, upper_letter in _SWEDISH_LOWER_TO_UPPER:
        key_sql = f"replace({key_sql}, '{lower}', '{upper_letter}')"
    return key_sql


def postcode_key_sql(expr: str) -> str:
    """Digits-only postcode join key, e.g. `"231 00"` -> `"23100"`.

    Engine-portable: see module docstring for why a plain 4-arg
    `regexp_replace(..., 'g')` is not usable against ClickHouse.
    """
    key_sql = f"coalesce({expr}, '')"
    for _pass in range(_POSTCODE_REPLACE_PASSES):
        key_sql = f"regexp_replace({key_sql}, '[^0-9]+', '')"
    return key_sql
