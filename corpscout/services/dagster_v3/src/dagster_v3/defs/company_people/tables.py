"""Cross-country people-search table constants and per-source SELECT registry.

``company_people_all`` is a per-source-row search table, mirroring
``companies_all``'s per-country SELECT registry (``companies_all/sql.py``'s
``SOURCES``) but keyed on a source *slug* rather than a country code, since
more than one source can eventually cover the same country (e.g. a future
Norway BRREG roles source would sit alongside a Norway companies-register
source). Identity resolution is deliberately OUT of scope here -- this table
is one row per (source, company, fiscal_year, person, signatory_kind); same-
identifier hard links and name-based "possible matches" are separate layers
built on top (see the company_people plan's Task 4 self-review notes).

``PEOPLE_SOURCES`` maps a source slug to a bare ``SELECT`` (no ``INSERT
INTO`` wrapper, no trailing semicolon) that produces exactly the
``COMPANY_PEOPLE_ALL_COLUMNS`` column list, in that order and under those
exact aliases -- the asset builder (``company_people.assets
.build_company_people_all_insert_sql``) concatenates every entry with
``UNION ALL`` positionally, so a new source added here must match the
column *order*, not just the column *names*. Adding a future source (NO
roles, EE officers, BR socios) is exactly one new dict entry.

The single entry today, ``se_xbrl_signatures``, reads Task 1's
``corpscout.se_company_officers`` (one row per person per signatory
occurrence within one statement -- a person who signs in both the
``board_signature`` handling block and the annual-report-representative
block, or across multiple concept triples, can appear more than once for
the same (company, fiscal_year, signatory_kind) before this dedupe). This
SELECT collapses those down to one row per
``(company_id, fiscal_year, first_name, last_name, signatory_kind)`` via
``GROUP BY``, picking the row-set's best role via ``argMax(..., role_kind !=
'unknown')`` -- the same "prefer a resolved role over the 'unknown'
fallback" preference used by the plan's Task 4 backoffice officers query
(``argMax(role_original, role_kind != 'unknown')``). ``company_name`` is
joined from ``se_companies`` on ``registration_number`` (== ``company_id``
in officer rows per the plan's identity note) and wrapped in ``any(...)``
rather than added to the ``GROUP BY`` key: ``se_companies`` is a
``ReplacingMergeTree`` and unmerged parts can transiently hold more than one
physical row per ``registration_number``, so the join can fan out before
this GROUP BY collapses it back down -- aggregating (instead of grouping)
guards against that fan-out ever changing this SELECT's *output* row count.
``legal_name`` itself is ``Nullable(String)`` even on a real match (migration
000084), hence the ``coalesce(..., '')``.

A driver-params placeholder (``%(resolved_at)s``) is embedded directly in
this SELECT rather than an inline ``now64(3)`` literal, mirroring Task 1's
``officers.py`` (not ``companies_all``, which has no params dict): the
builder in ``assets.py`` executes the combined ``INSERT`` via
``client.execute(sql, {"resolved_at": ..., "source_run_id": ...})``, so
``resolved_at`` is deterministic and test-controlled rather than wall-clock
``now64(3)`` baked into the SQL text. This SELECT has no ``LIKE``/``ILIKE``
literals, so the %-escaping discipline that doubles ``%`` in ``officers.py``
does not apply here -- flagged for whoever adds the next source with a
``LIKE`` pattern.
"""

COMPANY_PEOPLE_ALL_TABLE = "company_people_all"
QUALIFIED_COMPANY_PEOPLE_ALL_TABLE = f"corpscout.{COMPANY_PEOPLE_ALL_TABLE}"

# Schema order -- MUST match migration 000145's column order exactly (a
# contract test greps the migration file for each of these, mirroring
# officers.py's SE_COMPANY_OFFICERS_COLUMNS contract).
COMPANY_PEOPLE_ALL_COLUMNS = (
    "country_iso2",
    "company_id",
    "company_name",
    "first_name",
    "last_name",
    "full_name_normalized",
    "role_original",
    "role_kind",
    "signatory_kind",
    "fiscal_year",
    "identifier_kind",
    "identifier_value",
    "source",
    "source_statement_key",
    "resolved_at",
)

_SE_XBRL_SIGNATURES_SELECT = """SELECT
    'SE' AS country_iso2,
    o.company_id AS company_id,
    any(coalesce(c.legal_name, '')) AS company_name,
    o.first_name AS first_name,
    o.last_name AS last_name,
    lowerUTF8(trim(concat(o.first_name, ' ', o.last_name))) AS full_name_normalized,
    argMax(o.role_original, o.role_kind != 'unknown') AS role_original,
    argMax(o.role_kind, o.role_kind != 'unknown') AS role_kind,
    o.signatory_kind AS signatory_kind,
    o.fiscal_year AS fiscal_year,
    '' AS identifier_kind,
    '' AS identifier_value,
    'se_xbrl_signatures' AS source,
    argMax(o.statement_key, o.role_kind != 'unknown') AS source_statement_key,
    %(resolved_at)s AS resolved_at
FROM corpscout.se_company_officers AS o
LEFT JOIN corpscout.se_companies AS c ON c.registration_number = o.company_id
GROUP BY o.company_id, o.fiscal_year, o.first_name, o.last_name, o.signatory_kind"""

# code -> bare SELECT producing exactly COMPANY_PEOPLE_ALL_COLUMNS, in order.
# See the module docstring for the UNION ALL contract new entries must honor.
PEOPLE_SOURCES: dict[str, str] = {
    "se_xbrl_signatures": _SE_XBRL_SIGNATURES_SELECT,
}
