"""The three SE person source VIEWS -- read contracts over the originals, no copies.

Per `docs/superpowers/specs/2026-08-27-se-people-experiment-design.md` section 3.1: history
and evidence for a Swedish person observation live in the ORIGINAL source table only
(`se_financial_report_signatories`, `esef_document_people`, `wikidata_company_people` +
`wikidata_persons`). Sweden gets a uniform read *shape*, not a duplicated copy -- these
three plain ClickHouse VIEWS project each source's rows into a common column prefix
(``company_id``, ``source_record_uid``, ``person_profile_hash``, ``person_role_hash``,
``full_name``) followed by per-source typed extras. The evidence hashes
(``person_profile_hash`` / ``person_role_hash``) are migration 000289's MATERIALIZED
columns on all four upstream tables -- this module reads them, it does not recompute them.

Each view is pinned by a drift test (tests/test_se_company_person_views.py) comparing the
migration-embedded `CREATE OR REPLACE VIEW` statement against a fresh render of the builder
below -- the `se_address_geocodes_served` pattern (sweden_company/geocode_serving_overlay.py).
A builder here is therefore the single source of truth; the migration file is a generated
rendering of it and must never be hand-edited without also updating the function.

THE WIKIDATA BRIDGE. `wikidata_company_people`/`wikidata_persons` carry no company_id --
only a Wikidata QID. The bridge to a Swedish org number runs through
`wikidata_company_identifiers`, exactly the join shape `company_people/draft.py`'s
`_source_observations_sql` uses for its wikidata branch:

- ``identifier_type = 'se_orgnr'``: the identifier VALUE (once stripped of separators) IS
  the company_id.
- ``identifier_type = 'lei'``: the identifier value is a LEI, translated to a company_id via
  ``corpscout.company_identifier`` (``country_code = 'SE'``, ``issuer_scheme = 'lei'``,
  ``is_current = 1``) -- the same table draft.py's ``company_leis`` CTE reads.

Unlike draft.py (which additionally restricts the bridge to an explicit list of already
-registered Swedish company ids passed as a parameter), this view has no caller-supplied
scope: it bridges every wikidata_company_identifiers row of either kind and instead
validates the RESULT, filtering to ``match(company_id, '^[0-9]{10}([0-9]{2})?$')`` --
a malformed or non-Swedish identifier value (a bad ``se_orgnr`` scrape, a LEI that resolves
to nothing in company_identifier) never gets a fabricated company_id.
"""

DATABASE = "corpscout"

SE_COMPANY_PERSON_BOLAGSVERKET_VIEW = f"{DATABASE}.se_company_person_bolagsverket"
SE_COMPANY_PERSON_ESEF_VIEW = f"{DATABASE}.se_company_person_esef"
SE_COMPANY_PERSON_WIKIDATA_VIEW = f"{DATABASE}.se_company_person_wikidata"

SE_FINANCIAL_REPORT_SIGNATORIES_TABLE = f"{DATABASE}.se_financial_report_signatories"
ESEF_DOCUMENT_PEOPLE_TABLE = f"{DATABASE}.esef_document_people"
WIKIDATA_COMPANY_PEOPLE_TABLE = f"{DATABASE}.wikidata_company_people"
WIKIDATA_PERSONS_TABLE = f"{DATABASE}.wikidata_persons"
WIKIDATA_COMPANY_IDENTIFIERS_TABLE = f"{DATABASE}.wikidata_company_identifiers"
COMPANY_IDENTIFIER_TABLE = f"{DATABASE}.company_identifier"

# The uniform Swedish-orgnr shape every view's company_id is validated against: 10 digits,
# or 12 for a coordination-number-style suffix. Matches the CHECK constraint on the Task 2
# collision-candidate table (migration 000330) -- one vocabulary for "a real SE company_id".
SE_COMPANY_ID_PATTERN = r"^[0-9]{10}([0-9]{2})?$"

SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE = (
    f"{DATABASE}.se_company_person_collision_candidate"
)


def _projection(columns: tuple[str, ...]) -> str:
    return ",\n    ".join(columns)


def build_se_company_person_bolagsverket_view_sql() -> str:
    """The Bolagsverket XBRL signatory read: split names, concatenated full_name.

    A plain column projection over ``se_financial_report_signatories`` (a non-versioned
    MergeTree -- no FINAL needed): the optimizer pushes this straight down, per spec 3.1's
    sizing note. ``full_name`` does not exist upstream; it is derived here exactly once
    (``trim(concat(first_name, ' ', last_name))``) so every consumer of the uniform shape
    reads the same rule instead of re-deriving it per caller.
    """
    columns = (
        "company_id",
        "source_record_uid",
        "person_profile_hash",
        "person_role_hash",
        "trim(concat(first_name, ' ', last_name)) AS full_name",
        "first_name",
        "last_name",
        "role_original",
        "role_kind",
        "signatory_kind",
        "fiscal_year",
    )
    return (
        f"CREATE OR REPLACE VIEW {SE_COMPANY_PERSON_BOLAGSVERKET_VIEW} AS\n"
        f"SELECT\n    {_projection(columns)}\n"
        f"FROM {SE_FINANCIAL_REPORT_SIGNATORIES_TABLE}"
    )


def build_se_company_person_esef_view_sql() -> str:
    """The ESEF LLM-extracted people read, filtered to Sweden.

    ``esef_document_people`` is multi-country and ``ReplacingMergeTree(extracted_at)`` --
    FINAL dedupes a re-enrichment in place, and ``country_code = 'SE'`` is the only country
    filter this experiment ever applies (never copied out of the source table, per spec 3.1).
    """
    columns = (
        "company_id",
        "source_record_uid",
        "person_profile_hash",
        "person_role_hash",
        "name AS full_name",
        "role",
        "role_category",
        "organization",
        "status",
        "effective_from",
        "effective_to",
        "confidence",
    )
    return (
        f"CREATE OR REPLACE VIEW {SE_COMPANY_PERSON_ESEF_VIEW} AS\n"
        f"SELECT\n    {_projection(columns)}\n"
        f"FROM {ESEF_DOCUMENT_PEOPLE_TABLE} FINAL\n"
        "WHERE country_code = 'SE'"
    )


def build_se_company_person_wikidata_view_sql() -> str:
    """The Wikidata company-person read, bridged to a validated SE company_id.

    See the module docstring for the bridge shape (copied from draft.py's wikidata read) and
    why this view validates the RESULT rather than restricting the join to a known-company
    scope. Both ``wikidata_company_people`` and ``wikidata_persons`` are
    ``ReplacingMergeTree(resolved_at)`` -- both sides read FINAL, matching draft.py.
    """
    bridge = f"""company_leis AS (
    SELECT
        company_id,
        upperUTF8(issuer_id) AS lei
    FROM {COMPANY_IDENTIFIER_TABLE}
    WHERE country_code = 'SE'
      AND issuer_scheme = 'lei'
      AND is_current = 1
    GROUP BY company_id, lei
),
company_wikidata_bridge AS (
    SELECT company_id, wikidata_id
    FROM (
        SELECT
            replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '') AS company_id,
            identifiers.wikidata_id AS wikidata_id
        FROM {WIKIDATA_COMPANY_IDENTIFIERS_TABLE} AS identifiers FINAL
        WHERE identifiers.identifier_type = 'se_orgnr'

        UNION ALL

        SELECT
            leis.company_id AS company_id,
            identifiers.wikidata_id AS wikidata_id
        FROM {WIKIDATA_COMPANY_IDENTIFIERS_TABLE} AS identifiers FINAL
        INNER JOIN company_leis AS leis
            ON leis.lei = upperUTF8(identifiers.identifier_value)
        WHERE identifiers.identifier_type = 'lei'
    )
    WHERE match(company_id, '{SE_COMPANY_ID_PATTERN}')
    GROUP BY company_id, wikidata_id
)"""
    columns = (
        "bridge.company_id AS company_id",
        "persons.source_record_uid AS source_record_uid",
        "persons.person_profile_hash AS person_profile_hash",
        "links.person_role_hash AS person_role_hash",
        "persons.name AS full_name",
        "links.person_wikidata_id AS person_wikidata_id",
        "links.role_property AS role_property",
        "links.start_date AS start_date",
        "links.end_date AS end_date",
        "persons.birth_year AS birth_year",
        "persons.description AS description",
        "persons.image_url AS image_url",
        "persons.wikidata_url AS external_url",
    )
    return (
        f"CREATE OR REPLACE VIEW {SE_COMPANY_PERSON_WIKIDATA_VIEW} AS\n"
        f"WITH {bridge}\n"
        f"SELECT\n    {_projection(columns)}\n"
        "FROM company_wikidata_bridge AS bridge\n"
        f"INNER JOIN {WIKIDATA_COMPANY_PEOPLE_TABLE} AS links FINAL\n"
        "    ON links.company_wikidata_id = bridge.wikidata_id\n"
        f"INNER JOIN {WIKIDATA_PERSONS_TABLE} AS persons FINAL\n"
        "    ON persons.person_wikidata_id = links.person_wikidata_id"
    )
