"""The three SE person source VIEWS -- read contracts over the originals, no copies.

MIGRATION 000331 widened all three views by one column, ``source_observed_at`` (the
per-observation timestamp: ``resolved_at`` for bolagsverket, ``extracted_at`` for esef,
``greatest(links.resolved_at, persons.resolved_at)`` for wikidata), and the esef view by one
more, ``fiscal_year`` (``esef_document_people`` has always had the column; 000330's view
projection did not carry it forward). Task 3
(`docs/superpowers/sdd/2026-08-27-se-people-experiment/task-3-report.md`) needed both: a real
recency signal for LLM-batch ordering and the "newest observation wins" name tie-break, and a
real fiscal year for the esef branch of the shared source-observations read (previously
always populated from ``esef_document_people.fiscal_year`` by the retired
``se_company_person_draft`` collector) -- neither is derivable from the other four
uniform-prefix columns, and inventing either would have been a lossy hack. Per the
`se_address_geocodes_served` precedent (migration 000327 widening 000325's view in place),
000331 re-issues `CREATE OR REPLACE VIEW` for all three views rather than editing 000330's
already-committed rendering; 000330 keeps creating the views (and the collision-candidate
table), 000331 is the current definition, and the drift-pin test points at 000331.

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

import hashlib
import uuid

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
        "resolved_at AS source_observed_at",
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
        "fiscal_year",
        "extracted_at AS source_observed_at",
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
        "greatest(links.resolved_at, persons.resolved_at) AS source_observed_at",
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


# ---------------------------------------------------------------------------
# The shared observation read -- Task 3's replacement for se_company_person_draft.
#
# `company_people/normalization.py` and `company_people/roles.py` both need one flat stream
# of "every non-blank-name row across the three views, with a stable per-row id" -- the exact
# role draft.py's `se_company_person_draft` used to play. There is no table to hold that
# stream anymore (spec 3.1: the views ARE the read contract), so it is a CTE fragment both
# callers splice into their own WITH clause, computed fresh on every query.
#
# THE draft_id FORMULA. draft.py minted `draft_id` as
# SHA256('se-company-person-source-observation-v1\n{company_id}\n{source}\n{source_entity_id}
# \n{person_profile_hash}\n{person_role_hash}') truncated to a UUID. `source_entity_id` (e.g.
# bolagsverket's `signatory_uid`) is not one of the three views' projected columns -- Task 1
# deliberately kept the views to the uniform 5-column prefix plus typed extras (spec 3.1's
# table), so this experiment's `draft_id` (domain bumped to v2, since the input shape changed)
# drops it: `SHA256(v2-domain\ncompany_id\nsource\nsource_record_uid\nperson_profile_hash\n
# person_role_hash)`. That is exactly as granular as the views themselves: for bolagsverket,
# `person_profile_hash` (first_name+last_name only) and `person_role_hash`
# (role_original+role_kind+signatory_kind+fiscal_year) together cover every substantive field
# the bolagsverket view projects, so two signatories indistinguishable by this formula are
# already indistinguishable by a plain `SELECT *` over the view -- not a granularity loss this
# module introduces. `source_record_uid` is itself content-addressed per source (000244); for
# bolagsverket it is scoped to one XBRL statement (shared by every signatory in that filing),
# which is exactly why the two hash fields still have to carry the disambiguating weight.
#
# BLANK NAMES. The views are raw pass-throughs (module docstring) -- `WHERE trim(full_name)
# != ''` excludes them here per the controller's blank-name ruling;
# `build_se_company_person_blank_full_name_count_sql` counts what this filters out, for the
# normalization asset's metadata.
# ---------------------------------------------------------------------------

SOURCE_OBSERVATION_HASH_DOMAIN = "se-company-person-source-observation-v2"

_SOURCE_OBSERVATION_ID_SQL = f"""reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            '{SOURCE_OBSERVATION_HASH_DOMAIN}\\n',
            company_id, '\\n', {{source_literal}}, '\\n', toString(source_record_uid), '\\n',
            toString(person_profile_hash), '\\n', toString(person_role_hash)
        ))), 1, 32)))"""


def build_se_company_person_source_observations_sql(
    cte_name: str = "source_observations",
) -> str:
    """The shared ``{cte_name} AS (...)`` fragment: splice into a caller's own WITH clause.

    Columns: ``source, company_id, source_record_uid, person_profile_hash, person_role_hash,
    full_name, fiscal_year (Nullable(UInt16)), source_observed_at, draft_id,
    source_value_json``. ``source_value_json`` carries only the fields normalization.py's
    Python side and roles.py's SQL side actually read (see their respective ``.get(...)`` /
    ``JSONExtractString`` call sites) -- not an archival copy of the source row (that archive
    is the original table itself, per spec 3.1).
    """
    bolagsverket_id = _SOURCE_OBSERVATION_ID_SQL.format(source_literal="'bolagsverket'")
    esef_id = _SOURCE_OBSERVATION_ID_SQL.format(source_literal="'esef'")
    wikidata_id = _SOURCE_OBSERVATION_ID_SQL.format(source_literal="'wikidata'")
    return f"""{cte_name} AS (
    SELECT
        'bolagsverket' AS source,
        company_id,
        source_record_uid,
        person_profile_hash,
        person_role_hash,
        full_name,
        if(
            fiscal_year > 0,
            toNullable(toUInt16(fiscal_year)),
            CAST(NULL, 'Nullable(UInt16)')
        ) AS fiscal_year,
        source_observed_at,
        {bolagsverket_id} AS draft_id,
        toJSONString(CAST(tuple(
            first_name, last_name, role_original, role_kind
        ) AS Tuple(
            first_name String, last_name String, role_original String, role_kind String
        ))) AS source_value_json
    FROM {SE_COMPANY_PERSON_BOLAGSVERKET_VIEW}
    WHERE trim(full_name) != ''

    UNION ALL

    SELECT
        'esef' AS source,
        company_id,
        source_record_uid,
        person_profile_hash,
        person_role_hash,
        full_name,
        toNullable(fiscal_year) AS fiscal_year,
        source_observed_at,
        {esef_id} AS draft_id,
        toJSONString(CAST(tuple(
            full_name, role, role_category
        ) AS Tuple(
            name String, role String, role_category String
        ))) AS source_value_json
    FROM {SE_COMPANY_PERSON_ESEF_VIEW}
    WHERE trim(full_name) != ''

    UNION ALL

    SELECT
        'wikidata' AS source,
        company_id,
        source_record_uid,
        person_profile_hash,
        person_role_hash,
        full_name,
        CAST(NULL, 'Nullable(UInt16)') AS fiscal_year,
        source_observed_at,
        {wikidata_id} AS draft_id,
        toJSONString(CAST(tuple(
            full_name, role_property, ifNull(description, ''), person_wikidata_id
        ) AS Tuple(
            name String, role_property String, description String, person_wikidata_id String
        ))) AS source_value_json
    FROM {SE_COMPANY_PERSON_WIKIDATA_VIEW}
    WHERE trim(full_name) != ''
)"""


def build_se_company_person_blank_full_name_count_sql() -> str:
    """Count of raw view rows this experiment excludes for a blank/whitespace-only name.

    Runs over the UNFILTERED views (unlike ``build_se_company_person_source_observations_sql``,
    which already drops these rows) so the count and the exclusion share nothing but intent --
    a bug in one cannot silently zero out the other.
    """
    return f"""SELECT countIf(trim(full_name) = '') AS blank_full_name_count
FROM (
    SELECT full_name FROM {SE_COMPANY_PERSON_BOLAGSVERKET_VIEW}
    UNION ALL
    SELECT full_name FROM {SE_COMPANY_PERSON_ESEF_VIEW}
    UNION ALL
    SELECT full_name FROM {SE_COMPANY_PERSON_WIKIDATA_VIEW}
)"""


def source_observation_id(
    *,
    company_id: str,
    source: str,
    source_record_uid: str,
    person_profile_hash: str,
    person_role_hash: str,
) -> uuid.UUID:
    """Python mirror of the SQL ``draft_id`` expression above, for test parity ONLY.

    ClickHouse's ``reinterpretAsUUID(unhex(hex_string))`` does not read the 16 bytes as a
    plain big-endian UUID: it treats them as two little-endian ``UInt64`` halves, so each
    8-byte half of the raw hash is byte-reversed relative to ``uuid.UUID(bytes=raw)``.
    Empirically confirmed against a real engine: ``SELECT reinterpretAsUUID(unhex(substring(
    hex(SHA256('hello-world-test')), 1, 32)))`` does NOT equal
    ``uuid.UUID(bytes=hashlib.sha256(b'hello-world-test').digest()[:16])`` without this
    per-half reversal. Production code never recomputes ``draft_id`` in Python -- only the SQL
    formula is load-bearing for what gets written -- this helper exists purely so a test can
    assert the two independent implementations agree.
    """
    digest = hashlib.sha256(
        f"{SOURCE_OBSERVATION_HASH_DOMAIN}\n{company_id}\n{source}\n"
        f"{source_record_uid}\n{person_profile_hash}\n{person_role_hash}".encode()
    ).digest()
    raw = digest[:16]
    reordered = raw[7::-1] + raw[15:7:-1]
    return uuid.UUID(bytes=reordered)
