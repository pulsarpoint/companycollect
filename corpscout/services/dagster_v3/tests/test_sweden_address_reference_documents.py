"""The reference documents (OSM buildings + streets) built once per OSM extract, and the
fuzzy reference street postings built from them.

The shadow evaluation used to rebuild these on every run. Task 2 turns that build into a
named, idempotent step keyed on the OSM snapshot's md5 (`address_resolution_shadow.fresh_reference_md5`):
the address entity's geocode function (a later task) and the shadow evaluation both read the
result, and neither should pay to rebuild it when the snapshot has not moved.

The 2026-09-07 fix extends the same idea one step further, to the derived index the matcher's
fuzzy retrieval joins against. Those postings were rebuilt inside
`replace_address_resolution_candidates` on every call -- which the fold, calling the engine
once per 20,000-company page, paid per page over the whole reference table. Their key has
three parts: the extract md5, the policy version (the posting rule --
`minimum_fuzzy_street_length`, the `suffix_exact` exclusion -- is the policy's), and the
DOCUMENTS' `built_at` (the shadow run rebuilds the documents unconditionally under the same
md5, so the md5 alone cannot say the documents are the ones the postings were made from).
A missing postings table beats all three: the manifest can outlive the table it describes.
"""

from collections.abc import Iterator

import duckdb
import pytest

from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_REFERENCE_MANIFEST_TABLE,
    QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE,
    QUALIFIED_REFERENCE_POSTINGS_TABLE,
    QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
    REFERENCE_POSTINGS_TABLE,
    ensure_reference_documents,
    ensure_reference_postings,
    reference_documents_built_at,
    reference_documents_md5,
    reference_postings_key,
    replace_reference_documents,
)

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version


@pytest.fixture()
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """The two OSM workbench tables the reference-document builders read.

    Mirrors the shapes `tests/test_address_resolution.py`'s `_create_sweden_shadow_fixture`
    uses for `sweden_address_osm.address_points`/`street_segments`, trimmed to three address
    points on one street in one postcode -- enough for `_replace_building_reference_documents`
    and `_replace_street_reference_documents` to produce rows, nothing more.
    """
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        """
        create table sweden_address_osm.address_points (
            source_record_id varchar,
            country_code varchar,
            full_address varchar,
            street varchar,
            place varchar,
            house_number varchar,
            unit varchar,
            postcode varchar,
            city varchar,
            latitude double,
            longitude double,
            source_record_url varchar,
            source_url varchar default
                'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            source_object_key varchar default 'raw/sweden-test.osm.pbf',
            source_md5 varchar,
            source_snapshot_at timestamptz default '2026-08-16 00:00:00+00',
            source_retrieved_at timestamptz default '2026-08-16 01:00:00+00'
        )
        """
    )
    connection.execute(
        """
        insert into sweden_address_osm.address_points (
            source_record_id, country_code, full_address, street, place,
            house_number, unit, postcode, city, latitude, longitude,
            source_record_url, source_md5
        ) values
            (
                'osm/1', 'SE', 'Storgatan 1, 11122 Stockholm', 'Storgatan', '',
                '1', '', '11122', 'Stockholm', 59.331, 18.061,
                'https://www.openstreetmap.org/node/1', 'md5-a'
            ),
            (
                'osm/2', 'SE', 'Storgatan 2, 11122 Stockholm', 'Storgatan', '',
                '2', '', '11122', 'Stockholm', 59.3311, 18.0611,
                'https://www.openstreetmap.org/node/2', 'md5-a'
            ),
            (
                'osm/3', 'SE', 'Storgatan 3, 11122 Stockholm', 'Storgatan', '',
                '3', '', '11122', 'Stockholm', 59.3312, 18.0612,
                'https://www.openstreetmap.org/node/3', 'md5-a'
            )
        """
    )
    connection.execute(
        """
        create table sweden_address_osm.street_segments (
            source_record_id varchar,
            street varchar,
            latitude double,
            longitude double,
            source_record_url varchar
        )
        """
    )
    yield connection
    connection.close()


def test_reference_documents_are_built_once_per_extract(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    assert reference_documents_md5(connection) == ""
    first = ensure_reference_documents(connection)
    assert first == "md5-a"
    rows = connection.execute(
        f"select count(*) from {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE}"
    ).fetchone()[0]
    assert rows > 0
    built_at = connection.execute(
        f"select built_at from {QUALIFIED_REFERENCE_MANIFEST_TABLE}"
    ).fetchone()[0]
    assert ensure_reference_documents(connection) == "md5-a"
    assert (
        connection.execute(
            f"select built_at from {QUALIFIED_REFERENCE_MANIFEST_TABLE}"
        ).fetchone()[0]
        == built_at
    )  # no rebuild
    connection.execute(
        "update sweden_address_osm.address_points set source_md5 = 'md5-b'"
    )
    assert ensure_reference_documents(connection) == "md5-b"
    assert (
        connection.execute(
            f"select count(*) from {QUALIFIED_REFERENCE_MANIFEST_TABLE}"
        ).fetchone()[0]
        == 1
    )


def test_reference_postings_are_built_once_per_extract_and_policy(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Built on the first call, reused while the key holds, rebuilt when either half moves."""
    assert reference_postings_key(connection) == ("", "", "")

    assert ensure_reference_postings(connection) == "md5-a"
    assert reference_postings_key(connection) == (
        "md5-a",
        POLICY,
        reference_documents_built_at(connection),
    )
    postings = connection.execute(
        f"select count(*) from {QUALIFIED_REFERENCE_POSTINGS_TABLE}"
    ).fetchone()[0]
    assert postings > 0
    # A temporary table would die with the connection, so it could never be a cache.
    assert connection.execute(
        "select temporary from duckdb_tables() where table_name = ?",
        [REFERENCE_POSTINGS_TABLE],
    ).fetchone() == (False,)

    built_at = connection.execute(
        f"select built_at from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
    ).fetchone()[0]
    assert ensure_reference_postings(connection) == "md5-a"
    assert (
        connection.execute(
            f"select built_at from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
        ).fetchone()[0]
        == built_at
    )  # no rebuild

    # A policy bump on an unmoved extract still rebuilds: the posting rule is the policy's.
    connection.execute(
        f"update {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
        " set policy_version = 'se-address-resolution-policy-v6'"
    )
    assert ensure_reference_postings(connection) == "md5-a"
    assert reference_postings_key(connection)[:2] == ("md5-a", POLICY)
    assert (
        connection.execute(
            f"select count(*) from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
        ).fetchone()[0]
        == 1
    )


def test_a_new_extract_rebuilds_the_documents_and_the_postings(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """`ensure_reference_postings` chains `ensure_reference_documents`: one call keeps both
    caches honest, and returns the md5 the geocode function stamps on every row it writes."""
    assert ensure_reference_postings(connection) == "md5-a"
    connection.execute(
        "update sweden_address_osm.address_points set source_md5 = 'md5-b'"
    )

    assert ensure_reference_postings(connection) == "md5-b"

    assert reference_documents_md5(connection) == "md5-b"
    assert reference_postings_key(connection) == (
        "md5-b",
        POLICY,
        reference_documents_built_at(connection),
    )
    assert (
        connection.execute(
            f"select count(*) from {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE}"
        ).fetchone()[0]
        > 0
    )


def test_rebuilt_documents_on_the_same_extract_rebuild_the_postings(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """The md5 names the OSM EXTRACT, not the documents built from it.

    `replace_reference_documents` rebuilds them unconditionally whenever it is called, so
    the documents can change under a fixed md5 -- a change to the document builders or to
    `INDEX_SCOPE` would do it. Postings left from
    the previous build would then describe documents that no longer exist, silently, since
    a stale posting produces a wrong candidate rather than an error. The key therefore
    carries the documents' `built_at`.
    """
    assert ensure_reference_postings(connection) == "md5-a"
    stale = reference_documents_built_at(connection)
    assert reference_postings_key(connection)[2] == stale

    replace_reference_documents(connection)

    rebuilt = reference_documents_built_at(connection)
    # Guards the test itself: with an unmoved stamp the assertions below prove nothing.
    assert rebuilt != stale
    assert reference_postings_key(connection)[2] == stale  # the cache is now stale

    assert ensure_reference_postings(connection) == "md5-a"

    assert reference_postings_key(connection) == ("md5-a", POLICY, rebuilt)
    # And a plain second call still no-ops -- the fix must not rebuild on every call.
    built_at = connection.execute(
        f"select built_at from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
    ).fetchone()[0]
    assert ensure_reference_postings(connection) == "md5-a"
    assert (
        connection.execute(
            f"select built_at from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
        ).fetchone()[0]
        == built_at
    )


def test_a_missing_postings_table_rebuilds_however_well_the_manifest_matches(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """The manifest can outlive the table it describes -- dropped by hand, or lost with the
    workbench file. Trusting the manifest alone would report a cache that is not there and
    the engine would fail on a missing table, so the probe is on the TABLE."""
    assert ensure_reference_postings(connection) == "md5-a"
    connection.execute(f"drop table {QUALIFIED_REFERENCE_POSTINGS_TABLE}")

    assert reference_postings_key(connection) == ("", "", "")

    assert ensure_reference_postings(connection) == "md5-a"

    assert (
        connection.execute(
            f"select count(*) from {QUALIFIED_REFERENCE_POSTINGS_TABLE}"
        ).fetchone()[0]
        > 0
    )
    assert reference_postings_key(connection) == (
        "md5-a",
        POLICY,
        reference_documents_built_at(connection),
    )


def test_the_reference_builders_stand_alone_after_the_old_chain_retired() -> None:
    """Slice 4c: the shadow DRIVER is gone (its two input tables were built by assets slice
    4b deleted), but the reference-document and posting builders the address entity's
    geocode function calls stay -- and they no longer reach into geocode_demand for the
    extract md5, because that module went with the demand scan."""
    from dagster_v3.defs.sweden_company import address_resolution_shadow as shadow

    assert callable(shadow.fresh_reference_md5)
    assert callable(shadow.ensure_reference_documents)
    assert callable(shadow.ensure_reference_postings)
    assert not hasattr(shadow, "replace_sweden_address_resolution_shadow")
    assert not hasattr(shadow, "geocode_demand")
    assert not hasattr(shadow, "shared_addresses")
    assert not hasattr(shadow, "address_canonicalization")
