from collections.abc import Callable
from typing import Any

from dagster_v3.defs.address_resolution.resolution import (
    _replace_fuzzy_street_postings,
)
from dagster_v3.defs.address_resolution.search_documents import (
    replace_address_search_documents,
)
from dagster_v3.defs.sweden_address_osm import address_matching
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import geocode_store
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)

SHADOW_REFERENCE_DOCUMENTS_TABLE = "se_address_resolution_reference_index_shadow"
REFERENCE_MANIFEST_TABLE = "se_address_resolution_reference_manifest"
REFERENCE_POSTINGS_TABLE = "se_address_resolution_reference_street_postings"
REFERENCE_POSTINGS_MANIFEST_TABLE = (
    "se_address_resolution_reference_postings_manifest"
)

QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE = (
    f"{geocode_store.ENRICHMENT_SCHEMA}.{SHADOW_REFERENCE_DOCUMENTS_TABLE}"
)
QUALIFIED_REFERENCE_MANIFEST_TABLE = (
    f"{geocode_store.ENRICHMENT_SCHEMA}.{REFERENCE_MANIFEST_TABLE}"
)
QUALIFIED_REFERENCE_POSTINGS_TABLE = (
    f"{geocode_store.ENRICHMENT_SCHEMA}.{REFERENCE_POSTINGS_TABLE}"
)
QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE = (
    f"{geocode_store.ENRICHMENT_SCHEMA}"
    f".{REFERENCE_POSTINGS_MANIFEST_TABLE}"
)

INDEX_SCOPE = "SE-address-resolution-shadow-v2"


def fresh_reference_md5(connection: Any) -> str:
    """The OSM snapshot identity this run holds, read exactly as the promotion stamps it.

    Moved here from geocode_demand.py in slice 4c: the demand scan retired with the old
    chain, and the reference-document builders below are the only remaining callers.
    """
    [(reference_md5,)] = connection.execute(
        f"""
        select coalesce(first(source_md5 order by source_record_id), '')
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    ).fetchall()
    if not str(reference_md5):
        raise ValueError(
            "The Sweden OSM reference table carries no snapshot MD5 -- refusing to "
            "build address-resolution reference documents against an unidentifiable reference"
        )
    return str(reference_md5)


def replace_reference_documents(
    connection: Any, *, log: Callable[..., object] | None = None
) -> str:
    """Build the building and street reference documents from the current OSM workbench
    tables and record the extract they came from. The address entity's geocode function
    reads the result.

    An OSM workbench with no identifiable snapshot (no row's ``source_md5``) still gets its
    documents built -- that mirrors the retired shadow evaluation's behaviour, which never
    depended on ``source_md5`` to run matching. The manifest then honestly records ``''``
    rather than raising, which is this codebase's convention for "no identifiable value"; a
    missing reference identity is instead where the promotion step already refused to
    publish, same as it did before this manifest existed.
    """
    connection.execute(
        f"create schema if not exists {geocode_store.ENRICHMENT_SCHEMA}"
    )
    try:
        reference_md5 = fresh_reference_md5(connection)
    except ValueError:
        reference_md5 = ""
    _replace_building_reference_documents(connection)
    _replace_street_reference_documents(connection)
    connection.execute(
        f"""
        create or replace table {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE} as
        select * from _sweden_shadow_building_reference_documents
        union all
        select * from _sweden_shadow_street_reference_documents
        """
    )
    connection.execute(
        f"""
        create or replace table {QUALIFIED_REFERENCE_MANIFEST_TABLE} as
        select
            ?::varchar as reference_md5,
            ?::varchar as policy_version,
            now()::timestamp as built_at
        """,
        [reference_md5, SWEDEN_ADDRESS_RESOLUTION_POLICY.version],
    )
    if log is not None:
        log("reference documents rebuilt for extract %s", reference_md5)
    return reference_md5


def reference_documents_md5(connection: Any) -> str:
    """The manifest's recorded reference md5, or ``''`` when no manifest exists yet."""
    [(exists,)] = connection.execute(
        "select count(*) from information_schema.tables"
        " where table_schema = ? and table_name = ?",
        [geocode_store.ENRICHMENT_SCHEMA, REFERENCE_MANIFEST_TABLE],
    ).fetchall()
    if not exists:
        return ""
    row = connection.execute(
        f"select reference_md5 from {QUALIFIED_REFERENCE_MANIFEST_TABLE}"
    ).fetchone()
    return row[0] if row else ""


def reference_documents_built_at(connection: Any) -> str:
    """The documents manifest's ``built_at`` as text, ``''`` when there is none to read.

    The md5 alone does not say the documents are unchanged. `replace_reference_documents`
    rebuilds them UNCONDITIONALLY whenever it is called, under the same extract md5, so a
    change to the document builders or to `INDEX_SCOPE` produces different documents on the
    same md5. Anything derived from the documents (the fuzzy postings) follows this stamp too.
    A manifest that predates the column -- or no manifest at all -- reads as ``''``, which
    matches nothing and so forces one rebuild rather than raising.
    """
    [(exists,)] = connection.execute(
        "select count(*) from information_schema.columns"
        " where table_schema = ? and table_name = ? and column_name = 'built_at'",
        [geocode_store.ENRICHMENT_SCHEMA, REFERENCE_MANIFEST_TABLE],
    ).fetchall()
    if not exists:
        return ""
    row = connection.execute(
        f"select built_at::varchar from {QUALIFIED_REFERENCE_MANIFEST_TABLE}"
    ).fetchone()
    if not row or row[0] is None:
        return ""
    return str(row[0])


def ensure_reference_documents(
    connection: Any, *, log: Callable[..., object] | None = None
) -> str:
    """Rebuild the reference documents when the OSM extract moved, else no-op.

    Returns the current reference md5 either way.
    """
    current = fresh_reference_md5(connection)
    if reference_documents_md5(connection) == current:
        return current
    return replace_reference_documents(connection, log=log)


def replace_reference_postings(
    connection: Any, *, log: Callable[..., object] | None = None
) -> None:
    """Build the fuzzy reference street postings from the current reference documents.

    The postings are the reference side of the matcher's fuzzy retrieval: every reference
    street crossed with its deletion signatures, de-duplicated. `replace_address_resolution_candidates`
    used to rebuild them on every call, which a one-shot rematch over the whole query set pays
    once and a caller that pages (the address entity's fold, 20,000 companies at a time) pays
    per page over the WHOLE reference table. They depend only on the reference documents and
    the policy, so they cache exactly like the documents do -- one manifest row recording the
    documents build and the policy version they were made from.

    The table is persistent, not temporary: a DuckDB temporary table dies with the connection
    and cannot be schema-qualified, and this one lives beside the reference documents in the
    enrichment schema so a later run on the same extract inherits it.
    """
    connection.execute(
        f"create schema if not exists {geocode_store.ENRICHMENT_SCHEMA}"
    )
    _replace_fuzzy_street_postings(
        connection,
        source_table=QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
        postings_table=QUALIFIED_REFERENCE_POSTINGS_TABLE,
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        reference_documents=True,
        temporary=False,
    )
    reference_md5 = reference_documents_md5(connection)
    documents_built_at = reference_documents_built_at(connection)
    connection.execute(
        f"""
        create or replace table {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE} as
        select
            ?::varchar as reference_md5,
            ?::varchar as policy_version,
            ?::varchar as documents_built_at,
            now()::timestamp as built_at
        """,
        [
            reference_md5,
            SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            documents_built_at,
        ],
    )
    if log is not None:
        [(postings,)] = connection.execute(
            f"select count(*) from {QUALIFIED_REFERENCE_POSTINGS_TABLE}"
        ).fetchall()
        log(
            "reference postings rebuilt for extract %s policy %s: %d postings",
            reference_md5,
            SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            postings,
        )


def reference_postings_key(connection: Any) -> tuple[str, str, str]:
    """The cached postings' ``(reference_md5, policy_version, documents_built_at)``.

    ``('', '', '')`` -- a key nothing matches, so the caller rebuilds -- whenever there is no
    usable cache to describe: no manifest, a manifest predating one of the three columns, or
    a manifest whose POSTINGS TABLE is gone. The last case is the one a manifest cannot see:
    the table is dropped by hand, or lost with the workbench file, while the manifest that
    described it survives; reading the manifest alone would then report a cache that is not
    there and the engine would fail on a missing table.
    """
    [(columns,)] = connection.execute(
        "select count(*) from information_schema.columns"
        " where table_schema = ? and table_name = ?"
        " and column_name in ('reference_md5', 'policy_version', 'documents_built_at')",
        [
            geocode_store.ENRICHMENT_SCHEMA,
            REFERENCE_POSTINGS_MANIFEST_TABLE,
        ],
    ).fetchall()
    if columns != 3:
        return "", "", ""
    [(postings,)] = connection.execute(
        "select count(*) from information_schema.tables"
        " where table_schema = ? and table_name = ?",
        [geocode_store.ENRICHMENT_SCHEMA, REFERENCE_POSTINGS_TABLE],
    ).fetchall()
    if not postings:
        return "", "", ""
    row = connection.execute(
        "select reference_md5, policy_version, documents_built_at"
        f" from {QUALIFIED_REFERENCE_POSTINGS_MANIFEST_TABLE}"
    ).fetchone()
    if not row:
        return "", "", ""
    return str(row[0]), str(row[1]), str(row[2])


def ensure_reference_postings(
    connection: Any, *, log: Callable[..., object] | None = None
) -> str:
    """Rebuild the reference documents AND their fuzzy postings when either key moved.

    Returns the current reference md5, the same value `ensure_reference_documents` returns
    (and through the same call) -- a caller wanting the postings wants the documents too, and
    both are keyed on the extract. The postings key has two more parts. The POLICY VERSION,
    because the fuzzy posting rule is the policy's (`minimum_fuzzy_street_length`, the
    suffix_exact exclusion), so a policy bump on an unmoved extract must rebuild them. And the
    documents' own `built_at`, because the md5 identifies the OSM EXTRACT, not the documents:
    `replace_reference_documents` rebuilds them unconditionally under the same md5, and
    postings left over from the previous build would describe documents that no longer exist.
    """
    reference_md5 = ensure_reference_documents(connection, log=log)
    if reference_postings_key(connection) == (
        reference_md5,
        SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        reference_documents_built_at(connection),
    ):
        return reference_md5
    replace_reference_postings(connection, log=log)
    return reference_md5


def _replace_building_reference_documents(connection: Any) -> None:
    replace_address_search_documents(
        connection,
        table_name="_sweden_shadow_building_reference_documents",
        source_sql=f"""
            with expanded as (
                select
                    address.*,
                    trim(component.value) as house_number_component
                from {osm_tables.QUALIFIED_ADDRESS_TABLE} address
                cross join unnest(regexp_split_to_array(
                    coalesce(address.house_number, ''),
                    '[,;]'
                )) component(value)
            ), deduplicated as (
                select *
                from expanded
                where house_number_component != ''
                qualify row_number() over (
                    partition by
                        source_record_id,
                        regexp_replace(
                            lower(house_number_component),
                            '[^[:alnum:]]+',
                            '',
                            'g'
                        )
                    order by house_number_component
                ) = 1
            )
            select
                '{INDEX_SCOPE}'::varchar as index_scope,
                concat(
                    source_record_id,
                    '/house/',
                    md5(house_number_component)
                ) as document_id,
                country_code,
                coalesce(full_address, '') as raw_address,
                concat_ws(
                    ', ',
                    concat_ws(
                        ' ',
                        coalesce(nullif(street, ''), place, ''),
                        house_number_component
                    ),
                    concat_ws(
                        ' ',
                        coalesce(postcode, ''),
                        coalesce(city, '')
                    )
                ) as search_text,
                coalesce(nullif(street, ''), place, '') as street_name,
                house_number_component as house_number,
                coalesce(unit, '') as unit,
                coalesce(postcode, '') as postal_code,
                coalesce(city, '') as locality,
                'physical'::varchar as address_kind,
                'building'::varchar as reference_precision,
                latitude,
                longitude,
                0::double as coordinate_spread_meters,
                1::uinteger as supporting_record_count,
                source_record_id,
                source_record_url
            from deduplicated
            where coalesce(nullif(street, ''), place, '') != ''
        """,
    )


def _replace_street_reference_documents(connection: Any) -> None:
    connection.execute(
        """
        create or replace temporary table _sweden_shadow_postcode_centroids as
        select
            country_code,
            normalized_postal_code,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude
        from _sweden_shadow_building_reference_documents
        where normalized_postal_code != ''
        group by country_code, normalized_postal_code
        """
    )
    _replace_address_point_street_inputs(connection)
    _replace_road_search_documents(connection)
    _replace_road_street_inputs(connection)
    connection.execute(
        f"""
        create or replace temporary table _sweden_shadow_street_reference_input as
        with candidates as (
            select * from _sweden_shadow_address_point_street_input
            union all
            select * from _sweden_shadow_road_street_input
        )
        select * exclude (normalized_street, source_priority)
        from candidates
        qualify row_number() over (
            partition by
                country_code,
                normalized_street,
                regexp_replace(
                    strip_accents(lower(nfc_normalize(postal_code))),
                    '[^[:alnum:]]+',
                    '',
                    'g'
                )
            order by
                case
                    when coordinate_spread_meters
                        <= {SWEDEN_ADDRESS_RESOLUTION_POLICY.area_maximum_spread_meters}
                     and source_priority = 0 then 0
                    when coordinate_spread_meters
                        <= {SWEDEN_ADDRESS_RESOLUTION_POLICY.area_maximum_spread_meters}
                        then 1
                    when source_priority = 0 then 2
                    else 3
                end,
                supporting_record_count desc,
                document_id
        ) = 1
        """
    )
    replace_address_search_documents(
        connection,
        source_sql="select * from _sweden_shadow_street_reference_input",
        table_name="_sweden_shadow_street_reference_documents",
    )


def _replace_address_point_street_inputs(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace temporary table
            _sweden_shadow_address_point_street_input as
        select
            '{INDEX_SCOPE}'::varchar as index_scope,
            concat(
                'address-point-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code,
                    normalized_locality
                ))
            ) as document_id,
            country_code,
            ''::varchar as raw_address,
            concat_ws(
                ', ',
                first(street_name order by document_id),
                concat_ws(
                    ' ',
                    first(postal_code order by document_id),
                    first(locality order by document_id)
                )
            ) as search_text,
            first(street_name order by document_id) as street_name,
            ''::varchar as house_number,
            ''::varchar as unit,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality,
            'physical'::varchar as address_kind,
            'street'::varchar as reference_precision,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude,
            {_spread_sql()} as coordinate_spread_meters,
            count(*)::uinteger as supporting_record_count,
            concat(
                'osm-address-point-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code,
                    normalized_locality
                ))
            ) as source_record_id,
            ''::varchar as source_record_url,
            normalized_street,
            0::utinyint as source_priority
        from _sweden_shadow_building_reference_documents
        where normalized_street != ''
          and (normalized_postal_code != '' or normalized_locality != '')
        group by
            country_code,
            normalized_street,
            normalized_postal_code,
            normalized_locality
        """
    )


def _replace_road_search_documents(connection: Any) -> None:
    replace_address_search_documents(
        connection,
        source_sql=f"""
            select
                '{INDEX_SCOPE}'::varchar as index_scope,
                concat('road/', source_record_id) as document_id,
                'SE'::varchar as country_code,
                street as raw_address,
                street as search_text,
                street as street_name,
                ''::varchar as house_number,
                ''::varchar as unit,
                ''::varchar as postal_code,
                ''::varchar as locality,
                'physical'::varchar as address_kind,
                'street'::varchar as reference_precision,
                latitude,
                longitude,
                0::double as coordinate_spread_meters,
                1::uinteger as supporting_record_count,
                source_record_id,
                source_record_url
            from {osm_tables.QUALIFIED_STREET_SEGMENT_TABLE}
            where coalesce(street, '') != ''
        """,
        table_name="_sweden_shadow_road_documents",
    )


def _replace_road_street_inputs(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace temporary table _sweden_shadow_road_street_input as
        with postcode_cells as (
            select
                postcode.*,
                floor(
                    latitude / {address_matching.ROAD_LATITUDE_GRID_DEGREES}
                )::integer + latitude_offset.value as latitude_cell,
                floor(
                    longitude / {address_matching.ROAD_LONGITUDE_GRID_DEGREES}
                )::integer + longitude_offset.value as longitude_cell
            from _sweden_shadow_postcode_centroids postcode
            cross join range(
                -{address_matching.ROAD_LATITUDE_NEIGHBOR_CELLS},
                {address_matching.ROAD_LATITUDE_NEIGHBOR_CELLS + 1}
            ) latitude_offset(value)
            cross join range(
                -{address_matching.ROAD_LONGITUDE_NEIGHBOR_CELLS},
                {address_matching.ROAD_LONGITUDE_NEIGHBOR_CELLS + 1}
            ) longitude_offset(value)
        ), road_cells as (
            select
                *,
                floor(
                    latitude / {address_matching.ROAD_LATITUDE_GRID_DEGREES}
                )::integer as latitude_cell,
                floor(
                    longitude / {address_matching.ROAD_LONGITUDE_GRID_DEGREES}
                )::integer as longitude_cell
            from _sweden_shadow_road_documents
            where normalized_street != ''
        ), nearby as (
            select
                road.*,
                postcode.postal_code as context_postal_code,
                postcode.locality as context_locality,
                postcode.normalized_postal_code
                    as context_normalized_postal_code,
                2 * 6371000 * asin(least(1.0, sqrt(
                    pow(
                        sin(radians(road.latitude - postcode.latitude) / 2),
                        2
                    )
                    + cos(radians(postcode.latitude))
                      * cos(radians(road.latitude))
                      * pow(
                          sin(
                              radians(
                                  road.longitude - postcode.longitude
                              ) / 2
                          ),
                          2
                      )
                )))::double as postcode_distance_meters
            from road_cells road
            inner join postcode_cells postcode
                using (latitude_cell, longitude_cell)
        )
        select
            '{INDEX_SCOPE}'::varchar as index_scope,
            concat(
                'road-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    context_normalized_postal_code
                ))
            ) as document_id,
            country_code,
            ''::varchar as raw_address,
            concat_ws(
                ', ',
                first(street_name order by document_id),
                concat_ws(
                    ' ',
                    first(context_postal_code order by document_id),
                    first(context_locality order by document_id)
                )
            ) as search_text,
            first(street_name order by document_id) as street_name,
            ''::varchar as house_number,
            ''::varchar as unit,
            first(context_postal_code order by document_id) as postal_code,
            first(context_locality order by document_id) as locality,
            'physical'::varchar as address_kind,
            'street'::varchar as reference_precision,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude,
            {_spread_sql()} as coordinate_spread_meters,
            count(distinct source_record_id)::uinteger
                as supporting_record_count,
            concat(
                'osm-road-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    context_normalized_postal_code
                ))
            ) as source_record_id,
            ''::varchar as source_record_url,
            normalized_street,
            1::utinyint as source_priority
        from nearby
        where postcode_distance_meters
            <= {address_matching.ROAD_POSTCODE_CONTEXT_MAX_DISTANCE_METERS}
        group by
            country_code,
            normalized_street,
            context_normalized_postal_code
        """
    )


def _spread_sql() -> str:
    return """
2 * 6371000 * asin(least(1.0, sqrt(
    pow(sin(radians(max(latitude) - min(latitude)) / 2), 2)
    + cos(radians(min(latitude))) * cos(radians(max(latitude)))
      * pow(sin(radians(max(longitude) - min(longitude)) / 2), 2)
)))::double
""".strip()

