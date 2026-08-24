"""The Sweden shared-address serving table's names and column contract.

This module used to hold a second OSM matcher whose output was overwritten by the
resolver's promotion before anything read it. The matcher is gone (spec section 4.3); what
survives is the naming of se_address_geocodes_current, which is now DERIVED from
corpscout.se_address_geocodes by geocode_store's versioned read.
"""

from dagster_v3.defs.sweden_company import address_canonicalization

ADDRESS_GEOCODES_TABLE = "se_address_geocodes_current"
QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{ADDRESS_GEOCODES_TABLE}"
)
QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE = (
    f"{address_canonicalization.CLICKHOUSE_DATABASE}.{ADDRESS_GEOCODES_TABLE}"
)

ADDRESS_GEOCODE_COLUMNS = (
    "address_id",
    "address_identity_run_id",
    "normalized_match_key",
    "match_status",
    "candidate_count",
    "candidate_record_ids",
    "candidate_record_urls",
    "match_method",
    "match_confidence",
    "latitude",
    "longitude",
    "geocode_provider",
    "geocode_precision",
    "coordinate_method",
    "coordinate_locality",
    "coordinate_supporting_point_count",
    "coordinate_spread_meters",
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "geocode_run_id",
    "matched_at",
)

