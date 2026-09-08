"""The centroid-fallback policy the address entity's geocode function applies on the way out.

These values lived in `sweden_company`'s now-retired serving-overlay module until slice 4c
(2026-09-08): that module existed to build the `se_address_geocodes_served` VIEW (migrations
000325/000327), which is dropped with the rest of the old chain. The POLICY it also held is
not the view's -- `geocode.py` applies it per outcome, in Python, and stores nothing -- so it
moves here.

It is a leaf on purpose. `fold.py`, `warm.py`, `geocode.py` and `sweden_company/companies_current.py`
all need `GEOCODE_FALLBACK_PROVIDER`, and `companies_current.py` is a pure SQL builder the
serving migration renders: it must not have to import `geocode.py`, which pulls in DuckDB, the
matcher and the OSM workbench for one string.
"""

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.sweden_company.geocode_store import GEOCODED_STATUSES

# The centroid reference tables (migrations 000323 / 000324). Spelled here rather than
# imported from centroid_assets.py to keep this module free of Dagster asset definitions.
POSTCODE_CENTROIDS_TABLE = f"{tables.DATABASE}.se_postcode_centroids"
CITY_CENTROIDS_TABLE = f"{tables.DATABASE}.se_city_centroids"

# The precise outcomes the fallback is allowed to fill. `postal_box` joined 2026-08: a box
# postcode is a dedicated range tied to a postal town, so the coarse centroid is exactly as
# honest for a box as for an unmatched street. Everything else -- geocoded, invalid_address,
# foreign_address, property_identifier -- passes through untouched.
FALLBACK_ELIGIBLE_STATUSES = ("unmatched", "ambiguous", "postal_box")

# A postcode centroid looser than this is demoted to the city centroid: past a few km a
# "postcode" centroid no longer means the postcode.
POSTCODE_SPREAD_MAX_METERS = 3000.0

GEOCODE_FALLBACK_PROVIDER = "centroid_fallback"
GEOCODE_FALLBACK_COORDINATE_METHOD = "centroid_median"
POSTCODE_PRECISION = "postcode"
CITY_PRECISION = "city"

# The status a fallback row reports. `matched_area` is the store's coarsest GEOCODED status,
# so a consumer filtering on GEOCODED_STATUSES still counts a rescued identity;
# `geocode_precision` ('postcode'/'city') is what marks it coarse. Asserted a member of the
# store vocabulary here so a status rename cannot silently make the fallback serve a status
# no consumer recognizes.
GEOCODE_FALLBACK_STATUS = "matched_area"
assert GEOCODE_FALLBACK_STATUS in GEOCODED_STATUSES
