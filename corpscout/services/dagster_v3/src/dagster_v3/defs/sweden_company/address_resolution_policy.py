from dagster_v3.defs.address_resolution.model import AddressResolutionPolicy


SWEDEN_ADDRESS_RESOLUTION_POLICY = AddressResolutionPolicy(
    version="se-address-resolution-policy-v2",
    minimum_fuzzy_street_length=6,
    maximum_street_edit_distance=1,
    minimum_decisive_score_margin=0.05,
    site_maximum_spread_meters=100.0,
    area_maximum_spread_meters=1_000.0,
    exact_score=1.0,
    locality_fallback_score=0.97,
    postcode_mismatch_score=0.93,
    country_fallback_score=0.84,
    fuzzy_postcode_score=0.92,
    fuzzy_locality_score=0.88,
    street_without_house_score=0.65,
    street_missing_requested_house_score=0.45,
)
