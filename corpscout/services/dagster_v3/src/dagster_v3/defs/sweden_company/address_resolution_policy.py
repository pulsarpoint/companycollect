from dagster_v3.defs.address_resolution.model import AddressResolutionPolicy


SWEDEN_STREET_VARIANT_LANGUAGES = {"SE": ("sv",)}


# Swedish registers truncate a street suffix and GLUE what is left to the stem:
# `STAVSTENSV 3` is Stavstensvägen 3 and `SANDGR 1` is Sandgränd 1. libpostal expands
# a punctuated `v.` as a token of its own but never splits a glued abbreviation off a
# stem, and where it does expand it produces the indefinite `gata`/`väg` while Swedish
# street names -- and therefore OSM -- carry the definite `gatan`/`vägen` (8,970 vs 821
# and 15,349 vs 3,019 distinct OSM street names). So the resolver expands these itself,
# to the definite form.
#
# Only classes measured against the unmatched population are here. Of the identities
# `se_address_geocodes` holds as unmatched with a parseable street: `g` covers 34,446,
# `v` 14,262 and `gr` 384, and their expansions exist in the OSM snapshot for 16,302,
# 10,759 and 219 of them. `gat` (90), `stg` (156), `st` (102), `vg` (43) and `pl` (29)
# were measured and left out -- too rare to matter, and `st` is genuinely ambiguous
# between stigen and stråket.
#
# 2026-08-25 -- v7 promotion. `SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS` (the punctuated
# form of the same three glued abbreviations, e.g. `v.` -> `vägen`) and
# `SWEDEN_SEPARATE_DEFINITE_EXPANSIONS` (indefinite-as-its-own-word -> definite, e.g.
# `Väg` -> `Vägen`) were measured exact-only (variant_kind='suffix_exact', excluded from
# fuzzy postings) against the g8_v7_recommended candidate set on the 49,461-identity
# control pool and the 20,513-identity yield pool: +1,909 newly matched, 0 lost, 0
# regressions across both pools. Two classes of extension were measured and rejected:
# (1) making the punctuated forms fuzzy-eligible instead of exact-only -- this
# reintroduces the wrong-match risk exact-only exists to prevent; the control pool
# alone produced one flip (`strandbergsg.`, `matched_corrected` -> `ambiguous`) from a
# fuzzy-eligible punctuated variant landing a false 1-edit neighbor. (2) widening
# either map with more abbreviations (`gg`, `all`, `stg`, `tg`, `ba`, `li`, `str`,
# `vg`, `gt`, `pl`) -- each was measured and re-confirmed harmful (net-negative or
# too ambiguous), same as the v6 measurement above; none are included here either.
SWEDEN_STREET_SUFFIX_EXPANSIONS: dict[str, dict[str, str]] = {
    "SE": {
        "gr": "gränd",
        "v": "vägen",
        "g": "gatan",
    }
}


# Derived from `SWEDEN_STREET_SUFFIX_EXPANSIONS` so the punctuated (exact-only) form can
# never drift from the glued (v6, fuzzy-eligible) form: `gr` -> `gr.`, `v` -> `v.`, etc.
SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS: dict[str, dict[str, str]] = {
    country: {
        f"{abbreviation}.": expansion for abbreviation, expansion in glued.items()
    }
    for country, glued in SWEDEN_STREET_SUFFIX_EXPANSIONS.items()
}


# A Swedish street name sometimes spells its suffix as its own indefinite word instead
# of gluing or punctuating an abbreviation -- `Norra Villa Väg` for `Norra Villa Vägen`.
# Exact-only (variant_kind='suffix_exact'): same 2026-08-25 measurement as above.
SWEDEN_SEPARATE_DEFINITE_EXPANSIONS: dict[str, dict[str, str]] = {
    "SE": {
        "väg": "vägen",
        "gata": "gatan",
        "torg": "torget",
        "allé": "allén",
        "backe": "backen",
        "gränd": "gränden",
        "plan": "planen",
        "stig": "stigen",
        "led": "leden",
        "gång": "gången",
        "park": "parken",
    }
}


SWEDEN_ADDRESS_RESOLUTION_POLICY = AddressResolutionPolicy(
    version="se-address-resolution-policy-v7",
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
    street_without_house_locality_score=0.59,
    street_without_house_postcode_conflict_score=0.55,
    street_missing_requested_house_score=0.45,
    street_missing_requested_house_locality_score=0.39,
    street_missing_requested_house_postcode_conflict_score=0.35,
)
