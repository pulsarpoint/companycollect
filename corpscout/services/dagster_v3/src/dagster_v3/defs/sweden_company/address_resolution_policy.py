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
# 2026-08-25 -- v7 promotion. The exact-only tier below (variant_kind='suffix_exact',
# excluded from fuzzy postings) was measured against the g8 strictly-additive candidate
# set on the 49,461-identity control pool and the 20,513-identity yield pool. It is safe
# by CONSTRUCTION: a suffix_exact variant can only ever ADD an exact reference match,
# never remove or fuzzy-mismatch a v6 one, so v7 is a strict superset of v6.
#   - `SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS`: the punctuated twins of the v6 glued
#     abbreviations (`v.` -> `vägen`) PLUS the extra abbreviations in
#     `SWEDEN_STREET_EXTRA_ABBREVIATIONS`, both glued and punctuated.
#   - `SWEDEN_SEPARATE_DEFINITE_EXPANSIONS`: indefinite-as-its-own-word -> definite
#     (`Väg` -> `Vägen`).
# Combined yield on the yield pool: +1,919 newly matched, 0 lost, 0 regressions across
# both pools (the punctuated + separate-definite core carries +1,909; the extra
# abbreviations are a provably-safe +10 marginal add, per the g8_v7_plus_extra candidate).
#
# One extension was measured and REJECTED: making the punctuated forms fuzzy-eligible
# instead of exact-only reintroduces the wrong-match risk exact-only exists to prevent --
# the control pool alone produced one flip (`strandbergsg.`, `matched_corrected` ->
# `ambiguous`) from a fuzzy-eligible punctuated variant landing a false 1-edit neighbor
# (`Strindbergsgatan`). That is why every NEW variant is exact-only. `st` is excluded
# from the extra abbreviations (genuinely ambiguous: stigen vs Sankt vs storgatan).
SWEDEN_STREET_SUFFIX_EXPANSIONS: dict[str, dict[str, str]] = {
    "SE": {
        "gr": "gränd",
        "v": "vägen",
        "g": "gatan",
    }
}


# NEW v7 abbreviations beyond v6's glued set, added exact-only (see the note above). `st`
# is deliberately excluded as ambiguous (stigen vs Sankt vs storgatan).
SWEDEN_STREET_EXTRA_ABBREVIATIONS: dict[str, dict[str, str]] = {
    "SE": {
        "gg": "gången",
        "all": "allén",
        "stg": "stigen",
        "pl": "plan",
        "tg": "torget",
        "ba": "backen",
        "li": "liden",
        "str": "stråket",
        "vg": "vägen",
        "gt": "gatan",
    }
}


def _punctuated_twins(glued: dict[str, str]) -> dict[str, str]:
    """`{"v": "vägen"}` -> `{"v.": "vägen"}` -- the punctuated form of each abbreviation."""
    return {f"{abbreviation}.": expansion for abbreviation, expansion in glued.items()}


# Exact-only expansion map: the punctuated twins of the v6 glued abbreviations plus the
# extra abbreviations in BOTH glued and punctuated form. Derived from the two source maps
# so the exact-only form can never drift from them.
SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS: dict[str, dict[str, str]] = {
    country: {
        **_punctuated_twins(SWEDEN_STREET_SUFFIX_EXPANSIONS.get(country, {})),
        **SWEDEN_STREET_EXTRA_ABBREVIATIONS.get(country, {}),
        **_punctuated_twins(SWEDEN_STREET_EXTRA_ABBREVIATIONS.get(country, {})),
    }
    for country in (
        SWEDEN_STREET_SUFFIX_EXPANSIONS.keys() | SWEDEN_STREET_EXTRA_ABBREVIATIONS.keys()
    )
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
