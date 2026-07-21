"""Aggregates each country module's ``WikidataRegistrySeedSpec`` (see
``dagster_v3.defs.common.wikidata_registry_seed``) so the wikidata registry-number seed
(``defs/wikidata/assets.py``/``source.py``) can discover unlisted companies for every
country that has a national registry-number Wikidata property — not just the ones
listed on a stock exchange.

Specs are declared **in each country module** (one constant each, next to that module's
own tables — see e.g. ``defs/sweden_company/tables.py:WIKIDATA_REGISTRY_SEED_SPEC``) and
imported here explicitly. Adding a country's spec therefore happens naturally alongside
that country's own module; a central hardcoded list living in ``defs/wikidata/`` would be
easy to forget when a new country ships. ``tests/test_wikidata_assets.py`` enforces the
wiring: every spec's ``spine_asset_key`` must be a real, registered asset that appears in
``wikidata_company_seed_raw_objects``'s parent keys.
"""

from dagster_v3.defs.brazil_companies.rfb.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as BRAZIL_RFB_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec
from dagster_v3.defs.czech_ares.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as CZECH_ARES_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.denmark_cvr.wikidata_seed import (
    WIKIDATA_REGISTRY_SEED_SPEC as DENMARK_CVR_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.finland_ytj.resolved_tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as FINLAND_YTJ_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.france_sirene.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as FRANCE_SIRENE_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.latvia_ur.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as LATVIA_UR_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.norway_brreg.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as NORWAY_BRREG_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.sweden_company.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as SWEDEN_COMPANY_WIKIDATA_REGISTRY_SEED_SPEC,
)
from dagster_v3.defs.uk_companies_house.tables import (
    WIKIDATA_REGISTRY_SEED_SPEC as UK_COMPANIES_HOUSE_WIKIDATA_REGISTRY_SEED_SPEC,
)

WIKIDATA_REGISTRY_SEED_SPECS: tuple[WikidataRegistrySeedSpec, ...] = (
    SWEDEN_COMPANY_WIKIDATA_REGISTRY_SEED_SPEC,
    NORWAY_BRREG_WIKIDATA_REGISTRY_SEED_SPEC,
    DENMARK_CVR_WIKIDATA_REGISTRY_SEED_SPEC,
    FINLAND_YTJ_WIKIDATA_REGISTRY_SEED_SPEC,
    UK_COMPANIES_HOUSE_WIKIDATA_REGISTRY_SEED_SPEC,
    FRANCE_SIRENE_WIKIDATA_REGISTRY_SEED_SPEC,
    CZECH_ARES_WIKIDATA_REGISTRY_SEED_SPEC,
    LATVIA_UR_WIKIDATA_REGISTRY_SEED_SPEC,
    BRAZIL_RFB_WIKIDATA_REGISTRY_SEED_SPEC,
)

# Derived, not hand-maintained: the default registry-number property set the pull loop
# iterates (WikidataRawPullConfig.configured_registry_property_ids()) and the spine asset
# keys the seed asset declares ordering-only `deps` on for UI discoverability.
WIKIDATA_REGISTRY_NUMBER_PROPERTY_IDS: tuple[str, ...] = tuple(
    spec.property_id for spec in WIKIDATA_REGISTRY_SEED_SPECS
)
WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS: tuple[str, ...] = tuple(
    spec.spine_asset_key for spec in WIKIDATA_REGISTRY_SEED_SPECS
)
