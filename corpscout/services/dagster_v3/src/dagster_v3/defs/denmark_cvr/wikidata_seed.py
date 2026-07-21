from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

# Wikidata P1059 = Danish CVR number (DK). Aggregated by
# defs/wikidata/registry_seed.py; see WikidataRegistrySeedSpec for why this lives here
# instead of a central list in defs/wikidata/.
#
# Denmark CVR has no ClickHouse export yet (duckdb_asset.py stops at the normalized
# DuckDB companies table — there is no defs/denmark_cvr/clickhouse.py). The
# discoverability edge therefore points at the DuckDB companies asset instead of a
# `*_clickhouse` spine; update spine_asset_key once Denmark ships one.
WIKIDATA_REGISTRY_SEED_SPEC = WikidataRegistrySeedSpec(
    property_id="P1059",
    country_iso2="DK",
    spine_asset_key="denmark_cvr_companies_duckdb",
)
