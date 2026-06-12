import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec

source_system = dg.AssetSpec(
    key=dg.AssetKey([*spec.ASSET_KEY_PREFIX, "source_system"]),
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "external"},
    description=f"External source system for {spec.DISPLAY_NAME}.",
    metadata={"country": spec.COUNTRY, "source": spec.DISPLAY_NAME},
)
