"""External assets for Finland PRH YTJ."""

import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj import spec

source_system = dg.AssetSpec(
    key=dg.AssetKey([*spec.ASSET_KEY_PREFIX, "source_system"]),
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "external"},
    description="External PRH YTJ Open Data API used to download Finland company snapshots.",
    metadata={
        "base_url": spec.BASE_URL,
        "description_path": spec.DESCRIPTION_PATH,
        "country": spec.COUNTRY,
        "source": spec.DISPLAY_NAME,
    },
)
