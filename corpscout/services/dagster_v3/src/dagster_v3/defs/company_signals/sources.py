"""The procurement sources a country can read.

A source is declared here and projected in SQL: each country's view in the
migration merges its sources into one shape. This module is what the Dagster
layer needs to know about them -- which upstream asset produces the data, which
ClickHouse tables the view will read, and the slug that labels a row's origin.

The projections themselves deliberately do not live here. They are view bodies,
and views are schema, which the migrations own.
"""

from __future__ import annotations

from dataclasses import dataclass

UHM_SOURCE_SLUG = "sweden_uhm_procurement"
TED_SOURCE_SLUG = "ted_procurement"
HILMA_SOURCE_SLUG = "finland_hilma_procurement"
PNCP_SOURCE_SLUG = "brazil_pncp_procurement"
DOFFIN_SOURCE_SLUG = "norway_doffin_procurement"


@dataclass(frozen=True)
class ProcurementSource:
    """One source of government-contract evidence for a country.

    Shape is not part of this: UHM is a flat awards table, TED and Hilma are
    winners/notices pairs, and the country view reconciles that. What matters
    here is the dependency -- what must have run, and what must exist.
    """

    slug: str
    upstream_asset_key: str
    required_tables: tuple[str, ...]


UHM = ProcurementSource(
    slug=UHM_SOURCE_SLUG,
    upstream_asset_key="sweden_uhm_procurement_awards_clickhouse",
    required_tables=("se_uhm_procurement_awards",),
)

TED = ProcurementSource(
    slug=TED_SOURCE_SLUG,
    upstream_asset_key="ted_publish_clickhouse",
    required_tables=("ted_notice_winners", "ted_notices"),
)

HILMA = ProcurementSource(
    slug=HILMA_SOURCE_SLUG,
    upstream_asset_key="finland_hilma_clickhouse",
    required_tables=("fi_hilma_notice_winners", "fi_hilma_notices"),
)

PNCP = ProcurementSource(
    slug=PNCP_SOURCE_SLUG,
    upstream_asset_key="brazil_pncp_contracts_clickhouse",
    required_tables=("br_pncp_contracts",),
)


DOFFIN = ProcurementSource(
    slug=DOFFIN_SOURCE_SLUG,
    upstream_asset_key="norway_doffin_notices_clickhouse",
    required_tables=("no_doffin_notices",),
)
