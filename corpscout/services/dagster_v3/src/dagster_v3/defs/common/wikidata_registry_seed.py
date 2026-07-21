from dataclasses import dataclass


@dataclass(frozen=True)
class WikidataRegistrySeedSpec:
    """Declares one country's national registry-number Wikidata property so the
    Wikidata registry-number seed (``defs/wikidata/registry_seed.py``) can discover
    unlisted companies for that country — every Wikidata item carrying the property,
    not just the ones listed on a stock exchange.

    Each country module that has a Wikidata registry-number property owns exactly ONE
    module-level constant of this type (see e.g.
    ``defs/sweden_company/tables.py:WIKIDATA_REGISTRY_SEED_SPEC``). Declaring it next to
    the country's own tables — rather than in a central list inside ``defs/wikidata/`` —
    means adding a new country naturally includes wiring its Wikidata seed too; a central
    list would be easy to forget. ``defs/wikidata/registry_seed.py`` aggregates the specs
    via explicit imports from each country module and a test enforces the wiring
    (see ``tests/test_wikidata_assets.py``).

    Attributes:
        property_id: The Wikidata property PID carrying this country's national
            registry/company number (e.g. ``"P6460"`` for Sweden's Bolagsverket
            organisation number).
        country_iso2: The ISO 3166-1 alpha-2 country code the property identifies
            companies for (e.g. ``"SE"``).
        spine_asset_key: The country module's canonical companies ClickHouse-export
            asset key (its "spine" — the asset that marks the country's own register
            pipeline as complete). The Wikidata seed asset declares an
            **ordering-only** ``deps`` edge on this key purely for discoverability: it
            makes the Wikidata seed show up connected to the country's pipeline in the
            Dagster UI asset graph, and a country lacking the edge is visibly unwired.
            The edge does not force materialization — the Wikidata seed stays on its
            own weekly schedule, and the country's own pipeline stays on its own
            schedule.
    """

    property_id: str
    country_iso2: str
    spine_asset_key: str
