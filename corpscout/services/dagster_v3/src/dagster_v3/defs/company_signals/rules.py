"""Per-country rules for the government-contract signal.

One country = one entry here plus one asset built from it by the factory in
``procurement.py``. Countries are separate assets rather than partitions of a
single asset because their upstream dependencies genuinely differ: Sweden reads
its national procurement register alongside TED, Norway has no ingested national
source and reads TED alone. Dagster declares deps per asset, not per partition,
so a partitioned asset would make Norway falsely depend on Swedish UHM data and
hold it back whenever that source is stale.
"""

from __future__ import annotations

from dataclasses import dataclass

from dagster_v3.defs.company_signals.sources import (
    HILMA,
    PNCP,
    TED,
    UHM,
    ProcurementSource,
)

SIGNAL_NAME = "government_contract"
UHM_SOURCE = "sweden_uhm_procurement"
TED_SOURCE = "ted_procurement"


@dataclass(frozen=True)
class CountryProcurementRule:
    """How one country's government-contract evidence is assembled.

    company_id_column differs per register -- se_companies keys on company_id,
    no_companies on org_number -- and identifier_length is the national company
    number's digit count, used to reject TED winner ids that cannot be one.
    """

    country_code: str
    companies_table: str
    company_id_column: str
    identifier_length: int
    ted_winner_countries: tuple[str, ...]
    coverage_caveat: str
    sources: tuple[ProcurementSource, ...]

    @property
    def contracts_view(self) -> str:
        """The migration-owned view merging this country's sources."""
        return f"{self.country_code.lower()}_government_contracts"

    @property
    def asset_name(self) -> str:
        return f"{self.country_code.lower()}_government_contract_signals_clickhouse"

    @property
    def upstream_asset_keys(self) -> tuple[str, ...]:
        return tuple(sorted({source.upstream_asset_key for source in self.sources}))

    @property
    def source_slugs(self) -> tuple[str, ...]:
        return tuple(sorted(source.slug for source in self.sources))

    @property
    def required_clickhouse_tables(self) -> tuple[str, ...]:
        needed = {self.companies_table}
        for source in self.sources:
            needed.update(source.required_tables)
        return tuple(sorted(needed))


COUNTRY_PROCUREMENT_RULES: dict[str, CountryProcurementRule] = {
    "SE": CountryProcurementRule(
        country_code="SE",
        companies_table="se_companies",
        company_id_column="company_id",
        identifier_length=10,
        ted_winner_countries=("SE", "SWE"),
        coverage_caveat=(
            "UHM advertised procurement and TED eForms awards. Excludes "
            "direct/non-advertised procurement, missing after-notices, and "
            "many framework call-offs. UHM publishes no contract value at "
            "all, so Swedish awards carry none."
        ),
        sources=(UHM, TED),
    ),
    "FI": CountryProcurementRule(
        country_code="FI",
        companies_table="fi_companies",
        company_id_column="business_id",
        # Y-tunnus is 7 digits, a dash and a check digit: 1234567-8.
        identifier_length=9,
        ted_winner_countries=("FI", "FIN"),
        coverage_caveat=(
            "Hilma national awards and TED eForms awards. Hilma publishes a "
            "realized value per lot, which is one company's amount where a lot "
            "has a single winner and a shared figure where several split it. "
            "TED adds an amount per winner."
        ),
        sources=(HILMA, TED),
    ),
    "NO": CountryProcurementRule(
        country_code="NO",
        companies_table="no_companies",
        company_id_column="org_number",
        identifier_length=9,
        ted_winner_countries=("NO", "NOR"),
        # Deliberately blunt: with no national source ingested, a Norwegian
        # company showing no contracts is indistinguishable from one whose
        # contracts all sat below the EU threshold. Saying so is the whole
        # point of the coverage row.
        coverage_caveat=(
            "TED eForms awards only; Doffin, Norway's national procurement "
            "register, is not ingested, so contracts below the EU publication "
            "thresholds are absent entirely."
        ),
        sources=(TED,),
    ),
    # Brazil is the mirror of Norway: a national register and no TED, because
    # it is not in the EU. The per-country design allows either.
    "BR": CountryProcurementRule(
        country_code="BR",
        companies_table="br_companies",
        company_id_column="cnpj_basico",
        # The 8-digit company base. Contracts name a 14-digit establishment,
        # resolved to its company at export.
        identifier_length=8,
        ted_winner_countries=(),
        coverage_caveat=(
            "PNCP contract records only. Brazil is not in the EU, so there is "
            "no TED to complement it. PNCP publishes a value per contract per "
            "supplier, so Brazilian awards carry an amount attributable to the "
            "company rather than a notice-level total."
        ),
        sources=(PNCP,),
    ),
}
