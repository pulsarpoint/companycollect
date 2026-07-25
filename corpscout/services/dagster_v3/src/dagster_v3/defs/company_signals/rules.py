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
            "UHM advertised procurement and TED eForms awards; excludes "
            "direct/non-advertised procurement, missing after-notices, and "
            "many framework call-offs."
        ),
        sources=(UHM, TED),
    ),
    # Finland has its own register too -- fi_hilma_notice_winners, 11,265 rows --
    # but Hilma is shaped like TED (a winners/notices pair) rather than like
    # UHM's single flat awards table, so it cannot reuse the national-source CTE
    # as written. Finland is therefore TED-only for now, which still resolves
    # 39,314 of 43,731 winners to 7,067 companies (89.9%, measured 2026-07-25).
    # Wiring Hilma in means generalizing NationalProcurementSource to a second
    # shape, not adding a field.
    "FI": CountryProcurementRule(
        country_code="FI",
        companies_table="fi_companies",
        company_id_column="business_id",
        # Y-tunnus is 7 digits, a dash and a check digit: 1234567-8.
        identifier_length=9,
        ted_winner_countries=("FI", "FIN"),
        coverage_caveat=(
            "TED eForms awards only; Hilma, Finland's national procurement "
            "register, is ingested but not yet joined to this signal, so "
            "contracts below the EU publication thresholds are absent."
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
}
