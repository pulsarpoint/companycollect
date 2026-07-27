"""Per-country rules for the government-contract signal.

One country = one entry here plus one asset built from it by the factory in
``procurement.py``. Countries are separate assets rather than partitions of a
single asset because their upstream dependencies genuinely differ: Sweden reads
its national procurement register alongside TED, Norway has no ingested national
source and reads TED alone. Dagster declares deps per asset, not per partition,
so a partitioned asset would make Norway falsely depend on Swedish UHM data and
hold it back whenever that source is stale.

**A count here is not always a count of contracts.** Where a country reads two
registers, the same contract can appear in both, and whether it is collapsed
depends on the registers publishing a reference to each other:

* Finland's Hilma publishes ``ted_number``, so its two sources collapse.
* Sweden's UHM and TED publish no reference to each other. This was checked
  rather than assumed: UHM's 44 columns carry no TED number, and across sampled
  Swedish TED notices the only procurement-level identifier is
  ``cbc:ContractFolderID``, a UUID, with ``cbc:ID[InternalID]`` holding the
  buyer's own label (``22/137``) -- neither is a UHM ``Upphandlings-ID``
  (``SE75790``). A buyer/date/title hash matched zero of 242,699 rows.

So Swedish figures count *notices*, and the caveat says so. Fixing it means
answering "what is a contract across sources", which is the deferred
unification phase's job, not a filter change here.

The other structural gap the caveats now name: every country view joins only
its own company register, so a contract won by a foreign company appears in no
country's view at all. Unlike a NULL, nothing marks it -- which is why it has
to be said in words.
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
        # Sweden is the one country whose two registers cannot be deduplicated,
        # so its counts mean something different from the others'. Saying which
        # is the point -- see the module docstring on notices vs contracts.
        coverage_caveat=(
            "UHM advertised procurement and TED eForms awards. Excludes "
            "direct/non-advertised procurement, missing after-notices, and "
            "many framework call-offs. UHM publishes no contract value at "
            "all, so Swedish awards carry none. Neither register publishes a "
            "reference to the other, so a contract advertised in both is "
            "counted twice: Swedish figures are counts of notices, not of "
            "distinct contracts. Contracts won by non-Swedish companies are "
            "absent entirely."
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
            "TED adds an amount per winner. Hilma publishes its TED number, so "
            "a contract in both registers is counted once. Contracts won by "
            "non-Finnish companies are absent entirely."
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
            "thresholds are absent entirely. Contracts won by non-Norwegian "
            "companies are also absent."
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
