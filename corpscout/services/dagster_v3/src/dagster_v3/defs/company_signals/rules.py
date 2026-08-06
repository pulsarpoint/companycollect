"""Per-country rules for the government-contract signal.

One country = one entry here plus one asset built from it by the factory in
``procurement.py``. Countries are separate assets rather than partitions of a
single asset because their upstream dependencies genuinely differ: each
European country reads its own national register alongside TED, while Brazil
reads one national source without TED.
Dagster declares deps per asset, not per partition, so a partitioned asset
would make one country falsely depend on another country's source.

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
    DECP,
    DOFFIN,
    HILMA,
    IUB,
    PNCP,
    RHR,
    TED,
    UVO,
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
        # Doffin closes the below-threshold gap this caveat used to describe.
        # What it does not close is deduplication: like Sweden, the two
        # registers publish no reference to each other. Unlike Sweden the hash
        # has a real chance -- both sides are eForms -- so this says 'may be'
        # until it is measured against loaded data.
        coverage_caveat=(
            "Doffin national awards and TED eForms awards. Doffin publishes a "
            "realized value per winner, so a Norwegian award carries an amount "
            "attributable to the company rather than a notice-level total; "
            "where it publishes only an estimate the value is left empty "
            "rather than filled with the estimate. Neither register publishes "
            "a reference to the other, so a contract advertised in both may be "
            "counted twice. Contracts won by non-Norwegian companies are "
            "absent."
        ),
        sources=(DOFFIN, TED),
    ),
    "FR": CountryProcurementRule(
        country_code="FR",
        companies_table="fr_companies",
        company_id_column="siren",
        identifier_length=9,
        ted_winner_countries=("FR", "FRA"),
        coverage_caveat=(
            "DECP national and below-threshold contracts plus TED eForms "
            "awards. DECP's montant is published once per contract and may be "
            "shared by several holders, so it is retained as contract value "
            "but never counted as one holder's spend. DECP publishes no "
            "reliable TED cross-reference, so directive-level contracts "
            "present in both sources may be counted twice. Malformed "
            "identifiers and contracts won by non-French companies are absent "
            "from company summaries."
        ),
        sources=(DECP, TED),
    ),
    "SK": CountryProcurementRule(
        country_code="SK",
        companies_table="sk_companies",
        company_id_column="ico",
        identifier_length=8,
        ted_winner_countries=("SK", "SVK"),
        coverage_caveat=(
            "UVO national and below-threshold result notices plus TED eForms "
            "awards. The company view reads only UVO notices identified as "
            "national-law awards, avoiding directive-level duplication with "
            "TED. VAT identifiers cannot be converted to IČO, malformed IČO "
            "values remain unmatched, and contracts won by foreign companies "
            "are absent from company summaries."
        ),
        sources=(TED, UVO),
    ),
    "LV": CountryProcurementRule(
        country_code="LV",
        companies_table="lv_companies",
        company_id_column="regcode",
        identifier_length=11,
        ted_winner_countries=("LV", "LVA"),
        coverage_caveat=(
            "IUB national and below-threshold result notices plus TED eForms "
            "awards. The company view reads the latest IUB notice version and "
            "only national-law awards, while contract execution notices "
            "remain separate and do not create duplicate awards. Invalid "
            "registration codes remain unmatched and contracts won by "
            "non-Latvian companies are absent from company summaries."
        ),
        sources=(IUB, TED),
    ),
    "EE": CountryProcurementRule(
        country_code="EE",
        companies_table="ee_companies",
        company_id_column="reg_code",
        identifier_length=8,
        ted_winner_countries=("EE", "EST"),
        coverage_caveat=(
            "RHR national and below-threshold contract-award notices plus TED "
            "eForms awards. RHR contains both national and directive-level "
            "awards; its notice UUID is matched to TED's publication number so "
            "the TED branch excludes exact overlaps. Consortium tender values "
            "are retained but counted as company spend only for a single "
            "winner. VAT and malformed identifiers remain unmatched, and "
            "contracts won by non-Estonian companies are absent from company "
            "summaries."
        ),
        sources=(RHR, TED),
    ),
    # Brazil reads a national register and no TED because it is not in the EU.
    # The per-country design allows either source shape.
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
