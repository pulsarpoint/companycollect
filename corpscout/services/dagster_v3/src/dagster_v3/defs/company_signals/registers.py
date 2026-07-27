"""What each procurement register *is*, as data rather than UI constants.

The plan asked for "a table per country describing the register it reads". The
grain here is one row **per source**, not per (country, source), because TED is
one register serving three countries: a per-country grain would hold its licence
and operator three times and invite them to drift apart. Countries come back as
an array, so a country page filters and a source page does not have to.

That also matches what the source pages need. They are deliberately not
country-scoped -- a TED page shows every TED notice including the Danish company
that won in Sweden, which is the only place those rows are visible at all.

**Coverage is not here.** How much of a register we hold, over what span, with
what caveat, is per (country, source) and already lives in
``company_signal_coverage``. This table answers the other question: what is this
register, who runs it, what does it publish, under what licence, and where does
someone wanting to *bid* go. Nothing built so far answers that last one.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dagster_v3.defs.company_signals.sources import (
    DOFFIN_SOURCE_SLUG,
    HILMA_SOURCE_SLUG,
    PNCP_SOURCE_SLUG,
    TED_SOURCE_SLUG,
    UHM_SOURCE_SLUG,
)


@dataclass(frozen=True)
class ProcurementRegister:
    """One register, described as its publisher describes it."""

    source_slug: str
    register_name: str
    operator: str
    country_codes: tuple[str, ...]
    homepage_url: str
    api_or_download_url: str
    licence: str
    # What this register does and does not include -- the register's own scope,
    # not our ingest's. "Below-threshold contracts are absent" is a fact about
    # TED; "we have not loaded 2019" is a fact about us and belongs in coverage.
    coverage_description: str
    # Where someone wanting to BID goes. Everything built so far answers "what
    # was awarded"; nothing answers this, and it is the question a supplier
    # actually has.
    open_tenders_url: str
    # The natural grain of a row in this source's own tables, which is what the
    # source page lists.
    grain_description: str
    # The source's own tables, so a page can read them without a hardcoded map.
    source_tables: tuple[str, ...]
    notes: str = ""
    documentation_url: str = ""
    # The column a single record is looked up by, for the raw-record panel.
    notice_key_column: str = ""
    notice_table: str = ""


PROCUREMENT_REGISTERS: tuple[ProcurementRegister, ...] = (
    ProcurementRegister(
        source_slug=TED_SOURCE_SLUG,
        register_name="Tenders Electronic Daily (TED)",
        operator="Publications Office of the European Union",
        country_codes=("FI", "NO", "SE"),
        homepage_url="https://ted.europa.eu",
        api_or_download_url="https://api.ted.europa.eu/v3/notices/search",
        documentation_url="https://docs.ted.europa.eu/api/index.html",
        licence="Reuse permitted under Decision 2011/833/EU, attribution required",
        coverage_description=(
            "EU-wide publication of procurement above the directive thresholds. "
            "A contract below its country's threshold is not published here at "
            "all, so TED alone understates any country's procurement -- which "
            "is why each country pairs it with a national register."
        ),
        open_tenders_url="https://ted.europa.eu/en/search/result",
        grain_description="one row per (notice, lot, winning tenderer)",
        source_tables=("ted_notices", "ted_notice_lots", "ted_notice_winners"),
        notice_table="ted_notices",
        notice_key_column="publication_number",
        notes=(
            "eForms UBL XML per notice. Publishes ten distinct monetary "
            "elements across notice, lot and winner grain, including framework "
            "ceilings that are not spend."
        ),
    ),
    ProcurementRegister(
        source_slug=UHM_SOURCE_SLUG,
        register_name="Upphandlingsmyndigheten statistics",
        operator="Upphandlingsmyndigheten (Swedish National Agency for Public Procurement)",
        country_codes=("SE",),
        homepage_url="https://www.upphandlingsmyndigheten.se",
        api_or_download_url=(
            "https://www.upphandlingsmyndigheten.se/statistik/statistikdatabasen/"
        ),
        licence="Swedish public sector open data",
        coverage_description=(
            "Advertised Swedish procurement. Excludes direct and "
            "non-advertised procurement, missing after-notices, and many "
            "framework call-offs. Publishes no contract value at all -- none of "
            "its 44 columns is monetary."
        ),
        # Deliberately empty. Sweden has no single national tender portal --
        # notices are advertised across competing commercial ad databases, and
        # UHM is the policy agency rather than a publisher. Our own data shows
        # five of them: Mercell (67,622 awards), e-Avrop (23,364),
        # KommersAnnons (11,677), Clira (69), Konstpool (11). Any one URL here
        # would send a supplier to a fraction of the market, and pointing at
        # UHM's homepage would imply a search that does not exist there.
        open_tenders_url="",
        grain_description="one row per (procurement, lot, supplier)",
        source_tables=("se_uhm_procurement_awards",),
        notice_table="se_uhm_procurement_awards",
        notice_key_column="source_procurement_id",
        notes=(
            "Sweden has no single national tender portal: notices are spread "
            "across registered commercial ad databases, chiefly Mercell, "
            "e-Avrop and KommersAnnons, which is why every award here names "
            "the database it was advertised in. Suppliers may be natural "
            "persons as well as companies, so rows carry a person/company "
            "classification and only companies are matched to the register."
        ),
    ),
    ProcurementRegister(
        source_slug=HILMA_SOURCE_SLUG,
        register_name="Hilma",
        operator="Ministry of Finance, Finland",
        country_codes=("FI",),
        homepage_url="https://www.hankintailmoitukset.fi",
        api_or_download_url="https://api.hankintailmoitukset.fi/",
        licence="CC BY 4.0",
        coverage_description=(
            "Finnish national procurement notices, including contracts below "
            "the EU thresholds that never reach TED. Publishes a realized value "
            "per lot, which is one company's amount where a lot has a single "
            "winner and a shared figure where several split it."
        ),
        open_tenders_url="https://www.hankintailmoitukset.fi/fi/public/procurements",
        grain_description="one row per (notice, lot, winner)",
        source_tables=("fi_hilma_notices", "fi_hilma_notice_winners"),
        notice_table="fi_hilma_notices",
        notice_key_column="notice_number",
        notes=(
            "Publishes its TED number, which is why Finland is the one country "
            "whose two registers can be deduplicated."
        ),
    ),
    ProcurementRegister(
        source_slug=DOFFIN_SOURCE_SLUG,
        register_name="Doffin",
        operator="DFØ (Norwegian Agency for Public and Financial Management)",
        country_codes=("NO",),
        homepage_url="https://doffin.no",
        api_or_download_url="https://api.doffin.no/public/v2/search",
        documentation_url="https://dof-notices-prod-api.developer.azure-api.net/apis",
        licence="Norwegian Licence for Open Government Data (NLOD)",
        coverage_description=(
            "Norwegian national procurement notices, including contracts below "
            "the EU thresholds. Publishes a realized value per winner in the "
            "notice's eForms document; the search API alone carries only an "
            "estimate, and the two differ on roughly three quarters of the "
            "notices that carry both."
        ),
        open_tenders_url="https://doffin.no/search?status=ACTIVE",
        grain_description="one row per (notice, lot, winner)",
        source_tables=("no_doffin_notices",),
        notice_table="no_doffin_notices",
        notice_key_column="doffin_id",
        notes=(
            "Requires an Azure API Management subscription key. Winners carry "
            "the organisasjonsnummer directly, so no transformation is needed "
            "to match the company register -- but foreign winners appear too, "
            "and theirs is not a Norwegian number."
        ),
    ),
    ProcurementRegister(
        source_slug=PNCP_SOURCE_SLUG,
        register_name="Portal Nacional de Contratações Públicas (PNCP)",
        operator="Serpro, for the Brazilian federal government",
        country_codes=("BR",),
        homepage_url="https://www.gov.br/pncp/",
        api_or_download_url="https://pncp.gov.br/api/consulta/v1/contratos",
        documentation_url=(
            "https://www.gov.br/pncp/pt-br/acesso-a-informacao/dados-abertos"
        ),
        licence="Brazilian open government data (Lei 12.527/2011)",
        coverage_description=(
            "Federal, state and municipal contracts under Lei 14.133/2021. "
            "Publishes a value per contract per supplier, so a Brazilian award "
            "carries an amount attributable to the company. Adoption was "
            "partial before the law took full effect, so early years are thin."
        ),
        open_tenders_url="https://pncp.gov.br/app/editais",
        grain_description="one row per (contract, supplier)",
        source_tables=("br_pncp_contracts",),
        notice_table="br_pncp_contracts",
        notice_key_column="numero_controle_pncp",
        notes=(
            "No bulk download exists -- confirmed against portaldatransparencia "
            "and dadosabertos.compras.gov.br -- so the whole register is read "
            "through a rate-limited paginated API."
        ),
    ),
)


def register_for(source_slug: str) -> ProcurementRegister:
    for register in PROCUREMENT_REGISTERS:
        if register.source_slug == source_slug:
            return register
    raise KeyError(f"no procurement register declared for {source_slug!r}")
