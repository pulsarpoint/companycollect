"""What each procurement register *is*, as data rather than UI constants.

The plan asked for "a table per country describing the register it reads". The
grain here is one row **per source**, not per (country, source), because TED is
one register serving eight countries: a per-country grain would hold its licence
and operator eight times and invite them to drift apart. Countries come back as
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

from dataclasses import dataclass

from dagster_v3.defs.company_signals.sources import (
    DECP_SOURCE_SLUG,
    DOFFIN_SOURCE_SLUG,
    HILMA_SOURCE_SLUG,
    IUB_SOURCE_SLUG,
    PNCP_SOURCE_SLUG,
    RHR_SOURCE_SLUG,
    TED_SOURCE_SLUG,
    UVO_SOURCE_SLUG,
    UHM_SOURCE_SLUG,
)
from dagster_v3.defs.ted_procurement.tables import COUNTRIES as TED_COUNTRIES

# Which countries TED serves is decided by what we ingest, and that is decided
# in ted_procurement.tables.COUNTRIES -- adding a row there is what makes a
# country appear at all. Restating the list here is how the two drifted: four
# countries were added to the ingest and this register kept describing three,
# which the source page then reported faithfully, because a page can only be as
# current as the row behind it.
TED_COUNTRY_CODES = tuple(sorted(country.country_iso2 for country in TED_COUNTRIES))


@dataclass(frozen=True)
class ProcurementRegister:
    """One register, described as its publisher describes it."""

    source_slug: str
    register_name: str
    operator: str
    country_codes: tuple[str, ...]
    homepage_url: str
    # The artifact we actually read -- the exact API endpoint or file, not the
    # publisher's landing page. This is the provenance answer: "where did the
    # rows in this table come from". A catalogue page that merely links to the
    # data is not an answer.
    api_or_download_url: str
    # How it arrives, because that is not always "we call an API". Hilma is a
    # CSV a human exports and uploads; UHM is a file download; other sources
    # are fetched. A reader checking a number needs to know which.
    retrieval_method: str
    licence: str
    # What this register does and does not include -- the register's own scope,
    # not our ingest's. "Below-threshold contracts are absent" is a fact about
    # TED; "we have not loaded 2019" is a fact about us and belongs in coverage.
    coverage_description: str
    # Where someone wanting to BID goes, where such a place exists. Empty is a
    # perfectly good answer: Hilma and Doffin are notice portals that advertise
    # and then report awards, so one address serves both, while UHM is a
    # statistics agency that never advertises anything. Not every register has
    # this, and inventing one is worse than leaving it blank.
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
        country_codes=TED_COUNTRY_CODES,
        homepage_url="https://ted.europa.eu",
        api_or_download_url="https://api.ted.europa.eu/v3/notices/search",
        retrieval_method=(
            "Fetched. Search API for the notice listing, then one eForms UBL "
            "XML per notice from ted.europa.eu/en/notice/{id}/xml. Both are "
            "snapshotted to S3 before parsing."
        ),
        documentation_url="https://docs.ted.europa.eu/api/index.html",
        licence="Reuse permitted under Decision 2011/833/EU, attribution required",
        coverage_description=(
            "EU-wide publication of procurement above the directive thresholds. "
            "A contract below its country's threshold is not published here at "
            "all, so TED alone understates any country's procurement and must "
            "be complemented by a national register for complete coverage."
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
            "https://catalog.upphandlingsmyndigheten.se/store/12/resource/239"
        ),
        retrieval_method=(
            "Downloaded. A single bulk CSV of every advertised procurement, "
            "44 columns, replaced wholesale each refresh. This file is the "
            "entire Swedish source -- there is no API behind it."
        ),
        documentation_url="https://www.upphandlingsmyndigheten.se/om-oss/var-oppna-data/",
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
        api_or_download_url=(
            "https://www.hankintailmoitukset.fi/ (search results CSV export)"
        ),
        retrieval_method=(
            "MANUAL. A human logs in, exports the search results CSV with the "
            "full column set, and uploads it with scripts/upload_hilma_export.py. "
            "Nothing fetches Hilma on a schedule, so its freshness is whenever "
            "someone last did that."
        ),
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
        retrieval_method=(
            "Fetched. Search API sliced by issue date, then one eForms UBL XML "
            "per notice from /v2/download/{doffinId} -- the search alone "
            "carries no realized value. Requires a subscription key."
        ),
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
        retrieval_method=(
            "Fetched. Paginated consultation API, 500 records a page, read by "
            "publication month and then daily by publication and update date. "
            "No bulk download exists."
        ),
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
    ProcurementRegister(
        source_slug=DECP_SOURCE_SLUG,
        register_name="Données essentielles de la commande publique (DECP)",
        operator="French Ministry of the Economy, Finance and Industrial and Digital Sovereignty",
        country_codes=("FR",),
        homepage_url=(
            "https://data.economie.gouv.fr/explore/dataset/decp-2022-marches-valides/"
        ),
        api_or_download_url=(
            "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
            "decp-2022-marches-valides/exports/csv"
        ),
        retrieval_method=(
            "Downloaded. A complete validated bulk CSV is content-addressed in "
            "S3, expanded from three holder slots to one row per holder in "
            "DuckDB, and atomically replaced in ClickHouse."
        ),
        documentation_url=(
            "https://schema.data.gouv.fr/139bercy/format-commande-publique/"
        ),
        licence="Licence Ouverte v2.0 (Etalab)",
        coverage_description=(
            "French essential public-contract data published under the 22 "
            "December 2022 order, including national and below-threshold "
            "contracts that do not reach TED. Only records passing the "
            "publisher's principal schema validations enter this dataset."
        ),
        open_tenders_url="https://www.boamp.fr/pages/recherche/",
        grain_description="one row per (contract, holder)",
        source_tables=("fr_decp_contract_holders",),
        notice_table="fr_decp_contract_holders",
        notice_key_column="contract_id",
        notes=(
            "montant is a contract-level published amount and is not allocated "
            "across co-holders. BOAMP remains the complementary French notice "
            "and active-opportunity source rather than the award signal."
        ),
    ),
    ProcurementRegister(
        source_slug=UVO_SOURCE_SLUG,
        register_name="Vestník verejného obstarávania",
        operator="Úrad pre verejné obstarávanie (UVO)",
        country_codes=("SK",),
        homepage_url="https://www.uvo.gov.sk",
        api_or_download_url=(
            "https://www.uvo.gov.sk/vestnik-a-registre/vestnik?date=DD.MM.YYYY"
        ),
        retrieval_method=(
            "Access-gated. Bulletin issues are enumerated by date and official "
            "result-notice HTML is snapshotted to S3 only after machine-reuse "
            "permission is explicitly confirmed."
        ),
        documentation_url="https://www.uvo.gov.sk/vestnik-a-registre/vestnik",
        licence="Machine-reuse licence confirmation pending",
        coverage_description=(
            "Official Slovak procurement bulletin containing above-threshold "
            "and national result notices, including IČO, buyers, lots, tender "
            "ranges, and published awarded values. Company signals select "
            "national-law awards to complement TED without known overlap."
        ),
        open_tenders_url="https://www.uvo.gov.sk/vestnik-a-registre/vestnik",
        grain_description="one row per (notice, lot, winning tenderer)",
        source_tables=("sk_uvo_procurement_notices",),
        notice_table="sk_uvo_procurement_notices",
        notice_key_column="uvo_notice_id",
        notes=(
            "The downloader refuses network access unless "
            "SLOVAKIA_UVO_MACHINE_REUSE_CONFIRMED is true. CRZ daily XML is "
            "not used because it includes many non-procurement agreements."
        ),
    ),
    ProcurementRegister(
        source_slug=IUB_SOURCE_SLUG,
        register_name="IUB public procurement open data",
        operator="Iepirkumu uzraudzības birojs (Procurement Monitoring Bureau)",
        country_codes=("LV",),
        homepage_url="https://www.iub.gov.lv/en/open-data",
        api_or_download_url=(
            "https://open.iub.gov.lv/data/notice/YYYY/MM/DD-MM-YYYY.json"
        ),
        retrieval_method=(
            "Fetched. One official JSON file per publication day is snapshotted "
            "to S3 and parsed monthly into notices, lots, winners, and contract "
            "executions."
        ),
        documentation_url="https://www.iub.gov.lv/en/open-data",
        licence="CC0 1.0",
        coverage_description=(
            "Latvian procurement notices and contract information under the "
            "national procurement laws. Result notices include national and "
            "below-threshold awards; amendments and fulfilment notices are "
            "published separately from the original award."
        ),
        open_tenders_url="https://info.iub.gov.lv/lv/meklet",
        grain_description="one row per (notice, lot, contract, winner)",
        source_tables=(
            "lv_iub_notices",
            "lv_iub_notice_lots",
            "lv_iub_notice_winners",
            "lv_iub_contract_executions",
            "lv_iub_notices_current",
            "lv_iub_notice_winners_current",
        ),
        notice_table="lv_iub_notices",
        notice_key_column="notice_id",
        notes=(
            "All raw versions are retained. Current views exclude any notice "
            "identifier referenced by a later clonedFrom value. Execution "
            "notices never create a second company award."
        ),
    ),
    ProcurementRegister(
        source_slug=RHR_SOURCE_SLUG,
        register_name="Riigihangete register (RHR)",
        operator="Estonian State Shared Service Centre (RTK)",
        country_codes=("EE",),
        homepage_url="https://riigihanked.riik.ee/rhr-web/#/open-data",
        api_or_download_url=(
            "https://riigihanked.riik.ee/rhr/api/public/v1/opendata/"
            "notice_award/YYYY/month/MM/xml"
        ),
        retrieval_method=(
            "Fetched. One official monthly eForms UBL XML bundle of contract-"
            "award notices is content-addressed in S3. A contemporaneous TED "
            "notice-identifier index is snapshotted alongside it for exact "
            "cross-source deduplication."
        ),
        documentation_url="https://www.fin.ee/media/2955/download",
        licence="CC BY-SA 3.0 EE",
        coverage_description=(
            "Estonian public procurement and signed-contract award notices "
            "published by RHR, including national and below-threshold awards "
            "that do not reach TED. Monthly machine-readable files are "
            "available from September 2017."
        ),
        open_tenders_url="https://riigihanked.riik.ee/rhr-web/#/search",
        grain_description="one row per (notice version, lot, winning tenderer)",
        source_tables=(
            "ee_rhr_procurement_notices",
            "ee_rhr_procurement_lots",
            "ee_rhr_procurement_winners",
            "ee_rhr_procurement_notices_current",
            "ee_rhr_procurement_lots_current",
            "ee_rhr_procurement_winners_current",
        ),
        notice_table="ee_rhr_procurement_notices",
        notice_key_column="notice_id",
        notes=(
            "All published versions are retained; current views remove versions "
            "referenced by a later change notice. RHR uses the same eForms UBL "
            "model as TED but also publishes national-only awards. Shared "
            "consortium values are never allocated to every member."
        ),
    ),
)


def register_for(source_slug: str) -> ProcurementRegister:
    for register in PROCUREMENT_REGISTERS:
        if register.source_slug == source_slug:
            return register
    raise KeyError(f"no procurement register declared for {source_slug!r}")
