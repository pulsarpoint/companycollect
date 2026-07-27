"""What kind of entity a legal form denotes, normalised across registers.

Nordic and most European registers are **legal-entity** registers, not
company registers: a municipality, a ministry and a hospital trust each hold an
organisation number and sit in the same table as a hairdresser. That is faithful
to the source and worth keeping -- dropping public bodies would break the link
from a procurement buyer to the entity that issued the tender.

What it costs is that "company" becomes the wrong word for a slice of the data,
with no way to tell which slice. This module supplies the missing axis: one
normalised ``entity_type`` per (country, legal form), so a page can say
"Government agency" instead of implying a business, and a count can exclude them
deliberately rather than silently.

**Every mapping here is grounded in the register's own data**, not in memory of
a national scheme. Sweden publishes no description column and mixes two coding
schemes in one field, so its codes were identified from the entities carrying
them -- 81 holds JUSTITIEKANSLERN and Integritetsskyddsmyndigheten, 82 holds
UPPLANDS VÄSBY KOMMUN, 84 holds REGION STOCKHOLM. Norway and Finland publish
descriptions, so theirs are read off those.

Where a form is genuinely ambiguous it is NOT forced into a bucket. Norway's
``ORGL`` (Organisasjonsledd) is a sub-unit of some parent, public or private,
and only the parent settles it; calling all 1,607 of them public would be a
guess dressed as a fact. They get their own type and ``is_public_sector = 0``,
with the limitation recorded rather than hidden.
"""

from __future__ import annotations

from dataclasses import dataclass

# The normalised vocabulary. Deliberately small: this exists to answer "is this
# a business or an arm of the state", not to re-encode every national nuance --
# the original code and description stay on the register table for that.
GOVERNMENT = "government"
MUNICIPALITY = "municipality"
REGION = "region"
PUBLIC_BODY = "public_body"
COMPANY = "company"
SOLE_TRADER = "sole_trader"
ASSOCIATION = "association"
FOUNDATION = "foundation"
COOPERATIVE = "cooperative"
ESTATE = "estate"
ORGANISATIONAL_UNIT = "organisational_unit"
OTHER = "other"
UNKNOWN = "unknown"

# The types that make is_public_sector true. ORGANISATIONAL_UNIT is deliberately
# absent: see the module docstring.
PUBLIC_SECTOR_TYPES = frozenset({GOVERNMENT, MUNICIPALITY, REGION, PUBLIC_BODY})

LABELS: dict[str, str] = {
    GOVERNMENT: "Government agency",
    MUNICIPALITY: "Municipality",
    REGION: "Region",
    PUBLIC_BODY: "Public body",
    COMPANY: "Company",
    SOLE_TRADER: "Sole trader",
    ASSOCIATION: "Association",
    FOUNDATION: "Foundation",
    COOPERATIVE: "Cooperative",
    ESTATE: "Estate",
    ORGANISATIONAL_UNIT: "Organisational unit",
    OTHER: "Other",
    UNKNOWN: "Unknown",
}


@dataclass(frozen=True)
class LegalFormMapping:
    country_code: str
    legal_form_code: str
    entity_type: str
    # What the register itself calls this form, in its own language where that
    # is all it publishes. Kept so a reader can check the classification against
    # the source rather than trusting it.
    source_label: str


def _mappings(country: str, rows: dict[str, tuple[str, str]]) -> list[LegalFormMapping]:
    return [
        LegalFormMapping(
            country_code=country,
            legal_form_code=code,
            entity_type=entity_type,
            source_label=label,
        )
        for code, (entity_type, label) in rows.items()
    ]


# Norway -- Brreg organisasjonsform. Descriptions are the register's own.
NORWAY = _mappings(
    "NO",
    {
        "STAT": (GOVERNMENT, "Staten"),
        "ADOS": (GOVERNMENT, "Administrativ enhet - offentlig sektor"),
        "KOMM": (MUNICIPALITY, "Kommune"),
        "FYLK": (REGION, "Fylkeskommune"),
        "KF": (PUBLIC_BODY, "Kommunalt foretak"),
        "FKF": (PUBLIC_BODY, "Fylkeskommunalt foretak"),
        "SF": (PUBLIC_BODY, "Statsforetak"),
        "IKS": (PUBLIC_BODY, "Interkommunalt selskap"),
        "KIRK": (PUBLIC_BODY, "Den norske kirke"),
        # A sub-unit of some parent. 144 of Norway's procurement buyers are one,
        # which suggests most are public -- but 1,607 exist in total and the
        # form itself does not say. Left unclassified rather than guessed.
        "ORGL": (ORGANISATIONAL_UNIT, "Organisasjonsledd"),
        "AS": (COMPANY, "Aksjeselskap"),
        "ASA": (COMPANY, "Allmennaksjeselskap"),
        "NUF": (COMPANY, "Norskregistrert utenlandsk foretak"),
        "UTLA": (COMPANY, "Utenlandsk enhet"),
        "BA": (COMPANY, "Selskap med begrenset ansvar"),
        "SE": (COMPANY, "Europeisk selskap"),
        "DA": (COMPANY, "Ansvarlig selskap med delt ansvar"),
        "ANS": (COMPANY, "Ansvarlig selskap med solidarisk ansvar"),
        "KS": (COMPANY, "Kommandittselskap"),
        "SPA": (COMPANY, "Sparebank"),
        "GFS": (COMPANY, "Gjensidig forsikringsselskap"),
        "PRE": (COMPANY, "Partrederi"),
        "ENK": (SOLE_TRADER, "Enkeltpersonforetak"),
        "FLI": (ASSOCIATION, "Forening/lag/innretning"),
        "STI": (FOUNDATION, "Stiftelse"),
        "SA": (COOPERATIVE, "Samvirkeforetak"),
        "BRL": (COOPERATIVE, "Borettslag"),
        "BBL": (COOPERATIVE, "Boligbyggelag"),
        "KBO": (ESTATE, "Konkursbo"),
        "BO": (ESTATE, "Andre bo"),
        "ESEK": (OTHER, "Eierseksjonssameie"),
        "SAM": (OTHER, "Tingsrettslig sameie"),
        "ANNA": (OTHER, "Annen juridisk person"),
        "VPFO": (OTHER, "Verdipapirfond"),
        "PK": (OTHER, "Pensjonskasse"),
        "SÆR": (OTHER, "Annet foretak iflg. særskilt lov"),
        "OPMV": (OTHER, "Særskilt oppdelt enhet, jf. mval. § 2-2"),
        "PERS": (OTHER, "Andre enkeltpersoner i tilknyttet register"),
        "KTRF": (OTHER, "Kontorfellesskap"),
        "TVAM": (OTHER, "Tvangsregistrert for MVA"),
    },
)

# Sweden -- no description column, and two coding schemes in one field. Every
# entry below was identified from the entities carrying the code (see the
# module docstring); the label is the Swedish term those entities evidence.
SWEDEN = _mappings(
    "SE",
    {
        "81": (GOVERNMENT, "Statlig myndighet"),
        "82": (MUNICIPALITY, "Kommun"),
        "83": (PUBLIC_BODY, "Kommunalförbund"),
        "84": (REGION, "Region"),
        "88": (PUBLIC_BODY, "Offentligt kreditinstitut"),
        "AB-ORGFO": (COMPANY, "Aktiebolag"),
        "49": (COMPANY, "Aktiebolag"),
        "FL-ORGFO": (COMPANY, "Filial till utländskt bolag"),
        "KB-ORGFO": (COMPANY, "Kommanditbolag"),
        "31": (COMPANY, "Handels- eller kommanditbolag"),
        "HB-ORGFO": (COMPANY, "Handelsbolag"),
        "E-ORGFO": (SOLE_TRADER, "Enskild firma"),
        "10": (SOLE_TRADER, "Fysisk person / enskild näringsidkare"),
        "61": (ASSOCIATION, "Ideell förening"),
        "I-ORGFO": (ASSOCIATION, "Ideell förening"),
        "21": (ASSOCIATION, "Aktiesparklubb eller liknande"),
        "63": (ASSOCIATION, "Registrerat trossamfund"),
        "96": (ASSOCIATION, "Utländsk beskickning eller kammare"),
        "72": (FOUNDATION, "Stiftelse eller fond"),
        "71": (FOUNDATION, "Familjestiftelse eller legat"),
        "S-ORGFO": (FOUNDATION, "Stiftelse"),
        "EK-ORGFO": (COOPERATIVE, "Ekonomisk förening"),
        "51": (COOPERATIVE, "Ekonomisk förening"),
        "BRF-ORGFO": (COOPERATIVE, "Bostadsrättsförening"),
        "53": (COOPERATIVE, "Bostadsrättsförening"),
        "BF-ORGFO": (COOPERATIVE, "Bostadsförening"),
        "98": (COOPERATIVE, "Samfällighets- eller sambruksförening"),
        "62": (OTHER, "Samfällighet"),
        "91": (ESTATE, "Dödsbo"),
        "23": (OTHER, "Värdepappersfond"),
        # Semi-public agricultural societies established under public law but
        # not organs of government. Left out of the public-sector flag.
        "87": (OTHER, "Hushållningssällskap"),
        "22": (COMPANY, "Partrederi"),
        "92": (COMPANY, "Försäkringsförening"),
        "94": (COMPANY, "Understödsförening"),
        "FAB-ORGFO": (COMPANY, "Försäkringsaktiebolag"),
        "BAB-ORGFO": (COMPANY, "Bankaktiebolag"),
        "SB-ORGFO": (COMPANY, "Sparbank"),
        "BFL-ORGFO": (COMPANY, "Filial till utländsk bank"),
        "OFB-ORGFO": (COMPANY, "Ömsesidigt försäkringsbolag"),
        "KHF-ORGFO": (COOPERATIVE, "Kooperativ hyresrättsförening"),
        "99": (OTHER, "Övrig juridisk person"),
    },
)

# Finland -- PRH publishes an English description per code, so these are read
# off the register rather than interpreted.
FINLAND = _mappings(
    "FI",
    {
        "16": (COMPANY, "Limited company"),
        "13": (COMPANY, "Limited partnership"),
        "10": (COMPANY, "Mutual real estate limited company"),
        "5": (COMPANY, "Partnership"),
        "19": (COMPANY, "Branch"),
        "2": (COOPERATIVE, "Housing corporation"),
        "14": (COOPERATIVE, "Cooperative"),
        "6": (ASSOCIATION, "Non-profit association"),
        "17": (COMPANY, "Public limited company"),
        "15": (COMPANY, "Cooperative bank"),
        "12": (COMPANY, "Mutual insurance company"),
        "24": (COMPANY, "Insurance company"),
        "20": (COMPANY, "Savings bank"),
        "60": (COMPANY, "Foreign organisation"),
        "57": (COMPANY, "Business partnership"),
        "25": (ASSOCIATION, "Insurance association"),
        "50": (ASSOCIATION, "Joint interest groups"),
        "18": (FOUNDATION, "Foundation"),
        "1": (COOPERATIVE, "Housing co-operative"),
        "4": (COOPERATIVE, "Tenant-owners' society"),
        "54": (ESTATE, "Bankrupt's estate"),
        "51": (OTHER, "Taxable grouping"),
        "52": (OTHER, "Other subject with joint liability to tax withholding"),
        # The ONLY public form in the Finnish register. Metsähallitus, the state
        # forest enterprise, is one of three. See the note below on why Finland
        # has so few.
        "22": (PUBLIC_BODY, "Public business"),
    },
)

# Finland's register is a TRADE register, unlike Brreg and Bolagsverket which
# register legal entities of every kind. Finnish municipalities and ministries
# simply do not appear in it -- 536 of 919 Finnish procurement buyers resolve to
# nothing here, and of the 383 that do, almost all are state-OWNED COMPANIES
# (Fortum Power and Heat Oy, VR-Yhtymä Oyj) rather than organs of the state.
#
# So a near-zero public-sector count for Finland is the register being itself,
# not a hole in the mapping. Worth stating because the obvious reading of that
# number is that something is broken.

LEGAL_FORM_MAPPINGS: tuple[LegalFormMapping, ...] = (
    *NORWAY,
    *SWEDEN,
    *FINLAND,
)

# Registers with no legal-form column at all, so nothing can be classified.
# Named explicitly so the gap is a stated fact rather than an empty result.
REGISTERS_WITHOUT_LEGAL_FORM: tuple[str, ...] = ("BR", "DK")


def is_public_sector(entity_type: str) -> bool:
    return entity_type in PUBLIC_SECTOR_TYPES


def label_for(entity_type: str) -> str:
    return LABELS.get(entity_type, LABELS[UNKNOWN])
