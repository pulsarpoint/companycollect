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
# Brazil issues a CNPJ to every candidate for elected office so campaign
# finance can be tracked: 2,937,479 of them, 4.28% of the register. Not a
# business, not an arm of the state, and far too many to leave as "other".
POLITICAL_CANDIDATE = "political_candidate"
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
    POLITICAL_CANDIDATE: "Political candidate",
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

# Brazil: CONCLA legal natures, all 90 present in br_companies (68,629,147 rows,
# measured 2026-07-30). RFB publishes both the code and its Portuguese
# description, so every mapping below is read off the register rather than
# recalled -- the same rule the Nordic mappings follow.
#
# CONCLA's first digit is itself the official top-level group, and the corpus
# splits along it:
#     1  public administration       33 codes       73,306   0.11%
#     2  business entities           29 codes   62,574,170  91.18%
#     3  non-profits                 20 codes    2,190,359   3.19%
#     4  natural persons              3 codes    3,789,286   5.52%
#     5  extraterritorial             3 codes          619   0.00%
# So nearly 9% of Brazil's "companies" are not businesses, which is precisely
# what this module exists to make visible.
#
# Two deliberate calls. State-owned enterprises (2011 Empresa Pública, 2038
# Sociedade de Economia Mista) are COMPANY, not public sector: they trade as
# businesses and the source_label keeps the state ownership legible. Notary
# offices (3034) are OTHER rather than PUBLIC_BODY -- a Brazilian cartório
# exercises a public function under private ownership, and forcing it either way
# would be the kind of guess this module refuses to make.
BRAZIL = _mappings(
    "BR",
    {
        "0000": (UNKNOWN, "Natureza Jurídica não informada"),
        # 1xxx -- public administration
        "1015": (GOVERNMENT, "Órgão Público do Poder Executivo Federal"),
        "1023": (GOVERNMENT, "Órgão Público do Poder Executivo Estadual ou do Distrito Federal"),
        "1031": (GOVERNMENT, "Órgão Público do Poder Executivo Municipal"),
        "1040": (GOVERNMENT, "Órgão Público do Poder Legislativo Federal"),
        "1058": (GOVERNMENT, "Órgão Público do Poder Legislativo Estadual ou do Distrito Federal"),
        "1066": (GOVERNMENT, "Órgão Público do Poder Legislativo Municipal"),
        "1074": (GOVERNMENT, "Órgão Público do Poder Judiciário Federal"),
        "1082": (GOVERNMENT, "Órgão Público do Poder Judiciário Estadual"),
        "1104": (PUBLIC_BODY, "Autarquia Federal"),
        "1112": (PUBLIC_BODY, "Autarquia Estadual ou do Distrito Federal"),
        "1120": (PUBLIC_BODY, "Autarquia Municipal"),
        "1139": (PUBLIC_BODY, "Fundação Pública de Direito Público Federal"),
        "1147": (PUBLIC_BODY, "Fundação Pública de Direito Público Estadual ou do Distrito Federal"),
        "1155": (PUBLIC_BODY, "Fundação Pública de Direito Público Municipal"),
        "1163": (PUBLIC_BODY, "Órgão Público Autônomo Federal"),
        "1171": (PUBLIC_BODY, "Órgão Público Autônomo Estadual ou do Distrito Federal"),
        "1180": (PUBLIC_BODY, "Órgão Público Autônomo Municipal"),
        "1198": (PUBLIC_BODY, "Comissão Polinacional"),
        "1210": (PUBLIC_BODY, "Consórcio Público de Direito Público (Associação Pública)"),
        "1228": (PUBLIC_BODY, "Consórcio Público de Direito Privado"),
        "1236": (REGION, "Estado ou Distrito Federal"),
        "1244": (MUNICIPALITY, "Município"),
        "1252": (PUBLIC_BODY, "Fundação Pública de Direito Privado Federal"),
        "1260": (PUBLIC_BODY, "Fundação Pública de Direito Privado Estadual ou do Distrito Federal"),
        "1279": (PUBLIC_BODY, "Fundação Pública de Direito Privado Municipal"),
        "1287": (PUBLIC_BODY, "Fundo Público da Administração Indireta Federal"),
        "1295": (PUBLIC_BODY, "Fundo Público da Administração Indireta Estadual ou do Distrito Federal"),
        "1309": (PUBLIC_BODY, "Fundo Público da Administração Indireta Municipal"),
        "1317": (PUBLIC_BODY, "Fundo Público da Administração Direta Federal"),
        "1325": (PUBLIC_BODY, "Fundo Público da Administração Direta Estadual ou do Distrito Federal"),
        "1333": (PUBLIC_BODY, "Fundo Público da Administração Direta Municipal"),
        "1341": (GOVERNMENT, "União"),
        "1350": (PUBLIC_BODY, "ENTIDADE PÚBLICA SOB REGIME ESPECIAL"),
        # 2xxx -- business entities
        "2011": (COMPANY, "Empresa Pública"),
        "2038": (COMPANY, "Sociedade de Economia Mista"),
        "2046": (COMPANY, "Sociedade Anônima Aberta"),
        "2054": (COMPANY, "Sociedade Anônima Fechada"),
        "2062": (COMPANY, "Sociedade Empresária Limitada"),
        "2070": (COMPANY, "Sociedade Empresária em Nome Coletivo"),
        "2089": (COMPANY, "Sociedade Empresária em Comandita Simples"),
        "2097": (COMPANY, "Sociedade Empresária em Comandita por Ações"),
        "2100": (COMPANY, "Sociedade Mercantil de Capital e Indústria"),
        "2127": (COMPANY, "Sociedade em Conta de Participação"),
        "2135": (SOLE_TRADER, "Empresário (Individual)"),
        "2143": (COOPERATIVE, "Cooperativa"),
        "2151": (COMPANY, "Consórcio de Sociedades"),
        "2160": (COMPANY, "Grupo de Sociedades"),
        "2178": (COMPANY, "Estabelecimento, no Brasil, de Sociedade Estrangeira"),
        "2216": (COMPANY, "Empresa Domiciliada no Exterior"),
        "2224": (OTHER, "Clube/Fundo de Investimento"),
        "2232": (COMPANY, "Sociedade Simples Pura"),
        "2240": (COMPANY, "Sociedade Simples Limitada"),
        "2259": (COMPANY, "Sociedade Simples em Nome Coletivo"),
        "2267": (COMPANY, "Sociedade Simples em Comandita Simples"),
        "2275": (COMPANY, "Empresa Binacional"),
        "2283": (OTHER, "Consórcio de Empregadores"),
        "2291": (COMPANY, "Consórcio Simples"),
        "2305": (COMPANY, "Empresa Individual de Responsabilidade Limitada (de Natureza Empresária)"),
        "2313": (COMPANY, "Empresa Individual de Responsabilidade Limitada (de Natureza Simples)"),
        "2321": (COMPANY, "Sociedade Unipessoal de Advocacia"),
        "2330": (COOPERATIVE, "Cooperativas de Consumo"),
        "2348": (COMPANY, "Empresa Simples de Inovação"),
        # 3xxx -- non-profits
        "3034": (OTHER, "Serviço Notarial e Registral (Cartório)"),
        "3069": (FOUNDATION, "Fundação Privada"),
        "3077": (OTHER, "Serviço Social Autônomo"),
        "3085": (OTHER, "Condomínio Edilício"),
        "3107": (OTHER, "Comissão de Conciliação Prévia"),
        "3115": (OTHER, "Entidade de Mediação e Arbitragem"),
        "3131": (ASSOCIATION, "Entidade Sindical"),
        "3204": (FOUNDATION, "Estabelecimento, no Brasil, de Fundação ou Associação Estrangeiras"),
        "3212": (FOUNDATION, "Fundação ou Associação Domiciliada no Exterior"),
        "3220": (ASSOCIATION, "Organização Religiosa"),
        "3239": (OTHER, "Comunidade Indígena"),
        "3247": (OTHER, "Fundo Privado"),
        "3255": (ASSOCIATION, "Órgão de Direção Nacional de Partido Político"),
        "3263": (ASSOCIATION, "Órgão de Direção Regional de Partido Político"),
        "3271": (ASSOCIATION, "Órgão de Direção Local de Partido Político"),
        "3280": (ASSOCIATION, "Comitê Financeiro de Partido Político"),
        "3298": (ASSOCIATION, "Frente Plebiscitária ou Referendária"),
        "3301": (ASSOCIATION, "Organização Social (OS)"),
        "3328": (OTHER, "Plano de Benefícios de Previdência Complementar Fechada"),
        "3999": (ASSOCIATION, "Associação Privada"),
        # 4xxx -- natural persons holding a CNPJ
        "4014": (SOLE_TRADER, "Empresa Individual Imobiliária"),
        "4090": (POLITICAL_CANDIDATE, "Candidato a Cargo Político Eletivo"),
        "4120": (SOLE_TRADER, "Produtor Rural (Pessoa Física)"),
        # 5xxx -- extraterritorial. Not a Brazilian public body, so not flagged
        # public sector: an embassy is an arm of ANOTHER state.
        "5010": (OTHER, "Organização Internacional"),
        "5029": (OTHER, "Representação Diplomática Estrangeira"),
        "5037": (OTHER, "Outras Instituições Extraterritoriais"),
        "8885": (UNKNOWN, "Natureza Jurídica não informada"),
    },
)


LEGAL_FORM_MAPPINGS: tuple[LegalFormMapping, ...] = (
    *NORWAY,
    *SWEDEN,
    *FINLAND,
    *BRAZIL,
)

# Registers with no legal-form column at all, so nothing can be classified.
# Named explicitly so the gap is a stated fact rather than an empty result.
#
# BR was listed here until 2026-07-30 and should not have been: br_companies
# carries legal_nature_code and legal_nature_description_pt, a closed 90-value
# CONCLA domain. The entry was the reason nothing Brazilian was ever
# classified, so 3,789,286 natural persons and 2,190,359 non-profits read as
# companies. Verified against system.columns: dk_companies genuinely has no
# such column, br_companies does.
REGISTERS_WITHOUT_LEGAL_FORM: tuple[str, ...] = ("DK",)


def is_public_sector(entity_type: str) -> bool:
    return entity_type in PUBLIC_SECTOR_TYPES


def label_for(entity_type: str) -> str:
    return LABELS.get(entity_type, LABELS[UNKNOWN])
