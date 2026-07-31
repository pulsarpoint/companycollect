"""Curated English for legal forms, keyed by each register's own code.

A legal form is a term of art, not prose. "Aksjeselskap" is a private limited
company and "Bostadsrättsförening" is a tenant-owners' association; a machine
asked to translate them produces something readable and wrong, and there are
only a couple of hundred of them. So they are mapped by hand, reviewed in a
diff, and inserted as STATIC translations — the same mechanism
norway_brreg/assets/translation.py already uses for the same problem.

Keyed on the register's CODE rather than on the label, because a label can
repeat: Sweden has 57 codes carrying 47 distinct labels, and two codes meaning
different things could otherwise collapse onto one translation.

Norway is imported rather than re-typed. Its 40 forms were curated when
no_companies gained its English column, the codes are identical here, and a
second copy would drift.

Where a label is ALREADY English, no entry is made: Finland's register
publishes most of its forms in English (Limited company, Savings bank,
Bankrupt's estate), and the view falls back to the register's own wording, so
an identity mapping would only add rows to maintain.
"""

from __future__ import annotations

from dagster_v3.defs.norway_brreg.assets.translation import (
    LEGAL_FORM_DESCRIPTION_EN_BY_CODE as _NORWAY_EN_BY_CODE,
)

# Sweden — Bolagsverket / SCB legal forms.
SWEDEN_EN_BY_LABEL: dict[str, str] = {
    "Aktiebolag": "Limited company",
    "Aktiesparklubb eller liknande": "Share savings club or similar",
    "Arbetslöshetskassa": "Unemployment insurance fund",
    "Bankaktiebolag": "Banking limited company",
    "Bostadsförening": "Housing association",
    "Bostadsrättsförening": "Tenant-owners' association",
    "Dödsbo": "Estate of a deceased person",
    "Ekonomisk förening": "Economic association (cooperative)",
    "Enskild firma": "Sole proprietorship",
    "Europabolag (SE)": "European company (SE)",
    "Europeisk forskningsinfrastruktur (ERIC)": (
        "European research infrastructure consortium (ERIC)"
    ),
    "Europeisk kooperativ förening (SCE)": "European cooperative society (SCE)",
    "Familjestiftelse eller legat": "Family foundation or legacy",
    "Filial till utländsk bank": "Branch of a foreign bank",
    "Filial till utländskt bolag": "Branch of a foreign company",
    "Fysisk person / enskild näringsidkare": "Natural person / sole trader",
    "Försäkringsaktiebolag": "Insurance limited company",
    "Försäkringsförening": "Insurance association",
    "Handels- eller kommanditbolag": "General or limited partnership",
    "Handelsbolag": "General partnership",
    "Hushållningssällskap": "Rural economy and agricultural society",
    "Ideell förening": "Non-profit association",
    "Kommanditbolag": "Limited partnership",
    "Kommun": "Municipality",
    "Kommunalförbund": "Local government federation",
    "Kooperativ hyresrättsförening": "Cooperative tenancy association",
    "Medlemsbank": "Member bank (cooperative bank)",
    "Offentligt kreditinstitut": "Public credit institution",
    "Partrederi": "Shipping partnership",
    "Region": "Region",
    "Registrerat trossamfund": "Registered religious community",
    "Sambruksförening": "Joint farming association",
    "Samfällighet": "Joint property unit",
    "Samfällighets- eller sambruksförening": (
        "Joint property or joint farming association"
    ),
    "Sparbank": "Savings bank",
    "Statlig myndighet": "State authority",
    "Stiftelse": "Foundation",
    "Stiftelse eller fond": "Foundation or fund",
    "Tjänstepensionsaktiebolag": "Occupational pension limited company",
    "Tjänstepensionsförening": "Occupational pension association",
    "Trossamfund": "Religious community",
    "Understödsförening": "Friendly society",
    "Utländsk beskickning eller kammare": "Foreign mission or chamber of commerce",
    "Värdepappersfond": "Securities fund",
    "Ömsesidigt försäkringsbolag": "Mutual insurance company",
    "Ömsesidigt tjänstepensionsbolag": "Mutual occupational pension company",
    "Övrig juridisk person": "Other legal entity",
}

# Finland — only the forms YTJ publishes in Finnish. The rest of its register
# is already English and is left to fall through.
FINLAND_EN_BY_LABEL: dict[str, str] = {
    "Asukashallintoalue": "Resident administration area",
    "Eurooppayhtiö": "European company (SE)",
    "Hypoteekkiyhdistys": "Mortgage society",
    "Julkinen vakuutusosakeyhtiö": "Public insurance limited company",
    "Muu kiinteistöosakeyhtiö": "Other real estate limited company",
    "Muu säätiö": "Other foundation",
    "Muu taloudellinen yhdistys": "Other economic association",
    "Muut oikeushenkilöt": "Other legal entities",
    "Paikallisyhteisö": "Local community",
    "Taloudellinen yhdistys": "Economic association",
    "Yhteismetsä": "Jointly owned forest",
}


# Brazil — CONCLA legal natures (naturezas juridicas). Heavily structured: the
# public-sector forms repeat across the federal, state and municipal levels, so
# most of this map is that grid rather than 89 unrelated terms.
BRAZIL_EN_BY_LABEL: dict[str, str] = {
    "Associação Privada": "Private association",
    "Autarquia Estadual ou do Distrito Federal": "State or Federal District agency",
    "Autarquia Federal": "Federal agency",
    "Autarquia Municipal": "Municipal agency",
    "Candidato a Cargo Político Eletivo": "Candidate for elected political office",
    "Clube/Fundo de Investimento": "Investment club or fund",
    "Comissão Polinacional": "Multinational commission",
    "Comissão de Conciliação Prévia": "Prior conciliation commission",
    "Comitê Financeiro de Partido Político": "Political party finance committee",
    "Comunidade Indígena": "Indigenous community",
    "Condomínio Edilício": "Building condominium",
    "Consórcio Público de Direito Privado": "Public consortium under private law",
    "Consórcio Público de Direito Público (Associação Pública)": (
        "Public consortium under public law (public association)"
    ),
    "Consórcio Simples": "Simple consortium",
    "Consórcio de Empregadores": "Employers' consortium",
    "Consórcio de Sociedades": "Consortium of companies",
    "Cooperativa": "Cooperative",
    "Cooperativas de Consumo": "Consumer cooperative",
    "ENTIDADE PÚBLICA SOB REGIME ESPECIAL": "Public entity under special regime",
    "Empresa Binacional": "Binational company",
    "Empresa Domiciliada no Exterior": "Company domiciled abroad",
    "Empresa Individual Imobiliária": "Individual real estate business",
    "Empresa Individual de Responsabilidade Limitada (de Natureza Empresária)": (
        "Individual limited liability company (business)"
    ),
    "Empresa Individual de Responsabilidade Limitada (de Natureza Simples)": (
        "Individual limited liability company (non-business)"
    ),
    "Empresa Pública": "State-owned company",
    "Empresa Simples de Inovação": "Simple innovation company",
    "Empresário (Individual)": "Sole trader",
    "Entidade Sindical": "Trade union body",
    "Entidade de Mediação e Arbitragem": "Mediation and arbitration body",
    "Estabelecimento, no Brasil, de Fundação ou Associação Estrangeiras": (
        "Brazilian establishment of a foreign foundation or association"
    ),
    "Estabelecimento, no Brasil, de Sociedade Estrangeira": (
        "Brazilian establishment of a foreign company"
    ),
    "Estado ou Distrito Federal": "State or Federal District",
    "Frente Plebiscitária ou Referendária": "Plebiscite or referendum campaign front",
    "Fundação Privada": "Private foundation",
    "Fundação Pública de Direito Privado Estadual ou do Distrito Federal": (
        "State or Federal District public foundation under private law"
    ),
    "Fundação Pública de Direito Privado Federal": (
        "Federal public foundation under private law"
    ),
    "Fundação Pública de Direito Privado Municipal": (
        "Municipal public foundation under private law"
    ),
    "Fundação Pública de Direito Público Estadual ou do Distrito Federal": (
        "State or Federal District public foundation under public law"
    ),
    "Fundação Pública de Direito Público Federal": (
        "Federal public foundation under public law"
    ),
    "Fundação Pública de Direito Público Municipal": (
        "Municipal public foundation under public law"
    ),
    "Fundação ou Associação Domiciliada no Exterior": (
        "Foundation or association domiciled abroad"
    ),
    "Fundo Privado": "Private fund",
    "Fundo Público da Administração Direta Estadual ou do Distrito Federal": (
        "State or Federal District public fund, direct administration"
    ),
    "Fundo Público da Administração Direta Federal": (
        "Federal public fund, direct administration"
    ),
    "Fundo Público da Administração Direta Municipal": (
        "Municipal public fund, direct administration"
    ),
    "Fundo Público da Administração Indireta Estadual ou do Distrito Federal": (
        "State or Federal District public fund, indirect administration"
    ),
    "Fundo Público da Administração Indireta Federal": (
        "Federal public fund, indirect administration"
    ),
    "Fundo Público da Administração Indireta Municipal": (
        "Municipal public fund, indirect administration"
    ),
    "Grupo de Sociedades": "Group of companies",
    "Município": "Municipality",
    "Natureza Jurídica não informada": "Legal nature not reported",
    "Organização Internacional": "International organisation",
    "Organização Religiosa": "Religious organisation",
    "Organização Social (OS)": "Social organisation (OS)",
    "Outras Instituições Extraterritoriais": "Other extraterritorial institutions",
    "Plano de Benefícios de Previdência Complementar Fechada": (
        "Closed supplementary pension benefit plan"
    ),
    "Produtor Rural (Pessoa Física)": "Rural producer (natural person)",
    "Representação Diplomática Estrangeira": "Foreign diplomatic mission",
    "Serviço Notarial e Registral (Cartório)": "Notary and registry office",
    "Serviço Social Autônomo": "Autonomous social service",
    "Sociedade Anônima Aberta": "Publicly held corporation",
    "Sociedade Anônima Fechada": "Closely held corporation",
    "Sociedade Empresária Limitada": "Limited liability company",
    "Sociedade Empresária em Comandita Simples": "Limited partnership",
    "Sociedade Empresária em Comandita por Ações": "Partnership limited by shares",
    "Sociedade Empresária em Nome Coletivo": "General partnership",
    "Sociedade Mercantil de Capital e Indústria": (
        "Capital and industry commercial partnership"
    ),
    "Sociedade Simples Limitada": "Simple limited company",
    "Sociedade Simples Pura": "Simple company",
    "Sociedade Simples em Comandita Simples": "Simple limited partnership",
    "Sociedade Simples em Nome Coletivo": "Simple general partnership",
    "Sociedade Unipessoal de Advocacia": "Sole-practitioner law firm",
    "Sociedade de Economia Mista": "Mixed-capital company",
    "Sociedade em Conta de Participação": "Silent partnership",
    "União": "Federal Union",
    "Órgão Público Autônomo Estadual ou do Distrito Federal": (
        "State or Federal District autonomous public body"
    ),
    "Órgão Público Autônomo Federal": "Federal autonomous public body",
    "Órgão Público Autônomo Municipal": "Municipal autonomous public body",
    "Órgão Público do Poder Executivo Estadual ou do Distrito Federal": (
        "State or Federal District executive public body"
    ),
    "Órgão Público do Poder Executivo Federal": "Federal executive public body",
    "Órgão Público do Poder Executivo Municipal": "Municipal executive public body",
    "Órgão Público do Poder Judiciário Estadual": "State judicial public body",
    "Órgão Público do Poder Judiciário Federal": "Federal judicial public body",
    "Órgão Público do Poder Legislativo Estadual ou do Distrito Federal": (
        "State or Federal District legislative public body"
    ),
    "Órgão Público do Poder Legislativo Federal": "Federal legislative public body",
    "Órgão Público do Poder Legislativo Municipal": "Municipal legislative public body",
    "Órgão de Direção Local de Partido Político": "Political party local directorate",
    "Órgão de Direção Nacional de Partido Político": (
        "Political party national directorate"
    ),
    "Órgão de Direção Regional de Partido Político": (
        "Political party regional directorate"
    ),
}


def _sweden_by_code(labels_by_code: dict[str, str]) -> dict[str, str]:
    """Sweden's map is authored by LABEL because its codes are opaque.

    Ten of its 57 codes share a label with another, so keying the authored map
    on the label keeps it readable and this resolves it to codes at load time.
    """
    return {
        code: SWEDEN_EN_BY_LABEL[label]
        for code, label in labels_by_code.items()
        if label in SWEDEN_EN_BY_LABEL
    }


def _finland_by_code(labels_by_code: dict[str, str]) -> dict[str, str]:
    return {
        code: FINLAND_EN_BY_LABEL[label]
        for code, label in labels_by_code.items()
        if label in FINLAND_EN_BY_LABEL
    }


def english_by_code(
    country_code: str, labels_by_code: dict[str, str]
) -> dict[str, str]:
    """The curated code -> English map for one country.

    An empty result means the country has no curation, and its labels are left
    to the machine loader or to the register's own wording. Guessing is the one
    thing this must not do: a wrong legal form still reads like a right one.
    """
    if country_code == "NO":
        return dict(_NORWAY_EN_BY_CODE)
    if country_code == "SE":
        return _sweden_by_code(labels_by_code)
    if country_code == "FI":
        return _finland_by_code(labels_by_code)
    if country_code == "BR":
        return {
            code: BRAZIL_EN_BY_LABEL[label]
            for code, label in labels_by_code.items()
            if label in BRAZIL_EN_BY_LABEL
        }
    return {}


__all__ = [
    "BRAZIL_EN_BY_LABEL",
    "FINLAND_EN_BY_LABEL",
    "SWEDEN_EN_BY_LABEL",
    "english_by_code",
]
