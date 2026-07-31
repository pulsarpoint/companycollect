"""Curated English for Czech legal forms, written against ARES's own labels.

This replaces czech_ares.resources.CZ_LEGAL_FORM_EN_BY_CODE, which was wrong.
Its values were displaced against the codes -- 771 "Dobrovolny svazek obci"
(a voluntary association of municipalities) carried "Owners' association of
units", which is what 145 means, and 145 carried "Mutual fund", which is what
541 means. Roughly 196,000 companies displayed a confident, wrong legal form:
111,581 spolky (associations) read "Trade union organization", and 80,414
unit-owner associations read "Mutual fund".

That is the exact failure this codebase keeps guarding against, and it is why
every entry below is written next to the ARES label it translates rather than
against a code list on its own. The pairing is the thing that broke, so the
pairing is what the test asserts.

Only the codes Czech companies actually carry are curated. The rest of ARES's
151 fall to the translator, working from the Czech label -- an ordinary
translation job, unlike guessing at a bare number.
"""

from __future__ import annotations

# code -> (ARES label_cs, English). The Czech is carried alongside so a
# reviewer can check the pairing in the diff, and so a test can assert the map
# still lines up with what ARES publishes.
CZECH_LEGAL_FORMS: dict[str, tuple[str, str]] = {
    "101": ("Fyzická osoba podnikající dle živnostenského zákona", "Sole trader (trade licence)"),
    "105": (
        "Fyzická osoba podnikající dle jiných zákonů než živnostenského a zákona o zemědělství",
        "Sole trader (under laws other than the trade or agriculture acts)",
    ),
    "107": ("Zemědělský podnikatel - fyzická osoba", "Agricultural entrepreneur (natural person)"),
    "111": ("Veřejná obchodní společnost", "General partnership (v.o.s.)"),
    "112": ("Společnost s ručením omezeným", "Limited liability company (s.r.o.)"),
    "113": ("Společnost komanditní", "Limited partnership (k.s.)"),
    "115": ("Společný podnik", "Joint venture"),
    "116": ("Zájmové sdružení", "Interest association"),
    "117": ("Nadace", "Foundation"),
    "118": ("Nadační fond", "Endowment fund"),
    "121": ("Akciová společnost", "Joint-stock company (a.s.)"),
    "141": ("Obecně prospěšná společnost", "Public benefit society"),
    "145": ("Společenství vlastníků jednotek", "Unit owners' association"),
    "151": ("Komoditní burza", "Commodity exchange"),
    "161": ("Ústav", "Institute"),
    "205": ("Družstvo", "Cooperative"),
    "301": ("Státní podnik", "State enterprise"),
    "313": ("Česká národní banka", "Czech National Bank"),
    "325": ("Organizační složka státu", "Organisational unit of the state"),
    "326": ("Stálý rozhodčí soud", "Permanent arbitration court"),
    "331": ("Příspěvková organizace", "Contributory organisation"),
    "332": ("Státní příspěvková organizace", "State contributory organisation"),
    "352": ("Státní organizace Správa železnic", "Státní organizace Správa železnic (state railway administration)"),
    "353": ("Rada pro veřejný dohled nad auditem", "Public Audit Oversight Board"),
    "361": (
        "Veřejnoprávní instituce (ČT,ČRo,ČTK)",
        "Public-law institution (Czech Television, Czech Radio, Czech News Agency)",
    ),
    "381": ("Fond (ze zákona)", "Fund established by law"),
    "391": ("Zdravotní pojišťovna", "Health insurance company"),
    "421": ("Odštěpný závod zahraniční právnické osoby", "Branch of a foreign legal person"),
    "422": (
        "Organizační složka zahraničního nadačního fondu",
        "Organisational unit of a foreign endowment fund",
    ),
    "423": ("Organizační složka zahraniční nadace", "Organisational unit of a foreign foundation"),
    "424": ("Zahraniční fyzická osoba", "Foreign natural person"),
    "425": ("Odštěpný závod zahraniční fyzické osoby", "Branch of a foreign natural person"),
    "426": ("Zastoupení zahraniční banky", "Representative office of a foreign bank"),
    "501": (
        "Odštěpný závod nebo jiná organizační složka podniku zapisující se do obchodního rejstříku",
        "Branch or other organisational unit entered in the commercial register",
    ),
    "541": ("Podílový, penzijní fond", "Mutual or pension fund"),
    "601": ("Vysoká škola (veřejná, státní)", "University (public or state)"),
    "641": ("Školská právnická osoba", "School legal person"),
    "661": ("Veřejná výzkumná instituce", "Public research institution"),
    "704": (
        "Zvláštní organizace pro zastoupení českých zájmů v mezinárodních nevládních organizacích",
        "Special organisation representing Czech interests in international NGOs",
    ),
    "705": (
        "Podnik nebo hospodářské zařízení sdružení",
        "Enterprise or economic facility of an association",
    ),
    "706": ("Spolek", "Association"),
    "707": ("Odborová organizace", "Trade union organisation"),
    "708": ("Organizace zaměstnavatelů", "Employers' organisation"),
    "711": ("Politická strana, politické hnutí", "Political party or movement"),
    "715": (
        "Podnik nebo hospodářské zařízení politické strany",
        "Enterprise or economic facility of a political party",
    ),
    "721": ("Církve a náboženské společnosti", "Church or religious society"),
    "722": ("Evidované církevní právnické osoby", "Registered church legal person"),
    "723": ("Svazy církví a náboženských společností", "Union of churches and religious societies"),
    "733": (
        "Organizační jednotka odborové organizace a organizace zaměstnavatelů",
        "Organisational unit of a trade union or employers' organisation",
    ),
    "736": ("Pobočný spolek", "Branch association"),
    "741": ("Stavovská organizace - profesní komora", "Professional chamber"),
    "745": ("Komora (s výjimkou profesních komor)", "Chamber other than a professional chamber"),
    "751": ("Zájmové sdružení právnických osob", "Interest association of legal persons"),
    "761": ("Honební společenstvo", "Hunting association"),
    "771": ("Dobrovolný svazek obcí", "Voluntary association of municipalities"),
    "801": ("Obec nebo městská část hlavního města Prahy", "Municipality or Prague city district"),
    "804": ("Kraj a hl.m.Praha", "Region, and the City of Prague"),
    "901": ("Zastupitelský orgán jiných států", "Foreign diplomatic mission"),
    "906": ("Zahraniční spolek", "Foreign association"),
    "907": ("Mezinárodní odborová organizace", "International trade union organisation"),
    "911": (
        "Zahraniční kulturní, informační středisko, rozhlasová, tisková a televizní agentura",
        "Foreign cultural or information centre, radio, press or television agency",
    ),
    "921": ("Mezinárodní nevládní organizace", "International non-governmental organisation"),
    "922": (
        "Organizační jednotka mezinárodní nevládní organizace",
        "Organisational unit of an international non-governmental organisation",
    ),
    "931": (
        "Evropské hospodářské zájmové sdružení",
        "European economic interest grouping (EEIG)",
    ),
    "932": ("Evropská společnost", "European company (SE)"),
    "933": ("Evropská družstevní společnost", "European cooperative society (SCE)"),
    "936": ("Zahraniční pobočný spolek", "Foreign branch association"),
    "951": (
        "Mezinárodní vojenská organizace vzniklá na základě mezinárodní smlouvy",
        "International military organisation established by treaty",
    ),
    "952": (
        "Konsorcium evropské výzkumné infrastruktury",
        "European research infrastructure consortium (ERIC)",
    ),
    "960": (
        "Právnická osoba zřízená zvláštním zákonem zapisovaná do veřejného rejstříku",
        "Legal person established by special law, entered in the public register",
    ),
    "963": (
        "Národní akreditační úřad pro terciární vzdělávání",
        "National Accreditation Bureau for Higher Education",
    ),
}

CZ_LEGAL_FORM_EN_BY_CODE: dict[str, str] = {
    code: english for code, (_, english) in CZECH_LEGAL_FORMS.items()
}

CZ_LEGAL_FORM_CS_BY_CODE: dict[str, str] = {
    code: czech for code, (czech, _) in CZECH_LEGAL_FORMS.items()
}
