"""INSEE's catégorie juridique nomenclature, read from its SPARQL endpoint.

France's register stores a legal form as a bare code -- `fr_companies` holds
`5499`, never "Société à responsabilité limitée". Unlike Latvia or Estonia
there is no label column beside it, so without this nomenclature there is
nothing to translate and nothing to fall back to: 1.93M companies showed a
four-digit number.

**Why SPARQL and not the spreadsheet.** INSEE publishes the same nomenclature
as `cj_septembre_2022.xls`, which is a legacy BIFF/OLE2 workbook -- not xlsx,
so DuckDB's excel extension cannot read it and it would need `xlrd`, while
`pandas` is forbidden in this project. The SPARQL endpoint returns the same
data as CSV over plain HTTP with no new dependency, and it carries the
hierarchy (`skos:broader`) that the spreadsheet flattens.

**Why the URI filter.** A renamed form keeps its code and gains a second
concept: `.../niveauIII/5458` is the current "SARL coopérative de production
(SCOP)", while `.../niveauIII/5458/1973` is the historic "SARL coopérative
ouvrière de production (SCOP)". Both carry notation 5458, so an unfiltered
query returns 16 codes twice and the choice between them would be arbitrary.
The historic concepts carry a year suffix, so requiring the URI to end with
the code itself keeps the current label -- without naming a scheme version,
which would need editing when INSEE publishes cj15.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Mapping

INSEE_SPARQL_URL = "https://rdf.insee.fr/sparql"
CJ_GRAPH = "http://rdf.insee.fr/graphes/codes/cj"

CJ_QUERY = """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?code ?label ?parent WHERE {
  GRAPH <%s> {
    ?c a skos:Concept ; skos:notation ?code ; skos:prefLabel ?label .
    OPTIONAL { ?c skos:broader ?b . ?b skos:notation ?parent }
    FILTER(lang(?label)="fr")
    FILTER(STRENDS(STR(?c), CONCAT("/", ?code)))
  }
} ORDER BY ?code""" % CJ_GRAPH

# 10 + 39 + 260 = 309 as published. A floor rather than an equality, so a new
# revision does not fail the load while a truncated response still does.
MIN_LEGAL_FORM_ROWS = 280

# Code length by level: I is one digit, II is two, III is four.
_LEVEL_BY_LENGTH = {1: 1, 2: 2, 4: 3}


@dataclass(frozen=True)
class LegalForm:
    code: str
    level: int
    label_fr: str
    parent_code: str

    @classmethod
    def from_row(cls, code: str, label_fr: str, parent_code: str) -> LegalForm:
        return cls(
            code=code,
            level=_LEVEL_BY_LENGTH.get(len(code), 0),
            label_fr=label_fr,
            parent_code=parent_code,
        )


def parse_legal_forms(csv_text: str) -> list[LegalForm]:
    """Parse the endpoint's CSV. A real csv reader, because labels contain
    commas -- "SAS, société par actions simplifiée" is one field."""
    reader = csv.DictReader(io.StringIO(csv_text))
    forms = []
    for row in reader:
        code = (row.get("code") or "").strip()
        label = (row.get("label") or "").strip()
        if not code or not label:
            continue
        forms.append(
            LegalForm.from_row(code, label, (row.get("parent") or "").strip())
        )
    return forms


def resolve_code(code: str, nomenclature: Mapping[str, str]) -> str | None:
    """The nomenclature key that names `code`, or None.

    Sirene writes some units at level II padded to four digits: 28,520
    companies carry '2200', which is level II's '22'. Those are resolved by
    dropping the trailing zeros -- but ONLY the trailing zeros. '5498' is a
    level-III code that happens to be missing, not a padded level-II one, and
    truncating it to '54' would report the company as a plain SARL on no
    evidence.
    """
    if not code:
        return None
    if code in nomenclature:
        return code
    if len(code) == 4 and code.endswith("00") and code[:2] in nomenclature:
        return code[:2]
    return None
