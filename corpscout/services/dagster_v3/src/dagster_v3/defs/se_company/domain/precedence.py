"""Source precedence resolves claims about one domain; domains are not exclusive."""

from dagster_v3.defs.se_company.domain.tables import FOLDED_FIELDS

# An explicit filing claim precedes a linked Wikidata website, then web matching.
# Weak filing mentions still require verification; precedence is not confidence.
DOMAIN_PRECEDENCE = {
    field: {"reviewer": 20_000, "esef_filing": 900, "wikidata": 800, "common_crawl_identity": 600}
    for field in FOLDED_FIELDS
}
