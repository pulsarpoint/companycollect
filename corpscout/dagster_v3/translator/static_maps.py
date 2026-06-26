"""Static translation maps for closed-enumerated fields.

These maps provide authoritative code→English translations for fields that have
a finite, officially-defined value set. They are kept here (copied from upstream
sources) so the translator package stays self-contained and does NOT import from
the per-country dagster defs.
"""

# Authoritative Norwegian legal-form codes → English descriptions.
# Source: Brønnøysund Register Centre (BRREG) official entity type list.
LEGAL_FORM_DESCRIPTION_EN_BY_CODE: dict[str, str] = {
    "ANS": "General partnership",
    "AS": "Private limited company",
    "ASA": "Public limited company",
    "DA": "Partnership with shared liability",
    "ENK": "Sole proprietorship",
    "FKF": "Municipal enterprise",
    "FORE": "Association",
    "KOMM": "Municipality",
    "NUF": "Norwegian branch of foreign company",
    "SA": "Cooperative",
    "STI": "Foundation",
}
