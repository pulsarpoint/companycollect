"""Static translation maps for closed-enumerated fields.

These maps provide authoritative code→English translations for fields that have
a finite, officially-defined value set. They are kept here (copied from upstream
sources) so the translator package stays self-contained and does NOT import from
the per-country dagster defs.
"""

# Authoritative Norwegian legal-form codes → English descriptions.
# Source: Brønnøysund Register Centre (BRREG) official organisasjonsform list.
# Covers every code present in the Norway register (40 as of 2026-06).
LEGAL_FORM_DESCRIPTION_EN_BY_CODE: dict[str, str] = {
    "ENK": "Sole proprietorship",
    "AS": "Private limited company",
    "FLI": "Association/club/institution",
    "ESEK": "Condominium (owner-section co-ownership)",
    "UTLA": "Foreign entity",
    "NUF": "Norwegian-registered foreign company",
    "DA": "General partnership with shared liability",
    "BRL": "Housing cooperative",
    "KBO": "Bankruptcy estate",
    "ANS": "General partnership",
    "SAM": "Co-ownership under property law",
    "SA": "Cooperative",
    "STI": "Foundation",
    "ANNA": "Other legal entity",
    "ORGL": "Organisational subdivision",
    "KIRK": "Church of Norway",
    "BA": "Company with limited liability",
    "VPFO": "Securities fund",
    "KOMM": "Municipality",
    "IKS": "Inter-municipal company",
    "ASA": "Public limited company",
    "KF": "Municipal enterprise",
    "KS": "Limited partnership",
    "OPMV": "Separately divided unit (VAT Act section 2-2)",
    "PRE": "Shipping partnership",
    "PK": "Pension fund",
    "SPA": "Savings bank",
    "SÆR": "Other enterprise under special legislation",
    "BO": "Other estate",
    "BBL": "Housing cooperative building association",
    "PERS": "Other registered individuals",
    "GFS": "Mutual insurance company",
    "ADOS": "Administrative unit - public sector",
    "STAT": "The State",
    "KTRF": "Office-sharing arrangement",
    "FYLK": "County authority",
    "SF": "State enterprise",
    "TVAM": "Compulsorily registered for VAT",
    "FKF": "County municipal enterprise",
    "SE": "European company (SE)",
}

