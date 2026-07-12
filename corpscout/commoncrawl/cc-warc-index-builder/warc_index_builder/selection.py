"""Canonical Common Crawl page eligibility and ranking policy."""

import re

from ._identity import new_identity_digest, update_text
from .manifests import SourceSchema


SELECTION_POLICY_VERSION = "page-selection-v1"
HTML_MIME_TYPES = ("text/html", "application/xhtml+xml")
PRIORITY_PATH_TERMS = (
    "imprint",
    "impressum",
    "legal",
    "mentions",
    "aviso-legal",
    "note-legali",
    "datenschutz",
    "colofon",
    "about",
    "uber-uns",
    "ueber-uns",
    "a-propos",
    "apropos",
    "quienes-somos",
    "sobre-nos",
    "sobre-nosotros",
    "acerca",
    "chi-siamo",
    "over-ons",
    "o-nas",
    "om-oss",
    "om-os",
    "hakkimizda",
    "company",
    "unternehmen",
    "entreprise",
    "empresa",
    "azienda",
    "contact",
    "kontakt",
    "contatti",
    "contato",
    "iletisim",
    "privacy",
    "privacidad",
    "confidentialite",
    "terms",
    "termini",
    "termos",
    "terminos",
    "agb",
    "cgv",
    "cgu",
    "voorwaarden",
)

_HTML_MIME_SQL = ", ".join(f"'{mime_type}'" for mime_type in HTML_MIME_TYPES)
_ELIGIBILITY_TERMS = (
    "fetch_status = 200",
    "COALESCE(content_mime_detected, content_mime_type) "
    f"IN ({_HTML_MIME_SQL})",
    "NULLIF(trim(root_domain), '') IS NOT NULL",
    "NULLIF(trim(url), '') IS NOT NULL",
    "NULLIF(trim(warc_filename), '') IS NOT NULL",
    "warc_record_offset >= 0",
    "warc_record_length > 0",
)
_PRIORITY_PATH_PATTERN = "|".join(re.escape(term) for term in PRIORITY_PATH_TERMS)
_RANKING_COLUMNS = (
    (
        "rank_main_site",
        "CAST(CASE WHEN url_host_name = root_domain THEN 0 "
        "WHEN url_host_name = 'www.' || root_domain THEN 0 "
        "ELSE 1 END AS UTINYINT)",
    ),
    (
        "rank_homepage",
        "CAST(CASE WHEN url_path IN ('/', '') THEN 0 ELSE 1 END AS UTINYINT)",
    ),
    (
        "rank_priority_path",
        "CAST(CASE WHEN regexp_matches(lower(url_path), "
        f"'{_PRIORITY_PATH_PATTERN}') THEN 0 ELSE 1 END AS UTINYINT)",
    ),
    (
        "rank_path_depth",
        "CAST(length(url_path) - length(replace(url_path, '/', '')) AS UBIGINT)",
    ),
    ("rank_path_length", "CAST(length(url_path) AS UBIGINT)"),
    (
        "rank_apex",
        "CAST(CASE WHEN url_host_name = root_domain THEN 0 ELSE 1 END AS UTINYINT)",
    ),
)

RANKING_COLUMN_NAMES = tuple(name for name, _expression in _RANKING_COLUMNS)
CANDIDATE_COLUMNS = (
    ("source_index", "UINTEGER"),
    ("root_domain", "VARCHAR"),
    ("url", "VARCHAR"),
    ("content_languages", "VARCHAR"),
    ("warc_filename", "VARCHAR"),
    ("warc_record_offset", "UBIGINT"),
    ("warc_record_length", "UBIGINT"),
    ("rank_main_site", "UTINYINT"),
    ("rank_homepage", "UTINYINT"),
    ("rank_priority_path", "UTINYINT"),
    ("rank_path_depth", "UBIGINT"),
    ("rank_path_length", "UBIGINT"),
    ("rank_apex", "UTINYINT"),
)

_SINGLE_PAGE_RANKING_COLUMNS = (
    "rank_main_site",
    "rank_path_depth",
    "rank_path_length",
    "rank_apex",
)
_MULTI_PAGE_RANKING_COLUMNS = RANKING_COLUMN_NAMES
_DETERMINISTIC_TIE_COLUMNS = (
    "url",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
)


def normalized_source_projection(schema: SourceSchema) -> str:
    detected_mime = (
        "CAST(content_mime_detected AS VARCHAR)"
        if schema.has_content_mime_detected
        else "CAST(NULL AS VARCHAR)"
    )
    languages = (
        "CAST(content_languages AS VARCHAR)"
        if schema.has_content_languages
        else "CAST(NULL AS VARCHAR)"
    )
    return f"""
        CAST(url_host_registered_domain AS VARCHAR) AS root_domain,
        CAST(url_host_name AS VARCHAR) AS url_host_name,
        CAST(url AS VARCHAR) AS url,
        CAST(url_path AS VARCHAR) AS url_path,
        TRY_CAST(fetch_status AS BIGINT) AS fetch_status,
        CAST(content_mime_type AS VARCHAR) AS content_mime_type,
        {detected_mime} AS content_mime_detected,
        {languages} AS content_languages,
        CAST(warc_filename AS VARCHAR) AS warc_filename,
        TRY_CAST(warc_record_offset AS UBIGINT) AS warc_record_offset,
        TRY_CAST(warc_record_length AS UBIGINT) AS warc_record_length
    """.strip()


def eligibility_predicate() -> str:
    """Return the path-free predicate shared by every selection stage."""
    return "\nAND ".join(_ELIGIBILITY_TERMS)


def ranking_projection() -> str:
    """Return all stable, named ranking fields for an eligible page row."""
    return ",\n".join(
        f"{expression} AS {name}" for name, expression in _RANKING_COLUMNS
    )


def candidate_output_projection() -> str:
    """Return explicit casts into the stable local-candidate schema."""
    return ",\n".join(
        f"CAST({name} AS {column_type}) AS {name}"
        for name, column_type in CANDIDATE_COLUMNS
    )


def ranking_column_names(pages_per_domain: int) -> tuple[str, ...]:
    """Return the semantic ranking fields for the requested selection size."""
    if pages_per_domain < 1:
        raise ValueError("pages_per_domain must be at least 1")
    if pages_per_domain == 1:
        return _SINGLE_PAGE_RANKING_COLUMNS
    return _MULTI_PAGE_RANKING_COLUMNS


def ranking_order_terms(pages_per_domain: int) -> tuple[str, ...]:
    """Return the one total ordering consumed by local and global selection."""
    columns = ranking_column_names(pages_per_domain) + _DETERMINISTIC_TIE_COLUMNS
    return tuple(f"{column} ASC NULLS LAST" for column in columns)


def ranking_order_clause(pages_per_domain: int) -> str:
    """Return the SQL ordering clause body for window functions."""
    return ",\n".join(ranking_order_terms(pages_per_domain))


def selection_policy_sha256() -> str:
    """Hash both exact path-free eligibility and ranking profiles."""
    digest = new_identity_digest("selection-policy")
    update_text(digest, SELECTION_POLICY_VERSION)

    digest.update(len(_ELIGIBILITY_TERMS).to_bytes(4, byteorder="big"))
    for expression in _ELIGIBILITY_TERMS:
        update_text(digest, expression)

    digest.update(len(_RANKING_COLUMNS).to_bytes(4, byteorder="big"))
    for name, expression in _RANKING_COLUMNS:
        update_text(digest, name)
        update_text(digest, expression)

    for ranking_names in (
        _SINGLE_PAGE_RANKING_COLUMNS,
        _MULTI_PAGE_RANKING_COLUMNS,
        _DETERMINISTIC_TIE_COLUMNS,
    ):
        digest.update(len(ranking_names).to_bytes(4, byteorder="big"))
        for name in ranking_names:
            update_text(digest, name)
    update_text(digest, "ASC")
    update_text(digest, "NULLS LAST")
    return digest.hexdigest()
