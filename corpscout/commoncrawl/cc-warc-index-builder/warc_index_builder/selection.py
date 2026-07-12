"""DuckDB SQL for selecting representative Common Crawl pages."""

import re


SELECTION_VERSION = 1
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

_PRIORITY_PATH_PATTERN = "|".join(re.escape(term) for term in PRIORITY_PATH_TERMS)
_UBIGINT_MAX = 0xFFFFFFFFFFFFFFFF
_RANK_COLUMNS = (
    "rank_main_site",
    "rank_homepage",
    "rank_priority_path",
    "rank_path_depth",
    "rank_path_length",
    "rank_apex",
)
_SINGLE_PAGE_RANK_COLUMNS = (
    "rank_main_site",
    "rank_path_depth",
    "rank_path_length",
    "rank_apex",
)
_TIE_COLUMNS = ("url", "warc_filename", "warc_record_offset", "warc_record_length")


def global_order_clause(pages_per_domain: int) -> str:
    """Return the exact total order used for both local and global selection."""
    if not 1 <= pages_per_domain <= 0xFFFF:
        raise ValueError("pages_per_domain must be between 1 and uint16 max")
    ranks = _SINGLE_PAGE_RANK_COLUMNS if pages_per_domain == 1 else _RANK_COLUMNS
    return ",\n".join(f"{column} ASC NULLS LAST" for column in (*ranks, *_TIE_COLUMNS))


def candidate_query(
    source: str,
    source_index: int,
    pages_per_domain: int,
    has_detected_mime: bool,
    has_languages: bool,
) -> str:
    """Return SQL selecting one source's globally sufficient top-N candidates."""
    if not source.strip():
        raise ValueError("source must not be blank")
    if not 0 <= source_index <= 0xFFFFFFFF:
        raise ValueError("source_index must be between 0 and uint32 max")
    order = global_order_clause(pages_per_domain)
    detected_mime = (
        "CAST(content_mime_detected AS VARCHAR)"
        if has_detected_mime
        else "CAST(NULL AS VARCHAR)"
    )
    languages = (
        "CAST(content_languages AS VARCHAR)"
        if has_languages
        else "CAST(NULL AS VARCHAR)"
    )
    mime_types = ", ".join(f"'{value}'" for value in HTML_MIME_TYPES)

    return f"""
        WITH normalized AS (
            SELECT
                CAST(url_host_registered_domain AS VARCHAR) AS root_domain,
                CAST(url_host_name AS VARCHAR) AS url_host_name,
                CAST(url AS VARCHAR) AS url,
                CAST(url_path AS VARCHAR) AS url_path,
                TRY_CAST(fetch_status AS BIGINT) AS fetch_status,
                CAST(content_mime_type AS VARCHAR) AS content_mime_type,
                {detected_mime} AS content_mime_detected,
                {languages} AS content_languages,
                CAST(warc_filename AS VARCHAR) AS warc_filename,
                TRY_CAST(warc_record_offset AS HUGEINT) AS warc_record_offset,
                TRY_CAST(warc_record_length AS HUGEINT) AS warc_record_length
            FROM {source}
        ),
        eligible AS (
            SELECT
                CAST({source_index} AS UINTEGER) AS source_index,
                root_domain, url, content_languages, warc_filename,
                CASE
                    WHEN warc_record_offset > {_UBIGINT_MAX}::HUGEINT
                        THEN error('eligible WARC record offset exceeds UBIGINT')
                    ELSE CAST(warc_record_offset AS UBIGINT)
                END AS warc_record_offset,
                CASE
                    WHEN warc_record_length > {_UBIGINT_MAX}::HUGEINT
                        THEN error('eligible WARC record length exceeds UBIGINT')
                    ELSE CAST(warc_record_length AS UBIGINT)
                END AS warc_record_length,
                CAST(CASE WHEN url_host_name = root_domain
                               OR url_host_name = 'www.' || root_domain
                          THEN 0 ELSE 1 END AS UTINYINT) AS rank_main_site,
                CAST(CASE WHEN url_path IN ('/', '') THEN 0 ELSE 1 END AS UTINYINT)
                    AS rank_homepage,
                CAST(CASE WHEN regexp_matches(lower(url_path), '{_PRIORITY_PATH_PATTERN}')
                          THEN 0 ELSE 1 END AS UTINYINT) AS rank_priority_path,
                CAST(length(url_path) - length(replace(url_path, '/', '')) AS UBIGINT)
                    AS rank_path_depth,
                CAST(length(url_path) AS UBIGINT) AS rank_path_length,
                CAST(CASE WHEN url_host_name = root_domain THEN 0 ELSE 1 END AS UTINYINT)
                    AS rank_apex
            FROM normalized
            WHERE fetch_status = 200
              AND COALESCE(content_mime_detected, content_mime_type) IN ({mime_types})
              AND NULLIF(trim(root_domain), '') IS NOT NULL
              AND NULLIF(trim(url), '') IS NOT NULL
              AND NULLIF(trim(warc_filename), '') IS NOT NULL
              AND warc_record_offset >= 0
              AND warc_record_length > 0
        ),
        deduplicated AS (
            SELECT *
            FROM eligible
            QUALIFY row_number() OVER (
                PARTITION BY warc_filename, warc_record_offset, warc_record_length
                ORDER BY root_domain ASC NULLS LAST, {order},
                         source_index ASC NULLS LAST,
                         content_languages ASC NULLS LAST
            ) = 1
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY root_domain ORDER BY {order}
            ) AS local_rank
            FROM deduplicated
        )
        SELECT source_index, root_domain, url, content_languages, warc_filename,
               warc_record_offset, warc_record_length, {", ".join(_RANK_COLUMNS)}
        FROM ranked
        WHERE local_rank <= {pages_per_domain}
    """.strip()
