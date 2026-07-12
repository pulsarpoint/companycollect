"""Build one downloader worklist directly from the Common Crawl columnar index."""

import argparse
import gzip
import urllib.request
from pathlib import Path

import duckdb


BASE_URL = "https://data.commoncrawl.org/"
HTML_MIME = ("text/html", "application/xhtml+xml")
LEGAL_PATH = "|".join(
    (
        "imprint", "impressum", "legal", "mentions", "aviso-legal", "note-legali",
        "datenschutz", "colofon", "about", "uber-uns", "ueber-uns", "a-propos",
        "apropos", "quienes-somos", "sobre-nos", "sobre-nosotros", "acerca",
        "chi-siamo", "over-ons", "o-nas", "om-oss", "om-os", "hakkimizda",
        "company", "unternehmen", "entreprise", "empresa", "azienda", "contact",
        "kontakt", "contatti", "contato", "iletisim", "privacy", "privacidad",
        "confidentialite", "terms", "termini", "termos", "terminos", "agb", "cgv",
        "cgu", "voorwaarden",
    )
)
MAIN_SITE = """
    CASE WHEN url_host_name = url_host_registered_domain THEN 0
         WHEN url_host_name = 'www.' || url_host_registered_domain THEN 0
         ELSE 1 END
"""
DEPTH = "length(url_path) - length(replace(url_path, '/', ''))"
APEX_TIE = "CASE WHEN url_host_name = url_host_registered_domain THEN 0 ELSE 1 END"


def warc_part_url(crawl: str, part: int) -> str:
    manifest_url = f"{BASE_URL}crawl-data/{crawl}/cc-index-table.paths.gz"
    with urllib.request.urlopen(manifest_url, timeout=60) as response:
        paths = gzip.decompress(response.read()).decode().splitlines()
    parts = sorted(path for path in paths if "subset=warc" in path and path.endswith(".parquet"))
    if not parts:
        raise RuntimeError(f"no WARC index parts found for {crawl}")
    if part >= len(parts):
        raise ValueError(f"part {part} does not exist; {crawl} has {len(parts)} parts")
    return BASE_URL + parts[part]


def has_column(connection: duckdb.DuckDBPyConnection, source: str, column: str) -> bool:
    columns = connection.execute(f"DESCRIBE SELECT * FROM {source} LIMIT 0").fetchall()
    return any(candidate[0] == column for candidate in columns)


def order_by(pages_per_domain: int) -> str:
    if pages_per_domain == 1:
        return f"""
            {MAIN_SITE} ASC,
            {DEPTH} ASC, length(url_path) ASC, {APEX_TIE} ASC
        """
    return f"""
        {MAIN_SITE} ASC,
        CASE WHEN url_path IN ('/', '') THEN 0 ELSE 1 END ASC,
        CASE WHEN regexp_matches(lower(url_path), '{LEGAL_PATH}') THEN 0 ELSE 1 END ASC,
        {DEPTH} ASC, length(url_path) ASC, {APEX_TIE} ASC
    """


def worklist_query(source: str, pages_per_domain: int, languages_available: bool) -> str:
    mime = ", ".join(f"'{value}'" for value in HTML_MIME)
    languages = (
        "content_languages"
        if languages_available
        else "CAST(NULL AS VARCHAR) AS content_languages"
    )
    return f"""
        SELECT root_domain, url, warc_filename, warc_record_offset,
               warc_record_length, content_languages
        FROM (
          SELECT url_host_registered_domain AS root_domain, url, warc_filename,
                 warc_record_offset, warc_record_length, {languages},
                 row_number() OVER (
                   PARTITION BY url_host_registered_domain
                   ORDER BY {order_by(pages_per_domain)}
                 ) AS rn
          FROM {source}
          WHERE fetch_status = 200
            AND COALESCE(content_mime_detected, content_mime_type) IN ({mime})
            AND NULLIF(trim(url_host_registered_domain), '') IS NOT NULL
            AND NULLIF(trim(url), '') IS NOT NULL
            AND NULLIF(trim(warc_filename), '') IS NOT NULL
            AND warc_record_offset >= 0
            AND warc_record_length > 0
        ) WHERE rn <= {pages_per_domain}
        ORDER BY root_domain, rn
    """


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crawl", required=True)
    parser.add_argument("--pages", type=int, required=True)
    parser.add_argument("--part", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.pages < 1:
        parser.error("--pages must be at least 1")
    if args.part < 0:
        parser.error("--part must be non-negative")

    url = warc_part_url(args.crawl, args.part)
    source = f"read_parquet([{sql_string(url)}], hive_partitioning=true)"
    connection = duckdb.connect()
    connection.execute("INSTALL httpfs; LOAD httpfs")
    query = worklist_query(source, args.pages, has_column(connection, source, "content_languages"))
    connection.execute(
        f"COPY ({query}) TO {sql_string(str(args.out))} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    rows = connection.execute(
        f"SELECT count(*) FROM read_parquet({sql_string(str(args.out))})"
    ).fetchone()[0]
    print(f"built {args.out}: {rows} rows")


if __name__ == "__main__":
    main()
