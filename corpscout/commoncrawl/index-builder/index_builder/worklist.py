"""Build the per-domain worklist from the CommonCrawl columnar URL index.

`source` is a DuckDB table expression over the index parquet:
- test/off-AWS small: read_parquet('idx.parquet')
- off-AWS full: read_parquet(['https://data.commoncrawl.org/<part>', ...], hive_partitioning=true)
- on-AWS: read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=.../subset=warc/*.parquet')

Two shapes, because the two passes want different inputs:

- **industry** (default): ONE most-representative page per domain — the main-site homepage
  (apex/www, shallowest path). That single page is embedded → NACE classified.
- **tech**: MANY pages per domain (Wappalyzer + contacts + LEI/VAT + JSON-LD profiles improve
  with coverage; LEI/VAT live on /imprint, /legal, /contact — never the homepage). Ranked
  homepage-first, then legal/contact/about pages, then shallowest, and capped at `max_pages`
  per domain (0 = uncapped = every 200/HTML page, i.e. the whole crawl).
"""

_HTML_MIME = ("text/html", "application/xhtml+xml")

# Legal/imprint, about/company, and contact pages — where the company identifiers (LEI/VAT; the
# imprint is legally mandatory in DE/AT/CH) and the schema.org Organization JSON-LD live. Kept
# multilingual on purpose: a fixed English list misses most non-English sites, and the i18n page
# names are exactly where the value is (matched as substrings of the lower-cased url_path).
_LEGAL_PATH = "|".join((
    # imprint / legal notice
    "imprint", "impressum", "legal", "mentions", "aviso-legal", "note-legali", "datenschutz", "colofon",
    # about / company
    "about", "uber-uns", "ueber-uns", "a-propos", "apropos", "quienes-somos", "sobre-nos",
    "sobre-nosotros", "acerca", "chi-siamo", "over-ons", "o-nas", "om-oss", "om-os", "hakkimizda",
    "company", "unternehmen", "entreprise", "empresa", "azienda",
    # contact
    "contact", "kontakt", "contatti", "contato", "iletisim",
    # privacy / terms
    "privacy", "privacidad", "confidentialite", "terms", "termini", "termos", "terminos",
    "agb", "cgv", "cgu", "voorwaarden",
))

# apex and www are the "main site" (0), any other host a functional subdomain (1).
_MAIN_SITE = """
    CASE WHEN url_host_name = url_host_registered_domain THEN 0
         WHEN url_host_name = 'www.' || url_host_registered_domain THEN 0
         ELSE 1 END
"""
_DEPTH = "length(url_path) - length(replace(url_path, '/', ''))"
_APEX_TIE = "CASE WHEN url_host_name = url_host_registered_domain THEN 0 ELSE 1 END"


def _order_by(mode: str) -> str:
    if mode == "tech":
        # homepage first, then legal/contact pages, then shallowest — so a per-domain cap keeps
        # the identifier-bearing pages, not an arbitrary slice.
        return f"""
            {_MAIN_SITE} ASC,
            CASE WHEN url_path IN ('/', '') THEN 0 ELSE 1 END ASC,
            CASE WHEN regexp_matches(lower(url_path), '{_LEGAL_PATH}') THEN 0 ELSE 1 END ASC,
            {_DEPTH} ASC, length(url_path) ASC, {_APEX_TIE} ASC
        """
    # industry: the single most representative page (main-site homepage).
    return f"""
        {_MAIN_SITE} ASC,
        {_DEPTH} ASC, length(url_path) ASC, {_APEX_TIE} ASC
    """


def worklist_query(source: str, *, mode: str = "industry", max_pages: int = 25) -> str:
    mime = ", ".join(f"'{m}'" for m in _HTML_MIME)
    if mode == "industry":
        rn = "rn = 1"
    elif max_pages and max_pages > 0:
        rn = f"rn <= {int(max_pages)}"
    else:
        rn = "true"  # uncapped: every 200/HTML page
    return f"""
        SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
        FROM (
          SELECT url_host_registered_domain AS root_domain, url, warc_filename,
                 warc_record_offset, warc_record_length, content_languages,
                 row_number() OVER (
                   PARTITION BY url_host_registered_domain
                   ORDER BY {_order_by(mode)}
                 ) AS rn
          FROM {source}
          WHERE fetch_status = 200
            AND content_mime_detected IN ({mime})
        ) WHERE {rn}
        ORDER BY root_domain, rn
    """


def run_worklist(con, source: str, *, crawl: str = "", mode: str = "industry",
                 max_pages: int = 25):
    """Execute the worklist query; returns a DuckDB result (use .fetchall() or .arrow())."""
    return con.execute(worklist_query(source, mode=mode, max_pages=max_pages))


def build_worklist(con, source: str, out_path, *, mode: str = "industry",
                   max_pages: int = 25) -> int:
    """Write the worklist to a Parquet file; returns row count."""
    import pyarrow.parquet as pq

    table = run_worklist(con, source, mode=mode, max_pages=max_pages).to_arrow_table()
    pq.write_table(table, out_path)
    return table.num_rows
