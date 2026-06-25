"""Build the per-domain worklist from the CommonCrawl columnar URL index.

`source` is a DuckDB table expression over the index parquet:
- test/off-AWS small: read_parquet('idx.parquet')
- off-AWS full: read_parquet(['https://data.commoncrawl.org/<part>', ...], hive_partitioning=true)
- on-AWS: read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=.../subset=warc/*.parquet')

One row per domain: the most *representative* fetch_status=200 HTML page — preferring the
apex host, then www, then any other subdomain, and within that the shallowest URL path.
"""

_HTML_MIME = ("text/html", "application/xhtml+xml")

# Pick the registered domain's main-site homepage, not a functional subdomain (shop./blog./
# api.…). Rank apex and www as the "main site" (0) above any other subdomain (1); within that,
# the shallowest/shortest path wins (the homepage over a deep page); ties break apex over www.
_ORDER_BY = """
    CASE
        WHEN url_host_name = url_host_registered_domain THEN 0
        WHEN url_host_name = 'www.' || url_host_registered_domain THEN 0
        ELSE 1
    END ASC,
    length(url_path) - length(replace(url_path, '/', '')) ASC,
    length(url_path) ASC,
    CASE WHEN url_host_name = url_host_registered_domain THEN 0 ELSE 1 END ASC
"""


def worklist_query(source: str, *, where: str = "") -> str:
    extra = f" AND ({where})" if where else ""
    mime = ", ".join(f"'{m}'" for m in _HTML_MIME)
    return f"""
        SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
        FROM (
          SELECT url_host_registered_domain AS root_domain, url, warc_filename,
                 warc_record_offset, warc_record_length, content_languages,
                 row_number() OVER (
                   PARTITION BY url_host_registered_domain
                   ORDER BY {_ORDER_BY}
                 ) AS rn
          FROM {source}
          WHERE fetch_status = 200
            AND content_mime_detected IN ({mime}){extra}
        ) WHERE rn = 1
    """


def run_worklist(con, source: str, *, crawl: str = "", where: str = ""):
    """Execute the worklist query; returns a DuckDB result (use .fetchall() or .arrow())."""
    return con.execute(worklist_query(source, where=where))


def build_worklist(con, source: str, out_path, *, where: str = "") -> int:
    """Write the worklist to a Parquet file; returns row count."""
    import pyarrow.parquet as pq

    table = run_worklist(con, source, where=where).to_arrow_table()
    pq.write_table(table, out_path)
    return table.num_rows
