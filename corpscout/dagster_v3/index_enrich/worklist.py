"""Build the per-domain worklist from the CommonCrawl columnar URL index.

`source` is a DuckDB table expression over the index parquet:
- test/off-AWS small: read_parquet('idx.parquet')
- off-AWS full: read_parquet(['https://data.commoncrawl.org/<part>', ...], hive_partitioning=true)
- on-AWS: read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=.../subset=warc/*.parquet')

One row per domain: the shallowest fetch_status=200 HTML page + its WARC location.
"""

ATHENA_SQL = """
-- AWS path: run against the `ccindex` Athena table, UNLOAD result to your S3 bucket.
SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
FROM (
  SELECT url_host_registered_domain AS root_domain, url, warc_filename,
         warc_record_offset, warc_record_length, content_languages,
         ROW_NUMBER() OVER (
           PARTITION BY url_host_registered_domain
           ORDER BY length(url_path) - length(replace(url_path,'/','')) ASC,
                    length(url_path) ASC
         ) rn
  FROM ccindex
  WHERE crawl = ? AND subset = 'warc'
    AND fetch_status = 200
    AND content_mime_detected IN ('text/html','application/xhtml+xml')
) WHERE rn = 1
"""

_HTML_MIME = ("text/html", "application/xhtml+xml")


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
                   ORDER BY length(url_path) - length(replace(url_path,'/','')) ASC,
                            length(url_path) ASC
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
