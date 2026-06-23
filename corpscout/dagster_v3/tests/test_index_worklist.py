import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from index_enrich import worklist


def _write_index(path, rows):
    # rows: (domain, url, url_path, status, mime, warc_filename, offset, length)
    cols = ["url_host_registered_domain", "url", "url_path", "fetch_status",
            "content_mime_detected", "warc_filename", "warc_record_offset",
            "warc_record_length", "content_languages"]
    data = {c: [] for c in cols}
    for d, u, p, s, m, wf, off, ln in rows:
        for c, v in zip(cols, [d, u, p, s, m, wf, off, ln, "eng"]):
            data[c].append(v)
    pq.write_table(pa.table(data), path)


def test_worklist_picks_shallowest_200_html_per_domain(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("acme.com", "http://acme.com/en/about", "/en/about", 200, "text/html", "w1.gz", 10, 5),
        ("acme.com", "http://acme.com/en/", "/en/", 200, "text/html", "w2.gz", 20, 6),    # shallowest
        ("acme.com", "http://acme.com/en/", "/en/", 301, "text/html", "w3.gz", 0, 1),      # not 200
        ("shop.org", "http://shop.org/p/1.html", "/p/1.html", 200, "text/html", "w4.gz", 30, 7),
        ("img.org", "http://img.org/logo.png", "/logo.png", 200, "image/png", "w5.gz", 0, 1),  # not html
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')", crawl="CC-MAIN-2026-25").fetchall()
    by_dom = {r[0]: r for r in rows}
    # columns: root_domain(0), url(1), warc_filename(2), offset(3), length(4), languages(5)
    assert set(by_dom) == {"acme.com", "shop.org"}            # img.org dropped (no html), no dupes
    assert by_dom["acme.com"][1] == "http://acme.com/en/"     # shallowest 200-html
    assert by_dom["acme.com"][2] == "w2.gz" and by_dom["acme.com"][3] == 20


def test_build_worklist_writes_parquet_and_where_filter(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("a.sk", "http://a.sk/", "/", 200, "text/html", "w.gz", 0, 5),
        ("b.cz", "http://b.cz/", "/", 200, "text/html", "w.gz", 5, 5),
    ])
    con = duckdb.connect()
    out = tmp_path / "wl.parquet"
    n = worklist.build_worklist(con, f"read_parquet('{idx}')", out,
                                where="url_host_registered_domain LIKE '%.sk'")
    assert n == 1
    assert pq.read_table(out).to_pylist()[0]["root_domain"] == "a.sk"
