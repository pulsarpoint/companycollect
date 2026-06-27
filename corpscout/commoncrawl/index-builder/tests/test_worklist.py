import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from index_builder import worklist

COLS = ["url_host_registered_domain", "url_host_name", "url", "url_path", "fetch_status",
        "content_mime_detected", "warc_filename", "warc_record_offset",
        "warc_record_length", "content_languages"]


def _write_index(path, rows):
    # rows: (registered_domain, host_name, url, url_path, status, mime, warc_filename, offset, length)
    data = {c: [] for c in COLS}
    for r in rows:
        for c, v in zip(COLS, list(r) + ["eng"]):
            data[c].append(v)
    pq.write_table(pa.table(data), path)


def test_worklist_picks_shallowest_200_html_per_domain(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("acme.com", "acme.com", "http://acme.com/en/about", "/en/about", 200, "text/html", "w1.gz", 10, 5),
        ("acme.com", "acme.com", "http://acme.com/en/", "/en/", 200, "text/html", "w2.gz", 20, 6),   # shallowest
        ("acme.com", "acme.com", "http://acme.com/en/", "/en/", 301, "text/html", "w3.gz", 0, 1),     # not 200
        ("shop.org", "shop.org", "http://shop.org/p/1.html", "/p/1.html", 200, "text/html", "w4.gz", 30, 7),
        ("img.org", "img.org", "http://img.org/logo.png", "/logo.png", 200, "image/png", "w5.gz", 0, 1),  # not html
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')").fetchall()
    by_dom = {r[0]: r for r in rows}
    # columns: root_domain(0), url(1), warc_filename(2), offset(3), length(4), languages(5)
    assert set(by_dom) == {"acme.com", "shop.org"}            # img.org dropped (no html), no dupes
    assert by_dom["acme.com"][1] == "http://acme.com/en/"     # shallowest 200-html
    assert by_dom["acme.com"][2] == "w2.gz" and by_dom["acme.com"][3] == 20


def test_worklist_prefers_main_site_over_functional_subdomain(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        # a functional subdomain with a shallow homepage...
        ("ex.com", "shop.ex.com", "http://shop.ex.com/", "/", 200, "text/html", "shop.gz", 0, 5),
        # ...must lose to the apex homepage (same registered domain), and apex beats www on the tie
        ("ex.com", "www.ex.com", "http://www.ex.com/", "/", 200, "text/html", "www.gz", 20, 5),
        ("ex.com", "ex.com", "http://ex.com/", "/", 200, "text/html", "apex.gz", 10, 5),
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "http://ex.com/" and rows[0][2] == "apex.gz"


def test_tech_mode_multi_page_homepage_and_legal_first_then_caps(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("ex.com", "ex.com", "http://ex.com/a/b/c", "/a/b/c", 200, "text/html", "w", 4, 5),  # deep -> dropped at cap 3
        ("ex.com", "ex.com", "http://ex.com/blog", "/blog", 200, "text/html", "w", 3, 5),
        ("ex.com", "ex.com", "http://ex.com/imprint", "/imprint", 200, "text/html", "w", 2, 5),  # legal -> kept
        ("ex.com", "ex.com", "http://ex.com/", "/", 200, "text/html", "w", 0, 5),                # homepage -> first
        ("shop.ex.com.dom", "shop.ex.com.dom", "http://x/", "/", 404, "text/html", "w", 9, 5),   # not 200 -> dropped
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')", mode="tech", max_pages=3).fetchall()
    urls = [r[1] for r in rows]
    assert len(urls) == 3                          # capped to 3 pages for the one domain
    assert urls[0] == "http://ex.com/"             # homepage ranked first
    assert "http://ex.com/imprint" in urls         # legal page kept within the cap (identifiers live here)
    assert "http://ex.com/a/b/c" not in urls       # deepest page dropped by the cap


def test_tech_mode_promotes_multilingual_legal_pages(tmp_path):
    # non-English legal/about pages must be kept within the cap (a fixed English regex would miss them)
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("de.com", "de.com", "http://de.com/", "/", 200, "text/html", "w", 0, 5),
        ("de.com", "de.com", "http://de.com/produkte/x/y", "/produkte/x/y", 200, "text/html", "w", 1, 5),
        ("de.com", "de.com", "http://de.com/impressum", "/impressum", 200, "text/html", "w", 2, 5),  # DE imprint
        ("es.com", "es.com", "http://es.com/", "/", 200, "text/html", "w", 3, 5),
        ("es.com", "es.com", "http://es.com/tienda/p/1", "/tienda/p/1", 200, "text/html", "w", 4, 5),
        ("es.com", "es.com", "http://es.com/quienes-somos", "/quienes-somos", 200, "text/html", "w", 5, 5),  # ES about
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')", mode="tech", max_pages=2).fetchall()
    by_dom = {}
    for r in rows:
        by_dom.setdefault(r[0], []).append(r[1])
    assert set(by_dom["de.com"]) == {"http://de.com/", "http://de.com/impressum"}        # imprint kept
    assert "http://de.com/produkte/x/y" not in by_dom["de.com"]                          # deep page dropped
    assert set(by_dom["es.com"]) == {"http://es.com/", "http://es.com/quienes-somos"}    # about kept


def test_tech_mode_uncapped_returns_all_html_pages(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("ex.com", "ex.com", "http://ex.com/", "/", 200, "text/html", "w", 0, 5),
        ("ex.com", "ex.com", "http://ex.com/a", "/a", 200, "text/html", "w", 1, 5),
        ("ex.com", "ex.com", "http://ex.com/b", "/b", 200, "text/html", "w", 2, 5),
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')", mode="tech", max_pages=0).fetchall()
    assert len(rows) == 3                          # every 200/HTML page


def test_industry_mode_still_one_page_per_domain(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("ex.com", "ex.com", "http://ex.com/", "/", 200, "text/html", "w", 0, 5),
        ("ex.com", "ex.com", "http://ex.com/about", "/about", 200, "text/html", "w", 1, 5),
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')").fetchall()  # default mode=industry
    assert len(rows) == 1 and rows[0][1] == "http://ex.com/"


def test_build_worklist_writes_parquet_all_domains(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("a.sk", "a.sk", "http://a.sk/", "/", 200, "text/html", "w.gz", 0, 5),
        ("b.cz", "b.cz", "http://b.cz/", "/", 200, "text/html", "w.gz", 5, 5),
    ])
    con = duckdb.connect()
    out = tmp_path / "wl.parquet"
    # no filtering capability — every 200/HTML domain is included (global dataset)
    n = worklist.build_worklist(con, f"read_parquet('{idx}')", out)
    assert n == 2
    roots = {r["root_domain"] for r in pq.read_table(out).to_pylist()}
    assert roots == {"a.sk", "b.cz"}
