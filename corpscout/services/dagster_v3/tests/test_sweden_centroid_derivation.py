import duckdb

from dagster_v3.defs.sweden_company.centroid_derivation import replace_city_centroids


def _seed(con):
    con.execute("create table pts(postcode varchar, post_town varchar, latitude double, longitude double)")
    # Trelleborg: 3 tight points + 1 gross outlier -> median must ignore the outlier
    con.execute("""insert into pts values
      ('23100','Trelleborg',55.375,13.150),('23139','Trelleborg',55.377,13.152),
      ('23132','Trelleborg',55.373,13.148),('23100','Trelleborg',59.000,18.000),
      ('11456','Stockholm',59.339,18.05),('11457','Stockholm',59.341,18.06)""")  # 2 pts -> below N>=3


def test_city_centroid_is_robust_and_gated():
    con = duckdb.connect()
    _seed(con)
    n = replace_city_centroids(con, source_points_table="pts", out_table="cc", min_points=3)
    rows = {r[0]: r for r in con.execute("select key, latitude, longitude, point_count from cc").fetchall()}
    assert "STOCKHOLM" not in rows            # 2 points < 3 -> excluded
    assert "TRELLEBORG" in rows
    lat, lon = rows["TRELLEBORG"][1], rows["TRELLEBORG"][2]
    assert 55.37 < lat < 55.38 and 13.14 < lon < 13.16   # median, outlier ignored
    assert rows["TRELLEBORG"][3] == 4
    assert n == 1
