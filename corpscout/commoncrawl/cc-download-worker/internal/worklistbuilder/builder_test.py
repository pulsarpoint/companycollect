import unittest

import duckdb

import builder


class WorklistQueryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = duckdb.connect()
        self.connection.execute(
            """
            CREATE TABLE urls (
              url_host_registered_domain VARCHAR,
              url_host_name VARCHAR,
              url VARCHAR,
              url_path VARCHAR,
              fetch_status INTEGER,
              content_mime_type VARCHAR,
              content_mime_detected VARCHAR,
              warc_filename VARCHAR,
              warc_record_offset BIGINT,
              warc_record_length BIGINT,
              content_languages VARCHAR
            )
            """
        )
        self.connection.executemany(
            "INSERT INTO urls VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("ex.com", "ex.com", "https://ex.com/deep/page", "/deep/page", 200, "text/html", "text/html", "w", 30, 10, "eng"),
                ("ex.com", "ex.com", "https://ex.com/contact", "/contact", 200, "text/html", "text/html", "w", 20, 10, "eng"),
                ("ex.com", "ex.com", "https://ex.com/", "/", 200, "text/html", "text/html", "w", 10, 10, "eng"),
                ("ex.com", "shop.ex.com", "https://shop.ex.com/", "/", 200, "text/html", "text/html", "w", 40, 10, "eng"),
                ("ignored.com", "ignored.com", "https://ignored.com/logo.png", "/logo.png", 200, "image/png", "image/png", "w", 50, 10, "eng"),
            ],
        )

    def tearDown(self) -> None:
        self.connection.close()

    def test_one_page_selects_main_homepage(self) -> None:
        rows = self.connection.execute(builder.worklist_query("urls", 1, True)).fetchall()
        self.assertEqual([row[1] for row in rows], ["https://ex.com/"])

    def test_multiple_pages_prioritize_homepage_and_company_pages(self) -> None:
        rows = self.connection.execute(builder.worklist_query("urls", 2, True)).fetchall()
        self.assertEqual([row[1] for row in rows], ["https://ex.com/", "https://ex.com/contact"])


if __name__ == "__main__":
    unittest.main()
