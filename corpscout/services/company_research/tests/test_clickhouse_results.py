"""Lossless section mapping and optional real ClickHouse migration checks."""

import copy
import json
import os
import unittest
from pathlib import Path
from uuid import uuid4

from clickhouse_driver.errors import ServerException

from company_research.clickhouse_results import connect, result_row


def analysis_result():
    return {
        "schema_version": "company-research-result/1.0",
        "run_id": "test-analysis",
        "revision_id": "revision-one",
        "target_url": "https://WWW.Example.com/jobs",
        "created_at": "2026-09-17T10:00:00+00:00",
        "processing_status": "partial",
        "records": {
            "jobs": [
                {
                    "record_id": "job-1",
                    "title": "Engineer",
                    "salary": None,
                    "evidence": {
                        "source": "https://example.com/jobs",
                        "key.with.dot": "Original wording",
                    },
                }
            ],
            "products_services": [],
        },
        "coverage": {"jobs": {"status": "processed"}},
        "pages": [],
        "future_section": {"preserved": True},
    }


class ResultMappingTests(unittest.TestCase):
    def test_records_are_lossless_and_input_unchanged(self):
        payload = analysis_result()
        before = copy.deepcopy(payload)
        row = result_row(payload, source_path="saved.json")
        self.assertEqual(payload, before)
        self.assertEqual(row["domain"], "example.com")
        self.assertEqual(json.loads(row["jobs"]), payload["records"]["jobs"])
        self.assertEqual(row["products_services"], "[]")
        self.assertIsNone(row["people"])
        self.assertTrue(json.loads(row["metadata"])["future_section"]["preserved"])

    def test_crawl_has_no_fabricated_extraction(self):
        payload = {
            "schema_version": "company-crawl-result/1.1",
            "crawl": {
                "run_id": "crawl-1",
                "input_url": "https://example.com",
                "site_url": "https://careers.external.test/",
                "status": "finished",
                "finished_at": "2026-09-17T10:00:00Z",
                "site_info": {"description": "Example company"},
                "pages": [],
            },
            "documents": [{"html": "<h1>Jobs</h1>"}],
        }
        row = result_row(
            payload, source_path="crawls/one/result.json.gz", request_id="one"
        )
        self.assertEqual(row["domain"], "example.com")
        self.assertEqual(row["request_id"], "one")
        self.assertEqual(row["result_kind"], "crawl")
        self.assertIsNone(row["jobs"])
        self.assertEqual(json.loads(row["documents"]), payload["documents"])
        self.assertEqual(json.loads(row["site_info"]), payload["crawl"]["site_info"])

    def test_result_id_is_stable_for_reimport_but_changes_for_revision(self):
        payload = analysis_result()
        row = result_row(payload, source_path="one.json")
        reordered = dict(reversed(list(payload.items())))
        self.assertEqual(
            row["result_id"], result_row(reordered, source_path="two.json")["result_id"]
        )
        payload["revision_id"] = "revision-two"
        self.assertNotEqual(
            row["result_id"], result_row(payload, source_path="one.json")["result_id"]
        )

    def test_domain_keeps_real_subdomains_and_idna(self):
        for url, domain in [
            ("https://jobs.example.com/", "jobs.example.com"),
            ("https://www.bücher.de./", "xn--bcher-kva.de"),
        ]:
            payload = analysis_result() | {"target_url": url}
            self.assertEqual(result_row(payload, source_path="test")["domain"], domain)

    def test_bad_sections_and_credential_urls_are_rejected(self):
        for changes in [
            {"records": {"jobs": {}}},
            {"coverage": []},
            {"target_url": "https://user:secret@example.com"},
            {"schema_version": {}},
            {"created_at": "2026-09-17T10:00:00"},
        ]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                result_row(analysis_result() | changes, source_path="test")

    def test_error_result_can_be_mapped_to_website(self):
        row = result_row(
            {
                "schema_version": "company-crawl-error/1.0",
                "request_id": "failed",
                "url": "https://example.com",
                "error": "Crawl execution failed",
                "finished_at": "2026-09-17T10:00:00Z",
            },
            source_path="error.json",
        )
        self.assertEqual(row["result_kind"], "error")
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["jobs"])


@unittest.skipUnless(
    os.environ.get("CRAWL_CLICKHOUSE_TEST_URL"),
    "Set CRAWL_CLICKHOUSE_TEST_URL for isolated real ClickHouse checks",
)
class ClickHouseResultTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = connect(
            {"CLICKHOUSE_NATIVE_URL": os.environ["CRAWL_CLICKHOUSE_TEST_URL"]}
        )
        cls.database = "crawl_test_" + uuid4().hex
        cls.addClassCleanup(cls.client.disconnect)
        cls.addClassCleanup(
            cls.client.execute, f"DROP DATABASE IF EXISTS {cls.database}"
        )
        migration = (
            Path(__file__).resolve().parents[3]
            / "clickhouse/migrations/000420_corpscout_website_crawl_results.up.sql"
        )
        for statement in migration.read_text().split(";"):
            if statement.strip():
                cls.client.execute(statement.replace("corpscout", cls.database))
        followup = migration.with_name(
            "000421_corpscout_website_crawl_s3_domain_identity.up.sql"
        )
        for statement in followup.read_text().split(";"):
            if statement.strip():
                cls.client.execute(statement.replace("corpscout", cls.database))
        observations = migration.with_name(
            "000422_corpscout_website_crawl_page_observations.up.sql"
        )
        for statement in observations.read_text().split(";"):
            if statement.strip():
                cls.client.execute(statement.replace("corpscout", cls.database))
        attempts = migration.with_name(
            "000426_corpscout_website_crawl_s3_attempt_identity.up.sql"
        )
        for statement in attempts.read_text().split(";"):
            if statement.strip():
                cls.client.execute(statement.replace("corpscout", cls.database))

    def test_s3_request_identity_supports_attempt_paths_and_legacy_objects(self):
        migration = (
            Path(__file__).resolve().parents[3]
            / "clickhouse/migrations/000426_corpscout_website_crawl_s3_attempt_identity.up.sql"
        )
        select = migration.read_text().split("SQL SECURITY INVOKER AS\n", 1)[1]
        select = (
            select.rsplit("FROM s3(", 1)[0]
            + "FROM (SELECT %(payload)s AS result_json, %(path)s AS _path, 'result.json.gz' AS _file, 1 AS _size, now() AS _time)"
        )
        for path, explicit_id, request_id, attempt in [
            ("crawls/company-crawls/legacy/result.json.gz", None, "legacy", None),
            ("crawls/company-crawls/new-scan/attempts/0002/result.json.gz", None, "new-scan", 2),
            ("crawls/company-crawls/new-scan/attempts/0002/result.json.gz", "explicit", "explicit", 2),
        ]:
            with self.subTest(path=path, explicit_id=explicit_id):
                payload = analysis_result()
                if explicit_id is not None:
                    payload["request_id"] = explicit_id
                rows, columns = self.client.execute(
                    select,
                    {"payload": json.dumps(payload), "path": path},
                    with_column_types=True,
                )
                actual = dict(zip([name for name, _ in columns], rows[0], strict=True))
                self.assertEqual(actual["request_id"], request_id)
                self.assertEqual(actual["attempt"], attempt)

    def insert(self, row):
        self.client.execute(
            f"INSERT INTO {self.database}.website_crawl_results ({', '.join(row)}) VALUES",
            [list(row.values())],
        )

    def test_roundtrip_deduplication_and_latest_use_source_time(self):
        payload = analysis_result() | {"target_url": "https://history.example.test"}
        row = result_row(payload, source_path="original")
        self.insert(row)
        self.insert(row)
        self.assertEqual(
            self.client.execute(
                f"SELECT count() FROM {self.database}.website_crawl_results FINAL WHERE domain='history.example.test'"
            ),
            [(1,)],
        )
        older = payload | {"revision_id": "older", "created_at": "2026-09-01T00:00:00Z"}
        self.insert(result_row(older, source_path="late-import"))
        actual = self.client.execute(
            f"SELECT revision_id, jobs, people, products_services FROM {self.database}.website_crawl_results_latest WHERE domain='history.example.test'"
        )
        self.assertEqual(actual[0][0], "revision-one")
        self.assertEqual(json.loads(actual[0][1]), payload["records"]["jobs"])
        self.assertIsNone(actual[0][2])
        self.assertEqual(actual[0][3], "[]")

    def test_constraints_reject_non_array_jobs(self):
        row = result_row(analysis_result(), source_path="invalid")
        row["jobs"] = "{}"
        with self.assertRaises(ServerException) as rejected:
            self.insert(row)
        self.assertEqual(rejected.exception.code, 469)

    def test_observations_survive_import_and_latest_view_without_becoming_facts(self):
        observations = {
            "source_url": "https://observations.example.test",
            "contacts": [],
            "structured_data": {
                "jsonld_entities": [
                    {"types": ["JobPosting"], "data": {"title": "Engineer"}}
                ]
            },
        }
        payload = {
            "schema_version": "company-crawl-result/1.2",
            "crawl": {
                "input_url": "https://observations.example.test",
                "status": "finished",
            },
            "documents": [{"input": {"observations": observations}}],
        }
        row = result_row(payload, source_path="observation-test")
        self.insert(row)
        actual = self.client.execute(
            f"SELECT page_observations, jobs FROM {self.database}.website_crawl_results_latest WHERE domain='observations.example.test'"
        )
        self.assertEqual(json.loads(actual[0][0]), [observations])
        self.assertIsNone(actual[0][1])
        row["page_observations"] = "{}"
        with self.assertRaises(ServerException) as rejected:
            self.insert(row)
        self.assertEqual(rejected.exception.code, 469)

    def test_s3_observation_projection_matches_importer_for_old_and_new_results(self):
        migration = (
            Path(__file__).resolve().parents[3]
            / "clickhouse/migrations/000422_corpscout_website_crawl_page_observations.up.sql"
        )
        select = migration.read_text().split(
            "website_crawl_results_s3_archive SQL SECURITY INVOKER AS\n", 1
        )[1]
        select = (
            select.rsplit("FROM s3(", 1)[0]
            + "FROM (SELECT %(payload)s AS result_json, 'crawls/id/result.json.gz' AS _path, 'result.json.gz' AS _file, 1 AS _size, now() AS _time)"
        )
        observations = {
            "contacts": [{"value": "one@example.test"}],
            "structured_data": {"unknown": None},
        }
        for payload in [
            analysis_result(),
            analysis_result() | {"page_observations": [observations]},
            {
                "schema_version": "company-crawl-result/1.1",
                "crawl": {"input_url": "https://example.test"},
                "documents": [],
            },
            {
                "schema_version": "company-crawl-result/1.2",
                "crawl": {"input_url": "https://example.test"},
                "documents": [],
            },
            {
                "schema_version": "company-crawl-result/1.2",
                "crawl": {"input_url": "https://example.test"},
                "documents": [{"input": {"observations": observations}}],
            },
        ]:
            result = self.client.execute(
                select, {"payload": json.dumps(payload)}, with_column_types=True
            )
            actual = dict(
                zip([name for name, _ in result[1]], result[0][0], strict=True)
            )
            expected = result_row(payload, source_path="test")["page_observations"]
            self.assertEqual(
                json.loads(actual["page_observations"] or "null"),
                json.loads(expected or "null"),
            )

    def test_raw_crawl_does_not_replace_latest_analysis(self):
        url = "https://stages.example.test"
        self.insert(
            result_row(analysis_result() | {"target_url": url}, source_path="analysis")
        )
        self.insert(
            result_row(
                {
                    "schema_version": "company-crawl-result/1.1",
                    "crawl": {
                        "input_url": url,
                        "status": "finished",
                        "finished_at": "2026-09-18T00:00:00Z",
                    },
                    "documents": [],
                },
                source_path="crawl",
            )
        )
        self.assertEqual(
            self.client.execute(
                f"SELECT result_kind, jobs IS NULL FROM {self.database}.website_crawl_results_latest WHERE domain='stages.example.test' ORDER BY result_kind"
            ),
            [("analysis", 0), ("crawl", 1)],
        )

    def test_s3_mapping_reads_uploaded_bundles(self):
        rows = self.client.execute(
            f"SELECT _path, domain, length(result_json) FROM {self.database}.website_crawl_results_s3_archive WHERE domain='novelic.com' LIMIT 1"
        )
        self.assertTrue(rows)
        path, domain, size = rows[0]
        self.assertEqual(domain, "novelic.com")
        self.assertGreater(size, 100)
        matched = self.client.execute(
            f"SELECT _path FROM {self.database}.website_crawl_results_s3_archive WHERE _path=%(path)s",
            {"path": path},
        )
        self.assertEqual(matched, [(path,)])

    def test_s3_and_imported_domain_identity_match(self):
        migration = (
            Path(__file__).resolve().parents[3]
            / "clickhouse/migrations/000421_corpscout_website_crawl_s3_domain_identity.up.sql"
        )
        select = migration.read_text().split("SQL SECURITY INVOKER AS\n", 1)[1]
        select = (
            select.rsplit("FROM s3(", 1)[0]
            + "FROM (SELECT %(payload)s AS result_json, 'crawls/id/result.json.gz' AS _path, 'result.json.gz' AS _file, 1 AS _size, now() AS _time)"
        )
        for url in [
            "https://www.bücher.de./",
            "novelic.com",
            "https://jobs.example.com:443/",
            "http://[::1]:8080/",
        ]:
            with self.subTest(url=url):
                payload = analysis_result() | {
                    "target_url": url,
                    "records": {"jobs": None},
                }
                expected = result_row(payload, source_path="test")
                result = self.client.execute(
                    select, {"payload": json.dumps(payload)}, with_column_types=True
                )
                actual = dict(
                    zip([name for name, _ in result[1]], result[0][0], strict=True)
                )
                self.assertEqual(actual["domain"], expected["domain"])
                self.assertIsNone(actual["jobs"])
