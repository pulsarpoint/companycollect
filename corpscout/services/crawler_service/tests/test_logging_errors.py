import errno
import sqlite3
import unittest

import httpx
import psycopg2
from clickhouse_driver.errors import Error, NetworkError, ServerException

from crawler_service.logging_errors import error_details, safe_endpoint


class PublicationErrorDetailsTests(unittest.TestCase):
    def test_native_chain_preserves_table_and_code_without_sql_or_credentials(self):
        server = ServerException(
            "DB::Exception: Table missing. Query: INSERT secret VALUES ('customer-data')\n"
            "clickhouse://username:password@clickhouse:9000/?token=secret",
            code=60,
        )
        failure = RuntimeError("ClickHouse insert failed for corpscout.website_site_info_results")
        failure.__cause__ = server
        message = error_details(
            failure,
            endpoint="clickhouse://username:password@clickhouse:9000/private-token?password=secret#fragment",
            operation="publish_results",
        )
        self.assertIn("operation='publish_results'", message)
        self.assertIn("endpoint='clickhouse://clickhouse:9000'", message)
        self.assertIn("ClickHouse insert failed for corpscout.website_site_info_results", message)
        self.assertIn("ServerException code=60 reason=UNKNOWN_TABLE", message)
        for value in ("username", "password", "private-token", "secret", "customer-data", "fragment", "\n"):
            self.assertNotIn(value, message)

    def test_native_network_chain_keeps_errno_without_raw_connection_error(self):
        failure = NetworkError("Connection failed at credential-bearing-address")
        failure.__cause__ = ConnectionRefusedError(errno.ECONNREFUSED, "private-host-details")
        message = error_details(failure, endpoint="clickhouse://registry:9000", operation="register_parents")
        self.assertIn("NetworkError code=210 reason=NETWORK_ERROR", message)
        self.assertIn("reason=ECONNREFUSED", message)
        self.assertNotIn("private-host-details", message)
        self.assertNotIn("credential-bearing-address", message)

    def test_nested_native_error_and_unknown_code(self):
        failure = ServerException("private-data", code=9999, nested=ServerException("payload", code=60))
        message = error_details(failure, endpoint="clickhouse://registry", operation="publish_results")
        self.assertIn("code=9999 reason=UNKNOWN_CLICKHOUSE_ERROR", message)
        self.assertIn("code=60 reason=UNKNOWN_TABLE", message)
        self.assertNotIn("private-data", message)
        self.assertNotIn("payload", message)
        self.assertIn("UNKNOWN_CLICKHOUSE_ERROR", error_details(Error(), endpoint="", operation="connect"))

    def test_http_uses_actual_request_origin_and_status_without_response_body(self):
        request = httpx.Request("POST", "https://user:password@actual.example:8443/private?token=secret#fragment")
        response = httpx.Response(501, request=request, text="Query: SELECT secret FROM data")
        failure = httpx.HTTPStatusError("Provider secret payload", request=request, response=response)
        wrapped = RuntimeError("Company database search failed")
        wrapped.__cause__ = failure
        message = error_details(wrapped, endpoint="http://different-configured-host", operation="company_search")
        self.assertIn("endpoint='https://actual.example:8443'", message)
        self.assertIn("status=501 reason='Not Implemented'", message)
        for value in ("different-configured-host", "password", "token", "secret", "private", "fragment", "Provider"):
            self.assertNotIn(value, message)

    def test_invalid_url_is_safe_and_still_keeps_length_reason(self):
        for reason, expected in (
            ("URL component 'query' too long", "URL component 'query' too long"),
            ("Invalid port: 'secret'", "Invalid port"),
            ("arbitrary secret\nlog entry", "Invalid URL"),
        ):
            with self.subTest(reason=reason):
                message = error_details(httpx.InvalidURL(reason), endpoint="http://results/?key=secret", operation="publish_results")
                self.assertIn(expected, message)
                self.assertNotIn("secret", message)
                self.assertNotIn("\n", message)

    def test_known_local_validation_reason_and_unknown_message_redaction(self):
        known = "Result destination is missing registered website/page parents"
        self.assertIn(known, error_details(ValueError(known), endpoint="clickhouse://results", operation="publish_results"))
        message = error_details(ValueError("query and private data"), endpoint="clickhouse://results", operation="publish_results")
        self.assertIn("reason='unclassified error'", message)
        self.assertNotIn("private data", message)

    def test_postgres_connection_error_does_not_expose_dsn(self):
        message = error_details(
            psycopg2.OperationalError("password=private host=private"),
            endpoint="postgresql://user:password@processing:5432/private?token=secret",
            operation="register_parents",
        )
        self.assertIn("PostgreSQL connection or execution failed", message)
        self.assertIn("endpoint='postgresql://processing:5432'", message)
        self.assertNotIn("password", message)
        self.assertNotIn("private", message)

    def test_endpoint_redaction_handles_ipv6_and_malformed_input(self):
        self.assertEqual(safe_endpoint("clickhouses://user:password@[::1]:9440/db?token=secret"), "clickhouses://[::1]:9440")
        self.assertEqual(safe_endpoint("clickhouse://user:secret@[bad"), "<invalid endpoint>")
        self.assertEqual(safe_endpoint("clickhouse://db:secret"), "<invalid endpoint>")
        self.assertEqual(safe_endpoint("missing-scheme"), "<invalid endpoint>")

    def test_cyclic_exception_chain_is_bounded(self):
        first = RuntimeError("ClickHouse parent verification failed")
        second = NetworkError("private-data")
        first.__cause__ = second
        second.__cause__ = first
        message = error_details(first, endpoint="clickhouse://results", operation="publish_results")
        self.assertEqual(message.count("RuntimeError"), 1)
        self.assertEqual(message.count("NetworkError"), 1)

    def test_sqlite_codes_are_kept_without_query_values(self):
        database = sqlite3.connect(":memory:")
        self.addCleanup(database.close)
        try:
            database.execute("SELECT secret_column FROM private_table")
        except sqlite3.OperationalError as error:
            message = error_details(error, endpoint="sqlite://local", operation="cleanup")
        self.assertIn("code=1 reason=SQLITE_ERROR", message)
        self.assertNotIn("private_table", message)
        self.assertNotIn("secret_column", message)
        self.assertIn("reason=SQLITE_BUSY", error_details(
            sqlite3.OperationalError("database is locked"), endpoint="sqlite://local", operation="cleanup"
        ))
        self.assertNotIn("private SQL", error_details(
            sqlite3.OperationalError("private SQL"), endpoint="sqlite://local", operation="cleanup"
        ))
