import unittest
from unittest.mock import Mock, patch

from clickhouse_driver import Client
from corpscout_identity.registration import identify_website
from test_company_lookup_batches import result

from crawler_service.company_lookup_results import deliver, publish, timestamp


class PublicationRequestLimitsTests(unittest.TestCase):
    def test_large_redirect_batch_verifies_all_parents_before_inserting(self):
        for site_count in (200, 1001):
            with self.subTest(sites=site_count):
                self.assert_publication_batch(site_count)

    def assert_publication_batch(self, site_count):
        results = []
        expected = {}
        for index in range(site_count):
            domain = f"site-{index}.se"
            output = result(domain)
            output["request_id"] = f"request-{index}"
            requested = output["website_url"]
            redirected = f"https://www.{domain}/"
            original = f"http://{domain}/"
            output["site_info_result"]["crawl"]["pages"] = [
                {
                    "requested_url": requested,
                    "source_url": redirected,
                    "redirects": [{"url": original, "location": redirected}],
                }
            ]
            results.append(output)
            for url in (requested, redirected, original):
                identity = identify_website(url)
                expected[identity.page_id] = {
                    "page_id": identity.page_id,
                    "website_id": identity.website_id,
                    "domain_id": identity.domain_id,
                    "page_url": identity.page_url,
                }

        verified = []
        inserts = []
        missing = None

        client = Mock(spec=Client)

        def execute(query, params, **kwargs):
            if query.startswith("SELECT"):
                self.assertEqual(inserts, [])
                rendered = Client("unused").substitute_params(query, params, Client("unused").connection.context)
                self.assertLess(len(rendered.encode()), 262144)
                ids = params["ids"]
                websites = params["websites"]
                domains = params["domains"]
                self.assertIsInstance(ids, tuple)
                self.assertEqual(
                    set(websites), {expected[key]["website_id"] for key in ids}
                )
                self.assertEqual(
                    set(domains), {expected[key]["domain_id"] for key in ids}
                )
                verified.extend(ids)
                return [tuple(expected[key].values()) for key in ids if key != missing]
            self.assertCountEqual(verified, expected)
            columns = query.split("(", 1)[1].split(")", 1)[0].split(",")
            rows = [dict(zip(columns, row, strict=True)) for row in params]
            self.assertCountEqual(
                [row["request_id"] for row in rows],
                [output["request_id"] for output in results],
            )
            inserts.append(query)
            return len(params)

        client.execute.side_effect = execute
        publish(client, results)
        self.assertCountEqual(verified, expected)
        self.assertEqual(len(inserts), 2)
        self.assertIn("website_company_lookup_results", inserts[-1])

        # Missing parents still block every result insert.
        verified.clear()
        inserts.clear()
        missing = next(reversed(expected))
        with self.assertRaisesRegex(ValueError, "missing registered website/page parents"):
            publish(client, results)
        self.assertCountEqual(verified, expected)
        self.assertEqual(inserts, [])


    def test_native_destination_uses_dedicated_writer_and_closes_after_failure(self):
        endpoint = "clickhouse://writer:password@results:9000/corpscout"
        with (
            patch("crawler_service.company_lookup_results.Client.from_url") as factory,
            patch("crawler_service.company_lookup_results.publish", side_effect=ValueError("missing parents")) as writer,
            self.assertRaisesRegex(ValueError, "missing parents"),
        ):
            deliver({"CLICKHOUSE_RESULTS_NATIVE_URL": endpoint,
                     "CLICKHOUSE_NATIVE_URL": "clickhouse://registrar@registry:9000/corpscout"}, [])
        factory.assert_called_once_with(endpoint + "?send_receive_timeout=60")
        writer.assert_called_once_with(factory.return_value, [])
        factory.return_value.disconnect.assert_called_once()
        with self.assertRaisesRegex(ValueError, "native ClickHouse result connection"):
            deliver({"CLICKHOUSE_RESULTS_NATIVE_URL": "http://results:8123"}, [])

    def test_result_timestamps_are_normalized_to_utc(self):
        self.assertEqual(timestamp("2026-09-29T10:32:54.123456+02:00"),
                         "2026-09-29 08:32:54.123456")

    def test_native_connection_preserves_explicit_options(self):
        endpoint = "clickhouses://writer:pass%40word@results:9440/corpscout?send_receive_timeout=17&connect_timeout=3&secure=true"
        with (
            patch("crawler_service.company_lookup_results.Client.from_url", wraps=Client.from_url),
            patch("crawler_service.company_lookup_results.publish") as writer,
        ):
            deliver({"CLICKHOUSE_RESULTS_NATIVE_URL": endpoint}, [])
        connection = writer.call_args.args[0].connection
        self.assertEqual(connection.send_receive_timeout, 17)
        self.assertEqual(connection.connect_timeout, 3)
        self.assertEqual(connection.user, "writer")
        self.assertEqual(connection.password, "pass@word")
        self.assertTrue(connection.secure_socket)
