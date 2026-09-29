import unittest

import httpx

from crawler_service.company_search import search_companies


class CompanySearchErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_status_and_origin_are_saved_without_query_or_credentials(self):
        for status, reason in ((408, "Request Timeout"), (501, "Not Implemented")):
            with self.subTest(status=status):
                searches = []
                async with httpx.AsyncClient(
                    base_url="https://reader:password@registry:8443/private?token=query-secret",
                    transport=httpx.MockTransport(
                        lambda request, status=status: httpx.Response(status, text="SQL with private payload")
                    ),
                ) as http:
                    with self.assertRaises(RuntimeError) as caught:
                        await search_companies(
                            http, kind="name", value="Confidential customer name", searches=searches
                        )
                failure = str(caught.exception)
                self.assertIsInstance(caught.exception.__cause__, httpx.HTTPStatusError)
                self.assertIn("operation='company_search:name'", failure)
                self.assertIn("endpoint='https://registry:8443'", failure)
                self.assertIn(f"status={status} reason={reason!r}", failure)
                self.assertEqual(searches[0]["status"], "failed")
                self.assertEqual(searches[0]["http_status"], status)
                self.assertIn(searches[0]["error"], failure)
                for value in ("reader", "password", "query-secret", "private", "Confidential", "SELECT", "param_value"):
                    self.assertNotIn(value, failure)

    async def test_transport_timeout_keeps_useful_reason_without_provider_message(self):
        def timeout(request):
            raise httpx.ReadTimeout("private credentials", request=request)

        searches = []
        async with httpx.AsyncClient(
            base_url="http://registry", transport=httpx.MockTransport(timeout)
        ) as http:
            with self.assertRaises(RuntimeError) as caught:
                await search_companies(
                    http, kind="registration_number", value="1234567890", searches=searches
                )
        self.assertIn("ReadTimeout reason='HTTP request timed out'", str(caught.exception))
        self.assertNotIn("private credentials", str(caught.exception))
        self.assertEqual(searches[0]["status"], "failed")
