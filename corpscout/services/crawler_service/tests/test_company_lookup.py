import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import httpx
from browser_http_fixture import install_browser_api
from pydantic import ValidationError
from test_crawl import browser_responses
from test_llm_profile import API_KEY, KEY, profile_payload
from test_package import response
from test_site_gate import classification

from crawler_service.company_evidence import (
    company_page_text,
    identity_excerpt,
    registration_evidence,
)
from crawler_service.company_lookup import (
    CompanyAssessment,
    CompanyLookupRequest,
    WebsiteIdentity,
    accept_match,
    identity_links,
    parse_identity,
    verified_facts,
)
from crawler_service.company_search import (
    normalized_name,
    search_companies,
    swedish_company_id,
)
from crawler_service.service import CrawlService
from crawler_service.service_api import create_app

URL = "https://example.se/"
CONTACT = "https://example.se/contact"
COMPANY = {
    "company_id": "5560123456",
    "legal_name": "Example AB",
    "status": "active",
    "primary_street_address": "Testgatan 1",
    "primary_postal_code": "12345",
    "primary_city": "Stockholm",
    "activity_description": "Engineering",
    "name_similarity": 1.0,
}
FACTS = [
    {
        "kind": "legal_name",
        "value": "Example AB",
        "source_url": CONTACT,
        "quote": "Operator: Example AB",
    },
    {
        "kind": "registration_number",
        "value": "556012-3456",
        "source_url": CONTACT,
        "quote": "Organisation number 556012-3456",
    },
    {
        "kind": "trading_name",
        "value": "Example",
        "source_url": CONTACT,
        "quote": "Operator: Example AB",
    },
    {
        "kind": "registration_number",
        "value": "SE-556012345601",
        "source_url": CONTACT,
        "quote": "Momsregnr: SE-556012345601",
    },
    {
        "kind": "business_activity",
        "value": "Engineering consultancy",
        "source_url": CONTACT,
        "quote": "Engineering consultancy",
    },
]


class LookupValidationTests(unittest.TestCase):
    def test_identity_links_skip_catalogs_articles_and_streams(self):
        links = [
            {"href": path, "text": label}
            for path, label in [
                ("/kontakt", "Kontakta oss"),
                ("/om-oss", "Om oss"),
                ("/privacy-policy", "Privacy policy"),
                ("/kopvillkor", "Köpvillkor"),
                ("/bolagsinformation", "Bolagsinformation"),
                ("/products/contact-lenses", "Contact lenses"),
                ("/produkt/contact", "Contact lenses"),
                ("/artikel/company", "Company AB"),
                ("/nyheter/company-acquired", "About the company acquisition"),
                ("/article-about-company", "About Company AB"),
                ("/watch/company", "Company"),
                ("/film/company", "Company"),
                ("/series/the-company", "The company"),
                ("/forum/contact", "Contact"),
                ("https://supplier.se/contact", "Contact"),
                ("/about-us.pdf", "About us"),
            ]
        ]
        self.assertEqual(
            set(identity_links(links, URL, {URL})),
            {
                URL + path
                for path in (
                    "kontakt",
                    "om-oss",
                    "privacy-policy",
                    "kopvillkor",
                    "bolagsinformation",
                )
            },
        )

    def test_labelled_ids_are_discovered_without_inventing_ownership_or_phones(self):
        text = "Addtech AB Org.nr: 556302-9726 Momsregnr: SE-556302972601 Tel: 0812345678 Other company: 5560123456"
        observed = registration_evidence({CONTACT: text})
        self.assertEqual(
            [x["normalized_company_id"] for x in observed], ["5563029726", "5563029726"]
        )
        self.assertTrue(
            all(x["quote"] in text and x["source_url"] == CONTACT for x in observed)
        )
        spaced = registration_evidence({CONTACT: "VAT SE 556302 9726 01"})
        self.assertEqual(spaced[0]["normalized_company_id"], "5563029726")
        self.assertEqual(
            registration_evidence({CONTACT: "Phone: 0812345678, GLN: 7365568254740"}),
            [],
        )

    def test_cookie_removal_and_excerpt_keep_legal_content_and_footer(self):
        html = '<div id="CybotCookiebotDialog">cookie catalogue</div><nav>Navigation</nav><main>Our products</main><footer>Example AB Org nr: 556012-3456</footer>'
        text = company_page_text(html)
        self.assertNotIn("cookie catalogue", text)
        self.assertNotIn("Navigation", text)
        self.assertIn("Example AB Org nr: 556012-3456", text)
        purpose_text = company_page_text(html, include_navigation=True)
        self.assertIn("Navigation", purpose_text)
        self.assertNotIn("cookie catalogue", purpose_text)
        long = (
            "Company services " * 3000
            + "Contact Example AB Org nr: 556012-3456 "
            + "More services " * 3000
            + "Legal operator: Example AB"
        )
        excerpt = identity_excerpt(long)
        self.assertLessEqual(len(excerpt), 12000)
        self.assertIn("Legal operator: Example AB", excerpt)
        self.assertIn("556012-3456", excerpt)

    def test_footer_identity_survives_navigation_cleanup(self):
        text = company_page_text(
            "<nav>Product categories</nav><main>Shop</main>"
            "<footer><nav>Operator: Example AB. Org nr: 556012-3456</nav></footer>"
        )
        self.assertNotIn("Product categories", text)
        self.assertIn("Operator: Example AB", text)
        self.assertEqual(
            registration_evidence({URL: text})[0]["normalized_company_id"],
            COMPANY["company_id"],
        )

    def test_indutrade_and_stendorren_evidence_survives_wrong_model_basis(self):
        assessment = CompanyAssessment(
            company_id=COMPANY["company_id"],
            confidence=0.99,
            basis="insufficient",
            reasons=["Exact ID agrees"],
        )
        self.assertEqual(accept_match(assessment, [COMPANY], FACTS)[0], COMPANY)
        assessment.basis = "name_and_address"
        self.assertEqual(accept_match(assessment, [COMPANY], FACTS[:1])[0], COMPANY)
        unrelated = COMPANY | {"company_id": "5560999999", "legal_name": "Unrelated AB"}
        assessment.company_id = unrelated["company_id"]
        self.assertIsNone(accept_match(assessment, [COMPANY, unrelated], FACTS)[0])

    def test_latour_legal_forms_normalize_equally_without_collapsing_distinct_names(
        self,
    ):
        self.assertEqual(
            normalized_name("INVESTMENTAKTIEBOLAGET LATOUR"),
            normalized_name("Investment AB Latour (publ)"),
        )
        self.assertEqual(
            normalized_name("Investmentaktiebolag Latour"),
            normalized_name("Investment Aktiebolaget Latour"),
        )
        self.assertNotEqual(
            normalized_name("Fabege AB"), normalized_name("Fabege 1 AB")
        )

    def test_genova_malformed_fact_does_not_discard_supported_facts(self):
        document = {
            "facts": [
                FACTS[0],
                {
                    "kind": "registration_number",
                    "value": "not found",
                    "source_url": "",
                    "quote": "not found",
                },
            ],
            "reasons": ["operator"],
        }
        facts, _ = parse_identity(document, {CONTACT: "Operator: Example AB"})
        self.assertEqual(facts, FACTS[:1])
        designer = FACTS[0] | {"quote": "Website by Example AB"}
        self.assertEqual(
            parse_identity(
                {"facts": [designer], "reasons": ["designer"]},
                {CONTACT: designer["quote"]},
            )[0],
            [],
        )

    def test_blank_explanation_keeps_supported_facts_without_accepting_invented_ones(
        self,
    ):
        document = {"facts": FACTS[:1], "reasons": ["", "  "]}
        facts, reasons = parse_identity(document, {CONTACT: "Operator: Example AB"})
        self.assertEqual(facts, FACTS[:1])
        self.assertIn("no explanation", reasons[0])
        self.assertEqual(
            parse_identity(document, {CONTACT: "Different company"})[0], []
        )
        with self.assertRaises(ValidationError):
            parse_identity(
                {**document, "reasons": [42]}, {CONTACT: "Operator: Example AB"}
            )

    def test_ambiguous_legal_names_and_invented_evidence_still_fail_closed(self):
        assessment = CompanyAssessment(
            company_id=COMPANY["company_id"],
            confidence=1,
            basis="unique_legal_name",
            reasons=["claimed"],
        )
        names = [FACTS[0], FACTS[0] | {"value": "Other AB"}]
        self.assertIsNone(accept_match(assessment, [COMPANY], names)[0])
        duplicate = COMPANY | {"company_id": "5560999999"}
        accepted, reason = accept_match(assessment, [COMPANY, duplicate], FACTS[:1])
        self.assertIsNone(accepted)
        self.assertIn("Multiple registry companies", reason)
        self.assertIn(COMPANY["company_id"], reason)
        self.assertIn(duplicate["company_id"], reason)
        self.assertEqual(
            parse_identity(
                {
                    "facts": [FACTS[0] | {"quote": "<div>Example AB</div>"}],
                    "reasons": ["claimed"],
                },
                {CONTACT: "Operator: Example AB"},
            )[0],
            [],
        )

    def test_swedish_vat_normalization_preserves_other_identifier_types(self):
        for value, expected in [
            ("556302-9726", "5563029726"),
            ("SE-556302972601", "5563029726"),
            ("se 556302 9726 01", "5563029726"),
            ("SE–556302972601", "5563029726"),
            ("197001011201", "197001011201"),
            ("556302972601", "556302972601"),
            ("SE556302972602", None),
            ("DE556302972601", None),
            ("SE5563029726", None),
        ]:
            with self.subTest(value=value):
                self.assertEqual(swedish_company_id(value), expected)

    def test_addtech_organisation_and_vat_numbers_are_the_same_identity(self):
        facts = [
            {
                "kind": "registration_number",
                "value": "556302-9726",
                "source_url": CONTACT,
                "quote": "Org.nr: 556302-9726",
            },
            {
                "kind": "registration_number",
                "value": "SE-556302972601",
                "source_url": CONTACT,
                "quote": "Momsregnr: SE-556302972601",
            },
        ]
        verified = verified_facts(
            WebsiteIdentity(facts=facts, reasons=["Operator"]),
            {CONTACT: "Org.nr: 556302-9726 Momsregnr: SE-556302972601"},
        )
        self.assertEqual(
            [fact["normalized_company_id"] for fact in verified],
            ["5563029726", "5563029726"],
        )
        self.assertEqual(verified[1]["value"], "SE-556302972601")
        candidate = COMPANY | {"company_id": "5563029726", "legal_name": "ADDTECH AB"}
        assessment = CompanyAssessment(
            company_id="5563029726",
            confidence=0.99,
            basis="registration_number",
            reasons=["Same operator"],
        )
        self.assertEqual(accept_match(assessment, [candidate], verified)[0], candidate)
        self.assertEqual(accept_match(assessment, [candidate], facts)[0], candidate)
        conflicting = facts + [
            {"kind": "registration_number", "value": "SE556012345601"}
        ]
        rejected, reason = accept_match(assessment, [candidate], conflicting)
        self.assertIsNone(rejected)
        self.assertIn("5560123456", reason)
        self.assertIn("5563029726", reason)

    def test_country_models_domain_and_limits(self):
        request = {
            "domain": "www.example.se",
            "country": "se",
            "llm": profile_payload(),
        }
        self.assertEqual(CompanyLookupRequest(**request).country, "SE")
        for extra in [
            {"country": "NO"},
            {"domain": "https://example.se/about"},
            {"domain": "https://user:pass@example.se"},
            {"config": {"max_pages": 11}},
            {"llm": profile_payload(model="typesafe/jev-1.13")},
        ]:
            with self.subTest(extra=extra), self.assertRaises(ValidationError):
                CompanyLookupRequest(**(request | extra))

    def test_unobserved_identity_and_model_invented_ids_are_rejected(self):
        identity = WebsiteIdentity(facts=FACTS, reasons=["operator"])
        self.assertEqual(
            verified_facts(identity, {CONTACT: "Nothing about a company"}), []
        )
        assessment = CompanyAssessment(
            company_id="5560999999",
            confidence=0.99,
            basis="registration_number",
            reasons=["claimed"],
        )
        self.assertIsNone(accept_match(assessment, [COMPANY], FACTS)[0])
        assessment.company_id = COMPANY["company_id"]
        conflicting = FACTS + [{"kind": "registration_number", "value": "5560654321"}]
        self.assertIsNone(accept_match(assessment, [COMPANY], conflicting)[0])
        assessment.basis = "unique_legal_name"
        self.assertIsNone(
            accept_match(
                assessment, [COMPANY, COMPANY | {"company_id": "5560999999"}], FACTS[:1]
            )[0]
        )
        self.assertIsNone(
            accept_match(assessment, [COMPANY], [FACTS[0] | {"kind": "trading_name"}])[
                0
            ]
        )


class LookupApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def test_name_candidates_accept_both_registry_identifier_lengths(self):
        rows = [COMPANY, COMPANY | {"company_id": "197001011234"}]
        async with httpx.AsyncClient(
            base_url="http://registry",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, text="\n".join(json.dumps(row) for row in rows)
                )
            ),
        ) as http:
            searches = []
            found = await search_companies(
                http, kind="name", value="Example AB", searches=searches
            )
            self.assertEqual(found, rows)
            self.assertEqual(searches[0]["http_status"], 200)
            self.assertEqual(searches[0]["parameters"], {"value": "example"})

    async def exercise(
        self,
        *,
        site_type="company",
        jev=False,
        jev_ranking=False,
        database_failure=False,
        rows=None,
        operator_facts=None,
        page_fixture=None,
        max_pages=2,
        crawl_mode=None,
        mapped_ids=None,
        skip_if_mapped=False,
    ):
        model_calls, searches, visited = [], [], []
        network = httpx.AsyncHTTPTransport.handle_async_request

        async def transport(client, request):
            if request.url.host == "llm-fixture":
                payload = json.loads(request.content)
                prompt = payload["messages"][-1]["content"]
                model_calls.append(prompt)
                if '"task": "site_classification"' in prompt:
                    answer = classification(
                        crawl_decision="continue_crawling" if site_type == "company" else "skip_crawling",
                        site_types=[site_type], operator_name="Example", evidence=["Example"],
                        site_description="The website presents Example.", purpose="Website of Example",
                        business_activities=[], research_profiles=["general"],
                    )
                elif "Extract the legal operator" in prompt:
                    answer = {
                        "facts": FACTS if operator_facts is None else operator_facts,
                        "reasons": ["Explicit operator and organisation number"],
                    }
                elif "Assess ONLY compatibility" in prompt:
                    self.assertNotIn('"legal_name"', prompt)
                    self.assertNotIn('"street_address"', prompt)
                    answer = {
                        "checks": [
                            {
                                "company_id": COMPANY["company_id"],
                                "status": "consistent",
                                "reasons": ["Engineering consultancy agrees with 7112"],
                            }
                        ]
                    }
                    return httpx.Response(
                        200,
                        json={
                            "choices": [
                                {
                                    "message": {"content": json.dumps(answer)},
                                    "finish_reason": "stop",
                                }
                            ],
                            "usage": {"prompt_tokens": 20, "completion_tokens": 10},
                        },
                    )
                else:
                    self.assertNotIn('"industries"', prompt)
                    answer = {
                        "company_id": COMPANY["company_id"],
                        "confidence": 0.98,
                        "basis": "registration_number",
                        "reasons": ["Same explicit organisation number"],
                    }
                return httpx.Response(200, json=response(answer))
            if request.url.host == "openrouter.ai":
                body = json.loads(request.content)
                if "operator" in body["questions"]:
                    model_calls.append("jev_ranking")
                    return httpx.Response(
                        200,
                        json={
                            "answers": {
                                "operator": {
                                    "type": "choice",
                                    "choice": COMPANY["company_id"],
                                    "probabilities": {
                                        "none": 0.02,
                                        COMPANY["company_id"]: 0.98,
                                    },
                                },
                                "basis_" + COMPANY["company_id"]: {
                                    "type": "choice",
                                    "choice": "registration_number",
                                },
                            },
                            "usage": {"input_tokens": 20, "output_tokens": 2},
                        },
                    )
                if "industry_" + COMPANY["company_id"] in body["questions"]:
                    model_calls.append("jev_industry")
                    self.assertNotIn('"legal_name"', json.dumps(body))
                    return httpx.Response(
                        200,
                        json={
                            "answers": {
                                "industry_" + COMPANY["company_id"]: {
                                    "type": "choice",
                                    "choice": "consistent",
                                }
                            },
                            "usage": {"input_tokens": 20, "output_tokens": 2},
                        },
                    )
                model_calls.append("jev_classification")
                self.assertEqual(set(body["questions"]), {"site_type"})
                return httpx.Response(
                    200,
                    json={
                        "answers": {
                            "site_type": {"type": "choice", "choice": site_type},
                        },
                        "usage": {"input_tokens": 10, "output_tokens": 2},
                    },
                )
            if request.url.host == "clickhouse-fixture":
                searches.append(request)
                self.assertEqual(request.url.params["readonly"], "2")
                if "company_domains_resolved" in request.content.decode():
                    self.assertIn("country_code = 'SE'", request.content.decode())
                    self.assertIn("is_active = 1", request.content.decode())
                    if database_failure:
                        return httpx.Response(503)
                    return httpx.Response(200, text="\n".join(json.dumps({"company_id": id}) for id in (mapped_ids or [])))
                if "se_company_industry_display_current" in request.content.decode():
                    self.assertIn("{ids:Array(String)}", request.content.decode())
                    return httpx.Response(
                        200,
                        text=json.dumps(
                            {
                                "company_id": COMPANY["company_id"],
                                "classification_system": "NACE_REV2",
                                "classification_code": "7112",
                                "classification_version": "NACE_REV_2",
                                "reported_label": "Engineering activities and related technical consultancy",
                                "reference_label": "Engineering activities and related technical consultancy",
                                "is_primary": 1,
                                "source": "sweden_scb",
                                "source_record_uid": "fixture",
                            }
                        ),
                    )
                self.assertIn(
                    "corpscout.se_companies_serving", request.content.decode()
                )
                self.assertIn("{value:String}", request.content.decode())
                self.assertNotIn("INSERT", request.content.decode())
                if database_failure:
                    return httpx.Response(503, text="Registry unavailable")
                return httpx.Response(
                    200,
                    text="\n".join(
                        json.dumps(row) for row in ([COMPANY] if rows is None else rows)
                    ),
                )
            return await network(client, request)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment = {
                "CLICKHOUSE_URL": "http://clickhouse-fixture",
                "CLICKHOUSE_USER": "reader",
                "CLICKHOUSE_PASSWORD": "private-db-key",
                "CRAWLER_LLM_ENCRYPTION_KEY": KEY,
            }
            service = CrawlService(root, environment, concurrency=1, max_pending=2)
            app = create_app(service, api_token="admin-token")
            pages = (
                page_fixture
                if page_fixture is not None
                else {
                    URL: (
                        "<nav>Product categories and shopping cart</nav><h1>Example engineering</h1><a href='/contact'>Contact</a>",
                        [
                            {"href": "/contact", "text": "Contact"},
                            {
                                "href": "/products/contact-lenses",
                                "text": "Contact lenses",
                            },
                            {"href": "/news/company", "text": "Company news"},
                            {"href": "/watch/company", "text": "About the company"},
                        ],
                        200,
                        None,
                    ),
                    CONTACT: (
                        "<h1>Operator: Example AB</h1><p>Organisation number 556012-3456</p><p>Momsregnr: SE-556012345601</p><p>Engineering consultancy</p>",
                        [],
                        200,
                        None,
                    ),
                }
            )
            with (
                patch.object(
                    httpx.AsyncHTTPTransport, "handle_async_request", transport
                ),
                patch("crawler_service.crawl.open_browser", lambda *_args, **_: browser_responses(pages, visited)),
                patch(
                    "crawler_service.company_lookup.open_browser",
                    lambda *_args, **_: browser_responses(pages, visited),
                ),
            ):
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app), base_url="http://service"
                    ) as client,
                ):
                    service.results = Mock()
                    body = {
                        "request_id": "lookup",
                        "country": "SE",
                        "domain": "example.se",
                        "llm": profile_payload(),
                        "config": {"max_pages": max_pages},
                    }
                    if jev or jev_ranking:
                        body["decision_llm"] = profile_payload(
                            model="typesafe/jev-1.13",
                            base_url="https://openrouter.ai/api/v1",
                        )
                    if jev_ranking:
                        body["decision_tasks"] = (
                            ["site_eligibility"] if jev else []
                        ) + ["company_match"]
                    endpoint = "/v1/company-lookups"
                    if crawl_mode is None:
                        body["skip_if_mapped"] = skip_if_mapped
                    if crawl_mode is not None:
                        endpoint = "/v1/crawls"
                        body.pop("country")
                        body.pop("domain")
                        body.update(url=URL, site_info=True, crawl=False if crawl_mode == "site_info" else "full",
                            company_lookup={"country": "SE", "skip_if_mapped": skip_if_mapped})
                        body["config"].update(max_sitemap_urls=0, max_sitemap_files=0, web_search=False)
                    self.assertEqual(
                        (
                            await client.post(endpoint, json=body)
                        ).status_code,
                        401,
                    )
                    client.headers["Authorization"] = "Bearer admin-token"
                    submitted = await client.post(endpoint, json=body)
                    self.assertEqual(submitted.status_code, 202, submitted.text)
                    self.assertEqual(submitted.headers["location"], "/v1/crawls/lookup")
                    job = await asyncio.wait_for(service.wait("lookup"), 5)
                    self.assertEqual(
                        job.state, "failed" if database_failure else "completed"
                    )
                    self.assertEqual(job.purpose, "company_lookup")
                    if job.state == "completed":
                        self.assertIn("company match", job.reason.lower())
                    self.assertEqual(job.s3_state, "not_configured")
                    self.assertEqual(service.history.pending_uploads(), [])
                    service.results.prepare_event.assert_not_called()
                    result = (await client.get("/v1/crawls/lookup/result")).json()
                    self.assertFalse(result["persisted_to_database"])
                    same = (
                        await client.get(
                            "/v1/crawls/lookup/debug?attempt=1&result=true"
                        )
                    ).json()
                    self.assertEqual(same, result)
                    trace = (
                        await client.get("/v1/crawls/lookup/debug?download=true")
                    ).text
                    self.assertNotIn(API_KEY, trace)
                    self.assertNotIn("private-db-key", trace)
                    self.assertIn("company_lookup_result", trace)
                    if result["status"] != "already_mapped" and not (database_failure and skip_if_mapped):
                        self.assertIn("company_crawl_scope", trace)
                    if searches:
                        self.assertIn("company_search", trace)
                        self.assertIn("parameters", trace)
                        self.assertIn("duration_ms", trace)
                    retry = await client.post(endpoint, json=body)
                    self.assertEqual(retry.json()["attempt"], 1)
                    service.results = None
            restarted = CrawlService(root, environment, concurrency=1, max_pending=2)
            await restarted.start()
            try:
                restarted.results = Mock()
                restarted.persist(restarted.jobs["lookup"])
                self.assertEqual(restarted.history.pending_uploads(), [])
                restarted.results = None
            finally:
                await restarted.close()
            return result, model_calls, searches, visited

    async def test_basic_matching_reuses_homepage_and_classification(self):
        output, calls, _, visited = await self.exercise(crawl_mode="site_info", max_pages=1)
        self.assertEqual(output["status"], "matched")
        self.assertEqual(visited, [URL, CONTACT])
        self.assertEqual(len(output["crawl_result"]["crawl"]["pages"]), 1)
        self.assertEqual(len(calls), 4)
        self.assertEqual(output["usage"]["calls"], 4)

    async def test_full_matching_continues_identity_research_after_shop_gate(self):
        output, _, _, visited = await self.exercise(crawl_mode="full", site_type="online_store")
        self.assertEqual(output["status"], "matched")
        self.assertEqual(output["crawl_result"]["crawl"]["status"], "skip_crawling")
        self.assertEqual(output["site_info_result"]["crawl"]["status"], "finished")
        self.assertEqual(visited, [URL, CONTACT])

    async def test_existing_connections_skip_matching_but_preserve_basic_and_full_crawl(self):
        for mode in (None, "site_info", "full"):
            output, calls, searches, visited = await self.exercise(crawl_mode=mode, site_type="online_store",
                skip_if_mapped=True, mapped_ids=["5560123456", "5560999999"])
            self.assertEqual(output["status"], "already_mapped")
            self.assertFalse(output["found"])
            self.assertIsNone(output["company_id"])
            self.assertEqual(output["existing_company_ids"], ["5560123456", "5560999999"])
            self.assertEqual(visited, [URL])
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(searches), 1)
            self.assertEqual(output["site_info_result"]["crawl"]["status"], "finished")

    async def test_no_active_mapping_proceeds_and_failed_check_does_not_mean_already_mapped(self):
        output, _, searches, _ = await self.exercise(crawl_mode="site_info", skip_if_mapped=True)
        self.assertEqual(output["status"], "matched")
        self.assertEqual(len(searches), 3)
        output, calls, _, visited = await self.exercise(crawl_mode="site_info", skip_if_mapped=True, database_failure=True)
        self.assertEqual(output["status"], "failed")
        self.assertEqual(output["searches"][0]["status"], "failed")
        self.assertEqual(output["site_info_result"]["crawl"]["status"], "finished")
        self.assertEqual(visited, [URL])
        self.assertEqual(len(calls), 1)

    async def test_company_proposal_preserves_evidence_searches_trace_and_never_publishes(
        self,
    ):
        result, calls, searches, visited = await self.exercise()
        self.assertEqual(result["site_info_result"]["schema_version"], "company-crawl-result/1.2")
        self.assertEqual(result["site_info_result"]["crawl"]["status"], "finished")
        self.assertEqual(result["site_info"]["evidence_status"], "source_matched")
        self.assertEqual(result["site_info_result"]["documents"][0]["page_id"], "p0001")
        self.assertTrue(result["found"])
        self.assertEqual(result["company_id"], COMPANY["company_id"])
        self.assertEqual(result["confidence"], 0.98)
        self.assertEqual(len(calls), 4)
        self.assertIn("Product categories and shopping cart", calls[0])
        self.assertEqual(len(searches), 2)
        self.assertEqual(searches[0].url.params["param_value"], COMPANY["company_id"])
        self.assertEqual(
            searches[1].url.params["param_ids"], repr([COMPANY["company_id"]])
        )
        self.assertEqual(result["industry_assessments"][0]["status"], "consistent")
        self.assertEqual(
            [
                fact["normalized_company_id"]
                for fact in result["identity"]
                if fact["kind"] == "registration_number"
            ],
            [COMPANY["company_id"], COMPANY["company_id"]],
        )
        self.assertEqual(visited, [URL, CONTACT])

    async def test_jev_ranking_can_run_without_jev_classification(self):
        result, calls, _, _ = await self.exercise(jev_ranking=True)
        self.assertTrue(result["found"])
        self.assertIn("jev_ranking", calls)
        self.assertIn("jev_industry", calls)
        self.assertNotIn("jev_classification", calls)
        self.assertEqual(
            result["candidate_assessments"][0]["company_id"], COMPANY["company_id"]
        )
        self.assertEqual(result["no_match_probability"], 0.02)
        self.assertEqual(result["industry_assessments"][0]["status"], "consistent")

    async def test_generic_and_jev_match_operators_of_shops_and_content_sites(self):
        for jev in (False, True):
            for site_type in (
                "online_store",
                "news_media",
                "entertainment",
                "content_site",
                "forum_community",
                "marketplace",
                "mixed",
                "unknown",
            ):
                with self.subTest(jev=jev, site_type=site_type):
                    result, calls, searches, visited = await self.exercise(
                        site_type=site_type, jev=jev, max_pages=4
                    )
                    self.assertEqual(result["status"], "matched")
                    self.assertEqual(result["company_id"], COMPANY["company_id"])
                    self.assertEqual(result["site_type"], site_type)
                    self.assertEqual(result["crawl_scope"], "operator_identity")
                    self.assertNotIn("crawl_decision", result["classification"])
                    self.assertEqual(len(calls), 5 if jev else 4)
                    self.assertEqual(len(searches), 2)
                    self.assertEqual(visited, [URL, CONTACT])

    async def test_shop_operator_can_be_identified_from_homepage_footer_only(self):
        result, _, _, visited = await self.exercise(
            site_type="online_store",
            operator_facts=[fact | {"source_url": URL} for fact in FACTS[:2]],
            page_fixture={
                URL: (
                    "<main>Shop our products</main><footer>Operator: Example AB. Organisation number 556012-3456</footer>",
                    [],
                    200,
                    None,
                )
            },
        )
        self.assertEqual(result["company_id"], COMPANY["company_id"])
        self.assertEqual(visited, [URL])

    async def test_content_site_without_supported_operator_remains_unmatched(self):
        result, _, searches, visited = await self.exercise(
            site_type="news_media",
            # The model's claimed facts have no supporting text on this page.
            operator_facts=[FACTS[0] | {"source_url": URL}],
            page_fixture={
                URL: (
                    "<main>Latest news about Example AB</main>",
                    [{"href": "/news/example-ab", "text": "About Example AB"}],
                    200,
                    None,
                )
            },
        )
        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["stop_reason"], "operator_not_identified")
        self.assertEqual(result["site_type"], "news_media")
        self.assertEqual(result["identity"], [])
        self.assertEqual(searches, [])
        self.assertEqual(visited, [URL])

    async def test_no_candidates_and_database_failure_remain_distinct(self):
        result, calls, _, _ = await self.exercise(rows=[])
        self.assertEqual(result["status"], "not_found")
        self.assertEqual(len(calls), 2)
        failed, _, _, _ = await self.exercise(database_failure=True)
        self.assertEqual(failed["status"], "failed")
        self.assertFalse(failed["found"])
        self.assertEqual(failed["searches"][0]["status"], "failed")
