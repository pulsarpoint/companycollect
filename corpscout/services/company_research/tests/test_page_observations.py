"""Public source observations preserve entity boundaries and survive portable replay."""

import json
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from company_research.browser import PageCapture
from company_research.captures import open_crawl
from company_research.clickhouse_results import result_row
from company_research.crawl import crawl_company
from company_research.identifiers import identifier_validation
from company_research.page_agent import PageInput
from company_research.page_observations import collect_page_observations
from company_research.storage import content_hash

URL = "https://example.co.uk/jobs"
HTML = """<!doctype html><html lang="en-GB"><head>
<base href="https://example.co.uk/public/"><title>Example careers</title>
<meta charset="UTF-8"><meta name="description" content="Engineering jobs">
<meta name="description" content="Second description">
<meta property="og:site_name" content="Example"><meta name="generator" content="Example CMS 2.0">
<link rel="canonical" href="../careers"><link rel="alternate" hreflang="de" href="/de/jobs">
<link rel="stylesheet" href="site.css">
<script type="application/ld+json">{"@context":{"@type":"VocabularyNotAnEntity"},"@graph":[
{"@type":"Organization","@id":"#company","name":"Example","email":"info@example.co.uk",
"telephone":"020 8366 1177","leiCode":"HWUPKR0MPOU8FGXBT395","vatID":"DE000000003",
"sameAs":["https://linkedin.com/company/example","https://wikidata.org/wiki/Q1"],
"address":{"@type":"PostalAddress","streetAddress":"1 Example Street","addressCountry":"GB"}},
{"@type":"Organization","@id":"#publisher","name":"Different Publisher","email":"publisher@example.net"},
{"@type":"JobPosting","title":"Engineer","hiringOrganization":{"@id":"#company"},"baseSalary":null},
{"@type":"Product","name":"Sensor","offers":{"@type":"Offer","price":"100","priceCurrency":"EUR"}}
]}</script>
<script type="application/ld+json">{invalid json}</script>
<script src="https://www.googletagmanager.com/gtag/js?id=G-ABC123DEF4"></script>
<script>fbq('init', '123456789012345'); var img='logo@2x.png'; var pkg='bootstrap@5.3.3';</script>
</head><body><h1>Work with us</h1><main><p>Example develops industrial sensors.
Join our engineering team and design new products for industrial customers.</p></main>
<div itemscope itemtype="https://schema.org/Organization" itemid="#micro" itemref="external-vat">
<span itemprop="name">Microdata Company</span><a itemprop="email" href="mailto:micro@example.co.uk">Email</a>
<div itemprop="address" itemscope itemtype="https://schema.org/PostalAddress">
<span itemprop="streetAddress">2 Other Street</span><meta itemprop="addressCountry" content="GB"></div>
<div itemprop="numberOfEmployees" itemscope itemtype="https://schema.org/QuantitativeValue"><data itemprop="value" value="20">20 employees</data></div>
</div><span id="external-vat" itemprop="vatID">PT123456789</span>
<a href="mailto:hello%40example.co.uk?subject=Contact">Contact</a><a href="tel:+442083661177;ext=42">Call</a>
<a href="https://linkedin.com/company/example">LinkedIn</a><a href="https://netflix.com">Not X</a>
<a href="https://linkedin.com.evil.test">Not LinkedIn</a>
<a href="brochure.pdf?download=1">Brochure</a><iframe src="/job-board"></iframe>
<footer>LEI HWUPKR0MPOU8FGXBT394; ref REFAHWUPKR0MPOU8FGXBT394; VAT DE000000003; invalid DE000000004.
Call 020 8366 1177. Email footer@example.co.uk.</footer></body></html>"""


def observe(html=HTML, url=URL, headers=None):
    return collect_page_observations(
        html,
        page_id="p0001",
        source_url=url,
        representation="rendered_html",
        response_headers=headers,
    )


class ObservationTests(unittest.TestCase):
    def test_metadata_structured_entities_and_malformed_blocks_are_lossless(self):
        result = observe()
        meta = result["metadata"]
        self.assertEqual(meta["title"], "Example careers")
        self.assertEqual(meta["language"], "en-GB")
        self.assertEqual(meta["charset"], "utf-8")
        self.assertEqual(meta["canonical_url"], "https://example.co.uk/careers")
        self.assertEqual(meta["meta"]["description"], "Engineering jobs")
        self.assertEqual(
            len([m for m in meta["meta_entries"] if m["name"] == "description"]), 2
        )
        self.assertEqual(
            meta["alternate_languages"][0]["url"], "https://example.co.uk/de/jobs"
        )
        structured = result["structured_data"]
        self.assertNotIn("VocabularyNotAnEntity", structured["jsonld_types"])
        companies = [
            e for e in structured["jsonld_entities"] if e["types"] == ["Organization"]
        ]
        self.assertEqual(
            [e["data"]["name"] for e in companies], ["Example", "Different Publisher"]
        )
        self.assertEqual(
            [e["entity_path"] for e in companies], ["/@graph/0", "/@graph/1"]
        )
        job = next(
            e["data"]
            for e in structured["jsonld_entities"]
            if e["types"] == ["JobPosting"]
        )
        self.assertIsNone(job["baseSalary"])
        self.assertEqual(job["hiringOrganization"], {"@id": "#company"})
        self.assertEqual(structured["jsonld_blocks"][1]["raw"], "{invalid json}")
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["html_sha256"], content_hash(HTML))
        self.assertEqual(result, observe())

    def test_microdata_nested_scope_and_itemref_do_not_mix_companies(self):
        items = observe()["structured_data"]["microdata"]
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["properties"]["name"], ["Microdata Company"])
        self.assertEqual(items[0]["properties"]["address"], [{"entity_index": 1}])
        self.assertNotIn("streetAddress", items[0]["properties"])
        self.assertEqual(items[1]["properties"]["streetAddress"], ["2 Other Street"])
        self.assertEqual(items[2]["properties"]["value"], ["20"])
        self.assertEqual(items[0]["properties"]["vatID"], ["PT123456789"])
        cycle = '<div itemscope id="self" itemref="self self"><span itemprop="name">One</span></div>'
        self.assertEqual(
            observe(cycle)["structured_data"]["microdata"][0]["properties"]["name"],
            ["One"],
        )

    def test_contacts_filter_assets_match_host_boundaries_and_preserve_provenance(self):
        contacts = observe()["contacts"]
        emails = {c["value"] for c in contacts if c["type"] == "email"}
        self.assertTrue(
            {
                "publisher@example.net",
                "micro@example.co.uk",
                "hello@example.co.uk",
                "footer@example.co.uk",
            }
            <= emails
        )
        self.assertNotIn("logo@2x.png", emails)
        self.assertNotIn("bootstrap@5.3.3", emails)
        profiles = [c for c in contacts if c["type"] == "profile"]
        self.assertEqual(
            {c["value"] for c in profiles},
            {"https://linkedin.com/company/example", "https://wikidata.org/wiki/Q1"},
        )
        phone = next(
            c for c in contacts if c["type"] == "phone" and c["source"] == "link"
        )
        self.assertEqual(phone["value"], "+442083661177")
        self.assertEqual(phone["extension"], "42")
        self.assertTrue(phone["valid"])
        publisher = next(
            c
            for c in contacts
            if c["source"] == "jsonld" and c["value"] == "publisher@example.net"
        )
        self.assertEqual(publisher["locator"]["entity_path"], "/@graph/1")
        national = observe('<a href="tel:02083661177">Call</a>', "https://example.com")[
            "contacts"
        ][0]
        self.assertEqual(national["value"], "02083661177")
        self.assertFalse(national["valid"])

    def test_identifiers_keep_invalid_claims_but_reject_invalid_unlabelled_tokens(self):
        identifiers = observe()["identifiers"]
        claimed = next(
            i for i in identifiers if i["type"] == "lei" and i["source"] == "jsonld"
        )
        self.assertFalse(claimed["valid"])
        self.assertEqual(claimed["validation"], "checksum")
        text = [i for i in identifiers if i["source"] == "visible_text"]
        self.assertEqual(
            {i["value"] for i in text},
            {"HWUPKR0MPOU8FGXBT394", "DE000000003", "PT123456789"},
        )
        for value in ["DE000000003", "IT00000000000", "PT123456789", "NL123456789B01"]:
            self.assertTrue(identifier_validation("vat", value)["valid"])
        for value in ["DE000000004", "IT00000000001", "XX123456789", "FRABC", ""]:
            self.assertFalse(identifier_validation("vat", value)["valid"])
        self.assertIsNone(identifier_validation("tax", "123")["valid"])

    def test_technology_processing_is_deferred_but_source_evidence_is_kept(self):
        result = observe(
            headers={
                "Content-Type": "text/html",
                "Server": "nginx",
                "Strict-Transport-Security": "max-age=31536000",
                "Set-Cookie": "private=session",
                "Authorization": "secret",
            }
        )
        self.assertEqual(result["technology_analysis"], "deferred")
        self.assertIsNone(result["trackers"])
        self.assertIsNone(result["resources"])
        self.assertEqual(result["metadata"]["meta"]["generator"], "Example CMS 2.0")
        self.assertEqual(
            result["document_links"][0]["url"],
            "https://example.co.uk/public/brochure.pdf?download=1",
        )
        self.assertEqual(
            set(result["response_headers"]),
            {"content-type", "server", "strict-transport-security"},
        )
        self.assertNotIn("fbq", result["text"]["visible"])
        self.assertNotIn("Different Publisher", result["text"]["visible"])
        self.assertIn("industrial sensors", result["text"]["main"])

    def test_financial_links_keep_page_and_document_urls_without_classifying_contents(
        self,
    ):
        result = observe("""<base href="https://example.test/company/">
            <a href="../investor-relations/">Investors</a>
            <a href="/downloads/2025.pdf/?download=1">Annual report 2025</a>
            <a href="/finansijski-izvestaji/">Izveštaji</a>
            <a href="/opaque" title="Financial statements">Read more</a>
            <a href="/brochure.pdf">Product brochure</a>
            <a href="https://financial.test/contact">Contact</a>
            <a href="mailto:financial@example.test">Financial information</a>
            <a href="javascript:alert(1)">Annual report</a>""")
        self.assertEqual(
            [link["url"] for link in result["financial_links"]],
            [
                "https://example.test/investor-relations/",
                "https://example.test/downloads/2025.pdf/?download=1",
                "https://example.test/finansijski-izvestaji/",
                "https://example.test/opaque",
            ],
        )
        self.assertEqual(len(result["document_links"]), 2)
        self.assertTrue(
            all(link["content_examined"] is False for link in result["financial_links"])
        )
        self.assertEqual(
            [link["anchor_index"] for link in result["financial_links"]], [0, 1, 2, 3]
        )

    def test_nonfinite_json_and_readability_failure_do_not_destroy_capture(self):
        with patch(
            "company_research.page_observations.trafilatura.extract",
            side_effect=ValueError("bad tree"),
        ):
            result = observe(
                '<script type="application/ld+json">{"value":NaN}</script><p>Usable text</p>'
            )
        self.assertEqual(result["text"]["main"], "Usable text")
        self.assertEqual(result["text"]["main_method"], "visible_text_fallback")
        self.assertEqual(len(result["errors"]), 2)
        json.dumps(result, allow_nan=False)


class ObservationCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_rendered_observations_survive_both_retention_modes_and_import(
        self,
    ):
        @asynccontextmanager
        async def browser():
            async def navigate(url, **kwargs):
                return PageCapture(
                    url=URL,
                    html=HTML,
                    cleaned_html="<h1>Work with us</h1>",
                    status_code=200,
                    headers={
                        "Server": "nginx",
                        "Set-Cookie": "private=session",
                    },
                    metadata={},
                    links=[],
                    error=None,
                )

            yield SimpleNamespace(navigate=navigate)

        for retain in [True, False]:
            with self.subTest(retain=retain), TemporaryDirectory() as temporary:
                root = Path(temporary) / "crawl"
                with (
                    patch("company_research.crawl.open_browser", browser),
                    patch(
                        "company_research.page_observations.collect_technology_observations",
                        side_effect=AssertionError("Technology extraction disabled"),
                    ),
                ):
                    manifest = await crawl_company(
                        URL, output_dir=root, pages=[URL], save_artifacts=retain
                    )
                self.assertEqual(manifest["usage"]["calls"], 0)
                result = json.loads((root / "result.json").read_text())
                observations = result["documents"][0]["input"]["observations"]
                self.assertEqual(result["documents"][0]["rendered_html"], HTML)
                self.assertIsNone(observations["trackers"])
                self.assertIsNone(observations["resources"])
                self.assertEqual(observations["metadata"]["title"], "Example careers")
                self.assertEqual(observations["response_headers"], {"server": "nginx"})
                self.assertEqual(observations["representation"], "rendered_html")
                row = result_row(result, source_path="test")
                self.assertEqual(json.loads(row["page_observations"]), [observations])
                self.assertIsNone(row["jobs"])
                with open_crawl(root / "result.json") as (_, folders):
                    self.assertEqual(
                        PageInput.load(folders[0]).observations, observations
                    )
                if not retain:
                    self.assertEqual([p.name for p in root.iterdir()], ["result.json"])
                observations["contacts"] = []
                (root / "result.json").write_text(json.dumps(result))
                with (
                    self.assertRaisesRegex(ValueError, "observations hash"),
                    open_crawl(root / "result.json"),
                ):
                    pass

    async def test_old_captures_load_without_fabricating_missing_observations(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "page.html").write_text("<p>Old</p>")
            (root / "input.json").write_text(
                json.dumps(
                    {
                        "page": {"html_sha256": content_hash("<p>Old</p>")},
                        "target_url": URL,
                        "links": [],
                        "headings": [],
                    }
                )
            )
            self.assertIsNone(PageInput.load(root).observations)
