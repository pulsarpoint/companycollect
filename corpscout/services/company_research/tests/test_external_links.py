"""External link provenance, context boundaries, and model HTTP validation."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_package import page, response

from company_research.discovery import CrawlQueue
from company_research.external_links import (
    assess_external_links,
    collect_external_links,
    context_payload,
)
from company_research.llm import ModelClient
from company_research.models import ResearchConfig

HTML = """<html><body>
<header><nav aria-label="Our businesses"><a href="https://nova.example/labs?utm_source=group&amp;x=1#team"><img alt="Nova Labs" src="logo.png"></a></nav></header>
<main><section><h2>Implementation partners</h2><p>Our implementation partner <a href="https://nova.example/labs?utm_source=group&amp;x=1#team">Nova Labs</a> supports deployment.</p></section>
<a href="https://source.example/docs/">Our documentation</a>
<a href="mailto:hello@nova.example">Email Nova</a>
<a href="tel:020411212">020-41 12 12</a><a href="tel:+4684112122">Call</a>
<a href="javascript:alert(1)">Open</a><a href="https://user:secret@nova.example">Private</a>
<script>Our parent is Fake Corp <a href="https://fake.example">Fake</a></script>
</main><footer><p>Part of <a href="//group.example/">Group One</a>.</p>
<a href="https://www.linkedin.com/company/source/?trk=footer#about">LinkedIn</a>
<a href="https://reports.example/annual.pdf?version=2#page=4">Annual report</a>
<a href="https://accounts.example/login">Account</a></footer></body></html>"""


def observations(html: str = HTML, url: str = "https://source.example/about"):
    return collect_external_links(
        html,
        page(html, url),
        html_file="link-html/p0001.html",
        html_kind="rendered_html",
    )


class ExternalLinkCollectionTests(unittest.TestCase):
    def test_occurrences_full_urls_regions_and_unfollowed_destinations(self):
        links = observations()
        self.assertEqual(len(links), 6)
        header, partner, group, social, document, account = links
        self.assertEqual(
            header.url, "https://nova.example/labs?utm_source=group&x=1#team"
        )
        self.assertEqual(header.url, partner.url)
        self.assertNotEqual(header.link_id, partner.link_id)
        self.assertEqual(header.page_region, "header")
        self.assertEqual(header.image_alt, ["Nova Labs"])
        self.assertEqual(header.section_heading, "Our businesses")
        self.assertEqual(partner.page_region, "main")
        self.assertEqual(partner.section_heading, "Implementation partners")
        self.assertEqual(
            partner.surrounding_text,
            "Our implementation partner Nova Labs supports deployment.",
        )
        self.assertEqual(group.page_region, "footer")
        self.assertIsNone(group.section_heading)  # Not the earlier partners section.
        self.assertEqual(group.url, "https://group.example/")
        self.assertEqual(group.source_url, "https://source.example/about")
        self.assertEqual(
            social.url, "https://www.linkedin.com/company/source/?trk=footer#about"
        )
        self.assertIn("#page=4", document.url)
        queue = CrawlQueue("https://source.example/", ResearchConfig())
        for link in links:
            queue.add(
                link.url,
                source=link.source_url,
                label=link.anchor_text,
                context=context_payload(link),
            )
        self.assertNotIn(social.url, queue.candidates)
        self.assertNotIn(account.url, queue.candidates)
        self.assertEqual(len(queue.document_candidates), 1)
        candidate = queue.candidates["https://nova.example/labs?x=1"]
        self.assertEqual(len(candidate.prompt_data()["link_contexts"]), 2)
        self.assertEqual(len(links), 6)  # Queue exclusions never delete observations.

    def test_redirect_base_url_private_tenants_and_deterministic_ids(self):
        html = '<base href="https://vendor.example/products/"><a href="one?q=1#two">One</a><a href="https://beta.github.io">Other tenant</a>'
        source = page(html, "https://old.example/")
        source.source_url = "https://alpha.github.io/landing"
        links = collect_external_links(
            html, source, html_file="raw.html", html_kind="rendered_html"
        )
        again = collect_external_links(
            html, source, html_file="raw.html", html_kind="rendered_html"
        )
        self.assertEqual(links, again)
        self.assertEqual(links[0].url, "https://vendor.example/products/one?q=1#two")
        self.assertEqual(links[0].source_url, source.source_url)
        self.assertEqual(links[1].source_domain, "alpha.github.io")
        self.assertEqual(links[1].destination_domain, "beta.github.io")

    def test_metadata_fallback_has_no_invented_dom_context(self):
        links = collect_external_links(
            "<p>Source</p>",
            page("<p>Source</p>", "https://source.example/"),
            html_file="cleaned.html",
            html_kind="cleaned_html",
            supplemental_links=[
                {"href": "https://other.example/", "text": "Other"},
                {"href": "https://other.example/", "text": "Other"},
            ],
        )
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].extraction_method, "browser_links")
        self.assertIsNone(links[0].surrounding_text)
        self.assertEqual(links[0].page_region, "unknown")

    def test_subdomains_of_registered_site_are_internal(self):
        links = observations(
            '<a href="https://docs.example.com">Docs</a><a href="https://other.net">Other</a>',
            "https://www.example.com",
        )
        self.assertEqual([link.destination_domain for link in links], ["other.net"])

    def test_browser_normalized_url_does_not_duplicate_dom_occurrences(self):
        links = collect_external_links(
            HTML,
            page(HTML, "https://source.example/about"),
            html_file="raw.html",
            html_kind="rendered_html",
            supplemental_links=[
                {"href": "https://nova.example/labs?x=1", "text": "Nova Labs"}
            ],
        )
        nova = [link for link in links if link.destination_host == "nova.example"]
        self.assertEqual(len(nova), 2)
        self.assertTrue(
            all(link.url.endswith("utm_source=group&x=1#team") for link in nova)
        )

    def test_large_context_is_bounded_and_marked(self):
        links = observations(
            "<p>"
            + "before " * 500
            + '<a href="https://other.example">Other</a>'
            + " after" * 500
            + "</p>"
        )
        self.assertTrue(links[0].context_truncated)
        self.assertLessEqual(len(links[0].surrounding_text or ""), 1600)
        self.assertIn("Other", links[0].surrounding_text or "")


class ExternalLinkAssessmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_assessment_preserves_evidence_and_budget_pending(self):
        links = observations()

        def handle(request):
            payload = json.loads(request.content)
            prompt = payload["messages"][-1]["content"]
            data = json.loads(prompt.split("INPUT DATA:\n", 1)[1])
            self.assertEqual(data["links"][1]["url"], links[1].url)
            return httpx.Response(
                200,
                json=response(
                    {
                        "assessments": [
                            {
                                "link_id": links[0].link_id,
                                "relationship": "other_business",
                                "basis": "explicit_text",
                                "related_entity_name": "Nova Labs",
                                "description": "Listed under the source's businesses; ownership unspecified.",
                                "evidence": ["Our businesses", "Nova Labs"],
                            },
                            {
                                "link_id": links[1].link_id,
                                "relationship": "partner",
                                "basis": "explicit_text",
                                "related_entity_name": "Nova Labs",
                                "description": "Named implementation partner.",
                                "evidence": ["Our implementation partner", "Nova Labs"],
                            },
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://openrouter.ai/api/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "test-key",
                    ResearchConfig(
                        external_link_batch_size=2, max_external_link_assessment_calls=1
                    ),
                    root,
                )
                await assess_external_links(links, llm, root)
            self.assertEqual(
                [link.assessment_status for link in links],
                [
                    "assessed",
                    "assessed",
                    "not_assessed",
                    "not_assessed",
                    "not_assessed",
                    "not_assessed",
                ],
            )
            saved = json.loads(
                (root / "external-links.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(saved), 6)
            self.assertEqual(saved[0]["url"], links[0].url)

    async def test_missing_duplicate_and_unsupported_identity_do_not_become_facts(self):
        links = observations()[:4]
        valid = {
            "link_id": links[0].link_id,
            "relationship": "parent_company",
            "basis": "explicit_text",
            "related_entity_name": "Invented Holdings",
            "description": "Invented ownership",
            "evidence": ["Nova Labs"],
        }
        duplicated = valid | {"link_id": links[1].link_id}
        fabricated_quote = valid | {
            "link_id": links[2].link_id,
            "related_entity_name": "Group One",
            "evidence": ["Wholly owned by Group One"],
        }
        document = {
            "assessments": [
                valid,
                duplicated,
                duplicated,
                fabricated_quote,
                valid | {"link_id": "invented-id"},
            ]
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://openrouter.ai/api/v1/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json=response(document))
                ),
            ) as client:
                llm = ModelClient(client, "test-key", ResearchConfig(), root)
                await assess_external_links(links, llm, root)
            self.assertEqual(
                [link.assessment_status for link in links],
                ["needs_review", "failed", "needs_review", "failed"],
            )
            self.assertEqual(
                len(
                    json.loads(
                        (root / "external-links.json").read_text(encoding="utf-8")
                    )
                ),
                4,
            )

    async def test_http_failure_does_not_erase_inventory(self):
        links = observations()[:1]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://openrouter.ai/api/v1/",
                transport=httpx.MockTransport(lambda _: httpx.Response(503)),
            ) as client:
                llm = ModelClient(
                    client, "test-key", ResearchConfig(max_http_attempts=1), root
                )
                await assess_external_links(links, llm, root)
            self.assertEqual(links[0].assessment_status, "failed")
            self.assertEqual(
                len(
                    json.loads(
                        (root / "external-links.json").read_text(encoding="utf-8")
                    )
                ),
                1,
            )
