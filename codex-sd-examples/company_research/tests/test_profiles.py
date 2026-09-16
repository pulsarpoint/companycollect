import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_package import page, response

from company_research.content import HtmlWindow, source_finding
from company_research.llm import ModelClient
from company_research.models import CertificationClaim, ResearchConfig
from company_research.profiles import (
    classify_site,
    profile_objectives,
    validate_summary,
)


class ProfileHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_classification_retries_reconstructed_quotes_and_preserves_both_attempts(
        self,
    ):
        html = "<h1>DemoWorks</h1><h2>Services</h2><a>Semiconductors</a>"
        attempts = []

        def serve(request):
            body = json.loads(request.content)
            attempts.append(body)
            return httpx.Response(
                200,
                json=response(
                    {
                        "site_types": ["company"],
                        "research_profiles": ["service_provider"],
                        "purpose": "Engineering services",
                        "operator_name": "DemoWorks",
                        "business_activities": ["Semiconductors"],
                        "evidence": ["DemoWorks", "Services: Semiconductors"]
                        if len(attempts) == 1
                        else ["DemoWorks", "Services", "Semiconductors"],
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://example.test/", transport=httpx.MockTransport(serve)
            ) as client:
                llm = ModelClient(
                    client, "test-key", ResearchConfig(provider=None), root
                )
                finding = await classify_site(
                    HtmlWindow(0, len(html), html), page(html), [], llm, root
                )
            self.assertEqual(finding.evidence_status, "source_matched")
            first = json.loads(
                (root / "classification/p0001-0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(first["evidence_status"], "needs_review")
            self.assertEqual(len(attempts), 2)
            self.assertNotIn("only", attempts[0]["provider"])
            self.assertTrue(attempts[0]["provider"]["allow_fallbacks"])


class ProfileTests(unittest.TestCase):
    def test_certification_keeps_scope_and_does_not_claim_document_verification(self):
        html = '<p>DemoWorks hosting is ISO 27001 certified.</p><a href="/certificate.pdf">Certificate</a>'
        claim = CertificationClaim.model_validate(
            {
                "subject_name": "DemoWorks",
                "subject_kind": "service",
                "standard_name": "ISO 27001",
                "standard_version": None,
                "claim_type": "certification",
                "scope": "hosting",
                "issuer_or_assessor": None,
                "certificate_or_report_id": None,
                "issued_on": None,
                "valid_until": None,
                "document_url": "/certificate.pdf",
                "document_type": "certificate",
                "evidence": ["DemoWorks hosting is ISO 27001 certified."],
            }
        )
        finding = source_finding(
            "certifications_compliance",
            claim.model_dump(),
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(finding.evidence_status, "source_matched")
        self.assertEqual(finding.data["verification_level"], "website_claim")
        self.assertFalse(finding.data["document_examined"])
        self.assertEqual(finding.data["scope"], "hosting")
        changed = claim.model_dump() | {
            "subject_name": "Parent Company",
            "document_url": "/invented.pdf",
        }
        invalid = source_finding(
            "certifications_compliance",
            changed,
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(invalid.evidence_status, "needs_review")
        self.assertIn("document_url_absent", invalid.sources[0].issues)
        self.assertIn("subject_name_not_in_evidence", invalid.sources[0].issues)

    def test_service_profile_prioritizes_credentials_but_keeps_secondary_objectives(
        self,
    ):
        html = "DemoWorks provides engineering services."
        finding = source_finding(
            "site_classification",
            {"research_profiles": ["service_provider"], "evidence": [html]},
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        objectives = profile_objectives(finding)
        self.assertLess(
            objectives.index("certifications_compliance"), objectives.index("jobs")
        )
        self.assertIn("people", objectives)
        self.assertIn("technology_signals", objectives)

    def test_summary_links_are_derived_from_validated_record_ids(self):
        html = "DemoWorks provides engineering services."
        finding = source_finding(
            "company_profile",
            {
                "company": "DemoWorks",
                "field": "description",
                "value": html,
                "as_of": None,
                "evidence": [html],
            },
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        document = {
            "site_description": None,
            "company_name": None,
            "company_description": {"text": html, "record_ids": [finding.record_id]},
            "products_services": [],
            "industries": [],
            "company_relationships": [],
            "certifications_compliance": [],
        }
        overview = validate_summary(document, {finding.record_id: finding})
        self.assertEqual(
            overview["company_description"]["source_urls"], [page(html).source_url]
        )
        document["company_description"]["record_ids"] = ["invented"]
        with self.assertRaisesRegex(ValueError, "absent"):
            validate_summary(document, {finding.record_id: finding})
