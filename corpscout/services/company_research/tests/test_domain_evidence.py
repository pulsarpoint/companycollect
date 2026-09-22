import copy
import json
import unittest

from company_research.clickhouse_results import result_row
from company_research.domain_evidence import external_domain_evidence


def capture():
    return {"schema_version": "company-crawl-result/1.2", "crawl": {"input_url": "https://initial.com", "status": "finished"},
            "documents": [{"input": {"page": {"page_id": "p1", "source_url": "https://www.actual.com/partners",
                                              "fetched_at": "2026-09-18T00:00:00Z", "link_html_sha256": "abc"},
                                      "links": [{"link_id": "l1", "url": "https://nova.com", "anchor_text": "Nova",
                                                 "kind": "url", "inventory_source": "rendered_html",
                                                 "context": {"surrounding_text": "Nova supplies us.", "context_truncated": False,
                                                             "section_heading": "Suppliers", "dom_path": ["main", "p"]}}]}}]}


class DomainEvidenceTests(unittest.TestCase):
    def test_import_preserves_occurrences_with_actual_source_not_crawl_target(self):
        payload = capture()
        before = copy.deepcopy(payload)
        row = result_row(payload, source_path="s3/capture")
        evidence = json.loads(row["external_links"])[0]
        self.assertEqual(evidence["source_host"], "actual.com")
        self.assertEqual(evidence["link_id"], "p1:l1")
        self.assertEqual(evidence["surrounding_text"], "Nova supplies us.")
        self.assertEqual(evidence["html_sha256"], "abc")
        self.assertEqual(evidence["destination_domain"], "nova.com")
        self.assertEqual(payload, before)
        self.assertEqual(json.loads(row["documents"]), payload["documents"])

    def test_internal_links_are_not_relationships_but_private_tenants_are_distinct(self):
        payload = capture()
        page = payload["documents"][0]["input"]
        page["page"]["source_url"] = "https://one.github.io"
        link = page["links"][0]
        page["links"] = [link | {"url": url} for url in ["https://one.github.io/internal", "https://two.github.io", "javascript:alert(1)", "https://user:password@nova.com"]]
        self.assertEqual([link["destination_domain"] for link in external_domain_evidence(payload)], ["two.github.io"])

    def test_legacy_links_preserved_and_missing_inventory_differs_from_empty(self):
        link = {"url": "https://nova.com", "source_url": "https://actual.com", "surrounding_text": "Nova", "custom_field": "preserved"}
        evidence = external_domain_evidence({"external_links": [link]})[0]
        self.assertEqual(evidence["custom_field"], "preserved")
        self.assertIsNone(external_domain_evidence({}))
        self.assertEqual(external_domain_evidence({"external_links": []}), [])
