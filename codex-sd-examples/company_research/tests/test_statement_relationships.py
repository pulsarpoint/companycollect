"""One source statement can establish multiple independently checked relationships."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from test_statements import decisions, save_page, statement, technology

from company_research.content import HtmlWindow
from company_research.statements import (
    accept_normalization_items,
    accept_statement_decisions,
    statement_finding,
)


class StatementRelationshipTests(unittest.TestCase):
    def test_job_scope_conversion_merges_created_duplicates_only(self):
        html = "<h1>DemoWorks</h1><h2>Engineer</h2><p>Uses Python; Python experience required.</p>"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    source_name="Python",
                    job_title="Engineer",
                    section_heading=None,
                    evidence=["DemoWorks", "Engineer", "Python"],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            relationships = [
                technology(signal="stated_use", scope="company"),
                technology(signal="stated_use", scope="team"),
                technology(signal="stated_use", scope="role"),
                technology(signal="required_experience", scope="role"),
            ]
            findings, _, _, issues = accept_statement_decisions(
                decisions(*relationships), {"s1": record}, {page.page_id: page}, root
            )
            self.assertEqual(issues, {})
            self.assertEqual(len(findings), 2)
            self.assertEqual(
                {finding.data["signal"] for finding in findings},
                {"stated_use", "required_experience"},
            )
            self.assertTrue(
                all(finding.data["scope"] == "role" for finding in findings)
            )
            for finding in findings:
                self.assertEqual(finding.data["statement_ids"], [record.record_id])
                self.assertEqual(finding.sources[0].url, page.source_url)
            findings, _, _, issues = accept_statement_decisions(
                decisions(*relationships, relationships[0]),
                {"s1": record},
                {page.page_id: page},
                root,
            )
            self.assertEqual(findings, [])
            self.assertEqual(set(issues), {"s1"})

    def test_usage_and_preferred_experience_retain_same_employer_job_and_sources(self):
        html = (
            "<h1>DemoWorks</h1><h2>Engineer</h2>"
            "<p>Responsibilities: maintain MCAP logging.</p>"
            "<p>Nice to have: MCAP experience.</p>"
        )
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    source_name="MCAP",
                    job_title="Engineer",
                    section_heading=None,
                    context="The Engineer maintains MCAP logging; MCAP experience is preferred.",
                    qualifiers=["Responsibilities", "Nice to have"],
                    evidence=[
                        "DemoWorks",
                        "Engineer",
                        "Responsibilities: maintain MCAP logging.",
                        "Nice to have: MCAP experience.",
                    ],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            findings, _, _, issues = accept_statement_decisions(
                decisions(
                    technology(signal="stated_use", scope="company"),
                    technology(signal="preferred_experience", scope="role"),
                ),
                {"s1": record},
                {page.page_id: page},
                root,
            )
            self.assertEqual(issues, {})
            self.assertEqual(
                {r.data["signal"] for r in findings},
                {"stated_use", "preferred_experience"},
            )
            self.assertEqual(len({r.record_id for r in findings}), 2)
            for finding in findings:
                self.assertEqual(finding.data["scope"], "role")
                self.assertEqual(finding.data["company"], "DemoWorks")
                self.assertEqual(finding.data["job_title"], "Engineer")
                self.assertEqual(finding.data["technology"], "MCAP")
                self.assertEqual(finding.data["statement_ids"], [record.record_id])
                self.assertEqual(finding.sources[0].url, page.source_url)
                self.assertEqual(finding.data["context"], record.data["context"])

    def test_duplicate_relationship_is_held_but_other_statement_survives(self):
        html = "<h1>DemoWorks</h1><h2>Engineering skills</h2><p>Python Go</p>"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            records = {
                key: statement_finding(
                    statement(source_name=name, evidence=[name]),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                for key, name in [("s1", "Python"), ("s2", "Go")]
            }
            findings, _, _, issues = accept_statement_decisions(
                decisions(technology(), technology(), technology(statement_ids=["s2"])),
                records,
                {page.page_id: page},
                root,
            )
            self.assertEqual([r.data["technology"] for r in findings], ["Go"])
            self.assertEqual(set(issues), {"s1"})

    def test_one_bad_relationship_does_not_hide_failure_or_remove_good_relationship(
        self,
    ):
        html = "<h1>DemoWorks</h1><h2>Engineering skills</h2><p>Python</p>"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(), page, HtmlWindow(0, len(html), html)
            )
            for values in [
                [technology(signal="invalid"), technology()],
                [technology(), technology(signal="invalid")],
            ]:
                findings, _, _, issues = accept_normalization_items(
                    {"technologies": values, "certifications": [], "exclusions": []},
                    {"s1": record},
                    {page.page_id: page},
                    root,
                )
                self.assertEqual(len(findings), 1)
                self.assertEqual(set(issues), {"s1"})
