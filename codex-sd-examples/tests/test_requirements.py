import unittest

from ex1.models import (
    CompanyInformation,
    Contact,
    Evidence,
    Job,
    OtherFact,
    Product,
    UsefulInformation,
)
from ex3.requirements import TARGET_FIELD_KEYS, compute_gaps, requirements_text

EVIDENCE = Evidence(text="x", source_url="https://example.com/")


class GapComputationTest(unittest.TestCase):
    def test_everything_is_missing_for_an_empty_result(self) -> None:
        gaps = compute_gaps(UsefulInformation())

        self.assertEqual({gap.field for gap in gaps}, TARGET_FIELD_KEYS)
        self.assertTrue(all(gap.status == "missing" for gap in gaps))

    def test_filled_fields_are_not_reported_and_short_descriptions_are_weak(
        self,
    ) -> None:
        information = UsefulInformation(
            contacts=[
                Contact(type="phone", value="+46 8 1", evidence=EVIDENCE),
                Contact(type="email", value="a@example.com", evidence=EVIDENCE),
                Contact(type="address", value="Kungsgatan 1", evidence=EVIDENCE),
                Contact(
                    type="social",
                    value="https://linkedin.com/company/x",
                    evidence=EVIDENCE,
                ),
            ],
            company=CompanyInformation(
                name="Example",
                legal_name="Example AB",
                description="Short.",
                industries=["Banking"],
                identifiers=["Organisation number: 502007-7862"],
            ),
            products=[Product(name="Loans", evidence=EVIDENCE)],
            jobs=[Job(title="Engineer", evidence=EVIDENCE)],
            other_facts=[
                OtherFact(
                    name="Established", value="Established in 1871", evidence=EVIDENCE
                ),
                OtherFact(
                    name="Employees", value="Over 12,000 employees", evidence=EVIDENCE
                ),
                OtherFact(name="CEO", value="Michael Green", evidence=EVIDENCE),
                OtherFact(
                    name="Swedish subsidiaries",
                    value="Stadshypotek AB",
                    evidence=EVIDENCE,
                ),
            ],
        )

        gaps = {gap.field: gap for gap in compute_gaps(information)}

        self.assertEqual(set(gaps), {"description"})
        self.assertEqual(gaps["description"].status, "weak")

    def test_requirements_text_lists_every_field_once(self) -> None:
        text = requirements_text()

        for key in TARGET_FIELD_KEYS:
            self.assertEqual(text.count(f"- {key}:"), 1)


class GapPatternTest(unittest.TestCase):
    def test_generic_words_do_not_close_the_management_gap(self) -> None:
        for value in (
            "Local account management",
            "asset management, financing",
            "Asset Management team",
            "Career areas: compliance, law, audit",
            "dashboard",
        ):
            with self.subTest(value=value):
                self.assertIn("management", _open_gaps(("Fact", value)))

    def test_role_terms_close_the_management_gap(self) -> None:
        for value in (
            "CEO Michael Green",
            "Board of directors",
            "Styrelsen best\u00e5r av",
            "Verkst\u00e4llande direkt\u00f6r Anna",
        ):
            with self.subTest(value=value):
                self.assertNotIn("management", _open_gaps(("Fact", value)))

    def test_group_words_are_word_bounded(self) -> None:
        self.assertIn("group_structure", _open_gaps(("Place", "Brandenburg")))
        self.assertNotIn(
            "group_structure", _open_gaps(("Owner", "wholly owned subsidiary"))
        )
        self.assertNotIn("group_structure", _open_gaps(("Name", "Handelsbanken Group")))

    def test_founding_year_needs_both_signals_in_one_fact(self) -> None:
        self.assertNotIn("founded_year", _open_gaps(("History", "Founded 1871")))
        self.assertNotIn("founded_year", _open_gaps(("History", "Established in 1871")))
        self.assertIn(
            "founded_year",
            _open_gaps(
                ("History", "The bank was founded by merchants"),
                ("Award", "Best bank 2019"),
            ),
        )

    def test_employee_count_accepts_employs_and_needs_one_fact(self) -> None:
        self.assertNotIn("employee_count", _open_gaps(("Size", "employs 500 people")))
        self.assertNotIn(
            "employee_count", _open_gaps(("Size", "Drygt 12 000 medarbetare"))
        )
        self.assertIn(
            "employee_count",
            _open_gaps(("Size", "Many employees"), ("Offices", "12 000 branches")),
        )


def _open_gaps(*facts: tuple[str, str]) -> set[str]:
    information = UsefulInformation(
        other_facts=[
            OtherFact(name=name, value=value, evidence=EVIDENCE)
            for name, value in facts
        ]
    )
    return {gap.field for gap in compute_gaps(information)}


if __name__ == "__main__":
    unittest.main()
