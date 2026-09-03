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


if __name__ == "__main__":
    unittest.main()
