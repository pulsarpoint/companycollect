import unittest

from ex3.selection import (
    SelectionFilter,
    SelectionScorer,
    apply_head_metadata,
    assess_url,
    rank_urls,
)

BASE_URL = "https://www.example.se/en/"


class UrlScoringTest(unittest.TestCase):
    def test_homepage_about_and_contact_outrank_legal_pages(self) -> None:
        eligible, _ = rank_urls(
            [
                "https://www.example.se/en/about-us/legal-documents/privacy-notice",
                "https://www.example.se/en/about-us/legal-documents/cookies",
                "https://www.example.se/en/personal/contact-and-support",
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/",
            ],
            base_url=BASE_URL,
        )

        self.assertEqual(
            [assessment.url for assessment in eligible][:3],
            [
                "https://www.example.se/en/",
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/personal/contact-and-support",
            ],
        )
        self.assertLess(eligible[-1].score, eligible[2].score)
        self.assertIn("homepage", eligible[0].reasons)

    def test_excludes_other_locale_prefixes_but_rewards_the_base_locale(self) -> None:
        swedish = assess_url(
            "https://www.example.se/sv/om-oss/digital-tillganglighet",
            base_url=BASE_URL,
        )
        english = assess_url("https://www.example.se/en/careers", base_url=BASE_URL)
        german_host = assess_url("https://de.example.se/karriere", base_url=BASE_URL)
        query_locale = assess_url(
            "https://www.example.se/company?lang=fi",
            base_url=BASE_URL,
        )
        regional_english = assess_url(
            "https://www.example.se/en-gb/careers",
            base_url=BASE_URL,
        )

        self.assertEqual(swedish.exclusion, "locale prefix 'sv'")
        self.assertIsNone(english.exclusion)
        self.assertIn("base locale", english.reasons)
        self.assertEqual(german_host.exclusion, "locale subdomain 'de'")
        self.assertEqual(query_locale.exclusion, "locale query 'fi'")
        self.assertIsNone(regional_english.exclusion)

    def test_excludes_binary_files_and_external_domains(self) -> None:
        pdf = assess_url(
            "https://www.example.se/en/reports/annual-report-2025.pdf",
            base_url=BASE_URL,
        )
        image = assess_url("https://www.example.se/logo.PNG", base_url=BASE_URL)
        external = assess_url("https://www.example.com/en/about", base_url=BASE_URL)
        subdomain = assess_url("https://jobs.example.se/", base_url=BASE_URL)

        self.assertEqual(pdf.exclusion, "file extension '.pdf'")
        self.assertEqual(image.exclusion, "file extension '.png'")
        self.assertEqual(external.exclusion, "external domain 'example.com'")
        self.assertIsNone(subdomain.exclusion)

    def test_penalizes_locators_accounts_query_strings_and_deep_numeric_paths(
        self,
    ) -> None:
        locator = assess_url("https://www.example.se/en/find-branch", base_url=BASE_URL)
        login = assess_url("https://www.example.se/en/login", base_url=BASE_URL)
        filtered = assess_url(
            "https://www.example.se/en/news?page=3",
            base_url=BASE_URL,
        )
        article = assess_url(
            "https://www.example.se/en/news/2024/05/some-article-title",
            base_url=BASE_URL,
        )
        news_index = assess_url("https://www.example.se/en/news", base_url=BASE_URL)
        neutral = assess_url("https://www.example.se/en/misc", base_url=BASE_URL)

        self.assertLess(locator.score, neutral.score)
        self.assertLess(login.score, neutral.score)
        self.assertLess(filtered.score, news_index.score)
        self.assertLess(article.score, news_index.score)
        self.assertIn("branch locator", locator.reasons)
        self.assertIn("query string", filtered.reasons)

    def test_section_prefixes_count_less_than_the_page_slug(self) -> None:
        about = assess_url("https://www.example.se/en/about-us", base_url=BASE_URL)
        lei = assess_url(
            "https://www.example.se/en/about-us/legal-documents/lei",
            base_url=BASE_URL,
        )
        contact = assess_url(
            "https://www.example.se/en/personal/contact-and-support",
            base_url=BASE_URL,
        )
        card_reader = assess_url(
            "https://www.example.se/en/personal/contact-and-support/help-and-support/check-card-reader",
            base_url=BASE_URL,
        )
        corporate_financing = assess_url(
            "https://www.example.se/en/corporate/financing",
            base_url=BASE_URL,
        )
        subsidiaries = assess_url(
            "https://www.example.se/en/about-us/swedish-subsidiaries",
            base_url=BASE_URL,
        )

        self.assertGreater(about.score, lei.score + 20)
        self.assertGreater(contact.score, card_reader.score + 20)
        self.assertNotIn("about", corporate_financing.reasons)
        self.assertIn("about", subsidiaries.reasons)
        self.assertNotIn("deep path", about.reasons)
        self.assertIn("deep path", card_reader.reasons)

    def test_rewards_urls_linked_from_the_base_page(self) -> None:
        plain = assess_url("https://www.example.se/en/misc", base_url=BASE_URL)
        linked = assess_url(
            "https://www.example.se/en/misc",
            base_url=BASE_URL,
            linked_from_base={"https://www.example.se/en/misc/"},
        )

        self.assertGreater(linked.score, plain.score)
        self.assertIn("linked from base page", linked.reasons)

    def test_head_metadata_excludes_non_english_and_rewards_relevant_titles(
        self,
    ) -> None:
        branch = assess_url("https://www.example.se/alingsas", base_url=BASE_URL)
        management = assess_url(
            "https://www.example.se/en/organisation",
            base_url=BASE_URL,
        )

        excluded = apply_head_metadata(
            branch,
            language="sv",
            title="Handelsbanken Alingsås",
            description=None,
        )
        boosted = apply_head_metadata(
            management,
            language="en",
            title="Our management team and board",
            description="Meet the executives leading the company.",
        )
        unknown = apply_head_metadata(
            management,
            language=None,
            title=None,
            description=None,
        )
        generic = apply_head_metadata(
            management,
            language="en",
            title="Security for corporates | Example Group",
            description="Services from the company.",
        )

        self.assertEqual(excluded.exclusion, "document language 'sv'")
        self.assertEqual(excluded.language, "sv")
        self.assertGreater(boosted.score, management.score)
        self.assertIn("title mentions people", boosted.reasons)
        self.assertEqual(boosted.title, "Our management team and board")
        self.assertEqual(unknown.score, management.score)
        self.assertEqual(generic.score, management.score)

    def test_ranking_is_deterministic_for_equal_scores(self) -> None:
        eligible, excluded = rank_urls(
            [
                "https://www.example.se/en/zeta",
                "https://www.example.se/en/alpha",
                "https://www.example.se/en/zeta",
                "https://www.example.se/sv/alpha",
            ],
            base_url=BASE_URL,
        )

        self.assertEqual(
            [assessment.url for assessment in eligible],
            ["https://www.example.se/en/alpha", "https://www.example.se/en/zeta"],
        )
        self.assertEqual(
            [assessment.url for assessment in excluded],
            ["https://www.example.se/sv/alpha"],
        )


class Crawl4aiAdapterTest(unittest.TestCase):
    def test_scorer_and_filter_follow_the_assessment(self) -> None:
        scorer = SelectionScorer(base_url=BASE_URL)
        url_filter = SelectionFilter(
            base_url=BASE_URL,
            exclude_urls={"https://www.example.se/en/about-us"},
        )

        self.assertGreater(
            scorer.score("https://www.example.se/en/about-us"),
            scorer.score("https://www.example.se/en/privacy"),
        )
        self.assertLess(scorer.score("https://www.example.se/sv/om-oss"), 0)
        self.assertTrue(url_filter.apply("https://www.example.se/en/contact"))
        self.assertFalse(url_filter.apply("https://www.example.se/sv/om-oss"))
        self.assertFalse(url_filter.apply("https://www.example.se/en/about-us/"))


if __name__ == "__main__":
    unittest.main()
