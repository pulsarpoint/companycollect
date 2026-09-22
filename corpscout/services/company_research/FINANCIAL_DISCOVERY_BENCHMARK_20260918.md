# Financial discovery on additional domains — 18 September 2026

The crawler found useful acquisition, financing and merger sources for TrustMotion
(formerly TTTech Auto) and RT-RK. Automatic discovery missed Nordic's published
annual report, although a separate direct-page test captured its link immediately.
Melexis blocked the initial fetch. These results expose discovery and relevance
problems that the NOVELIC test alone did not show.

All outputs are local. Company research v0.30.0 was unchanged throughout these tests.
No financial amounts were interpreted and no PDF contents were analyzed.

## Automatic benchmark

The user requested three varied companies. Melexis, TTTech Auto and RT-RK were
selected; Nordic Semiconductor was added after Melexis blocked collection.

Every automatic run used the same financial-only instructions and limits:
DeepSeek `deepseek-flash` with high reasoning, 24 pages, 12 external pages,
40 model calls, three searches, three approved source domains, four pages per
approved source domain and source depth three. Runs were sequential.
No investor URLs, parent domains or search results were manually seeded.

| Input domain | Captured / attempted pages | Elapsed | LLM calls | Total tokens | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| melexis.com | 0 / 1 | 7.492 s | 0 | 0 | Needs review: initial Cloudflare challenge, HTTP 307 then 403 |
| tttech-auto.com → trustmotion.com | 24 / 24 | 559.593 s (9m20s) | 26 | 280,203 | Partial: page limit |
| rt-rk.com | 23 / 24 | 663.543 s (11m04s) | 26 | 295,420 | Partial: page limit; search failures and blocked investor portal |
| nordicsemi.com | 1 / 3 | 99.280 s (1m39s) | 7 | 104,450 | Partial: no matching candidates after discovery failures |

Total: **680,073 tokens**, 59 model calls and 48 captured pages. The sum of crawl
elapsed times is 22m10s, excluding gaps between runs and the separate diagnostic.
The provider returned no dollar costs. Different sites and access outcomes make
these measurements operational results, not a controlled speed ranking.

TrustMotion used two successful searches. RT-RK's two searches and Nordic's three
searches returned HTTP 429: **five failures across seven searches**, despite
sequential execution. Melexis stopped before searching or calling the model.

## Sources found

### TrustMotion / TTTech Auto

The initial URL redirected to TrustMotion. The crawler recognized the current
company and searched its Austrian legal name, reaching registry records under
both current and former names.

Useful captures include:

- [Official publication of TTTech Auto's 2024 annual accounts](https://www.evi.gv.at/b/pi/bml-glx).
  The captured page names the attachment `060625_TTTech Auto AG_2024.pdf`.
- [NXP's acquisition announcement](https://www.nxp.com/company/about-nxp/newsroom/NW-NXP-ACCELERATES-THE-TRANSFORMATION).
  Its [German PDF reference](https://www.nxp.com/docs/en/supporting-information/NW-NXP-ACCELERATES-THE-TRANSFORMATION-GER.pdf)
  was retained as a generic document link; its body was not examined.
- [European Investment Bank financing announcement](https://www.trustmotion.com/newsroom/european-investment-bank-provides-tttech-auto-eu30-million-advance-its-leading-safety)
  and other target-site acquisition/investment announcements.

The Austrian attachment appears as a styled `<span>` without an `href`; client
application data contains route information. The capture preserves the filename
and surrounding publication text, but the crawler did not resolve a download URL.
This is a concrete gap in handling JavaScript download controls.

The run retained **15 generic document references**, mostly policies/certificates
and corporate material. They are not 15 financial statements. Some announcement
captures are translations, so page counts also overstate distinct disclosures.

[Portable result](data/multi-domain-financial-discovery-20260918/tttech-auto/result.json)
· [Manifest](data/multi-domain-financial-discovery-20260918/tttech-auto/crawl-manifest.json)

### RT-RK

The crawler found a [Serbian merger announcement involving Oblo Living and RT-RK](https://www.rt-rk.com/nacrt-ugovora-o-pripajanju-oblo-living-doo-istrazivacko-razvojnom-institutu-rt-rk-doo-novi-sad/)
and retained the [draft merger agreement PDF link](https://www.rt-rk.com/wp-content/uploads/Nacrt-Ugovora-o-statusnoj-promeni-Oblo-Living-RT-RK.pdf)
with its Serbian label and surrounding text. Investment and partnership press-release
PDF references were also retained. This demonstrates useful local-language
selection even when web search is unavailable.

TTTech's homepage was reached at page 23; its investor portal returned HTTP 403
at page 24. No RT-RK-specific annual financial statements were captured. Failed
searches and bounded coverage prevent any conclusion that such reports are absent.

The run retained **49 generic document references**, including sitemap-discovered
patents, policies and technical material. Only a subset concerns the requested
financial/ownership objective; all PDF bodies remain unexamined.

Two quality issues need attention:

- Product demonstrations and partner announcements were accepted as medium-value
  navigation without a specific financial or ownership signal, consuming budget.
- The stored quote approving TTTech as a parent source describes TTTech as a
  technology leader; it does **not** state ownership of RT-RK. The model's reason
  uses group context, but quotation matching alone did not ensure that the saved
  evidence supports the relationship. This approval must remain a navigation
  hypothesis, not a verified ownership fact.

[Portable result](data/multi-domain-financial-discovery-20260918/rt-rk/result.json)
· [Manifest](data/multi-domain-financial-discovery-20260918/rt-rk/crawl-manifest.json)

### Nordic Semiconductor

The homepage was captured, but its extracted links contained no investor route.
The sitemap returned HTTP 403; two selected news pages hit Cloudflare challenges;
all three search queries failed with HTTP 429. The automatic run found no report
references and correctly retained a partial result.

An independent check using the assistant's web search found the official
[annual reports page](https://www.nordicsemi.com/Investor-Relations/Reports/Annual).
That URL was **not** fed into the automatic run above.

A separate CLI run then supplied only that page through `--pages`:

| Direct-page diagnostic | Result |
| --- | --- |
| HTTP status / crawl status | 200 / finished |
| Elapsed | 5.668 s |
| LLM calls / tokens | 0 / 0 |
| Document reference | [Annual Report 2025 PDF](https://www.nordicsemi.com/-/media/Investor-Relations-and-QA/Annual-Reports/2025/Annual-Report-2025.pdf) |
| Preserved label | Download Annual Report 2025 |
| Preserved heading | Annual Report 2025 (March 24, 2026) |

This establishes that the known report page can be fetched and its document link
extracted. Automatic discovery was the limiting step in this case. The diagnostic
must not be counted as successful automatic discovery.

[Automatic result](data/multi-domain-financial-discovery-20260918/nordicsemi/result.json)
· [Direct-page result](data/multi-domain-financial-discovery-20260918/nordicsemi-explicit-reports/result.json)

### Melexis

Both initial fetch attempts were challenged by Cloudflare. The service stopped
with `needs_review` and `initial_page_unavailable`; it made no model calls.
No inference about the availability of company reports is justified by this run.

[Portable result](data/multi-domain-financial-discovery-20260918/melexis/result.json)
· [Fetch receipt](data/multi-domain-financial-discovery-20260918/melexis/fetches/p0001-2.json)

## What to improve next

1. Use a reliable search-provider integration. Browser-based Brave search failed
   five of seven times and prevented discovery of known public reports.
2. Tighten financial page selection and prioritize report/filing routes earlier.
   Avoid spending the budget on product partnerships and translated duplicates.
   Link assessment consumed about 90–91% of tokens in the two longer runs.
3. Verify that ownership quotations actually express the proposed relationship
   before granting parent-source navigation scope.
4. Resolve JavaScript document controls, while preserving their labels and source
   provenance. The EVI annual-accounts attachment is a reproducible example.
5. Keep generic document references available to offline processing. The current
   deterministic `financial_links` tags missed the Serbian merger and NXP
   acquisition PDF references even though their links were retained elsewhere.

## Artifacts and validation

- [Machine-readable comparison](data/multi-domain-financial-discovery-20260918/summary.json)
- [Benchmark commands and parameters](data/multi-domain-financial-discovery-20260918/benchmark.json)
- [Additional Nordic run](data/multi-domain-financial-discovery-20260918/nordicsemi-run.json)
- [Source fingerprint and package versions](data/multi-domain-financial-discovery-20260918/source-fingerprint.json)
- [Review notes](data/multi-domain-financial-discovery-20260918/review-notes.json)
- [Validation receipt](data/multi-domain-financial-discovery-20260918/validation.json)

Validation confirmed that all four automatic runs respected their page, model,
search and source-domain budgets, their portable results matched their manifests,
and all 48 captured HTML hashes matched. Source fingerprints were unchanged.
These are live integration tests; no production source changes were made and no
financial interpretation or database/S3 publication was performed.
