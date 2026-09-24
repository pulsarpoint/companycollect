# NOVELIC financial discovery audit — 18 September 2026

The full crawl missed actual NOVELIC financial statements published by its parent,
Sona Comstar. This is principally a source-discovery and navigation-policy gap.
It is not evidence that NOVELIC has no public financial information, nor that a
larger selection model is required.

This audit inspected the existing full-crawl decisions, fetched the official
subsidiary-report index, replayed our deterministic HTML parser, and visually
checked the four pages of the latest Serbian NOVELIC PDF. It made no production
code changes and ran no additional paid-model crawl.

## Verified sources missed by the crawl

[Sona Comstar's subsidiary financial statements index](https://sonacomstar.com/investor/subsidiary-companies-financial-statements)
contains four NOVELIC-related PDF links: three labeled for the Serbian company and
one for Novelic India Private Limited. The Indian company is a distinct entity;
these documents should not be merged as interchangeable statements.

The index's “Novelic d.o.o Consolidated Financials.” entry links to
[NOVELIC Group financial statements](https://sonacomstar.com/files/documents/novelic-serbia-document-wMpyMY.pdf).
The four-page document contains a financial-position statement dated 31 March
2026, a profit-and-loss statement for April 2025–March 2026, a cash-flow statement,
and changes in equity. It includes consolidation columns and uses thousands of
RSD, with EUR conversion columns on the first two pages. This audit establishes
the availability of the source; it does not normalize or validate financial
amounts or establish an audit opinion. Group, standalone, currency and period
must be preserved when financial analysis is implemented.

The other Serbian links are:

- [Novelic d.o.o Beograd report](https://sonacomstar.com/files/documents/novelic-d-o-o-beograd--document-aWNkDI.pdf)
- [Novelic d.o.o.Beograd report](https://sonacomstar.com/files/documents/novelic-d-o-o-beograd-document-eCZdgN.pdf)

Those two older documents were identified on the index; their contents were not
analyzed in this audit.

## Where the current pipeline loses this evidence

### 1. Parent-company navigation is rejected even when the relationship is known

The recorded queue contains two unvisited Sona homepage candidates:

| Candidate | Discovery source | Recorded decision |
| --- | --- | --- |
| `c00185` | NOVELIC announcement about expansion into India | `related_company`, low potential, no role |
| `c00188` | NOVELIC announcement about partnering with Sona Comstar | `related_company`, low potential, no role |

The first candidate's captured surrounding text explicitly calls Sona Comstar
NOVELIC's parent. Its recorded reason nevertheless rejects the homepage for
lacking target financial content. This fails to distinguish a source of navigation
from a page that directly contains the requested evidence.

This behavior is reinforced by code, not just the selected model:

- `src/crawler_service/prompts.py:104` classifies parent-company general pages as
  `related_company` and restricts external navigation to already target-scoped pages.
- `src/crawler_service/discovery.py:717` excludes `related_company` candidates in
  `pick_for_instructions`, even if they otherwise have useful navigation potential.
- `src/crawler_service/discovery.py:383` restricts subsequent external navigation.

The missed route is: NOVELIC announcement → confirmed parent → Investors →
Accounts of Subsidiaries → NOVELIC report. A different custom objective alone
cannot override the hard exclusion of a candidate classified `related_company`.

### 2. Discovery has no web-search or source-planning step

`crawl.py` seeds the homepage and sitemap, then assesses discovered links. The
model ranks supplied candidates; it is explicitly instructed not to browse or
invent URLs. There is no web-search operation to find a registry, filing,
acquisition announcement or parent disclosure when those sources are absent from
the queue.

`crawl.py:444` also records that selection has no extraction-coverage feedback.
Once candidates are exhausted, an unresolved financial objective does not trigger
a new search. A broader research agent can use additional sources and searches;
the current crawler does not have that capability.

### 3. Financial-link recognition misses nearby document labels

The same `collect_page_observations` function used by captures was replayed on the
official index HTML:

| Parser output | All links | NOVELIC-related PDF links |
| --- | ---: | ---: |
| `document_links` | 71 | 4 |
| `financial_links` | 49 | 0 |

All four PDF anchors say only “Download”, have no title attribute, and have URLs
without financial keywords. The nearby document-row text contains the useful
entity/report label. The financial-link heuristic at
`src/crawler_service/page_observations.py:405` checks the URL path, anchor text and
title, but not that surrounding row or section heading.

The generic document inventory therefore preserves the links if the page is
reached, while the financial-specific inventory misses them. The raw page also
preserves the surrounding content, but the document observation does not attach
that context to each link.

Conversely, the full crawl's 20 financial-link candidates were generic investor
links from Analog Devices, Infineon and Lattice partner-profile pages, not
established NOVELIC statements. Keyword matches require source/subject attribution
before being treated as target-company financial evidence.

### 4. Document content and financial interpretation are deliberately deferred

`crawl.py:52` requests document links without downloading their contents.
`discovery.py:305` retains PDF/office-document URLs separately instead of fetching
them as HTML pages. Financial observations are links with
`content_examined=false`, not extracted revenue, profit, assets or financial periods.

The latest NOVELIC PDF is scanned: `pdftotext` returned zero non-whitespace
characters. A later document processor needs OCR in addition to ordinary PDF text
extraction. Collecting the PDF bytes optionally would preserve evidence for that
later stage without introducing financial interpretation into crawling.

## Recommended change

Keep collection and financial analysis separate, while expanding discovery:

1. Add bounded discovery driven by the objective: start with the company website,
   follow explicitly evidenced parent/filing sources, and use web search when
   useful sources remain unresolved. Record query, result URL and discovery reason.
2. Separate the owner of a source from the subject of its evidence. Permit a
   confirmed parent's investor/subsidiary index as a navigation source without
   treating the parent's financial totals or other subsidiaries as NOVELIC facts.
   Preserve external page/depth/domain budgets.
3. Attach document-row text, nearby headings, entity labels and source URLs to
   document candidates. Use those observations for classification and later
   matching, preserving uncertainty rather than inferring a period from a filename.
4. Offer optional document capture with type/size/time limits, a content hash and
   provenance. Keep OCR, financial normalization and interpretation in the offline
   processing module.
5. Report collection coverage per objective: sources found, documents linked or
   captured, exclusions and unresolved gaps. Do not treat exhausted candidates as
   proof of financial-data completeness.

The first regression should use the saved NOVELIC parent-link decisions and Sona
index HTML. It should verify bounded navigation, the Serbian report's retained
label/context, separation from the Indian entity, and rejection of unrelated
partner financial totals. A subsequent live benchmark can measure marginal pages,
tokens, search calls and wall time against the existing full crawl.

## Local evidence

- Original crawl: `data/novelic-full-crawl-20260918-verified/crawl/queue.json`
- Audit directory: `data/novelic-financial-discovery-audit-20260918/`
- `recorded-sona-candidates.json`: the two original rejection decisions
- `subsidiary-statements.html`: fetched official index
- `parser-replay.json`: actual deterministic parser output
- `novelic-serbia-financial-statements.pdf`: downloaded source PDF
- `statement-1.png` through `statement-4.png`: inspected page renders
- `statement.txt`: empty text-extraction result apart from page separators

No database import, S3 upload or deployment was performed for this audit.
