# Informer, B92 and Google: first-page gate test

Tested 16 September 2026 using the public `research_company` URL controller,
Crawl4AI native cleaned HTML and direct `deepseek-flash` with high reasoning.
Configuration allowed three pages and eight model calls per site, so these stops
were eligibility decisions or fetch failures, not exhausted test budgets.
Robots checks remained enabled. No sitemap or further page was requested for any site.
All objective coverage remained `not_assessed`, and no detailed company extraction,
link ranking, external-link assessment or company summary request was made.

| Site | Outcome | Pages successfully fetched | Model calls | Description words |
| --- | --- | ---: | ---: | ---: |
| Informer | `skip_crawling` | 1 | 1 | 112 |
| B92 | `skip_crawling` | 1 | 1 | 152 |
| Google | `needs_review` | 0 | 0 | 18 |

Both news classifications were `news_media` with source-matched quotation evidence
and no corrections. These are successful checks of two requested news portals,
not a population-wide accuracy measurement. Google was not model-classified.

## Returned descriptions

### Informer

Informer is a Serbian news portal presenting itself as an independent daily newspaper (Nezavisne Dnevne Novine). The homepage is dominated by editorial content: breaking news and articles organised into sections such as Politika, Društvo, Hronika, Planeta, Sport, Zabava, Magazin, Džet set and Exatlon, plus columns, a daily top story, most-read ranking, weather forecast, horoscope and an opinion poll. It also promotes its own television output (Live TV, shows like "Budilnik" and "Na merama"), its printed edition, social media channels and mobile apps. Advertising banners appear alongside the editorial feed. The site's primary purpose is publishing news, entertainment and magazine content for a Serbian audience, not marketing a company's own products or services.

[Full JSON](data/site-gate-requested-20260916/informer/result.json)

### B92

B92.net is a Serbian-language news website whose front page presents a rolling stream of news articles across politics, world affairs, business, sport, lifestyle, health, technology, cars, culture and esports. The homepage is organised into labelled sections such as Info, Sport, Biz, Lokal, Život, Zdravlje, Tehnopolis, Automobili, Kultura, eSports and Putovanja, with headline lists, live-blog items, comment counts, video items and weather forecasts for several Serbian cities. Content consists of editorial reporting on current events in Serbia, the region and the world, sport results and fixtures, and lighter entertainment and lifestyle items. Navigation links lead to topical subsections and tag pages, plus companion brands (Superžena, 92Putovanja, B92.sport) and mobile applications. Advertising slots and a sponsored-content widget appear alongside editorial material. The footer carries a copyright notice, marketing, imprint, terms of use and privacy links. The site's primary purpose is publishing news and editorial content rather than representing a specific company's products or services.

[Full JSON](data/site-gate-requested-20260916/b92/result.json)

### Google

The first page could not be retrieved, so the site's purpose and company eligibility could not be determined.

[Full JSON](data/site-gate-requested-20260916/google-fixed/result.json)

## Google fetch diagnosis and fix

The initial Google run repeated the earlier adapter exception:
`AttributeError: 'tuple' object has no attribute 'redirected_url'`.
The installed Crawl4AI code returns a bare `CrawlResult` when its robots check
rejects a URL, whereas successful calls return a `CrawlResultContainer`. Iterating
a bare Pydantic result produces field/value tuples. The adapter now accepts both
return types, preserving the underlying failure instead of replacing it with a
Python exception. No robots rule was disabled or bypassed.

The corrected retry returned `initial_page_unavailable`, one fetch attempt and
`Access denied by robots.txt`. Crawl4AI supplies status 403 for this refusal; this
is not evidence that Google's homepage itself returned HTTP 403. No HTML or model
classification exists for Google in this experiment. The generic description states
that the site could not be assessed; it does not guess its type from the domain.

The live browser regression uses a local HTTP server whose robots.txt disallows
all paths. It verifies that the homepage and sitemap are never requested, no model
call is made, and the actual denial is saved after one attempt. All six real-browser
integration tests passed. The normal suite passed 167 tests, with six opt-in browser
tests skipped in that invocation. Ruff format/lint and type checks passed for the
changed Python files. Package version is 0.17.1; output schema remains 1.11.

## Artifacts and resource use

Run directory: `data/site-gate-requested-20260916`. `run.py` reproduces the original
three-site batch, requiring a fresh output directory. `informer/` and `b92/` retain
complete result JSON, native/rendered HTML, source hashes, model requests/responses
and fetched link inventories. `google/` retains the initial adapter failure;
`google-fixed/` contains the corrected retry. `summary.json` is the original batch
summary and therefore intentionally still references the initial Google failure.

The existing local technology snapshot was pinned; no central system or S3 writes
were performed. Data directories are locally saved and Git-ignored.

| Site | Native HTML characters | Input tokens | Output tokens |
| --- | ---: | ---: | ---: |
| Informer | 205,544 | 59,314 | 509 |
| B92 | 484,110 | 115,236 | 788 |
| Google | No captured page | 0 | 0 |

API responses did not report monetary cost. The large news homepages demonstrate a
remaining efficiency opportunity: a smaller first-page representation could reduce
classification input, but requires separate testing for reliable site-purpose
coverage. This test used complete native cleaned HTML and unchanged gate prompts.
