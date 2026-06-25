# Qatar Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: MoCI, Single Window, QFC public register, QSE listed, data.gov.qa
- Language: English
- Why this query was tried: locate the official onshore registry, financial-centre
  register, exchange, and open-data portal.
- Top relevant URLs:
  - https://www.moci.gov.qa/en/ → HTTP 200
  - https://www.businessinqatar.gov.qa/ → DNS does not resolve
  - https://www.qfc.qa/en/registration/public-register → 302 → /errors/404
  - https://www.qe.com.qa/listed-securities → HTTP 200
  - https://www.data.gov.qa/ → 302 → /pages/homepage/
- Result: MoCI main site up; QFC register link broken on the marketing site; QSE up;
  data portal up.
- Decision: chase the QFC register's real host and the data.gov.qa API.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes
- Query: QFC firms/public-register variants, MoCI CR inquiry, data.gov.qa catalog, QCB
- Language: English
- Why this query was tried: find the working QFC register URL and test the open-data API.
- Top relevant URLs:
  - https://www.qfc.qa/en/public-register → 302 → https://eservices.qfc.qa/qfcpublicregister/publicregister.aspx
  - https://www.data.gov.qa/api/explore/v2.1/catalog/datasets?limit=5 → HTTP 200 (Opendatasoft)
  - https://www.moci.gov.qa/en/eservices/commercial-registration-inquiry/ → 404
- Result: QFC register host found (eservices.qfc.qa); data.gov.qa runs Opendatasoft v2.1.
- Decision: fetch the QFC register page; search the open-data catalog for company datasets.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: direct HTTP fetch + catalog query
- Query: QFC publicregister.aspx; data.gov.qa `q=company`
- Language: English
- Why this query was tried: inspect QFC register structure; confirm whether the open-data
  portal carries a company register.
- Top relevant URLs:
  - https://eservices.qfc.qa/qfcpublicregister/publicregister.aspx → HTTP 200 (840 KB)
  - https://www.data.gov.qa/api/explore/v2.1/catalog/datasets?q=company → total_count 1405,
    all statistical datasets
- Result: QFC register is a real public register (table headers: Firm Name, QFC Number,
  Senior Executive Function, Full Name, Address, Date Of Registration, QFCA Licensed) but
  the grid is empty on GET; data.gov.qa has no company register.
- Decision: classify QFC as useful_secondary_source (postback); data.gov.qa as
  not_company_data.

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes
- Query: QFC SearchResult.aspx; QSE /api/markets/* and financial-statements; MoCI/Hukoomi/
  Invest Qatar/Single Window; __VIEWSTATE check
- Language: English
- Why this query was tried: confirm QFC is postback-gated, look for a QSE JSON API, and
  locate the onshore Single Window.
- Top relevant URLs:
  - https://eservices.qfc.qa/qfcpublicregister/SearchResult.aspx → 302 → 404_Error.aspx
  - https://www.qe.com.qa/api/markets/marketWatch → 404 (HTML)
  - https://hukoomi.gov.qa/en → 301/200; https://www.invest.qa/en → 200
  - https://www.business.gov.qa/ → DNS does not resolve
- Result: QFC register confirmed single-page postback (__VIEWSTATE, action publicregister.aspx);
  QSE has no clean open JSON API; onshore Single Window host not resolvable.
- Decision: finalize — Qatar is a browser-public/lookup country with no open bulk/API.
