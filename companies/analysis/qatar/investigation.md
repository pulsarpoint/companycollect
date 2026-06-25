# Qatar Company Data Investigation

## Goal

Find official/open sources for Qatari company data: registry, identifiers, status,
activities, ownership, and financials, with reproducible access notes.

## What was found

Qatar has a **dual-registry** structure plus a separate exchange:

1. **QFC Public Register** — Qatar Financial Centre Authority (QFCA).
   `https://eservices.qfc.qa/qfcpublicregister/publicregister.aspx`. The official public
   register of **QFC-licensed firms**, **approved individuals** (with senior executive
   functions), **registered insolvency practitioners**, and **official liquidators**.
   Browser-public, no login or payment. It is a single ASP.NET page driven by `__VIEWSTATE`
   and **search postback** — the result grid is **empty on a plain GET** and populated only
   after a search, so there is **no clean GET/bulk/API**. The page's table headers reveal
   the data model: *Firm Name, QFC Number, Senior Executive Function, Full Name, Address,
   Date Of Registration, QFCA Licensed*. Scope is limited to the **financial centre** — it
   is **not** the onshore companies registry. Approved-individual names/addresses are
   personal data.

2. **Ministry of Commerce and Industry (MoCI) — Commercial Registration** — the **onshore**
   companies registry (the bulk of Qatari companies), keyed on the **Commercial
   Registration (CR) number**. The main MoCI site (`moci.gov.qa`) is reachable, but the
   e-service paths now return **404** (the portal was restructured) and the Single Window
   hosts (`businessinqatar.gov.qa`, `business.gov.qa`) **did not resolve**. The CR
   verification service is a **per-CR lookup** (commonly Arabic, often behind the national
   portal). **No open bulk file or API** was found. The field model here is documented from
   public knowledge only; no per-company values were captured.

3. **Qatar Stock Exchange (QSE / Bourse de Doha)** — `https://www.qe.com.qa/listed-securities`.
   Listed-company directory, financial statements, and disclosures. **Browser-public**
   (Liferay portal) but data is loaded via **portlet AJAX**; guessed JSON endpoints
   (`/api/markets/marketWatch`, `/api/markets/companies`) returned **404**. No clean open
   API. Listed companies only; identifiers are the **QSE ticker symbol** and **ISIN (QA…)**.

4. **Qatar Open Data Portal (data.gov.qa)** — Planning and Statistics Authority. Runs an
   **Opendatasoft Explore v2.1 API** that works (1,405 datasets), but the catalog is
   **statistical** (census, trade, employment, sports). **No company/legal-entity register
   dataset** exists there. Classified `not_company_data`.

## What was NOT found

- No onshore (MoCI) open bulk file, CSV/XML dump, or documented open company API.
- No clean machine-readable API for QFC firms (postback only) or QSE listed companies.
- No beneficial-ownership open register; no open VAT register (Qatar introduced no general
  VAT as of investigation; tax administration is via the General Tax Authority, not open).

## Conclusion

Qatar is a **browser-public / lookup** country with **no open bulk or API** for company
data. The best structured open source is the **QFC Public Register** (financial-centre
firms only, postback-driven). The **onshore** registry (**MoCI CR**) is the authoritative
identifier source but is not openly downloadable. **QSE** covers listed companies. The
open-data portal carries statistics, not a register. Nothing here was bypassed or
fabricated; gated/postback/AJAX sources are documented from public structure only.

## Recommended ingestion approach

Manual review / browser-public lookup. Implement QFC firm retrieval behind a browser
context (respecting the postback), confirm an official MoCI data-sharing/CR-lookup channel
for onshore companies, and use QSE for listed entities. Redact personal data (approved
individuals, owners, managers).
