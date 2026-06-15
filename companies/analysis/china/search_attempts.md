# China — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `China National Enterprise Credit Information Publicity System GSXT gsxt.gov.cn company search unified social credit code open data API bulk`
- Result: GSXT (SAMR) = official register; search by name or **USCC (18-char, doubles as taxpayer id)**; returns USCC, status, legal rep, address, establishment date. **Real-name authentication required since Nov 2021**; no documented open API/bulk.
- Decision: confirm gated; check financials + open data.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `China listed company financial statements CSRC Shanghai Shenzhen stock exchange annual report cninfo open access`
- Result: listed-company financials via **cninfo (巨潮资讯网)** + SSE/SZSE; CSMAR (paid) for research. Non-listed not publicly disclosed.
- Decision: financials = listed-only (cninfo/SSE/SZSE).

## Attempt 3
- Date/time: 2026-06-15
- Source: curl (reachability)
- Query: HEAD/GET gsxt.gov.cn, cninfo.com.cn, sse.com.cn, creditchina.gov.cn, tianyancha.com
- Result: **GSXT HTTP 521** (gated/unreachable externally); **cninfo 200**; **SSE 200**; **Credit China 412** (bot-protected); **Tianyancha 419** (anti-bot).
- Decision: GSXT = gated (real-name + CAPTCHA), no open bulk; financials reachable (cninfo/SSE, listed-only); aggregators anti-bot/paid. Classify China as portal-gated, no open bulk, financials listed-only.
