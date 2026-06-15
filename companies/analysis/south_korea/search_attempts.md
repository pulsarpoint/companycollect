# South Korea — Search Attempts

## Attempt 1

- Date/time: 2026-06-15
- Source: OpenDART API (FSS) + DART portal
- URL: https://opendart.fss.or.kr/api/list.json ; https://dart.fss.or.kr/
- Language: Korean/English
- Why: DART is Korea's authoritative corporate disclosure system; OpenDART is its free API.
- Result: OpenDART endpoints return `{"status":"900"}` (rejected) without a key; corpCode.xml → 302. DART portal HTTP 200.
- Decision: RECOMMENDED but blocked_by_authentication (free crtfc_key). Best identity + financial source.

## Attempt 2

- Date/time: 2026-06-15
- Source: OpenDART API guide pages
- URL: https://opendart.fss.or.kr/guide/detail.do (company / corpCode / financial endpoints)
- Language: Korean
- Why: Confirm field schemas accurately.
- Result: confirmed company.json (corp_code, corp_name(_eng), jurir_no, bizr_no, corp_cls, ceo_nm, est_dt, adres, induty_code, hm_url), corpCode.xml (corp_code/name/stock_code/modify_date), fnlttSinglAcnt (bsns_year, reprt_code, fs_div, sj_div, account_nm, thstrm_amount, currency).
- Decision: Schema documented from the official guide.

## Attempt 3

- Date/time: 2026-06-15
- Source: NTS business-status API (data.go.kr / odcloud)
- URL: https://www.data.go.kr/data/15081808/openapi.do ; https://api.odcloud.kr/api/nts-businessman/v1/status
- Language: Korean
- Why: Validate business-registration status / tax status by 사업자등록번호.
- Result: data.go.kr page 200; POST without key → 401 `인증키는 필수 항목 입니다` (key required).
- Decision: blocked_by_authentication (free data.go.kr service key).

## Attempt 4

- Date/time: 2026-06-15
- Source: IROS — Supreme Court commercial registry
- URL: https://www.iros.go.kr/
- Language: Korean
- Why: The full commercial register, including unlisted companies.
- Result: HTTP 200; registry extracts are fee-based per issue; no open bulk/API.
- Decision: blocked_by_payment. Documentation only.

## Attempt 5

- Date/time: 2026-06-15
- Source: data.go.kr (Korea Open Data Portal)
- URL: https://www.data.go.kr/
- Language: Korean
- Why: Find open company datasets/APIs.
- Result: HTTP 200; hosts many company/tax APIs (NTS status, FSC/KED corporate-info) behind free service keys, mostly under KOGL.
- Decision: useful_secondary_source — the hub for non-DART company APIs (free key).
